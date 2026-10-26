use super::multicast::ReceivedPacket;
use crate::devices::transport::DeviceTransport;
use anyhow::{Context, Result, bail, ensure};
use netaudio_core::conmon_export::{ExportCollector, ExportConfiguration, ExportKind};
use serde_json::{Map, Value, json};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

const MAXIMUM_ENCODED_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_ARCHIVE_BYTES: u64 = 256 * 1024 * 1024;
const FRAGMENT_QUEUE_CAPACITY: usize = 128;
const AUDIO_LOG_MEMBER: &str = "tmp/dante_data/apec.log";

struct Pending {
    key: String,
    registered: Instant,
    settings_port: u16,
    packets: mpsc::Sender<ReceivedPacket>,
}

pub struct Exports {
    pending: Arc<Mutex<Option<Pending>>>,
    capacity: Arc<Semaphore>,
}

struct Registration(Arc<Mutex<Option<Pending>>>);

impl Drop for Registration {
    fn drop(&mut self) {
        match self.0.lock() {
            Ok(mut pending) => *pending = None,
            Err(error) => tracing::error!(%error, "could not release diagnostic export"),
        }
    }
}

#[derive(Debug)]
pub struct Unsupported;

impl std::fmt::Display for Unsupported {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("device returned an empty diagnostic export")
    }
}

impl std::error::Error for Unsupported {}

impl Default for Exports {
    fn default() -> Self {
        Self {
            pending: Arc::new(Mutex::new(None)),
            capacity: Arc::new(Semaphore::new(1)),
        }
    }
}

impl Exports {
    pub fn invalidate(&self, key: &str) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("diagnostic export lock poisoned"))?;
        if pending.as_ref().is_some_and(|pending| pending.key == key) {
            *pending = None;
        }
        Ok(())
    }

    pub fn observe(&self, key: &str, packet: &ReceivedPacket) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("diagnostic export lock poisoned"))?;
        let Some(request) = pending.as_ref() else {
            return Ok(());
        };

        if request.key != key || packet.received_monotonic < request.registered {
            return Ok(());
        }

        if packet.payload.is_empty() && packet.source.port() != request.settings_port {
            return Ok(());
        }

        if request.packets.try_send(packet.clone()).is_err() {
            *pending = None;
            bail!("diagnostic export fragment queue unavailable; export failed");
        }

        Ok(())
    }

    pub async fn audio_capabilities(
        &self,
        transport: &mut DeviceTransport,
        key: &str,
        duration: Duration,
        cancellation: &CancellationToken,
    ) -> Result<Map<String, Value>> {
        let deadline = Instant::now() + duration;
        let capacity = Arc::clone(&self.capacity);
        let permit = tokio::select! {
            _ = cancellation.cancelled() => bail!("diagnostic export canceled"),
            permit = timeout_at(deadline, capacity.acquire_owned()) => {
                permit.context("diagnostic export capacity deadline expired")??
            }
        };
        let (sender, mut receiver) = mpsc::channel(FRAGMENT_QUEUE_CAPACITY);
        let registration = Registration(Arc::clone(&self.pending));
        *self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("diagnostic export lock poisoned"))? = Some(Pending {
            key: key.to_owned(),
            registered: Instant::now(),
            settings_port: transport.address().settings_port,
            packets: sender,
        });
        transport
            .send_command(&json!({"command": "device_log_export"}))
            .await?;
        let mut collector = ExportCollector::new(ExportConfiguration {
            kind: ExportKind::DiagnosticLogs,
            maximum_encoded_size: MAXIMUM_ENCODED_BYTES
                .try_into()
                .expect("nonzero export bound"),
        });
        let encoded = loop {
            let packet = tokio::select! {
                _ = cancellation.cancelled() => bail!("diagnostic export canceled"),
                packet = timeout_at(deadline, receiver.recv()) => {
                    packet.context("diagnostic export deadline expired")?
                        .context("diagnostic export fragment queue closed before completion")?
                }
            };

            if packet.payload.is_empty() {
                return Err(Unsupported.into());
            }

            let fragment = netaudio_core::responses::parse_conmon_export_fragment(&packet.payload)
                .context("malformed diagnostic export fragment")?;
            let progress = collector.accept(fragment).map_err(anyhow::Error::msg)?;

            if let Some(result) = progress.result {
                let result = serde_json::to_value(result)?;
                let encoded = result["encoded_payload_hexadecimal"]
                    .as_str()
                    .context("diagnostic export has no payload")?;
                break hex::decode(encoded).context("diagnostic export payload is invalid")?;
            }
        };
        drop(registration);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            audio_capabilities(&encoded)
        })
        .await
        .context("diagnostic archive worker failed")?
    }
}

fn audio_capabilities(encoded: &[u8]) -> Result<Map<String, Value>> {
    let mut decoder = flate2::bufread::GzDecoder::new(encoded);
    let mut decoded = Vec::new();
    decoder
        .by_ref()
        .take(MAXIMUM_ARCHIVE_BYTES + 1)
        .read_to_end(&mut decoded)
        .context("diagnostic export is not a complete gzip archive")?;
    ensure!(
        decoded.len() as u64 <= MAXIMUM_ARCHIVE_BYTES,
        "diagnostic archive exceeds decoded size limit"
    );
    ensure!(
        decoder.get_ref().is_empty(),
        "diagnostic gzip archive has trailing data"
    );
    let mut archive = tar::Archive::new(decoded.as_slice());
    let mut audio = None;

    for entry in archive
        .entries()
        .context("invalid diagnostic tar archive")?
    {
        let mut entry = entry.context("invalid diagnostic archive member")?;

        if entry.path()?.as_ref() == std::path::Path::new(AUDIO_LOG_MEMBER)
            && entry.header().entry_type().is_file()
        {
            ensure!(
                audio.is_none(),
                "diagnostic archive has duplicate audio logs"
            );
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes)?;
            audio = Some(bytes);
        }
    }

    let mut fields = Map::from_iter([("diagnostic_log_export_supported".to_owned(), json!(true))]);
    let Some(capabilities) = audio
        .as_deref()
        .and_then(netaudio_core::diagnostic_audio::parse)
    else {
        return Ok(fields);
    };
    let capabilities = serde_json::to_value(capabilities)?;

    for name in [
        "license_signature_length_bytes",
        "licensed_receive_channel_count",
        "licensed_transmit_channel_count",
        "licensed_redundancy_enabled",
    ] {
        if !capabilities[name].is_null() {
            fields.insert(name.to_owned(), capabilities[name].clone());
        }
    }

    if capabilities["channel_capacities"]
        .as_array()
        .is_some_and(|values| !values.is_empty())
    {
        fields.insert(
            "sample_rate_channel_capacities".to_owned(),
            capabilities["channel_capacities"].clone(),
        );
    }

    Ok(fields)
}
