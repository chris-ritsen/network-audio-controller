use super::status::StatusKind;
use crate::devices::transport::{CommandIntent, DeviceTransport};
use anyhow::{Context, Result, bail, ensure};
use netaudio_core::responses::{CmcRegistrationResponse, parse_cmc_registration_response};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Instant, timeout_at};

const OBSERVATION_DEADLINE: Duration = Duration::from_secs(3);
const MAXIMUM_PENDING_OBSERVATIONS: usize = 256;

#[derive(Clone, Copy, Debug)]
pub enum StatusQuery {
    Lock,
    Manufacturer,
    Platform,
    Interface,
    Clock,
    ClockProfile(u16),
    SampleRate,
    Encoding,
    Pullup,
    Codec,
    Aes67,
    InterfaceStatistics,
    SwitchConfiguration,
}

impl StatusQuery {
    pub fn kind(self) -> StatusKind {
        match self {
            Self::Lock => StatusKind::Lock,
            Self::SwitchConfiguration => StatusKind::SwitchConfiguration,
            Self::Manufacturer => StatusKind::Manufacturer,
            Self::Platform => StatusKind::Platform,
            Self::Interface => StatusKind::Interface,
            Self::Clock | Self::ClockProfile(_) => StatusKind::Clock,
            Self::SampleRate => StatusKind::SampleRate,
            Self::Encoding => StatusKind::Encoding,
            Self::Pullup => StatusKind::Pullup,
            Self::Codec => StatusKind::Codec,
            Self::Aes67 => StatusKind::Aes67,
            Self::InterfaceStatistics => StatusKind::InterfaceStatistics,
        }
    }

    pub fn command(self, device_identity: Option<&str>) -> Result<Value> {
        let name = match self {
            Self::Lock => "probe_lock_reset_status",
            Self::SwitchConfiguration => "probe_switch_configuration",
            Self::Manufacturer => "make_model",
            Self::Platform => "dante_model",
            Self::Interface => "probe_interface_status",
            Self::Clock | Self::ClockProfile(_) => "refresh_clock_status",
            Self::SampleRate => "probe_sample_rate",
            Self::Encoding => "probe_encoding",
            Self::Pullup => "probe_sample_rate_pullup",
            Self::Codec => "probe_codec_status",
            Self::Aes67 => "probe_aes67",
            Self::InterfaceStatistics => "probe_interface_statistics",
        };
        let mut command = json!({"command": name});

        if let Self::ClockProfile(profile) = self {
            command["control_profile"] = json!(profile);
        }

        if matches!(self, Self::Manufacturer | Self::Platform) {
            let identity = device_identity
                .and_then(netaudio_core::device_identity::canonical_device_mac)
                .context("device identity is unavailable for its version query")?;
            command["mac"] = json!(identity);
        }

        Ok(command)
    }
}

struct PendingObservation {
    key: String,
    kind: StatusKind,
    registered_at: Instant,
    sender: watch::Sender<Option<Map<String, Value>>>,
}

struct PreparedQuery {
    command: Value,
    kind: StatusKind,
}

impl StatusQuery {
    fn prepare(self, identity: Option<&str>) -> Result<PreparedQuery> {
        Ok(PreparedQuery {
            command: self.command(identity)?,
            kind: self.kind(),
        })
    }
}

#[derive(Default)]
struct PendingState {
    identifier: u64,
    observations: BTreeMap<u64, PendingObservation>,
}

#[derive(Clone, Default)]
pub struct StatusRequests {
    state: Arc<Mutex<PendingState>>,
}

struct ObservationGuard {
    identifier: u64,
    state: Arc<Mutex<PendingState>>,
}

pub struct StatusObservation {
    receiver: watch::Receiver<Option<Map<String, Value>>>,
    kind: StatusKind,
    _guard: ObservationGuard,
}

impl StatusObservation {
    pub async fn wait(
        mut self,
        duration: Duration,
        matches: impl Fn(&Map<String, Value>) -> bool + Send,
    ) -> Result<Map<String, Value>> {
        let deadline = Instant::now() + duration;
        let mut last_observed = None;

        loop {
            if let Some(observed) = self.receiver.borrow_and_update().clone() {
                if matches(&observed) {
                    return Ok(observed);
                }

                last_observed = Some(observed);
            }

            match timeout_at(deadline, self.receiver.changed()).await {
                Ok(changed) => changed.context("status observation canceled")?,
                Err(_) => {
                    return last_observed
                        .with_context(|| format!("{} observation timed out", self.kind.as_str()));
                }
            }
        }
    }
}

impl Drop for ObservationGuard {
    fn drop(&mut self) {
        match self.state.lock() {
            Ok(mut state) => {
                state.observations.remove(&self.identifier);
            }
            Err(error) => tracing::error!(%error, "could not release status observation"),
        }
    }
}

impl StatusRequests {
    pub async fn query(
        &self,
        transport: &mut DeviceTransport,
        device: &str,
        identity: Option<&str>,
        query: StatusQuery,
    ) -> Result<Map<String, Value>> {
        self.query_until(transport, device, identity, query, |_| true)
            .await
    }

    pub async fn query_until(
        &self,
        transport: &mut DeviceTransport,
        device: &str,
        identity: Option<&str>,
        query: StatusQuery,
        matches: impl Fn(&Map<String, Value>) -> bool + Send,
    ) -> Result<Map<String, Value>> {
        self.observe_command(
            transport,
            device,
            query.prepare(identity)?,
            OBSERVATION_DEADLINE,
            matches,
        )
        .await
    }

    pub async fn query_with_deadline(
        &self,
        transport: &mut DeviceTransport,
        device: &str,
        query: StatusQuery,
        duration: Duration,
        matches: impl Fn(&Map<String, Value>) -> bool + Send,
    ) -> Result<Map<String, Value>> {
        self.observe_command(transport, device, query.prepare(None)?, duration, matches)
            .await
    }

    async fn observe_command(
        &self,
        transport: &mut DeviceTransport,
        device: &str,
        query: PreparedQuery,
        duration: Duration,
        matches: impl Fn(&Map<String, Value>) -> bool + Send,
    ) -> Result<Map<String, Value>> {
        let observation = self.subscribe(device, query.kind)?;
        transport
            .execute(&query.command, CommandIntent::Read)
            .await?;
        observation.wait(duration, matches).await
    }

    pub fn subscribe(&self, device: &str, kind: StatusKind) -> Result<StatusObservation> {
        let (sender, receiver) = watch::channel(None);
        let guard = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("status request lock poisoned"))?;
            ensure!(
                state.observations.len() < MAXIMUM_PENDING_OBSERVATIONS,
                "too many pending status requests"
            );
            state.identifier = state
                .identifier
                .checked_add(1)
                .context("status request identifiers exhausted")?;
            let identifier = state.identifier;
            state.observations.insert(
                identifier,
                PendingObservation {
                    key: device.to_owned(),
                    kind,
                    registered_at: Instant::now(),
                    sender,
                },
            );
            ObservationGuard {
                identifier,
                state: Arc::clone(&self.state),
            }
        };
        Ok(StatusObservation {
            receiver,
            kind,
            _guard: guard,
        })
    }

    pub fn observe(
        &self,
        device: &str,
        kind: StatusKind,
        received: Instant,
        fields: &Map<String, Value>,
    ) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("status request lock poisoned"))?;
        for pending in state.observations.values() {
            if pending.key == device && pending.kind == kind && pending.registered_at <= received {
                pending.sender.send_replace(Some(fields.clone()));
            }
        }

        Ok(())
    }
}

pub async fn register(
    transport: &mut DeviceTransport,
    expected_identity: Option<&str>,
) -> Result<CmcRegistrationResponse> {
    let response = transport
        .execute(&json!({"command": "cmc_register"}), CommandIntent::Read)
        .await?;
    let registration = parse_cmc_registration_response(&response)
        .context("malformed monitoring registration response")?;
    ensure!(
        registration.accepted,
        "monitoring registration was rejected"
    );

    if let (Some(expected), Some(actual)) = (
        expected_identity.map(|value| value.replace(':', "")),
        registration.responder_instance.as_ref(),
    ) && expected.len() == 16
    {
        ensure!(
            expected.eq_ignore_ascii_case(actual),
            "monitoring registration answered by a different device instance"
        );
    }

    if let Some(addresses) = &registration.control_addresses
        && !addresses.is_empty()
    {
        let address = transport.address().address.to_string();
        let ports: BTreeSet<_> = addresses
            .iter()
            .filter(|entry| entry.usable && entry.ipv4 == address)
            .map(|entry| entry.port)
            .collect();

        if ports.len() != 1 {
            bail!("monitoring registration has no unique control address on {address}");
        }

        transport.set_settings_port(*ports.first().context("missing settings port")?)?;
    }

    Ok(registration)
}
