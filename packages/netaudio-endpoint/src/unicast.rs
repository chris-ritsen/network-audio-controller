use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::{NonZeroU16, NonZeroU32};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{SecondsFormat, Utc};
use netaudio_core::native_audio::source_control::{
    self, Allocation, AllocationOutcome, Header, ReplyProfile,
};
use netaudio_core::native_audio::{
    Format, MAXIMUM_UDP_PAYLOAD_BYTES, REVERSE_KEEPALIVE, SampleWidth,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout_at};

use crate::recording::{Recording, RecordingContext, Summary};
use crate::recording_intake::Intake;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Profile {
    NativeV2Reference1102,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    profile: Profile,
    control_endpoint: SocketAddrV4,
    source_address: Ipv4Addr,
    interface_address: Ipv4Addr,
    receiver_hostname: String,
    receiver_flow_name: String,
    source_channel_identifiers: Vec<u16>,
    source_slot_capacity: NonZeroU16,
    sample_rate: NonZeroU32,
    sample_bits: u16,
    requested_frames_per_packet: NonZeroU16,
    maximum_media_datagram_bytes: NonZeroU16,
    reply_profile: ReplyProfile,
}

impl Session {
    pub fn read(path: &Path) -> Result<Self> {
        let mut data = Vec::new();
        File::open(path)?.take(1_048_577).read_to_end(&mut data)?;
        if data.len() > 1_048_576 {
            bail!("session configuration exceeds the one-megabyte input budget");
        }
        serde_json::from_slice(&data).context("read explicit unicast source context")
    }

    fn format(&self) -> Result<Format> {
        match self.profile {
            Profile::NativeV2Reference1102 => {}
        }

        for address in [self.interface_address, self.source_address, *self.control_endpoint.ip()] {
            if address.is_unspecified() || address.is_multicast() || address.is_broadcast() {
                bail!(
                    "session addresses must identify the selected local interface and admitted unicast source"
                );
            }
        }

        if self.control_endpoint.port() == 0 {
            bail!("source-control port must be the actual attributed service port");
        }

        let slots = u16::try_from(self.source_channel_identifiers.len())
            .context("source slot count is unrepresentable")?;
        if slots > self.source_slot_capacity.get() {
            bail!("ordered source slots exceed the established source slot capacity");
        }

        let width = match self.sample_bits {
            16 => SampleWidth::Signed16,
            24 => SampleWidth::Signed24,
            32 => SampleWidth::Signed32,
            _ => bail!("session sample width must be established signed PCM16, PCM24 or PCM32"),
        };

        let format = Format::new(self.sample_rate.get(), slots, width)?;
        let frame_bytes =
            u16::try_from(format.frame_bytes()).context("format exceeds WAVE frame size")?;
        format
            .sample_rate()
            .checked_mul(u32::from(frame_bytes))
            .context("format exceeds WAVE byte rate")?;
        let requested_bytes = usize::from(self.requested_frames_per_packet.get())
            .checked_mul(format.frame_bytes())
            .and_then(|bytes| bytes.checked_add(9))
            .context("requested media geometry overflow")?;
        let budget = usize::from(self.maximum_media_datagram_bytes.get());
        if budget > MAXIMUM_UDP_PAYLOAD_BYTES || requested_bytes > budget {
            bail!("requested media geometry exceeds the explicitly established UDP payload budget");
        }

        Ok(format)
    }
}

#[derive(Serialize)]
pub struct ControlSummary {
    control_endpoint: SocketAddrV4,
    reply_profile: ReplyProfile,
    source_channel_identifiers: Vec<u16>,
    requested_frames_per_packet: u16,
    allocation_state: &'static str,
    allocation_outcome: Option<AllocationOutcome>,
    stop_state: &'static str,
    stop_result: Option<u16>,
    reverse_packets_sent: u64,
    source_admission: &'static str,
    reverse_schedule: &'static str,
    journal_failure: Option<String>,
}

struct Journal {
    file: File,
    directory: PathBuf,
    failure: Option<String>,
}

impl Journal {
    fn create(directory: &Path) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(directory.join("source-control.partial.jsonl"))?,
            directory: directory.to_path_buf(),
            failure: None,
        })
    }

    fn record(&mut self, event: &'static str, data: &[u8]) -> bool {
        let result = (|| -> Result<()> {
            serde_json::to_writer(
                &mut self.file,
                &json!({
                    "observed_at": now(), "event": event, "datagram": data,
                }),
            )?;
            self.file.write_all(b"\n")?;
            self.file.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            if self.failure.is_none() {
                self.failure = Some(format!("{error:#}"));
            }
            false
        } else {
            true
        }
    }

    fn finish(self) -> Result<()> {
        self.file.sync_all()?;
        drop(self.file);
        if let Some(error) = self.failure {
            bail!("source-control journal remains partial: {error}");
        }
        std::fs::rename(
            self.directory.join("source-control.partial.jsonl"),
            self.directory.join("source-control.jsonl"),
        )?;
        Ok(())
    }
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn complete(recording: Recording, journal: Journal, result: Result<&str>) -> Result<Summary> {
    let result = match (result, journal.finish()) {
        (Ok(status), Ok(())) => Ok(status),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(journal_error)) => {
            Err(error.context(format!("control journal failed: {journal_error:#}")))
        }
    };

    let summary = recording.finish(&result)?;
    result?;
    Ok(summary)
}

async fn exchange(
    socket: &UdpSocket,
    request: &[u8],
    journal: &mut Journal,
    timeout: Duration,
) -> Result<Vec<u8>> {
    let owner = Header::decode(request)?;
    let deadline = Instant::now().checked_add(timeout).context("control deadline overflow")?;
    let sent = timeout_at(deadline, socket.send(request))
        .await
        .context("source request submission deadline")??;
    if sent != request.len() {
        bail!("source request was not submitted completely");
    }
    journal.record("submitted", request);
    let mut buffer = vec![0; 65_536];
    loop {
        let length = timeout_at(deadline, socket.recv(&mut buffer))
            .await
            .context("source reply deadline; remote effect is uncertain")??;
        journal.record("received", &buffer[..length]);
        let Ok(reply) = Header::decode(&buffer[..length]) else {
            continue;
        };

        if owner.owns_reply(reply) {
            return Ok(buffer[..length].to_vec());
        }
    }
}

pub async fn run(
    session: Session,
    duration: Duration,
    timeout: Duration,
    output: PathBuf,
) -> Result<Summary> {
    let format = session.format()?;
    Instant::now().checked_add(timeout).context("control deadline overflow")?;
    Instant::now().checked_add(duration).context("recording deadline overflow")?;
    let socket = std::net::UdpSocket::bind((session.interface_address, 0))
        .context("reserve a fresh media receiver")?;
    let mut receiver = Intake::start(socket).await?;
    let destination = match receiver.local_address()? {
        SocketAddr::V4(address) => address,
        _ => bail!("receiver socket did not bind IPv4"),
    };

    let control = UdpSocket::bind((session.interface_address, 0))
        .await
        .context("reserve source-control ownership")?;
    control.connect(session.control_endpoint).await.context("select source-control peer")?;
    let request = source_control::allocate(
        &Allocation {
            receiver_hostname: &session.receiver_hostname,
            receiver_flow_name: &session.receiver_flow_name,
            destination,
            format,
            source_channel_identifiers: &session.source_channel_identifiers,
            requested_frames_per_packet: session.requested_frames_per_packet.get(),
        },
        1,
    )?;
    let mut recording = Recording::begin(&RecordingContext {
        source: None,
        destination,
        interface_address: session.interface_address,
        format,
        output: output.clone(),
    })?;
    recording.summary.source_control = Some(ControlSummary {
        control_endpoint: session.control_endpoint,
        reply_profile: session.reply_profile,
        source_channel_identifiers: session.source_channel_identifiers.clone(),
        requested_frames_per_packet: session.requested_frames_per_packet.get(),
        allocation_state: "prepared",
        allocation_outcome: None,
        stop_state: "not_attempted",
        stop_result: None,
        reverse_packets_sent: 0,
        source_admission: "explicit_source_address_and_first_valid_sender_port_pinned_to_reserved_receiver; no_authentication_or_universal_stale_packet_fence",
        reverse_schedule: "independent_receive_driven_sample_progress; first_valid_packet_then_floor_rate_over_four_ticks; no_network_clock_readiness_claim",
        journal_failure: None,
    });
    recording.manifest()?;
    let mut journal = Journal::create(&output)?;
    let mut session_file =
        OpenOptions::new().write(true).create_new(true).open(output.join("session.json"))?;
    serde_json::to_writer_pretty(&mut session_file, &session)?;
    session_file.write_all(b"\n")?;
    session_file.sync_all()?;
    drop(session_file);
    let interruption = tokio::signal::ctrl_c();
    tokio::pin!(interruption);
    tokio::select! {
        biased;
        signal = &mut interruption => {
            signal?;
            return complete(recording, journal, Ok("interrupted"));
        }
        _ = std::future::ready(()) => {}
    }

    if !journal.record("allocation_prepared", &request) {
        let state = recording
            .summary
            .source_control
            .as_mut()
            .context("source-control state unavailable")?;
        state.allocation_state = "not_submitted";
        state.journal_failure.clone_from(&journal.failure);
        return complete(
            recording,
            journal,
            Err(anyhow::anyhow!("allocation was not sent because its journal could not be saved")),
        );
    }

    let allocation = exchange(&control, &request, &mut journal, timeout).await.and_then(|reply| {
        source_control::allocation_reply(&reply, session.reply_profile).map_err(anyhow::Error::from)
    });
    let outcome = match allocation {
        Ok(outcome) => outcome,
        Err(error) => {
            let state = recording
                .summary
                .source_control
                .as_mut()
                .context("source-control state unavailable")?;
            state.allocation_state = "uncertain";
            state.journal_failure.clone_from(&journal.failure);
            return complete(recording, journal, Err(error));
        }
    };

    let handle = outcome.handle;
    let interface_usable =
        outcome.interface_outcomes.as_ref().is_none_or(|outcomes| outcomes.first() == Some(&1));
    {
        let state = recording
            .summary
            .source_control
            .as_mut()
            .context("source-control state unavailable")?;
        state.allocation_state = if handle.is_some() {
            "qualified_handle_observed"
        } else {
            "rejected_without_handle"
        };
        state.allocation_outcome = Some(outcome);
    }

    let Some(handle) = handle else {
        return complete(
            recording,
            journal,
            Err(anyhow::anyhow!("source rejected allocation; no owned handle was acquired")),
        );
    };

    let result = async {
        recording.manifest()?;
        if journal.failure.is_some() {
            bail!("allocation journal failed; retiring the acquired handle");
        }

        if !interface_usable {
            bail!("source did not report the requested receiver interface ordinal usable");
        }

        let deadline =
            Instant::now().checked_add(duration).context("recording deadline overflow")?;
        collect(&mut receiver, &session, format, &mut recording, deadline, interruption.as_mut())
            .await
    }
    .await;
    let stop_request = source_control::stop(handle, 2)?;
    journal.record("stop_prepared", &stop_request);
    let stopped = exchange(&control, &stop_request, &mut journal, timeout)
        .await
        .and_then(|reply| source_control::stop_reply(&reply).map_err(anyhow::Error::from));
    {
        let state = recording
            .summary
            .source_control
            .as_mut()
            .context("source-control state unavailable")?;
        match &stopped {
            Ok(reply) => {
                state.stop_result = Some(reply.result);
                state.stop_state = if reply.result == 1 {
                    "acknowledged; no_physical_drain_proof"
                } else {
                    "source_reported_failure"
                };
            }

            Err(_) => state.stop_state = "uncertain; no_retry",
        }
        state.journal_failure.clone_from(&journal.failure);
    }
    let intake_closed = receiver.close().await;
    drop(control);
    let result = match (result, stopped) {
        (Ok(status), Ok(reply)) if reply.result == 1 && journal.failure.is_none() => Ok(status),
        (Err(error), Err(stop_error)) => Err(error.context(format!(
            "owned STOP was attempted once and remains uncertain: {stop_error:#}"
        ))),
        (Err(error), _) => Err(error),
        (_, Err(error)) => {
            Err(error.context("owned STOP was attempted once; remote release remains uncertain"))
        }
        (_, Ok(reply)) => Err(anyhow::anyhow!(
            "owned STOP returned result 0x{:04x}; journal failure: {:?}",
            reply.result,
            journal.failure
        )),
    };
    let result = match (result, intake_closed) {
        (Ok(status), Ok(())) => Ok(status),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(intake_error)) => {
            Err(error.context(format!("recording intake also failed: {intake_error:#}")))
        }
    };
    complete(recording, journal, result)
}

async fn collect(
    receiver: &mut Intake,
    session: &Session,
    format: Format,
    recording: &mut Recording,
    deadline: Instant,
    mut interruption: Pin<&mut impl Future<Output = std::io::Result<()>>>,
) -> Result<&'static str> {
    let expiration = tokio::time::sleep_until(deadline);
    tokio::pin!(expiration);
    let mut source = None;
    let mut next_reverse_tick = None;
    let mut previous_next_tick = None;

    loop {
        tokio::select! {
            biased;
            _ = &mut expiration => return Ok("complete"),
            signal = &mut interruption => {
                signal?;
                return Ok("interrupted");
            }
            result = receiver.receive() => {
                let datagram = result.context("receive owned unicast media")?;
                let SocketAddr::V4(peer) = datagram.peer else {
                    continue;
                };

                if *peer.ip() != session.source_address
                    || source.is_some_and(|current| current != peer)
                {
                    recording.summary.other_source_packets += 1;
                    continue;
                }

                if datagram.bytes.len() > usize::from(session.maximum_media_datagram_bytes.get()) {
                    recording.reject(
                        &datagram.bytes,
                        &datagram.received_at,
                        peer,
                        "observed media exceeds the established datagram budget",
                    )?;
                    bail!("observed media exceeds the established datagram budget");
                }

                let interval = recording.admit(
                    &datagram.bytes,
                    &datagram.received_at,
                    peer,
                    format,
                )?;
                let Some(interval) = interval else {
                    continue;
                };

                if source.is_none() {
                    source = Some(peer);
                    recording.pin_source(peer);
                    recording.manifest()?;
                }

                if previous_next_tick.is_some_and(|expected| expected != interval.first_tick) {
                    next_reverse_tick = None;
                }

                if next_reverse_tick.is_none_or(|next| interval.first_tick >= next) {
                    let sent = timeout_at(deadline, receiver.send(REVERSE_KEEPALIVE, peer)).await
                        .context("reverse activity deadline")??;
                    if sent != REVERSE_KEEPALIVE.len() {
                        bail!("reverse activity was not submitted completely");
                    }

                    let state = recording
                        .summary
                        .source_control
                        .as_mut()
                        .context("source-control state unavailable")?;
                    state.reverse_packets_sent += 1;
                    next_reverse_tick = Some(
                        interval.first_tick
                            .checked_add(u64::from((format.sample_rate() / 4).max(1)))
                            .context("reverse sample-progress deadline overflow")?,
                    );
                }

                previous_next_tick = Some(interval.next_tick);
            }
        }
    }
}
