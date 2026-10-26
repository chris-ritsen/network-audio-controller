pub mod capture;
pub(super) mod configuration;
mod encoder;

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use netaudio_core::video::{RECEIVE_RETURN, h264, rtp};
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use super::parameters::{Format, Parameters};
use super::recording::write_json;
use configuration::{
    BASELINE_PROFILE, Configuration, FRAME_BYTES, FRAME_RATE, HEIGHT, RTP_CLOCK_RATE, WIDTH,
    frame_timestamp,
};
use encoder::{Encoder, Output};

const MAXIMUM_REFERENCE_FRAMES: u32 = 16;
const RETURN_BUFFER_BYTES: usize = 65_536;

pub fn session_schema() -> schemars::Schema {
    schemars::schema_for!(Configuration)
}

#[derive(Default, Serialize)]
pub struct Summary {
    owner: String,
    generation: String,
    state: String,
    stop_reason: String,
    prepare_only: bool,
    reads_started: u64,
    reads_failed: u64,
    reads_cancelled: u64,
    frames_read: u64,
    provider_submissions: u64,
    provider_rejections: u64,
    frames_encoded: u64,
    frames_sent: u64,
    datagrams_sent: u64,
    bytes_sent: u64,
    pending_frames_cancelled: u64,
    maximum_send_lateness_nanoseconds: u64,
    valid_returns: u64,
    rejected_returns: u64,
    last_return_nanoseconds: Option<i64>,
    failure: Option<String>,
    retirement_failure: Option<String>,
    remote_decode_verified: bool,
}

struct Source {
    input: SourceInput,
    frames: u64,
    assembly: h264::Receiver,
    next_sequence: u16,
    parameters: Option<Parameters>,
    directory: PathBuf,
    frame_log: File,
    read_task: Option<tokio::task::JoinHandle<Result<Vec<u8>>>>,
    description: Option<Description>,
    peer: Option<Peer>,
}

pub enum SourceInput {
    RawFile(Arc<File>),
    Capture(capture::Capture),
}

pub struct Peer {
    pub prepared: oneshot::Sender<Description>,
    pub ready: oneshot::Receiver<()>,
}

pub struct Session {
    pub configuration: Configuration,
    pub input: SourceInput,
    pub frames: u64,
    pub directory: PathBuf,
    pub duration: Duration,
    pub prepare_only: bool,
    pub socket: Option<UdpSocket>,
    pub peer: Option<Peer>,
    pub cancellation: Option<watch::Receiver<bool>>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Description {
    pub owner: String,
    pub generation: String,
    pub provider: String,
    pub gstreamer_version: String,
    pub width: u32,
    pub height: u32,
    pub cadence_numerator: u64,
    pub cadence_denominator: u64,
    pub colorimetry: String,
    pub profile_identifier: u8,
    pub level_identifier: u8,
    pub sequence_parameters_hex: String,
    pub picture_parameters_hex: String,
    pub source: std::net::SocketAddrV4,
    pub destination: std::net::SocketAddrV4,
    pub payload_type: u8,
    pub synchronization_source: u32,
    pub rtp_clock_rate: u32,
}

pub async fn run(
    configuration_path: &Path,
    input: &Path,
    directory: PathBuf,
    duration: Duration,
    prepare_only: bool,
) -> Result<Summary> {
    gstreamer::init().context("initialize source media types")?;
    let configuration = Configuration::read(configuration_path)?;
    let (input, frames) = SourceInput::open(input, &configuration)?;
    let socket = if prepare_only {
        None
    } else {
        Some(bind(&configuration)?)
    };
    run_session(Session {
        configuration,
        input,
        frames,
        directory,
        duration,
        prepare_only,
        socket,
        peer: None,
        cancellation: None,
    })
    .await
}

pub async fn run_session(session: Session) -> Result<Summary> {
    let Session {
        configuration,
        input,
        frames,
        directory,
        duration,
        prepare_only,
        socket,
        peer,
        mut cancellation,
    } = session;
    configuration.validate()?;
    let mut source = Source::new(input, frames, directory, &configuration)?;
    source.peer = peer;
    let origin = Instant::now();
    let deadline = origin
        .checked_add(duration.min(configuration.authority_remaining()?))
        .context("source deadline overflow")?;
    let generation = uuid::Uuid::new_v4().to_string();
    let mut summary = Summary {
        owner: configuration.owner.clone(),
        generation,
        state: "configured".to_owned(),
        prepare_only,
        ..Summary::default()
    };
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut termination =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let (encoder, mut output) = match Encoder::start(&configuration).await {
        Ok(started) => started,
        Err(error) => {
            summary.state = "setup_failed".to_owned();
            summary.failure = Some(format!("{error:#}"));
            write_json(&source.directory.join("manifest.json"), &summary)?;
            return Err(error);
        }
    };
    let mut return_bytes = vec![0; RETURN_BUFFER_BYTES];
    let mut valid_returns = 0;
    let mut rejected_returns = 0;
    let mut last_return_nanoseconds = None;
    let mut return_deadline = deadline;
    let mut provider_failure = output.failure.clone();
    let collected = {
        let work = source.send(
            &configuration,
            &encoder,
            &mut output,
            socket.as_ref(),
            &mut summary,
            origin,
        );
        tokio::pin!(work);

        loop {
            tokio::select! {
                biased;
                _ = super::cancelled(&mut cancellation) => break Ok("peer_closed"),
                _ = interrupt.recv() => break Ok("interrupted"),
                _ = termination.recv() => break Ok("interrupted"),
                _ = tokio::time::sleep_until(deadline) => break Ok("duration_or_authority_expired"),
                _ = tokio::time::sleep_until(return_deadline), if valid_returns > 0 => break Err(anyhow::anyhow!("armed receiver-return deadline expired")),
                changed = provider_failure.changed() => {
                    match changed {
                        Ok(()) => {
                            if let Some(failure) = provider_failure.borrow().as_ref() {
                                break Err(anyhow::anyhow!("encoder failed: {failure}"));
                            }
                        }
                        Err(error) => break Err(error.into()),
                    }
                }
                result = &mut work => break result,
                received = receive_return(socket.as_ref(), &mut return_bytes) => {
                    match received {
                        Ok((length, peer)) if peer == SocketAddr::V4(configuration.destination) && &return_bytes[..length] == RECEIVE_RETURN => {
                            valid_returns += 1;
                            match super::clock_sample(origin) {
                                Ok(sample) => last_return_nanoseconds = Some(sample),
                                Err(error) => break Err(error),
                            }

                            match Instant::now().checked_add(Duration::from_millis(u64::from(configuration.limits.return_timeout_milliseconds))) {
                                Some(deadline) => return_deadline = deadline,
                                None => break Err(anyhow::anyhow!("return deadline overflow")),
                            }
                        }
                        Ok(_) => rejected_returns += 1,
                        Err(error) => break Err(error),
                    }
                }
            }
        }
    };

    summary.state = "stopping".to_owned();
    summary.valid_returns = valid_returns;
    summary.rejected_returns = rejected_returns;
    summary.last_return_nanoseconds = last_return_nanoseconds;
    summary.reads_cancelled =
        summary.reads_started.saturating_sub(summary.frames_read + summary.reads_failed);
    summary.pending_frames_cancelled =
        summary.provider_submissions.saturating_sub(if prepare_only {
            summary.frames_encoded
        } else {
            summary.frames_sent
        });
    source.assembly.invalidate();
    drop(socket);
    output.datagrams.close();
    let (encoder_retirement, read_retirement) = tokio::join!(
        encoder.stop(&configuration),
        source.stop_read(configuration.operation_timeout()),
    );
    let retirement_errors: Vec<_> = [encoder_retirement, read_retirement]
        .into_iter()
        .filter_map(|result| result.err().map(|error| format!("{error:#}")))
        .collect();
    let retired = if retirement_errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(retirement_errors.join("; ")))
    };
    let capture_retirement = match source.input {
        SourceInput::Capture(capture) => capture.stop(configuration.operation_timeout()).await,
        SourceInput::RawFile(_) => Ok(()),
    };
    let retired = retired.and(capture_retirement);
    summary.retirement_failure = retired.as_ref().err().map(|error| format!("{error:#}"));
    summary.state = if retired.is_ok() {
        "stopped"
    } else {
        "quiescence_unknown"
    }
    .to_owned();
    summary.stop_reason = collected.as_ref().copied().unwrap_or("failed").to_owned();
    let result = collected.map(|_| ()).and(retired).and_then(|_| {
        ensure!(summary.frames_encoded > 0, "no complete source frames were encoded");
        Ok(())
    });
    summary.failure = result.as_ref().err().map(|error| format!("{error:#}"));
    write_json(&source.directory.join("manifest.json"), &summary)?;
    result?;
    Ok(summary)
}

impl SourceInput {
    fn open(input: &Path, configuration: &Configuration) -> Result<(Self, u64)> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(input)
            .context("open finite BGRA source")?;
        let metadata = file.metadata()?;
        ensure!(metadata.is_file(), "source input must be a regular finite file");
        ensure!(
            metadata.len() > 0 && metadata.len() % FRAME_BYTES as u64 == 0,
            "source file must contain complete tightly packed 1920x1080 BGRA frames"
        );
        let frames = metadata.len() / FRAME_BYTES as u64;
        ensure!(
            frames <= configuration.limits.maximum_frames,
            "source exceeds the configured frame limit"
        );
        Ok((Self::RawFile(Arc::new(file)), frames))
    }
}

impl Source {
    fn new(
        input: SourceInput,
        frames: u64,
        directory: PathBuf,
        configuration: &Configuration,
    ) -> Result<Self> {
        std::fs::create_dir(&directory).context("create new source output directory")?;
        write_json(&directory.join("configuration.json"), configuration)?;
        let frame_log =
            OpenOptions::new().create_new(true).write(true).open(directory.join("frames.jsonl"))?;
        let assembly = h264::Receiver::new(h264::Limits {
            maximum_access_unit_bytes: configuration.limits.maximum_encoded_frame_bytes,
            maximum_packets: configuration.limits.maximum_packets_per_frame,
            maximum_age_nanoseconds: i64::from(configuration.limits.operation_timeout_milliseconds)
                * netaudio_core::video::NANOSECONDS_PER_MILLISECOND,
        })?;
        Ok(Self {
            input,
            frames,
            assembly,
            next_sequence: configuration.initial_sequence,
            parameters: None,
            directory,
            frame_log,
            read_task: None,
            description: None,
            peer: None,
        })
    }

    async fn send(
        &mut self,
        configuration: &Configuration,
        encoder: &Encoder,
        output: &mut Output,
        socket: Option<&UdpSocket>,
        summary: &mut Summary,
        origin: Instant,
    ) -> Result<&'static str> {
        let mut peer = self.peer.take();
        let mut pacing_origin = None;

        let mut previous_index = 0_u64;
        let live = matches!(self.input, SourceInput::Capture(_));

        for ordinal in 0..self.frames {
            let operation = async {
                summary.reads_started += 1;
                let bytes = match &mut self.input {
                    SourceInput::RawFile(input) => {
                        let input = input.clone();
                        let offset = ordinal
                            .checked_mul(FRAME_BYTES as u64)
                            .context("source byte offset overflow")?;
                        self.read_task = Some(tokio::task::spawn_blocking(move || {
                            let mut bytes = vec![0; FRAME_BYTES];
                            input
                                .read_exact_at(&mut bytes, offset)
                                .context("read complete BGRA frame")?;
                            Ok::<_, anyhow::Error>(bytes)
                        }));
                        let result =
                            self.read_task.as_mut().context("source read was not started")?.await;
                        self.read_task = None;
                        match result.context("source read task failed").and_then(|result| result) {
                            Ok(bytes) => bytes,
                            Err(error) => {
                                summary.reads_failed += 1;
                                return Err(error);
                            }
                        }
                    }
                    SourceInput::Capture(capture) => match capture.next().await? {
                        Some(bytes) => bytes,
                        None => return Ok(None),
                    },
                };
                let index = if live && ordinal > 0 {
                    let elapsed =
                        pacing_origin.map(|start: Instant| start.elapsed()).unwrap_or_default();
                    previous_index.saturating_add(1).max(u64::try_from(
                        elapsed.as_nanos() * u128::from(FRAME_RATE)
                            / u128::from(configuration::NANOSECONDS_PER_SECOND),
                    )?)
                } else {
                    ordinal
                };
                previous_index = index;
                summary.frames_read += 1;

                if let Err(error) = encoder.submit(bytes, index) {
                    summary.provider_rejections += 1;
                    return Err(error);
                }
                summary.provider_submissions += 1;
                let (datagrams, access_unit) =
                    self.collect(configuration, output, index, origin).await?;
                Ok(Some((datagrams, access_unit, index)))
            };
            let encoded = tokio::time::timeout(configuration.operation_timeout(), operation)
                .await
                .context("source read/encode deadline expired")??;
            let Some((datagrams, access_unit, index)) = encoded else {
                return Ok("input_complete");
            };
            encoder.verify_input(configuration)?;
            self.validate_parameters(configuration, &access_unit, summary)?;
            summary.frames_encoded += 1;

            if summary.prepare_only {
                return Ok("prepared_without_transmission");
            }

            if let Some(peer) = peer.take() {
                peer.prepared
                    .send(self.description.clone().context("source description is unavailable")?)
                    .map_err(|_| anyhow::anyhow!("peer setup closed before source preparation"))?;
                peer.ready.await.context("receiver did not become ready")?;
            }

            let socket = socket.context("transmission has no owned source socket")?;
            let start = *pacing_origin.get_or_insert_with(Instant::now);
            let due = start
                .checked_add(Duration::from_nanos(frame_timestamp(index)?))
                .context("source pacing deadline overflow")?;
            if !live {
                tokio::time::sleep_until(due).await;
            }
            let lateness = Instant::now().saturating_duration_since(due);
            ensure!(
                lateness
                    <= Duration::from_millis(u64::from(
                        configuration.limits.allowed_lateness_milliseconds
                    )),
                "source missed its configured send deadline"
            );
            summary.maximum_send_lateness_nanoseconds =
                summary.maximum_send_lateness_nanoseconds.max(u64::try_from(lateness.as_nanos())?);
            summary.state = "active".to_owned();

            if index == 0 {
                write_json(&self.directory.join("active.json"), summary)?;
            }

            let sent_at = super::clock_sample(origin)?;

            for bytes in &datagrams {
                let length = socket
                    .send_to(bytes, configuration.destination)
                    .await
                    .context("send complete H.264 datagram")?;
                ensure!(length == bytes.len(), "UDP send did not accept the complete datagram");
                summary.datagrams_sent += 1;
                summary.bytes_sent += length as u64;
            }

            summary.frames_sent += 1;
            if live {
                continue;
            }

            serde_json::to_writer(
                &mut self.frame_log,
                &serde_json::json!({
                    "owner": summary.owner,
                    "generation": summary.generation,
                    "frame_index": index,
                    "provider_timestamp_nanoseconds": frame_timestamp(index)?,
                    "rtp_timestamp": access_unit.timestamp,
                    "random_access": access_unit.random_access,
                    "datagrams": datagrams.len(),
                    "encoded_bytes": access_unit.bytes.len(),
                    "send_started_nanoseconds": sent_at,
                    "send_completed_nanoseconds": super::clock_sample(origin)?,
                    "send_lateness_nanoseconds": lateness.as_nanos(),
                }),
            )?;
            self.frame_log.write_all(b"\n")?;
            self.frame_log.flush()?;
        }

        Ok("input_complete")
    }

    async fn stop_read(&mut self, timeout: Duration) -> Result<()> {
        if let Some(task) = self.read_task.take() {
            let _cancelled_read = tokio::time::timeout(timeout, task)
                .await
                .context("source read quiescence is unknown: cancellation deadline expired")?
                .context("source read task failed during cancellation")?;
        }

        Ok(())
    }

    async fn collect(
        &mut self,
        configuration: &Configuration,
        output: &mut Output,
        index: u64,
        origin: Instant,
    ) -> Result<(Vec<Vec<u8>>, h264::AccessUnit)> {
        let presentation = frame_timestamp(index)?;
        let timestamp = configuration
            .initial_timestamp
            .wrapping_add(index.wrapping_mul(u64::from(RTP_CLOCK_RATE) / FRAME_RATE) as u32);
        let mut datagrams = Vec::new();
        let mut total_bytes = 0_usize;

        loop {
            if let Some(failure) = output.failure.borrow().as_ref() {
                bail!("encoder failed: {failure}");
            }

            let datagram = tokio::select! {
                biased;
                changed = output.failure.changed() => {
                    changed.context("encoder failure channel closed")?;
                    continue;
                }
                datagram = output.datagrams.recv() => datagram.context("encoder output channel closed")?,
            };
            ensure!(
                datagram.presentation_nanoseconds == presentation,
                "packetizer returned a different source frame identity"
            );
            ensure!(
                datagrams.len() < configuration.limits.maximum_packets_per_frame,
                "encoded frame exceeds packet limit"
            );
            total_bytes = total_bytes
                .checked_add(datagram.bytes.len())
                .context("encoded frame byte count overflow")?;
            ensure!(
                total_bytes <= configuration.limits.maximum_encoded_frame_bytes,
                "encoded frame datagrams exceed the byte limit"
            );
            let packet = rtp::Packet::parse(
                &datagram.bytes,
                configuration.payload_type,
                configuration.synchronization_source,
            )?;
            ensure!(
                packet.sequence == self.next_sequence && packet.timestamp == timestamp,
                "packetizer sequence/timestamp differs from configured source policy"
            );
            self.next_sequence = self.next_sequence.wrapping_add(1);
            let complete = self.assembly.push(packet, super::clock_sample(origin)?)?;
            datagrams.push(datagram.bytes);

            if let Some(access_unit) = complete {
                return Ok((datagrams, access_unit));
            }
        }
    }

    fn validate_parameters(
        &mut self,
        configuration: &Configuration,
        access_unit: &h264::AccessUnit,
        summary: &Summary,
    ) -> Result<()> {
        if self.parameters.is_none() {
            let find = |kind| -> Result<Vec<u8>> {
                let units: Vec<_> = access_unit
                    .nal_ranges
                    .iter()
                    .map(|range| &access_unit.bytes[range.clone()])
                    .filter(|bytes| bytes[0] & h264::NAL_TYPE_MASK == kind)
                    .collect();
                let first = units
                    .first()
                    .context("encoder did not emit required initial parameter sets")?;
                ensure!(
                    units.iter().all(|unit| unit == first),
                    "encoder emitted conflicting parameter sets"
                );
                Ok(first.to_vec())
            };
            let sequence = find(h264::SEQUENCE_PARAMETERS)?;
            let picture = find(h264::PICTURE_PARAMETERS)?;
            let parameters = Parameters::from_bytes(
                sequence.clone(),
                picture.clone(),
                Format {
                    width: WIDTH,
                    height: HEIGHT,
                    profile_identifier: BASELINE_PROFILE,
                    level_identifier: configuration.encoder.level_identifier,
                    maximum_reference_frames: MAXIMUM_REFERENCE_FRAMES,
                },
            )?;
            parameters.validate(access_unit)?;
            let description = Description {
                owner: summary.owner.clone(),
                generation: summary.generation.clone(),
                provider: "openh264enc/h264parse/rtph264pay".to_owned(),
                gstreamer_version: gstreamer::version_string().to_string(),
                width: WIDTH,
                height: HEIGHT,
                cadence_numerator: FRAME_RATE,
                cadence_denominator: 1,
                colorimetry: configuration.encoder.colorimetry.clone(),
                profile_identifier: BASELINE_PROFILE,
                level_identifier: configuration.encoder.level_identifier,
                sequence_parameters_hex: hexadecimal(&sequence),
                picture_parameters_hex: hexadecimal(&picture),
                source: configuration.source,
                destination: configuration.destination,
                payload_type: configuration.payload_type,
                synchronization_source: configuration.synchronization_source,
                rtp_clock_rate: configuration.rtp_clock_rate,
            };
            write_json(&self.directory.join("parameters.json"), &description)?;
            self.description = Some(description);
            self.parameters = Some(parameters);
        }

        self.parameters.as_ref().context("source has no accepted parameters")?.validate(access_unit)
    }
}

fn bind(configuration: &Configuration) -> Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket
        .bind_device(Some(configuration.interface.as_bytes()))
        .context("bind source socket to selected interface")?;
    socket
        .bind(&SocketAddr::V4(configuration.source).into())
        .context("bind exact source media endpoint")?;
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket.into()).context("register source media socket")
}

async fn receive_return(
    socket: Option<&UdpSocket>,
    bytes: &mut [u8],
) -> Result<(usize, SocketAddr)> {
    match socket {
        Some(socket) => socket.recv_from(bytes).await.context("receive source return"),
        None => std::future::pending().await,
    }
}

fn hexadecimal(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }

    output
}
