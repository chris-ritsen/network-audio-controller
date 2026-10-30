mod configuration;
pub mod live;
pub mod owned_multicast;
mod parameters;
pub mod presentation;
mod provider;
mod recording;
pub mod transmit;

use std::collections::BTreeMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use netaudio_core::video::{RECEIVE_RETURN, ReceiveReturn, h264, local_deadline, rtp};
use serde::Serialize;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use configuration::{Configuration, LatePolicy};
use parameters::Parameters;
use provider::Decoder;
use recording::{Client as RecordingClient, Supervisor as RecordingSupervisor};

const NANOSECONDS_PER_SECOND: u64 = 1_000_000_000;
const RECEIVE_BUFFER_BYTES: usize = 65_536;

pub fn session_schema() -> schemars::Schema {
    schemars::schema_for!(Configuration)
}

#[derive(Clone, Default, Serialize)]
pub struct Summary {
    owner: String,
    generation: String,
    monotonic_clock: String,
    state: String,
    stop_reason: String,
    datagrams_received: u64,
    packets_admitted: u64,
    other_source_packets: u64,
    rejected: BTreeMap<String, u64>,
    assemblies_completed: u64,
    provider_submissions: u64,
    decoded_frames: u64,
    published_frames: usize,
    selected_frames: u64,
    recording_queued_frames: u64,
    recording_payload_written_frames: u64,
    recording_index_written_frames: u64,
    recording_drained_frames: u64,
    recording_drained: bool,
    recording_ingress_rejection: Option<recording::RecordingIngressRejection>,
    late_frames_dropped: u64,
    cancelled_frames: usize,
    returns_attempted: u64,
    returns_sent: u64,
    return_failures: u64,
    failure: Option<String>,
    retirement_failure: Option<String>,
    last_rejection: Option<String>,
    physical_presentation_verified: bool,
}

impl Summary {
    pub(crate) fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    pub(crate) fn selected_frames(&self) -> u64 {
        self.selected_frames
    }

    pub(crate) fn apply_recording_status(&mut self, status: &recording::Status) {
        self.recording_queued_frames = status.queued;
        self.recording_payload_written_frames = status.payload_written;
        self.recording_index_written_frames = status.index_written;
        self.recording_drained_frames = status.drained_frames;
        self.recording_drained = status.drained;
    }

    pub(crate) fn recording_ingress_rejection(
        &self,
    ) -> Option<&recording::RecordingIngressRejection> {
        self.recording_ingress_rejection.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Observation {
    pub configured: bool,
    pub socket_owned: bool,
    pub multicast_joined: bool,
    pub decoder_ready: bool,
    pub admitted_packet: bool,
    pub decoded_frame: bool,
    pub output_queued: bool,
    pub terminal: Option<Terminal>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Terminal {
    Retired,
    QuiescenceUnknown,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FrameIdentity {
    pub(crate) owner: String,
    pub(crate) generation: String,
    pub(crate) monotonic_clock: String,
    pub(crate) access_unit: u64,
    pub(crate) source: std::net::SocketAddrV4,
    pub(crate) payload_type: u8,
    pub(crate) synchronization_source: u32,
    pub(crate) rtp_timestamp: u32,
    pub(crate) arrival_nanoseconds: i64,
    pub(crate) deadline_nanoseconds: i64,
    pub(crate) random_access: bool,
}

struct Pending {
    identity: FrameIdentity,
    expires: Instant,
}

struct Ready {
    identity: FrameIdentity,
    bytes: Vec<u8>,
}

pub struct Session {
    pub configuration: Configuration,
    pub output: PathBuf,
    pub duration: Duration,
    pub socket: ReceiverSocket,
    pub presentation: presentation::Options,
    pub record_frames: bool,
    pub ready: Option<oneshot::Sender<()>>,
    pub cancellation: Option<watch::Receiver<bool>>,
    pub idle_timeout: Option<Duration>,
    pub observation: Option<watch::Sender<Observation>>,
}

pub(crate) struct ReceiverSocket {
    socket: UdpSocket,
    destination: SocketAddrV4,
    multicast_membership: Option<(std::net::Ipv4Addr, std::net::Ipv4Addr)>,
}

impl ReceiverSocket {
    pub(crate) fn unicast(socket: UdpSocket) -> Self {
        let destination = match socket.local_addr() {
            Ok(SocketAddr::V4(address)) => address,
            _ => SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0),
        };
        Self {
            socket,
            destination,
            multicast_membership: None,
        }
    }
}

pub async fn cancelled(receiver: &mut Option<watch::Receiver<bool>>) {
    match receiver {
        Some(receiver) => {
            let _ = receiver.wait_for(|cancelled| *cancelled).await;
        }
        None => std::future::pending::<()>().await,
    }
}

pub async fn run(
    configuration_path: &Path,
    output: PathBuf,
    duration: Duration,
) -> Result<Summary> {
    let configuration = Configuration::read(configuration_path)?;
    let socket = bind(&configuration)?;
    run_session(Session {
        configuration,
        output,
        duration,
        socket,
        presentation: presentation::Options::default(),
        record_frames: true,
        ready: None,
        cancellation: None,
        idle_timeout: None,
        observation: None,
    })
    .await
}

pub(crate) async fn run_session(session: Session) -> Result<Summary> {
    let origin = Instant::now();
    let generation = uuid::Uuid::new_v4().to_string();
    let (mut supervisor, recording) = RecordingSupervisor::start(
        session.output.clone(),
        session.configuration.clone(),
        session.record_frames,
        origin,
    )?;
    let result = run_session_with_recording(
        session,
        recording::Session {
            client: recording,
            origin,
            generation,
        },
    )
    .await;
    let retired = supervisor.terminal().await;
    match (result, retired) {
        (Ok(mut summary), Ok(terminal)) => {
            summary.apply_recording_status(&terminal.status);
            Ok(summary)
        }
        (Err(error), Ok(_)) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(retirement)) => {
            Err(error.context(format!("recording retirement also failed: {retirement:#}")))
        }
    }
}

pub(crate) async fn run_session_with_recording(
    session: Session,
    recording: recording::Session,
) -> Result<Summary> {
    let recording::Session {
        client: mut recording,
        origin,
        generation,
    } = recording;
    let Session {
        configuration,
        output: _,
        duration,
        socket,
        presentation: options,
        record_frames,
        ready: ready_signal,
        mut cancellation,
        idle_timeout,
        mut observation,
    } = session;
    configuration.validate()?;
    observe(&mut observation, |value| value.configured = true);
    observe(&mut observation, |value| value.socket_owned = true);
    ensure!(
        socket.destination == configuration.destination,
        "owned receiver socket does not match the configured destination"
    );
    let expected_membership = configuration
        .destination
        .ip()
        .is_multicast()
        .then_some((*configuration.destination.ip(), configuration.interface_address));
    ensure!(
        socket.multicast_membership == expected_membership,
        "owned receiver socket membership does not match the configured group and interface"
    );
    if socket.multicast_membership.is_some() {
        observe(&mut observation, |value| value.multicast_joined = true);
    }
    let socket = socket.socket;
    let parameters = Parameters::new(&configuration)?;
    let session_started = Instant::now();
    let authority_deadline = session_started
        .checked_add(configuration.authority_remaining()?)
        .context("authority deadline overflow")?;
    let run_deadline =
        session_started.checked_add(duration).context("recording deadline overflow")?;
    let mut summary = Summary {
        owner: configuration.owner.clone(),
        monotonic_clock: format!("process-monotonic:{generation}"),
        generation,
        state: "configured".to_owned(),
        ..Summary::default()
    };
    let mut receiver = h264::Receiver::new(h264::Limits {
        maximum_access_unit_bytes: configuration.limits.maximum_access_unit_bytes,
        maximum_packets: configuration.limits.maximum_packets_per_access_unit,
        maximum_age_nanoseconds: i64::from(configuration.limits.maximum_assembly_milliseconds)
            * netaudio_core::video::NANOSECONDS_PER_MILLISECOND,
    })?;
    let mut pending = BTreeMap::<u64, Pending>::new();
    let mut ready = BTreeMap::<(i64, u64), Ready>::new();
    let mut receive_return = ReceiveReturn::default();
    let mut bytes = vec![0; RECEIVE_BUFFER_BYTES];
    let frame_duration = NANOSECONDS_PER_SECOND
        .checked_mul(u64::from(configuration.cadence_denominator))
        .context("provider frame duration overflow")?
        / u64::from(configuration.cadence_numerator);
    ensure!(frame_duration > 0, "cadence exceeds nanosecond precision");
    let provider_timeout =
        Duration::from_millis(u64::from(configuration.limits.provider_timeout_milliseconds));
    let interrupted = interruption();
    tokio::pin!(interrupted);
    let (decoder, mut decoder_output) = match Decoder::start(&configuration, provider_timeout).await
    {
        Ok(started) => started,
        Err(error) => {
            summary.state = "setup_failed".to_owned();
            summary.failure = Some(format!("{error:#}"));
            finish_recording(&mut recording, &summary)?;
            return Err(error);
        }
    };
    observe(&mut observation, |value| value.decoder_ready = true);

    let mut presentation = match presentation::Presentation::start(&options, &configuration).await {
        Ok(presentation) => presentation,
        Err(error) => {
            let retired = decoder.stop(provider_timeout).await;
            summary.failure = Some(format!("{error:#}"));
            summary.retirement_failure = retired.err().map(|error| format!("{error:#}"));
            summary.state = "setup_failed".to_owned();
            finish_recording(&mut recording, &summary)?;
            return Err(error);
        }
    };
    let collected: Result<&str> = async {
        if Instant::now() >= authority_deadline {
            return Ok("authority_expired");
        }

        if Instant::now() >= run_deadline {
            return Ok("duration_complete");
        }

        let mut idle_deadline = idle_timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        let mut recording_status = recording.status();
        summary.state = "active".to_owned();
        tokio::select! {
            activated = recording.activate(summary.clone()) => activated.context("activate recording artifact owner")?,
            _ = &mut interrupted => return Ok("interrupted"),
            _ = cancelled(&mut cancellation) => return Ok("cancelled"),
            _ = tokio::time::sleep_until(authority_deadline) => return Ok("authority_expired"),
            _ = tokio::time::sleep_until(run_deadline) => return Ok("duration_complete"),
        }

        if let Some(ready) = ready_signal {
            ready.send(()).map_err(|_| anyhow::anyhow!("peer setup closed before receiver became ready"))?;
        }

        loop {
            let assembly_expiration = receiver.expiration_nanoseconds().map(|value| instant_at(origin, value)).transpose()?;
            let job_expiration = pending.values().map(|job| job.expires).min();
            let publication_deadline = ready.first_key_value().map(|((deadline, _), _)| instant_at(origin, *deadline)).transpose()?;
            let deadline = [Some(authority_deadline), Some(run_deadline), assembly_expiration, job_expiration, publication_deadline, idle_deadline].into_iter().flatten().min().unwrap();

            tokio::select! {
                biased;
                _ = cancelled(&mut cancellation) => return Ok("peer_closed"),
                changed = presentation.closed.changed() => {
                    changed.context("display lifecycle channel closed")?;

                    if *presentation.closed.borrow() {
                        return Ok("display_closed");
                    }
                }
                changed = presentation.failure.changed() => {
                    changed.context("presentation error channel closed")?;

                    if let Some(failure) = presentation.failure.borrow().as_ref() {
                        bail!("presentation failed: {failure}");
                    }
                }
                result = &mut interrupted => {
                    result?;
                    return Ok("interrupted");
                }
                changed = decoder_output.failure.changed() => {
                    changed.context("decoder error channel closed")?;

                    if let Some(failure) = decoder_output.failure.borrow().as_ref() {
                        bail!("decoder failed: {failure}");
                    }
                }
                changed = recording_status.changed() => {
                    changed.context("recording writer status channel closed")?;
                    if let Some(failure) = recording_status.borrow().failure.as_ref() {
                        bail!("recording writer failed: {failure}");
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let now = Instant::now();

                    if now >= authority_deadline {
                        return Ok("authority_expired");
                    }

                    if now >= run_deadline {
                        return Ok("duration_complete");
                    }

                    if idle_deadline.is_some_and(|expiry| now >= expiry) {
                        bail!("video source stopped delivering media before the idle deadline");
                    }

                    if job_expiration.is_some_and(|expiry| now >= expiry) {
                        bail!("decoder did not complete an owned access unit before its deadline");
                    }

                    if assembly_expiration.is_some_and(|expiry| now >= expiry) {
                        receiver.invalidate();
                        reject(&mut summary, "H.264 assembly expired");
                    }

                    let now_nanoseconds = clock_sample(origin)?;

                    while ready.first_key_value().is_some_and(|((deadline, _), _)| *deadline <= now_nanoseconds) {
                        let (_, frame) = ready.pop_first().unwrap();
                        let last_allowed = frame
                            .identity
                            .deadline_nanoseconds
                            .checked_add(i64::try_from(configuration.playout.allowed_lateness_nanoseconds)?)
                            .context("late-frame deadline overflow")?;

                        if matches!(configuration.playout.late_policy, LatePolicy::Drop)
                            && now_nanoseconds > last_allowed
                        {
                            summary.late_frames_dropped += 1;
                            continue;
                        }

                        let selected_nanoseconds = clock_sample(origin)?;
                        if matches!(configuration.playout.late_policy, LatePolicy::Drop)
                            && selected_nanoseconds > last_allowed
                        {
                            summary.late_frames_dropped += 1;
                            continue;
                        }

                        summary.selected_frames = summary.selected_frames.checked_add(1).context("video selection count exhausted")?;
                        if presentation.has_sink() {
                            presentation.publish(frame.bytes.clone())?;
                            summary.published_frames = summary.published_frames.checked_add(1).context("video presentation count exhausted")?;
                            if summary.published_frames == 1 {
                                observe(&mut observation, |value| value.output_queued = true);
                            }
                        }
                        if record_frames {
                            let ordinal = summary.recording_queued_frames;
                            if let Err(rejection) = recording.enqueue(
                                ordinal,
                                recording::FrameDescriptor {
                                    identity: frame.identity,
                                    width: configuration.width,
                                    height: configuration.height,
                                    colorimetry: configuration.colorimetry.clone(),
                                    selected_nanoseconds,
                                },
                                frame.bytes,
                            ) {
                                summary.recording_ingress_rejection = Some((*rejection).clone());
                                return Err(anyhow::Error::new(*rejection));
                            }
                            summary.recording_queued_frames = summary.recording_queued_frames.checked_add(1).context("recording acceptance count exhausted")?;
                            if summary.recording_queued_frames == 1 {
                                observe(&mut observation, |value| value.output_queued = true);
                            }
                        }
                        if summary.selected_frames == u64::try_from(configuration.limits.maximum_recorded_frames)? {
                            return Ok("frame_limit_reached");
                        }
                    }
                }
                sample = decoder_output.samples.recv() => {
                    let sample = sample.context("decoder output channel closed")?;
                    let (identity_timestamp, frame) = provider::copy_frame(&sample, &configuration)?;
                    let job = pending
                        .remove(&identity_timestamp)
                        .context("decoder output has no matching owned submission")?;
                    ensure!(Instant::now() <= job.expires, "decoder output arrived after its operation deadline");
                    summary.decoded_frames += 1;
                    if summary.decoded_frames == 1 {
                        observe(&mut observation, |value| value.decoded_frame = true);
                    }
                    let key = (job.identity.deadline_nanoseconds, job.identity.access_unit);
                    ensure!(
                        ready
                            .insert(
                                key,
                                Ready {
                                    identity: job.identity,
                                    bytes: frame
                                }
                            )
                            .is_none(),
                        "duplicate decoded access-unit output"
                    );
                }
                received = socket.recv_from(&mut bytes) => {
                    let (length, peer) = received.context("receive configured H.264 datagram")?;
                    let arrival_nanoseconds = clock_sample(origin)?;
                    summary.datagrams_received += 1;

                    if peer != SocketAddr::V4(configuration.source) {
                        summary.other_source_packets += 1;
                        continue;
                    }

                    if length > configuration.limits.maximum_datagram_bytes {
                        receiver.invalidate();
                        reject(&mut summary, "datagram exceeds configured byte limit");
                        continue;
                    }

                    let packet = match rtp::Packet::parse(
                        &bytes[..length],
                        configuration.payload_type,
                        configuration.synchronization_source,
                    ) {
                        Ok(packet) => packet,
                        Err(error) => {
                            receiver.invalidate();
                            reject(&mut summary, error.0);
                            continue;
                        }
                    };
                    summary.packets_admitted += 1;
                    if summary.packets_admitted == 1 {
                        observe(&mut observation, |value| value.admitted_packet = true);
                    }
                    idle_deadline = idle_timeout.and_then(|timeout| Instant::now().checked_add(timeout));

                    if !configuration.destination.ip().is_multicast()
                        && receive_return.attempt_due(arrival_nanoseconds)?
                    {
                        summary.returns_attempted += 1;

                        match socket.try_send_to(RECEIVE_RETURN, peer) {
                            Ok(sent) if sent == RECEIVE_RETURN.len() => summary.returns_sent += 1,
                            _ => summary.return_failures += 1,
                        }
                    }

                    let access_unit = match receiver.push(packet, arrival_nanoseconds) {
                        Ok(Some(access_unit)) => access_unit,
                        Ok(None) => continue,
                        Err(error) => {
                            reject(&mut summary, error.0);
                            continue;
                        }
                    };

                    if let Err(error) = parameters.validate(&access_unit) {
                        receiver.invalidate();
                        reject(&mut summary, "access unit failed configured parameter or slice validation");
                        summary.last_rejection = Some(format!("{error:#}"));
                        continue;
                    }

                    summary.assemblies_completed += 1;
                    ensure!(
                        pending.len() + ready.len() < configuration.limits.maximum_pending_frames,
                        "video frame queue is full"
                    );
                    let identity_timestamp = access_unit
                        .identifier
                        .checked_mul(frame_duration)
                        .filter(|timestamp| *timestamp != u64::MAX)
                        .context("provider submission identity exhausted")?;
                    let deadline_nanoseconds =
                        local_deadline(access_unit.arrival_nanoseconds, configuration.playout.delay_milliseconds)?;
                    let identity = FrameIdentity {
                        owner: summary.owner.clone(),
                        generation: summary.generation.clone(),
                        monotonic_clock: summary.monotonic_clock.clone(),
                        access_unit: access_unit.identifier,
                        source: configuration.source,
                        payload_type: configuration.payload_type,
                        synchronization_source: configuration.synchronization_source,
                        rtp_timestamp: access_unit.timestamp,
                        arrival_nanoseconds: access_unit.arrival_nanoseconds,
                        deadline_nanoseconds,
                        random_access: access_unit.random_access,
                    };
                    let mut encoded = parameters.prefix();
                    encoded.extend_from_slice(&access_unit.bytes);
                    ensure!(
                        encoded.len() <= configuration.limits.maximum_access_unit_bytes,
                        "parameter-prefixed access unit exceeds provider byte limit"
                    );
                    decoder.submit(encoded, identity_timestamp, frame_duration)?;
                    pending.insert(
                        identity_timestamp,
                        Pending {
                            identity,
                            expires: Instant::now()
                                .checked_add(provider_timeout)
                                .context("provider deadline overflow")?,
                        },
                    );
                    summary.provider_submissions += 1;
                }
            }
        }
    }.await;

    summary.state = "stopping".to_owned();
    receiver.invalidate();
    drop(socket);
    summary.cancelled_frames = pending.len() + ready.len();
    pending.clear();
    ready.clear();
    decoder_output.samples.close();
    let (decoded, presented) =
        tokio::join!(decoder.stop(provider_timeout), presentation.stop(provider_timeout));
    let retired = decoded.and(presented);
    summary.retirement_failure = retired.as_ref().err().map(|error| format!("{error:#}"));
    drop(decoder_output);
    summary.state = if retired.is_ok() {
        observe(&mut observation, |value| value.terminal = Some(Terminal::Retired));
        "stopped"
    } else {
        observe(&mut observation, |value| value.terminal = Some(Terminal::QuiescenceUnknown));
        "quiescence_unknown"
    }
    .to_owned();
    summary.stop_reason = collected.as_ref().copied().unwrap_or("failed").to_owned();
    let result = collected.map(|_| ()).and(retired).and_then(|_| {
        ensure!(
            summary.selected_frames > 0,
            "no complete H.264 frames were selected; inspect the recording manifest"
        );
        Ok(())
    });
    summary.failure = result.as_ref().err().map(|error| format!("{error:#}"));
    let recording_result = finish_recording(&mut recording, &summary);
    match (result, recording_result) {
        (Ok(()), Ok(())) => Ok(summary),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(retirement)) => {
            Err(error.context(format!("recording finalization also failed: {retirement:#}")))
        }
    }
}

fn finish_recording(recording: &mut RecordingClient, summary: &Summary) -> Result<()> {
    recording.finish(summary.clone())
}

pub(crate) fn bind(configuration: &Configuration) -> Result<ReceiverSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.bind_device(Some(configuration.interface.as_bytes())).context(
        "bind video socket to the selected interface; the OS must permit interface binding",
    )?;

    #[cfg(target_os = "linux")]
    socket.set_multicast_all_v4(false)?;

    socket
        .bind(&SocketAddr::V4(configuration.destination).into())
        .context("bind configured video destination")?;

    let multicast_membership = if configuration.destination.ip().is_multicast() {
        socket
            .join_multicast_v4(configuration.destination.ip(), &configuration.interface_address)?;
        Some((*configuration.destination.ip(), configuration.interface_address))
    } else {
        None
    };

    socket.set_nonblocking(true)?;
    Ok(ReceiverSocket {
        socket: UdpSocket::from_std(socket.into()).context("register configured video socket")?,
        destination: configuration.destination,
        multicast_membership,
    })
}

fn clock_sample(origin: Instant) -> Result<i64> {
    i64::try_from(Instant::now().duration_since(origin).as_nanos())
        .context("monotonic video clock exceeded its range")
}

fn instant_at(origin: Instant, nanoseconds: i64) -> Result<Instant> {
    origin
        .checked_add(Duration::from_nanos(u64::try_from(nanoseconds)?))
        .context("monotonic video deadline overflow")
}

fn reject(summary: &mut Summary, reason: &str) {
    *summary.rejected.entry(reason.to_owned()).or_default() += 1;
}

fn observe(
    observer: &mut Option<watch::Sender<Observation>>,
    update: impl FnOnce(&mut Observation),
) {
    if let Some(observer) = observer {
        let mut snapshot = *observer.borrow();
        update(&mut snapshot);
        observer.send_replace(snapshot);
    }
}

async fn interruption() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut termination = signal(SignalKind::terminate())?;

        tokio::select! {
            _ = interrupt.recv() => {}
            _ = termination.recv() => {}
        }
    }

    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;

    Ok(())
}
