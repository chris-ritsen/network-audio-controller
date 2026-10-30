mod discovery;
mod inventory;
mod publication;
mod unicast;
mod workers;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, SocketAddrV4};
use std::num::{NonZeroU16, NonZeroU32};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use netaudio_core::video::{inventory as wire_inventory, multicast, source_control, subscription};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use super::recording;
use super::transmit::{self, Observation as TransmitObservation};
use super::{Configuration as ReceiveConfiguration, Observation as ReceiveObservation};
use publication::Publication;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub device_name: String,
    pub arc_bind: SocketAddrV4,
    pub run_duration_seconds: u64,
    pub retirement_timeout_milliseconds: u64,
    pub evidence: Evidence,
    pub source: Option<Source>,
    pub receivers: Vec<Receiver>,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub path: Option<PathBuf>,
    pub standard_output: bool,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub transport: SourceTransport,
    pub global_flow: NonZeroU16,
    pub local_flow: NonZeroU16,
    pub global_channel: NonZeroU16,
    pub local_channel: NonZeroU16,
    pub service_channel: NonZeroU16,
    pub channel_codec_identifier: u32,
    pub channel_count: NonZeroU16,
    pub channel_name: String,
    pub flow_name: String,
    pub latency_nanoseconds: u64,
    pub input: PathBuf,
    pub output: PathBuf,
    pub configuration: transmit::configuration::Configuration,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceTransport {
    Multicast,
    Unicast {
        control_bind: SocketAddrV4,
        permitted_requester: SocketAddrV4,
        receiver_host_label: String,
        ownership_value: NonZeroU32,
    },
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Acquisition {
    Multicast {
        source_flow: String,
        source_local_flow: NonZeroU16,
        source_association_slot: NonZeroU16,
    },
    Unicast {
        control: unicast::Configuration,
    },
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Receiver {
    pub acquisition: Acquisition,
    pub identifier: NonZeroU16,
    pub global_identifier: NonZeroU16,
    pub local_flow: NonZeroU16,
    pub global_flow: NonZeroU16,
    pub channel_name: String,
    pub source_device: String,
    pub source_channel: String,
    pub source_service_channel: NonZeroU16,
    pub source_channel_count: NonZeroU16,
    pub discovery_timeout_milliseconds: u64,
    pub output: PathBuf,
    pub record_frames: bool,
    pub configuration: ReceiveConfiguration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct NamedSource {
    channel: Vec<u8>,
    device: Vec<u8>,
}

#[derive(Default, Serialize)]
struct SourceState {
    generation: u64,
    allocated: bool,
    allocation_ownership_value: Option<u32>,
    reserved_socket_owned: bool,
    used: bool,
    worker_active: bool,
    publication_requested: bool,
    observation: TransmitObservation,
    failure: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReceiverPhase {
    #[default]
    Idle,
    Unresolved,
    Resolving,
    Preparing,
    Receiving,
    Retiring,
    Retired,
    Failed,
    FreshAuthorityRequired,
}

#[derive(Default, Serialize)]
struct ReceiverState {
    generation: u64,
    desired: Option<NamedSource>,
    worker_generation: Option<u64>,
    phase: ReceiverPhase,
    destination_previously_owned: bool,
    source_control: Option<unicast::State>,
    source_control_history: Vec<unicast::Event>,
    failure_subscription_status: u16,
    observation: ReceiveObservation,
    failure: Option<String>,
    media_finished: bool,
    recording_pending: bool,
}

#[derive(Serialize)]
struct Status<'a> {
    device_name: &'a str,
    incarnation: uuid::Uuid,
    revision: u64,
    source: Option<&'a SourceState>,
    receivers: &'a BTreeMap<NonZeroU16, ReceiverState>,
}

pub struct Runtime {
    configuration: Configuration,
    incarnation: uuid::Uuid,
    revision: u64,
    source: SourceState,
    source_socket: Option<tokio::net::UdpSocket>,
    source_exchanges: BTreeMap<u16, Exchange>,
    receivers: BTreeMap<NonZeroU16, ReceiverState>,
    source_cancellation: Option<watch::Sender<bool>>,
    receiver_cancellation: BTreeMap<NonZeroU16, watch::Sender<bool>>,
    publication: Option<Publication>,
    events: mpsc::Receiver<workers::Event>,
    event_sender: mpsc::Sender<workers::Event>,
    jobs: JoinSet<workers::Completion>,
    recording_jobs: JoinSet<workers::Completion>,
    recordings: BTreeMap<(NonZeroU16, u64), recording::Supervisor>,
    stopping: bool,
}

struct Exchange {
    request: Vec<u8>,
    reply: Vec<u8>,
}

struct EvidenceWriter {
    file: Option<std::fs::File>,
    standard_output: bool,
}

impl EvidenceWriter {
    fn new(evidence: &Evidence) -> Result<Self> {
        Ok(Self {
            file: evidence
                .path
                .as_ref()
                .map(|path| std::fs::OpenOptions::new().write(true).create_new(true).open(path))
                .transpose()
                .context("create owned video evidence stream")?,
            standard_output: evidence.standard_output,
        })
    }

    fn emit(&mut self, event: &str, runtime: &Runtime, failure: Option<&str>) -> Result<()> {
        let value = serde_json::json!({
            "event": event,
            "observed_at": chrono::Utc::now(),
            "status": runtime.status(),
            "failure": failure,
        });
        let mut line = serde_json::to_vec(&value)?;
        line.push(b'\n');
        if let Some(file) = &mut self.file {
            file.write_all(&line)?;
            file.flush()?;
        }
        if self.standard_output {
            std::io::stdout().lock().write_all(&line)?;
        }
        Ok(())
    }
}

impl Configuration {
    pub fn read(path: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)?.take(65_537).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 65_536, "owned video configuration exceeds the input limit");
        let configuration: Self =
            serde_json::from_slice(&bytes).context("decode owned video configuration")?;
        validate(&configuration)?;
        Ok(configuration)
    }
}

pub async fn run(configuration_path: &Path) -> Result<()> {
    gstreamer::init().context("initialize owned video providers")?;
    let configuration = Configuration::read(configuration_path)?;
    let retirement_timeout = Duration::from_millis(configuration.retirement_timeout_milliseconds);
    let mut evidence = EvidenceWriter::new(&configuration.evidence)?;
    let socket = tokio::net::UdpSocket::bind(configuration.arc_bind)
        .await
        .context("bind explicit owned video ARC endpoint")?;
    let source_control_socket = match configuration.source.as_ref() {
        Some(Source {
            transport:
                SourceTransport::Unicast {
                    control_bind,
                    ..
                },
            configuration,
            ..
        }) => Some(bind_control(*control_bind, &configuration.interface)?),
        _ => None,
    };
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(configuration.run_duration_seconds))
        .context("owned video runtime deadline overflow")?;
    let mut runtime = Runtime::new(configuration)?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut termination =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut bytes = vec![0; 65_536];
    let mut control_bytes = vec![0; source_control::MAXIMUM_DATAGRAM_BYTES + 1];
    let execution = async {
        runtime.prepare_source().await?;
        evidence.emit("listening", &runtime, None)?;
        loop {
            tokio::select! {
                _ = interrupt.recv() => break Ok(()),
                _ = termination.recv() => break Ok(()),
                _ = tokio::time::sleep_until(deadline) => break Ok(()),
                event = runtime.events.recv() => {
                    runtime.observe(event.context("owned video observation stream ended")?)?;
                    evidence.emit("lifecycle", &runtime, None)?;
                }
                completed = runtime.jobs.join_next(), if !runtime.jobs.is_empty() => {
                    match completed {
                        Some(Ok(completed)) => runtime.complete(completed).await?,
                        Some(Err(error)) => {
                            runtime.mark_active_media_unknown();
                            return Err(anyhow::Error::from(error).context("owned video worker panicked; provider quiescence is unknown"));
                        }
                        None => bail!("owned video worker set ended unexpectedly"),
                    }
                    evidence.emit("worker_completed", &runtime, None)?;
                }
                completed = runtime.recording_jobs.join_next(), if !runtime.recording_jobs.is_empty() => {
                    match completed {
                        Some(Ok(completion)) => {
                            runtime.complete(completion).await?;
                            evidence.emit("recording_completed", &runtime, None)?;
                        }
                        Some(Err(error)) => {
                            runtime.refresh_recording_watchers()?;
                            evidence.emit("recording_watcher_restarted", &runtime, Some(&error.to_string()))?;
                        }
                        None => {}
                    }
                }
                received = socket.recv_from(&mut bytes) => {
                    let (length, peer) = received.context("receive owned video ARC request")?;
                    match runtime.apply(&bytes[..length]).await {
                        Ok(reply) => {
                            match socket.send_to(&reply, peer).await {
                                Ok(sent) if sent == reply.len() => evidence.emit("control_accepted", &runtime, None)?,
                                Ok(_) => evidence.emit("reply_delivery_unknown", &runtime, Some("ARC datagram was not sent completely"))?,
                                Err(error) => evidence.emit("reply_delivery_unknown", &runtime, Some(&error.to_string()))?,
                            }
                            runtime.reconcile().await?;
                            evidence.emit("intent_reconciled", &runtime, None)?;
                        }
                        Err(error) => evidence.emit("control_discarded", &runtime, Some(&format!("{error:#}")))?,
                    }
                }
                received = receive_control(source_control_socket.as_ref(), &mut control_bytes) => {
                    let (length, peer) = received.context("receive owned video source-control request")?;
                    match runtime.apply_source_control(&control_bytes[..length], peer).await {
                        Ok(reply) => {
                            let socket = source_control_socket.as_ref().unwrap();
                            match socket.send_to(&reply, peer).await {
                                Ok(sent) if sent == reply.len() => evidence.emit("source_control_accepted", &runtime, None)?,
                                Ok(_) => evidence.emit("source_reply_delivery_unknown", &runtime, Some("source-control datagram was not sent completely"))?,
                                Err(error) => evidence.emit("source_reply_delivery_unknown", &runtime, Some(&error.to_string()))?,
                            }
                        }
                        Err(error) => evidence.emit("source_control_discarded", &runtime, Some(&format!("{error:#}")))?,
                    }
                }
            }
        }
    }.await;
    drop(socket);
    let retirement = match tokio::time::timeout(retirement_timeout, runtime.shutdown(&mut evidence))
        .await
    {
        Ok(result) => result,
        Err(error) => {
            runtime.jobs.abort_all();
            if runtime.source.worker_active {
                runtime.source.observation.terminal = Some(transmit::Terminal::QuiescenceUnknown);
            }
            for receiver in runtime
                .receivers
                .values_mut()
                .filter(|receiver| receiver.worker_generation.is_some())
            {
                receiver.observation.terminal = Some(super::Terminal::QuiescenceUnknown);
            }
            let mut cleanup_failures = Vec::new();
            if let Err(error) = runtime.advance() {
                cleanup_failures.push(format!("cannot advance timeout state: {error:#}"));
            }
            if let Err(error) = evidence.emit(
                "retirement_unknown",
                &runtime,
                Some("owned video worker retirement exceeded its configured deadline"),
            ) {
                cleanup_failures
                    .push(format!("cannot emit retirement timeout evidence: {error:#}"));
            }
            runtime.collect_retired(&mut cleanup_failures).await;
            if !runtime.recordings.is_empty() {
                cleanup_failures
                    .push("owned recording supervisors remain after retirement timeout".to_owned());
            }
            if let Err(error) = evidence.emit(
                "retired_after_timeout",
                &runtime,
                (!cleanup_failures.is_empty()).then(|| cleanup_failures.join("; ")).as_deref(),
            ) {
                cleanup_failures
                    .push(format!("cannot emit post-timeout retirement evidence: {error:#}"));
            }
            let deadline = anyhow::Error::from(error)
                .context("owned video worker retirement exceeded its configured deadline; provider quiescence is unknown");
            if cleanup_failures.is_empty() {
                Err(deadline)
            } else {
                Err(deadline.context(format!(
                    "post-timeout recording retirement also failed: {}",
                    cleanup_failures.join("; ")
                )))
            }
        }
    };
    match (execution, retirement) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(retirement)) => {
            Err(error.context(format!("owned video retirement also failed: {retirement:#}")))
        }
    }
}

impl Runtime {
    pub fn schema() -> schemars::Schema {
        schemars::schema_for!(Configuration)
    }

    fn new(configuration: Configuration) -> Result<Self> {
        validate(&configuration)?;
        let receivers = configuration
            .receivers
            .iter()
            .map(|receiver| (receiver.identifier, ReceiverState::default()))
            .collect();
        let (event_sender, events) = mpsc::channel(32);
        Ok(Self {
            configuration,
            incarnation: uuid::Uuid::new_v4(),
            revision: 0,
            source: SourceState::default(),
            source_socket: None,
            source_exchanges: BTreeMap::new(),
            receivers,
            source_cancellation: None,
            receiver_cancellation: BTreeMap::new(),
            publication: None,
            events,
            event_sender,
            jobs: JoinSet::new(),
            recording_jobs: JoinSet::new(),
            recordings: BTreeMap::new(),
            stopping: false,
        })
    }

    fn status(&self) -> Status<'_> {
        Status {
            device_name: &self.configuration.device_name,
            incarnation: self.incarnation,
            revision: self.revision,
            source: self.configuration.source.as_ref().map(|_| &self.source),
            receivers: &self.receivers,
        }
    }

    fn advance(&mut self) -> Result<()> {
        self.revision = self.revision.checked_add(1).context("owned video revision exhausted")?;
        Ok(())
    }

    async fn prepare_source(&mut self) -> Result<()> {
        let Some(source) = self.configuration.source.as_ref() else {
            return Ok(());
        };
        if matches!(source.transport, SourceTransport::Unicast { .. }) {
            self.source_socket = Some(transmit::bind(&source.configuration)?);
            self.source.reserved_socket_owned = true;
            let publication = Publication::start(&self.configuration, source).await?;
            self.publication = Some(publication);
            self.source.publication_requested = true;
            self.advance()?;
        }
        Ok(())
    }

    async fn apply_source_control(&mut self, bytes: &[u8], peer: SocketAddr) -> Result<Vec<u8>> {
        ensure!(!self.stopping, "owned video endpoint is stopping");
        let source = self.configuration.source.as_ref().context("endpoint owns no source")?;
        let SourceTransport::Unicast {
            permitted_requester,
            receiver_host_label,
            ownership_value,
            ..
        } = &source.transport
        else {
            bail!("source is not configured for unicast allocation");
        };
        ensure!(
            peer == SocketAddr::V4(*permitted_requester),
            "source-control peer is outside the supplied authority"
        );
        let operation = word(bytes, 6)?;
        let transaction = word(bytes, 4)?;
        if let Some(exchange) = self.source_exchanges.get(&transaction) {
            ensure!(
                exchange.request == bytes,
                "source-control transaction was reused with different request bytes"
            );
            return Ok(exchange.reply.clone());
        }
        match operation {
            source_control::ALLOCATION_OPERATION => {
                let request = source_control::decode_allocation_request(bytes)?;
                ensure!(
                    !self.source.used,
                    "owned source allocation was already consumed; fresh authority is required"
                );
                let allocation = &request.allocation;
                ensure!(
                    allocation.receiver_host_label == receiver_host_label.as_bytes()
                        && allocation.source_service_channel == source.service_channel
                        && allocation.destination == source.configuration.destination,
                    "allocation does not match the supplied owned source and receiver authority"
                );
                ensure!(
                    allocation.optional.raw_at_8 == Some(0)
                        && allocation.optional.raw_at_28 == Some(0)
                        && allocation.optional.raw_option == Some(0)
                        && allocation.optional.opaque_span.is_none(),
                    "allocation contains unsupported optional semantics"
                );
                source.configuration.validate()?;
                ensure!(!source.output.exists(), "source output directory already exists");
                let (input, frames) =
                    transmit::SourceInput::open(&source.input, &source.configuration)?;
                let generation =
                    self.source.generation.checked_add(1).context("source generation exhausted")?;
                self.revision.checked_add(1).context("owned video revision exhausted")?;
                let reply = source_control::encode_allocation_reply(
                    transaction,
                    1,
                    Some(source_control::AllocationOutcome {
                        allocation: source_control::AllocationHandle {
                            global_flow_identifier: source.global_flow.get(),
                            ownership_value: ownership_value.get(),
                        },
                        interface_outcomes: &[1],
                    }),
                )?;
                let socket = self
                    .source_socket
                    .take()
                    .context("owned source media socket is not reserved")?;
                let (cancellation, cancelled) = watch::channel(false);
                self.source_cancellation = Some(cancellation);
                self.source.generation = generation;
                self.source.allocated = true;
                self.source.allocation_ownership_value = Some(ownership_value.get());
                self.source.reserved_socket_owned = false;
                self.source.used = true;
                self.source.worker_active = true;
                self.jobs.spawn(workers::source(
                    transmit::Session {
                        configuration: source.configuration.clone(),
                        input,
                        frames,
                        directory: source.output.clone(),
                        duration: Duration::from_secs(self.configuration.run_duration_seconds),
                        prepare_only: false,
                        socket: Some(socket),
                        peer: None,
                        cancellation: Some(cancelled),
                        observation: None,
                    },
                    generation,
                    self.event_sender.clone(),
                ));
                self.source_exchanges.insert(
                    transaction,
                    Exchange {
                        request: bytes.to_vec(),
                        reply: reply.clone(),
                    },
                );
                self.advance()?;
                Ok(reply)
            }
            source_control::STOP_OPERATION => {
                let request = source_control::decode_stop_request(bytes)?;
                let result = if request.allocation.global_flow_identifier
                    != source.global_flow.get()
                    || !self.source.allocated
                {
                    0x0102
                } else if self.source.allocation_ownership_value
                    != Some(request.allocation.ownership_value)
                {
                    0x0103
                } else {
                    self.revision.checked_add(1).context("owned video revision exhausted")?;
                    self.source.allocated = false;
                    self.source.allocation_ownership_value = None;
                    if let Some(cancellation) = &self.source_cancellation {
                        cancellation.send_replace(true);
                    }
                    self.advance()?;
                    match self.withdraw().await {
                        Ok(()) => 1,
                        Err(error) => {
                            self.source.failure = Some(format!(
                                "source STOP publication withdrawal failed: {error:#}"
                            ));
                            0x03f0
                        }
                    }
                };
                let reply = source_control::StopReply {
                    transaction,
                    result,
                }
                .encode()?;
                self.source_exchanges.insert(
                    transaction,
                    Exchange {
                        request: bytes.to_vec(),
                        reply: reply.clone(),
                    },
                );
                Ok(reply)
            }
            _ => bail!("source-control operation is outside the configured owned video role"),
        }
    }

    async fn apply(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        ensure!(!self.stopping, "owned video endpoint is stopping");
        let operation = word(bytes, 6)?;
        match operation {
            netaudio_core::protocol::OPCODE_CHANNEL_COUNT => {
                let context = context(bytes)?;
                ensure!(bytes.len() == 10, "video capability query has an unsupported extent");
                self.capability(context)
            }
            netaudio_core::commands::OPCODE_CREATE_TX_FLOW_2809 => {
                let request = multicast::CreateRequest::decode(bytes)?;
                ensure!(
                    !self.source.used,
                    "source group has already been used; fresh source authority is required before reuse"
                );
                let source =
                    self.configuration.source.as_ref().context("this endpoint owns no source")?;
                ensure!(
                    matches!(source.transport, SourceTransport::Multicast),
                    "ARC multicast creation does not select a unicast source"
                );
                ensure!(
                    request.media_local_flow == source.local_flow
                        && request.media_local_channels == [source.local_channel],
                    "create does not select the configured source flow and channel"
                );
                source.configuration.validate()?;
                ensure!(!source.output.exists(), "source output directory already exists");
                let (input, frames) =
                    transmit::SourceInput::open(&source.input, &source.configuration)?;
                let socket = transmit::bind(&source.configuration)?;
                let source = source.clone();
                self.source.generation =
                    self.source.generation.checked_add(1).context("source generation exhausted")?;
                self.source.allocated = true;
                self.source.used = true;
                self.source.worker_active = true;
                let (cancellation, cancelled) = watch::channel(false);
                self.source_cancellation = Some(cancellation);
                self.jobs.spawn(workers::source(
                    transmit::Session {
                        configuration: source.configuration.clone(),
                        input,
                        frames,
                        directory: source.output.clone(),
                        duration: Duration::from_secs(self.configuration.run_duration_seconds),
                        prepare_only: false,
                        socket: Some(socket),
                        peer: None,
                        cancellation: Some(cancelled),
                        observation: None,
                    },
                    self.source.generation,
                    self.event_sender.clone(),
                ));
                self.advance()?;
                match Publication::start(&self.configuration, &source).await {
                    Ok(publication) => {
                        self.publication = Some(publication);
                        self.source.publication_requested = true;
                    }
                    Err(error) => {
                        self.source.failure = Some(format!("source publication failed: {error:#}"));
                        self.source_cancellation.as_ref().unwrap().send_replace(true);
                        return Err(error.context("source allocation committed but publication failed; read current state before another operation"));
                    }
                }
                let mut response = self.transmitter_flow_page(
                    wire_inventory::Context {
                        revision: request.context.revision,
                        transaction: request.context.transaction,
                    },
                    source.local_flow.get(),
                    source.local_flow.get(),
                    1,
                )?;
                response[6..8].copy_from_slice(&operation.to_be_bytes());
                Ok(response)
            }
            netaudio_core::commands::OPCODE_DELETE_TX_FLOW_2809 => {
                let request = multicast::DeleteRequest::decode(bytes)?;
                let source =
                    self.configuration.source.as_ref().context("this endpoint owns no source")?;
                ensure!(
                    matches!(source.transport, SourceTransport::Multicast),
                    "ARC multicast deletion does not select a unicast source"
                );
                ensure!(
                    request.media_local_flow == source.local_flow && self.source.allocated,
                    "delete does not select an allocated owned source flow"
                );
                self.source.allocated = false;
                if let Some(cancellation) = &self.source_cancellation {
                    cancellation.send_replace(true);
                }
                self.advance()?;
                self.withdraw().await?;
                Ok(common_reply(
                    request.context.revision,
                    request.context.transaction,
                    operation,
                    1,
                ))
            }
            netaudio_core::commands::OPCODE_MODERN_ARC_SUBSCRIPTION => {
                let request = subscription::Request::decode(bytes)?;
                for intent in &request.intents {
                    let identifier = match intent {
                        subscription::Intent::Set {
                            receiver,
                            ..
                        }
                        | subscription::Intent::Clear {
                            receiver,
                        } => NonZeroU16::new(*receiver).context("receiver identifier is zero")?,
                    };
                    ensure!(
                        self.receivers.contains_key(&identifier),
                        "subscription selects an unconfigured current receiver"
                    );
                }
                if request.unsupported_form {
                    return Ok(request.acknowledgement().to_vec());
                }
                let mut staged = self
                    .receivers
                    .iter()
                    .map(|(identifier, receiver)| (*identifier, receiver.desired.clone()))
                    .collect::<BTreeMap<_, _>>();
                for intent in &request.intents {
                    match intent {
                        subscription::Intent::Set {
                            receiver,
                            source_channel,
                            source_device,
                        } => {
                            staged.insert(
                                NonZeroU16::new(*receiver).unwrap(),
                                Some(NamedSource {
                                    channel: source_channel.clone(),
                                    device: source_device.clone(),
                                }),
                            );
                        }
                        subscription::Intent::Clear {
                            receiver,
                        } => {
                            staged.insert(NonZeroU16::new(*receiver).unwrap(), None);
                        }
                    }
                }
                let changes = staged
                    .into_iter()
                    .filter(|(identifier, desired)| self.receivers[identifier].desired != *desired)
                    .collect::<Vec<_>>();
                for (identifier, _) in &changes {
                    self.receivers[identifier]
                        .generation
                        .checked_add(1)
                        .context("receiver intent generation exhausted")?;
                }
                self.revision.checked_add(1).context("owned video revision exhausted")?;
                for (identifier, desired) in changes {
                    let receiver = self.receivers.get_mut(&identifier).unwrap();
                    receiver.generation += 1;
                    receiver.desired = desired;
                    receiver.failure = None;
                    if let Some(cancellation) = self.receiver_cancellation.get(&identifier) {
                        cancellation.send_replace(true);
                        receiver.phase = ReceiverPhase::Retiring;
                    } else {
                        receiver.phase = if receiver.desired.is_some() {
                            ReceiverPhase::Unresolved
                        } else {
                            ReceiverPhase::Idle
                        };
                    }
                }
                self.advance()?;
                Ok(request.acknowledgement().to_vec())
            }
            netaudio_core::commands::OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809
            | netaudio_core::commands::OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809
            | netaudio_core::commands::OPCODE_QUERY_TX_FLOWS_2809
            | netaudio_core::commands::OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809 => {
                self.inventory(wire_inventory::Query::decode(bytes)?)
            }
            _ => bail!("video control operation is outside the configured endpoint role"),
        }
    }

    async fn reconcile(&mut self) -> Result<()> {
        if self.stopping {
            return Ok(());
        }
        let mut changed = false;
        for definition in &self.configuration.receivers {
            let state = self.receivers.get_mut(&definition.identifier).unwrap();
            if state.worker_generation.is_some()
                || state.recording_pending
                || state.phase != ReceiverPhase::Unresolved
            {
                continue;
            }
            let Some(desired) = &state.desired else {
                continue;
            };
            if desired.channel != definition.source_channel.as_bytes()
                || desired.device != definition.source_device.as_bytes()
            {
                continue;
            }
            changed = true;
            if state.destination_previously_owned {
                state.phase = ReceiverPhase::FreshAuthorityRequired;
                state.failure = Some("the previous destination was owned; local retirement does not prove old network traffic is drained; supply fresh source/destination authority before another receive context".to_owned());
                continue;
            }
            let generation = state.generation;
            let origin = tokio::time::Instant::now();
            let session_generation = uuid::Uuid::new_v4().to_string();
            ensure!(
                !self.recordings.contains_key(&(definition.identifier, generation)),
                "receiver recording generation already has an owner"
            );
            let prepared = async {
                let (mut supervisor, client) = recording::Supervisor::start(
                    definition.output.clone(),
                    definition.configuration.clone(),
                    definition.record_frames,
                    origin,
                )?;
                let terminal = match supervisor.watcher() {
                    Ok(Some(terminal)) => terminal,
                    failure => {
                        drop(client);
                        let retired = supervisor.terminal().await;
                        let failure = failure.err().unwrap_or_else(|| {
                            anyhow::anyhow!(
                                "new recording supervisor did not provide a terminal watcher"
                            )
                        });
                        return match retired {
                            Ok(_) => Err(failure),
                            Err(retirement) => Err(failure.context(format!(
                                "recording retirement also failed: {retirement:#}"
                            ))),
                        };
                    }
                };
                Ok((supervisor, client, terminal))
            }
            .await;
            let (supervisor, client, terminal) = match prepared {
                Ok(prepared) => prepared,
                Err(error) => {
                    state.phase = ReceiverPhase::Failed;
                    state.failure_subscription_status = 19;
                    state.failure = Some(format!("receiver recording setup failed: {error:#}"));
                    continue;
                }
            };
            self.recordings.insert((definition.identifier, generation), supervisor);
            self.recording_jobs.spawn(workers::recording(
                definition.identifier,
                generation,
                terminal,
            ));
            let (cancellation, cancelled) = watch::channel(false);
            state.worker_generation = Some(generation);
            state.phase = ReceiverPhase::Resolving;
            state.observation = ReceiveObservation::default();
            state.source_control = None;
            state.source_control_history.clear();
            state.media_finished = false;
            state.recording_pending = true;
            self.receiver_cancellation.insert(definition.identifier, cancellation);
            self.jobs.spawn(workers::receiver(
                definition.clone(),
                generation,
                self.event_sender.clone(),
                cancelled,
                Duration::from_secs(self.configuration.run_duration_seconds),
                recording::Session {
                    client,
                    origin,
                    generation: session_generation,
                },
            ));
        }
        if changed {
            self.advance()?;
        }
        Ok(())
    }

    fn observe(&mut self, event: workers::Event) -> Result<()> {
        match event {
            workers::Event::SourceResolved(identifier, generation) => {
                let receiver =
                    self.receivers.get_mut(&identifier).context("unknown resolved receiver")?;
                if receiver.worker_generation != Some(generation)
                    || receiver.generation != generation
                    || receiver.phase == ReceiverPhase::Retiring
                {
                    return Ok(());
                }
                receiver.phase = ReceiverPhase::Preparing;
            }
            workers::Event::Source(generation, observation)
                if self.source.worker_active && self.source.generation == generation =>
            {
                self.source.observation = observation;
            }
            workers::Event::Receiver(identifier, generation, observation) => {
                let receiver =
                    self.receivers.get_mut(&identifier).context("unknown receiver observation")?;
                if receiver.worker_generation != Some(generation) {
                    return Ok(());
                }
                receiver.observation = observation;
                receiver.destination_previously_owned |= observation.socket_owned;
                if receiver.generation == generation
                    && receiver.phase != ReceiverPhase::Retiring
                    && (observation.multicast_joined
                        || (observation.socket_owned
                            && receiver
                                .source_control
                                .as_ref()
                                .is_some_and(unicast::State::is_allocated)
                            && observation.configured))
                {
                    receiver.phase = ReceiverPhase::Receiving;
                }
            }
            workers::Event::SourceControl(identifier, generation, state) => {
                let receiver = self
                    .receivers
                    .get_mut(&identifier)
                    .context("unknown receiver source-control observation")?;
                if receiver.worker_generation != Some(generation) {
                    return Ok(());
                }
                let allocated = state.is_allocated();
                receiver.source_control = Some(state);
                if receiver.generation == generation
                    && receiver.phase != ReceiverPhase::Retiring
                    && allocated
                    && receiver.observation.socket_owned
                    && receiver.observation.configured
                {
                    receiver.phase = ReceiverPhase::Receiving;
                }
            }
            _ => return Ok(()),
        }
        self.advance()
    }

    async fn complete(&mut self, completion: workers::Completion) -> Result<()> {
        match completion {
            workers::Completion::Source {
                generation,
                observation,
                failure,
            } => {
                ensure!(
                    self.source.generation == generation && self.source.worker_active,
                    "source completion does not belong to the active generation"
                );
                self.source.observation = observation;
                self.source.worker_active = false;
                self.source_cancellation = None;
                self.source.failure = match (self.source.failure.take(), failure) {
                    (Some(previous), Some(current)) => {
                        Some(format!("{previous}; source worker: {current}"))
                    }
                    (previous, current) => previous.or(current),
                };
                self.withdraw().await?;
            }
            workers::Completion::Receiver {
                identifier,
                generation,
                observation,
                failure,
                failure_subscription_status,
                source_control,
                source_control_history,
            } => {
                let receiver =
                    self.receivers.get_mut(&identifier).context("unknown receiver completion")?;
                ensure!(
                    receiver.worker_generation == Some(generation),
                    "receiver completion does not belong to its active generation"
                );
                receiver.observation = observation;
                receiver.destination_previously_owned |= observation.socket_owned;
                receiver.worker_generation = None;
                receiver.failure_subscription_status = failure_subscription_status;
                receiver.source_control = source_control;
                receiver.source_control_history = source_control_history;
                receiver.failure = match (receiver.failure.take(), failure) {
                    (Some(previous), Some(current)) => {
                        Some(format!("{previous}; receiver worker: {current}"))
                    }
                    (previous, current) => previous.or(current),
                };
                receiver.media_finished = true;
                receiver.phase = ReceiverPhase::Retiring;
                self.receiver_cancellation.remove(&identifier);
                if !receiver.recording_pending {
                    Self::settle_receiver(receiver, generation);
                }
            }
            workers::Completion::Recording {
                identifier,
                generation,
            } => {
                let terminal = self
                    .recordings
                    .get_mut(&(identifier, generation))
                    .context("recording completion has no owned supervisor")?
                    .join();
                self.recordings
                    .remove(&(identifier, generation))
                    .context("joined recording supervisor disappeared")?;
                self.recording_finished(identifier, generation, terminal)?;
            }
        }
        self.advance()?;
        self.reconcile().await?;
        Ok(())
    }

    fn recording_finished(
        &mut self,
        identifier: NonZeroU16,
        generation: u64,
        terminal: Result<recording::Terminal>,
    ) -> Result<()> {
        let cancellation = self.receiver_cancellation.get(&identifier).cloned();
        let receiver =
            self.receivers.get_mut(&identifier).context("unknown receiver recording completion")?;
        receiver.recording_pending = false;
        if let Err(error) = terminal {
            let current = format!("recording worker: {error:#}");
            receiver.failure = match receiver.failure.take() {
                Some(previous) => Some(format!("{previous}; {current}")),
                None => Some(current),
            };
            if let Some(cancellation) = cancellation {
                cancellation.send_replace(true);
            }
        }
        if receiver.media_finished {
            Self::settle_receiver(receiver, generation);
        }
        Ok(())
    }

    fn mark_active_media_unknown(&mut self) {
        if self.source.worker_active {
            self.source.observation.terminal = Some(transmit::Terminal::QuiescenceUnknown);
        }
        for receiver in
            self.receivers.values_mut().filter(|receiver| receiver.worker_generation.is_some())
        {
            receiver.observation.terminal = Some(super::Terminal::QuiescenceUnknown);
        }
    }

    fn settle_receiver(receiver: &mut ReceiverState, generation: u64) {
        if receiver.worker_generation.is_some() || receiver.recording_pending {
            receiver.phase = ReceiverPhase::Retiring;
        } else if receiver.desired.is_none() {
            receiver.phase = ReceiverPhase::Retired;
        } else if receiver.generation != generation {
            receiver.phase = ReceiverPhase::Unresolved;
        } else if receiver.failure.is_some() {
            receiver.phase = ReceiverPhase::Failed;
        } else {
            receiver.phase = ReceiverPhase::Retired;
        }
    }

    fn refresh_recording_watchers(&mut self) -> Result<()> {
        let mut watchers = Vec::new();
        for ((identifier, generation), supervisor) in &self.recordings {
            if let Some(watcher) = supervisor.watcher()? {
                watchers.push((*identifier, *generation, watcher));
            }
        }
        for (identifier, generation, watcher) in watchers {
            self.recording_jobs.spawn(workers::recording(identifier, generation, watcher));
        }
        Ok(())
    }

    async fn withdraw(&mut self) -> Result<()> {
        if let Some(publication) = self.publication.take() {
            let result = publication.stop().await;
            self.source.publication_requested = false;
            if let Err(error) = result {
                self.source.failure =
                    Some(format!("publication withdrawal is uncertain: {error:#}"));
                return Err(error);
            }
        }
        Ok(())
    }

    async fn shutdown(&mut self, evidence: &mut EvidenceWriter) -> Result<()> {
        self.stopping = true;
        self.source_socket.take();
        self.source.reserved_socket_owned = false;
        if let Some(cancellation) = &self.source_cancellation {
            cancellation.send_replace(true);
        }
        for cancellation in self.receiver_cancellation.values() {
            cancellation.send_replace(true);
        }
        let mut failures = Vec::new();
        if let Err(error) = self.withdraw().await {
            failures.push(format!("{error:#}"));
        }
        self.collect_retired(&mut failures).await;
        if !self.recordings.is_empty() {
            failures.push("owned recording supervisors remain after retirement".to_owned());
        }
        if self.source.observation.terminal == Some(transmit::Terminal::QuiescenceUnknown)
            || self.receivers.values().any(|receiver| {
                receiver.observation.terminal == Some(super::Terminal::QuiescenceUnknown)
            })
        {
            failures
                .push("a provider could not establish quiescence before its deadline".to_owned());
        }
        evidence.emit(
            "retired",
            self,
            (!failures.is_empty()).then(|| failures.join("; ")).as_deref(),
        )?;
        ensure!(failures.is_empty(), "{}", failures.join("; "));
        Ok(())
    }

    async fn collect_retired(&mut self, failures: &mut Vec<String>) {
        while !self.jobs.is_empty()
            || !self.recording_jobs.is_empty()
            || !self.recordings.is_empty()
        {
            if self.recording_jobs.is_empty() && !self.recordings.is_empty() {
                if let Err(error) = self.refresh_recording_watchers() {
                    failures.push(format!("video recording watcher recovery failed: {error:#}"));
                    break;
                }
                if self.recording_jobs.is_empty() {
                    failures.push("recording supervisor has no terminal watcher".to_owned());
                    break;
                }
                continue;
            }
            tokio::select! {
                event = self.events.recv() => {
                    if let Some(event) = event
                        && let Err(error) = self.observe(event) {
                            failures.push(format!("video retirement observation failed: {error:#}"));
                    }
                }
                completion = self.jobs.join_next(), if !self.jobs.is_empty() => {
                    match completion {
                        Some(Ok(completion)) => { if let Err(error) = self.complete(completion).await { failures.push(format!("{error:#}")); } }
                        Some(Err(error)) => {
                            self.mark_active_media_unknown();
                            failures.push(format!("video worker quiescence is unknown after a task failure: {error}"));
                        }
                        None => {},
                    }
                }
                completion = self.recording_jobs.join_next(), if !self.recording_jobs.is_empty() => {
                    match completion {
                        Some(Ok(completion)) => { if let Err(error) = self.complete(completion).await { failures.push(format!("{error:#}")); } }
                        Some(Err(error)) => {
                            failures.push(format!("video recording worker quiescence is unknown after a task failure: {error}"));
                            if let Err(error) = self.refresh_recording_watchers() {
                                failures.push(format!("video recording watcher recovery failed: {error:#}"));
                            }
                        }
                        None => {},
                    }
                }
            }
        }
        if !self.jobs.is_empty() {
            self.mark_active_media_unknown();
            self.jobs.abort_all();
            while let Some(completion) = self.jobs.join_next().await {
                match completion {
                    Ok(completion) => {
                        if let Err(error) = self.complete(completion).await {
                            failures.push(format!("media retirement completion failed: {error:#}"));
                        }
                    }
                    Err(error) => failures.push(format!("media retirement task failed: {error}")),
                }
            }
        }
        self.recording_jobs.abort_all();
        while self.recording_jobs.join_next().await.is_some() {}
        while let Some(key) = self.recordings.keys().next().copied() {
            let terminal = self.recordings.get_mut(&key).unwrap().terminal().await;
            self.recordings.remove(&key);
            if let Err(error) = self.recording_finished(key.0, key.1, terminal) {
                failures.push(format!("recording retirement completion failed: {error:#}"));
            }
            if let Err(error) = self.advance() {
                failures.push(format!("recording retirement revision failed: {error:#}"));
            }
        }
    }
}

fn bind_control(address: SocketAddrV4, interface: &str) -> Result<tokio::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.bind_device(Some(interface.as_bytes()))?;
    socket.bind(&SocketAddr::V4(address).into())?;
    socket.set_nonblocking(true)?;
    tokio::net::UdpSocket::from_std(socket.into()).context("register owned video control socket")
}

async fn receive_control(
    socket: Option<&tokio::net::UdpSocket>,
    bytes: &mut [u8],
) -> std::io::Result<(usize, SocketAddr)> {
    match socket {
        Some(socket) => socket.recv_from(bytes).await,
        None => std::future::pending().await,
    }
}

fn context(bytes: &[u8]) -> Result<wire_inventory::Context> {
    ensure!(
        bytes.len() >= 10 && word(bytes, 2)? as usize == bytes.len() && word(bytes, 8)? == 0,
        "invalid ARC request envelope"
    );
    let revision = word(bytes, 0)?;
    ensure!(
        (0x2802..=0x280f).contains(&revision),
        "ARC revision is outside the selected video profile"
    );
    Ok(wire_inventory::Context {
        revision,
        transaction: word(bytes, 4)?,
    })
}

fn word(bytes: &[u8], offset: usize) -> Result<u16> {
    Ok(u16::from_be_bytes(bytes.get(offset..offset + 2).context("truncated ARC word")?.try_into()?))
}

fn common_reply(revision: u16, transaction: u16, operation: u16, result: u16) -> Vec<u8> {
    [revision, 10, transaction, operation, result].into_iter().flat_map(u16::to_be_bytes).collect()
}

fn valid_name(value: &str) -> bool {
    !value.trim().is_empty() && !value.as_bytes().contains(&0)
}

fn validate(configuration: &Configuration) -> Result<()> {
    ensure!(
        valid_name(&configuration.device_name)
            && configuration.device_name.len() <= 63
            && !configuration.device_name.contains('.'),
        "owned video device name must be one complete DNS label"
    );
    ensure!(
        configuration.arc_bind.port() != 0
            && !configuration.arc_bind.ip().is_unspecified()
            && !configuration.arc_bind.ip().is_multicast(),
        "owned video ARC endpoint must use an explicit local unicast address and port"
    );
    ensure!(configuration.run_duration_seconds > 0, "owned video runtime duration is required");
    ensure!(
        configuration.retirement_timeout_milliseconds > 0,
        "owned video retirement deadline is required"
    );
    ensure!(
        configuration.source.is_some() || !configuration.receivers.is_empty(),
        "owned video endpoint has no configured resources"
    );
    ensure!(
        configuration.receivers.len() <= usize::from(u16::MAX),
        "receiver resources exceed the protocol identifier extent"
    );
    let mut outputs = BTreeSet::new();
    if let Some(source) = &configuration.source {
        ensure!(
            valid_name(&source.channel_name) && valid_name(&source.flow_name),
            "source names must be complete"
        );
        source.configuration.validate()?;
        match &source.transport {
            SourceTransport::Multicast => ensure!(
                source.configuration.destination.ip().is_multicast(),
                "multicast source requires an explicitly reserved group"
            ),
            SourceTransport::Unicast {
                control_bind,
                permitted_requester,
                receiver_host_label,
                ..
            } => {
                ensure!(
                    !source.configuration.destination.ip().is_multicast(),
                    "unicast source cannot use a multicast destination"
                );
                ensure!(
                    *control_bind.ip() == source.configuration.interface_address
                        && control_bind.port() != 0
                        && *control_bind != configuration.arc_bind
                        && *control_bind != source.configuration.source,
                    "unicast source-control socket must be separately reserved on its interface"
                );
                ensure!(
                    !permitted_requester.ip().is_unspecified()
                        && !permitted_requester.ip().is_multicast()
                        && permitted_requester.port() != 0
                        && valid_name(receiver_host_label),
                    "unicast source requires explicit requester and receiver-host authority"
                );
            }
        }
        ensure!(
            source.configuration.interface_address == *configuration.arc_bind.ip(),
            "source and ARC must belong to the configured endpoint interface"
        );
        ensure!(source.channel_codec_identifier == 1, "selected channel codec must be H264:1");
        ensure!(
            source.channel_count.get() == 1
                && source.local_channel.get() == 1
                && source.local_flow.get() == 1,
            "this configured source role owns one current channel and flow; counts and local identifiers must describe that actual resource"
        );
        ensure!(source.latency_nanoseconds > 0, "source latency metadata must be explicit");
        outputs.insert(source.output.clone());
    }
    let mut identifiers = BTreeSet::new();
    let mut global_identifiers = BTreeSet::new();
    let mut local_flows = BTreeSet::new();
    let mut global_flows = BTreeSet::new();
    for receiver in &configuration.receivers {
        ensure!(
            identifiers.insert(receiver.identifier)
                && global_identifiers.insert(receiver.global_identifier)
                && local_flows.insert(receiver.local_flow)
                && global_flows.insert(receiver.global_flow),
            "receiver channel and flow identities must each be unique in their namespace"
        );
        ensure!(
            usize::from(receiver.identifier.get()) <= configuration.receivers.len()
                && usize::from(receiver.local_flow.get()) <= configuration.receivers.len(),
            "receiver local channel and flow identities must cover the actual current resource interval"
        );
        ensure!(
            outputs.insert(receiver.output.clone()),
            "each media worker must own a distinct output directory"
        );
        receiver.configuration.validate()?;
        ensure!(
            receiver.configuration.interface_address == *configuration.arc_bind.ip(),
            "receiver must use the configured local interface"
        );
        match &receiver.acquisition {
            Acquisition::Multicast {
                source_flow,
                source_association_slot,
                ..
            } => {
                ensure!(
                    receiver.configuration.destination.ip().is_multicast()
                        && valid_name(source_flow)
                        && source_association_slot.get() == 1,
                    "multicast receiver requires a named group and selected source association"
                );
            }
            Acquisition::Unicast {
                control,
            } => {
                control.validate()?;
                ensure!(
                    !receiver.configuration.destination.ip().is_multicast()
                        && control.source_control != receiver.configuration.source
                        && *receiver.configuration.destination.ip()
                            == receiver.configuration.interface_address
                        && *control.control_bind.ip() == receiver.configuration.interface_address
                        && control.control_bind != configuration.arc_bind
                        && control.control_bind != receiver.configuration.destination,
                    "unicast receiver requires separate owned control and media sockets on its interface"
                );
            }
        }
        ensure!(
            valid_name(&receiver.channel_name)
                && valid_name(&receiver.source_device)
                && valid_name(&receiver.source_channel),
            "receiver and source names must be complete"
        );
        ensure!(
            receiver.discovery_timeout_milliseconds > 0,
            "receiver requires a bounded discovery deadline"
        );
    }
    Ok(())
}
