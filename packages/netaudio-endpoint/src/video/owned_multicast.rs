mod discovery;
mod inventory;
mod publication;
mod workers;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::SocketAddrV4;
use std::num::NonZeroU16;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use netaudio_core::video::{inventory as wire_inventory, multicast, subscription};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

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
#[serde(deny_unknown_fields)]
pub struct Receiver {
    pub identifier: NonZeroU16,
    pub global_identifier: NonZeroU16,
    pub local_flow: NonZeroU16,
    pub global_flow: NonZeroU16,
    pub channel_name: String,
    pub source_device: String,
    pub source_channel: String,
    pub source_flow: String,
    pub source_service_channel: NonZeroU16,
    pub source_channel_count: NonZeroU16,
    pub source_local_flow: NonZeroU16,
    pub source_association_slot: NonZeroU16,
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
    group_previously_joined: bool,
    failure_subscription_status: u16,
    observation: ReceiveObservation,
    failure: Option<String>,
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
    receivers: BTreeMap<NonZeroU16, ReceiverState>,
    source_cancellation: Option<watch::Sender<bool>>,
    receiver_cancellation: BTreeMap<NonZeroU16, watch::Sender<bool>>,
    publication: Option<Publication>,
    events: mpsc::Receiver<workers::Event>,
    event_sender: mpsc::Sender<workers::Event>,
    jobs: JoinSet<workers::Completion>,
    stopping: bool,
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
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(configuration.run_duration_seconds))
        .context("owned video runtime deadline overflow")?;
    let mut runtime = Runtime::new(configuration)?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut termination =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut bytes = vec![0; 65_536];
    let execution = async {
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
                    runtime.complete(completed.context("owned video worker set ended")?.context("owned video worker panicked")?).await?;
                    evidence.emit("worker_completed", &runtime, None)?;
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
                        }
                        Err(error) => evidence.emit("control_discarded", &runtime, Some(&format!("{error:#}")))?,
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
            runtime.advance()?;
            evidence.emit(
                "retirement_unknown",
                &runtime,
                Some("owned video worker retirement exceeded its configured deadline"),
            )?;
            Err(anyhow::Error::from(error).context("owned video worker retirement exceeded its configured deadline; provider quiescence is unknown"))
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
            receivers,
            source_cancellation: None,
            receiver_cancellation: BTreeMap::new(),
            publication: None,
            events,
            event_sender,
            jobs: JoinSet::new(),
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
                self.reconcile();
                Ok(request.acknowledgement().to_vec())
            }
            netaudio_core::commands::OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809
            | netaudio_core::commands::OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809
            | netaudio_core::commands::OPCODE_QUERY_TX_FLOWS_2809
            | netaudio_core::commands::OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809 => {
                self.inventory(wire_inventory::Query::decode(bytes)?)
            }
            _ => bail!("video control operation is outside the configured multicast role"),
        }
    }

    fn reconcile(&mut self) {
        if self.stopping {
            return;
        }
        for definition in &self.configuration.receivers {
            let state = self.receivers.get_mut(&definition.identifier).unwrap();
            if state.worker_generation.is_some() || state.phase != ReceiverPhase::Unresolved {
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
            if state.group_previously_joined {
                state.phase = ReceiverPhase::FreshAuthorityRequired;
                state.failure = Some("the previous group membership retired, but that does not prove old network traffic is drained; supply fresh source/group authority before another receive context".to_owned());
                continue;
            }
            let (cancellation, cancelled) = watch::channel(false);
            state.worker_generation = Some(state.generation);
            state.phase = ReceiverPhase::Resolving;
            state.observation = ReceiveObservation::default();
            self.receiver_cancellation.insert(definition.identifier, cancellation);
            self.jobs.spawn(workers::receiver(
                definition.clone(),
                state.generation,
                self.event_sender.clone(),
                cancelled,
                Duration::from_secs(self.configuration.run_duration_seconds),
            ));
        }
    }

    fn observe(&mut self, event: workers::Event) -> Result<()> {
        match event {
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
                receiver.group_previously_joined |= observation.multicast_joined;
                if receiver.generation == generation
                    && receiver.phase != ReceiverPhase::Retiring
                    && observation.multicast_joined
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
            } => {
                let receiver =
                    self.receivers.get_mut(&identifier).context("unknown receiver completion")?;
                ensure!(
                    receiver.worker_generation == Some(generation),
                    "receiver completion does not belong to its active generation"
                );
                receiver.observation = observation;
                receiver.group_previously_joined |= observation.multicast_joined;
                receiver.worker_generation = None;
                receiver.failure_subscription_status = failure_subscription_status;
                receiver.failure = failure;
                receiver.phase = if receiver.desired.is_none() {
                    ReceiverPhase::Retired
                } else if receiver.generation != generation {
                    ReceiverPhase::Unresolved
                } else if receiver.failure.is_some() {
                    ReceiverPhase::Failed
                } else {
                    ReceiverPhase::Retired
                };
                self.receiver_cancellation.remove(&identifier);
            }
        }
        self.advance()?;
        self.reconcile();
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
        while !self.jobs.is_empty() {
            tokio::select! {
                event = self.events.recv() => {
                    if let Some(event) = event { self.observe(event)?; }
                }
                completion = self.jobs.join_next() => {
                    match completion {
                        Some(Ok(completion)) => { if let Err(error) = self.complete(completion).await { failures.push(format!("{error:#}")); } }
                        Some(Err(error)) => failures.push(format!("video worker quiescence is unknown after a task failure: {error}")),
                        None => break,
                    }
                }
            }
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
        ensure!(
            source.configuration.destination.ip().is_multicast(),
            "owned source requires an explicitly reserved multicast group"
        );
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
            receiver.configuration.interface_address == *configuration.arc_bind.ip()
                && receiver.configuration.destination.ip().is_multicast(),
            "receiver must use the configured local interface and explicit multicast group"
        );
        ensure!(
            valid_name(&receiver.channel_name)
                && valid_name(&receiver.source_device)
                && valid_name(&receiver.source_channel)
                && valid_name(&receiver.source_flow),
            "receiver and source names must be complete"
        );
        ensure!(
            receiver.discovery_timeout_milliseconds > 0
                && receiver.source_association_slot.get() == 1,
            "receiver requires a bounded discovery deadline and qualified slot one"
        );
    }
    Ok(())
}
