use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::clock::discipline::Configuration as ClockConfiguration;
use netaudio_core::native_audio::clock::version_one::{DEFAULT_DOMAIN, PortIdentity};
use netaudio_core::native_audio::endpoint_control::Endpoint;
use netaudio_core::native_audio::endpoint_control::monitoring::{
    self, Identity, Interface, Manager, Statistics, StatusContext,
};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

use super::acquisition;
use super::clock_driver::Worker;
use super::deadline::Deadline;
use super::discovery::Publication;
use super::platform::{CounterProvider, GenerationIssuer, Generations, InterfaceObserver};
use super::receive_runtime;
use super::receiver;
use super::selection::Candidate;
use super::sockets::{self, Channel, Datagram, MediaPool, Monotonic};
use super::source;

const RECEIVE_QUEUE_DATAGRAMS: usize = 256;
const STATUS_QUEUE_REQUESTS: usize = 64;
const SOFTWARE_VERSION: u32 = 0x0001_0000;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusPublicationMode {
    StoredSubscriptions,
    MulticastAndStoredSubscriptions,
}

impl StatusPublicationMode {
    pub fn multicast_destination(self) -> Option<SocketAddrV4> {
        match self {
            Self::StoredSubscriptions => None,
            Self::MulticastAndStoredSubscriptions => {
                Some(monitoring::CLASS_TWO_MULTICAST_DESTINATION)
            }
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub name: String,
    pub interface: String,
    pub address: Ipv4Addr,
    pub identity: Identity,
    pub status_publication: StatusPublicationMode,
    pub local_clock_identity: PortIdentity,
    pub parent_clock_identity: PortIdentity,
    pub state_directory: PathBuf,
    pub audio: AudioConfiguration,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AudioConfiguration {
    Wave {
        input: PathBuf,
        output: Option<PathBuf>,
    },
    Native(crate::audio::Configuration),
}

impl Configuration {
    pub fn read(path: &Path) -> Result<Self> {
        Ok(serde_json::from_reader(std::fs::File::open(path)?)?)
    }
}

#[derive(Default, Serialize)]
pub struct Summary {
    pub routing_replies: u64,
    pub source_replies: u64,
    pub monitoring_replies: u64,
    pub multicast_status_sends: u64,
    pub unicast_status_sends: u64,
    pub status_send_failures: u64,
    pub rejected_datagrams: u64,
    pub media_failures: u64,
    pub maximum_clock_observation_nanoseconds: u64,
    pub submitted_packets: u64,
    pub maximum_submission_lateness_nanoseconds: u64,
    pub final_clock: Option<netaudio_core::native_audio::clock::discipline::Snapshot>,
    pub receiver_candidate: Option<Candidate>,
    pub receiver_unavailable: Option<String>,
    pub receiver_acquisition: Option<acquisition::Progress>,
    pub receive: Option<receive_runtime::Summary>,
    pub final_endpoint: Option<Endpoint>,
    pub audio: Option<crate::audio::Statistics>,
}

struct PendingStatus {
    request: Vec<u8>,
    due: u64,
}

struct StatusObservation {
    request: Vec<u8>,
    provider: CounterProvider,
    statistics: Result<Option<Statistics>>,
}

struct ControlSockets {
    routing: Arc<UdpSocket>,
    monitoring: Arc<UdpSocket>,
    control: Arc<UdpSocket>,
    source: Arc<UdpSocket>,
}

impl ControlSockets {
    async fn bind(address: Ipv4Addr, mode: StatusPublicationMode) -> Result<Self> {
        let monitoring = sockets::bind(address).await?;

        if mode.multicast_destination().is_some() {
            sockets::enable_multicast_publication(&monitoring, address)?;
        }

        Ok(Self {
            routing: sockets::bind(address).await?,
            monitoring,
            control: sockets::bind(address).await?,
            source: sockets::bind(address).await?,
        })
    }
}

fn address(socket: &UdpSocket) -> Result<SocketAddrV4> {
    match socket.local_addr()? {
        std::net::SocketAddr::V4(address) => Ok(address),
        _ => bail!("endpoint control socket does not own an IPv4 address"),
    }
}

fn reply(socket: &UdpSocket, peer: SocketAddrV4, bytes: &[u8]) -> Result<()> {
    let length = socket.try_send_to(bytes, peer.into())?;

    if length != bytes.len() {
        bail!("control socket submitted an incomplete datagram");
    }

    Ok(())
}

struct Server {
    configuration: Configuration,
    audio: Option<crate::audio::Engine>,
    service: super::service::Service,
    time: Monotonic,
    endpoint: Endpoint,
    interface: Interface,
    interface_observer: InterfaceObserver,
    clock: Worker,
    source: source::Runtime,
    receiver_discovery: receiver::Discovery,
    receiver_acquisition: acquisition::Client,
    receive: receive_runtime::Runtime,
    sockets: ControlSockets,
    monitoring: Manager,
    publication: Publication,
    readers: JoinSet<(Channel, Result<()>)>,
    datagrams: mpsc::Receiver<Datagram>,
    pending_status: VecDeque<PendingStatus>,
    counters: Option<CounterProvider>,
    status_job: Option<JoinHandle<StatusObservation>>,
    summary: Summary,
}

impl Server {
    async fn start(configuration: Configuration) -> Result<Self> {
        let service = super::service::Service::open()?;
        let (audio, capture, playback) = match &configuration.audio {
            AudioConfiguration::Wave {
                input,
                output,
            } => (
                None,
                super::pcm::CaptureFactory::Wave(input.clone()),
                output.as_deref().map(super::playback::Playback::open).transpose()?,
            ),
            AudioConfiguration::Native(configuration) => {
                let (engine, capture, playback) = crate::audio::Engine::open(configuration)?;
                (
                    Some(engine),
                    super::pcm::CaptureFactory::Native(capture),
                    Some(super::playback::Playback::Native(Box::new(playback))),
                )
            }
        };
        let endpoint = Endpoint::new(configuration.name.clone())?;
        let interface_observer =
            InterfaceObserver::start(configuration.interface.clone(), configuration.address)
                .await?;
        let interface = interface_observer.initial.clone();

        if !interface.link_up {
            bail!("selected endpoint interface is down");
        }

        let mut issuer = GenerationIssuer::open(
            &configuration.state_directory,
            &configuration.name,
            configuration.identity,
        )?;
        let incarnation = u64::from(issuer.reserve()?);
        let time = Monotonic::new(incarnation);
        let clock = Worker::start(
            configuration.address,
            ClockConfiguration {
                domain: DEFAULT_DOMAIN,
                local_identity: configuration.local_clock_identity,
                parent_identity: configuration.parent_clock_identity,
                interface_index: interface.index,
                provider_incarnation: incarnation,
                receive_calibration_nanoseconds: 0,
                transmit_calibration_nanoseconds: 0,
            },
            time.clone(),
        )
        .await?;
        let sockets =
            ControlSockets::bind(configuration.address, configuration.status_publication).await?;
        let pool = MediaPool::reserve(configuration.address, time.now()).await?;
        let generations = Generations::start(issuer);
        let source = source::Runtime::new(pool, generations, capture);
        let receiver_discovery =
            receiver::Discovery::new(configuration.address, interface.index, time.clone());
        let receiver_acquisition =
            acquisition::Client::new(configuration.address, time.clone()).await?;
        let receive = receive_runtime::Runtime::new(
            configuration.address,
            playback,
            time.clone(),
            incarnation,
        )
        .await?;
        let monitoring = Manager::new(
            configuration.identity,
            configuration.name.clone(),
            address(&sockets.control)?,
        )?;
        let (sender, datagrams) = mpsc::channel(RECEIVE_QUEUE_DATAGRAMS);
        let mut readers = JoinSet::new();

        for (channel, socket) in [
            (Channel::Routing, sockets.routing.clone()),
            (Channel::Monitoring, sockets.monitoring.clone()),
            (Channel::Control, sockets.control.clone()),
            (Channel::Source, sockets.source.clone()),
        ] {
            let owner = Arc::new(AtomicU64::new(incarnation));
            let time = time.clone();
            let sender = sender.clone();
            readers.spawn(async move {
                (channel, sockets::receive(socket, channel, owner, time, sender).await)
            });
        }

        for (index, slot) in source.pool.slots.iter().enumerate() {
            let channel = Channel::SourceMedia(index);
            let socket = slot.socket.clone();
            let owner = slot.owner.clone();
            let time = time.clone();
            let sender = sender.clone();
            readers.spawn(async move {
                (channel, sockets::receive(socket, channel, owner, time, sender).await)
            });
        }

        for (index, slot) in receive.pool.slots.iter().enumerate() {
            let channel = Channel::ReceiveMedia(index);
            let socket = slot.socket.clone();
            let owner = slot.owner.clone();
            let time = time.clone();
            let sender = sender.clone();
            readers.spawn(async move {
                (channel, sockets::receive(socket, channel, owner, time, sender).await)
            });
        }

        drop(sender);
        let publication = Publication::start(
            &endpoint,
            configuration.address,
            configuration.identity,
            address(&sockets.routing)?.port(),
            address(&sockets.monitoring)?.port(),
            address(&sockets.source)?.port(),
        )?;
        let mut counters = CounterProvider::new(configuration.interface.clone());
        counters.measure(time.now())?;

        Ok(Self {
            configuration,
            audio,
            service,
            time,
            endpoint,
            interface,
            interface_observer,
            clock,
            source,
            receiver_discovery,
            receiver_acquisition,
            receive,
            sockets,
            monitoring,
            publication,
            readers,
            datagrams,
            pending_status: VecDeque::new(),
            counters: Some(counters),
            status_job: None,
            summary: Summary::default(),
        })
    }

    fn datagram(&mut self, datagram: Datagram) -> Result<()> {
        match datagram.channel {
            Channel::Routing => {
                if let Some(answer) = self.endpoint.answer(&datagram.bytes)? {
                    reply(&self.sockets.routing, datagram.peer, &answer.bytes)?;
                    self.summary.routing_replies += 1;

                    if answer.routes_changed {
                        self.receiver_acquisition.synchronize(None);
                        self.receiver_discovery.select(&self.endpoint)?;
                        self.receiver_acquisition.synchronize(self.receiver_discovery.candidate());
                    }
                }
            }
            Channel::Monitoring => {
                if let Some(bytes) = self.monitoring.answer(&datagram.bytes, self.time.now())? {
                    reply(&self.sockets.monitoring, datagram.peer, &bytes)?;
                    self.summary.monitoring_replies += 1;
                }
            }
            Channel::Control => {
                let Some(query) = monitoring::StatusQuery::decode(&datagram.bytes)? else {
                    return Ok(());
                };

                if self.pending_status.len() >= STATUS_QUEUE_REQUESTS {
                    bail!("monitoring publication request queue is full");
                }

                let pending = PendingStatus {
                    request: datagram.bytes,
                    due: self
                        .time
                        .now()
                        .checked_add(query.delay_nanoseconds())
                        .context("status deadline overflow")?,
                };
                let position =
                    self.pending_status.partition_point(|request| request.due <= pending.due);
                self.pending_status.insert(position, pending);
            }
            Channel::Source => {
                if let Some(response) =
                    self.source.request(&mut self.endpoint, &datagram, self.time.now())?
                {
                    reply(&self.sockets.source, response.peer, &response.bytes)?;
                    self.summary.source_replies += 1;
                }
            }
            Channel::SourceMedia(index) => {
                self.source.keepalive(
                    &self.endpoint,
                    index,
                    &datagram,
                    self.clock.current().as_ref(),
                    self.time.now(),
                )?;
            }
            Channel::ReceiveMedia(index) => {
                self.receive.media(
                    index,
                    &datagram,
                    self.clock.current().as_ref(),
                    &mut self.endpoint,
                )?;
            }
        }

        Ok(())
    }

    fn prepare_status(&mut self) -> Result<()> {
        let pending = self.pending_status.pop_front().context("status request has retired")?;
        let mut provider = self.counters.take().context("statistics provider is busy")?;
        let time = self.time.clone();
        self.status_job = Some(tokio::task::spawn_blocking(move || {
            let statistics = provider.measure(time.now());
            StatusObservation {
                request: pending.request,
                provider,
                statistics,
            }
        }));
        Ok(())
    }

    fn status_ready(&mut self, observation: StatusObservation) -> Result<()> {
        self.status_job.take();
        self.counters = Some(observation.provider);
        let statistics = observation.statistics?;
        let snapshot = self.clock.current().snapshot(self.time.now())?;
        let context = StatusContext {
            interface: &self.interface,
            clock: &snapshot,
            local_clock_identity: self.configuration.local_clock_identity,
            statistics: statistics.as_ref(),
            software_version: SOFTWARE_VERSION,
        };

        if let Some(body) = monitoring::answer_status_query(&observation.request, &context)? {
            let packet = self.monitoring.publication(&body)?;
            let multicast = self.configuration.status_publication.multicast_destination();
            let subscriptions = self.monitoring.destinations(self.time.now());

            for destination in multicast.into_iter().chain(subscriptions) {
                match reply(&self.sockets.monitoring, destination, &packet) {
                    Ok(()) => {
                        if Some(destination) == multicast {
                            self.summary.multicast_status_sends += 1;
                        } else {
                            self.summary.unicast_status_sends += 1;
                        }
                    }
                    Err(error) => {
                        self.summary.status_send_failures += 1;
                        eprintln!("status publication to {destination} failed: {error:#}");
                    }
                }
            }
        }

        Ok(())
    }

    fn audio_event(&mut self, event: crate::audio::Event) -> Result<()> {
        eprintln!("native audio interrupted: {}", serde_json::to_string(&event)?);
        self.source.suspend(&mut self.endpoint)?;
        self.receive.suspend_output(&mut self.endpoint, &event.fault.to_string())?;
        self.summary.media_failures += 1;
        self.audio.as_mut().context("native audio provider disappeared")?.recover(event)?;
        eprintln!(
            "native audio recovery armed; media requires fresh capture and output consumption"
        );
        Ok(())
    }

    async fn serve(&mut self, stopped: impl std::future::Future<Output = ()>) -> Result<()> {
        let monitor = self.publication.daemon.monitor()?;
        let mut media_deadline = Deadline::new(self.time.clone())?;
        let mut receive_deadline = Deadline::new(self.time.clone())?;
        let mut status_deadline = Deadline::new(self.time.clone())?;
        tokio::pin!(stopped);
        let clock = self.clock.current().snapshot(self.time.now())?;
        self.service.ready(clock.usable, &clock.reason)?;

        loop {
            if let Some(event) = self.audio.as_ref().and_then(crate::audio::Engine::pending_event) {
                self.audio_event(event)?;
                continue;
            }

            if let Err(error) = self.receive.synchronize(
                &mut self.endpoint,
                self.receiver_discovery.candidate(),
                self.receiver_acquisition.progress(self.receiver_discovery.candidate()),
                self.clock.current().as_ref(),
            ) {
                self.receive.summary.unavailable = Some(format!("{error:#}"));
                eprintln!("receiver reconciliation failed: {error:#}");
            }

            let receive_due = match self.receive.deadline(self.clock.current().as_ref()) {
                Ok(due) => due,
                Err(error) => {
                    self.receive.retire(&mut self.endpoint, &format!("{error:#}"))?;
                    None
                }
            };
            receive_deadline.arm(receive_due)?;
            let due = match self.source.deadline(self.clock.current().as_ref(), self.time.now()) {
                Ok(due) => due,
                Err(error) => {
                    eprintln!("source output suspended: {error:#}");
                    self.summary.media_failures += 1;
                    self.source.suspend(&mut self.endpoint)?;
                    None
                }
            };
            media_deadline.arm(due)?;
            let status_due = self
                .pending_status
                .front()
                .filter(|_| self.status_job.is_none())
                .map(|pending| pending.due);
            status_deadline.arm(status_due)?;

            tokio::select! {
                biased;
                _ = &mut stopped => return Ok(()),
                event = async {
                    match &self.audio {
                        Some(audio) => audio.next_event().await,
                        None => std::future::pending().await,
                    }
                } => self.audio_event(event)?,
                result = receive_deadline.expired(), if receive_due.is_some() => {
                    result?;

                    if let Err(error) = self.receive.dispatch(&mut self.endpoint, self.clock.current().as_ref()) {
                        eprintln!("receive output suspended: {error:#}");
                        if let Some(event) = self.audio.as_ref().and_then(crate::audio::Engine::pending_event) {
                            self.audio_event(event)?;
                        } else {
                            self.receive.retire(&mut self.endpoint, &format!("{error:#}"))?;
                        }
                    }
                }
                result = media_deadline.expired(), if due.is_some() => {
                    result?;

                    if let Err(error) = self.source.send(&mut self.endpoint, self.clock.current().as_ref(), &self.time) {
                        eprintln!("source output suspended: {error:#}");
                        self.summary.media_failures += 1;
                        self.source.suspend(&mut self.endpoint)?;
                    }
                }
                result = self.clock.changed() => {
                    result?;
                    let clock = self.clock.current().snapshot(self.time.now())?;
                    self.service.status(clock.usable, &clock.reason)?;
                }
                result = self.receiver_discovery.changed() => {
                    result?;
                    self.receiver_acquisition.synchronize(self.receiver_discovery.candidate());
                }
                _ = self.receiver_acquisition.changed() => {}
                event = self.receive.next_event() => {
                    match event {
                        Ok(event) => {
                            if let Err(error) = self.receive.event(event, &mut self.endpoint) {
                                self.receive.summary.unavailable = Some(format!("{error:#}"));
                                eprintln!("receive control event failed: {error:#}");
                            }
                        }
                        Err(error) => {
                            if let Some(event) = self.audio.as_ref().and_then(crate::audio::Engine::pending_event) {
                                self.audio_event(event)?;
                            } else {
                                self.receive.retire(&mut self.endpoint, &format!("receive provider failed: {error:#}"))?;
                                return Err(error);
                            }
                        }
                    }
                }
                result = self.source.next_event(&mut self.endpoint, &self.time) => {
                    match result {
                        Ok(Some(response)) => {
                            reply(&self.sockets.source, response.peer, &response.bytes)?;
                            self.summary.source_replies += 1;
                        }
                        Ok(None) => {}
                        Err(error) => eprintln!("source worker failed: {error:#}"),
                    }
                }
                datagram = self.datagrams.recv() => {
                    let datagram = datagram.context("endpoint receive provider stopped")?;
                    let channel = datagram.channel;

                    if let Err(error) = self.datagram(datagram) {
                        self.summary.rejected_datagrams += 1;
                        eprintln!("endpoint {channel:?} datagram rejected: {error:#}");
                    }
                }
                event = monitor.recv_async() => self.publication.check_event(event?)?,
                result = status_deadline.expired(), if status_due.is_some() => {
                    result?;
                    self.prepare_status()?;
                }
                observation = async {
                    match &mut self.status_job {
                        Some(job) => job.await,
                        None => std::future::pending().await,
                    }
                } => {
                    let observation = observation?;

                    if let Err(error) = self.status_ready(observation) {
                        self.summary.rejected_datagrams += 1;
                        eprintln!("status preparation failed: {error:#}");
                    }
                }
                result = self.interface_observer.changed() => {
                    let observed = result?;
                    self.source.suspend(&mut self.endpoint)?;
                    bail!("selected network interface changed from {:?} to {observed:?}; endpoint identity must be re-established", self.interface);
                }
                reader = self.readers.join_next() => {
                    let (channel, result) = reader.context("endpoint socket readers stopped")??;
                    let reason = result.err().map_or_else(|| "reader stopped".to_owned(), |error| format!("{error:#}"));

                    match channel {
                        Channel::SourceMedia(index) => {
                            if self.source.flow.as_ref().is_some_and(|flow| flow.port_index == index) {
                                self.source.suspend(&mut self.endpoint)?;
                            }
                            self.source.pool.disable(index)?;
                            self.summary.media_failures += 1;
                            eprintln!("source media port retired: {reason}");
                        }
                        Channel::ReceiveMedia(index) => {
                            self.receive.retire(&mut self.endpoint, &format!("receive media port failed: {reason}"))?;
                            self.receive.pool.disable(index)?;
                            self.summary.media_failures += 1;
                        }
                        _ => bail!("endpoint {channel:?} provider failed: {reason}"),
                    }
                }
            }
        }
    }

    async fn close(mut self) -> Result<Summary> {
        self.readers.abort_all();

        while let Some(result) = self.readers.join_next().await {
            if let Err(error) = result
                && !error.is_cancelled()
            {
                eprintln!("socket reader failed during shutdown: {error}");
            }
        }

        if let Some(job) = self.status_job.take() {
            job.await?;
        }

        self.summary.submitted_packets = self.source.submitted_packets;
        self.summary.maximum_submission_lateness_nanoseconds =
            self.source.maximum_submission_lateness_nanoseconds;
        let final_clock = self.clock.current().snapshot(self.time.now());
        self.summary.receiver_candidate = self.receiver_discovery.candidate().cloned();
        self.summary.receiver_acquisition =
            self.receiver_acquisition.progress(self.receiver_discovery.candidate()).cloned();
        self.summary.receiver_unavailable =
            self.receiver_discovery.unavailable().map(str::to_owned);
        let source = self.source.close(&mut self.endpoint, self.time.now()).await;
        let receive = self.receive.close(&mut self.endpoint).await;
        let clock = self.clock.close().await;
        let publication = self.publication.stop().await;
        self.summary.final_endpoint = Some(self.endpoint);
        self.summary.audio = self.audio.take().map(crate::audio::Engine::close).transpose()?;
        source?;
        self.summary.receive = Some(receive?);
        self.summary.maximum_clock_observation_nanoseconds = clock?.maximum_observation_nanoseconds;
        publication?;
        self.summary.final_clock = Some(final_clock?);
        Ok(self.summary)
    }
}

pub async fn run(
    configuration: Configuration,
    stopped: impl std::future::Future<Output = ()>,
) -> Result<Summary> {
    let mut server = Server::start(configuration).await?;
    let result = server.serve(stopped).await;
    let notification = server.service.stopping(result.as_ref().err());
    let summary = server.close().await;
    result?;
    notification?;
    summary
}
