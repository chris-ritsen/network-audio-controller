use std::collections::{BTreeSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket as StandardSocket};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use if_addrs::IfAddr;
use mdns_sd::{IfKind, ServiceDaemon};
use netaudio_core::native_audio::clock::discipline::Snapshot;
use netaudio_core::native_audio::clock::version_one::{ETHERNET_TECHNOLOGY, PortIdentity};
use netaudio_core::native_audio::endpoint_control::monitoring::{
    self, Identity, Interface, Manager, StatusContext, StatusQuery,
};
use netaudio_core::native_audio::endpoint_control::source::{self, Action};
use netaudio_core::native_audio::endpoint_control::{Endpoint, Profile};
use netaudio_core::protocol::MULTICAST_GROUP_HEARTBEAT;
use netaudio_endpoint::advertisement::Advertisement;
use socket2::SockRef;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;

const LINK_SPEED_MEGABITS_PER_SECOND: u32 = 1000;
const LOCAL_MULTICAST_TIME_TO_LIVE: u32 = 1;
const MAXIMUM_DATAGRAM_BYTES: usize = 2048;
const MAXIMUM_DEVICES: u16 = 16;
const MAXIMUM_FLOW_SLOTS: u16 = 8;
const READINESS_TIMEOUT: Duration = Duration::from_secs(10);
const RECEIVE_QUEUE_DATAGRAMS: usize = 256;
const SOFTWARE_VERSION: u32 = 0x0001_0000;
const STATUS_QUEUE_REQUESTS: usize = 64;

pub struct Options {
    pub address: Option<Ipv4Addr>,
    pub count: u16,
    pub manufacturer: String,
    pub model: String,
    pub name: String,
    pub ownership_token: Option<String>,
    pub readiness_port: Option<u16>,
    pub receive_channels: u16,
    pub sample_rate: u32,
    pub transmit_channels: u16,
}

#[derive(Clone, Copy)]
enum Channel {
    Control,
    Monitoring,
    Routing,
    Source,
}

struct Received {
    bytes: Vec<u8>,
    channel: Channel,
    peer: SocketAddrV4,
}

struct PendingStatus {
    due: Instant,
    request: Vec<u8>,
}

struct Sockets {
    control: Arc<UdpSocket>,
    monitoring: Arc<UdpSocket>,
    routing: Arc<UdpSocket>,
    source: Arc<UdpSocket>,
}

struct Device {
    clock: Snapshot,
    endpoint: Endpoint,
    identity: Identity,
    interface: Interface,
    manufacturer: String,
    model: String,
    monitoring: Manager,
    pending_status: VecDeque<PendingStatus>,
    readers: JoinSet<()>,
    reported: BTreeSet<&'static str>,
    sockets: Sockets,
    started: Instant,
}

#[derive(serde::Serialize)]
struct Summary {
    mac: String,
    name: String,
    ports: Vec<u16>,
}

#[derive(serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Readiness<'a> {
    Error {
        error: String,
        pid: u32,
        token: Option<&'a str>,
    },
    Ready {
        devices: &'a [Summary],
        pid: u32,
        token: Option<&'a str>,
    },
}

struct Running {
    discovery: ServiceDaemon,
    summaries: Vec<Summary>,
    tasks: JoinSet<Result<()>>,
}

pub async fn run(options: Options) -> Result<()> {
    let token = options.ownership_token.as_deref();
    let pid = std::process::id();
    let mut running = match start(&options).await {
        Ok(running) => running,
        Err(error) => {
            if let Some(port) = options.readiness_port {
                let report = Readiness::Error {
                    error: format!("{error:#}"),
                    pid,
                    token,
                };

                if let Err(unreported) = report_readiness(port, &report).await {
                    eprintln!("could not report the startup failure: {unreported:#}");
                }
            }

            return Err(error);
        }
    };

    if let Some(port) = options.readiness_port {
        let report = Readiness::Ready {
            devices: &running.summaries,
            pid,
            token,
        };
        report_readiness(port, &report).await?;
    }

    println!(
        "Virtual device '{}' running ({} TX, {} RX, {} Hz)",
        options.name, options.transmit_channels, options.receive_channels, options.sample_rate
    );

    for summary in &running.summaries {
        println!(
            "  {}: MAC {}, ports {}",
            summary.name,
            summary.mac,
            summary.ports.iter().map(u16::to_string).collect::<Vec<_>>().join(", ")
        );
    }

    let outcome = tokio::select! {
        stopped = stop_requested() => stopped,
        finished = running.tasks.join_next() => match finished {
            Some(Ok(result)) => result.and_then(|()| bail!("a simulated device stopped")),
            Some(Err(error)) => Err(error).context("a simulated device task failed"),
            None => Ok(()),
        },
    };
    running.tasks.shutdown().await;

    if let Ok(stopped) = running.discovery.shutdown() {
        let _ = stopped.recv_timeout(Duration::from_secs(2));
    }

    outcome
}

async fn start(options: &Options) -> Result<Running> {
    if options.count == 0 || options.count > MAXIMUM_DEVICES {
        bail!("count must be from 1 through {MAXIMUM_DEVICES}");
    }

    monitoring::validate_product(&options.manufacturer, &options.model)?;
    let mut profile = Profile {
        flow_slots: options.transmit_channels.max(options.receive_channels).min(MAXIMUM_FLOW_SLOTS),
        receive_channels: options.receive_channels,
        sample_rate: options.sample_rate,
        transmit_channels: options.transmit_channels,
        ..Profile::default()
    };
    profile.frames_per_packet = profile.minimum_frames_per_packet();
    profile.validate()?;
    let address = match options.address {
        Some(address) => address,
        None => routed_address()?,
    };
    let network = network(address)?;
    let discovery = ServiceDaemon::new().context("start discovery for the simulated devices")?;
    discovery.disable_interface(IfKind::All)?;
    discovery.enable_interface(IfKind::Addr(IpAddr::V4(address)))?;
    let mut summaries = Vec::new();
    let mut tasks = JoinSet::new();

    for index in 0..options.count {
        let name = if index == 0 {
            options.name.clone()
        } else {
            format!("{}-{}", options.name, index + 1)
        };
        let (device, requests) = Device::open(options, profile, name, &network, index).await?;

        for service in device.advertisement()?.services()? {
            let name = service.get_fullname().to_owned();
            discovery.register(service).with_context(|| format!("advertise {name}"))?;
        }

        summaries.push(Summary {
            mac: hardware_address_text(device.interface.hardware_address),
            name: device.endpoint.name.clone(),
            ports: device.ports()?,
        });
        tasks.spawn(device.serve(requests));
    }

    Ok(Running {
        discovery,
        summaries,
        tasks,
    })
}

async fn report_readiness(port: u16, report: &Readiness<'_>) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut message = serde_json::to_vec(report)?;
    message.push(b'\n');
    let mut connection = tokio::time::timeout(
        READINESS_TIMEOUT,
        tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port)),
    )
    .await
    .context("report readiness to the starting command")??;
    connection.write_all(&message).await?;
    connection.shutdown().await?;
    Ok(())
}

async fn stop_requested() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut termination = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = termination.recv() => {}
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("wait for an interrupt")
    }
}

fn routed_address() -> Result<Ipv4Addr> {
    let probe = StandardSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    let group: Ipv4Addr = MULTICAST_GROUP_HEARTBEAT.parse()?;
    probe.connect((group, 1)).context("find the address that reaches the Dante network")?;

    match probe.local_addr()? {
        SocketAddr::V4(local) => Ok(*local.ip()),
        SocketAddr::V6(_) => bail!("the route to the Dante network is not IPv4"),
    }
}

fn network(address: Ipv4Addr) -> Result<Interface> {
    for interface in if_addrs::get_if_addrs().context("enumerate network interfaces")? {
        let IfAddr::V4(assigned) = &interface.addr else {
            continue;
        };

        if assigned.ip == address {
            return Ok(Interface {
                address,
                gateway: Ipv4Addr::UNSPECIFIED,
                hardware_address: [0; 6],
                index: interface.index.unwrap_or(0),
                link_up: true,
                name_server: Ipv4Addr::UNSPECIFIED,
                netmask: assigned.netmask,
                speed_megabits_per_second: Some(LINK_SPEED_MEGABITS_PER_SECOND),
            });
        }
    }

    bail!("{address} is not an address of this computer")
}

fn hardware_address_text(address: [u8; 6]) -> String {
    address.iter().map(|octet| format!("{octet:02x}")).collect::<Vec<_>>().join(":")
}

async fn bind(address: Ipv4Addr) -> Result<Arc<UdpSocket>> {
    Ok(Arc::new(
        UdpSocket::bind(SocketAddrV4::new(address, 0))
            .await
            .with_context(|| format!("open a control socket on {address}"))?,
    ))
}

fn port(socket: &UdpSocket) -> Result<u16> {
    Ok(socket.local_addr()?.port())
}

async fn receive(socket: Arc<UdpSocket>, channel: Channel, sender: mpsc::Sender<Received>) {
    let mut buffer = [0; MAXIMUM_DATAGRAM_BYTES];

    loop {
        let Ok((length, SocketAddr::V4(peer))) = socket.recv_from(&mut buffer).await else {
            continue;
        };
        let received = Received {
            bytes: buffer[..length].to_vec(),
            channel,
            peer,
        };

        if sender.send(received).await.is_err() {
            return;
        }
    }
}

impl Device {
    async fn open(
        options: &Options,
        profile: Profile,
        name: String,
        network: &Interface,
        index: u16,
    ) -> Result<(Self, mpsc::Receiver<Received>)> {
        let address = network.address.octets();
        let process = index.to_be_bytes();
        let identity = Identity {
            instance: [
                0, 0, address[0], address[1], address[2], address[3], process[0], process[1],
            ],
            process: index,
        };
        let interface = Interface {
            hardware_address: [0x02, process[1], address[0], address[1], address[2], address[3]],
            ..network.clone()
        };
        let endpoint = Endpoint::with_profile(name.clone(), profile)?;
        let sockets = Sockets {
            control: bind(network.address).await?,
            monitoring: bind(network.address).await?,
            routing: bind(network.address).await?,
            source: bind(network.address).await?,
        };
        let publication = SockRef::from(sockets.monitoring.as_ref());
        publication.set_multicast_if_v4(&network.address)?;
        publication.set_multicast_ttl_v4(LOCAL_MULTICAST_TIME_TO_LIVE)?;
        publication.set_multicast_loop_v4(true)?;
        let monitoring = Manager::new(
            identity,
            name,
            SocketAddrV4::new(network.address, port(&sockets.control)?),
        )?;
        let (sender, requests) = mpsc::channel(RECEIVE_QUEUE_DATAGRAMS);
        let mut readers = JoinSet::new();

        for (channel, socket) in [
            (Channel::Control, &sockets.control),
            (Channel::Monitoring, &sockets.monitoring),
            (Channel::Routing, &sockets.routing),
            (Channel::Source, &sockets.source),
        ] {
            readers.spawn(receive(Arc::clone(socket), channel, sender.clone()));
        }

        let device = Self {
            clock: Snapshot {
                complete_observations: 0,
                estimated_error_nanoseconds: None,
                fitted_rate_parts_per_billion: None,
                generation: 0,
                last_dataset: None,
                last_fit_rejection: None,
                network_time: None,
                parent_identity: PortIdentity {
                    communication_technology: ETHERNET_TECHNOLOGY,
                    identifier: [0; 6],
                    port: 0,
                },
                reason: "a simulated device follows no clock".to_owned(),
                usable: false,
            },
            endpoint,
            identity,
            interface,
            manufacturer: options.manufacturer.clone(),
            model: options.model.clone(),
            monitoring,
            pending_status: VecDeque::new(),
            readers,
            reported: BTreeSet::new(),
            sockets,
            started: Instant::now(),
        };

        Ok((device, requests))
    }

    fn advertisement(&self) -> Result<Advertisement<'_>> {
        Ok(Advertisement {
            address: self.interface.address,
            endpoint: &self.endpoint,
            identity: self.identity,
            manufacturer: &self.manufacturer,
            model: &self.model,
            monitoring_port: port(&self.sockets.monitoring)?,
            routing_port: port(&self.sockets.routing)?,
            source_port: port(&self.sockets.source)?,
        })
    }

    fn ports(&self) -> Result<Vec<u16>> {
        Ok(vec![
            port(&self.sockets.routing)?,
            port(&self.sockets.monitoring)?,
            port(&self.sockets.control)?,
            port(&self.sockets.source)?,
        ])
    }

    fn now(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    async fn serve(mut self, mut requests: mpsc::Receiver<Received>) -> Result<()> {
        loop {
            let due = self.pending_status.front().map(|pending| pending.due);

            tokio::select! {
                received = requests.recv() => {
                    let Some(received) = received else {
                        return Ok(());
                    };

                    if let Err(error) = self.respond(received).await {
                        self.report(error);
                    }
                }
                _ = tokio::time::sleep_until(due.unwrap_or_else(Instant::now)), if due.is_some() => {
                    if let Err(error) = self.publish_status().await {
                        self.report(error);
                    }
                }
                Some(stopped) = self.readers.join_next() => {
                    stopped.context("a control socket reader stopped")?;
                    bail!("a control socket reader stopped");
                }
            }
        }
    }

    fn report(&mut self, error: netaudio_core::native_audio::source_control::Error) {
        if self.reported.insert(error.0) {
            eprintln!("{}: ignored a request: {error}", self.endpoint.name);
        }
    }

    async fn respond(
        &mut self,
        received: Received,
    ) -> Result<(), netaudio_core::native_audio::source_control::Error> {
        let (socket, reply) = match received.channel {
            Channel::Control => {
                if let Some(query) = StatusQuery::decode(&received.bytes)?
                    && self.pending_status.len() < STATUS_QUEUE_REQUESTS
                {
                    let pending = PendingStatus {
                        due: Instant::now() + Duration::from_nanos(query.delay_nanoseconds()),
                        request: received.bytes,
                    };
                    let position =
                        self.pending_status.partition_point(|request| request.due <= pending.due);
                    self.pending_status.insert(position, pending);
                }

                return Ok(());
            }
            Channel::Monitoring => {
                (&self.sockets.monitoring, self.monitoring.answer(&received.bytes, self.now())?)
            }
            Channel::Routing => (
                &self.sockets.routing,
                self.endpoint.answer(&received.bytes)?.map(|answer| answer.bytes),
            ),
            Channel::Source => (
                &self.sockets.source,
                match source::admit(&self.endpoint, &received.bytes)? {
                    Some(Action::Reply(bytes)) => Some(bytes),
                    Some(
                        Action::Allocate {
                            header,
                            ..
                        }
                        | Action::Remap {
                            header,
                            ..
                        }
                        | Action::Stop {
                            header,
                            ..
                        },
                    ) => Some(source::unavailable(header)?),
                    None => None,
                },
            ),
        };

        if let Some(reply) = reply {
            let _ = socket.send_to(&reply, received.peer).await;
        }

        Ok(())
    }

    async fn publish_status(
        &mut self,
    ) -> Result<(), netaudio_core::native_audio::source_control::Error> {
        let Some(pending) = self.pending_status.pop_front() else {
            return Ok(());
        };
        let context = StatusContext {
            clock: &self.clock,
            interface: &self.interface,
            local_clock_identity: PortIdentity {
                communication_technology: ETHERNET_TECHNOLOGY,
                identifier: self.interface.hardware_address,
                port: 1,
            },
            manufacturer: &self.manufacturer,
            model: &self.model,
            software_version: SOFTWARE_VERSION,
            statistics: None,
        };
        let Some(body) = monitoring::answer_status_query(&pending.request, &context)? else {
            return Ok(());
        };
        let packet = self.monitoring.publication(&body)?;
        let now = self.now();
        let destinations = std::iter::once(monitoring::CLASS_TWO_MULTICAST_DESTINATION)
            .chain(self.monitoring.destinations(now));

        for destination in destinations {
            let _ = self.sockets.monitoring.send_to(&packet, destination).await;
        }

        Ok(())
    }
}
