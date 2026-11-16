use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, Hasher};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket as StandardSocket};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use mdns_sd::{IfKind, ServiceDaemon, ServiceInfo};
use netaudio_core::commands::{self, AudioConfigurationRequest, ChannelType, MeteringRequest};
use netaudio_core::device_requests::{DeviceRequest, SettingsRequest, parse_device_request};
use netaudio_core::discovery::{VirtualDeviceAdvertisement, virtual_device};
use netaudio_core::parser::ChannelAudioConfiguration;
use netaudio_core::protocol::{
    ArcOperation, ControlRequestHeader, MULTICAST_GROUP_CONTROL_MONITORING,
    MULTICAST_GROUP_HEARTBEAT, NetaudioPort, RESULT_CODE_SUCCESS,
};
use netaudio_core::publications::{
    ArcResponse, ArcResponseBody, AudioCapabilityKind, ChannelDirection, ChannelSource,
    Publication, PublicationBody, PublishedChannel, next_publication_id,
};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;

const CO_HOSTED_PORT_BASE: u16 = 24500;
const CO_HOSTED_PORT_STRIDE: u16 = 10;
const DEFAULT_LATENCY_NANOSECONDS: u32 = 1_000_000;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const MAXIMUM_DATAGRAM_BYTES: usize = 2048;
const MAXIMUM_DEVICES: u16 = 16;
const MAXIMUM_LATENCY_NANOSECONDS: u32 = 21_333_334;
const METERING_INTERVAL: Duration = Duration::from_millis(100);
const METERING_LEASE: Duration = Duration::from_secs(10);
const MINIMUM_LATENCY_NANOSECONDS: u32 = 150_000;
const READINESS_TIMEOUT: Duration = Duration::from_secs(10);
const SUPPORTED_ENCODINGS: [u16; 3] = [24, 16, 32];

pub struct Options {
    pub address: Option<Ipv4Addr>,
    pub count: u16,
    pub detailed_metering: bool,
    pub manufacturer: String,
    pub model: String,
    pub name: String,
    pub ownership_token: Option<String>,
    pub readiness_port: Option<u16>,
    pub receive_channels: u16,
    pub sample_rate: u32,
    pub transmit_channels: u16,
}

struct Ports {
    arc: u16,
    control: u16,
    settings: u16,
}

struct Received {
    data: Vec<u8>,
    peer: SocketAddr,
    reply: Arc<UdpSocket>,
    settings_only: bool,
}

struct Device {
    active_latency: u32,
    address: Ipv4Addr,
    configured_latency: u32,
    detailed_metering: bool,
    encoding: u16,
    hardware_address: [u8; 6],
    manufacturer: String,
    metering: BTreeMap<(String, u16), (SocketAddrV4, Instant)>,
    metering_sequence: u16,
    metering_socket: UdpSocket,
    model: String,
    multicast: Option<Arc<UdpSocket>>,
    name: String,
    ports: Ports,
    process: u16,
    publication_sequence: u16,
    receive_channels: Vec<String>,
    sample_rate: u32,
    started: Instant,
    subscriptions: BTreeMap<u16, (String, String)>,
    transmit_channels: Vec<String>,
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

    let address = match options.address {
        Some(address) => address,
        None => routed_address()?,
    };
    let discovery = ServiceDaemon::new().context("start discovery for the simulated devices")?;
    discovery.disable_interface(IfKind::All)?;
    discovery.enable_interface(IfKind::Addr(IpAddr::V4(address)))?;
    let mut summaries = Vec::new();
    let mut tasks = JoinSet::new();

    for index in 0..options.count {
        let ports = if index == 0 {
            Ports {
                arc: NetaudioPort::Arc as u16,
                control: NetaudioPort::Control as u16,
                settings: NetaudioPort::Settings as u16,
            }
        } else {
            let base = CO_HOSTED_PORT_BASE + index * CO_HOSTED_PORT_STRIDE;
            Ports {
                arc: base,
                control: base + 1,
                settings: base + 3,
            }
        };
        let name = if index == 0 {
            options.name.clone()
        } else {
            format!("{}-{}", options.name, index + 1)
        };
        let (device, requests, listening) =
            Device::open(options, name, address, index, ports).await?;
        device.advertise(&discovery)?;
        summaries.push(Summary {
            mac: hardware_address_text(device.hardware_address),
            name: device.name.clone(),
            ports: listening,
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

fn hardware_address_text(address: [u8; 6]) -> String {
    address.iter().map(|octet| format!("{octet:02x}")).collect::<Vec<_>>().join(":")
}

fn random_hardware_address() -> [u8; 6] {
    let random = RandomState::new().build_hasher().finish().to_be_bytes();
    [0x02, random[0], random[1], random[2], random[3], random[4]]
}

fn multicast_socket(address: Ipv4Addr, port: u16) -> Result<UdpSocket> {
    let group: Ipv4Addr = MULTICAST_GROUP_CONTROL_MONITORING.parse()?;
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_multicast_ttl_v4(255)?;
    socket.set_multicast_if_v4(&address)?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port).into())?;
    socket.join_multicast_v4(&group, &address)?;
    socket.set_nonblocking(true)?;
    Ok(UdpSocket::from_std(socket.into())?)
}

fn metering_socket(address: Ipv4Addr) -> Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_multicast_ttl_v4(1)?;
    socket.set_multicast_if_v4(&address)?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into())?;
    socket.set_nonblocking(true)?;
    Ok(UdpSocket::from_std(socket.into())?)
}

fn listen(socket: Arc<UdpSocket>, settings_only: bool, requests: mpsc::Sender<Received>) {
    tokio::spawn(async move {
        let mut buffer = vec![0; MAXIMUM_DATAGRAM_BYTES];

        while let Ok((length, peer)) = socket.recv_from(&mut buffer).await {
            let received = Received {
                data: buffer[..length].to_vec(),
                peer,
                reply: Arc::clone(&socket),
                settings_only,
            };

            if requests.send(received).await.is_err() {
                return;
            }
        }
    });
}

fn acknowledgement(data: &[u8], accepted: bool) -> ArcResponseBody {
    ArcResponseBody::Acknowledgement {
        request: data.to_vec(),
        accepted,
    }
}

impl Device {
    async fn open(
        options: &Options,
        name: String,
        address: Ipv4Addr,
        process: u16,
        ports: Ports,
    ) -> Result<(Self, mpsc::Receiver<Received>, Vec<u16>)> {
        let (sender, requests) = mpsc::channel(64);
        let mut listening = Vec::new();
        let mut responders = vec![ports.arc];

        if ports.arc == NetaudioPort::Arc as u16 {
            responders.push(NetaudioPort::ArcSecondary as u16);
        }

        responders.push(ports.control);

        for port in responders {
            match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)).await {
                Ok(socket) => {
                    listening.push(port);
                    listen(Arc::new(socket), false, sender.clone());
                }
                Err(error) => eprintln!("{name}: could not listen on port {port}: {error}"),
            }
        }

        let multicast = match multicast_socket(address, ports.settings) {
            Ok(socket) => {
                let socket = Arc::new(socket);
                listening.push(ports.settings);
                listen(Arc::clone(&socket), true, sender.clone());
                Some(socket)
            }
            Err(error) => {
                eprintln!("{name}: could not open the settings port {}: {error}", ports.settings);
                None
            }
        };
        let channels = |count: u16| (1..=count).map(|number| format!("Ch {number}")).collect();
        let device = Self {
            active_latency: DEFAULT_LATENCY_NANOSECONDS,
            address,
            configured_latency: DEFAULT_LATENCY_NANOSECONDS,
            detailed_metering: options.detailed_metering,
            encoding: SUPPORTED_ENCODINGS[0],
            hardware_address: random_hardware_address(),
            manufacturer: options.manufacturer.clone(),
            metering: BTreeMap::new(),
            metering_sequence: 0,
            metering_socket: metering_socket(address)?,
            model: options.model.clone(),
            multicast,
            name,
            ports,
            process,
            publication_sequence: 1,
            receive_channels: channels(options.receive_channels),
            sample_rate: options.sample_rate,
            started: Instant::now(),
            subscriptions: BTreeMap::new(),
            transmit_channels: channels(options.transmit_channels),
        };

        Ok((device, requests, listening))
    }

    fn advertise(&self, discovery: &ServiceDaemon) -> Result<()> {
        let advertisements = virtual_device(VirtualDeviceAdvertisement {
            name: self.name.clone(),
            model: self.model.clone(),
            manufacturer: self.manufacturer.clone(),
            address: self.address,
            arc_port: self.ports.arc,
            control_port: Some(self.ports.control),
            process_id: self.process,
            tx_channels: self.transmit_channels.clone(),
            sample_rate: self.sample_rate,
            encoding: self.encoding,
            supported_encodings: SUPPORTED_ENCODINGS.to_vec(),
            configured_latency_ns: self.configured_latency,
        });

        for advertisement in advertisements {
            let instance = advertisement
                .name
                .strip_suffix(&format!(".{}", advertisement.service_type))
                .unwrap_or(&advertisement.name)
                .to_owned();
            let properties: HashMap<String, String> =
                advertisement.properties.into_iter().collect();
            let service = ServiceInfo::new(
                &advertisement.service_type,
                &instance,
                &advertisement.server,
                IpAddr::V4(Ipv4Addr::from(advertisement.address)),
                advertisement.port,
                properties,
            )?;
            discovery
                .register(service)
                .with_context(|| format!("advertise {}", advertisement.name))?;
        }

        Ok(())
    }

    async fn serve(mut self, mut requests: mpsc::Receiver<Received>) -> Result<()> {
        let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
        let mut metering = tokio::time::interval(METERING_INTERVAL);
        self.publish_status(self.board_info()).await;
        self.publish_status(self.product_info()).await;

        loop {
            tokio::select! {
                received = requests.recv() => {
                    let Some(received) = received else {
                        return Ok(());
                    };

                    if let Some(response) = self.respond(&received).await? {
                        let _ = received.reply.send_to(&response, received.peer).await;
                    }
                }
                _ = heartbeat.tick() => self.send_heartbeat().await,
                _ = metering.tick(), if !self.metering.is_empty() => self.send_levels().await,
            }
        }
    }

    fn identity(&self) -> [u8; 8] {
        let address = self.address.octets();
        let process = self.process.to_be_bytes();
        [0, 0, address[0], address[1], address[2], address[3], process[0], process[1]]
    }

    fn board_info(&self) -> PublicationBody {
        PublicationBody::BoardInfo {
            name: self.name.clone(),
            detailed_metering: self.detailed_metering,
        }
    }

    fn product_info(&self) -> PublicationBody {
        PublicationBody::ProductInfo {
            name: self.name.clone(),
            manufacturer: self.manufacturer.clone(),
            model: self.model.clone(),
        }
    }

    fn publication(&mut self, body: PublicationBody) -> Option<Vec<u8>> {
        let publication = Publication {
            device_id: Some(self.identity()),
            message_id: self.publication_sequence,
            process: self.process,
            publication: body,
            source_ip: self.address,
        };
        self.publication_sequence = next_publication_id(self.publication_sequence);

        match publication.packet() {
            Ok(packet) => Some(packet),
            Err(error) => {
                eprintln!("{}: could not build a status message: {error:?}", self.name);
                None
            }
        }
    }

    async fn publish(&mut self, body: PublicationBody, destination: SocketAddr) {
        let Some(packet) = self.publication(body) else {
            return;
        };

        if let Some(multicast) = &self.multicast {
            let _ = multicast.send_to(&packet, destination).await;
        }
    }

    async fn publish_status(&mut self, body: PublicationBody) {
        if let Ok(group) = MULTICAST_GROUP_CONTROL_MONITORING.parse::<Ipv4Addr>() {
            self.publish(body, SocketAddr::from((group, NetaudioPort::Info as u16))).await;
        }
    }

    async fn send_heartbeat(&mut self) {
        let body = PublicationBody::Heartbeat {
            tx_count: self.transmit_channels.len() as u16,
            rx_count: self.receive_channels.len() as u16,
        };

        if let Ok(group) = MULTICAST_GROUP_HEARTBEAT.parse::<Ipv4Addr>() {
            self.publish(body, SocketAddr::from((group, NetaudioPort::Heartbeat as u16))).await;
        }
    }

    fn levels(&self, count: usize, phase: f64, seconds: f64) -> Vec<u8> {
        (0..count)
            .map(|index| {
                let swing =
                    (seconds * 2.0 + index as f64 * 0.7 + phase + f64::from(self.process) * 1.3)
                        .sin();
                (1.0 + (2.0 * (30.0 - 24.0 * swing)).round()) as u8
            })
            .collect()
    }

    async fn send_levels(&mut self) {
        let now = Instant::now();
        self.metering.retain(|_, (_, expires)| *expires > now);

        if self.metering.is_empty() {
            return;
        }

        let seconds = self.started.elapsed().as_secs_f64();
        let publication = Publication {
            device_id: Some(self.identity()),
            message_id: self.metering_sequence,
            process: 0,
            publication: PublicationBody::Metering {
                tx_levels: self.levels(self.transmit_channels.len(), 0.0, seconds),
                rx_levels: self.levels(self.receive_channels.len(), std::f64::consts::PI, seconds),
            },
            source_ip: self.address,
        };
        self.metering_sequence = self.metering_sequence.wrapping_add(1);
        let Ok(packet) = publication.packet() else {
            return;
        };
        let mut destinations: Vec<SocketAddrV4> =
            self.metering.values().map(|(destination, _)| *destination).collect();
        destinations.sort();
        destinations.dedup();

        for destination in destinations {
            let _ = self.metering_socket.send_to(&packet, destination).await;
        }
    }

    async fn respond(&mut self, received: &Received) -> Result<Option<Vec<u8>>> {
        let Some(request) = parse_device_request(&received.data) else {
            return Ok(None);
        };

        match request {
            DeviceRequest::Metering {
                request,
            } => {
                if received.settings_only || !self.detailed_metering {
                    return Ok(None);
                }

                Ok(self.metering_reply(request, &received.data))
            }
            DeviceRequest::Settings {
                request,
            } => {
                self.settings(request, received.peer).await;
                Ok(None)
            }
            DeviceRequest::Control {
                header,
            } => {
                if received.settings_only {
                    return Ok(None);
                }

                Ok(self.control(&header, &received.data))
            }
        }
    }

    fn metering_reply(&mut self, request: MeteringRequest, data: &[u8]) -> Option<Vec<u8>> {
        let key = (request.owner.clone(), request.discriminator);

        match request.destination {
            Some(destination) => {
                self.metering.insert(
                    key,
                    (
                        SocketAddrV4::new(destination.ipv4, destination.port),
                        Instant::now() + METERING_LEASE,
                    ),
                );
            }
            None => {
                self.metering.remove(&key);
            }
        }

        self.response(
            commands::PROTOCOL_CMC,
            request.transaction_id,
            ArcResponseBody::Metering {
                request: data.to_vec(),
                responder_process: self.process,
                responder_identity: self.identity(),
            },
        )
    }

    async fn settings(&mut self, request: SettingsRequest, peer: SocketAddr) {
        match request {
            SettingsRequest::BoardInfo => {
                self.publish_status(self.board_info()).await;
                self.publish(self.board_info(), peer).await;
            }
            SettingsRequest::ProductInfo => {
                self.publish_status(self.product_info()).await;
                self.publish(self.product_info(), peer).await;
            }
            SettingsRequest::ClockStatus => {
                self.publish_status(PublicationBody::ClockStatus {
                    mac_address: self.hardware_address,
                })
                .await;
            }
            SettingsRequest::InterfaceStatus => {
                self.publish_status(PublicationBody::InterfaceStatus {
                    mac_address: self.hardware_address,
                })
                .await;
            }
            SettingsRequest::Audio {
                request,
            } => self.audio(request).await,
        }
    }

    async fn audio(&mut self, request: AudioConfigurationRequest) {
        let body = match request {
            AudioConfigurationRequest::SampleRate {
                value,
            } => {
                if value.is_some_and(|value| value != self.sample_rate) {
                    return;
                }

                PublicationBody::Audio {
                    capability: AudioCapabilityKind::SampleRate,
                    current_value: self.sample_rate,
                    supported_values: vec![self.sample_rate],
                }
            }
            AudioConfigurationRequest::Encoding {
                value,
            } => {
                if let Some(value) = value {
                    let Some(encoding) = SUPPORTED_ENCODINGS
                        .iter()
                        .copied()
                        .find(|supported| u32::from(*supported) == value)
                    else {
                        return;
                    };
                    self.encoding = encoding;
                }

                PublicationBody::Audio {
                    capability: AudioCapabilityKind::Encoding,
                    current_value: u32::from(self.encoding),
                    supported_values: SUPPORTED_ENCODINGS.iter().copied().map(u32::from).collect(),
                }
            }
        };
        self.publish_status(body).await;
    }

    fn response(&self, protocol: u16, transaction: u16, body: ArcResponseBody) -> Option<Vec<u8>> {
        let response = ArcResponse {
            protocol_id: protocol,
            transaction_id: transaction,
            result_code: RESULT_CODE_SUCCESS,
            response: body,
        };

        match response.packet() {
            Ok(packet) => Some(packet),
            Err(error) => {
                eprintln!("{}: could not build a reply: {error:?}", self.name);
                None
            }
        }
    }

    fn audio_configuration(&self) -> ChannelAudioConfiguration {
        ChannelAudioConfiguration {
            sample_rate: self.sample_rate,
            encoding: self.encoding,
            supported_encodings: SUPPORTED_ENCODINGS.to_vec(),
        }
    }

    fn control(&mut self, header: &ControlRequestHeader, data: &[u8]) -> Option<Vec<u8>> {
        let mut protocol = header.protocol_id;
        let body = match header.operation {
            ArcOperation::CmcRegistration => {
                protocol = commands::PROTOCOL_CMC;
                ArcResponseBody::CmcRegistration {
                    device_ip: self.address,
                    instance: Some(u64::from_be_bytes(self.identity())),
                    settings_port: NonZeroU16::new(self.ports.settings)?,
                }
            }
            ArcOperation::DeviceName => ArcResponseBody::DeviceName {
                name: self.name.clone(),
            },
            ArcOperation::ChannelCount => ArcResponseBody::ChannelCount {
                tx_count: self.transmit_channels.len() as u16,
                rx_count: self.receive_channels.len() as u16,
            },
            ArcOperation::DeviceInfo => ArcResponseBody::DeviceInfo {
                model_name: self.model.clone(),
                display_name: self.name.clone(),
                model_code: self.model.clone(),
                port: String::new(),
            },
            ArcOperation::TransmitterChannels => ArcResponseBody::ChannelStatus {
                channel_type: ChannelDirection::Tx,
                channels: self
                    .transmit_channels
                    .iter()
                    .map(|name| PublishedChannel {
                        name: name.clone(),
                        source: None,
                    })
                    .collect(),
                audio: self.audio_configuration(),
            },
            ArcOperation::TransmitterChannelNames => ArcResponseBody::TransmitterNames {
                names: self.transmit_channels.clone(),
            },
            ArcOperation::ReceiverChannels => ArcResponseBody::ChannelStatus {
                channel_type: ChannelDirection::Rx,
                channels: self
                    .receive_channels
                    .iter()
                    .zip(1u16..)
                    .map(|(name, number)| PublishedChannel {
                        name: name.clone(),
                        source: self.subscriptions.get(&number).map(|(channel, device)| {
                            ChannelSource {
                                channel: channel.clone(),
                                device: device.clone(),
                            }
                        }),
                    })
                    .collect(),
                audio: self.audio_configuration(),
            },
            ArcOperation::DeviceSettings => ArcResponseBody::DeviceSettings {
                sample_rate: self.sample_rate,
                default_latency_ns: DEFAULT_LATENCY_NANOSECONDS,
                configured_latency_ns: self.configured_latency,
                active_latency_ns: self.active_latency,
                maximum_latency_ns: MAXIMUM_LATENCY_NANOSECONDS,
                minimum_latency_ns: MINIMUM_LATENCY_NANOSECONDS,
            },
            ArcOperation::SetLatency => match commands::parse_set_latency_request(data) {
                Some(latency) => {
                    self.configured_latency = latency;
                    self.active_latency = latency;
                    ArcResponseBody::LatencyApplied {
                        latency_ns: latency,
                    }
                }
                None => acknowledgement(data, false),
            },
            ArcOperation::PropertyDirectory => ArcResponseBody::PropertyDirectory,
            ArcOperation::TransmitterFlows => ArcResponseBody::EmptyFlows {
                channel_type: ChannelDirection::Tx,
            },
            ArcOperation::ReceiverFlows => ArcResponseBody::EmptyFlows {
                channel_type: ChannelDirection::Rx,
            },
            ArcOperation::TransmitterFlowLabels => ArcResponseBody::EmptyTransmitterFlowLabels,
            ArcOperation::ReceiverPortRanges => ArcResponseBody::ReceiverPortRanges,
            ArcOperation::SetChannelName => {
                let accepted = self.rename_channel(data);
                acknowledgement(data, accepted)
            }
            ArcOperation::AddSubscriptions => {
                let accepted = self.subscribe(data);
                acknowledgement(data, accepted)
            }
            ArcOperation::RemoveSubscriptions => {
                let accepted = self.unsubscribe(data);
                acknowledgement(data, accepted)
            }
            _ => return None,
        };

        self.response(protocol, header.transaction_id, body)
    }

    fn rename_channel(&mut self, data: &[u8]) -> bool {
        let Some(change) = commands::parse_set_channel_name_request(data) else {
            return false;
        };
        let channels = match change.channel_type {
            ChannelType::Rx => &mut self.receive_channels,
            ChannelType::Tx => &mut self.transmit_channels,
        };
        let Some(channel) = usize::from(change.channel_number)
            .checked_sub(1)
            .and_then(|index| channels.get_mut(index))
        else {
            return false;
        };
        *channel = change.name;
        true
    }

    fn subscribe(&mut self, data: &[u8]) -> bool {
        let Some(subscriptions) = commands::parse_add_subscriptions_request(data) else {
            return false;
        };

        if subscriptions
            .iter()
            .any(|subscription| usize::from(subscription.rx_channel) > self.receive_channels.len())
        {
            return false;
        }

        for subscription in subscriptions {
            println!(
                "{}: receive channel {} subscribed to {}@{}",
                self.name, subscription.rx_channel, subscription.tx_channel, subscription.tx_device
            );
            self.subscriptions.insert(
                u16::from(subscription.rx_channel),
                (subscription.tx_channel, subscription.tx_device),
            );
        }

        true
    }

    fn unsubscribe(&mut self, data: &[u8]) -> bool {
        let Some(channels) = commands::parse_remove_subscriptions_request(data) else {
            return false;
        };

        if channels.iter().any(|channel| *channel as usize > self.receive_channels.len()) {
            return false;
        }

        for channel in channels {
            println!("{}: receive channel {channel} unsubscribed", self.name);
            self.subscriptions.remove(&(channel as u16));
        }

        true
    }
}
