use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use netaudio_core::native_audio::clock::discipline::Configuration;
use netaudio_core::native_audio::clock::version_one::{self, Body, Packet};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};

use super::clock_socket::{ClockSocket, Event};

const PROTOCOL_VERSION: u32 = 1;
const MAXIMUM_FRAME_BYTES: usize = 8192;
const MAXIMUM_CLIENTS: usize = 64;
const CLIENT_QUEUE_CAPACITY: usize = 64;
const REQUEST_DEADLINE: Duration = Duration::from_secs(2);

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransportConfiguration {
    #[default]
    Exclusive,
    Shared {
        socket: PathBuf,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    version: u32,
    address: Ipv4Addr,
    configuration: Configuration,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Submission {
    identifier: u32,
    bytes: Vec<u8>,
}

#[derive(Deserialize, Serialize)]
enum Response {
    Ready {
        version: u32,
    },
    Observed {
        event_port: bool,
        event: Event,
    },
    Failed {
        reason: String,
    },
}

async fn read_frame<T: DeserializeOwned>(reader: &mut (impl AsyncRead + Unpin)) -> Result<T> {
    let length = reader.read_u32().await? as usize;
    ensure!(length > 0 && length <= MAXIMUM_FRAME_BYTES, "clock service frame exceeds its budget");
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn write_frame<T: Serialize>(
    writer: &mut (impl AsyncWrite + Unpin),
    value: &T,
) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= MAXIMUM_FRAME_BYTES, "clock service response exceeds its budget");
    writer.write_u32(bytes.len().try_into()?).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}

pub struct Client {
    submissions: mpsc::Sender<Submission>,
    observations: mpsc::Receiver<Result<(bool, Event)>>,
    worker: tokio::task::JoinHandle<()>,
    next_identifier: Option<u32>,
}

impl Client {
    pub async fn connect(
        path: &Path,
        address: Ipv4Addr,
        configuration: Configuration,
    ) -> Result<Self> {
        let mut socket = timeout(REQUEST_DEADLINE, UnixStream::connect(path)).await??;
        ensure!(
            socket.peer_cred()?.uid() == unsafe { libc::geteuid() },
            "clock service belongs to another user"
        );
        timeout(
            REQUEST_DEADLINE,
            write_frame(
                &mut socket,
                &Registration {
                    version: PROTOCOL_VERSION,
                    address,
                    configuration,
                },
            ),
        )
        .await??;
        match timeout(REQUEST_DEADLINE, read_frame::<Response>(&mut socket)).await?? {
            Response::Ready {
                version: PROTOCOL_VERSION,
            } => {}
            Response::Failed {
                reason,
            } => bail!("clock registration rejected: {reason}"),
            _ => bail!("clock service returned an incompatible registration response"),
        }
        let (submissions, mut submitted) = mpsc::channel::<Submission>(CLIENT_QUEUE_CAPACITY);
        let (observations, observed) = mpsc::channel(CLIENT_QUEUE_CAPACITY);
        let worker = tokio::spawn(async move {
            let (mut reader, mut writer) = socket.into_split();
            let result: Result<()> = tokio::select! {
                result = async {
                    loop {
                        match read_frame::<Response>(&mut reader).await? {
                            Response::Observed { event_port, event } => {
                                observations.try_send(Ok((event_port, event))).context("clock consumer fell behind its bounded observation queue")?;
                            }
                            Response::Failed { reason } => bail!("clock service failed: {reason}"),
                            Response::Ready { .. } => bail!("clock service repeated its registration response"),
                        }
                    }
                } => result,
                result = async {
                    while let Some(submission) = submitted.recv().await {
                        timeout(REQUEST_DEADLINE, write_frame(&mut writer, &submission)).await??;
                    }
                    Ok(())
                } => result,
            };
            if let Err(error) = result {
                let _ = observations.try_send(Err(error));
            }
        });
        Ok(Self {
            submissions,
            observations: observed,
            worker,
            next_identifier: Some(0),
        })
    }

    pub async fn receive(&mut self) -> Result<(bool, Event)> {
        ensure!(
            !self.observations.is_closed(),
            "shared clock transport disconnected; fresh acquisition is required"
        );
        let observed = self
            .observations
            .recv()
            .await
            .context("shared clock transport disconnected; fresh acquisition is required")??;
        ensure!(
            !self.observations.is_closed(),
            "shared clock transport disconnected; queued observations retired"
        );
        Ok(observed)
    }

    pub fn submit(&mut self, bytes: &[u8]) -> Result<u32> {
        let identifier =
            self.next_identifier.context("clock consumer transmit identifier space exhausted")?;
        self.submissions
            .try_send(Submission {
                identifier,
                bytes: bytes.to_vec(),
            })
            .context("shared clock transmit queue unavailable")?;
        self.next_identifier = identifier.checked_add(1);
        Ok(identifier)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

struct SocketOwner {
    path: PathBuf,
    _lock: File,
}

impl SocketOwner {
    fn bind(path: &Path) -> Result<(Self, UnixListener)> {
        ensure!(path.is_absolute(), "clock service socket must use an absolute path");
        let parent = path.parent().context("clock socket needs a private directory")?;
        let metadata = std::fs::metadata(parent)?;
        ensure!(
            metadata.is_dir()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.mode() & 0o077 == 0,
            "clock socket directory must belong to this user and deny group/other access"
        );
        let lock_path = path.with_extension("lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(lock_path)?;
        ensure!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "another clock service owns this socket path"
        );
        match std::fs::symlink_metadata(path) {
            Ok(metadata) => {
                ensure!(
                    metadata.file_type().is_socket()
                        && metadata.uid() == unsafe { libc::geteuid() },
                    "clock socket path is occupied by an unowned file"
                );
                std::fs::remove_file(path)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        Ok((
            Self {
                path: path.to_owned(),
                _lock: lock,
            },
            listener,
        ))
    }
}

impl Drop for SocketOwner {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path) {
            eprintln!("clock service socket cleanup failed: {error}");
        }
    }
}

struct Consumer {
    registration: Registration,
    outgoing: mpsc::Sender<Response>,
    cancellation: oneshot::Sender<()>,
    pending: Option<Pending>,
    last_identifier: Option<u32>,
}

struct Pending {
    socket_identifier: u32,
    client_identifier: u32,
    deadline: Instant,
}

enum Request {
    Register {
        identifier: u64,
        registration: Registration,
        outgoing: mpsc::Sender<Response>,
        cancellation: oneshot::Sender<()>,
        ready: oneshot::Sender<Result<()>>,
    },
    Submit {
        identifier: u64,
        submission: Submission,
    },
}

async fn connection(
    identifier: u64,
    mut socket: UnixStream,
    requests: mpsc::Sender<Request>,
) -> Result<()> {
    ensure!(
        socket.peer_cred()?.uid() == unsafe { libc::geteuid() },
        "clock client belongs to another user"
    );
    let registration = timeout(REQUEST_DEADLINE, read_frame::<Registration>(&mut socket)).await??;
    let (outgoing, mut output) = mpsc::channel(CLIENT_QUEUE_CAPACITY);
    let (cancellation, canceled) = oneshot::channel();
    let (ready, registered) = oneshot::channel();
    timeout(
        REQUEST_DEADLINE,
        requests.send(Request::Register {
            identifier,
            registration,
            outgoing,
            cancellation,
            ready,
        }),
    )
    .await??;
    let result = timeout(REQUEST_DEADLINE, registered).await??;
    if let Err(error) = result {
        timeout(
            REQUEST_DEADLINE,
            write_frame(
                &mut socket,
                &Response::Failed {
                    reason: format!("{error:#}"),
                },
            ),
        )
        .await??;
        return Err(error);
    }
    timeout(
        REQUEST_DEADLINE,
        write_frame(
            &mut socket,
            &Response::Ready {
                version: PROTOCOL_VERSION,
            },
        ),
    )
    .await??;
    let (mut reader, mut writer) = socket.into_split();
    tokio::select! {
        result = async {
            loop {
                let submission = read_frame(&mut reader).await?;
                requests.send(Request::Submit { identifier, submission }).await.context("clock service request channel closed")?;
            }
        } => result,
        result = async {
            while let Some(response) = output.recv().await {
                timeout(REQUEST_DEADLINE, write_frame(&mut writer, &response)).await??;
            }
            Ok(())
        } => result,
        _ = canceled => Ok(()),
    }
}

struct Broker {
    event: ClockSocket,
    general: ClockSocket,
    consumers: HashMap<u64, Consumer>,
    memberships: HashMap<(u32, Ipv4Addr), usize>,
}

impl Broker {
    fn new() -> Result<Self> {
        Ok(Self {
            event: ClockSocket::bind_transport(version_one::EVENT_PORT)?,
            general: ClockSocket::bind_transport(version_one::GENERAL_PORT)?,
            consumers: HashMap::new(),
            memberships: HashMap::new(),
        })
    }

    fn register(
        &mut self,
        identifier: u64,
        registration: Registration,
        outgoing: mpsc::Sender<Response>,
        cancellation: oneshot::Sender<()>,
    ) -> Result<()> {
        ensure!(
            registration.version == PROTOCOL_VERSION,
            "unsupported clock service protocol version"
        );
        ensure!(self.consumers.len() < MAXIMUM_CLIENTS, "clock service consumer budget exhausted");
        let configuration = &registration.configuration;
        configuration.local_identity.validate()?;
        configuration.parent_identity.validate()?;
        ensure!(
            configuration.local_identity != configuration.parent_identity,
            "clock parent and local identities must differ"
        );
        ensure!(
            if_addrs::get_if_addrs()?
                .iter()
                .any(|interface| interface.ip() == registration.address
                    && interface.index == Some(configuration.interface_index)),
            "clock interface address/index is not current on this host"
        );
        ensure!(
            !self.consumers.values().any(|consumer| {
                let current = &consumer.registration.configuration;
                current.interface_index == configuration.interface_index
                    && current.domain == configuration.domain
                    && current.local_identity == configuration.local_identity
            }),
            "clock identity already has a consumer on this interface and domain"
        );
        let group = version_one::multicast_group(configuration.domain)?;
        let membership = (configuration.interface_index, group);
        if !self.memberships.contains_key(&membership) {
            self.event.join_on(configuration.interface_index, group)?;
            if let Err(error) = self.general.join_on(configuration.interface_index, group) {
                self.event.leave_on(configuration.interface_index, group)?;
                return Err(error);
            }
        }
        *self.memberships.entry(membership).or_default() += 1;
        self.consumers.insert(
            identifier,
            Consumer {
                registration,
                outgoing,
                cancellation,
                pending: None,
                last_identifier: None,
            },
        );
        eprintln!("clock consumer {identifier} registered; consumers={}", self.consumers.len());
        Ok(())
    }

    fn remove(&mut self, identifier: u64) -> Result<()> {
        let Some(consumer) = self.consumers.remove(&identifier) else {
            return Ok(());
        };
        let _ = consumer.cancellation.send(());
        let interface_index = consumer.registration.configuration.interface_index;
        let group = version_one::multicast_group(consumer.registration.configuration.domain)?;
        let membership = self
            .memberships
            .get_mut(&(interface_index, group))
            .context("clock membership ownership disappeared")?;
        *membership -= 1;
        if *membership == 0 {
            self.event.leave_on(interface_index, group)?;
            self.general.leave_on(interface_index, group)?;
            self.memberships.remove(&(interface_index, group));
        }
        eprintln!("clock consumer {identifier} retired; consumers={}", self.consumers.len());
        Ok(())
    }

    fn submit(&mut self, identifier: u64, submission: Submission) -> Result<()> {
        let Some(consumer) = self.consumers.get_mut(&identifier) else {
            return Ok(());
        };
        ensure!(
            consumer.last_identifier.is_none_or(|previous| submission.identifier > previous),
            "clock submission identifier repeated or reversed"
        );
        let packet = Packet::decode(&submission.bytes)?;
        let configuration = &consumer.registration.configuration;
        ensure!(
            packet.source == configuration.local_identity
                && packet.domain == configuration.domain
                && matches!(packet.body, Body::DelayRequest { .. }),
            "clock consumer attempted an unowned or unsupported submission"
        );
        ensure!(
            consumer.pending.as_ref().is_none_or(|pending| Instant::now() >= pending.deadline),
            "clock consumer has an unresolved transmit timestamp"
        );
        let group = version_one::multicast_group(configuration.domain)?;
        let socket_identifier = self.event.try_send_on(
            &submission.bytes,
            SocketAddrV4::new(group, version_one::EVENT_PORT),
            consumer.registration.address,
            configuration.interface_index,
        )?;
        consumer.last_identifier = Some(submission.identifier);
        consumer.pending = Some(Pending {
            socket_identifier,
            client_identifier: submission.identifier,
            deadline: Instant::now() + REQUEST_DEADLINE,
        });
        Ok(())
    }

    fn observe(&mut self, event_port: bool, event: Event) -> Result<()> {
        let mut deliveries = Vec::new();
        match &event {
            Event::Received {
                bytes,
                interface_index,
                ..
            } => {
                let Ok(packet) = Packet::decode(bytes) else {
                    return Ok(());
                };
                if matches!(packet.body, Body::Sync { .. } | Body::DelayRequest { .. })
                    != event_port
                {
                    return Ok(());
                }
                for (identifier, consumer) in &self.consumers {
                    let configuration = &consumer.registration.configuration;
                    if configuration.interface_index != *interface_index
                        || configuration.domain != packet.domain
                        || configuration.parent_identity != packet.source
                    {
                        continue;
                    }
                    if let Body::DelayResponse {
                        requester,
                        ..
                    } = packet.body
                        && requester != configuration.local_identity
                    {
                        continue;
                    }
                    deliveries.push((*identifier, event.clone()));
                }
            }
            Event::Transmitted {
                identifier,
                realtime_nanoseconds,
            } if event_port => {
                for (consumer_identifier, consumer) in &mut self.consumers {
                    if let Some(pending) = consumer.pending.as_ref()
                        && pending.socket_identifier == *identifier
                    {
                        let pending =
                            consumer.pending.take().context("clock transmit owner disappeared")?;
                        if Instant::now() < pending.deadline {
                            deliveries.push((
                                *consumer_identifier,
                                Event::Transmitted {
                                    identifier: pending.client_identifier,
                                    realtime_nanoseconds: *realtime_nanoseconds,
                                },
                            ));
                        }
                        break;
                    }
                }
            }
            _ => {}
        }
        for (identifier, event) in deliveries {
            if self.consumers[&identifier]
                .outgoing
                .try_send(Response::Observed {
                    event_port,
                    event,
                })
                .is_err()
            {
                eprintln!(
                    "clock consumer {identifier} exceeded its bounded delivery queue; disconnecting"
                );
                self.remove(identifier)?;
            }
        }
        Ok(())
    }
}

pub async fn run(path: &Path, stopped: impl std::future::Future<Output = ()>) -> Result<()> {
    let service = super::service::Service::open()?;
    let (_owner, listener) = SocketOwner::bind(path)?;
    let mut broker = Broker::new()?;
    let (requests, mut requested) = mpsc::channel(CLIENT_QUEUE_CAPACITY);
    let mut connections = JoinSet::new();
    let mut next_identifier = 0u64;
    let mut published_consumers = 0;
    tokio::pin!(stopped);
    eprintln!("shared clock transport ready: {}", path.display());
    service.send("READY=1\nSTATUS=Shared PTP transport ready; no timing consumers")?;
    let result: Result<()> = async {
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                accepted = listener.accept(), if connections.len() < MAXIMUM_CLIENTS => {
                    let (socket, _) = accepted?;
                    next_identifier = next_identifier.checked_add(1).context("clock consumer identity space exhausted")?;
                    let identifier = next_identifier;
                    let requests = requests.clone();
                    connections.spawn(async move { (identifier, connection(identifier, socket, requests).await) });
                }
                Some(completed) = connections.join_next(), if !connections.is_empty() => {
                    let (identifier, result) = completed?;
                    if let Err(error) = result { eprintln!("clock consumer {identifier} disconnected: {error:#}"); }
                    broker.remove(identifier)?;
                }
                Some(request) = requested.recv() => match request {
                    Request::Register { identifier, registration, outgoing, cancellation, ready } => {
                        let result = broker.register(identifier, registration, outgoing, cancellation);
                        if ready.send(result).is_err() { broker.remove(identifier)?; }
                    }
                    Request::Submit { identifier, submission } => {
                        if let Err(error) = broker.submit(identifier, submission) {
                            eprintln!("clock consumer {identifier} submission failed: {error:#}");
                            broker.remove(identifier)?;
                        }
                    }
                },
                event = broker.event.receive() => broker.observe(true, event?)?,
                event = broker.general.receive() => broker.observe(false, event?)?,
            }
            if broker.consumers.len() != published_consumers {
                published_consumers = broker.consumers.len();
                service.send(&format!("STATUS=Shared PTP transport ready; timing consumers={published_consumers}"))?;
            }
        }
        Ok(())
    }.await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    let notification = service.send(&format!(
        "STOPPING=1\nSTATUS=Shared PTP transport stopped{}",
        result
            .as_ref()
            .err()
            .map(|error| format!(": {error:#}").replace(['\n', '\r'], " "))
            .unwrap_or_default()
    ));
    result?;
    notification
}
