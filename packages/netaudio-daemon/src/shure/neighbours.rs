use anyhow::{Context, Result, ensure};
use netlink_packet_core::{
    NLM_F_DUMP, NLM_F_DUMP_INTR, NLM_F_REQUEST, NetlinkMessage, NetlinkPayload,
};
use netlink_packet_route::{
    AddressFamily, RouteNetlinkMessage,
    neighbour::{NeighbourAddress, NeighbourAttribute, NeighbourMessage, NeighbourState},
};
use netlink_sys::{Socket, SocketAddr, protocols::NETLINK_ROUTE};
use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::io::unix::AsyncFd;
use tokio::time::{Instant, timeout_at};

const NEIGHBOUR_GROUP: u32 = 4;
const MAXIMUM_DATAGRAM: usize = 1_048_576;

pub type Entries = BTreeMap<(u32, Ipv4Addr), String>;

pub struct Observer {
    socket: AsyncFd<Socket>,
    sequence: u32,
    entries: Entries,
    buffer: Vec<u8>,
}

impl Observer {
    pub fn new() -> Result<Self> {
        let mut socket = Socket::new(NETLINK_ROUTE)?;
        socket.set_non_blocking(true)?;
        socket.bind(&SocketAddr::new(0, NEIGHBOUR_GROUP))?;
        Ok(Self {
            socket: unsafe { AsyncFd::register(socket) }?,
            sequence: 0,
            entries: BTreeMap::new(),
            buffer: vec![0; MAXIMUM_DATAGRAM],
        })
    }

    pub async fn snapshot(&mut self) -> Result<Entries> {
        let deadline = Instant::now() + Duration::from_secs(5);
        for _ in 0..3 {
            if let Some(snapshot) = self.snapshot_attempt(deadline).await? {
                self.entries = snapshot;
                return Ok(self.entries.clone());
            }
        }
        anyhow::bail!("Neighbour snapshot kept changing during acquisition")
    }

    async fn snapshot_attempt(&mut self, deadline: Instant) -> Result<Option<Entries>> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .context("Neighbour snapshot sequence exhausted")?;
        let mut neighbour = NeighbourMessage::default();
        neighbour.header.family = AddressFamily::Inet;
        let mut request = NetlinkMessage::from(RouteNetlinkMessage::GetNeighbour(neighbour));
        request.header.flags = NLM_F_REQUEST | NLM_F_DUMP;
        request.header.sequence_number = self.sequence;
        request.finalize();
        let mut bytes = vec![0; request.buffer_len()];
        request.serialize(&mut bytes);
        loop {
            let mut ready = timeout_at(deadline, self.socket.writable()).await??;
            match ready.try_io(|socket| socket.get_ref().send_to(&bytes, &SocketAddr::new(0, 0), 0))
            {
                Ok(result) => {
                    ensure!(
                        result? == bytes.len(),
                        "Incomplete neighbour snapshot request"
                    );
                    break;
                }
                Err(_) => continue,
            }
        }
        let mut snapshot = BTreeMap::new();
        let mut interrupted = false;
        loop {
            let messages = timeout_at(deadline, self.receive()).await??;
            let mut done = false;
            for message in messages {
                if message.header.sequence_number == self.sequence {
                    interrupted |= message.header.flags & NLM_F_DUMP_INTR != 0;
                    match message.payload {
                        NetlinkPayload::Done(done_message) => {
                            ensure!(done_message.code == 0, "Neighbour snapshot failed");
                            done = true;
                        }
                        NetlinkPayload::InnerMessage(message) => {
                            apply(&mut snapshot, message);
                        }
                        NetlinkPayload::Error(error) => {
                            ensure!(error.code.is_none(), "Neighbour snapshot failed: {error:?}")
                        }
                        NetlinkPayload::Overrun(_) => {
                            anyhow::bail!("Neighbour event queue overflowed")
                        }
                        _ => {}
                    }
                } else if message.header.sequence_number == 0 {
                    match message.payload {
                        NetlinkPayload::InnerMessage(_) => {
                            interrupted = true;
                        }
                        NetlinkPayload::Overrun(_) => {
                            anyhow::bail!("Neighbour event queue overflowed")
                        }
                        NetlinkPayload::Error(error) => ensure!(
                            error.code.is_none(),
                            "Neighbour observation failed: {error:?}"
                        ),
                        _ => {}
                    }
                }
            }
            if done {
                return Ok((!interrupted).then_some(snapshot));
            }
        }
    }

    pub async fn changed(&mut self) -> Result<Entries> {
        loop {
            let before = self.entries.clone();
            for message in self.receive().await? {
                if message.header.sequence_number != 0 {
                    continue;
                }
                match message.payload {
                    NetlinkPayload::InnerMessage(message) => {
                        apply(&mut self.entries, message);
                    }
                    NetlinkPayload::Overrun(_) => anyhow::bail!("Neighbour event queue overflowed"),
                    NetlinkPayload::Error(error) => ensure!(
                        error.code.is_none(),
                        "Neighbour observation failed: {error:?}"
                    ),
                    _ => {}
                }
            }
            if before != self.entries {
                return Ok(self.entries.clone());
            }
        }
    }

    async fn receive(&mut self) -> Result<Vec<NetlinkMessage<RouteNetlinkMessage>>> {
        let (length, source) = loop {
            let mut ready = self.socket.readable().await?;
            let mut destination = self.buffer.as_mut_slice();
            match ready.try_io(|socket| {
                socket
                    .get_ref()
                    .recv_from(&mut destination, nix::libc::MSG_TRUNC)
            }) {
                Ok(result) => break result?,
                Err(_) => continue,
            }
        };
        ensure!(
            source.port_number() == 0,
            "Neighbour observation came from a non-kernel source"
        );
        ensure!(
            length <= self.buffer.len(),
            "Neighbour observation datagram exceeded its size limit"
        );
        let mut bytes = &self.buffer[..length];
        let mut messages = Vec::new();
        while !bytes.is_empty() {
            let message = NetlinkMessage::<RouteNetlinkMessage>::deserialize(bytes)?;
            let length = message.header.length as usize;
            ensure!(
                length >= 16 && length <= bytes.len(),
                "Invalid neighbour message length"
            );
            messages.push(message);
            let aligned = length
                .checked_add(3)
                .context("Neighbour message length overflow")?
                & !3;
            bytes = if aligned > bytes.len() {
                &[]
            } else {
                &bytes[aligned..]
            };
        }
        Ok(messages)
    }
}

fn apply(entries: &mut Entries, message: RouteNetlinkMessage) {
    let (message, removed) = match message {
        RouteNetlinkMessage::NewNeighbour(message) => (message, false),
        RouteNetlinkMessage::DelNeighbour(message) => (message, true),
        _ => return,
    };
    if message.header.family != AddressFamily::Inet {
        return;
    }
    let address = message
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            NeighbourAttribute::Destination(NeighbourAddress::Inet(address)) => Some(*address),
            _ => None,
        });
    let Some(address) = address else {
        return;
    };
    let key = (message.header.ifindex, address);
    if removed
        || matches!(
            message.header.state,
            NeighbourState::Failed | NeighbourState::Incomplete | NeighbourState::None
        )
    {
        entries.remove(&key);
        return;
    }
    let mac = message
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            NeighbourAttribute::LinkLocalAddress(bytes)
                if bytes.len() == 6 && bytes.starts_with(&[0x00, 0x0e, 0xdd]) =>
            {
                Some(
                    bytes
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<Vec<_>>()
                        .join(":"),
                )
            }
            _ => None,
        });
    match mac {
        Some(mac) => {
            entries.insert(key, mac);
        }
        None => {
            entries.remove(&key);
        }
    }
}
