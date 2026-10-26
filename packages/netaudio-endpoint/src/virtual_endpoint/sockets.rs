use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::clock::discipline::LocalTimestamp;
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

const DATAGRAM_BUFFER_BYTES: usize = 65_536;
const MEDIA_PORT_COUNT: u16 = 8;
const FIRST_DYNAMIC_PORT: u16 = 49_152;
const LAST_MEDIA_BLOCK_START: u16 = u16::MAX - MEDIA_PORT_COUNT + 1;
const MEDIA_QUARANTINE_NANOSECONDS: u64 = 60_000_000_000;
const LOCAL_MULTICAST_TIME_TO_LIVE: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Channel {
    Routing,
    Monitoring,
    Control,
    Source,
    SourceMedia(usize),
    ReceiveMedia(usize),
}

pub struct Datagram {
    pub channel: Channel,
    pub peer: SocketAddrV4,
    pub bytes: Vec<u8>,
    pub timestamp: LocalTimestamp,
    pub generation: u64,
}

#[derive(Clone)]
pub struct Monotonic {
    started: std::time::Instant,
    incarnation: u64,
}

impl Monotonic {
    pub fn new(incarnation: u64) -> Self {
        Self {
            started: std::time::Instant::now(),
            incarnation,
        }
    }

    pub fn now(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos())
            .expect("monotonic process lifetime exceeds u64 nanoseconds")
    }

    pub fn instant(&self, nanoseconds: u64) -> Result<tokio::time::Instant> {
        Ok(tokio::time::Instant::from_std(
            self.started
                .checked_add(std::time::Duration::from_nanos(nanoseconds))
                .context("monotonic deadline is unrepresentable")?,
        ))
    }

    pub fn stamp(&self, before: u64, after: u64) -> Result<LocalTimestamp> {
        let extent = after.checked_sub(before).context("monotonic timestamp provider reversed")?;
        Ok(LocalTimestamp {
            nanoseconds: before + extent / 2,
            uncertainty_nanoseconds: u32::try_from(extent.div_ceil(2))
                .context("timestamp observation uncertainty exceeds supported range")?,
            provider_incarnation: self.incarnation,
        })
    }
}

pub async fn bind(address: Ipv4Addr) -> Result<Arc<UdpSocket>> {
    let socket = UdpSocket::bind(SocketAddrV4::new(address, 0)).await?;
    socket.writable().await?;
    Ok(Arc::new(socket))
}

pub fn enable_multicast_publication(socket: &UdpSocket, address: Ipv4Addr) -> Result<()> {
    let socket = SockRef::from(socket);
    socket.set_multicast_if_v4(&address)?;
    socket.set_multicast_ttl_v4(LOCAL_MULTICAST_TIME_TO_LIVE)?;
    socket.set_multicast_loop_v4(true)?;
    Ok(())
}

pub fn multicast(address: Ipv4Addr, group: Ipv4Addr, port: u16) -> Result<Arc<UdpSocket>> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port).into())?;
    socket.set_multicast_all_v4(false)?;
    socket.set_multicast_if_v4(&address)?;
    socket.set_multicast_loop_v4(false)?;
    socket.join_multicast_v4(&group, &address)?;
    Ok(Arc::new(UdpSocket::from_std(socket.into())?))
}

pub async fn receive(
    socket: Arc<UdpSocket>,
    channel: Channel,
    generation: Arc<AtomicU64>,
    time: Monotonic,
    sender: mpsc::Sender<Datagram>,
) -> Result<()> {
    let mut buffer = vec![0; DATAGRAM_BUFFER_BYTES];
    loop {
        socket.readable().await?;
        let owner = generation.load(Ordering::Acquire);
        let before = time.now();
        let (length, peer) = match socket.try_recv_from(&mut buffer) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error.into()),
        };

        let after = time.now();
        let std::net::SocketAddr::V4(peer) = peer else {
            continue;
        };

        if generation.load(Ordering::Acquire) != owner {
            continue;
        }

        if owner == 0 && matches!(channel, Channel::SourceMedia(_) | Channel::ReceiveMedia(_)) {
            continue;
        }

        let datagram = Datagram {
            channel,
            peer,
            bytes: buffer[..length].to_vec(),
            timestamp: time.stamp(before, after)?,
            generation: owner,
        };
        sender.try_send(datagram).map_err(|error| {
            anyhow::anyhow!("endpoint receive queue cannot accept a whole datagram: {error}")
        })?;
    }
}

pub struct PoolSlot {
    pub socket: Arc<UdpSocket>,
    pub owner: Arc<AtomicU64>,
    eligible_at: u64,
    disabled: bool,
}

pub struct MediaPool {
    pub slots: Vec<PoolSlot>,
    next: usize,
}

impl MediaPool {
    pub async fn reserve(address: Ipv4Addr, now: u64) -> Result<Self> {
        for first in
            (FIRST_DYNAMIC_PORT..=LAST_MEDIA_BLOCK_START).step_by(usize::from(MEDIA_PORT_COUNT))
        {
            let mut sockets = Vec::new();
            for offset in 0..MEDIA_PORT_COUNT {
                match UdpSocket::bind(SocketAddrV4::new(address, first + offset)).await {
                    Ok(socket) => {
                        socket.writable().await?;
                        sockets.push(Arc::new(socket));
                    }
                    Err(_) => break,
                }
            }

            if sockets.len() == usize::from(MEDIA_PORT_COUNT) {
                let eligible_at = now
                    .checked_add(MEDIA_QUARANTINE_NANOSECONDS)
                    .context("media pool startup quarantine overflow")?;
                return Ok(Self {
                    slots: sockets
                        .into_iter()
                        .map(|socket| PoolSlot {
                            socket,
                            owner: Arc::new(AtomicU64::new(0)),
                            eligible_at,
                            disabled: false,
                        })
                        .collect(),
                    next: 0,
                });
            }
        }
        bail!("no fully owned aligned eight-port media block is available")
    }

    pub fn acquire(&mut self, generation: u64, now: u64) -> Option<usize> {
        for offset in 0..self.slots.len() {
            let index = (self.next + offset) % self.slots.len();
            let slot = &self.slots[index];
            if !slot.disabled && slot.owner.load(Ordering::Acquire) == 0 && now >= slot.eligible_at
            {
                slot.owner.store(generation, Ordering::Release);
                self.next = (index + 1) % self.slots.len();
                return Some(index);
            }
        }
        None
    }

    pub fn next_available(&self) -> Option<u64> {
        self.slots
            .iter()
            .filter(|slot| !slot.disabled && slot.owner.load(Ordering::Acquire) == 0)
            .map(|slot| slot.eligible_at)
            .min()
    }

    pub fn release(&mut self, index: usize, now: u64) -> Result<()> {
        let slot = self.slots.get_mut(index).context("media pool release index is invalid")?;
        slot.owner.store(0, Ordering::Release);
        slot.eligible_at = now
            .checked_add(MEDIA_QUARANTINE_NANOSECONDS)
            .context("media pool retirement quarantine overflow")?;
        Ok(())
    }

    pub fn disable(&mut self, index: usize) -> Result<()> {
        let slot = self.slots.get_mut(index).context("media pool disable index is invalid")?;
        slot.owner.store(0, Ordering::Release);
        slot.disabled = true;
        Ok(())
    }
}
