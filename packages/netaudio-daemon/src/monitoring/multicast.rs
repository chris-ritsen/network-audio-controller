use crate::network::NetworkSnapshot;
use crate::transport::datagram::{DatagramSocket, MulticastInterface};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub(super) const MAXIMUM_DATAGRAM_BYTES: usize = u16::MAX as usize;

#[derive(Clone)]
pub struct ReceivedPacket {
    pub source: SocketAddr,
    pub destination: SocketAddr,
    pub received_at: DateTime<Utc>,
    pub received_monotonic: tokio::time::Instant,
    pub payload: Arc<[u8]>,
}

pub struct MulticastReceiver {
    socket: DatagramSocket,
    port: u16,
    group: Ipv4Addr,
    interfaces: BTreeMap<u32, MulticastInterface>,
    wanted: BTreeSet<u32>,
    observer: Option<crate::capture::Observer>,
}

impl MulticastReceiver {
    pub fn bind(
        group: Ipv4Addr,
        port: u16,
        interfaces: &BTreeMap<u32, MulticastInterface>,
    ) -> Result<Self> {
        Self::bind_socket(group, port, interfaces, true)
    }

    pub fn bind_exclusive(
        group: Ipv4Addr,
        port: u16,
        interfaces: &BTreeMap<u32, MulticastInterface>,
    ) -> Result<Self> {
        Self::bind_socket(group, port, interfaces, false)
    }

    fn bind_socket(
        group: Ipv4Addr,
        port: u16,
        interfaces: &BTreeMap<u32, MulticastInterface>,
        reuse: bool,
    ) -> Result<Self> {
        ensure!(group.is_multicast(), "monitoring group must be multicast");
        let socket = if reuse {
            DatagramSocket::bind(port)
        } else {
            DatagramSocket::bind_exclusive(port)
        }
        .with_context(|| format!("bind multicast monitoring port {port}"))?;

        let mut receiver = Self {
            socket,
            port,
            group,
            interfaces: BTreeMap::new(),
            wanted: BTreeSet::new(),
            observer: None,
        };
        receiver.update_interfaces(interfaces.clone());

        Ok(receiver)
    }

    pub fn observe(mut self, observer: Option<crate::capture::Observer>) -> Self {
        self.observer = observer;
        self
    }

    fn record(&self, packet: &ReceivedPacket) -> Result<()> {
        if let Some(observer) = &self.observer {
            observer.record(crate::capture::Packet {
                observed_at: packet.received_at,
                source: packet.source,
                destination: packet.destination,
                direction: "response",
                payload: packet.payload.clone(),
            });
        }
        Ok(())
    }

    pub async fn run(
        mut self,
        sender: mpsc::Sender<ReceivedPacket>,
        cancellation: CancellationToken,
        mut network: watch::Receiver<NetworkSnapshot>,
    ) -> Result<()> {
        let mut buffer = vec![0; MAXIMUM_DATAGRAM_BYTES];

        loop {
            let metadata = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                changed = network.changed() => {
                    changed.context("network monitor stopped")?;
                    self.update_interfaces(network.borrow_and_update().ipv4_multicast_interfaces());
                    continue;
                }
                received = self.socket.receive(&mut buffer) => received?,
            };

            if !self.wants(metadata.interface_index) {
                continue;
            }

            let packet = ReceivedPacket {
                source: SocketAddr::V4(metadata.source),
                destination: SocketAddr::from((metadata.destination, self.port)),
                received_at: Utc::now(),
                received_monotonic: tokio::time::Instant::now(),
                payload: Arc::from(&buffer[..metadata.length]),
            };
            self.record(&packet)?;

            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                result = sender.send(packet) => result.context("monitoring consumer stopped")?,
            }
        }
    }

    pub(super) async fn receive(&self, buffer: &mut [u8]) -> Result<ReceivedPacket> {
        let metadata = loop {
            let metadata = self.socket.receive(buffer).await?;

            if self.wants(metadata.interface_index) {
                break metadata;
            }
        };
        let packet = ReceivedPacket {
            source: SocketAddr::V4(metadata.source),
            destination: SocketAddr::from((metadata.destination, self.port)),
            received_at: Utc::now(),
            received_monotonic: tokio::time::Instant::now(),
            payload: Arc::from(&buffer[..metadata.length]),
        };
        self.record(&packet)?;
        Ok(packet)
    }

    fn wants(&self, interface_index: u32) -> bool {
        interface_index == 0 || self.wanted.contains(&interface_index)
    }

    pub(super) fn update_interfaces(&mut self, interfaces: BTreeMap<u32, MulticastInterface>) {
        self.wanted = interfaces.keys().copied().collect();
        let group = self.group;
        let socket = &self.socket;

        self.interfaces.retain(|index, interface| {
            if interfaces.contains_key(index) {
                return true;
            }

            if let Err(error) = socket.leave(group, *interface) {
                tracing::warn!(interface = %index, address = %interface.address, %group, %error, "could not leave monitoring group on removed interface");
            }

            false
        });

        for (index, interface) in interfaces {
            if self.interfaces.contains_key(&index) {
                continue;
            }

            match self.socket.join(group, interface) {
                Ok(()) => {
                    self.interfaces.insert(index, interface);
                }
                Err(error) => {
                    tracing::warn!(interface = %index, address = %interface.address, %group, %error, "could not join monitoring group");
                }
            }
        }
    }
}
