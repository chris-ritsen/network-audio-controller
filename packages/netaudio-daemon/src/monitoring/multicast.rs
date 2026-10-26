use crate::network::NetworkSnapshot;
use crate::transport::datagram::DatagramSocket;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use std::collections::BTreeSet;
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
    interfaces: BTreeSet<u32>,
    observer: Option<crate::capture::Observer>,
}

impl MulticastReceiver {
    pub fn bind(group: Ipv4Addr, port: u16, interfaces: &[u32]) -> Result<Self> {
        Self::bind_socket(group, port, interfaces, true)
    }

    pub fn bind_exclusive(group: Ipv4Addr, port: u16, interfaces: &[u32]) -> Result<Self> {
        Self::bind_socket(group, port, interfaces, false)
    }

    fn bind_socket(group: Ipv4Addr, port: u16, interfaces: &[u32], reuse: bool) -> Result<Self> {
        ensure!(group.is_multicast(), "monitoring group must be multicast");
        let socket = if reuse {
            DatagramSocket::bind(port)
        } else {
            DatagramSocket::bind_exclusive(port)
        }
        .with_context(|| format!("bind multicast monitoring port {port}"))?;

        for interface in interfaces {
            socket
                .join(group, *interface)
                .with_context(|| format!("join monitoring group {group} on {interface}"))?;
        }

        Ok(Self {
            socket,
            port,
            group,
            interfaces: interfaces.iter().copied().collect(),
            observer: None,
        })
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
        selected: Option<Ipv4Addr>,
    ) -> Result<()> {
        let mut buffer = vec![0; MAXIMUM_DATAGRAM_BYTES];

        loop {
            let metadata = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                changed = network.changed() => {
                    changed.context("network monitor stopped")?;
                    self.update_interfaces(network.borrow_and_update().ipv4_interface_indices(selected)?)?;
                    continue;
                }
                received = self.socket.receive(&mut buffer) => received?,
            };
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
        let metadata = self.socket.receive(buffer).await?;
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

    pub(super) fn update_interfaces(&mut self, interfaces: BTreeSet<u32>) -> Result<()> {
        for interface in self.interfaces.difference(&interfaces) {
            if let Err(error) = self.socket.leave(self.group, *interface) {
                tracing::warn!(%interface, %error, "could not leave monitoring group on removed interface");
            }
        }

        for interface in interfaces.difference(&self.interfaces) {
            self.socket
                .join(self.group, *interface)
                .with_context(|| format!("join monitoring group {} on {interface}", self.group))?;
        }

        self.interfaces = interfaces;
        Ok(())
    }
}
