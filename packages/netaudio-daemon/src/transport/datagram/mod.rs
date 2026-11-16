#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
use unix::PacketReceiver;
#[cfg(windows)]
use windows::PacketReceiver;

use socket2::{Domain, InterfaceIndexOrAddress, Protocol, SockRef, Socket, Type};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::io::Interest;
use tokio::net::UdpSocket;

pub const MAXIMUM_DATAGRAM_BYTES: usize = u16::MAX as usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MulticastInterface {
    pub address: Ipv4Addr,
    pub index: u32,
}

impl MulticastInterface {
    fn selector(&self) -> InterfaceIndexOrAddress {
        #[cfg(target_vendor = "apple")]
        return InterfaceIndexOrAddress::Address(self.address);
        #[cfg(not(target_vendor = "apple"))]
        return InterfaceIndexOrAddress::Index(self.index);
    }
}

pub struct PacketMetadata {
    pub length: usize,
    pub source: SocketAddrV4,
    pub destination: Ipv4Addr,
    pub interface_index: u32,
}

pub struct DatagramSocket {
    socket: UdpSocket,
    receiver: PacketReceiver,
}

impl DatagramSocket {
    pub fn bind(port: u16) -> io::Result<Self> {
        Self::bind_with_reuse(port, true)
    }

    pub fn bind_exclusive(port: u16) -> io::Result<Self> {
        Self::bind_with_reuse(port, false)
    }

    fn bind_with_reuse(port: u16, reuse: bool) -> io::Result<Self> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(reuse)?;
        #[cfg(target_vendor = "apple")]
        socket.set_reuse_port(reuse)?;
        socket.set_nonblocking(true)?;
        let receiver = PacketReceiver::new(&socket)?;
        socket.bind(&SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)).into())?;

        Ok(Self {
            socket: UdpSocket::from_std(socket.into())?,
            receiver,
        })
    }

    pub fn join(&self, group: Ipv4Addr, interface: MulticastInterface) -> io::Result<()> {
        SockRef::from(&self.socket).join_multicast_v4_n(&group, &interface.selector())
    }

    pub fn leave(&self, group: Ipv4Addr, interface: MulticastInterface) -> io::Result<()> {
        SockRef::from(&self.socket).leave_multicast_v4_n(&group, &interface.selector())
    }

    pub async fn receive(&self, buffer: &mut [u8]) -> io::Result<PacketMetadata> {
        self.socket
            .async_io(Interest::READABLE, || {
                self.receiver.receive(&self.socket, buffer)
            })
            .await
    }
}

fn invalid_packet(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
