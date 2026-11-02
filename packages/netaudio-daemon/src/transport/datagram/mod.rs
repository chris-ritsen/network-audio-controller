#[cfg(target_vendor = "apple")]
mod apple;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
use unix::PacketReceiver;
#[cfg(windows)]
use windows::PacketReceiver;

#[cfg(not(target_vendor = "apple"))]
use socket2::{InterfaceIndexOrAddress, SockRef};
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::io::Interest;
use tokio::net::UdpSocket;

pub const MAXIMUM_DATAGRAM_BYTES: usize = u16::MAX as usize;

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

    pub fn local_address(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    #[cfg(target_vendor = "apple")]
    pub fn join(&self, group: Ipv4Addr, interface_index: u32) -> io::Result<()> {
        apple::join(&self.socket, group, interface_index)
    }

    #[cfg(not(target_vendor = "apple"))]
    pub fn join(&self, group: Ipv4Addr, interface_index: u32) -> io::Result<()> {
        SockRef::from(&self.socket)
            .join_multicast_v4_n(&group, &InterfaceIndexOrAddress::Index(interface_index))
    }

    #[cfg(target_vendor = "apple")]
    pub fn leave(&self, group: Ipv4Addr, interface_index: u32) -> io::Result<()> {
        apple::leave(&self.socket, group, interface_index)
    }

    #[cfg(not(target_vendor = "apple"))]
    pub fn leave(&self, group: Ipv4Addr, interface_index: u32) -> io::Result<()> {
        SockRef::from(&self.socket)
            .leave_multicast_v4_n(&group, &InterfaceIndexOrAddress::Index(interface_index))
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    const GROUP: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 192);
    const UNKNOWN_INTERFACE: u32 = 60_000;

    fn loopback_interface() -> u32 {
        if_addrs::get_if_addrs()
            .unwrap()
            .into_iter()
            .find(|interface| interface.is_loopback() && interface.ip().is_ipv4())
            .and_then(|interface| interface.index)
            .unwrap()
    }

    fn other_interface() -> Option<u32> {
        if_addrs::get_if_addrs()
            .unwrap()
            .into_iter()
            .find(|interface| !interface.is_loopback() && interface.ip().is_ipv4())
            .and_then(|interface| interface.index)
    }

    #[tokio::test]
    async fn a_join_uses_the_requested_interface() {
        let socket = DatagramSocket::bind(0).unwrap();

        let unknown = socket.join(GROUP, UNKNOWN_INTERFACE);
        assert!(unknown.is_err(), "joined an interface that does not exist");

        let loopback = loopback_interface();
        socket.join(GROUP, loopback).unwrap();
        assert_eq!(
            socket.join(GROUP, loopback).unwrap_err().kind(),
            io::ErrorKind::AddrInUse
        );
        socket.leave(GROUP, loopback).unwrap();
        assert!(socket.leave(GROUP, loopback).is_err());
    }

    #[tokio::test]
    async fn one_socket_joins_a_group_on_each_interface() {
        let Some(other) = other_interface() else {
            return;
        };
        let loopback = loopback_interface();
        let socket = DatagramSocket::bind(0).unwrap();

        socket.join(GROUP, loopback).unwrap();
        socket.join(GROUP, other).unwrap();
        socket.leave(GROUP, loopback).unwrap();
        socket.leave(GROUP, other).unwrap();
    }
}
