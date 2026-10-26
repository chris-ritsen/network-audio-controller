use super::{PacketMetadata, invalid_packet};
use nix::sys::socket::{ControlMessageOwned, MsgFlags, SockaddrIn, recvmsg, setsockopt, sockopt};
use socket2::Socket;
use std::io::{self, IoSliceMut};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::AsRawFd;
use tokio::net::UdpSocket;

pub struct PacketReceiver;

impl PacketReceiver {
    pub fn new(socket: &Socket) -> io::Result<Self> {
        setsockopt(socket, sockopt::Ipv4PacketInfo, &true)?;
        Ok(Self)
    }

    pub fn receive(&self, socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<PacketMetadata> {
        let mut buffers = [IoSliceMut::new(buffer)];
        let mut control = nix::cmsg_space!(nix::libc::in_pktinfo);
        let message = recvmsg::<SockaddrIn>(
            socket.as_raw_fd(),
            &mut buffers,
            Some(&mut control),
            MsgFlags::MSG_DONTWAIT,
        )?;

        if message
            .flags
            .intersects(MsgFlags::MSG_TRUNC | MsgFlags::MSG_CTRUNC)
        {
            return Err(invalid_packet(
                "received truncated UDP packet or interface metadata",
            ));
        }

        let source = message
            .address
            .ok_or_else(|| invalid_packet("UDP source address is absent"))?;
        let information = message
            .cmsgs()?
            .find_map(|message| match message {
                ControlMessageOwned::Ipv4PacketInfo(information) => Some(information),
                _ => None,
            })
            .ok_or_else(|| invalid_packet("UDP receiving interface is absent"))?;
        let interface_index = u32::try_from(i64::from(information.ipi_ifindex))
            .ok()
            .filter(|index| *index != 0)
            .ok_or_else(|| invalid_packet("UDP receiving interface is invalid"))?;

        Ok(PacketMetadata {
            length: message.bytes,
            source: SocketAddrV4::new(source.ip(), source.port()),
            destination: Ipv4Addr::from(information.ipi_addr.s_addr.to_ne_bytes()),
            interface_index,
        })
    }
}
