use super::{invalid_packet, PacketMetadata};
use nix::libc;
use socket2::Socket;
use std::io;
use std::mem::{size_of, size_of_val, zeroed};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::AsRawFd;
use tokio::net::UdpSocket;

pub struct PacketReceiver;

impl PacketReceiver {
    pub fn new(socket: &Socket) -> io::Result<Self> {
        let enabled: libc::c_int = 1;
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_PKTINFO,
                (&raw const enabled).cast(),
                size_of::<libc::c_int>() as libc::socklen_t,
            )
        };

        if result != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self)
    }

    pub fn receive(&self, socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<PacketMetadata> {
        let mut source: libc::sockaddr_in = unsafe { zeroed() };
        let mut control = [0usize; 8];
        let mut vector = libc::iovec {
            iov_base: buffer.as_mut_ptr().cast(),
            iov_len: buffer.len(),
        };
        let mut header: libc::msghdr = unsafe { zeroed() };
        header.msg_name = (&raw mut source).cast();
        header.msg_namelen = size_of::<libc::sockaddr_in>() as libc::socklen_t;
        header.msg_iov = &raw mut vector;
        header.msg_iovlen = 1;
        header.msg_control = control.as_mut_ptr().cast();
        header.msg_controllen = size_of_val(&control) as _;
        let length =
            unsafe { libc::recvmsg(socket.as_raw_fd(), &raw mut header, libc::MSG_DONTWAIT) };

        if length < 0 {
            return Err(io::Error::last_os_error());
        }

        if header.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
            return Err(invalid_packet(
                "received truncated UDP packet or interface metadata",
            ));
        }

        if (header.msg_namelen as usize) < size_of::<libc::sockaddr_in>()
            || libc::c_int::from(source.sin_family) != libc::AF_INET
        {
            return Err(invalid_packet("UDP source address is absent"));
        }

        let mut information = None;
        let mut message = unsafe { libc::CMSG_FIRSTHDR(&raw const header) };

        while !message.is_null() {
            let (level, kind) = unsafe { ((*message).cmsg_level, (*message).cmsg_type) };

            if level == libc::IPPROTO_IP && kind == libc::IP_PKTINFO {
                information = Some(unsafe {
                    std::ptr::read_unaligned(libc::CMSG_DATA(message).cast::<libc::in_pktinfo>())
                });
                break;
            }

            message = unsafe { libc::CMSG_NXTHDR(&raw const header, message) };
        }

        let information =
            information.ok_or_else(|| invalid_packet("UDP receiving interface is absent"))?;
        let interface_index = u32::try_from(i64::from(information.ipi_ifindex))
            .ok()
            .filter(|index| *index != 0)
            .ok_or_else(|| invalid_packet("UDP receiving interface is invalid"))?;

        Ok(PacketMetadata {
            length: length as usize,
            source: SocketAddrV4::new(
                Ipv4Addr::from(source.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(source.sin_port),
            ),
            destination: Ipv4Addr::from(information.ipi_addr.s_addr.to_ne_bytes()),
            interface_index,
        })
    }
}
