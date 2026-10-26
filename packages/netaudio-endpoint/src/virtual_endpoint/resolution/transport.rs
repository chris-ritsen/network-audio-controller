use std::io;
use std::mem::{size_of, zeroed};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::AsRawFd;

use anyhow::{Result, bail};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::unix::AsyncFd;

const MULTICAST_ADDRESS: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 251);
const MULTICAST_PORT: u16 = 5353;
const LINK_LOCAL_HOP_LIMIT: u32 = 255;
const DATAGRAM_BYTES: usize = 65_536;
const METADATA_WORDS: usize = 16;

pub struct Transport {
    socket: AsyncFd<Socket>,
    interface_index: u32,
}

struct Datagram {
    bytes: Vec<u8>,
    peer: SocketAddrV4,
    destination: Ipv4Addr,
    interface_index: u32,
}

impl Transport {
    pub fn bind(address: Ipv4Addr, interface_index: u32) -> Result<Self> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_nonblocking(true)?;
        socket.set_reuse_address(true)?;
        socket.set_multicast_all_v4(false)?;
        socket.set_multicast_if_v4(&address)?;
        socket.set_multicast_ttl_v4(LINK_LOCAL_HOP_LIMIT)?;
        socket.set_multicast_loop_v4(true)?;
        socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, MULTICAST_PORT).into())?;
        socket.join_multicast_v4(&MULTICAST_ADDRESS, &address)?;
        let enabled: libc::c_int = 1;
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_PKTINFO,
                (&enabled as *const libc::c_int).cast(),
                size_of::<libc::c_int>() as libc::socklen_t,
            )
        };

        if result != 0 {
            return Err(io::Error::last_os_error().into());
        }

        Ok(Self {
            socket: unsafe { AsyncFd::register(socket)? },
            interface_index,
        })
    }

    pub async fn send(&self, bytes: &[u8]) -> Result<()> {
        let destination = SocketAddrV4::new(MULTICAST_ADDRESS, MULTICAST_PORT);

        loop {
            let mut ready = self.socket.writable().await?;
            let length =
                match ready.try_io(|socket| socket.get_ref().send_to(bytes, &destination.into())) {
                    Ok(result) => result?,
                    Err(_) => continue,
                };

            if length != bytes.len() {
                bail!("discovery socket submitted an incomplete question");
            }

            return Ok(());
        }
    }

    pub async fn receive(&self) -> Result<Vec<u8>> {
        loop {
            let mut ready = self.socket.readable().await?;
            let datagram = match ready.try_io(|socket| receive(socket.get_ref())) {
                Ok(result) => result?,
                Err(_) => continue,
            };

            if datagram.interface_index == self.interface_index
                && datagram.destination == MULTICAST_ADDRESS
                && datagram.peer.port() == MULTICAST_PORT
                && !datagram.peer.ip().is_unspecified()
                && !datagram.peer.ip().is_multicast()
                && !datagram.peer.ip().is_broadcast()
            {
                return Ok(datagram.bytes);
            }
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn receive(socket: &Socket) -> io::Result<Datagram> {
    let mut bytes = vec![0; DATAGRAM_BYTES];
    let mut metadata = [0usize; METADATA_WORDS];
    let mut address: libc::sockaddr_in = unsafe { zeroed() };
    let mut vector = libc::iovec {
        iov_base: bytes.as_mut_ptr().cast(),
        iov_len: bytes.len(),
    };
    let mut message: libc::msghdr = unsafe { zeroed() };
    message.msg_name = (&mut address as *mut libc::sockaddr_in).cast();
    message.msg_namelen = size_of::<libc::sockaddr_in>() as libc::socklen_t;
    message.msg_iov = &mut vector;
    message.msg_iovlen = 1;
    message.msg_control = metadata.as_mut_ptr().cast();
    message.msg_controllen = size_of::<[usize; METADATA_WORDS]>();
    let length = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, libc::MSG_DONTWAIT) };

    if length < 0 {
        return Err(io::Error::last_os_error());
    }

    if message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
        return Err(invalid("discovery datagram or interface metadata was truncated"));
    }

    if message.msg_namelen as usize != size_of::<libc::sockaddr_in>()
        || address.sin_family != libc::AF_INET as libc::sa_family_t
    {
        return Err(invalid("discovery receive peer is not a complete IPv4 address"));
    }

    let mut information = None;
    let mut header = unsafe { libc::CMSG_FIRSTHDR(&message) };

    while !header.is_null() {
        let value = unsafe { &*header };
        let minimum = unsafe { libc::CMSG_LEN(0) } as usize;
        let offset = (header as usize)
            .checked_sub(message.msg_control as usize)
            .ok_or_else(|| invalid("discovery metadata pointer precedes its buffer"))?;

        if value.cmsg_len < minimum
            || offset.checked_add(value.cmsg_len).is_none_or(|end| end > message.msg_controllen)
        {
            return Err(invalid("discovery metadata extent is invalid"));
        }

        if value.cmsg_level == libc::IPPROTO_IP && value.cmsg_type == libc::IP_PKTINFO {
            if value.cmsg_len - minimum != size_of::<libc::in_pktinfo>() {
                return Err(invalid("discovery interface metadata is incomplete"));
            }

            let current =
                unsafe { libc::CMSG_DATA(header).cast::<libc::in_pktinfo>().read_unaligned() };

            if information.replace(current).is_some() {
                return Err(invalid("discovery interface metadata is duplicated"));
            }
        }

        header = unsafe { libc::CMSG_NXTHDR(&message, header) };
    }

    let information = information.ok_or_else(|| invalid("discovery interface is unavailable"))?;
    bytes.truncate(length as usize);

    Ok(Datagram {
        bytes,
        peer: SocketAddrV4::new(
            Ipv4Addr::from(address.sin_addr.s_addr.to_ne_bytes()),
            u16::from_be(address.sin_port),
        ),
        destination: Ipv4Addr::from(information.ipi_addr.s_addr.to_ne_bytes()),
        interface_index: information
            .ipi_ifindex
            .try_into()
            .map_err(|_| invalid("discovery interface index is negative"))?,
    })
}
