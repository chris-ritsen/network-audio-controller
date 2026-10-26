use super::{PacketMetadata, invalid_packet};
use socket2::Socket;
use std::io;
use std::mem::{size_of, size_of_val};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::windows::io::AsRawSocket;
use std::ptr;
use tokio::net::UdpSocket;
use windows_sys::Win32::Networking::WinSock::{
    self, AF_INET, CMSGHDR, IN_PKTINFO, IP_PKTINFO, IPPROTO_IP, LPFN_WSARECVMSG, MSG_CTRUNC,
    MSG_TRUNC, SIO_GET_EXTENSION_FUNCTION_POINTER, SOCKADDR_IN, WSABUF, WSAID_WSARECVMSG, WSAMSG,
};

const CONTROL_WORDS: usize =
    (size_of::<CMSGHDR>() + size_of::<IN_PKTINFO>()).div_ceil(size_of::<usize>());

pub struct PacketReceiver {
    receive_message: LPFN_WSARECVMSG,
}

impl PacketReceiver {
    pub fn new(socket: &Socket) -> io::Result<Self> {
        let enabled: i32 = 1;
        let result = unsafe {
            WinSock::setsockopt(
                socket.as_raw_socket() as _,
                IPPROTO_IP,
                IP_PKTINFO,
                ptr::from_ref(&enabled).cast(),
                size_of_val(&enabled) as i32,
            )
        };
        succeeded(result)?;
        let mut receive_message: LPFN_WSARECVMSG = None;
        let mut returned_bytes = 0;
        let result = unsafe {
            WinSock::WSAIoctl(
                socket.as_raw_socket() as _,
                SIO_GET_EXTENSION_FUNCTION_POINTER,
                ptr::from_ref(&WSAID_WSARECVMSG).cast(),
                size_of_val(&WSAID_WSARECVMSG) as u32,
                ptr::from_mut(&mut receive_message).cast(),
                size_of_val(&receive_message) as u32,
                &mut returned_bytes,
                ptr::null_mut(),
                None,
            )
        };
        succeeded(result)?;

        if returned_bytes as usize != size_of_val(&receive_message) || receive_message.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "WSARecvMsg is unavailable",
            ));
        }

        Ok(Self { receive_message })
    }

    pub fn receive(&self, socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<PacketMetadata> {
        let mut source = SOCKADDR_IN::default();
        let mut control = [0usize; CONTROL_WORDS];
        let mut payload = WSABUF {
            len: buffer
                .len()
                .try_into()
                .map_err(|_| invalid_packet("UDP buffer is too large"))?,
            buf: buffer.as_mut_ptr(),
        };
        let mut message = WSAMSG {
            name: ptr::from_mut(&mut source).cast(),
            namelen: size_of_val(&source) as i32,
            lpBuffers: &mut payload,
            dwBufferCount: 1,
            Control: WSABUF {
                len: size_of_val(&control) as u32,
                buf: control.as_mut_ptr().cast(),
            },
            dwFlags: 0,
        };
        let mut received_bytes = 0;
        let receive_message = self.receive_message.ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "WSARecvMsg is unavailable")
        })?;
        let result = unsafe {
            receive_message(
                socket.as_raw_socket() as _,
                &mut message,
                &mut received_bytes,
                ptr::null_mut(),
                None,
            )
        };
        succeeded(result)?;

        if message.dwFlags & (MSG_TRUNC | MSG_CTRUNC) != 0
            || received_bytes as usize > buffer.len()
            || message.Control.len as usize > size_of_val(&control)
        {
            return Err(invalid_packet(
                "received truncated UDP packet or interface metadata",
            ));
        }

        if source.sin_family != AF_INET || message.namelen as usize != size_of::<SOCKADDR_IN>() {
            return Err(invalid_packet("UDP source address is invalid"));
        }

        let information = packet_information(&control, message.Control.len as usize)?;

        if information.ipi_ifindex == 0 {
            return Err(invalid_packet("UDP receiving interface is invalid"));
        }

        let source_address = unsafe { source.sin_addr.S_un.S_addr };
        let destination_address = unsafe { information.ipi_addr.S_un.S_addr };

        Ok(PacketMetadata {
            length: received_bytes as usize,
            source: SocketAddrV4::new(
                Ipv4Addr::from(source_address.to_ne_bytes()),
                u16::from_be(source.sin_port),
            ),
            destination: Ipv4Addr::from(destination_address.to_ne_bytes()),
            interface_index: information.ipi_ifindex,
        })
    }
}

fn packet_information(control: &[usize], length: usize) -> io::Result<IN_PKTINFO> {
    let header_bytes = size_of::<CMSGHDR>();
    let alignment = size_of::<usize>();
    let mut offset = 0;

    while length.saturating_sub(offset) >= header_bytes {
        let address = unsafe { control.as_ptr().cast::<u8>().add(offset) };
        let header = unsafe { address.cast::<CMSGHDR>().read_unaligned() };

        if header.cmsg_len < header_bytes || header.cmsg_len > length - offset {
            return Err(invalid_packet("UDP interface metadata length is invalid"));
        }

        if header.cmsg_level == IPPROTO_IP && header.cmsg_type == IP_PKTINFO {
            if header.cmsg_len - header_bytes < size_of::<IN_PKTINFO>() {
                return Err(invalid_packet("UDP interface metadata is incomplete"));
            }

            return Ok(unsafe {
                address
                    .add(header_bytes)
                    .cast::<IN_PKTINFO>()
                    .read_unaligned()
            });
        }

        offset += header.cmsg_len.div_ceil(alignment) * alignment;
    }

    Err(invalid_packet("UDP receiving interface is absent"))
}

fn succeeded(result: i32) -> io::Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(unsafe {
            WinSock::WSAGetLastError()
        }))
    }
}
