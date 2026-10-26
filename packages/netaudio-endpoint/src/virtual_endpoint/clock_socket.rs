use std::io;
use std::mem::{size_of, zeroed};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::AsRawFd;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::clock::NANOSECONDS_PER_SECOND;
use netaudio_core::native_audio::clock::discipline::LocalTimestamp;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

use super::sockets::Monotonic;

const DATAGRAM_BUFFER_BYTES: usize = 65_536;
const CONTROL_BUFFER_WORDS: usize = 64;
const SOFTWARE_TIMESTAMP_INDEX: usize = 0;
const TIMESTAMP_COUNT: usize = 3;
const TRANSMIT_TIMESTAMP_KIND: u32 = 0;
const MAXIMUM_CLOCK_MAPPING_CHANGE_NANOSECONDS: i128 = 2_000;

pub struct ClockSocket {
    socket: AsyncFd<Socket>,
    next_transmit_identifier: Option<u32>,
}

pub enum Event {
    Received {
        bytes: Vec<u8>,
        peer: SocketAddrV4,
        interface_index: u32,
        realtime_nanoseconds: u64,
    },
    Transmitted {
        identifier: u32,
        realtime_nanoseconds: u64,
    },
}

struct Ancillary {
    timestamp: Option<u64>,
    interface_index: Option<u32>,
    transmit_identifier: Option<u32>,
}

impl ClockSocket {
    pub fn bind(address: Ipv4Addr, group: Ipv4Addr, port: u16) -> Result<Self> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_nonblocking(true)?;
        socket.set_multicast_all_v4(false)?;
        socket.set_multicast_if_v4(&address)?;
        socket.set_multicast_loop_v4(false)?;
        socket.set_recv_buffer_size(256 * 1024)?;
        socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port).into()).with_context(|| {
            format!("bind exclusive PTP UDP port {port}; privileged ports require bind permission")
        })?;
        socket.join_multicast_v4(&group, &address)?;

        set_option(&socket, libc::SOL_IP, libc::IP_PKTINFO, 1)?;
        set_option(&socket, libc::SOL_IP, libc::IP_RECVERR, 1)?;

        let flags = libc::SOF_TIMESTAMPING_RX_SOFTWARE
            | libc::SOF_TIMESTAMPING_TX_SOFTWARE
            | libc::SOF_TIMESTAMPING_SOFTWARE
            | libc::SOF_TIMESTAMPING_OPT_ID
            | libc::SOF_TIMESTAMPING_OPT_TSONLY;
        set_option(&socket, libc::SOL_SOCKET, libc::SO_TIMESTAMPING, flags as i32)?;

        Ok(Self {
            socket: unsafe { AsyncFd::register(socket)? },
            next_transmit_identifier: Some(0),
        })
    }

    pub async fn send(&mut self, bytes: &[u8], destination: SocketAddrV4) -> Result<u32> {
        let identifier = self
            .next_transmit_identifier
            .context("clock socket transmit identifier space is exhausted")?;

        loop {
            let mut ready = self.socket.writable().await?;
            let written =
                match ready.try_io(|socket| socket.get_ref().send_to(bytes, &destination.into())) {
                    Ok(result) => result?,
                    Err(_) => continue,
                };

            self.next_transmit_identifier = identifier.checked_add(1);

            if written != bytes.len() {
                bail!("clock socket submitted an incomplete datagram");
            }

            return Ok(identifier);
        }
    }

    pub fn try_send(&mut self, bytes: &[u8], destination: SocketAddrV4) -> Result<u32> {
        let identifier = self
            .next_transmit_identifier
            .context("clock socket transmit identifier space is exhausted")?;
        let written = self.socket.get_ref().send_to(bytes, &destination.into())?;
        self.next_transmit_identifier = identifier.checked_add(1);

        if written != bytes.len() {
            bail!("clock socket submitted an incomplete datagram");
        }

        Ok(identifier)
    }

    pub async fn receive(&self) -> Result<Event> {
        tokio::select! {
            result = self.receive_queue(Interest::ERROR, true) => result,
            result = self.receive_queue(Interest::READABLE, false) => result,
        }
    }

    async fn receive_queue(&self, interest: Interest, error_queue: bool) -> Result<Event> {
        loop {
            let mut ready = self.socket.ready(interest).await?;
            let result = ready.try_io(|socket| receive_message(socket.get_ref(), error_queue));

            match result {
                Ok(result) => return Ok(result?),
                Err(_) => continue,
            }
        }
    }
}

fn set_option(socket: &Socket, level: i32, option: i32, value: i32) -> io::Result<()> {
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            option,
            (&value as *const i32).cast(),
            size_of::<i32>() as libc::socklen_t,
        )
    };

    if result != 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn nanoseconds(timestamp: libc::timespec) -> io::Result<u64> {
    let seconds = u64::try_from(timestamp.tv_sec)
        .map_err(|_| invalid("kernel clock timestamp has negative seconds"))?;
    let fraction = u64::try_from(timestamp.tv_nsec)
        .map_err(|_| invalid("kernel clock timestamp has negative nanoseconds"))?;

    if fraction >= NANOSECONDS_PER_SECOND {
        return Err(invalid("kernel clock timestamp is not normalized"));
    }

    seconds
        .checked_mul(NANOSECONDS_PER_SECOND)
        .and_then(|value| value.checked_add(fraction))
        .filter(|value| *value != 0)
        .ok_or_else(|| invalid("kernel clock timestamp is absent or unrepresentable"))
}

fn receive_message(socket: &Socket, error_queue: bool) -> io::Result<Event> {
    let mut bytes = vec![0; DATAGRAM_BUFFER_BYTES];
    let mut control = [0usize; CONTROL_BUFFER_WORDS];
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
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = size_of::<[usize; CONTROL_BUFFER_WORDS]>();

    let flags = libc::MSG_DONTWAIT
        | if error_queue {
            libc::MSG_ERRQUEUE
        } else {
            0
        };
    let length = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, flags) };

    if length < 0 {
        return Err(io::Error::last_os_error());
    }

    if message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
        return Err(invalid("clock datagram or kernel metadata was truncated"));
    }

    let metadata = ancillary(&message)?;
    let realtime_nanoseconds = metadata
        .timestamp
        .ok_or_else(|| invalid("clock datagram lacks its kernel software timestamp"))?;

    if error_queue {
        return Ok(Event::Transmitted {
            identifier: metadata
                .transmit_identifier
                .ok_or_else(|| invalid("clock transmit timestamp lacks its socket identifier"))?,
            realtime_nanoseconds,
        });
    }

    if message.msg_namelen as usize != size_of::<libc::sockaddr_in>()
        || address.sin_family != libc::AF_INET as libc::sa_family_t
    {
        return Err(invalid("clock receive peer is not a complete IPv4 address"));
    }

    bytes.truncate(length as usize);

    Ok(Event::Received {
        bytes,
        peer: SocketAddrV4::new(
            Ipv4Addr::from(address.sin_addr.s_addr.to_ne_bytes()),
            u16::from_be(address.sin_port),
        ),
        interface_index: metadata
            .interface_index
            .ok_or_else(|| invalid("clock receive timestamp lacks its interface identity"))?,
        realtime_nanoseconds,
    })
}

fn ancillary(message: &libc::msghdr) -> io::Result<Ancillary> {
    let mut result = Ancillary {
        timestamp: None,
        interface_index: None,
        transmit_identifier: None,
    };
    let mut header = unsafe { libc::CMSG_FIRSTHDR(message) };

    while !header.is_null() {
        let value = unsafe { &*header };
        let minimum = unsafe { libc::CMSG_LEN(0) } as usize;
        let offset = (header as usize)
            .checked_sub(message.msg_control as usize)
            .ok_or_else(|| invalid("kernel metadata pointer precedes its buffer"))?;

        if value.cmsg_len < minimum
            || offset.checked_add(value.cmsg_len).is_none_or(|end| end > message.msg_controllen)
        {
            return Err(invalid("kernel metadata extent is invalid"));
        }

        let data = unsafe {
            std::slice::from_raw_parts(libc::CMSG_DATA(header), value.cmsg_len - minimum)
        };

        match (value.cmsg_level, value.cmsg_type) {
            (libc::SOL_SOCKET, libc::SCM_TIMESTAMPING) => {
                if data.len() < size_of::<[libc::timespec; TIMESTAMP_COUNT]>() {
                    return Err(invalid("kernel timestamp vector is incomplete"));
                }

                let timestamps = unsafe {
                    data.as_ptr().cast::<[libc::timespec; TIMESTAMP_COUNT]>().read_unaligned()
                };
                let timestamp = nanoseconds(timestamps[SOFTWARE_TIMESTAMP_INDEX])?;

                if result.timestamp.replace(timestamp).is_some() {
                    return Err(invalid("kernel timestamp vector is duplicated"));
                }
            }
            (libc::SOL_IP, libc::IP_PKTINFO) => {
                if data.len() < size_of::<libc::in_pktinfo>() {
                    return Err(invalid("kernel interface metadata is incomplete"));
                }

                let information =
                    unsafe { data.as_ptr().cast::<libc::in_pktinfo>().read_unaligned() };
                let index = u32::try_from(information.ipi_ifindex)
                    .map_err(|_| invalid("kernel interface index is negative"))?;

                if index == 0 || result.interface_index.replace(index).is_some() {
                    return Err(invalid("kernel interface identity is absent or duplicated"));
                }
            }
            (libc::SOL_IP, libc::IP_RECVERR) => {
                if data.len() < size_of::<libc::sock_extended_err>() {
                    return Err(invalid("kernel transmit metadata is incomplete"));
                }

                let information =
                    unsafe { data.as_ptr().cast::<libc::sock_extended_err>().read_unaligned() };

                if information.ee_errno != libc::ENOMSG as u32
                    || information.ee_origin != libc::SO_EE_ORIGIN_TIMESTAMPING
                    || information.ee_info != TRANSMIT_TIMESTAMP_KIND
                {
                    return Err(invalid("clock socket reported a network or timestamp failure"));
                }

                if result.transmit_identifier.replace(information.ee_data).is_some() {
                    return Err(invalid("kernel transmit identity is duplicated"));
                }
            }
            _ => {}
        }

        header = unsafe { libc::CMSG_NXTHDR(message, header) };
    }

    Ok(result)
}

pub struct ClockMapping {
    time: Monotonic,
    realtime_offset: i128,
    initial_uncertainty: u32,
}

impl ClockMapping {
    pub fn new(time: Monotonic) -> Result<Self> {
        let (realtime_offset, initial_uncertainty) = Self::measure(&time)?;

        Ok(Self {
            time,
            realtime_offset,
            initial_uncertainty,
        })
    }

    fn measure(time: &Monotonic) -> Result<(i128, u32)> {
        let before = time.now();
        let realtime = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let after = time.now();
        let sample = time.stamp(before, after)?;
        let offset = i128::try_from(realtime)? - i128::from(sample.nanoseconds);
        Ok((offset, sample.uncertainty_nanoseconds))
    }

    pub fn project(&self, realtime_nanoseconds: u64) -> Result<LocalTimestamp> {
        let (offset, uncertainty) = Self::measure(&self.time)?;
        let mapping_change = (offset - self.realtime_offset).abs();
        let observation_uncertainty = uncertainty.max(self.initial_uncertainty);

        if mapping_change
            > MAXIMUM_CLOCK_MAPPING_CHANGE_NANOSECONDS + i128::from(observation_uncertainty)
        {
            bail!("system realtime clock moved relative to the monotonic timestamp provider");
        }

        let nanoseconds = u64::try_from(i128::from(realtime_nanoseconds) - offset)
            .context("kernel timestamp precedes this clock provider incarnation")?;
        let now = self.time.now();

        if nanoseconds > now {
            bail!("kernel timestamp is ahead of the monotonic timestamp provider");
        }

        let mut timestamp = self.time.stamp(nanoseconds, nanoseconds)?;
        timestamp.uncertainty_nanoseconds = observation_uncertainty
            .checked_add(u32::try_from(MAXIMUM_CLOCK_MAPPING_CHANGE_NANOSECONDS)?)
            .context("kernel timestamp mapping uncertainty overflow")?;
        Ok(timestamp)
    }
}
