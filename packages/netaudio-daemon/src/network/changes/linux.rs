use anyhow::Result;
use netlink_sys::{protocols::NETLINK_ROUTE, Socket, SocketAddr};
use nix::libc::{ENOBUFS, RTMGRP_IPV4_IFADDR, RTMGRP_IPV6_IFADDR, RTMGRP_LINK};
use std::io::ErrorKind;
use tokio::io::unix::AsyncFd;

const GROUPS: u32 = (RTMGRP_IPV4_IFADDR | RTMGRP_IPV6_IFADDR | RTMGRP_LINK) as u32;

pub struct Changes {
    buffer: Vec<u8>,
    socket: AsyncFd<Socket>,
}

impl Changes {
    pub fn new() -> Result<Self> {
        let mut socket = Socket::new(NETLINK_ROUTE)?;
        socket.set_non_blocking(true)?;
        socket.bind(&SocketAddr::new(0, GROUPS))?;
        Ok(Self {
            buffer: vec![0; 65536],
            socket: unsafe { AsyncFd::register(socket) }?,
        })
    }

    pub async fn next(&mut self) -> Result<()> {
        loop {
            let mut ready = self.socket.readable().await?;
            let mut changed = false;
            loop {
                let mut destination = self.buffer.as_mut_slice();
                match ready.get_inner().recv_from(&mut destination, 0) {
                    Ok((_, source)) => changed |= source.port_number() == 0,
                    Err(error) if error.raw_os_error() == Some(ENOBUFS) => changed = true,
                    Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                    Err(error) => return Err(error.into()),
                }
            }
            ready.clear_ready();
            if changed {
                return Ok(());
            }
        }
    }
}
