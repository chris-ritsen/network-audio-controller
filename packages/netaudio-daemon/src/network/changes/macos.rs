use anyhow::Result;
use nix::libc::{AF_ROUTE, RTM_DELADDR, RTM_IFINFO, RTM_NEWADDR};
use socket2::{Domain, Socket, Type};
use std::io::{ErrorKind, Read};
use tokio::io::unix::AsyncFd;

pub struct Changes {
    buffer: Vec<u8>,
    socket: AsyncFd<Socket>,
}

impl Changes {
    pub fn new() -> Result<Self> {
        let socket = Socket::new(Domain::from(AF_ROUTE), Type::RAW, None)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            buffer: vec![0; 8192],
            socket: unsafe { AsyncFd::register(socket) }?,
        })
    }

    pub async fn next(&mut self) -> Result<()> {
        loop {
            let mut ready = self.socket.readable().await?;
            let mut changed = false;
            loop {
                let mut socket = ready.get_inner();
                match socket.read(&mut self.buffer) {
                    Ok(length) if length >= 4 => {
                        changed |= matches!(
                            i32::from(self.buffer[3]),
                            RTM_DELADDR | RTM_IFINFO | RTM_NEWADDR
                        );
                    }
                    Ok(_) => {}
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
