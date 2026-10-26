use anyhow::{Result, ensure};
use netlink_sys::{Socket, SocketAddr, protocols::NETLINK_KOBJECT_UEVENT};
use tokio::io::unix::AsyncFd;

pub struct Observer {
    socket: AsyncFd<Socket>,
    buffer: Vec<u8>,
    sequence: Option<u64>,
}

impl Observer {
    pub fn new() -> Result<Self> {
        let mut socket = Socket::new(NETLINK_KOBJECT_UEVENT)?;
        socket.set_non_blocking(true)?;
        socket.bind(&SocketAddr::new(0, 1))?;
        Ok(Self {
            socket: unsafe { AsyncFd::register(socket) }?,
            buffer: vec![0; 65536],
            sequence: None,
        })
    }

    pub async fn changed(&mut self) -> Result<()> {
        loop {
            let mut ready = self.socket.readable().await?;
            let mut destination = self.buffer.as_mut_slice();
            let received = ready.try_io(|socket| {
                socket
                    .get_ref()
                    .recv_from(&mut destination, nix::libc::MSG_TRUNC)
            });
            let (length, source) = match received {
                Err(_) => continue,
                Ok(Err(error)) if error.raw_os_error() == Some(nix::libc::ENOBUFS) => {
                    self.sequence = None;
                    return Ok(());
                }
                Ok(result) => result?,
            };
            if source.port_number() != 0 {
                continue;
            }
            ensure!(
                length <= self.buffer.len(),
                "Sound card event exceeded its message budget"
            );
            let fields: Vec<_> = self.buffer[..length].split(|byte| *byte == 0).collect();
            let sequence = fields
                .iter()
                .find_map(|field| field.strip_prefix(b"SEQNUM="))
                .and_then(|sequence| std::str::from_utf8(sequence).ok())
                .and_then(|sequence| sequence.parse::<u64>().ok());
            let missed = match (self.sequence, sequence) {
                (Some(previous), Some(current)) => previous.checked_add(1) != Some(current),
                _ => false,
            };
            self.sequence = sequence;
            if missed || fields.contains(&b"SUBSYSTEM=sound".as_slice()) {
                return Ok(());
            }
        }
    }
}
