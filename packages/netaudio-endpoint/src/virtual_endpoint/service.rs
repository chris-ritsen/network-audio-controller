use std::os::linux::net::SocketAddrExt;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::{SocketAddr, UnixDatagram};

use anyhow::{Result, bail};

pub struct Service {
    socket: Option<(UnixDatagram, SocketAddr)>,
    last_status: Option<String>,
}

impl Service {
    pub fn open() -> Result<Self> {
        let socket = std::env::var_os("NOTIFY_SOCKET")
            .map(|path| -> Result<_> {
                let bytes = path.as_bytes();
                let address = match bytes.first() {
                    Some(b'@') => SocketAddr::from_abstract_name(&bytes[1..])?,
                    Some(b'/') => SocketAddr::from_pathname(path)?,
                    _ => bail!("service notification socket must be an absolute or abstract path"),
                };
                let socket = UnixDatagram::unbound()?;
                socket.set_nonblocking(true)?;
                Ok((socket, address))
            })
            .transpose()?;
        Ok(Self {
            socket,
            last_status: None,
        })
    }

    fn send(&self, message: &str) -> Result<()> {
        if let Some((socket, address)) = &self.socket {
            let sent = socket.send_to_addr(message.as_bytes(), address)?;

            if sent != message.len() {
                bail!("service notification was not submitted completely");
            }
        }

        Ok(())
    }

    pub fn ready(&mut self, clock_usable: bool, reason: &str) -> Result<()> {
        let status = Self::status_text(clock_usable, reason);
        self.send(&format!("READY=1\nSTATUS={status}"))?;
        self.last_status = Some(status);
        Ok(())
    }

    pub fn status(&mut self, clock_usable: bool, reason: &str) -> Result<()> {
        let status = Self::status_text(clock_usable, reason);

        if self.last_status.as_ref() != Some(&status) {
            self.send(&format!("STATUS={status}"))?;
            self.last_status = Some(status);
        }

        Ok(())
    }

    fn status_text(clock_usable: bool, reason: &str) -> String {
        if clock_usable {
            "Control service ready; audio adapter open; network clock usable".into()
        } else {
            format!(
                "Control service ready; audio adapter open; media waiting for network clock: {reason}"
            )
        }
    }

    pub fn stopping(&self, failure: Option<&anyhow::Error>) -> Result<()> {
        let status = failure
            .map(|error| format!("Endpoint failed: {error:#}").replace(['\n', '\r'], " "))
            .unwrap_or_else(|| "Endpoint stopping; retiring owned flows".into());
        self.send(&format!("STOPPING=1\nSTATUS={status}"))
    }
}
