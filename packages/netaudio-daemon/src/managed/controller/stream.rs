use super::Session;
use crate::managed::{Credential, Error};
use chrono::{DateTime, Utc};
use netaudio_core::dapi::{self, SignalPresencePublication};
use netaudio_core::managed_session::{self, ManagedOperation, ManagedSessionRequest};
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Instant, timeout_at};

pub struct Signal {
    pub publication: SignalPresencePublication,
    pub frame: Arc<[u8]>,
    pub source: SocketAddr,
    pub received_at: DateTime<Utc>,
    pub received_monotonic: Instant,
}

pub enum StreamEvent {
    Ready,
    Signal(Signal),
}

impl Session {
    pub async fn monitor(
        mut self,
        credential: &Credential,
        expected_domain: String,
        mut receive: impl FnMut(StreamEvent) -> Result<ControlFlow<()>, Error>,
    ) -> Result<(), Error> {
        let mut connection = self
            .connection
            .take()
            .ok_or(Error::Invalid("Controller session is closed"))?;
        let source = connection
            .stream
            .get_ref()
            .0
            .peer_addr()
            .map_err(|_| Error::Transport)?;
        let startup_deadline = Instant::now() + self.limits.timeout;
        let mut initialized = connection.state.is_some();
        let mut announced = false;
        let mut received_bytes = 0;
        let mut frame = Vec::new();
        let mut frame_deadline = None;
        let mut received_at = Utc::now();
        let mut received_monotonic = Instant::now();
        let mut request = ManagedSessionRequest::Begin {
            state: connection.state.take(),
            credential: timeout_at(startup_deadline, credential.read())
                .await
                .map_err(|_| Error::Deadline)??,
            local_ipv4: connection.address,
            notification_port: connection.notification_port,
            expected_domain_id: Some(expected_domain),
            operation: ManagedOperation::MonitorSignals,
        };

        loop {
            let step = managed_session::advance(request).map_err(Error::Session)?;
            let complete_frame = dapi::parse_frame(&frame).is_some();

            if complete_frame && dapi::parse_session_description(&frame).is_some() {
                initialized = true;
            }

            let deadline = if announced {
                frame_deadline.unwrap_or_else(|| Instant::now() + self.limits.timeout)
            } else {
                startup_deadline
            };

            for outgoing in step.outgoing {
                timeout_at(deadline, connection.stream.write_all(&outgoing))
                    .await
                    .map_err(|_| Error::Deadline)?
                    .map_err(|_| Error::Transport)?;
            }

            if initialized && !announced {
                announced = true;

                if receive(StreamEvent::Ready)?.is_break() {
                    return Ok(());
                }
            }

            if let Some(publication) = step.signal_presence {
                if !complete_frame {
                    return Err(Error::Response(
                        "Controller signal publication has no complete source frame",
                    ));
                }

                let event = StreamEvent::Signal(Signal {
                    publication,
                    frame: Arc::from(std::mem::take(&mut frame)),
                    source,
                    received_at,
                    received_monotonic,
                });

                if receive(event)?.is_break() {
                    return Ok(());
                }
            }

            if complete_frame {
                frame.clear();
                frame_deadline = None;
            }

            let available = if initialized {
                self.limits.maximum_response_bytes - frame.len()
            } else {
                self.limits.maximum_response_bytes - received_bytes
            };

            if step.complete || step.receive_bytes == 0 || step.receive_bytes > available {
                return Err(Error::TooLarge);
            }

            let mut bytes = vec![0; step.receive_bytes];
            let deadline = if initialized {
                if frame.is_empty() {
                    connection
                        .stream
                        .read_exact(&mut bytes[..1])
                        .await
                        .map_err(|_| Error::Transport)?;
                    let deadline = Instant::now() + self.limits.timeout;
                    frame_deadline = Some(deadline);
                    timeout_at(deadline, connection.stream.read_exact(&mut bytes[1..]))
                        .await
                        .map_err(|_| Error::Deadline)?
                        .map_err(|_| Error::Transport)?;
                    None
                } else {
                    frame_deadline
                }
            } else {
                Some(startup_deadline)
            };

            if let Some(deadline) = deadline {
                timeout_at(deadline, connection.stream.read_exact(&mut bytes))
                    .await
                    .map_err(|_| Error::Deadline)?
                    .map_err(|_| Error::Transport)?;
            }

            received_at = Utc::now();
            received_monotonic = Instant::now();

            if !initialized {
                received_bytes += bytes.len();
            }

            frame.extend_from_slice(&bytes);
            request = ManagedSessionRequest::Receive {
                state: step.state,
                data: bytes,
            };
        }
    }
}
