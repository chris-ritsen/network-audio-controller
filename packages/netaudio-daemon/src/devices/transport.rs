use netaudio_core::protocol::{NetaudioError, NetaudioPort, next_message_id, response_envelope};
use netaudio_core::spec::{IoMode, Target, build_routed_command};
use serde::Serialize;
use serde_json::Value;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout_at};

const MAXIMUM_DATAGRAM_BYTES: usize = u16::MAX as usize;
const RESPONSE_MESSAGE_IDENTIFIER_OFFSET: usize = 4;
const MESSAGE_IDENTIFIER_BYTES: usize = 2;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_READ_ATTEMPTS: u32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct DeviceAddress {
    pub address: Ipv4Addr,
    pub routing_port: u16,
    pub monitoring_port: u16,
    pub settings_port: u16,
}

impl DeviceAddress {
    pub fn new(address: Ipv4Addr, routing_port: u16) -> Self {
        Self {
            address,
            routing_port,
            monitoring_port: NetaudioPort::Control as u16,
            settings_port: NetaudioPort::Settings as u16,
        }
    }

    pub fn destination(self, target: Target) -> SocketAddr {
        let port = match target {
            Target::Arc => self.routing_port,
            Target::Control => self.monitoring_port,
            Target::Settings => self.settings_port,
        };
        SocketAddr::from((self.address, port))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandIntent {
    Read,
    Write,
}

#[derive(Debug)]
pub enum TransportError {
    InvalidCommand(String),
    Network(io::Error),
    ReadTimeout,
    WriteOutcomeUnknown(String),
    MalformedResponse(&'static str),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCommand(message) => write!(formatter, "invalid device command: {message}"),
            Self::Network(error) => write!(formatter, "device network error: {error}"),
            Self::ReadTimeout => formatter.write_str("device read timed out"),
            Self::WriteOutcomeUnknown(message) => {
                write!(formatter, "device write outcome is unknown: {message}")
            }
            Self::MalformedResponse(operation) => {
                write!(formatter, "malformed device response to {operation}")
            }
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Network(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for TransportError {
    fn from(error: io::Error) -> Self {
        Self::Network(error)
    }
}

impl From<NetaudioError> for TransportError {
    fn from(error: NetaudioError) -> Self {
        Self::InvalidCommand(error.to_string())
    }
}

pub struct DeviceTransport {
    socket: UdpSocket,
    address: DeviceAddress,
    host_mac: Option<[u8; 6]>,
    next_identifier: u16,
    request_timeout: Duration,
    read_attempts: u32,
    receive_buffer: Vec<u8>,
    local: SocketAddr,
    observer: Option<crate::capture::Observer>,
}

impl DeviceTransport {
    pub async fn connect(
        address: DeviceAddress,
        local_address: Option<Ipv4Addr>,
    ) -> Result<Self, TransportError> {
        if address.address.is_unspecified()
            || address.address.is_multicast()
            || address.address.is_broadcast()
            || address.routing_port == 0
        {
            return Err(TransportError::InvalidCommand(
                "a unicast IPv4 address and routing port are required".to_owned(),
            ));
        }

        let local_address = local_address
            .or_else(|| netaudio_core::netif::source_ipv4_for(address.address))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NetworkUnreachable, "no IPv4 network path")
            })?;
        let socket = UdpSocket::bind((local_address, 0)).await?;
        let local = socket.local_addr()?;

        Ok(Self {
            socket,
            address,
            host_mac: netaudio_core::netif::host_mac_for_ipv4(local_address),
            next_identifier: 0,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            read_attempts: DEFAULT_READ_ATTEMPTS,
            receive_buffer: vec![0; MAXIMUM_DATAGRAM_BYTES],
            local,
            observer: None,
        })
    }

    pub fn with_observer(mut self, observer: Option<crate::capture::Observer>) -> Self {
        self.observer = observer;
        self
    }

    fn observe(
        &self,
        payload: &[u8],
        source: SocketAddr,
        destination: SocketAddr,
        direction: &'static str,
    ) {
        if let Some(observer) = &self.observer {
            observer.record(crate::capture::Packet {
                observed_at: chrono::Utc::now(),
                source,
                destination,
                direction,
                payload: std::sync::Arc::from(payload),
            });
        }
    }

    pub fn set_request_policy(
        &mut self,
        timeout: Duration,
        read_attempts: u32,
    ) -> Result<(), TransportError> {
        if timeout.is_zero() || read_attempts == 0 {
            return Err(TransportError::InvalidCommand(
                "request timeout and read attempts must be positive".to_owned(),
            ));
        }

        self.request_timeout = timeout;
        self.read_attempts = read_attempts;
        Ok(())
    }

    pub fn address(&self) -> DeviceAddress {
        self.address
    }

    pub fn set_settings_port(&mut self, port: u16) -> Result<(), TransportError> {
        if port == 0 {
            return Err(TransportError::InvalidCommand(
                "settings port must be nonzero".to_owned(),
            ));
        }

        self.address.settings_port = port;
        Ok(())
    }

    pub fn local_address(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub async fn execute(
        &mut self,
        command: &Value,
        intent: CommandIntent,
    ) -> Result<Vec<u8>, TransportError> {
        let prepared = self.prepare(command)?;
        let destination = self.address.destination(prepared.target);

        match prepared.io {
            IoMode::Request => {
                let operation = expected_operation(&prepared.packet, prepared.target)?;
                self.exchange(
                    &prepared.packet,
                    prepared.message_id,
                    destination,
                    intent,
                    operation,
                )
                .await
            }
            IoMode::Fire {
                repeat,
                interval_ms,
            } => {
                let transmissions = repeat.max(1);

                for transmission in 0..transmissions {
                    if let Err(error) = self.socket.send_to(&prepared.packet, destination).await {
                        return Err(if transmission == 0 || intent == CommandIntent::Read {
                            TransportError::Network(error)
                        } else {
                            TransportError::WriteOutcomeUnknown(error.to_string())
                        });
                    }
                    self.observe(&prepared.packet, self.local, destination, "request");

                    if transmission + 1 < transmissions && interval_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                    }
                }

                Ok(Vec::new())
            }
        }
    }

    fn prepare(&mut self, command: &Value) -> Result<netaudio_core::spec::Routed, TransportError> {
        let serialized = serde_json::to_string(command)
            .map_err(|error| TransportError::InvalidCommand(error.to_string()))?;
        let next_identifier = &mut self.next_identifier;
        let prepared = build_routed_command(&serialized, self.host_mac, || {
            *next_identifier = next_message_id(*next_identifier);
            *next_identifier
        })
        .map_err(|error| TransportError::InvalidCommand(error.to_string()))?;
        Ok(prepared)
    }

    pub async fn send_command(&mut self, command: &Value) -> Result<(), TransportError> {
        let prepared = self.prepare(command)?;
        self.socket
            .send_to(&prepared.packet, self.address.destination(prepared.target))
            .await?;
        self.observe(
            &prepared.packet,
            self.local,
            self.address.destination(prepared.target),
            "request",
        );
        Ok(())
    }

    pub async fn request_command(
        &mut self,
        command: &Value,
        intent: CommandIntent,
    ) -> Result<Vec<u8>, TransportError> {
        let prepared = self.prepare(command)?;
        let envelope = response_envelope(&prepared.packet).ok_or_else(|| {
            TransportError::InvalidCommand("request requires a command envelope".to_owned())
        })?;
        self.exchange(
            &prepared.packet,
            prepared.message_id,
            self.address.destination(prepared.target),
            intent,
            Some(envelope.opcode),
        )
        .await
    }

    pub async fn request(
        &mut self,
        build: impl FnOnce(u16) -> Result<Vec<u8>, NetaudioError>,
        target: Target,
        intent: CommandIntent,
    ) -> Result<Vec<u8>, TransportError> {
        self.next_identifier = next_message_id(self.next_identifier);
        let packet = build(self.next_identifier)?;
        let operation = expected_operation(&packet, target)?;
        self.exchange(
            &packet,
            self.next_identifier,
            self.address.destination(target),
            intent,
            operation,
        )
        .await
    }

    async fn exchange(
        &mut self,
        packet: &[u8],
        message_identifier: u16,
        destination: SocketAddr,
        intent: CommandIntent,
        expected_operation: Option<u16>,
    ) -> Result<Vec<u8>, TransportError> {
        let attempts = match intent {
            CommandIntent::Read => self.read_attempts,
            CommandIntent::Write => 1,
        };

        for _ in 0..attempts {
            self.socket.send_to(packet, destination).await?;
            self.observe(packet, self.local, destination, "request");
            let deadline = Instant::now() + self.request_timeout;

            loop {
                let received =
                    timeout_at(deadline, self.socket.recv_from(&mut self.receive_buffer)).await;
                let (length, source) = match received {
                    Ok(Ok(received)) => received,
                    Ok(Err(error)) => {
                        return Err(match intent {
                            CommandIntent::Read => TransportError::Network(error),
                            CommandIntent::Write => {
                                TransportError::WriteOutcomeUnknown(error.to_string())
                            }
                        });
                    }
                    Err(_) => break,
                };
                let response = &self.receive_buffer[..length];
                self.observe(response, source, self.local, "response");
                let identifier_end = RESPONSE_MESSAGE_IDENTIFIER_OFFSET + MESSAGE_IDENTIFIER_BYTES;
                let received_identifier =
                    response.get(RESPONSE_MESSAGE_IDENTIFIER_OFFSET..identifier_end);

                if source.ip() == destination.ip()
                    && received_identifier == Some(message_identifier.to_be_bytes().as_slice())
                    && expected_operation.is_none_or(|operation| {
                        response_envelope(response)
                            .is_some_and(|response| matching_operation(operation, response.opcode))
                    })
                {
                    return Ok(response.to_vec());
                }
            }
        }

        Err(match intent {
            CommandIntent::Read => TransportError::ReadTimeout,
            CommandIntent::Write => TransportError::WriteOutcomeUnknown(
                "no matching response arrived before the deadline; command was not retried"
                    .to_owned(),
            ),
        })
    }
}

fn matching_operation(requested: u16, received: u16) -> bool {
    requested == received
        || (requested == netaudio_core::commands::OPCODE_QUERY_TX_FLOWS_2809
            && received == netaudio_core::commands::OPCODE_QUERY_TX_FLOWS)
}

fn expected_operation(packet: &[u8], target: Target) -> Result<Option<u16>, TransportError> {
    if target == Target::Arc {
        response_envelope(packet)
            .map(|envelope| Some(envelope.opcode))
            .ok_or_else(|| {
                TransportError::InvalidCommand("invalid ARC command envelope".to_owned())
            })
    } else {
        Ok(None)
    }
}
