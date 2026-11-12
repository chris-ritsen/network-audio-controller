use super::{Credential, Error, Limits, Transport};
use netaudio_core::controller_api::{self, ControllerEndpoints, ControllerLogin};
use netaudio_core::managed_session::{
    self, ManagedOperation, ManagedSessionRequest, ManagedSessionState,
};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE};
use reqwest::{Method, Url};
use rustls::pki_types::ServerName;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU16;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::OwnedSemaphorePermit;
use tokio_rustls::{client::TlsStream, TlsConnector};

mod stream;
pub use stream::{Lookup, Signal, StreamEvent};

#[derive(Clone)]
pub struct Controller {
    origin: Url,
    transport: Transport,
    limits: Limits,
}

impl Controller {
    pub fn new(
        server: &str,
        port: NonZeroU16,
        transport: Transport,
        limits: Limits,
    ) -> Result<Self, Error> {
        if server.is_empty() || server.contains(['/', '@', '?', '#']) {
            return Err(Error::Invalid(
                "Controller server must be a hostname or IP address",
            ));
        }

        let mut origin = Url::parse("https://localhost").expect("fixed HTTPS URL");
        origin
            .set_host(Some(server.trim_end_matches('.')))
            .map_err(|_| Error::Invalid("Controller hostname is invalid"))?;
        origin
            .set_port(Some(port.get()))
            .map_err(|_| Error::Invalid("Controller authentication port is invalid"))?;

        Ok(Self {
            origin,
            transport,
            limits: Limits {
                maximum_response_bytes: limits.maximum_response_bytes.min(1024 * 1024),
                ..limits.validate()?
            },
        })
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, Error> {
        let mut url = self.origin.clone();
        url.set_path(path);
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

        if body.is_some() {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/x-www-form-urlencoded"),
            );
        }

        self.transport
            .request(method, url, headers, body.unwrap_or_default(), self.limits)
            .await
    }

    async fn bootstrap(
        &self,
    ) -> Result<(controller_api::ControllerApiRoutes, ControllerEndpoints), Error> {
        let versions = self
            .request(Method::GET, controller_api::VERSIONS_PATH, None)
            .await?;
        let routes = controller_api::routes(&versions).map_err(Error::Response)?;
        let advertised = self.request(Method::GET, routes.endpoints, None).await?;
        let endpoints = controller_api::endpoints(&advertised).map_err(Error::Response)?;
        Ok((routes, endpoints))
    }

    pub async fn endpoints(&self) -> Result<ControllerEndpoints, Error> {
        tokio::time::timeout(self.limits.timeout, self.bootstrap())
            .await
            .map_err(|_| Error::Deadline)?
            .map(|(_, endpoints)| endpoints)
    }

    pub async fn login(&self, username: &str, password: &str) -> Result<ControllerLogin, Error> {
        if username.is_empty() || password.is_empty() {
            return Err(Error::Invalid(
                "Username and password must be non-empty strings",
            ));
        }

        let exchange = async {
            let (routes, advertised) = self.bootstrap().await?;
            let body = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("username", username)
                .append_pair("password", password)
                .finish()
                .into_bytes();
            let response = self.request(Method::POST, routes.login, Some(body)).await?;
            let login = controller_api::login(&response)
                .map_err(Error::Response)?
                .ok_or(Error::Authentication)?;

            if login.endpoints.service_port != advertised.service_port
                || login.endpoints.device_port != advertised.device_port
                || login.endpoints.graphql_url != advertised.graphql_url
            {
                return Err(Error::Response(
                    "DDM changed its advertised endpoints during login",
                ));
            }

            Ok(login)
        };
        tokio::time::timeout(self.limits.timeout, exchange)
            .await
            .map_err(|_| Error::Deadline)?
    }

    pub async fn connect(&self, port: NonZeroU16) -> Result<Session, Error> {
        let host = self
            .origin
            .host_str()
            .ok_or(Error::Invalid("Controller hostname is missing"))?
            .to_owned();
        let permit = Arc::clone(&self.transport.capacity)
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let connection = async {
            let addresses = tokio::net::lookup_host((host.as_str(), port.get()))
                .await
                .map_err(|error| Error::Transport(format!("could not resolve {host}: {error}")))?;
            let mut connection = None;
            let mut failure = None;

            for address in addresses.filter(SocketAddr::is_ipv4) {
                match TcpStream::connect(address).await {
                    Ok(stream) => {
                        connection = Some(stream);
                        break;
                    }
                    Err(error) => {
                        failure = Some(format!("could not connect to {address}: {error}"))
                    }
                }
            }

            let stream = connection.ok_or_else(|| {
                Error::Transport(failure.unwrap_or_else(|| format!("{host} has no IPv4 address")))
            })?;
            let local = stream.local_addr().map_err(|error| {
                Error::Transport(format!(
                    "could not read the local address of the connection to {host}: {error}"
                ))
            })?;
            let IpAddr::V4(address) = local.ip() else {
                return Err(Error::Invalid(
                    "Controller session requires an IPv4 notification address",
                ));
            };
            let name = ServerName::try_from(host.clone())
                .map_err(|_| Error::Invalid("Controller TLS hostname is invalid"))?;
            let stream = TlsConnector::from(Arc::clone(&self.transport.tls))
                .connect(name, stream)
                .await
                .map_err(|error| {
                    if error.kind() == std::io::ErrorKind::InvalidData {
                        Error::Tls("Controller TLS authentication failed")
                    } else {
                        Error::Transport(format!(
                            "could not secure the connection to {host}: {error}"
                        ))
                    }
                })?;
            let notification = UdpSocket::bind(SocketAddr::new(local.ip(), 0))
                .await
                .map_err(|error| {
                    Error::Transport(format!(
                        "could not open a notification socket on {}: {error}",
                        local.ip()
                    ))
                })?;
            let notification_port = NonZeroU16::new(
                notification
                    .local_addr()
                    .map_err(|error| {
                        Error::Transport(format!(
                            "could not read the notification socket address: {error}"
                        ))
                    })?
                    .port(),
            )
            .ok_or_else(|| {
                Error::Transport("the notification socket was not given a port".to_owned())
            })?;
            Ok(Connection {
                stream,
                notification,
                address,
                notification_port,
                state: None,
                _permit: permit,
            })
        };
        let connection = tokio::time::timeout(self.limits.timeout, connection)
            .await
            .map_err(|_| Error::Deadline)??;
        Ok(Session {
            connection: Some(connection),
            limits: self.limits,
        })
    }
}

struct Connection {
    stream: TlsStream<TcpStream>,
    notification: UdpSocket,
    address: Ipv4Addr,
    notification_port: NonZeroU16,
    state: Option<ManagedSessionState>,
    _permit: OwnedSemaphorePermit,
}

pub struct Session {
    connection: Option<Connection>,
    limits: Limits,
}

#[derive(Debug)]
pub struct Exchange {
    pub packet: Option<Vec<u8>>,
    pub request_sent: bool,
    pub peer: SocketAddr,
    pub received_at: chrono::DateTime<chrono::Utc>,
    pub received_monotonic: tokio::time::Instant,
}

#[derive(Debug)]
pub struct ExchangeError {
    pub error: Error,
    pub request_sent: Option<bool>,
}

impl std::fmt::Display for ExchangeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ExchangeError {}

impl Session {
    pub async fn exchange(
        &mut self,
        credential: &Credential,
        operation: ManagedOperation,
        expected_domain: Option<String>,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.exchange_with_receipt(credential, operation, expected_domain)
            .await
            .map(|exchange| exchange.packet)
            .map_err(|failure| failure.error)
    }

    pub async fn exchange_with_receipt(
        &mut self,
        credential: &Credential,
        operation: ManagedOperation,
        expected_domain: Option<String>,
    ) -> Result<Exchange, ExchangeError> {
        if matches!(operation, ManagedOperation::MonitorSignals) {
            return Err(ExchangeError {
                error: Error::Invalid("Signal monitoring requires a persistent subscription"),
                request_sent: Some(false),
            });
        }

        let mut connection = self.connection.take().ok_or(ExchangeError {
            error: Error::Invalid("Controller session is closed; uncertain commands must not be retried automatically"),
            request_sent: Some(false),
        })?;
        let peer = connection
            .stream
            .get_ref()
            .0
            .peer_addr()
            .map_err(|error| ExchangeError {
                error: Error::Transport(format!(
                    "could not read the remote address of the control connection: {error}"
                )),
                request_sent: Some(false),
            })?;
        let mut request_sent = Some(false);
        let mut received = None;
        let exchange = async {
            let mut received_bytes = 0;
            let mut operation_emitted = false;
            let mut request = ManagedSessionRequest::Begin {
                state: connection.state.take(),
                credential: credential.read().await?,
                local_ipv4: connection.address,
                notification_port: connection.notification_port,
                expected_domain_id: expected_domain,
                operation,
            };

            loop {
                let step = managed_session::advance(request).map_err(Error::Session)?;
                if !operation_emitted && step.state.operation_emitted() {
                    operation_emitted = true;
                    request_sent = None;
                }

                for frame in step.outgoing {
                    connection.stream.write_all(&frame).await.map_err(|error| {
                        Error::Transport(format!("could not send a request to {peer}: {error}"))
                    })?;
                }

                if operation_emitted && request_sent.is_none() {
                    request_sent = Some(true);
                }

                if step.complete {
                    if request_sent != Some(true) {
                        return Err(Error::Response(
                            "Controller completed an operation without an emitted request",
                        ));
                    }
                    connection.state = Some(step.state);
                    return step
                        .packet_hex
                        .map(|packet| {
                            hex::decode(packet).map_err(|_| {
                                Error::Response("Controller operation returned invalid packet data")
                            })
                        })
                        .transpose();
                }

                if step.receive_bytes == 0
                    || step.receive_bytes > self.limits.maximum_response_bytes - received_bytes
                {
                    return Err(Error::TooLarge);
                }

                let mut bytes = vec![0; step.receive_bytes];
                connection
                    .stream
                    .read_exact(&mut bytes)
                    .await
                    .map_err(|error| {
                        Error::Transport(format!("could not read a reply from {peer}: {error}"))
                    })?;
                received = Some((chrono::Utc::now(), tokio::time::Instant::now()));
                received_bytes += bytes.len();
                request = ManagedSessionRequest::Receive {
                    state: step.state,
                    data: bytes,
                };
            }
        };
        let result = tokio::time::timeout(self.limits.timeout, exchange)
            .await
            .unwrap_or(Err(Error::Deadline));

        if result.is_ok() {
            self.connection = Some(connection);
        }

        result
            .map(|packet| Exchange {
                packet,
                request_sent: true,
                peer,
                received_at: received.expect("completed exchange received a frame").0,
                received_monotonic: received.expect("completed exchange received a frame").1,
            })
            .map_err(|error| ExchangeError {
                error,
                request_sent,
            })
    }

    pub fn notification_address(&self) -> Result<SocketAddr, Error> {
        self.connection
            .as_ref()
            .ok_or(Error::Invalid("Controller session is closed"))?
            .notification
            .local_addr()
            .map_err(|error| {
                Error::Transport(format!(
                    "could not read the notification socket address: {error}"
                ))
            })
    }
}
