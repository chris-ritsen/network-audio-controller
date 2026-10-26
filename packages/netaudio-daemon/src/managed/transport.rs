use reqwest::{Method, Url, header::HeaderMap};
use rustls::{ClientConfig, RootCertStore, crypto::ring};
use std::fmt;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub enum Error {
    Invalid(&'static str),
    Credential(&'static str),
    Authentication,
    Response(&'static str),
    Status(u16),
    Transport,
    Tls(&'static str),
    Deadline,
    TooLarge,
    Busy,
    Persistence(&'static str),
    Conflict(&'static str),
    Session(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message)
            | Self::Credential(message)
            | Self::Response(message)
            | Self::Persistence(message)
            | Self::Conflict(message)
            | Self::Tls(message) => formatter.write_str(message),
            Self::Authentication => formatter.write_str("DDM rejected authentication"),
            Self::Status(status) => write!(formatter, "Managed service returned HTTP {status}"),
            Self::Transport => formatter.write_str("Managed service transport failed"),
            Self::Deadline => formatter.write_str("Managed service request exceeded its deadline"),
            Self::TooLarge => {
                formatter.write_str("Managed service response exceeded its size limit")
            }
            Self::Busy => formatter.write_str("Managed service has no available request capacity"),
            Self::Session(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Copy)]
pub struct Limits {
    pub timeout: Duration,
    pub maximum_response_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            maximum_response_bytes: 8 * 1024 * 1024,
        }
    }
}

impl Limits {
    pub fn validate(self) -> Result<Self, Error> {
        if self.timeout.is_zero() || self.timeout > Duration::from_secs(60) {
            return Err(Error::Invalid(
                "Managed service timeout must be greater than zero and no more than 60 seconds",
            ));
        }

        if !(1..=16 * 1024 * 1024).contains(&self.maximum_response_bytes) {
            return Err(Error::Invalid(
                "Managed service response limit must be between 1 byte and 16 MiB",
            ));
        }

        Ok(self)
    }
}

#[derive(Clone)]
pub struct Transport {
    client: reqwest::Client,
    pub(super) tls: Arc<ClientConfig>,
    pub(super) capacity: Arc<Semaphore>,
}

impl Transport {
    pub(super) fn independent_capacity(&self, capacity: std::num::NonZeroUsize) -> Self {
        Self {
            client: self.client.clone(),
            tls: Arc::clone(&self.tls),
            capacity: Arc::new(Semaphore::new(capacity.get())),
        }
    }

    pub fn new(certificate_file: Option<&Path>) -> Result<Self, Error> {
        let bytes = certificate_file
            .map(|path| {
                super::resources::read_file(path, 1024 * 1024).map_err(|_| {
                    Error::Tls("Managed service trust certificate file is unavailable or invalid")
                })
            })
            .transpose()?;
        Self::from_certificate_bytes(bytes.as_deref())
    }

    pub(super) fn from_certificate_bytes(certificate_bytes: Option<&[u8]>) -> Result<Self, Error> {
        let mut roots = RootCertStore::empty();
        let native = rustls_native_certs::load_native_certs();

        for certificate in native.certs {
            roots
                .add(certificate)
                .map_err(|_| Error::Tls("A system trust certificate is invalid"))?;
        }

        if !native.errors.is_empty() {
            tracing::warn!(
                failures = native.errors.len(),
                "Some system trust sources could not be loaded"
            );
        }

        if let Some(bytes) = certificate_bytes {
            let mut reader = BufReader::new(bytes);
            let certificates = rustls_pemfile::certs(&mut reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| Error::Tls("Managed service trust certificate file is invalid"))?;

            if certificates.is_empty() {
                return Err(Error::Tls(
                    "Managed service trust certificate file contains no certificates",
                ));
            }

            for certificate in certificates {
                roots
                    .add(certificate)
                    .map_err(|_| Error::Tls("Managed service trust certificate is invalid"))?;
            }
        }

        if roots.is_empty() {
            return Err(Error::Tls(
                "No trusted certificates are available for managed services",
            ));
        }

        let configuration = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|_| Error::Tls("Managed service TLS configuration failed"))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let client = reqwest::Client::builder()
            .use_preconfigured_tls(configuration.clone())
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .pool_max_idle_per_host(2)
            .build()
            .map_err(|_| Error::Tls("Managed service HTTP client configuration failed"))?;

        Ok(Self {
            client,
            tls: Arc::new(configuration),
            capacity: Arc::new(Semaphore::new(8)),
        })
    }

    pub(super) async fn request(
        &self,
        method: Method,
        url: Url,
        headers: HeaderMap,
        body: Vec<u8>,
        limits: Limits,
    ) -> Result<Vec<u8>, Error> {
        let limits = limits.validate()?;

        if body.len() > crate::http::MAXIMUM_REQUEST_BYTES {
            return Err(Error::Invalid("Managed service request exceeds 8 MiB"));
        }

        let _permit = self.capacity.try_acquire().map_err(|_| Error::Busy)?;
        let request = async {
            let mut response = self
                .client
                .request(method, url)
                .headers(headers)
                .body(body)
                .send()
                .await
                .map_err(|_| Error::Transport)?;
            let status = response.status();

            if status == reqwest::StatusCode::UNAUTHORIZED {
                return Err(Error::Authentication);
            }

            if !status.is_success() {
                return Err(Error::Status(status.as_u16()));
            }

            if response
                .content_length()
                .is_some_and(|length| length > limits.maximum_response_bytes as u64)
            {
                return Err(Error::TooLarge);
            }

            let mut output = Vec::new();

            while let Some(chunk) = response.chunk().await.map_err(|_| Error::Transport)? {
                if chunk.len() > limits.maximum_response_bytes - output.len() {
                    return Err(Error::TooLarge);
                }

                output.extend_from_slice(&chunk);
            }

            Ok(output)
        };

        tokio::time::timeout(limits.timeout, request)
            .await
            .map_err(|_| Error::Deadline)?
    }
}
