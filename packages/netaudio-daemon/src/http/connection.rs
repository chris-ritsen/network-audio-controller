use axum::http::HeaderMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use url::Url;

#[derive(Clone, Debug)]
pub struct ConnectionContext {
    pub peer: SocketAddr,
    pub encrypted: bool,
    pub listener_port: u16,
    pub trusted_proxies: Arc<[IpAddr]>,
}

impl ConnectionContext {
    pub fn trusts_forwarding(&self) -> bool {
        self.peer.ip().is_loopback() || self.trusted_proxies.contains(&self.peer.ip())
    }

    pub fn client_address(&self, headers: &HeaderMap) -> IpAddr {
        if self.trusts_forwarding() {
            for name in ["cf-connecting-ip", "x-forwarded-for"] {
                if let Some(address) =
                    first_header(headers, name).and_then(|value| value.parse::<IpAddr>().ok())
                {
                    return address;
                }
            }
        }

        self.peer.ip()
    }

    pub fn base_url(&self, headers: &HeaderMap) -> Result<Url, super::ApiError> {
        let mut scheme = if self.encrypted { "https" } else { "http" };
        let mut host = first_header(headers, "host");

        if self.trusts_forwarding() {
            if let Some(forwarded) = first_header(headers, "x-forwarded-proto") {
                if !matches!(forwarded, "http" | "https") {
                    return Err(super::ApiError::invalid("invalid forwarded protocol"));
                }

                scheme = forwarded;
            }

            if let Some(forwarded) = first_header(headers, "x-forwarded-host") {
                host = Some(forwarded);
            }
        }

        let authority = host
            .map(str::to_owned)
            .unwrap_or_else(|| format!("localhost:{}", self.listener_port));
        let url = Url::parse(&format!("{scheme}://{authority}"))
            .map_err(|_| super::ApiError::invalid("invalid request authority"))?;

        if url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(super::ApiError::invalid("invalid request authority"));
        }

        Ok(url)
    }
}

fn first_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)?
        .to_str()
        .ok()?
        .split(',')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}
