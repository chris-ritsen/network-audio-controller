use anyhow::{Context, Result};
use mdns_sd::ServiceInfo;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use tokio::sync::watch;

pub const SERVICE_TYPE: &str = "_netaudio-relay._tcp.local.";
const MAXIMUM_LABEL_BYTES: usize = 63;
const INSTANCE_PREFIX: &str = "netaudio-daemon (";
const INSTANCE_SUFFIX: &str = ")";
const HASH_SUFFIX_LENGTH: usize = 12;

pub struct Advertisement {
    hostname: String,
    port: u16,
    properties: HashMap<String, String>,
    fingerprint: Option<watch::Receiver<String>>,
}

impl Advertisement {
    pub fn new(
        information: &Value,
        port: u16,
        encrypted: bool,
        fingerprint: Option<watch::Receiver<String>>,
    ) -> Result<Self> {
        let hostname = information["hostname"]
            .as_str()
            .filter(|name| !name.is_empty())
            .context("relay hostname is unavailable")?
            .to_owned();
        let mut properties = HashMap::from([
            ("version".to_owned(), "1".to_owned()),
            (
                "scheme".to_owned(),
                if encrypted { "https" } else { "http" }.to_owned(),
            ),
        ]);

        if let Some(path) = information["mcp"]["path"].as_str() {
            properties.insert("mcp_path".to_owned(), path.to_owned());
        }

        for (property, field) in [
            ("server_version", "version"),
            ("git_revision", "git_revision"),
        ] {
            if let Some(value) = information[field].as_str() {
                properties.insert(property.to_owned(), value.to_owned());
            }
        }

        if encrypted {
            properties.insert("tls_port".to_owned(), port.to_string());
        }

        Ok(Self {
            hostname,
            port,
            properties,
            fingerprint,
        })
    }

    pub fn service(&mut self, addresses: &[Ipv4Addr]) -> Result<Option<ServiceInfo>> {
        if addresses.is_empty() {
            return Ok(None);
        }

        let maximum_hostname_bytes =
            MAXIMUM_LABEL_BYTES - INSTANCE_PREFIX.len() - INSTANCE_SUFFIX.len();
        let instance = format!(
            "{INSTANCE_PREFIX}{}{INSTANCE_SUFFIX}",
            bounded_label(&self.hostname, maximum_hostname_bytes)
        );
        let hostname = format!(
            "{}.local.",
            bounded_label(&self.hostname, MAXIMUM_LABEL_BYTES)
        );

        if let Some(fingerprint) = &mut self.fingerprint {
            self.properties.insert(
                "tls_sha256".to_owned(),
                fingerprint.borrow_and_update().clone(),
            );
        }

        Ok(Some(ServiceInfo::new(
            SERVICE_TYPE,
            &instance,
            &hostname,
            addresses
                .iter()
                .copied()
                .map(IpAddr::V4)
                .collect::<Vec<_>>()
                .as_slice(),
            self.port,
            self.properties.clone(),
        )?))
    }

    pub async fn changed(&mut self) -> Result<()> {
        match &mut self.fingerprint {
            Some(fingerprint) => fingerprint
                .changed()
                .await
                .context("TLS identity stream stopped"),
            None => std::future::pending().await,
        }
    }
}

fn bounded_label(value: &str, maximum: usize) -> String {
    if value.len() <= maximum {
        return value.to_owned();
    }

    let digest = format!("{:x}", Sha256::digest(value.as_bytes()));
    let suffix = &digest[..HASH_SUFFIX_LENGTH];
    let mut boundary = maximum - HASH_SUFFIX_LENGTH - 1;

    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }

    format!("{}-{suffix}", &value[..boundary])
}
