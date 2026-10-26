mod tls;

use crate::devices::discovery::DiscoveredService;
use anyhow::{Context, Result, ensure};
use reqwest::Method;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, watch};

const MAXIMUM_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

pub enum Resource {
    Snapshot,
    Meter(Value),
    History(Value),
}

pub struct Peers {
    local_host: String,
    services: watch::Sender<BTreeMap<String, DiscoveredService>>,
    capacity: Arc<Semaphore>,
}

pub fn normalized_host(value: &str) -> String {
    caseless::default_case_fold_str(value.trim().trim_end_matches('.'))
        .trim_end_matches(".local")
        .to_owned()
}

impl Peers {
    pub fn new(local_host: String) -> Self {
        Self {
            local_host: normalized_host(&local_host),
            services: watch::channel(BTreeMap::new()).0,
            capacity: Arc::new(Semaphore::new(8)),
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<BTreeMap<String, DiscoveredService>> {
        self.services.subscribe()
    }

    pub fn observe(&self, service: DiscoveredService) {
        if normalized_host(&service.server_name) == self.local_host {
            self.remove(&service.name);
            return;
        }
        self.services.send_if_modified(|services| {
            if services.get(&service.name) == Some(&service) {
                return false;
            }
            services.insert(service.name.clone(), service);
            true
        });
    }

    pub fn remove(&self, name: &str) {
        self.services
            .send_if_modified(|services| services.remove(name).is_some());
    }

    pub fn clear(&self) {
        self.services.send_if_modified(|services| {
            let changed = !services.is_empty();
            services.clear();
            changed
        });
    }

    pub fn hosts(&self) -> Vec<String> {
        let mut hosts: Vec<_> = self
            .services
            .borrow()
            .values()
            .map(|service| {
                service
                    .server_name
                    .trim_end_matches('.')
                    .trim_end_matches(".local")
                    .to_owned()
            })
            .collect();
        hosts.sort();
        hosts.dedup();
        hosts
    }

    fn find(&self, host: &str) -> Result<DiscoveredService> {
        let wanted = normalized_host(host);
        let services = self.services.borrow();
        let mut matches = services
            .values()
            .filter(|service| normalized_host(&service.server_name) == wanted);
        let service = matches.next().with_context(|| {
            format!("No NetAudio server on {host:?} has been found on the network")
        })?;
        ensure!(
            matches.all(|other| other.ipv4 == service.ipv4
                && other.port == service.port
                && other.properties == service.properties),
            "Several NetAudio services claim host {host}; their addresses or capabilities disagree"
        );
        Ok(service.clone())
    }

    pub async fn request(
        &self,
        host: &str,
        resource: Resource,
        deadline: Duration,
    ) -> Result<(u16, Value)> {
        ensure!(
            !deadline.is_zero() && deadline <= Duration::from_secs(30),
            "Host audio peer request deadline must be between zero and 30 seconds"
        );
        let _permit = self
            .capacity
            .try_acquire()
            .context("Host audio peer request capacity is exhausted")?;
        let service = self.find(host)?;
        let scheme = service.property("scheme").unwrap_or("http");
        ensure!(
            matches!(scheme, "http" | "https"),
            "Peer advertised an unsupported transport scheme"
        );
        ensure!(service.port != 0, "Peer advertised an invalid service port");
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .pool_max_idle_per_host(0)
            .timeout(deadline);
        if scheme == "https" {
            let fingerprint = service
                .property("tls_sha256")
                .context("Peer does not advertise a TLS certificate fingerprint")?;
            builder = builder.use_preconfigured_tls(tls::configuration(fingerprint)?);
        }
        let client = builder.build()?;
        let (method, path, body) = match resource {
            Resource::Snapshot => (Method::GET, "/host-audio", None),
            Resource::Meter(body) => (Method::POST, "/host-audio/meter", Some(body)),
            Resource::History(body) => (Method::POST, "/host-audio/history", Some(body)),
        };
        let request = client
            .request(
                method,
                format!("{scheme}://{}:{}{path}", service.ipv4, service.port),
            )
            .header("accept", "application/json");
        let request = if let Some(body) = body {
            request
                .header("content-type", "application/json")
                .body(serde_json::to_vec(&body)?)
        } else {
            request
        };
        let result = tokio::time::timeout(deadline, async {
            let mut response = request
                .send()
                .await
                .context("Host audio peer transport failed")?;
            ensure!(
                response
                    .content_length()
                    .is_none_or(|length| length <= MAXIMUM_RESPONSE_BYTES as u64),
                "Peer response exceeds its byte budget"
            );
            let status = response.status().as_u16();
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                ensure!(
                    bytes.len().saturating_add(chunk.len()) <= MAXIMUM_RESPONSE_BYTES,
                    "Peer response exceeds its byte budget"
                );
                bytes.extend_from_slice(&chunk);
            }
            let body = if bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&bytes)
                    .context("Host audio peer returned a reply that is not JSON")?
            };
            Ok::<_, anyhow::Error>((status, body))
        })
        .await
        .context("Host audio peer request exceeded its deadline")??;
        ensure!(
            self.find(host)? == service,
            "Host audio peer discovery changed during the request"
        );
        Ok(result)
    }
}
