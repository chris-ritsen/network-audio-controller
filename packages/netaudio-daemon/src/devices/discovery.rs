use crate::network::NetworkSnapshot;
use crate::relay::Advertisement;
use anyhow::{Context, Result, bail};
use futures_util::stream::{BoxStream, SelectAll};
use futures_util::{StreamExt, stream};
use mdns_sd::{DaemonEvent, DaemonStatus, IfKind, ResolvedService, ServiceDaemon, ServiceEvent};
use netaudio_core::protocol::{SERVICE_ARC, SERVICE_CHAN, SERVICE_CMC, SERVICE_DBC, SERVICE_VIDEO};
use serde::Serialize;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const SERVICE_TYPES: [&str; 5] = [
    SERVICE_ARC,
    SERVICE_CHAN,
    SERVICE_CMC,
    SERVICE_DBC,
    SERVICE_VIDEO,
];
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DiscoveredService {
    pub ipv4: Ipv4Addr,
    pub name: String,
    pub port: u16,
    pub properties: BTreeMap<String, Option<String>>,
    pub server_name: String,
    #[serde(rename = "type")]
    pub service_type: String,
}

impl DiscoveredService {
    pub fn device_key(&self) -> Result<String> {
        let suffix = format!(".{}", self.service_type);

        if !self.name.to_lowercase().ends_with(&suffix.to_lowercase()) {
            bail!("service name does not end in its service type");
        }

        Ok(format!(
            "{}.local.",
            &self.name[..self.name.len() - suffix.len()]
        ))
    }

    pub fn property(&self, key: &str) -> Option<&str> {
        self.properties.get(key).and_then(Option::as_deref)
    }

    pub fn is_media(&self) -> bool {
        matches!(self.service_type.as_str(), SERVICE_CHAN | SERVICE_VIDEO)
    }

    fn resolved(
        service_type: &str,
        service: &ResolvedService,
        networks: &[if_watch::IpNet],
    ) -> Result<Self> {
        let ipv4 = service
            .get_addresses_v4()
            .into_iter()
            .filter(|address| {
                !address.is_unspecified() && !address.is_multicast() && !address.is_broadcast()
            })
            .min_by_key(|address| {
                let prefix = networks
                    .iter()
                    .filter(|network| network.contains(&IpAddr::V4(*address)))
                    .map(if_watch::IpNet::prefix_len)
                    .max();
                let secondary = address.octets()[..2] == [172, 31];
                (
                    prefix.is_none(),
                    secondary,
                    std::cmp::Reverse(prefix),
                    address.is_link_local(),
                    *address,
                )
            })
            .context("resolved service has no usable IPv4 address")?;
        let properties = service
            .get_properties()
            .iter()
            .map(|property| {
                let value = property
                    .val()
                    .map(|value| std::str::from_utf8(value).map(str::to_owned))
                    .transpose()
                    .with_context(|| format!("service property {} is not UTF-8", property.key()))?;
                Ok((property.key().to_owned(), value))
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            ipv4,
            name: service.get_fullname().to_owned(),
            port: service.get_port(),
            properties,
            server_name: service.get_hostname().to_owned(),
            service_type: service_type.to_owned(),
        })
    }
}

#[derive(Debug)]
pub enum DiscoveryEvent {
    Resolved(DiscoveredService),
    Removed { service_type: String, name: String },
    NetworkChanged,
}

struct Browser {
    daemon: ServiceDaemon,
    streams: SelectAll<BoxStream<'static, Result<(&'static str, ServiceEvent)>>>,
}

impl Browser {
    fn start(address: Option<Ipv4Addr>, peers: bool) -> Result<Self> {
        let daemon = ServiceDaemon::new().context("start multicast discovery")?;
        let configured = (|| -> Result<_> {
            daemon.set_ip_check_interval(0)?;

            if let Some(address) = address {
                daemon.disable_interface(IfKind::All)?;
                daemon.enable_interface(IfKind::Addr(IpAddr::V4(address)))?;
            } else {
                daemon.disable_interface(IfKind::IPv6)?;
            }

            let mut streams = SelectAll::new();

            for service_type in SERVICE_TYPES
                .into_iter()
                .chain(peers.then_some(crate::relay::SERVICE_TYPE))
            {
                let receiver = daemon.browse(service_type)?;
                let events = stream::unfold(receiver, move |receiver| async move {
                    let event = receiver
                        .recv_async()
                        .await
                        .map(|event| (service_type, event))
                        .context("multicast discovery stream closed");
                    Some((event, receiver))
                });
                streams.push(events.boxed());
            }

            Ok(streams)
        })();

        match configured {
            Ok(streams) => Ok(Self { daemon, streams }),
            Err(error) => {
                let _ = daemon.shutdown();
                Err(error)
            }
        }
    }

    async fn stop(&mut self) -> Result<()> {
        let stopped = self.daemon.shutdown()?;
        let status = tokio::time::timeout(SHUTDOWN_DEADLINE, stopped.recv_async())
            .await
            .context("multicast discovery shutdown timed out")??;

        if status != DaemonStatus::Shutdown {
            bail!("multicast discovery did not shut down");
        }

        Ok(())
    }
}

impl Drop for Browser {
    fn drop(&mut self) {
        if let Err(error) = self.daemon.shutdown() {
            tracing::debug!(%error, "multicast discovery already stopped");
        }
    }
}

pub async fn run(
    address: Option<Ipv4Addr>,
    sender: mpsc::Sender<DiscoveryEvent>,
    cancellation: CancellationToken,
    mut network: watch::Receiver<NetworkSnapshot>,
    mut relay: Option<Advertisement>,
    peers: Option<Arc<crate::host_audio::peers::Peers>>,
) -> Result<()> {
    let mut networks = network.borrow_and_update().networks(address);
    let mut browser = Browser::start(address, peers.is_some())?;
    let mut monitor = browser.daemon.monitor()?;
    publish_relay(&browser, &mut relay, &network, address)?;
    let result = loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break Ok(()),
            changed = network.changed() => {
                changed.context("network monitor stopped")?;
                let updated = network.borrow_and_update().networks(address);

                if updated != networks {
                    networks = updated;
                    browser.stop().await?;
                    sender.send(DiscoveryEvent::NetworkChanged).await
                        .context("device registry stopped")?;
                    if let Some(peers) = &peers { peers.clear(); }
                    browser = Browser::start(address, peers.is_some())?;
                    monitor = browser.daemon.monitor()?;
                    publish_relay(&browser, &mut relay, &network, address)?;
                }
            }
            changed = relay_changed(&mut relay) => {
                changed?;
                publish_relay(&browser, &mut relay, &network, address)?;
            }
            event = monitor.recv_async() => {
                match event.context("multicast discovery monitor stopped")? {
                    DaemonEvent::Error(error) => break Err(error.into()),
                    DaemonEvent::NameChange(change) => {
                        tracing::info!(
                            original = change.original,
                            observed = change.new_name,
                            interface = change.intf_name,
                            "DNS-SD name conflict resolved"
                        );
                    }
                    _ => {}
                }
            }
            event = browser.streams.next() => {
                let (service_type, event) = event.context("multicast browsers stopped")??;
                let event = match event {
                    ServiceEvent::ServiceResolved(service) => {
                        match DiscoveredService::resolved(service_type, &service, &networks) {
                            Ok(service) if service_type == crate::relay::SERVICE_TYPE => {
                                if let Some(peers) = &peers { peers.observe(service); }
                                None
                            },
                            Ok(service) => Some(DiscoveryEvent::Resolved(service)),
                            Err(error) => {
                                tracing::warn!(name = service.get_fullname(), %error, "invalid discovered service");
                                None
                            }
                        }
                    }
                    ServiceEvent::ServiceRemoved(service_type, name) => {
                        if service_type == crate::relay::SERVICE_TYPE {
                            if let Some(peers) = &peers { peers.remove(&name); }
                            None
                        } else {
                            Some(DiscoveryEvent::Removed { service_type, name })
                        }
                    }
                    ServiceEvent::SearchStopped(service_type) => {
                        break Err(anyhow::anyhow!("multicast browser stopped for {service_type}"));
                    }
                    _ => None,
                };

                if let Some(event) = event {
                    sender.send(event).await.context("device registry stopped")?;
                }
            }
        }
    };
    if let Some(peers) = &peers {
        peers.clear();
    }
    browser.stop().await?;
    result
}

fn publish_relay(
    browser: &Browser,
    advertisement: &mut Option<Advertisement>,
    network: &watch::Receiver<NetworkSnapshot>,
    selected: Option<Ipv4Addr>,
) -> Result<()> {
    if let Some(advertisement) = advertisement {
        let addresses: Vec<_> = network
            .borrow()
            .ipv4_addresses(selected)
            .into_iter()
            .collect();

        if let Some(service) = advertisement.service(&addresses)? {
            browser.daemon.register(service)?;
        }
    }

    Ok(())
}

async fn relay_changed(advertisement: &mut Option<Advertisement>) -> Result<()> {
    match advertisement {
        Some(advertisement) => advertisement.changed().await,
        None => std::future::pending().await,
    }
}
