use super::inventory::{Announcement, Inventory, Observation};
use crate::network::NetworkSnapshot;
use crate::state::ReadModel;
use crate::transport::datagram::{DatagramSocket, MAXIMUM_DATAGRAM_BYTES, PacketMetadata};
use anyhow::{Context, Result, ensure};
use chrono::Utc;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const DEFAULT_GROUPS: [&str; 2] = [netaudio_core::sap::MULTICAST_ADDRESS, "224.0.0.56"];
const ANNOUNCEMENT_LIFETIME: Duration = Duration::from_secs(3600);

pub struct Settings {
    pub groups: Vec<Ipv4Addr>,
    pub expiry: Duration,
}

impl Settings {
    pub fn from_environment() -> Result<Self> {
        let groups = match std::env::var("NETAUDIO_SAP_GROUPS") {
            Ok(groups) => groups,
            Err(std::env::VarError::NotPresent) => DEFAULT_GROUPS.join(","),
            Err(error) => return Err(error.into()),
        };
        let mut seen = BTreeSet::new();
        let groups = groups
            .split(',')
            .map(str::trim)
            .filter(|group| !group.is_empty())
            .map(str::parse::<Ipv4Addr>)
            .collect::<Result<Vec<_>, _>>()
            .context("NETAUDIO_SAP_GROUPS must contain IPv4 multicast addresses")?
            .into_iter()
            .filter(|group| seen.insert(*group))
            .collect::<Vec<_>>();
        ensure!(
            !groups.is_empty() && groups.iter().all(Ipv4Addr::is_multicast),
            "SAP groups must be nonempty IPv4 multicast addresses"
        );

        Ok(Self {
            groups,
            expiry: ANNOUNCEMENT_LIFETIME,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct Interface {
    name: String,
    address: Ipv4Addr,
    #[serde(skip)]
    index: u32,
}

#[derive(Clone, Serialize)]
pub struct Diagnostics {
    groups: Vec<Ipv4Addr>,
    interfaces: BTreeSet<Interface>,
    listener_errors: BTreeMap<String, String>,
    received: u64,
    accepted: u64,
    rejected: u64,
    latest_error: Option<String>,
    source_count: usize,
}

pub struct Discovery {
    socket: DatagramSocket,
    inventory: Inventory,
    model: Arc<ReadModel>,
    groups: Vec<Ipv4Addr>,
    memberships: BTreeMap<(u32, Ipv4Addr), Interface>,
    diagnostics: Arc<Mutex<Diagnostics>>,
}

impl Discovery {
    pub fn bind(
        settings: Settings,
        model: Arc<ReadModel>,
        network: &NetworkSnapshot,
        selected: Option<Ipv4Addr>,
    ) -> Result<Self> {
        ensure!(
            !settings.groups.is_empty() && settings.groups.iter().all(Ipv4Addr::is_multicast),
            "SAP groups must be nonempty IPv4 multicast addresses"
        );
        let mut discovery = Self {
            socket: DatagramSocket::bind(netaudio_core::sap::PORT).context("bind SAP listener")?,
            inventory: Inventory::new(settings.expiry)?,
            model,
            groups: settings.groups.clone(),
            memberships: BTreeMap::new(),
            diagnostics: Arc::new(Mutex::new(Diagnostics {
                groups: settings.groups,
                interfaces: BTreeSet::new(),
                listener_errors: BTreeMap::new(),
                received: 0,
                accepted: 0,
                rejected: 0,
                latest_error: None,
                source_count: 0,
            })),
        };
        discovery.update_interfaces(network, selected)?;
        Ok(discovery)
    }

    pub fn diagnostics(&self) -> Arc<Mutex<Diagnostics>> {
        Arc::clone(&self.diagnostics)
    }

    pub async fn run(
        mut self,
        mut network: watch::Receiver<NetworkSnapshot>,
        selected: Option<Ipv4Addr>,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let mut buffer = vec![0; MAXIMUM_DATAGRAM_BYTES];

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                changed = network.changed() => {
                    changed.context("network monitor stopped")?;
                    self.update_interfaces(&network.borrow_and_update(), selected)?;
                }
                change = self.inventory.expired(), if !self.inventory.is_empty() => {
                    let change = change?;
                    self.update_source_count()?;
                    self.model.external_flow(change)?;
                }
                packet = self.socket.receive(&mut buffer) => {
                    match packet {
                        Ok(packet) => self.receive(packet, &buffer)?,
                        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                            self.reject(error.to_string())?;
                        }
                        Err(error) => return Err(error).context("receive SAP packet"),
                    }
                }
            }
        }
    }

    fn receive(&mut self, packet: PacketMetadata, buffer: &[u8]) -> Result<()> {
        let Some(interface) = self
            .memberships
            .get(&(packet.interface_index, packet.destination))
        else {
            return Ok(());
        };
        let observation = Observation {
            interface: interface.name.clone(),
            source: packet.source,
            received_at: Utc::now(),
            received_monotonic: Instant::now(),
        };
        let announcement = match Announcement::parse(&buffer[..packet.length]) {
            Ok(announcement) => announcement,
            Err(error) => return self.reject(format!("{error:#}")),
        };

        let changes = self.inventory.apply(announcement, observation)?;
        let mut diagnostics = self
            .diagnostics
            .lock()
            .map_err(|_| anyhow::anyhow!("SAP diagnostics lock poisoned"))?;
        diagnostics.received += 1;
        diagnostics.accepted += 1;
        diagnostics.source_count = self.inventory.len();
        drop(diagnostics);

        for change in changes {
            self.model.external_flow(change)?;
        }

        Ok(())
    }

    fn reject(&self, message: String) -> Result<()> {
        let mut diagnostics = self
            .diagnostics
            .lock()
            .map_err(|_| anyhow::anyhow!("SAP diagnostics lock poisoned"))?;
        diagnostics.received += 1;
        diagnostics.rejected += 1;
        diagnostics.latest_error = Some(message);
        Ok(())
    }

    fn update_source_count(&self) -> Result<()> {
        self.diagnostics
            .lock()
            .map_err(|_| anyhow::anyhow!("SAP diagnostics lock poisoned"))?
            .source_count = self.inventory.len();
        Ok(())
    }

    fn update_interfaces(
        &mut self,
        network: &NetworkSnapshot,
        selected: Option<Ipv4Addr>,
    ) -> Result<()> {
        let mut desired = BTreeMap::new();
        let mut errors = BTreeMap::new();

        for interface in &network.interfaces {
            let IpAddr::V4(address) = interface.network.addr() else {
                continue;
            };

            if address.is_loopback() || selected.is_some_and(|selected| selected != address) {
                continue;
            }

            let Some(index) = interface.index.filter(|index| *index != 0) else {
                errors.insert(
                    format!("{}/{}", interface.name, address),
                    "network interface has no OS index".to_owned(),
                );
                continue;
            };
            let interface = Interface {
                name: interface.name.clone(),
                address,
                index,
            };

            for group in &self.groups {
                desired
                    .entry((index, *group))
                    .or_insert_with(|| interface.clone());
            }
        }

        let removed = self
            .memberships
            .iter()
            .filter(|(key, _)| !desired.contains_key(key))
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();

        for key in removed {
            let interface = self
                .memberships
                .remove(&key)
                .context("SAP membership disappeared")?;

            if let Err(error) = self.socket.leave(key.1, interface.index) {
                tracing::warn!(%error, interface = %interface.name, group = %key.1, "could not leave SAP multicast membership");
            }
        }

        for (key, interface) in desired {
            if let Some(previous) = self.memberships.get_mut(&key) {
                *previous = interface;
                continue;
            }

            match self.socket.join(key.1, interface.index) {
                Ok(()) => {
                    self.memberships.insert(key, interface);
                }
                Err(error) => {
                    tracing::warn!(%error, interface = %interface.name, group = %key.1, "could not join SAP multicast membership");
                    errors.insert(
                        format!("{}/{}/{}", interface.name, interface.address, key.1),
                        error.to_string(),
                    );
                }
            }
        }

        let mut diagnostics = self
            .diagnostics
            .lock()
            .map_err(|_| anyhow::anyhow!("SAP diagnostics lock poisoned"))?;
        diagnostics.interfaces = self.memberships.values().cloned().collect();
        diagnostics.listener_errors = errors;
        Ok(())
    }
}
