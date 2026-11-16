mod changes;
pub mod selection;

use crate::transport::datagram::MulticastInterface;
use anyhow::{Context, Result};
use ipnet::IpNet;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interface {
    pub name: String,
    pub index: Option<u32>,
    pub network: IpNet,
    pub running: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NetworkSnapshot {
    pub interfaces: Vec<Interface>,
}

impl NetworkSnapshot {
    pub fn read() -> Result<Self> {
        let mut interfaces = if_addrs::get_if_addrs()?
            .into_iter()
            .map(|interface| {
                let running = interface.is_oper_up();
                let network = match interface.addr {
                    if_addrs::IfAddr::V4(address) => {
                        IpNet::new(address.ip.into(), address.prefixlen)?
                    }
                    if_addrs::IfAddr::V6(address) => {
                        IpNet::new(address.ip.into(), address.prefixlen)?
                    }
                };
                Ok(Interface {
                    name: interface.name,
                    index: interface.index,
                    network,
                    running,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        interfaces
            .sort_by(|left, right| (&left.name, left.network).cmp(&(&right.name, right.network)));
        interfaces.dedup();
        Ok(Self { interfaces })
    }

    pub fn selected(&self, selection: &BTreeSet<String>) -> Self {
        if selection.is_empty() {
            return self.clone();
        }

        Self {
            interfaces: self
                .interfaces
                .iter()
                .filter(|interface| {
                    selection.contains(&interface.name)
                        || selection.contains(&interface.network.addr().to_string())
                })
                .cloned()
                .collect(),
        }
    }

    pub fn networks(&self) -> Vec<IpNet> {
        self.interfaces
            .iter()
            .filter(|interface| match interface.network.addr() {
                IpAddr::V4(address) => interface.running && !address.is_loopback(),
                IpAddr::V6(_) => false,
            })
            .map(|interface| interface.network)
            .collect()
    }

    pub fn ipv4_query_addresses(&self) -> (Vec<Ipv4Addr>, Vec<Ipv4Addr>) {
        let mut first = BTreeSet::new();
        let mut shared = Vec::new();
        let mut alone = Vec::new();

        for interface in &self.interfaces {
            let IpAddr::V4(address) = interface.network.addr() else {
                continue;
            };

            if !interface.running || address.is_loopback() {
                continue;
            }

            if first.insert(&interface.name) {
                shared.push(address);
            } else {
                alone.push(address);
            }
        }

        (shared, alone)
    }

    pub fn ipv4_multicast_interfaces(&self) -> BTreeMap<u32, MulticastInterface> {
        let mut interfaces = BTreeMap::new();

        for interface in &self.interfaces {
            let IpAddr::V4(address) = interface.network.addr() else {
                continue;
            };

            if address.is_loopback() {
                continue;
            }

            let Some(index) = interface.index.filter(|index| *index != 0) else {
                continue;
            };
            interfaces
                .entry(index)
                .or_insert(MulticastInterface { address, index });
        }

        interfaces
    }

    pub fn local_address(&self, destination: Ipv4Addr) -> Option<Ipv4Addr> {
        self.interfaces
            .iter()
            .filter(|interface| {
                interface.running && interface.network.contains(&IpAddr::V4(destination))
            })
            .find_map(|interface| match interface.network.addr() {
                IpAddr::V4(address) if !address.is_loopback() => Some(address),
                _ => None,
            })
    }

    pub fn addresses(&self) -> BTreeSet<IpAddr> {
        self.interfaces
            .iter()
            .map(|interface| interface.network.addr())
            .collect()
    }
}

#[cfg(target_os = "ios")]
pub fn interfaces_changed() {
    changes::announce();
}

pub struct NetworkMonitor {
    changes: changes::Changes,
    selection: Arc<selection::Selection>,
}

impl NetworkMonitor {
    pub fn new(selection: Arc<selection::Selection>) -> Result<Self> {
        let changes = changes::Changes::new().context("watch network interface changes")?;
        selection.observe(NetworkSnapshot::read()?)?;
        Ok(Self { changes, selection })
    }

    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                changed = self.changes.next() => {
                    changed.context("network interface watcher stopped")?;
                    self.selection.observe(NetworkSnapshot::read()?)?;
                }
            }
        }
    }
}
