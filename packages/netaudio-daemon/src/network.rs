use anyhow::{Context, Result};
use futures_util::StreamExt;
use if_watch::{IpNet, tokio::IfWatcher};
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interface {
    pub name: String,
    pub index: Option<u32>,
    pub network: IpNet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkSnapshot {
    pub interfaces: Vec<Interface>,
}

impl NetworkSnapshot {
    fn read() -> Result<Self> {
        let mut interfaces = if_addrs::get_if_addrs()?
            .into_iter()
            .map(|interface| {
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
                })
            })
            .collect::<Result<Vec<_>>>()?;
        interfaces
            .sort_by(|left, right| (&left.name, left.network).cmp(&(&right.name, right.network)));
        interfaces.dedup();
        Ok(Self { interfaces })
    }

    pub fn networks(&self, selected: Option<Ipv4Addr>) -> Vec<IpNet> {
        self.interfaces
            .iter()
            .filter(|interface| match interface.network.addr() {
                IpAddr::V4(address) => {
                    !address.is_loopback() && selected.is_none_or(|selected| selected == address)
                }
                IpAddr::V6(_) => false,
            })
            .map(|interface| interface.network)
            .collect()
    }

    pub fn ipv4_addresses(&self, selected: Option<Ipv4Addr>) -> BTreeSet<Ipv4Addr> {
        self.networks(selected)
            .into_iter()
            .filter_map(|network| match network.addr() {
                IpAddr::V4(address) => Some(address),
                IpAddr::V6(_) => None,
            })
            .collect()
    }

    pub fn ipv4_interface_indices(&self, selected: Option<Ipv4Addr>) -> Result<BTreeSet<u32>> {
        self.interfaces
            .iter()
            .filter(|interface| match interface.network.addr() {
                IpAddr::V4(address) => {
                    !address.is_loopback() && selected.is_none_or(|selected| selected == address)
                }
                IpAddr::V6(_) => false,
            })
            .map(|interface| {
                interface
                    .index
                    .filter(|index| *index != 0)
                    .with_context(|| {
                        format!("network interface {} has no OS index", interface.name)
                    })
            })
            .collect()
    }

    pub fn addresses(&self) -> BTreeSet<IpAddr> {
        self.interfaces
            .iter()
            .map(|interface| interface.network.addr())
            .collect()
    }
}

pub struct NetworkMonitor {
    watcher: IfWatcher,
    snapshots: watch::Sender<NetworkSnapshot>,
}

impl NetworkMonitor {
    pub fn new() -> Result<Self> {
        let watcher = IfWatcher::new().context("watch network interface changes")?;
        let (snapshots, _) = watch::channel(NetworkSnapshot::read()?);
        Ok(Self { watcher, snapshots })
    }

    pub fn subscribe(&self) -> watch::Receiver<NetworkSnapshot> {
        self.snapshots.subscribe()
    }

    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                event = self.watcher.next() => {
                    event.context("network interface watcher stopped")??;
                    let snapshot = NetworkSnapshot::read()?;
                    self.snapshots.send_if_modified(|previous| {
                        if *previous == snapshot {
                            return false;
                        }

                        *previous = snapshot;
                        true
                    });
                }
            }
        }
    }
}
