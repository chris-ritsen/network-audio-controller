use super::NetworkSnapshot;
use crate::preferences::Preferences;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

const MAXIMUM_INTERFACES: usize = 64;
const MAXIMUM_NAME_BYTES: usize = 255;
const PREFERENCE: &str = "dante_interfaces";
pub const FIXED_AT_LAUNCH: &str = "The Dante interfaces were chosen when the server was started.";

pub struct Selection {
    chosen: Mutex<BTreeSet<String>>,
    dante: watch::Sender<NetworkSnapshot>,
    every: watch::Sender<NetworkSnapshot>,
    fixed: bool,
    preferences: Arc<Preferences>,
}

impl Selection {
    pub fn open(preferences: Arc<Preferences>, launch: Option<&str>) -> Result<Arc<Self>> {
        let launch = launch.filter(|launch| !launch.trim().is_empty());
        let chosen = match launch {
            Some(launch) => names(launch.split(',').map(str::to_owned))?,
            None => saved(&preferences).unwrap_or_else(|error| {
                tracing::warn!(%error, "saved Dante interfaces ignored");
                BTreeSet::new()
            }),
        };
        Ok(Arc::new(Self {
            chosen: Mutex::new(chosen),
            dante: watch::channel(NetworkSnapshot::default()).0,
            every: watch::channel(NetworkSnapshot::default()).0,
            fixed: launch.is_some(),
            preferences,
        }))
    }

    pub fn dante(&self) -> watch::Receiver<NetworkSnapshot> {
        self.dante.subscribe()
    }

    pub fn every(&self) -> watch::Receiver<NetworkSnapshot> {
        self.every.subscribe()
    }

    pub fn observe(&self, snapshot: NetworkSnapshot) -> Result<()> {
        let chosen = self.chosen()?;
        self.publish(snapshot, &chosen);
        Ok(())
    }

    pub fn choose(&self, interfaces: Vec<String>) -> Result<Value> {
        ensure!(!self.fixed, FIXED_AT_LAUNCH);
        let replacement = names(interfaces)?;
        let mut chosen = self
            .chosen
            .lock()
            .map_err(|_| anyhow::anyhow!("interface selection lock poisoned"))?;
        self.preferences
            .set(PREFERENCE.to_owned(), json!(replacement))
            .context("save the Dante interfaces")?;
        *chosen = replacement.clone();
        let snapshot = self.every.borrow().clone();
        self.publish(snapshot, &replacement);
        drop(chosen);
        self.snapshot()
    }

    pub fn snapshot(&self) -> Result<Value> {
        let chosen = self.chosen()?;
        let every = self.every.borrow().clone();
        let mut interfaces = BTreeMap::<String, (BTreeSet<String>, bool)>::new();

        for interface in &every.interfaces {
            let IpAddr::V4(address) = interface.network.addr() else {
                continue;
            };

            if address.is_loopback() {
                continue;
            }

            let entry = interfaces.entry(interface.name.clone()).or_default();
            entry.0.insert(address.to_string());
            entry.1 |= interface.running;
        }

        Ok(json!({
            "dante_interfaces": chosen,
            "dante_interfaces_fixed": self.fixed,
            "network_interfaces": interfaces
                .into_iter()
                .map(|(name, (addresses, running))| json!({
                    "addresses": addresses,
                    "name": name,
                    "running": running,
                }))
                .collect::<Vec<_>>(),
        }))
    }

    fn chosen(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .chosen
            .lock()
            .map_err(|_| anyhow::anyhow!("interface selection lock poisoned"))?
            .clone())
    }

    fn publish(&self, snapshot: NetworkSnapshot, chosen: &BTreeSet<String>) {
        let dante = snapshot.selected(chosen);
        self.every.send_if_modified(|previous| {
            if *previous == snapshot {
                return false;
            }

            *previous = snapshot;
            true
        });
        self.dante.send_if_modified(|previous| {
            if *previous == dante {
                return false;
            }

            *previous = dante;
            true
        });
    }
}

fn names(candidates: impl IntoIterator<Item = String>) -> Result<BTreeSet<String>> {
    let names = candidates
        .into_iter()
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .collect::<BTreeSet<_>>();
    ensure!(
        names.len() <= MAXIMUM_INTERFACES
            && names.iter().all(|name| name.len() <= MAXIMUM_NAME_BYTES),
        "Dante interfaces must be a short list of interface names or IPv4 addresses"
    );
    Ok(names)
}

fn saved(preferences: &Preferences) -> Result<BTreeSet<String>> {
    match preferences.snapshot()?.get(PREFERENCE) {
        None | Some(Value::Null) => Ok(BTreeSet::new()),
        Some(Value::Array(values)) => names(
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .context("dante_interfaces must be a list of names")
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        Some(_) => anyhow::bail!("dante_interfaces must be a list of names"),
    }
}
