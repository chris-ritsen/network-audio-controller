mod audit;
pub mod correlations;
#[cfg(target_os = "linux")]
mod neighbours;
mod session;

use anyhow::{Context, Result, ensure};
use netaudio_core::shure::{Change, Device};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub struct Registry {
    state: Mutex<State>,
    model: Arc<crate::state::ReadModel>,
    history: Arc<crate::monitoring::level_history::LevelHistory>,
    journal: Arc<crate::monitoring::journal::service::Journal>,
    changed: tokio::sync::Notify,
    legacy_memory: std::path::PathBuf,
    connecting: Arc<tokio::sync::Semaphore>,
    pub correlations: correlations::Configuration,
}

struct State {
    next_generation: u64,
    devices: BTreeMap<String, Entry>,
    remembered: BTreeMap<String, Value>,
    discovery: Value,
}

struct Entry {
    generation: u64,
    address: Ipv4Addr,
    device: Option<Device>,
    status: Value,
    requests: mpsc::Sender<Request>,
    cancellation: CancellationToken,
}

struct Request {
    arguments: Value,
    expected: Option<Value>,
    reply: oneshot::Sender<Outcome>,
}

pub struct Outcome {
    pub status: u16,
    pub value: Value,
}

impl Registry {
    pub fn new(
        model: Arc<crate::state::ReadModel>,
        history: Arc<crate::monitoring::level_history::LevelHistory>,
        journal: Arc<crate::monitoring::journal::service::Journal>,
        legacy_memory: std::path::PathBuf,
        configuration_path: std::path::PathBuf,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                next_generation: 0,
                devices: BTreeMap::new(),
                remembered: BTreeMap::new(),
                discovery: json!({"state": "initializing"}),
            }),
            model,
            history,
            journal,
            changed: tokio::sync::Notify::new(),
            legacy_memory,
            correlations: correlations::Configuration::new(configuration_path),
            connecting: Arc::new(tokio::sync::Semaphore::new(8)),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("Wireless state lock poisoned"))
    }

    pub fn records(&self) -> Result<Value> {
        let state = self.lock()?;
        Ok(Value::Object(
            state
                .devices
                .iter()
                .filter_map(|(mac, entry)| {
                    let mut record = entry.device.as_ref()?.presentation();
                    record["control_status"] = entry.status.clone();
                    Some((mac.clone(), record))
                })
                .collect(),
        ))
    }

    pub fn device(&self, selector: &str) -> Result<Option<Value>> {
        let state = self.lock()?;
        let Some(entry) = select(&state, selector)? else {
            return Ok(None);
        };
        Ok(entry.device.as_ref().map(|device| {
            let mut value = device.presentation();
            value["control_status"] = entry.status.clone();
            value
        }))
    }

    pub fn prepare(&self, arguments: &Value) -> Result<(Change, Value)> {
        let state = self.lock()?;
        let selector = arguments["device"]
            .as_str()
            .context("A wireless device is required")?;
        let entry = select(&state, selector)?.context("Wireless device not found")?;
        ensure!(
            entry.status["state"] == "ready",
            "Wireless control connection is not ready"
        );
        let device = entry
            .device
            .as_ref()
            .context("Wireless inventory is unavailable")?;
        let change = Change::prepare(device, arguments)?;
        let precondition = precondition(entry, &change);
        Ok((change, precondition))
    }

    pub async fn execute(&self, arguments: Value, expected: Option<Value>) -> Result<Outcome> {
        let sender = {
            let state = self.lock()?;
            let selector = arguments["device"]
                .as_str()
                .context("A wireless device is required")?;
            let entry = select(&state, selector)?.context("Wireless device not found")?;
            ensure!(
                entry.status["state"] == "ready",
                "Wireless control connection is not ready"
            );
            entry.requests.clone()
        };
        let (reply, result) = oneshot::channel();
        sender
            .try_send(Request {
                arguments,
                expected,
                reply,
            })
            .map_err(|_| anyhow::anyhow!("Wireless control queue is full or unavailable"))?;
        result
            .await
            .context("Wireless command ended without a result")
    }

    fn prepare_owned(&self, mac: &str, generation: u64, request: &Request) -> Result<Change> {
        let state = self.lock()?;
        let entry = state
            .devices
            .get(mac)
            .filter(|entry| entry.generation == generation)
            .context("Wireless connection identity changed")?;
        let device = entry
            .device
            .as_ref()
            .context("Wireless inventory is unavailable")?;
        ensure!(device.online, "Wireless device is offline");
        let change = Change::prepare(device, &request.arguments)?;
        ensure!(
            request
                .expected
                .as_ref()
                .is_none_or(|expected| *expected == precondition(entry, &change)),
            "Wireless state changed after the preview"
        );
        Ok(change)
    }

    fn observe(
        &self,
        mac: &str,
        generation: u64,
        device: &Device,
        substantive: bool,
    ) -> Result<()> {
        let mut state = self.lock()?;
        let Some(entry) = state
            .devices
            .get_mut(mac)
            .filter(|entry| entry.generation == generation)
        else {
            return Ok(());
        };
        let first = entry.device.is_none();
        entry.device = Some(device.clone());
        if substantive {
            self.changed.notify_one();
        }
        let mut record = device.presentation();
        record["control_status"] = entry.status.clone();
        self.model.shure_device(
            mac,
            record,
            substantive.then_some(if first {
                "shure_device_discovered"
            } else {
                "shure_device_updated"
            }),
        )
    }

    fn status(&self, mac: &str, generation: u64, status: Value) -> Result<()> {
        let mut state = self.lock()?;
        let Some(entry) = state
            .devices
            .get_mut(mac)
            .filter(|entry| entry.generation == generation)
        else {
            return Ok(());
        };
        if entry.status == status {
            return Ok(());
        }
        entry.status = status;
        self.changed.notify_one();
        if let Some(device) = entry.device.as_mut() {
            device.online = entry.status["state"] == "ready";
            let mut record = device.presentation();
            record["control_status"] = entry.status.clone();
            self.model
                .shure_device(mac, record, Some("shure_device_updated"))?;
        }
        Ok(())
    }

    pub async fn run(
        self: Arc<Self>,
        cancellation: CancellationToken,
        ready: oneshot::Sender<Result<(), String>>,
    ) -> Result<()> {
        let initialized = async {
            self.restore().await?;
            self.correlations.reload(&self.model).await
        }
        .await;
        if let Err(error) = initialized {
            let _ = ready.send(Err(error.to_string()));
            return Err(error);
        }
        let _ = ready.send(Ok(()));
        let correlations = self
            .correlations
            .run(Arc::clone(&self.model), cancellation.child_token());
        let discovery = async {
            let result = self.discovery(cancellation.child_token()).await;
            if result.is_err() {
                cancellation.cancel();
            }
            result
        };
        let (correlations, discovery) = tokio::join!(correlations, discovery);
        correlations?;
        discovery
    }

    pub fn discovery_state(&self) -> Result<Value> {
        Ok(self.lock()?.discovery.clone())
    }

    fn discovery_status(&self, status: Value) -> Result<()> {
        self.lock()?.discovery = status.clone();
        self.model.observe(
            "shure_discovery",
            status.clone(),
            json!({"event": "shure_discovery_status", "discovery": status}),
        )
    }

    #[cfg(target_os = "linux")]
    async fn discovery(self: &Arc<Self>, cancellation: CancellationToken) -> Result<()> {
        let mut tasks = JoinSet::new();
        let mut observer = match neighbours::Observer::new() {
            Ok(observer) => Some(observer),
            Err(error) => {
                self.discovery_status(json!({"state": "unavailable", "error": error.to_string()}))?;
                None
            }
        };
        let mut entries = BTreeMap::new();
        if let Some(source) = &mut observer {
            match source.snapshot().await {
                Ok(snapshot) => {
                    entries = snapshot;
                    self.discovery_status(json!({"state": "ready"}))?;
                }
                Err(error) => {
                    self.discovery_status(
                        json!({"state": "unavailable", "error": error.to_string()}),
                    )?;
                    observer = None;
                }
            }
        }
        let mut recovery_attempts = 0;
        let result = async {
            loop {
                self.reconcile(&entries, &mut tasks, &cancellation)?;
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = self.changed.notified() => self.persist().await?,
                    result = tasks.join_next(), if !tasks.is_empty() => {
                        result.context("Wireless session task disappeared")?.context("Wireless session task failed")??;
                    }
                    result = async { observer.as_mut().expect("active neighbour observer").changed().await }, if observer.is_some() => {
                        match result {
                            Ok(snapshot) => {
                                entries = snapshot;
                                recovery_attempts = 0;
                            }
                            Err(error) => {
                                tracing::error!(%error, "Wireless neighbour observation lost continuity");
                                self.discovery_status(json!({"state": "recovering", "error": error.to_string()}))?;
                                entries.clear();
                                self.reconcile(&entries, &mut tasks, &cancellation)?;
                                recovery_attempts += 1;
                                let recovered = async {
                                    ensure!(recovery_attempts <= 3, "Wireless neighbour recovery budget exhausted");
                                    let mut replacement = neighbours::Observer::new()?;
                                    let snapshot = replacement.snapshot().await?;
                                    Ok::<_, anyhow::Error>((replacement, snapshot))
                                }.await;
                                match recovered {
                                    Ok((replacement, snapshot)) => {
                                        observer = Some(replacement);
                                        entries = snapshot;
                                        self.discovery_status(json!({"state": "ready"}))?;
                                    }
                                    Err(error) => {
                                        observer = None;
                                        self.discovery_status(json!({"state": "unavailable", "error": error.to_string()}))?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }.await;
        for entry in self.lock()?.devices.values() {
            entry.cancellation.cancel();
        }
        while let Some(result) = tasks.join_next().await {
            result.context("Wireless session task failed")??;
        }
        self.persist().await?;
        result
    }

    #[cfg(target_os = "linux")]
    fn reconcile(
        self: &Arc<Self>,
        entries: &neighbours::Entries,
        tasks: &mut JoinSet<Result<()>>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let mut candidates: BTreeMap<String, std::collections::BTreeSet<Ipv4Addr>> =
            BTreeMap::new();
        for ((_, address), mac) in entries {
            candidates.entry(mac.clone()).or_default().insert(*address);
        }
        let desired: BTreeMap<_, _> = candidates
            .into_iter()
            .filter_map(|(mac, addresses)| {
                if addresses.len() == 1 {
                    Some((mac, *addresses.first().expect("one address")))
                } else {
                    tracing::warn!(%mac, ?addresses, "Wireless control address is ambiguous");
                    None
                }
            })
            .collect();
        let mut state = self.lock()?;
        for (mac, entry) in &mut state.devices {
            if desired.get(mac) != Some(&entry.address) && !entry.cancellation.is_cancelled() {
                entry.cancellation.cancel();
                entry.status = json!({"state": "unavailable", "error": "Wireless neighbour identity is no longer present"});
                self.changed.notify_one();
                if let Some(device) = entry.device.as_mut() {
                    device.online = false;
                    let mut record = device.presentation();
                    record["control_status"] = entry.status.clone();
                    self.model
                        .shure_device(mac, record, Some("shure_device_updated"))?;
                }
            }
        }
        for (mac, address) in desired {
            if state
                .devices
                .get(&mac)
                .is_some_and(|entry| entry.address == address && !entry.cancellation.is_cancelled())
            {
                continue;
            }
            state.next_generation = state
                .next_generation
                .checked_add(1)
                .context("Wireless generation exhausted")?;
            let generation = state.next_generation;
            let lifetime = cancellation.child_token();
            let (requests, receiver) = mpsc::channel(16);
            state.devices.insert(
                mac.clone(),
                Entry {
                    generation,
                    address,
                    device: None,
                    status: json!({"state": "connecting"}),
                    requests,
                    cancellation: lifetime.clone(),
                },
            );
            let registry = Arc::clone(self);
            tasks.spawn(async move {
                session::run(registry, mac, address, generation, receiver, lifetime).await
            });
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    async fn discovery(self: &Arc<Self>, cancellation: CancellationToken) -> Result<()> {
        self.discovery_status(json!({"state": "unavailable", "error": "Native neighbour observation is unavailable on this platform"}))?;
        cancellation.cancelled().await;
        Ok(())
    }

    async fn restore(&self) -> Result<()> {
        let mut records = self.journal.wireless_devices().await?;
        if records
            .as_object()
            .is_some_and(|records| records.is_empty())
        {
            let path = self.legacy_memory.clone();
            let restored = tokio::task::spawn_blocking(move || -> Result<Option<Value>> {
                use std::io::Read;
                let mut options = std::fs::OpenOptions::new();
                options.read(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(nix::libc::O_NONBLOCK);
                }
                let file = match options.open(&path) {
                    Ok(file) => file,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(error) => return Err(error).context("Open saved wireless inventory"),
                };
                ensure!(
                    file.metadata()?.is_file(),
                    "Saved wireless inventory is not a regular file"
                );
                let mut bytes = Vec::new();
                file.take(8 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
                ensure!(
                    bytes.len() <= 8 * 1024 * 1024,
                    "Saved wireless inventory exceeds its read budget"
                );
                let records: Value = serde_json::from_slice(&bytes)?;
                ensure!(
                    records.as_object().is_some_and(|records| records
                        .iter()
                        .all(|(mac, record)| !mac.is_empty() && record.is_object())),
                    "Saved wireless inventory is malformed"
                );
                Ok(Some(records))
            })
            .await?;
            match restored {
                Ok(Some(restored)) => {
                    self.journal.remember_wireless(restored.clone()).await?;
                    records = restored;
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(%error, "Saved wireless inventory could not be restored")
                }
            }
        }
        self.lock()?.remembered = serde_json::from_value(records)?;
        Ok(())
    }

    async fn persist(&self) -> Result<()> {
        let records = self.records()?;
        self.journal.remember_wireless(records.clone()).await?;
        self.lock()?
            .remembered
            .extend(serde_json::from_value::<BTreeMap<String, Value>>(records)?);
        Ok(())
    }

    pub fn remembered(&self) -> Result<BTreeMap<String, Value>> {
        let state = self.lock()?;
        Ok(state
            .remembered
            .iter()
            .filter(|(mac, _)| !state.devices.contains_key(*mac))
            .map(|(mac, record)| {
                let mut record = record.clone();
                record["online"] = json!(false);
                (mac.clone(), record)
            })
            .collect())
    }
}

fn precondition(entry: &Entry, change: &Change) -> Value {
    json!({"generation": entry.generation, "address": entry.address, "mac": change.mac, "device_type": change.device_type,
        "channel": change.channel, "channel_name": change.channel_name, "key": change.key, "current": change.current})
}

fn select<'a>(state: &'a State, selector: &str) -> Result<Option<&'a Entry>> {
    let wanted = selector.trim();
    let normalized = wanted.replace([':', '-', '.'], "");
    let mut matches = state
        .devices
        .iter()
        .filter(|(mac, entry)| {
            wanted.eq_ignore_ascii_case(mac)
                || (normalized.len() == 12
                    && normalized.eq_ignore_ascii_case(&mac.replace(':', "")))
                || wanted == entry.address.to_string()
                || entry
                    .device
                    .as_ref()
                    .is_some_and(|device| device.name().eq_ignore_ascii_case(wanted))
        })
        .map(|(_, entry)| entry);
    let first = matches.next();
    ensure!(
        matches.next().is_none(),
        "Wireless selector matches multiple devices; use a MAC address"
    );
    Ok(first)
}
