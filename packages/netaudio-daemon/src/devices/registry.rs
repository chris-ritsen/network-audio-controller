use super::discovery::{DiscoveredService, DiscoveryEvent};
use super::inventory;
use super::operations::ControlError;
use super::presence::{Lapses, Returned};
use super::record::{DeviceRecord, InventoryState};
use super::transport::{DeviceTransport, TransportError};
use crate::monitoring::diagnostics::Diagnostics;
use crate::monitoring::journal::service::Journal;
use crate::monitoring::metering::Metering;
use crate::monitoring::multicast::ReceivedPacket;
use crate::monitoring::requests::StatusRequests;
use crate::network::NetworkSnapshot;
use crate::state::ReadModel;
use anyhow::{bail, Context, Result};
use netaudio_core::parser::ChannelCount;
use netaudio_core::protocol::SERVICE_CMC;
use netaudio_core::responses::{DeviceSettings, PropertyDirectory};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex, Notify, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const MAXIMUM_CONCURRENT_REFRESHES: usize = 16;
const MAXIMUM_PENDING_OPERATIONS: usize = 128;
const READ_ATTEMPTS: u32 = 3;
const READ_RETRY_DELAY: Duration = Duration::from_secs(1);
const CLOCK_FRESHNESS: Duration = Duration::from_secs(5);
const CLOCK_FRESHNESS_LIMIT: Duration = Duration::from_secs(30);

mod association;
mod audio;
mod execution;
mod external_subscription;
mod flow_authoring;
mod forgetting;
mod heartbeat;
mod inspection;
mod locking;
mod maintenance;
mod metering;
mod multicast_prefix;
mod network_configuration;
mod observation;
mod panels;
mod preset_loading;
mod presets;
mod refresh_queue;
mod refreshing;
mod sample_rate;
mod status_configuration;

pub use forgetting::cache_precondition;
pub use heartbeat::{diagnostic_precondition, DiagnosticOptions};
pub use inspection::Inspection;
pub use metering::{MeteringLease, MeteringTarget};
use refresh_queue::{RefreshQueue, ScheduledRefresh};
use refreshing::{inventory_changed, read_inventory};

struct DeviceState {
    associated_at: Instant,
    association: Option<association::Signature>,
    clock_interval: Option<Duration>,
    diagnostics: Diagnostics,
    field_observed_at: BTreeMap<String, Instant>,
    generation: u64,
    identity_generation: u64,
    record: DeviceRecord,
}

struct Device {
    state: Mutex<DeviceState>,
    transport: AsyncMutex<Option<DeviceTransport>>,
    refresh: AsyncMutex<()>,
    inventory_changes: tokio::sync::watch::Sender<()>,
}

struct ClockLease {
    key: String,
    expires: Option<Instant>,
}

struct PacketObservation {
    key: String,
    clock: Option<ClockLease>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadProgress {
    pub reading: usize,
    pub working: bool,
}

#[derive(Default, Serialize)]
pub struct RefreshReport {
    pub errors: BTreeMap<&'static str, String>,
    pub observed: Vec<&'static str>,
    pub superseded: bool,
    #[serde(skip)]
    pub unanswered: bool,
}

struct RefreshData {
    name: String,
    counts: ChannelCount,
    channels: Option<inventory::ChannelInventory>,
    modern_receivers: Option<Value>,
    modern_transmitters: Option<Value>,
    settings: Option<DeviceSettings>,
    properties: Option<PropertyDirectory>,
    receiver_flows: Option<Value>,
    partial_receiver_flows: Option<Value>,
    receiver_flow_family: Option<&'static str>,
    receiver_flow_error: Option<String>,
    report: RefreshReport,
}

pub struct DeviceRegistry {
    pub metering: Arc<Metering>,
    model: Arc<ReadModel>,
    journal: Arc<Journal>,
    devices: RwLock<BTreeMap<String, Arc<Device>>>,
    association_generation: std::sync::atomic::AtomicU64,
    media: RwLock<BTreeMap<String, DiscoveredService>>,
    network: watch::Receiver<NetworkSnapshot>,
    refreshes: Arc<Semaphore>,
    refresh_queue: RefreshQueue,
    refresh_ready: Notify,
    progress: watch::Sender<ReadProgress>,
    pending: Arc<Semaphore>,
    tasks: TaskTracker,
    cancellation: CancellationToken,
    status_requests: StatusRequests,
    panel_requests: crate::monitoring::panels::PanelRequests,
    exports: crate::monitoring::exports::Exports,
    lock_settings: std::sync::OnceLock<crate::devices::locking::Settings>,
    managed: std::sync::OnceLock<Arc<crate::managed::registry::Registry>>,
    forgetting: AsyncMutex<()>,
    observation_origin: Instant,
    wire_observer: std::sync::OnceLock<crate::capture::Observer>,
}

impl DeviceRegistry {
    fn local_address(&self, destination: Ipv4Addr) -> Option<Ipv4Addr> {
        self.network.borrow().local_address(destination)
    }

    pub fn new(
        model: Arc<ReadModel>,
        network: watch::Receiver<NetworkSnapshot>,
        journal: Arc<Journal>,
    ) -> Arc<Self> {
        Arc::new(Self {
            metering: Metering::new(Arc::clone(&model)),
            model,
            journal,
            devices: RwLock::new(BTreeMap::new()),
            association_generation: std::sync::atomic::AtomicU64::new(1),
            media: RwLock::new(BTreeMap::new()),
            network,
            refreshes: Arc::new(Semaphore::new(MAXIMUM_CONCURRENT_REFRESHES)),
            refresh_queue: RefreshQueue::default(),
            refresh_ready: Notify::new(),
            progress: watch::channel(ReadProgress::default()).0,
            pending: Arc::new(Semaphore::new(MAXIMUM_PENDING_OPERATIONS)),
            tasks: TaskTracker::new(),
            cancellation: CancellationToken::new(),
            status_requests: StatusRequests::default(),
            panel_requests: crate::monitoring::panels::PanelRequests::default(),
            exports: crate::monitoring::exports::Exports::default(),
            lock_settings: std::sync::OnceLock::new(),
            managed: std::sync::OnceLock::new(),
            forgetting: AsyncMutex::new(()),
            observation_origin: Instant::now(),
            wire_observer: std::sync::OnceLock::new(),
        })
    }

    pub fn progress(&self) -> watch::Receiver<ReadProgress> {
        self.progress.subscribe()
    }

    pub fn configure_capture(&self, observer: crate::capture::Observer) -> Result<()> {
        self.wire_observer
            .set(observer)
            .map_err(|_| anyhow::anyhow!("Packet capture is already configured"))
    }

    pub fn wire_observer(&self) -> Option<crate::capture::Observer> {
        self.wire_observer.get().cloned()
    }

    pub async fn run(self: Arc<Self>, mut discovery: mpsc::Receiver<DiscoveryEvent>) -> Result<()> {
        let mut workers = JoinSet::new();
        let mut retries = JoinSet::new();
        let mut attempts = BTreeMap::new();
        let mut active = BTreeSet::new();
        let mut lapses = Lapses::default();
        let result = async {
            loop {
                while workers.len() < MAXIMUM_CONCURRENT_REFRESHES {
                    let Some(refresh) = self.refresh_queue.next(&active)? else {
                        break;
                    };
                    active.insert(refresh.key.clone());
                    let registry = Arc::clone(&self);
                    workers.spawn(async move {
                        let outcome = registry
                            .refresh_entry(Arc::clone(&refresh.device), Some(refresh.generation))
                            .await;
                        (refresh.key, refresh.device, outcome)
                    });
                }

                let progress = ReadProgress {
                    reading: self.reading()?,
                    working: !workers.is_empty() || !retries.is_empty(),
                };
                self.progress
                    .send_if_modified(|current| std::mem::replace(current, progress) != progress);

                tokio::select! {
                    _ = self.cancellation.cancelled() => return Ok(()),
                    _ = self.refresh_ready.notified() => {}
                    completed = workers.join_next(), if !workers.is_empty() => {
                        let (key, device, outcome) =
                            completed.context("inventory refresh worker disappeared")??;
                        active.remove(&key);

                        if let Some(delay) = self.conclude(&key, &device, outcome, &mut attempts)? {
                            retries.spawn(async move {
                                tokio::time::sleep(delay).await;
                                (key, device)
                            });
                        }
                    }
                    waited = retries.join_next(), if !retries.is_empty() => {
                        let (key, device) = waited.context("inventory retry timer disappeared")??;
                        self.reschedule(key, device)?;
                    }
                    name = lapses.withdrawn(), if !lapses.is_empty() => {
                        if let Some(name) = name {
                            self.withdraw(&name)?;
                        }
                    }
                    event = discovery.recv() => {
                        let Some(event) = event else {
                            bail!("device discovery stopped before registry shutdown");
                        };
                        self.observe(event, &mut lapses).await?;
                    }
                }
            }
        }
        .await;
        workers.shutdown().await;
        retries.shutdown().await;
        result
    }

    fn reading(&self) -> Result<usize> {
        let devices = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;
        let mut reading = 0;

        for device in devices.values() {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.record.inventory_state == Some(InventoryState::Reading) {
                reading += 1;
            }
        }

        Ok(reading)
    }

    fn conclude(
        &self,
        key: &str,
        device: &Arc<Device>,
        outcome: Result<RefreshReport>,
        attempts: &mut BTreeMap<String, u32>,
    ) -> Result<Option<Duration>> {
        let (unanswered, failure) = match &outcome {
            Ok(report) if report.superseded => {
                let reading = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?
                    .record
                    .inventory_state
                    == Some(InventoryState::Reading);

                if reading && !self.refresh_queue.contains(key)? {
                    self.reschedule(key.to_owned(), Arc::clone(device))?;
                }

                return Ok(None);
            }
            Ok(report) => (report.unanswered, None),
            Err(error) => {
                tracing::warn!(device = key, %error, "device inventory refresh failed");
                (
                    error
                        .downcast_ref::<TransportError>()
                        .is_some_and(TransportError::unanswered),
                    Some(error.to_string()),
                )
            }
        };

        if unanswered {
            let attempt = attempts.entry(key.to_owned()).or_insert(1);

            if *attempt < READ_ATTEMPTS {
                let delay = READ_RETRY_DELAY * *attempt;
                *attempt += 1;
                return Ok(Some(delay));
            }
        }

        attempts.remove(key);
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.record.control_address().is_none() {
            return Ok(None);
        }

        let previous = state.record.inventory_state;
        let mut changed = false;

        if let Some(reason) = failure {
            let errors = json!({"inventory": reason});
            changed = state.record.properties.get("read_errors") != Some(&errors);
            state
                .record
                .properties
                .insert("read_errors".to_owned(), errors);

            if previous != Some(InventoryState::Read) {
                state.record.inventory_state = Some(InventoryState::Unread);
            }
        } else {
            state.record.inventory_state = Some(InventoryState::Read);
        }

        if changed || state.record.inventory_state != previous {
            self.publish(&state.record)?;
        }

        Ok(None)
    }

    fn reschedule(&self, key: String, device: Arc<Device>) -> Result<()> {
        let generation = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.record.control_address().is_none() {
                return Ok(());
            }

            state.generation
        };
        self.schedule_refresh(key, device, generation)
    }

    fn schedule_refresh(&self, key: String, device: Arc<Device>, generation: u64) -> Result<()> {
        self.refresh_queue.schedule(ScheduledRefresh {
            key,
            device,
            generation,
        })?;
        self.refresh_ready.notify_one();
        Ok(())
    }

    async fn observe(self: &Arc<Self>, event: DiscoveryEvent, lapses: &mut Lapses) -> Result<()> {
        let now = Instant::now();

        match event {
            DiscoveryEvent::Resolved(service) => {
                if let Returned::Widened(lifetime) = lapses.returned(&service.name, now) {
                    tracing::info!(
                        service = service.name,
                        lifetime_seconds = lifetime.as_secs(),
                        "advertisement returned after it was withdrawn; its next lapse is held longer"
                    );
                }

                if service.is_media() {
                    self.media
                        .write()
                        .map_err(|_| anyhow::anyhow!("media inventory lock poisoned"))?
                        .insert(service.name.clone(), service);
                } else {
                    for name in self.attach(service).await? {
                        if lapses.released(&name) {
                            self.withdraw(&name)?;
                        }
                    }
                }
            }
            DiscoveryEvent::Removed { name, .. } => {
                if self.advertisements()?.contains(&name) {
                    lapses.lapsed(name, now);
                }
            }
            DiscoveryEvent::NetworkChanged => {
                for name in self.advertisements()? {
                    lapses.lapsed(name, now);
                }
            }
        }

        Ok(())
    }

    fn advertisements(&self) -> Result<BTreeSet<String>> {
        let mut names: BTreeSet<String> = self
            .media
            .read()
            .map_err(|_| anyhow::anyhow!("media inventory lock poisoned"))?
            .keys()
            .cloned()
            .collect();
        let devices = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;

        for device in devices.values() {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
            names.extend(state.record.services.keys().cloned());
        }

        Ok(names)
    }

    fn withdraw(&self, name: &str) -> Result<()> {
        self.media
            .write()
            .map_err(|_| anyhow::anyhow!("media inventory lock poisoned"))?
            .remove(name);
        let devices = self
            .devices
            .write()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;
        let now = Instant::now();
        let mut withdrawn = Vec::new();

        for device in devices.values() {
            let mut state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.record.services.remove(name).is_some() {
                state.record.expect_inventory(false);
                state.generation += 1;
                state.identity_generation += 1;
                let advertised = state.record.advertised();
                state.record.presence.advertised(advertised, false, now);

                if !state.record.online() {
                    self.metering.invalidate_direct(&state.record.key)?;
                }

                if state.record.control_address().is_none() {
                    self.refresh_queue.remove(&state.record.key)?;
                }

                withdrawn.push(state.record.key.clone());
            }
        }

        if !withdrawn.is_empty() {
            self.refresh_associations(&devices, &withdrawn)?;
        }

        Ok(())
    }

    async fn attach(self: &Arc<Self>, service: DiscoveredService) -> Result<Vec<String>> {
        let key = service.device_key()?;
        let now = Instant::now();
        let identity = (service.service_type == SERVICE_CMC)
            .then(|| service.property("id").map(str::to_owned))
            .flatten();
        let mut superseded = Vec::new();
        let (device, changed) = {
            let mut devices = self
                .devices
                .write()
                .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;

            let result = if let Some(device) = devices.get(&key) {
                let mut state = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
                let previous_address = state.record.address;
                let previous_identity = state.record.properties.get("mac_address").cloned();
                let changed = state.record.attach(service, now)?;

                if previous_address != state.record.address
                    || previous_identity.as_ref() != state.record.properties.get("mac_address")
                {
                    self.metering.invalidate_direct(&state.record.key)?;
                }

                if changed {
                    state.generation += 1;
                    state.identity_generation += 1;
                }

                (Arc::clone(device), changed)
            } else {
                let record = DeviceRecord::discovered(&service, now)?;
                let device = Arc::new(Device {
                    state: Mutex::new(DeviceState {
                        associated_at: Instant::now(),
                        association: None,
                        clock_interval: None,
                        diagnostics: Diagnostics::default(),
                        field_observed_at: BTreeMap::new(),
                        generation: 1,
                        identity_generation: 1,
                        record,
                    }),
                    transport: AsyncMutex::new(None),
                    refresh: AsyncMutex::new(()),
                    inventory_changes: tokio::sync::watch::channel(()).0,
                });
                devices.insert(key.clone(), Arc::clone(&device));
                (device, true)
            };

            if let Some(identity) = &identity {
                for (other, device) in devices.iter().filter(|(other, _)| **other != key) {
                    let state = device
                        .state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

                    if state.record.properties.get("mac_address") == Some(&json!(identity)) {
                        tracing::debug!(device = other, successor = key, "device identity moved");
                        superseded.extend(state.record.services.keys().cloned());
                    }
                }
            }

            if result.1 {
                self.refresh_associations(&devices, std::slice::from_ref(&key))?;
            }

            result
        };

        if changed {
            let generation = {
                let state = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

                if state.record.control_address().is_none() {
                    return Ok(superseded);
                }

                state.generation
            };
            self.schedule_refresh(key, device, generation)?;
        }

        Ok(superseded)
    }

    pub fn configure_managed(
        &self,
        managed: Arc<crate::managed::registry::Registry>,
    ) -> Result<()> {
        self.managed
            .set(managed)
            .map_err(|_| anyhow::anyhow!("managed control already configured"))
    }

    pub async fn stop(&self) {
        self.cancellation.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }

    fn publish(&self, record: &DeviceRecord) -> Result<()> {
        if record.retired {
            return Ok(());
        }

        self.model
            .replace_device(record.key.clone(), record.presentation()?)?;
        Ok(())
    }
}
