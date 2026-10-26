use super::discovery::{DiscoveredService, DiscoveryEvent};
use super::inventory;
use super::operations::{ControlCompletion, ControlError, ControlOperation, ControlReadback};
use super::record::DeviceRecord;
use super::transport::{CommandIntent, DeviceAddress, DeviceTransport, TransportError};
use crate::monitoring::diagnostics::Diagnostics;
use crate::monitoring::journal::service::Journal;
use crate::monitoring::metering::Metering;
use crate::monitoring::requests::{self, StatusQuery, StatusRequests};
use crate::monitoring::{multicast::ReceivedPacket, status};
use crate::state::ReadModel;
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use netaudio_core::parser::ChannelCount;
use netaudio_core::protocol::{SERVICE_ARC, SERVICE_CMC};
use netaudio_core::responses::{DeviceSettings, PropertyDirectory};
use netaudio_core::responses::{NetaudioNotification, parse_notification_envelope};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, Notify, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::time::DelayQueue;

const MAXIMUM_CONCURRENT_REFRESHES: usize = 16;
const MAXIMUM_PENDING_OPERATIONS: usize = 128;
const CLOCK_FRESHNESS: Duration = Duration::from_secs(5);

mod audio;
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
mod panels;
mod preset_loading;
mod presets;
mod refresh_queue;
mod sample_rate;
mod status_configuration;

pub use forgetting::cache_precondition;
pub use heartbeat::{DiagnosticOptions, diagnostic_precondition};
pub use inspection::Inspection;
pub use metering::{MeteringLease, MeteringTarget};
use refresh_queue::{RefreshQueue, ScheduledRefresh};

struct DeviceState {
    generation: u64,
    identity_generation: u64,
    field_observed_at: BTreeMap<String, Instant>,
    record: DeviceRecord,
    diagnostics: Diagnostics,
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

#[derive(Default, Serialize)]
pub struct RefreshReport {
    pub observed: Vec<&'static str>,
    pub errors: BTreeMap<&'static str, String>,
    pub superseded: bool,
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
    media: RwLock<BTreeMap<String, DiscoveredService>>,
    local_address: Option<Ipv4Addr>,
    refreshes: Arc<Semaphore>,
    refresh_queue: RefreshQueue,
    refresh_ready: Notify,
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
    pub fn new(
        model: Arc<ReadModel>,
        local_address: Option<Ipv4Addr>,
        journal: Arc<Journal>,
    ) -> Arc<Self> {
        Arc::new(Self {
            metering: Metering::new(Arc::clone(&model)),
            model,
            journal,
            devices: RwLock::new(BTreeMap::new()),
            media: RwLock::new(BTreeMap::new()),
            local_address,
            refreshes: Arc::new(Semaphore::new(MAXIMUM_CONCURRENT_REFRESHES)),
            refresh_queue: RefreshQueue::default(),
            refresh_ready: Notify::new(),
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
        let mut active = BTreeSet::new();
        let result = async {
            loop {
                while workers.len() < MAXIMUM_CONCURRENT_REFRESHES {
                    let Some(refresh) = self.refresh_queue.next(&active)? else {
                        break;
                    };
                    active.insert(refresh.key.clone());
                    let registry = Arc::clone(&self);
                    workers.spawn(async move {
                        if let Err(error) = registry
                            .refresh_entry(refresh.device, Some(refresh.generation))
                            .await
                        {
                            tracing::warn!(device = refresh.key, %error, "device inventory refresh failed");
                        }

                        refresh.key
                    });
                }

                tokio::select! {
                    _ = self.cancellation.cancelled() => return Ok(()),
                    _ = self.refresh_ready.notified() => {}
                    completed = workers.join_next(), if !workers.is_empty() => {
                        let key = completed.context("inventory refresh worker disappeared")??;
                        active.remove(&key);
                    }
                    event = discovery.recv() => {
                        let Some(event) = event else {
                            bail!("device discovery stopped before registry shutdown");
                        };
                        self.observe(event).await?;
                    }
                }
            }
        }
        .await;
        workers.shutdown().await;
        result
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

    pub async fn observe(self: &Arc<Self>, event: DiscoveryEvent) -> Result<()> {
        match event {
            DiscoveryEvent::Resolved(service) if service.is_media() => {
                self.media
                    .write()
                    .map_err(|_| anyhow::anyhow!("media inventory lock poisoned"))?
                    .insert(service.name.clone(), service);
            }
            DiscoveryEvent::Resolved(service) => self.attach(service).await?,
            DiscoveryEvent::Removed { name, .. } => {
                self.media
                    .write()
                    .map_err(|_| anyhow::anyhow!("media inventory lock poisoned"))?
                    .remove(&name);
                let devices = self.entries()?;

                for device in devices {
                    let mut state = device
                        .state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

                    if state.record.services.remove(&name).is_some() {
                        state.generation += 1;
                        state.identity_generation += 1;
                        state.record.online = state.record.service(SERVICE_ARC).is_some()
                            || state.record.service(SERVICE_CMC).is_some();
                        if !state.record.online {
                            self.metering.invalidate_direct(&state.record.key)?;
                        }
                        self.publish(&state.record)?;
                    }
                }
            }
            DiscoveryEvent::NetworkChanged => {
                self.media
                    .write()
                    .map_err(|_| anyhow::anyhow!("media inventory lock poisoned"))?
                    .clear();

                for device in self.entries()? {
                    let mut state = device
                        .state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
                    state.generation += 1;
                    state.identity_generation += 1;
                    state.record.services.clear();
                    state.record.online = false;
                    self.metering.invalidate_direct(&state.record.key)?;
                    self.publish(&state.record)?;
                }
            }
        }

        Ok(())
    }

    async fn attach(self: &Arc<Self>, service: DiscoveredService) -> Result<()> {
        let key = service.device_key()?;
        let (device, changed) = {
            let mut devices = self
                .devices
                .write()
                .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;

            if let Some(device) = devices.get(&key) {
                let mut state = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
                let previous_address = state.record.address;
                let previous_identity = state.record.properties.get("mac_address").cloned();
                let changed = state.record.attach(service)?;

                if previous_address != state.record.address
                    || previous_identity.as_ref() != state.record.properties.get("mac_address")
                {
                    self.metering.invalidate_direct(&state.record.key)?;
                }

                if changed {
                    state.generation += 1;
                    state.identity_generation += 1;
                }

                self.publish(&state.record)?;
                (Arc::clone(device), changed)
            } else {
                let record = DeviceRecord::discovered(&service)?;
                self.publish(&record)?;
                let device = Arc::new(Device {
                    state: Mutex::new(DeviceState {
                        generation: 1,
                        identity_generation: 1,
                        field_observed_at: BTreeMap::new(),
                        record,
                        diagnostics: Diagnostics::default(),
                    }),
                    transport: AsyncMutex::new(None),
                    refresh: AsyncMutex::new(()),
                    inventory_changes: tokio::sync::watch::channel(()).0,
                });
                devices.insert(key.clone(), Arc::clone(&device));
                (device, true)
            }
        };

        if changed {
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
            self.schedule_refresh(key, device, generation)?;
        }

        Ok(())
    }

    pub async fn refresh(self: &Arc<Self>, reference: &str) -> Result<RefreshReport> {
        let record = self.model.device(reference)?.context("device not found")?;
        let key = record
            .get("server_name")
            .and_then(Value::as_str)
            .context("device has no server name")?;
        let device = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?
            .get(key)
            .cloned()
            .context("device has no native control transport")?;
        self.refresh_entry(device, None).await
    }

    pub fn request_refresh(&self, reference: &str) -> Result<()> {
        let Some(record) = self.model.device(reference)? else {
            return Ok(());
        };
        let key = record["server_name"]
            .as_str()
            .context("device has no server name")?;
        let device = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?
            .get(key)
            .cloned();

        if let Some(device) = device {
            let generation = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?
                .generation;
            self.schedule_refresh(key.to_owned(), device, generation)?;
        }

        Ok(())
    }

    async fn refresh_entry(
        &self,
        device: Arc<Device>,
        expected_generation: Option<u64>,
    ) -> Result<RefreshReport> {
        let _serial = tokio::select! {
            _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
            guard = device.refresh.lock() => guard,
        };
        let started = Instant::now();
        let (generation, address, protocol_identifier) = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if expected_generation.is_some_and(|expected| expected != state.generation) {
                return Ok(RefreshReport {
                    superseded: true,
                    ..RefreshReport::default()
                });
            }

            let address = state
                .record
                .control_address()
                .context("device has no routing service")?;
            let protocol = state
                .record
                .routing_protocol()?
                .context("device has no ARC protocol metadata")?;
            (state.generation, address, protocol.protocol_id)
        };
        let _permit = tokio::select! {
            _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
            permit = self.refreshes.acquire() => permit?,
        };
        let mut owned_transport = device.transport.lock().await;

        if owned_transport
            .as_ref()
            .is_none_or(|transport| transport.address() != address)
        {
            *owned_transport = Some(
                DeviceTransport::connect(address, self.local_address)
                    .await?
                    .with_observer(self.wire_observer()),
            );
        }

        let transport = owned_transport
            .as_mut()
            .context("device transport was not created")?;

        let data = tokio::select! {
            _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
            data = read_inventory(transport, protocol_identifier) => data?,
        };
        let mut report = self.accept_inventory(&device, generation, started, data)?;

        if !report.superseded {
            tokio::select! {
                _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
                result = self.acquire_status(&device, transport, generation, &mut report) => result?,
            }
        }

        Ok(report)
    }

    async fn acquire_status(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        report: &mut RefreshReport,
    ) -> Result<()> {
        let (key, identity) = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation || state.record.service(SERVICE_CMC).is_none() {
                return Ok(());
            }

            (
                state.record.key.clone(),
                state
                    .record
                    .properties
                    .get("mac_address")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            )
        };
        let registration = match requests::register(transport, identity.as_deref()).await {
            Ok(registration) => registration,
            Err(error) => {
                report
                    .errors
                    .insert("monitoring_registration", error.to_string());
                return Ok(());
            }
        };
        {
            let mut state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation {
                report.superseded = true;
                return Ok(());
            }

            state.record.properties.insert(
                "control_addresses".to_owned(),
                json!(registration.control_addresses),
            );
            state.record.properties.insert(
                "settings_port".to_owned(),
                json!(transport.address().settings_port),
            );
            self.publish(&state.record)?;
        }

        for query in [
            StatusQuery::Manufacturer,
            StatusQuery::Platform,
            StatusQuery::Interface,
            StatusQuery::Clock,
            StatusQuery::SampleRate,
            StatusQuery::Encoding,
            StatusQuery::Pullup,
            StatusQuery::Codec,
            StatusQuery::Aes67,
        ] {
            {
                let state = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

                if state.generation != generation {
                    report.superseded = true;
                    return Ok(());
                }

                let required_capability = match query {
                    StatusQuery::SampleRate => Some("sample_rate_configuration_supported"),
                    StatusQuery::Encoding => Some("encoding_configuration_supported"),
                    StatusQuery::Pullup => Some("sample_rate_pullup_configuration_supported"),
                    StatusQuery::Codec => Some("generic_codec_control_supported"),
                    StatusQuery::Aes67 => Some("aes67_configuration_supported"),
                    _ => None,
                };

                if let Some(capability) = required_capability
                    && state
                        .record
                        .properties
                        .get(capability)
                        .and_then(Value::as_bool)
                        != Some(true)
                {
                    continue;
                }
            }

            match self
                .status_requests
                .query(transport, &key, identity.as_deref(), query)
                .await
            {
                Ok(_) => report.observed.push(query.kind().as_str()),
                Err(error) => {
                    tracing::debug!(device = key, status = query.kind().as_str(), %error, "initial status unavailable");
                    report
                        .errors
                        .insert(query.kind().as_str(), error.to_string());
                }
            }
        }

        Ok(())
    }

    fn accept_inventory(
        &self,
        device: &Device,
        generation: u64,
        started: Instant,
        data: RefreshData,
    ) -> Result<RefreshReport> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        let mut report = data.report;

        if generation != state.generation {
            report.superseded = true;
            return Ok(report);
        }

        let mut record = state.record.clone();
        record.name = data.name;
        record.apply_counts(&data.counts)?;

        if let Some(channels) = data.channels {
            record.apply_legacy_channels(channels)?;
        }

        if let Some(receivers) = data.modern_receivers {
            record.apply_modern_channels(&receivers, true)?;
        }

        if let Some(transmitters) = data.modern_transmitters {
            record.apply_modern_channels(&transmitters, false)?;
        }

        if let Some(settings) = data.settings {
            record.apply_settings(&settings)?;
        }

        if let Some(properties) = data.properties {
            record.apply_property_directory(&properties)?;
        }

        record.apply_receiver_inventory(
            data.receiver_flow_family,
            data.receiver_flows,
            data.partial_receiver_flows,
            data.receiver_flow_error.as_deref(),
        )?;

        for (field, observed_at) in &state.field_observed_at {
            if *observed_at > started {
                if let Some(value) = state.record.properties.get(field) {
                    record.properties.insert(field.clone(), value.clone());
                } else {
                    record.properties.remove(field);
                }
            }
        }

        record.online = true;
        state.diagnostics.last_seen = Some(Instant::now());
        record.properties.insert(
            "last_seen".to_owned(),
            json!(chrono::Utc::now().timestamp_micros() as f64 / 1_000_000.0),
        );
        for (operation, error) in &report.errors {
            tracing::warn!(
                device = record.key,
                operation,
                error,
                "device read unavailable"
            );
        }

        self.publish(&record)?;
        state.record = record;
        Ok(report)
    }

    pub async fn apply(
        self: &Arc<Self>,
        reference: &str,
        operation: ControlOperation,
    ) -> Result<ControlCompletion, ControlError> {
        self.apply_conditionally(reference, operation, None).await
    }

    pub fn configure_managed(
        &self,
        managed: Arc<crate::managed::registry::Registry>,
    ) -> Result<()> {
        self.managed
            .set(managed)
            .map_err(|_| anyhow::anyhow!("managed control already configured"))
    }

    pub fn bind_managed_precondition(
        &self,
        reference: &str,
        condition: &mut Value,
    ) -> Result<(), ControlError> {
        if let Some(record) = self.model.device(reference)?
            && crate::managed::control::required(&record)
        {
            let managed = self.managed.get().ok_or_else(|| {
                ControlError::Unavailable("Managed control is unavailable".into())
            })?;
            condition["managed_configuration"] = json!(managed.configuration_revision()?);
        }
        Ok(())
    }

    fn controlled_device(&self, reference: &str) -> Result<Arc<Device>, ControlError> {
        let record = self
            .model
            .device(reference)?
            .ok_or_else(|| ControlError::NotFound("device not found".to_owned()))?;
        if crate::managed::control::required(&record) {
            return Err(ControlError::Conflict(
                "Enrolled devices require managed control".into(),
            ));
        }
        let key = record
            .get("server_name")
            .and_then(Value::as_str)
            .context("device has no server name")?;
        let device = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?
            .get(key)
            .cloned()
            .context("device has no native control transport")?;
        Ok(device)
    }

    pub fn validate_operation(
        &self,
        reference: &str,
        operation: &ControlOperation,
    ) -> Result<(), ControlError> {
        if let Some(record) = self.model.device(reference)?
            && crate::managed::control::required(&record)
        {
            return crate::managed::control::validate(&record, operation);
        }
        self.planned_batch_count(reference, operation).map(|_| ())
    }

    pub fn planned_batch_count(
        &self,
        reference: &str,
        operation: &ControlOperation,
    ) -> Result<usize, ControlError> {
        if let Some(record) = self.model.device(reference)?
            && crate::managed::control::required(&record)
        {
            return crate::managed::control::command_count(operation);
        }
        let device = self.controlled_device(reference)?;
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        let plan = operation.plan(&state.record)?;

        if let ControlOperation::Subscriptions { changes } = operation {
            let protocol = state
                .record
                .routing_protocol()?
                .context("device has no ARC service metadata")?;
            let clears = changes
                .iter()
                .filter(|change| {
                    matches!(change, super::operations::SubscriptionChange::Clear { .. })
                })
                .count();
            return Ok(clears.div_ceil(protocol.subscription_batch_limit)
                + (changes.len() - clears).div_ceil(protocol.subscription_batch_limit));
        }

        Ok(plan.command_count())
    }

    pub async fn apply_conditionally(
        self: &Arc<Self>,
        reference: &str,
        operation: ControlOperation,
        precondition: Option<Value>,
    ) -> Result<ControlCompletion, ControlError> {
        if self.cancellation.is_cancelled() {
            return Err(ControlError::Unavailable(
                "device registry is stopping".to_owned(),
            ));
        }

        if let Some(record) = self.model.device(reference)?
            && crate::managed::control::required(&record)
        {
            let managed = self.managed.get().ok_or_else(|| {
                ControlError::Unavailable("Managed control is unavailable".into())
            })?;
            return managed
                .control(crate::managed::control::DeviceRequest {
                    record: record.as_ref().clone(),
                    operation,
                    precondition,
                    configuration_revision: managed.configuration_revision()?,
                })
                .await;
        }

        let device = self.controlled_device(reference)?;
        let permit = Arc::clone(&self.pending)
            .try_acquire_owned()
            .map_err(|_| ControlError::Unavailable("device operation queue is full".to_owned()))?;
        let registry = Arc::clone(self);
        let (sender, receiver) = oneshot::channel();
        self.tasks.spawn(async move {
            let _permit = permit;
            let result = registry
                .execute_operation(device, operation, precondition)
                .await;

            if let Err(error) = &result {
                tracing::warn!(%error, "device operation failed");
            }

            if sender.send(result).is_err() {
                tracing::info!("device operation completed after its caller disconnected");
            }
        });
        receiver
            .await
            .map_err(|_| ControlError::Unavailable("device operation worker stopped".to_owned()))?
    }

    async fn execute_operation(
        &self,
        device: Arc<Device>,
        operation: ControlOperation,
        precondition: Option<Value>,
    ) -> Result<ControlCompletion, ControlError> {
        let _serial = device.refresh.lock().await;
        if device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?
            .record
            .retired
        {
            return Err(ControlError::Conflict(
                "device was removed from the cache".to_owned(),
            ));
        }

        let snapshot = || -> Result<Value> {
            device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?
                .record
                .presentation()
        };
        let mut audit = super::audit::Audit::begin(&self.journal, snapshot()?, &operation).await?;
        let mut result = self
            .execute_recorded_operation(&device, &operation, precondition, audit.as_mut())
            .await;

        if let Some(audit) = audit {
            if let Ok(completion) = &mut result
                && let Some(observation) = completion.observation.as_mut()
                && matches!(operation, ControlOperation::FlowAuthoring(_))
            {
                observation["operation_id"] = json!(audit.identifier());
            }

            audit
                .finish(&self.journal, &operation, snapshot()?, &result)
                .await
                .context("record final device operation result; do not replay the command")?;
        }

        result
    }

    async fn execute_recorded_operation(
        &self,
        device: &Arc<Device>,
        operation: &ControlOperation,
        precondition: Option<Value>,
        audit: Option<&mut super::audit::Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        if self.cancellation.is_cancelled() {
            return Err(ControlError::Unavailable(
                "device registry is stopping".to_owned(),
            ));
        }

        let (generation, address, protocol_identifier) = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if self
                .model
                .device(&state.record.key)?
                .as_ref()
                .is_some_and(|record| crate::managed::control::required(record))
            {
                return Err(ControlError::Conflict(
                    "Device became managed before the operation; request a new preview".into(),
                ));
            }

            let protocol = state
                .record
                .routing_protocol()?
                .context("device has no ARC service metadata")?;
            let address = state
                .record
                .control_address()
                .context("device has no routing service")?;
            (state.generation, address, protocol.protocol_id)
        };
        let mut owned_transport = device.transport.lock().await;

        if owned_transport
            .as_ref()
            .is_none_or(|transport| transport.address() != address)
        {
            *owned_transport = Some(
                DeviceTransport::connect(address, self.local_address)
                    .await
                    .map_err(|error| ControlError::Transport {
                        error,
                        acknowledged_commands: 0,
                    })?
                    .with_observer(self.wire_observer()),
            );
        }

        let transport = owned_transport
            .as_mut()
            .context("device transport was not created")?;

        if let ControlOperation::Panel(operation) = operation {
            return self
                .control_panel(
                    device,
                    transport,
                    generation,
                    operation,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::Locking(requested) = operation {
            return self
                .configure_lock(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::Maintenance(requested) = operation {
            return self
                .maintain_device(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::SampleRate(requested) = operation {
            return self
                .configure_sample_rate(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if matches!(
            operation,
            ControlOperation::MulticastPrefix(_) | ControlOperation::InspectMulticastPrefix
        ) {
            let configuration = match operation {
                ControlOperation::MulticastPrefix(configuration) => Some(configuration),
                _ => None,
            };
            return self
                .configure_multicast_prefix(
                    device,
                    transport,
                    generation,
                    configuration,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::FlowAuthoring(requested) = operation {
            return self
                .author_flow(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::NetworkConfiguration(requested) = operation {
            return self
                .configure_network(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::StatusConfiguration(requested) = operation {
            return self
                .configure_status(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::InspectStatus(inspection) = operation {
            return self
                .inspect_status(device, transport, generation, *inspection)
                .await;
        }

        if matches!(operation, ControlOperation::RefreshExternalReceiver) {
            let fresh = self
                .refresh_external_receiver(device, transport, generation)
                .await?;
            return Ok(ControlCompletion {
                acknowledged_commands: 0,
                readback_completed: fresh.before.is_some(),
                readback_errors: fresh.errors,
                observation: fresh.before,
            });
        }

        if let ControlOperation::ExternalSubscription(subscription) = operation {
            return self
                .execute_external_subscription(
                    device,
                    transport,
                    subscription,
                    generation,
                    precondition,
                    audit,
                )
                .await;
        }

        let initial_clock = match &operation {
            ControlOperation::Clock(clock) => Some(
                self.read_clock(device, transport, generation, clock.profile, None)
                    .await
                    .map_err(|error| ControlError::Unavailable(error.to_string()))?,
            ),
            ControlOperation::RefreshClock { profile } => Some(
                self.read_clock(device, transport, generation, *profile, None)
                    .await
                    .map_err(|error| ControlError::Unavailable(error.to_string()))?,
            ),
            _ => None,
        };
        let audio_kind = match &operation {
            ControlOperation::AudioConfiguration(configuration) => Some(configuration.kind),
            ControlOperation::RefreshAudio(kind) => Some(*kind),
            _ => None,
        };
        let initial_audio = match audio_kind {
            Some(kind) => Some(
                self.read_audio(device, transport, generation, kind, None)
                    .await
                    .map_err(|error| {
                        tracing::warn!(%error, "initial audio configuration observation unavailable");
                        ControlError::ObservationUnavailable(format!("{} readback was unavailable", kind.label()))
                    })?,
            ),
            None => None,
        };
        let mut plan = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation {
                return Err(ControlError::Conflict(
                    "device identity changed before configuration request".to_owned(),
                ));
            }

            if let Some(expected) = &precondition
                && operation.precondition(&state.record.presentation()?) != *expected
            {
                return Err(ControlError::Conflict(
                    "the device configuration changed after confirmation; obtain a new preview before retrying".to_owned(),
                ));
            }

            operation.plan(&state.record)?
        };

        if let ControlReadback::Performance(performance) = &plan.readback {
            self.invalidate_performance(device, generation, performance.requested.keys().copied())?;
        }

        let mut execution = plan.execute(transport).await;

        if let Some(audit) = audit {
            audit
                .acknowledgement(&self.journal, &plan.acknowledgements, plan.command_count())
                .await
                .context("record device acknowledgements; do not replay the command")?;
        }

        if execution.is_ok() {
            for event in plan.pending_events {
                self.model.publish(event)?;
            }
        }

        let mut completion = ControlCompletion {
            acknowledged_commands: execution.as_ref().copied().unwrap_or_default(),
            readback_completed: false,
            readback_errors: Vec::new(),
            observation: None,
        };

        match plan.readback {
            ControlReadback::AudioStatus(kind) => {
                completion.readback_completed = true;
                completion.observation = Some(kind.status(&json!(initial_audio)));
                return Ok(completion);
            }
            ControlReadback::AudioConfiguration {
                operation,
                request_sent,
            } => {
                execution?;
                let observed = if request_sent {
                    match self
                        .read_audio(
                            device,
                            transport,
                            generation,
                            operation.kind,
                            Some(&operation),
                        )
                        .await
                    {
                        Ok(observed) => Some(observed),
                        Err(error) => {
                            completion.readback_errors.push(error.to_string());
                            None
                        }
                    }
                } else {
                    initial_audio
                };
                completion.readback_completed = observed.is_some();
                completion.observation =
                    Some(operation.completion(observed.as_ref(), request_sent)?);
                return Ok(completion);
            }
            ControlReadback::TransmitFlows(protocols) => {
                let inventory = tokio::select! {
                    _ = self.cancellation.cancelled() => return Err(ControlError::Unavailable("device registry is stopping".to_owned())),
                    inventory = super::flows::read_transmit(transport, &protocols) => inventory?,
                };
                let mut state = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

                if state.generation != generation {
                    return Err(ControlError::Conflict(
                        "device identity changed during flow inventory".to_owned(),
                    ));
                }

                for (field, value) in [
                    ("flow_protocol_id", json!(inventory.protocol)),
                    ("transmitter_flows", inventory.raw["flows"].clone()),
                    ("transmitter_flow_inventory", inventory.raw),
                ] {
                    state.record.properties.insert(field.to_owned(), value);
                    state
                        .field_observed_at
                        .insert(field.to_owned(), Instant::now());
                }

                self.publish(&state.record)?;
                let mut inventory = inventory.canonical;
                inventory["device"] = json!(state.record.key);
                completion.readback_completed = true;
                completion.observation = Some(inventory);
                return Ok(completion);
            }
            ControlReadback::Clock(clock) => {
                let observed = if clock.request_sent {
                    match self
                        .read_clock(device, transport, generation, clock.profile, Some(&clock))
                        .await
                    {
                        Ok(observed) => Some(observed),
                        Err(error) => {
                            completion.readback_errors.push(error.to_string());
                            None
                        }
                    }
                } else {
                    initial_clock
                };
                completion.readback_completed = observed.is_some();
                let mut result = clock.completion(observed.as_ref());

                if let Err(error) = execution {
                    result["request_sent"] = json!(false);
                    result["transport_error"] = json!(error.to_string());
                    result["effective_state_confirmed"] = json!(false);
                }

                completion.observation = Some(result);
                return Ok(completion);
            }
            ControlReadback::Performance(performance) => {
                let readback = if let Some(query) = performance.readback_command() {
                    match transport.execute(&query, CommandIntent::Read).await {
                        Ok(response) => {
                            if let Some(settings) =
                                netaudio_core::responses::parse_device_settings(&response)
                            {
                                self.accept_settings(device, generation, &settings)?;
                                completion.readback_completed = true;
                            }

                            Some(response)
                        }
                        Err(error) => {
                            completion.readback_errors.push(error.to_string());
                            None
                        }
                    }
                } else {
                    None
                };
                let mut result =
                    performance.completion(plan.acknowledgements.into_iter().next(), readback)?;

                if let Err(ControlError::Transport { error, .. }) = &execution {
                    result["transport_error"] = json!(error.to_string());
                    result["outcome_unknown"] =
                        json!(matches!(error, TransportError::WriteOutcomeUnknown(_)));
                }

                completion.observation = Some(result);
                return Ok(completion);
            }
            ControlReadback::None => return execution.map(|_| completion),
            ControlReadback::Latency { milliseconds } => {
                let settings = self.read_latency(device, transport, generation).await;
                let acknowledged = match &execution {
                    Ok(_) => Some(true),
                    Err(ControlError::Rejected { .. }) => Some(false),
                    Err(_) => None,
                };
                let observed = match settings {
                    Ok(settings) => {
                        completion.readback_completed = true;
                        Some(serde_json::to_value(settings).map_err(anyhow::Error::from)?)
                    }
                    Err(error) => {
                        completion.readback_errors.push(error.to_string());
                        None
                    }
                };
                let request: netaudio_core::latency_configuration::LatencyControl =
                    serde_json::from_value(json!({
                        "requested_milliseconds": milliseconds,
                        "acknowledged": acknowledged,
                        "settings": observed,
                    }))
                    .map_err(anyhow::Error::from)?;
                completion.observation = Some(
                    serde_json::to_value(
                        request
                            .resolve()
                            .map_err(|error| anyhow::anyhow!("{error}"))?,
                    )
                    .map_err(anyhow::Error::from)?,
                );
                return execution.map(|_| completion);
            }
            ControlReadback::Inventory => {}
        }

        let started = Instant::now();
        match read_inventory(transport, protocol_identifier).await {
            Ok(data) => {
                let report = self.accept_inventory(device, generation, started, data)?;
                completion.readback_completed = !report.superseded && report.errors.is_empty();
                completion.readback_errors = report.errors.into_values().collect();
            }
            Err(error) => {
                tracing::warn!(%error, "device operation readback failed");
                completion.readback_errors.push(error.to_string());
            }
        }

        if let Err(ControlError::Subscription(failure)) = &mut execution {
            failure.readback_completed = completion.readback_completed;
            failure
                .readback_errors
                .clone_from(&completion.readback_errors);
        }

        execution.map(|_| completion)
    }

    async fn read_latency(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
    ) -> Result<DeviceSettings> {
        let response = transport
            .execute(
                &json!({"command": "query_latency_config"}),
                CommandIntent::Read,
            )
            .await?;
        let settings = netaudio_core::responses::parse_device_settings(&response)
            .ok_or(TransportError::MalformedResponse("latency readback"))?;
        self.accept_settings(device, generation, &settings)?;
        Ok(settings)
    }

    async fn read_clock(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        profile: u16,
        expected: Option<&super::clock::ClockReadback>,
    ) -> Result<serde_json::Map<String, Value>> {
        let key = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation || !state.record.online {
                bail!("device identity changed before clock observation");
            }

            state.record.key.clone()
        };
        let observed = tokio::select! {
            _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
            observed = self.status_requests.query_until(
                transport,
                &key,
                None,
                StatusQuery::ClockProfile(profile),
                |fields| expected.is_none_or(|expected| expected.matches(fields)),
            ) => observed?,
        };
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation {
            bail!("device identity changed during clock observation");
        }

        Ok(observed)
    }

    fn accept_settings(
        &self,
        device: &Device,
        generation: u64,
        settings: &DeviceSettings,
    ) -> Result<()> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation {
            bail!("device identity changed during configuration readback");
        }

        let previous = state.record.properties.clone();
        state.record.apply_settings(settings)?;
        let received = Instant::now();
        let changed: Vec<_> = state
            .record
            .properties
            .iter()
            .filter(|(field, value)| previous.get(*field) != Some(*value))
            .map(|(field, _)| field.clone())
            .collect();

        for field in changed {
            state.field_observed_at.insert(field, received);
        }

        self.publish(&state.record)?;
        Ok(())
    }

    fn invalidate_performance(
        &self,
        device: &Device,
        generation: u64,
        properties: impl Iterator<Item = u16>,
    ) -> Result<()> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation {
            bail!("device identity changed before configuration request");
        }

        let mut invalidated = false;

        if let Some(values) = state
            .record
            .properties
            .get_mut("performance_settings")
            .and_then(Value::as_object_mut)
        {
            for property in properties {
                invalidated |= values.remove(&property.to_string()).is_some();
            }
        }

        if invalidated {
            state
                .field_observed_at
                .insert("performance_settings".to_owned(), Instant::now());
            self.publish(&state.record)?;
        }

        Ok(())
    }

    pub async fn monitor(
        self: Arc<Self>,
        mut packets: mpsc::Receiver<ReceivedPacket>,
        mut heartbeats: mpsc::Receiver<ReceivedPacket>,
        mut meters: mpsc::Receiver<ReceivedPacket>,
    ) -> Result<()> {
        let mut deadlines = DelayQueue::<String>::new();
        let mut clock_leases = BTreeMap::new();
        let mut diagnostic_deadlines = DelayQueue::<String>::new();
        let mut diagnostic_leases = BTreeMap::new();

        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => return Ok(()),
                packet = meters.recv() => {
                    let packet = packet.context("metering receiver stopped")?;

                    match self.observe_metering(&packet) {
                        Ok(Some(key)) => self.schedule_diagnostics(&key, &mut diagnostic_deadlines, &mut diagnostic_leases)?,
                        Ok(None) => {}
                        Err(error) => tracing::warn!(source = %packet.source, %error, "invalid detailed meter observation"),
                    }
                }
                expired = diagnostic_deadlines.next(), if !diagnostic_deadlines.is_empty() => {
                    if let Some(expired) = expired {
                        let key = expired.into_inner();
                        diagnostic_leases.remove(&key);
                        self.expire_diagnostics(&key)?;
                        self.schedule_diagnostics(&key, &mut diagnostic_deadlines, &mut diagnostic_leases)?;
                    }
                }
                packet = heartbeats.recv() => {
                    let packet = packet.context("heartbeat receiver stopped")?;

                    match self.observe_heartbeat(&packet) {
                        Ok(Some(key)) => self.schedule_diagnostics(&key, &mut diagnostic_deadlines, &mut diagnostic_leases)?,
                        Ok(None) => {},
                        Err(error) => tracing::warn!(source = %packet.source, %error, "invalid heartbeat observation"),
                    }
                }
                expired = deadlines.next(), if !deadlines.is_empty() => {
                    if let Some(expired) = expired {
                        let key = expired.into_inner();
                        clock_leases.remove(&key);
                        self.expire_clock(&key)?;
                    }
                }
                packet = packets.recv() => {
                    let packet = packet.context("monitoring receiver stopped")?;
                    match self.observe_packet(&packet) {
                        Ok(Some(observation)) => {
                            if let Some(lease) = observation.clock {
                                if let Some(deadline) = clock_leases.remove(&lease.key) {
                                    deadlines.remove(&deadline);
                                }

                                if let Some(expires) = lease.expires {
                                    clock_leases.insert(
                                        lease.key.clone(),
                                        deadlines.insert_at(lease.key, expires),
                                    );
                                }
                            }

                            self.schedule_diagnostics(&observation.key, &mut diagnostic_deadlines, &mut diagnostic_leases)?;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::warn!(source = %packet.source, %error, "invalid monitoring observation");
                        }
                    }
                }
            }
        }
    }

    fn observe_packet(
        self: &Arc<Self>,
        packet: &ReceivedPacket,
    ) -> Result<Option<PacketObservation>> {
        if packet.payload.is_empty() {
            if let Some(device) = self.observation_device(packet, None)? {
                let state = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
                self.exports.observe(&state.record.key, packet)?;
            }

            return Ok(None);
        }

        let Some(envelope) = parse_notification_envelope(&packet.payload) else {
            return Ok(None);
        };
        let device = self.observation_device(packet, envelope.device_id.as_deref())?;
        let Some(device) = device else {
            return Ok(None);
        };
        let mut refresh = None;
        let mut clock = None;
        let key;
        {
            let mut state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
            key = state.record.key.clone();
            if state.record.retired {
                return Ok(None);
            }

            state.diagnostics.last_seen = Some(packet.received_monotonic);

            if envelope.response_kind == Some("panel_status") {
                let family = super::panels::family_name(
                    netaudio_core::device_controls::profile(&super::panels::record_facts(
                        &state.record,
                    ))
                    .family,
                );
                if let Some(status) =
                    netaudio_core::device_controls::parse_panel_status(&packet.payload, family)
                {
                    let correlated =
                        self.panel_requests
                            .observe(&key, packet.received_monotonic, &status)?;
                    super::panels::observe(
                        &mut state.record,
                        &status,
                        correlated,
                        packet.received_at.timestamp_micros() as f64 / 1_000_000.0,
                    )?;
                    state
                        .field_observed_at
                        .insert("device_controls".to_owned(), packet.received_monotonic);
                    self.publish(&state.record)?;
                }
            } else if envelope.response_kind == Some("conmon_export_fragment") {
                self.exports.observe(&key, packet)?;
            } else if let Some(kind) = envelope.response_kind
                && let Some(observation) = status::decode(
                    kind,
                    &packet.payload,
                    &state.record.properties,
                    packet.received_at,
                )?
            {
                let mut clock_updated = false;
                let mut changed = false;

                for (field, value) in &observation.fields {
                    if state
                        .field_observed_at
                        .get(field)
                        .is_some_and(|observed| *observed > packet.received_monotonic)
                    {
                        continue;
                    }

                    clock_updated |= field == "clock_status";
                    changed = true;
                    state
                        .field_observed_at
                        .insert(field.clone(), packet.received_monotonic);
                    state.record.properties.insert(field.clone(), value.clone());
                }

                if clock_updated {
                    let status = state
                        .record
                        .properties
                        .get("clock_status")
                        .cloned()
                        .unwrap_or(Value::Null);
                    state
                        .diagnostics
                        .clock_status(&status, packet, self.observation_origin)?;
                    let fields = state.diagnostics.fields()?;

                    for (field, value) in fields {
                        state.record.properties.insert(field.clone(), value);
                        state
                            .field_observed_at
                            .insert(field, packet.received_monotonic);
                    }

                    let mut expires = None;

                    if let Some(status) = state.record.properties.get_mut("clock_status") {
                        let deadline = packet.received_monotonic + CLOCK_FRESHNESS;

                        if status.get("status_supported").and_then(Value::as_bool) == Some(false) {
                            status["observation_state"] = json!("unavailable");
                        } else if deadline <= Instant::now() {
                            expire_clock_status(status);
                        } else {
                            status["observation_state"] = json!("fresh");
                            expires = Some(deadline);
                        }
                    }

                    clock = Some(ClockLease {
                        key: state.record.key.clone(),
                        expires,
                    });
                }

                if changed {
                    state.record.online = true;
                    self.publish(&state.record)?;
                    self.status_requests.observe(
                        &state.record.key,
                        observation.kind,
                        packet.received_monotonic,
                        &observation.fields,
                    )?;
                }
            } else if inventory_changed(envelope.notification_id) {
                state.generation += 1;
                device.inventory_changes.send_replace(());
                refresh = Some((state.record.key.clone(), state.generation));
            }
        }

        if let Some((key, generation)) = refresh {
            self.schedule_refresh(key, device, generation)?;
        }

        Ok(Some(PacketObservation { key, clock }))
    }

    fn expire_clock(&self, key: &str) -> Result<()> {
        let device = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?
            .get(key)
            .cloned();
        let Some(device) = device else {
            return Ok(());
        };
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if let Some(status) = state.record.properties.get_mut("clock_status") {
            expire_clock_status(status);
            self.publish(&state.record)?;
        }

        Ok(())
    }

    pub async fn stop(&self) {
        self.cancellation.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }

    fn entries(&self) -> Result<Vec<Arc<Device>>> {
        Ok(self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?
            .values()
            .cloned()
            .collect())
    }

    fn publish(&self, record: &DeviceRecord) -> Result<()> {
        if record.retired {
            return Ok(());
        }

        self.model
            .replace_device(record.key.clone(), record.presentation()?)?;
        Ok(())
    }

    pub fn media_services(&self) -> Result<Vec<DiscoveredService>> {
        Ok(self
            .media
            .read()
            .map_err(|_| anyhow::anyhow!("media inventory lock poisoned"))?
            .values()
            .cloned()
            .collect())
    }

    pub fn control_address(&self, key: &str) -> Result<Option<DeviceAddress>> {
        let devices = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;
        let Some(device) = devices.get(key) else {
            return Ok(None);
        };
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        Ok(state.record.control_address())
    }
}

fn expire_clock_status(status: &mut Value) {
    if status.get("observation_state").and_then(Value::as_str) == Some("stale") {
        return;
    }

    status["observation_state"] = json!("stale");
    status["observed_synchronization"] = status
        .get("synchronization")
        .cloned()
        .unwrap_or_else(|| json!("unknown"));
    status["synchronization"] = json!("unknown");
}

fn inventory_changed(identifier: u16) -> bool {
    [
        NetaudioNotification::TopologyChange,
        NetaudioNotification::TxChannelChange,
        NetaudioNotification::RxChannelChange,
        NetaudioNotification::TxLabelChange,
        NetaudioNotification::TxFlowChange,
        NetaudioNotification::RxFlowChange,
        NetaudioNotification::PropertyChange,
        NetaudioNotification::SettingsChange,
        NetaudioNotification::RoutingDeviceChange,
        NetaudioNotification::RoutingReady,
    ]
    .into_iter()
    .any(|notification| notification as u16 == identifier)
}

async fn read_inventory(
    transport: &mut DeviceTransport,
    protocol_identifier: u16,
) -> Result<RefreshData> {
    let name = inventory::device_name(transport).await?;
    let counts = inventory::channel_count(transport).await?;
    let mut data = RefreshData {
        name,
        counts,
        channels: None,
        modern_receivers: None,
        modern_transmitters: None,
        settings: None,
        properties: None,
        receiver_flows: None,
        partial_receiver_flows: None,
        receiver_flow_family: None,
        receiver_flow_error: None,
        report: RefreshReport {
            observed: vec!["name", "channel_count"],
            ..RefreshReport::default()
        },
    };

    if netaudio_core::protocol::is_modern_arc_protocol(protocol_identifier) {
        for (kind, receiver) in [("rx_channels", true), ("tx_channels", false)] {
            match inventory::paged_inventory(transport, kind, protocol_identifier, |_| {}).await {
                Ok(inventory) => {
                    data.report.observed.push(kind);

                    if receiver {
                        data.modern_receivers = Some(inventory);
                    } else {
                        data.modern_transmitters = Some(inventory);
                    }
                }
                Err(error) => {
                    data.report.errors.insert(kind, error.to_string());
                }
            }
        }
    } else {
        match inventory::legacy_channels(transport, &data.counts).await {
            Ok(channels) => {
                data.channels = Some(channels);
                data.report.observed.push("channels");
            }
            Err(error) => {
                data.report.errors.insert("channels", error.to_string());
            }
        }
    }

    match inventory::settings(transport).await {
        Ok(settings) => {
            data.settings = Some(settings);
            data.report.observed.push("settings");
        }
        Err(error) => {
            data.report.errors.insert("settings", error.to_string());
        }
    }

    match inventory::properties(transport).await {
        Ok(properties) => {
            data.properties = Some(properties);
            data.report.observed.push("properties");
        }
        Err(error) => {
            data.report.errors.insert("properties", error.to_string());
        }
    }

    let capabilities = netaudio_core::parser::flow_authoring_capabilities(
        data.counts.transmit_flow_authoring_capability_word,
    );
    let (kind, family) = match capabilities.receiver_flow_query_family {
        "fixed" => ("fixed_rx_flows", "legacy"),
        "segmented" => ("segmented_rx_flows", "modern"),
        family => bail!("unsupported receive-flow inventory family {family}"),
    };
    data.receiver_flow_family = Some(family);

    match inventory::paged_inventory(transport, kind, protocol_identifier, |partial| {
        data.partial_receiver_flows = Some(Value::Object(partial.clone()));
    })
    .await
    {
        Ok(inventory) => {
            data.receiver_flows = Some(inventory);
            data.partial_receiver_flows = None;
            data.report.observed.push("receiver_flows");
        }
        Err(error) => {
            data.receiver_flow_error = Some(error.to_string());
        }
    }

    Ok(data)
}
