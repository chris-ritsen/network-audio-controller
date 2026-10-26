use crate::devices::registry::{DeviceRegistry, MeteringLease, MeteringTarget};
use crate::events::EventOptions;
use crate::state::ReadModel;
use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const KEEPALIVE: Duration = Duration::from_secs(3);
const ABANDON_AFTER: Duration = Duration::from_secs(60);
const SNAPSHOT_DEADLINE: Duration = Duration::from_secs(3);
const MAXIMUM_STREAMS: usize = 128;
const MAXIMUM_CLIENTS_PER_STREAM: usize = 128;
const MAXIMUM_CLIENT_IDENTIFIER_BYTES: usize = 256;

#[derive(Default)]
struct StreamState {
    clients: BTreeSet<String>,
    temporary: u64,
    revision: u64,
    request_revision: u64,
    error: Option<String>,
    transmitted: bool,
}

#[derive(Clone, Copy)]
struct PortConfiguration {
    revision: u64,
    port: Option<u16>,
}

#[derive(Clone, Default)]
struct PortApplication {
    revision: u64,
    error: Option<String>,
}

struct Stream {
    state: Mutex<StreamState>,
    changed: Notify,
    port_applied: watch::Sender<PortApplication>,
}

impl Default for Stream {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            changed: Notify::new(),
            port_applied: watch::channel(PortApplication::default()).0,
        }
    }
}

impl Stream {
    fn lock_state(&self) -> Result<MutexGuard<'_, StreamState>> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("meter stream state lock poisoned"))
    }
}

pub struct MeteringReference(Arc<Stream>);

impl Drop for MeteringReference {
    fn drop(&mut self) {
        match self.0.state.lock() {
            Ok(mut state) => {
                state.temporary -= 1;
                self.0.changed.notify_one();
            }
            Err(error) => tracing::error!(%error, "could not release meter snapshot reference"),
        }
    }
}

pub struct MeteringStreams {
    registry: Arc<DeviceRegistry>,
    model: Arc<ReadModel>,
    streams: Mutex<BTreeMap<String, Arc<Stream>>>,
    port: watch::Sender<PortConfiguration>,
    cancellation: CancellationToken,
    tasks: TaskTracker,
    leases: Arc<Semaphore>,
}

impl MeteringStreams {
    pub fn new(
        registry: Arc<DeviceRegistry>,
        model: Arc<ReadModel>,
        port: u16,
        cancellation: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            model,
            streams: Mutex::new(BTreeMap::new()),
            port: watch::channel(PortConfiguration {
                revision: 0,
                port: Some(port),
            })
            .0,
            cancellation,
            tasks: TaskTracker::new(),
            leases: Arc::new(Semaphore::new(MAXIMUM_STREAMS)),
        })
    }

    fn stream(self: &Arc<Self>, name: &str) -> Result<Arc<Stream>> {
        ensure!(!self.cancellation.is_cancelled(), "metering is stopping");
        let mut streams = self
            .streams
            .lock()
            .map_err(|_| anyhow::anyhow!("meter stream lock poisoned"))?;

        if let Some(stream) = streams.get(name) {
            return Ok(Arc::clone(stream));
        }

        let stream = Arc::new(Stream::default());
        streams.insert(name.to_owned(), Arc::clone(&stream));
        let manager = Arc::clone(self);
        let worker = Arc::clone(&stream);
        let name = name.to_owned();
        self.tasks.spawn(async move {
            if let Err(error) = manager.run_stream(&name, &worker).await {
                tracing::error!(device = name, %error, "meter stream failed");

                if let Ok(mut state) = worker.state.lock() {
                    state.error = Some(error.to_string());
                    state.transmitted = false;
                }

                manager.cancellation.cancel();
            }
        });
        Ok(stream)
    }

    pub fn start(self: &Arc<Self>, name: &str, client: &str) -> Result<()> {
        ensure!(
            !client.is_empty() && client.len() <= MAXIMUM_CLIENT_IDENTIFIER_BYTES,
            "invalid metering client identifier"
        );
        self.registry.metering_target(name)?;
        let stream = self.stream(name)?;
        let mut state = stream.lock_state()?;
        ensure!(
            state.clients.contains(client) || state.clients.len() < MAXIMUM_CLIENTS_PER_STREAM,
            "too many metering clients"
        );
        state.clients.insert(client.to_owned());
        state.revision = state
            .revision
            .checked_add(1)
            .context("meter stream revision exhausted")?;
        state.request_revision = state.revision;
        stream.changed.notify_one();
        Ok(())
    }

    pub fn stop(&self, name: &str, client: &str) -> Result<()> {
        let streams = self
            .streams
            .lock()
            .map_err(|_| anyhow::anyhow!("meter stream lock poisoned"))?;

        if let Some(stream) = streams.get(name) {
            let mut state = stream.lock_state()?;
            state.clients.remove(client);
            stream.changed.notify_one();
        }

        Ok(())
    }

    pub fn acquire(self: &Arc<Self>, name: &str) -> Result<MeteringReference> {
        self.registry.metering_target(name)?;
        let stream = self.stream(name)?;
        {
            let mut state = stream.lock_state()?;
            ensure!(
                state.temporary < MAXIMUM_CLIENTS_PER_STREAM as u64,
                "too many pending meter snapshots"
            );
            state.temporary = state
                .temporary
                .checked_add(1)
                .context("meter snapshot reference overflow")?;
            state.revision = state
                .revision
                .checked_add(1)
                .context("meter stream revision exhausted")?;
            state.request_revision = state.revision;
            stream.changed.notify_one();
        }

        Ok(MeteringReference(stream))
    }

    pub async fn snapshot(self: &Arc<Self>, name: &str) -> Result<Option<Arc<Value>>> {
        let mut observed = self.registry.metering.subscribe(name)?;

        if let Some(sample) = self.registry.metering.sample(name)? {
            return Ok(Some(sample));
        }

        let managed = self
            .model
            .device(name)?
            .as_deref()
            .is_some_and(crate::managed::control::required);
        let _reference = if managed {
            None
        } else {
            Some(self.acquire(name)?)
        };
        let wait = async {
            loop {
                if let Some(sample) = self.registry.metering.sample(name)? {
                    return Ok(Some(sample));
                }

                observed
                    .changed()
                    .await
                    .context("meter observations stopped")?;
            }
        };

        tokio::select! {
            _ = self.cancellation.cancelled() => Ok(None),
            result = tokio::time::timeout(SNAPSHOT_DEADLINE, wait) => result.unwrap_or(Ok(None)),
        }
    }

    pub fn status(&self) -> Result<Value> {
        let streams = self
            .streams
            .lock()
            .map_err(|_| anyhow::anyhow!("meter stream lock poisoned"))?;
        let mut result = Map::new();

        for (name, stream) in streams.iter() {
            let state = stream.lock_state()?;

            if state.clients.is_empty() && state.temporary == 0 && state.error.is_none() {
                continue;
            }

            let record = self.model.device(name)?;
            let sample = self.registry.metering.sample(name)?;
            result.insert(name.clone(), json!({
                "name": record.as_ref().and_then(|record| record.get("name")),
                "server_name": name,
                "online": record.as_ref().and_then(|record| record.get("online")).and_then(Value::as_bool).unwrap_or(false),
                "receiving": sample.is_some(),
                "metering_source": sample.as_ref().and_then(|sample| sample.get("metering_source")),
                "request_transmitted": state.transmitted,
                "error": state.error,
            }));
        }

        Ok(Value::Object(result))
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let result = self.observe_devices().await;
        self.cancellation.cancel();
        self.tasks.close();
        self.tasks.wait().await;
        result
    }

    async fn observe_devices(&self) -> Result<()> {
        let options = EventOptions::from_query("patches=1&meters=0&telemetry=none");
        let mut events = self.model.subscribe(options)?;
        let mut targets = BTreeMap::new();

        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => return Ok(()),
                event = events.next() => {
                    let Some(event) = event else {
                        return Ok(());
                    };
                    self.observe_device_event(&event.value, &mut targets)?;
                }
            }
        }
    }

    fn observe_device_event(
        &self,
        event: &Value,
        targets: &mut BTreeMap<String, Result<MeteringTarget, String>>,
    ) -> Result<()> {
        let streams = self
            .streams
            .lock()
            .map_err(|_| anyhow::anyhow!("meter stream lock poisoned"))?;
        let selected: Vec<_> = if event["event"] == "snapshot" {
            streams.iter().collect()
        } else {
            event["server_name"]
                .as_str()
                .and_then(|name| streams.get_key_value(name))
                .into_iter()
                .collect()
        };

        for (name, stream) in selected {
            let target = self
                .registry
                .metering_target(name)
                .map_err(|error| error.to_string());

            if targets.get(name) == Some(&target) {
                continue;
            }

            targets.insert(name.clone(), target);
            let mut state = stream.lock_state()?;
            state.revision = state
                .revision
                .checked_add(1)
                .context("meter stream revision exhausted")?;
            stream.changed.notify_one();
        }

        Ok(())
    }

    async fn run_stream(&self, name: &str, stream: &Stream) -> Result<()> {
        let mut lease = None;
        let result = self.drive_stream(name, stream, &mut lease).await;
        let _ = Self::close_lease(name, stream, lease).await;
        result
    }

    pub async fn suspend(&self) -> Result<()> {
        let (revision, receivers) = {
            let streams = self
                .streams
                .lock()
                .map_err(|_| anyhow::anyhow!("meter stream lock poisoned"))?;
            let receivers = streams
                .iter()
                .map(|(name, stream)| (name.clone(), stream.port_applied.subscribe()))
                .collect::<Vec<_>>();
            (self.select_port(None)?, receivers)
        };
        let settled = futures_util::future::join_all(receivers.into_iter().map(|(name, mut applied)| async move {
            loop {
                let observed = applied.borrow_and_update().clone();

                if observed.revision >= revision {
                    return match observed.error {
                        Some(error) => Err(anyhow::anyhow!("{name}: {error}")),
                        None => Ok(()),
                    };
                }

                tokio::select! {
                    _ = self.cancellation.cancelled() => anyhow::bail!("metering stopped while suspending streams"),
                    changed = applied.changed() => changed.context("meter stream worker stopped")?,
                }
            }
        })).await;
        let failures = settled
            .into_iter()
            .filter_map(Result::err)
            .map(|error| error.to_string())
            .collect::<Vec<_>>();
        ensure!(
            failures.is_empty(),
            "could not suspend meter streams: {}",
            failures.join("; ")
        );
        Ok(())
    }

    pub fn resume(&self, port: u16) -> Result<()> {
        self.select_port(Some(port))?;
        Ok(())
    }

    fn select_port(&self, port: Option<u16>) -> Result<u64> {
        let previous = *self.port.borrow();
        let revision = previous
            .revision
            .checked_add(1)
            .context("meter port revision exhausted")?;
        self.port.send_replace(PortConfiguration { revision, port });
        Ok(revision)
    }

    async fn drive_stream(
        &self,
        name: &str,
        stream: &Stream,
        lease: &mut Option<MeteringLease>,
    ) -> Result<()> {
        let mut attempted = None;
        let mut blocked_request = None;
        let mut renewal = None;
        let mut configuration = self.port.subscribe();

        loop {
            if self.cancellation.is_cancelled() {
                return Ok(());
            }

            let changed = stream.changed.notified();
            let selected = *configuration.borrow_and_update();
            let (desired, revision, request_revision) = {
                let state = stream.lock_state()?;
                (
                    selected.port.is_some() && (!state.clients.is_empty() || state.temporary != 0),
                    state.revision,
                    state.request_revision,
                )
            };
            let target = desired.then(|| self.registry.metering_target(name));
            let matches = lease.as_ref().is_some_and(|lease| {
                selected.port == Some(lease.port)
                    && target.as_ref().and_then(|target| target.as_ref().ok())
                        == Some(&lease.target)
            });

            if lease.is_some() && !matches {
                if Self::close_lease(name, stream, lease.take()).await.is_err() {
                    blocked_request = Some(request_revision);
                }

                renewal = None;
                attempted = None;
            }

            let attempt = (revision, selected.revision);

            if desired
                && lease.is_none()
                && attempted != Some(attempt)
                && blocked_request != Some(request_revision)
            {
                attempted = Some(attempt);
                let opened = match target.context("meter stream has no target")? {
                    Ok(target) => {
                        self.open_lease(
                            target,
                            selected.port.context("metering port is suspended")?,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                let mut state = stream.lock_state()?;

                match opened {
                    Ok(opened) => {
                        *lease = Some(opened);
                        state.error = None;
                        state.transmitted = true;
                        renewal = Some(Instant::now() + KEEPALIVE);
                    }
                    Err(error) => {
                        if matches!(
                            error.downcast_ref::<crate::devices::transport::TransportError>(),
                            Some(crate::devices::transport::TransportError::WriteOutcomeUnknown(_))
                        ) {
                            blocked_request = Some(request_revision);
                        }

                        state.error = Some(error.to_string());
                        state.transmitted = false;
                        tracing::warn!(device = name, %error, "meter stream request failed");
                    }
                }
            }

            if !desired {
                attempted = None;

                if blocked_request != Some(request_revision) {
                    stream.lock_state()?.transmitted = false;
                }
            }

            let error = if blocked_request == Some(request_revision) {
                stream.lock_state()?.error.clone()
            } else {
                None
            };
            stream.port_applied.send_replace(PortApplication {
                revision: selected.revision,
                error,
            });
            let deadline = async {
                match renewal {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };

            tokio::select! {
                _ = self.cancellation.cancelled() => return Ok(()),
                _ = changed => {}
                changed = configuration.changed() => changed.context("metering port owner stopped")?,
                _ = deadline => {
                    match self.renew_lease(name, stream, lease).await {
                        Ok(next) => renewal = next,
                        Err(error) => {
                            blocked_request = Some(request_revision);
                            renewal = None;
                            stream.lock_state()?.error = Some(error.to_string());
                        }
                    }
                }
            }
        }
    }

    async fn open_lease(&self, target: MeteringTarget, port: u16) -> Result<MeteringLease> {
        let permit = Arc::clone(&self.leases)
            .try_acquire_owned()
            .context("concurrent meter stream limit")?;
        MeteringLease::open(target, port, permit, self.registry.wire_observer()).await
    }

    async fn renew_lease(
        &self,
        name: &str,
        stream: &Stream,
        lease: &mut Option<MeteringLease>,
    ) -> Result<Option<Instant>> {
        let now = Instant::now();
        let observed = self.registry.metering.detailed_observed(name)?;
        let observed_owned = lease.as_ref().is_some_and(|active| {
            observed.is_some_and(|observed| {
                observed >= active.started_at && now < observed + ABANDON_AFTER
            })
        });

        if observed_owned && let Some(active) = lease.as_mut() {
            match active.renew().await {
                Ok(()) => return Ok(Some(now + KEEPALIVE)),
                Err(error) => stream.lock_state()?.error = Some(error.to_string()),
            }
        } else {
            stream.lock_state()?.error = Some("no recent detailed metering data".to_owned());
        }

        Self::close_lease(name, stream, lease.take()).await?;
        Ok(None)
    }

    async fn close_lease(name: &str, stream: &Stream, lease: Option<MeteringLease>) -> Result<()> {
        if let Some(lease) = lease {
            let result = lease.close().await;

            if let Err(error) = &result {
                tracing::error!(device = name, %error, "owned meter stream stop failed");
            }

            if let Ok(mut state) = stream.state.lock() {
                match &result {
                    Ok(()) => state.transmitted = false,
                    Err(error) => {
                        state.error = Some(format!("owned meter stream stop failed: {error}"))
                    }
                }
            }

            result?;
        }

        Ok(())
    }
}
