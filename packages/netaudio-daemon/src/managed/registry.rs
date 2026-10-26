use super::operations::Operation;
use super::resources::{ResourceWatch, Resources};
use super::{
    Credential, Error, Limits, Transport,
    client::Client,
    configuration::{Server, Settings},
    inventory::{InventoryState, freshness},
    store::{Snapshot, Store},
};
use crate::state::ReadModel;
use anyhow::{Context, Result};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

mod signals;
mod status;

pub struct Registry {
    state: Mutex<State>,
    pub(super) store: Store,
    pub(super) configuration_change: Arc<tokio::sync::Mutex<()>>,
    pub(super) model: Arc<ReadModel>,
    pub(super) journal: Arc<crate::monitoring::journal::service::Journal>,
    pub(super) requests: mpsc::Sender<Request>,
    changed: Notify,
    signal_changes: watch::Sender<()>,
}

struct State {
    snapshot: Snapshot,
    servers: BTreeMap<String, Entry>,
    status: Value,
    domains: Vec<Value>,
    deadline: Option<Instant>,
    configuration_error: Option<String>,
}

struct Entry {
    source: Arc<Source>,
    inventory: InventoryState,
    signals: BTreeMap<String, Value>,
    controller_status: super::status::observations::Cache,
}

pub(super) struct Source {
    pub configuration: Server,
    pub client: Option<Client>,
    pub transport: Option<Transport>,
    pub setup_error: Option<Error>,
    pub credential: Option<Credential>,
    resource_revision: [u8; 32],
    resource_paths: BTreeSet<PathBuf>,
    refresh: Semaphore,
}

pub struct Worker {
    registry: Arc<Registry>,
    receiver: mpsc::Receiver<Request>,
}

pub(super) enum Request {
    Control {
        request: super::control::DeviceRequest,
        response: oneshot::Sender<
            Result<
                crate::devices::operations::ControlCompletion,
                crate::devices::operations::ControlError,
            >,
        >,
    },
    Execute {
        operation: Operation,
        expected: Option<super::planning::Conditions>,
        response: oneshot::Sender<Result<Output, Error>>,
    },
    Prepare {
        operation: Operation,
        response: oneshot::Sender<Result<super::planning::Plan, Error>>,
    },
    Selection {
        server: Option<String>,
        response: oneshot::Sender<Result<super::planning::SelectionInventory, Error>>,
    },
}

pub struct Output {
    pub status: u16,
    pub value: Value,
}

impl Registry {
    pub async fn open(
        configuration: &crate::configuration::Configuration,
        model: Arc<ReadModel>,
        journal: Arc<crate::monitoring::journal::service::Journal>,
    ) -> Result<(Arc<Self>, Worker)> {
        let store = Store::new(configuration);
        let loader = store.clone();
        let (snapshot, servers) = tokio::task::spawn_blocking(move || -> Result<_, Error> {
            let snapshot = loader.snapshot()?;
            let servers = entries(&snapshot.settings, resources(&snapshot.settings));
            Ok((snapshot, servers))
        })
        .await
        .context("load managed configuration worker")??;
        let (requests, receiver) = mpsc::channel(32);
        let registry = Arc::new(Self {
            state: Mutex::new(State {
                snapshot,
                servers,
                status: Value::Null,
                domains: Vec::new(),
                deadline: None,
                configuration_error: None,
            }),
            store,
            configuration_change: Arc::new(tokio::sync::Mutex::new(())),
            model,
            journal,
            requests,
            changed: Notify::new(),
            signal_changes: watch::channel(()).0,
        });
        registry.publish()?;
        let worker = Worker {
            registry: Arc::clone(&registry),
            receiver,
        };
        Ok((registry, worker))
    }

    pub fn status(&self) -> Result<Value> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("managed inventory lock poisoned"))?
            .status
            .clone())
    }

    pub fn connections(&self) -> Result<Value> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("managed inventory lock poisoned"))?
            .snapshot
            .connections())
    }

    pub fn domains(&self, context: Option<&str>) -> Result<Vec<Value>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("managed inventory lock poisoned"))?;
        Ok(state
            .domains
            .iter()
            .filter(|domain| context.is_none_or(|context| domain["ddm_context"] == context))
            .cloned()
            .collect())
    }

    pub async fn execute(&self, operation: Operation) -> Result<Output, Error> {
        self.execute_conditionally(operation, None).await
    }

    pub async fn execute_conditionally(
        &self,
        operation: Operation,
        expected: Option<super::planning::Conditions>,
    ) -> Result<Output, Error> {
        let (response, received) = oneshot::channel();
        self.requests
            .try_send(Request::Execute {
                operation,
                expected,
                response,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::Busy,
                mpsc::error::TrySendError::Closed(_) => {
                    Error::Conflict("Managed service is stopping")
                }
            })?;
        received.await.map_err(|_| {
            Error::Conflict("Managed service stopped before the result was available")
        })?
    }

    async fn perform(
        &self,
        operation: Operation,
        expected: Option<super::planning::Conditions>,
    ) -> Result<Output, Error> {
        let _configuration = self.configuration_change.lock().await;
        let snapshot = self.load_configuration().await?;
        let inventory = if let Some(expected) = expected {
            let inspection = self.inspect(&operation, &snapshot).await?;
            if inspection.conditions != expected {
                return Err(Error::Conflict(
                    "Managed configuration or target changed after confirmation; request a new preview",
                ));
            }
            inspection.inventory
        } else {
            None
        };
        match operation {
            Operation::Login(request) => self.login(request, snapshot).await,
            Operation::Logout(name) => self.logout(name, snapshot).await,
            Operation::Profile(request) => self.edit_profile(request, snapshot).await,
            Operation::Context(request) => self.select_context(request, snapshot, inventory).await,
            Operation::Mutation(request) => self.mutate(request, inventory).await,
            Operation::Enrollment(request) => self.enroll(request, inventory).await,
            Operation::Refresh { context } => {
                if !self.enabled()? {
                    return Err(Error::Conflict("Managed API is not configured"));
                }
                let refreshed = self.refresh(context.as_deref()).await?;
                Ok(Output {
                    status: if refreshed { 200 } else { 502 },
                    value: self
                        .status()
                        .map_err(|_| Error::Conflict("Managed inventory is unavailable"))?,
                })
            }
            Operation::Graphql {
                query,
                variables,
                operation_name,
                context,
            } => {
                let (_, source) = self.selected_source(context.as_deref())?;
                let client = source.client.as_ref().ok_or_else(|| {
                    source.setup_error.clone().unwrap_or(Error::Conflict(
                        "Managed API is not configured for the selected context",
                    ))
                })?;
                let response = client
                    .execute(&query, variables, operation_name.as_deref())
                    .await?;
                Ok(Output {
                    status: 200,
                    value: json!(response),
                })
            }
        }
    }

    fn enabled(&self) -> Result<bool, Error> {
        let state = self.lock()?;
        Ok(state
            .servers
            .values()
            .any(|entry| entry.source.configuration.enabled()))
    }

    pub(super) fn selected_source(
        &self,
        context: Option<&str>,
    ) -> Result<(String, Arc<Source>), Error> {
        let state = self.lock()?;
        if !state
            .servers
            .values()
            .any(|entry| entry.source.configuration.enabled())
        {
            return Err(Error::Conflict("Managed API is not configured"));
        }
        let (name, _) = state.snapshot.settings.selected_server(context)?;
        Ok((name.to_owned(), Arc::clone(&state.servers[name].source)))
    }

    pub(super) fn source(&self, name: &str) -> Result<Arc<Source>, Error> {
        let state = self.lock()?;
        state
            .servers
            .get(name)
            .map(|entry| Arc::clone(&entry.source))
            .ok_or(Error::Invalid("Select a saved DDM server"))
    }

    pub(super) fn command_inventory(
        &self,
        name: &str,
        source: &Arc<Source>,
    ) -> Result<super::mutations::Inventory, Error> {
        let state = self.lock()?;
        let entry = state
            .servers
            .get(name)
            .filter(|entry| Arc::ptr_eq(&entry.source, source))
            .ok_or(Error::Conflict(
                "Managed configuration changed during the operation",
            ))?;
        Ok(super::mutations::Inventory {
            fresh: entry.inventory.fresh(),
            domains: entry
                .inventory
                .domains(name, &state.snapshot.settings.contexts),
            devices: entry
                .inventory
                .observations(name, &state.snapshot.settings.contexts),
        })
    }

    pub(super) async fn install(&self, snapshot: Snapshot) -> Result<(), Error> {
        let settings = snapshot.settings.clone();
        let resources = tokio::task::spawn_blocking(move || resources(&settings))
            .await
            .map_err(|_| Error::Persistence("Managed resources worker failed"))?;
        self.install_resources(snapshot, resources).await
    }

    pub(super) async fn install_resources(
        &self,
        snapshot: Snapshot,
        resources: BTreeMap<String, Resources>,
    ) -> Result<(), Error> {
        let settings = snapshot.settings.clone();
        let replacement = tokio::task::spawn_blocking(move || entries(&settings, resources))
            .await
            .map_err(|_| Error::Persistence("Managed configuration worker failed"))?;
        {
            let mut state = self.lock()?;
            let reusable = state.configuration_error.take().is_none();
            let previous = std::mem::take(&mut state.servers);
            state.servers = replacement;
            for (name, entry) in previous {
                if state.servers.get(&name).is_some_and(|replacement| {
                    reusable
                        && replacement.source.configuration == entry.source.configuration
                        && replacement.source.resource_revision == entry.source.resource_revision
                }) {
                    state.servers.insert(name, entry);
                }
            }
            state.snapshot = snapshot;
            self.publish_locked(&mut state)
                .map_err(|_| Error::Conflict("Managed inventory could not be published"))?;
        }
        self.changed.notify_one();
        self.signal_changes.send_replace(());
        Ok(())
    }

    pub(super) async fn refresh(&self, context: Option<&str>) -> Result<bool, Error> {
        if let Some(context) = context {
            let (name, _) = self.selected_source(Some(context))?;
            return self.refresh_server(&name).await;
        }
        let names: Vec<_> = self
            .lock()?
            .servers
            .iter()
            .filter(|(_, entry)| entry.source.configuration.enabled())
            .map(|(name, _)| name.clone())
            .collect();
        if names.is_empty() {
            return Ok(false);
        }
        let mut results = stream::iter(names)
            .map(|name| async move { self.refresh_server(&name).await })
            .buffer_unordered(4);
        let mut complete = true;
        let mut failure = None;
        while let Some(result) = results.next().await {
            match result {
                Ok(refreshed) => complete &= refreshed,
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(complete), Err)
    }

    pub(super) async fn refresh_server(&self, name: &str) -> Result<bool, Error> {
        let source = self.source(name)?;
        if !source.configuration.enabled() {
            return Ok(false);
        }
        let permit = source.refresh.try_acquire().map_err(|_| Error::Busy)?;
        {
            let mut state = self.lock()?;
            let entry = current_entry(&mut state, name, &source)?;
            entry.inventory.attempting();
            self.publish_locked(&mut state)
                .map_err(|_| Error::Conflict("Managed inventory could not be published"))?;
        }
        let result = match &source.client {
            Some(client) => client.inventory().await,
            None => Err(source
                .setup_error
                .clone()
                .unwrap_or(Error::Conflict("Managed API is not configured"))),
        };
        let accepted;
        {
            let mut state = self.lock()?;
            let entry = current_entry(&mut state, name, &source)?;
            match result {
                Ok((response, inventory)) => match entry.inventory.apply(
                    response,
                    inventory,
                    freshness(&source.configuration),
                ) {
                    Ok(result) => accepted = result,
                    Err(error) => {
                        entry.inventory.fail(&error);
                        accepted = false;
                    }
                },
                Err(error) => {
                    entry.inventory.fail(&error);
                    accepted = false;
                }
            }
            drop(permit);
            self.publish_locked(&mut state)
                .map_err(|_| Error::Conflict("Managed inventory could not be published"))?;
        }
        self.changed.notify_one();
        self.signal_changes.send_replace(());
        Ok(accepted)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>, Error> {
        self.state
            .lock()
            .map_err(|_| Error::Conflict("Managed inventory state is unavailable"))
    }

    fn publish(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("managed inventory lock poisoned"))?;
        self.publish_locked(&mut state)
    }

    fn publish_locked(&self, state: &mut State) -> Result<()> {
        let now = Instant::now();
        let contexts = &state.snapshot.settings.contexts;
        let servers: BTreeMap<_, _> = state
            .servers
            .iter()
            .map(|(name, entry)| {
                let mut status =
                    entry
                        .inventory
                        .status(name, &entry.source.configuration, contexts);
                status["refreshing"] = json!(entry.source.refresh.available_permits() == 0);
                status["signal_sessions"] = json!(entry.signals);
                (name.clone(), status)
            })
            .collect();
        let enabled: Vec<_> = servers
            .values()
            .filter(|server| server["enabled"] == true)
            .collect();
        let condition = if enabled.is_empty() {
            "disabled"
        } else if enabled.iter().any(|server| server["state"] == "degraded") {
            "degraded"
        } else if enabled.iter().any(|server| server["state"] == "starting") {
            "starting"
        } else if enabled.iter().any(|server| server["state"] == "stale") {
            "stale"
        } else {
            "ready"
        };
        let sum = |field: &str| {
            enabled
                .iter()
                .filter_map(|server| server[field].as_u64())
                .sum::<u64>()
        };
        state.status = json!({
            "enabled": !enabled.is_empty(), "state": condition, "default_context": state.snapshot.settings.default_context,
            "server_count": enabled.len(), "domain_count": sum("domain_count"), "device_count": sum("device_count"),
            "enrolled_device_count": sum("enrolled_device_count"), "unenrolled_device_count": sum("unenrolled_device_count"),
            "fresh": !enabled.is_empty() && enabled.iter().all(|server| server["fresh"] == true), "servers": servers,
            "refresh_mode": "explicit", "configuration_error": state.configuration_error,
        });
        state.domains = state
            .servers
            .iter()
            .flat_map(|(name, entry)| entry.inventory.domains(name, contexts))
            .collect();
        let mut observations = Vec::new();
        let mut controller_status = Vec::new();
        for (name, entry) in &mut state.servers {
            let source_observations = entry.inventory.observations(name, contexts);
            controller_status.extend(entry.controller_status.project(&source_observations, now)?);
            observations.extend(source_observations);
        }
        state.deadline = state
            .servers
            .values()
            .flat_map(|entry| {
                [
                    entry.inventory.deadline_after(now),
                    entry.controller_status.deadline_after(now),
                ]
            })
            .flatten()
            .min();
        self.model.managed_inventory(
            state.status.clone(),
            state.domains.clone(),
            observations,
            controller_status,
        )
    }

    fn deadline(&self) -> Result<Option<Instant>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("managed inventory lock poisoned"))?
            .deadline)
    }
}

impl Registry {
    pub(super) fn resources_changed(
        &self,
        snapshot: &Snapshot,
        resources: &BTreeMap<String, Resources>,
    ) -> Result<bool, Error> {
        let state = self.lock()?;
        Ok(state.configuration_error.is_some()
            || state.snapshot.revision != snapshot.revision
            || resources.iter().any(|(name, resources)| {
                state
                    .servers
                    .get(name)
                    .is_none_or(|entry| entry.source.resource_revision != resources.revision)
            }))
    }

    pub(super) fn state_revision(&self) -> Result<String, Error> {
        let state = self.lock()?;
        let mut revision = Sha256::new();
        revision.update(state.snapshot.revision);
        for (name, entry) in &state.servers {
            revision.update((name.len() as u64).to_be_bytes());
            revision.update(name.as_bytes());
            revision.update(entry.source.resource_revision);
        }
        if let Some(error) = &state.configuration_error {
            revision.update(error.as_bytes());
        }
        Ok(hex::encode(revision.finalize()))
    }

    fn resource_paths(&self) -> Result<BTreeSet<PathBuf>> {
        let mut paths = self.store.resource_paths();
        for entry in self.lock()?.servers.values() {
            paths.extend(entry.source.resource_paths.clone());
            for path in [
                entry.source.configuration.credential_file.as_deref(),
                entry.source.configuration.trust_certificate_file.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                paths.extend(super::resources::resource_paths(path));
            }
        }
        Ok(paths)
    }

    async fn reload_resources(&self) -> Result<()> {
        let _configuration = self.configuration_change.lock().await;
        let before = self.state_revision()?;
        match self.load_configuration().await {
            Ok(_) => {
                if self.state_revision()? != before
                    && let Err(error) = self.refresh(None).await
                {
                    tracing::error!(%error, "managed inventory refresh after resource replacement failed");
                }
            }
            Err(error) => {
                tracing::error!(%error, "managed configuration reload failed");
                let mut state = self.lock()?;
                let description = error.to_string();
                if state.configuration_error.as_ref() != Some(&description) {
                    state.configuration_error = Some(description);
                    for entry in state.servers.values_mut() {
                        entry.inventory.fail(&error);
                        entry.source = Arc::new(Source {
                            configuration: entry.source.configuration.clone(),
                            client: None,
                            transport: None,
                            credential: None,
                            setup_error: Some(error.clone()),
                            resource_revision: entry.source.resource_revision,
                            resource_paths: entry.source.resource_paths.clone(),
                            refresh: Semaphore::new(1),
                        });
                        entry.controller_status = super::status::observations::Cache::default();
                    }
                    self.publish_locked(&mut state)?;
                    self.signal_changes.send_replace(());
                }
            }
        }
        Ok(())
    }
}

pub(super) fn resources(settings: &Settings) -> BTreeMap<String, Resources> {
    settings
        .servers
        .iter()
        .map(|(name, configuration)| (name.clone(), Resources::read(configuration)))
        .collect()
}

impl Worker {
    pub async fn run(mut self, lifetime: CancellationToken) -> Result<()> {
        let mut observer = ResourceWatch::new()?;
        observer.update(self.registry.resource_paths()?)?;
        self.registry.reload_resources().await?;
        let mut tasks = JoinSet::new();
        let registry = Arc::clone(&self.registry);
        tasks.spawn(async move {
            if let Err(error) = registry.refresh(None).await {
                tracing::error!(%error, "managed startup inventory refresh failed");
            }
        });
        loop {
            let deadline = self.registry.deadline()?;
            tokio::select! {
                biased;
                _ = lifetime.cancelled() => break,
                _ = wait_deadline(deadline) => self.registry.publish()?,
                _ = observer.changed.notified() => {
                    self.registry.reload_resources().await?;
                    if observer.update(self.registry.resource_paths()?)? { self.registry.reload_resources().await?; }
                },
                _ = self.registry.changed.notified() => {
                    if observer.update(self.registry.resource_paths()?)? { self.registry.reload_resources().await?; }
                },
                result = tasks.join_next(), if !tasks.is_empty() => {
                    result.context("managed worker task disappeared")?.context("managed worker task failed")?;
                }
                request = self.receiver.recv(), if tasks.len() < 16 => {
                    let Some(request) = request else { break; };
                    let registry = Arc::clone(&self.registry);
                    tasks.spawn(async move {
                        match request {
                            Request::Control { request, response } => {
                                let _ = response.send(registry.control_owned(request).await);
                            }
                            Request::Execute { operation, expected, response } => {
                                let _ = response.send(registry.perform(operation, expected).await);
                            }
                            Request::Prepare { operation, response } => {
                                let _ = response.send(registry.prepare_owned(operation).await);
                            }
                            Request::Selection { server, response } => {
                                let _ = response.send(registry.selection_owned(server).await);
                            }
                        }
                    });
                }
            }
        }
        self.receiver.close();
        while let Ok(request) = self.receiver.try_recv() {
            let error =
                Error::Conflict("Managed service is stopping; the request was not executed");
            match request {
                Request::Control { response, .. } => {
                    let _ = response.send(Err(
                        crate::devices::operations::ControlError::Unavailable(error.to_string()),
                    ));
                }
                Request::Execute { response, .. } => {
                    let _ = response.send(Err(error));
                }
                Request::Prepare { response, .. } => {
                    let _ = response.send(Err(error));
                }
                Request::Selection { response, .. } => {
                    let _ = response.send(Err(error));
                }
            }
        }
        while let Some(result) = tasks.join_next().await {
            result.context("managed worker task failed during shutdown")?;
        }
        Ok(())
    }
}

async fn wait_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

fn entries(
    settings: &Settings,
    mut resources: BTreeMap<String, Resources>,
) -> BTreeMap<String, Entry> {
    settings
        .servers
        .iter()
        .map(|(name, configuration)| {
            let resources = resources
                .remove(name)
                .expect("resources were loaded for each configured server");
            let mut source = Source {
                configuration: configuration.clone(),
                client: None,
                transport: None,
                setup_error: None,
                credential: None,
                resource_revision: resources.revision,
                resource_paths: resources.paths,
                refresh: Semaphore::new(1),
            };
            let mut inventory = InventoryState::default();
            if configuration.enabled() {
                let setup = (|| {
                    let credential = resources.credential?;
                    let certificates = resources.certificates?;
                    let transport = Transport::from_certificate_bytes(certificates.as_deref())?;
                    source.credential = Some(credential.clone());
                    source.client = Some(Client::new(
                        configuration
                            .url
                            .as_deref()
                            .ok_or(Error::Invalid("DDM URL is missing"))?,
                        credential,
                        transport.clone(),
                        Limits::default(),
                    )?);
                    source.transport = Some(transport);
                    Ok::<(), Error>(())
                })();
                if let Err(error) = setup {
                    inventory.attempting();
                    inventory.fail(&error);
                    source.setup_error = Some(error);
                }
            }
            (
                name.clone(),
                Entry {
                    source: Arc::new(source),
                    inventory,
                    signals: BTreeMap::new(),
                    controller_status: super::status::observations::Cache::default(),
                },
            )
        })
        .collect()
}

fn current_entry<'a>(
    state: &'a mut State,
    name: &str,
    source: &Arc<Source>,
) -> Result<&'a mut Entry, Error> {
    state
        .servers
        .get_mut(name)
        .filter(|entry| Arc::ptr_eq(&entry.source, source))
        .ok_or(Error::Conflict(
            "Managed server configuration changed during the request",
        ))
}
