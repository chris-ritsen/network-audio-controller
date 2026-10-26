use super::{Registry, Source};
use crate::managed::{
    Error, Limits,
    controller::{Controller, StreamEvent},
};
use crate::monitoring::metering::Metering;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use netaudio_core::device_identity::{DeviceIdentityRequest, normalize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU16, NonZeroUsize};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

const MAXIMUM_SESSIONS: usize = 8;
const MAXIMUM_ATTEMPTS: u32 = 3;
type Key = (String, String);

#[derive(Clone, PartialEq)]
struct Target {
    name: String,
    inventory_id: String,
}

#[derive(Clone, PartialEq)]
struct Route {
    revision: u64,
    targets: BTreeMap<String, Target>,
    unavailable: Vec<String>,
}

struct Plan {
    source: Arc<Source>,
    route: Route,
    supported: bool,
}

struct Session {
    source: Arc<Source>,
    route: watch::Sender<Route>,
    cancellation: CancellationToken,
    owner: u64,
    running: bool,
    attempted_revision: u64,
}

impl Registry {
    pub async fn monitor_signals(
        self: Arc<Self>,
        metering: Arc<Metering>,
        lifetime: CancellationToken,
    ) -> Result<()> {
        let mut changes = self.signal_changes.subscribe();
        let mut observations = self.model.subscribe_device_observations()?;
        let mut sessions: BTreeMap<Key, Session> = BTreeMap::new();
        let mut tasks = JoinSet::new();
        let mut next_owner = 0u64;
        let result = async {
            loop {
                let desired = self.signal_plans()?;
                for (key, session) in &sessions {
                    if desired.get(key).is_none_or(|plan| !Arc::ptr_eq(&plan.source, &session.source) || !plan.supported || plan.route.targets.is_empty()) {
                        session.cancellation.cancel();
                    }
                }
                let removed: Vec<_> = sessions.iter()
                    .filter(|(key, session)| !session.running && (session.cancellation.is_cancelled() || desired.get(*key)
                        .is_none_or(|plan| !Arc::ptr_eq(&plan.source, &session.source)))
                    )
                    .map(|(key, _)| key.clone()).collect();
                for key in removed {
                    sessions.remove(&key);
                }
                self.retain_signal_statuses(&desired)?;
                for (key, plan) in desired {
                    if let Some(session) = sessions.get_mut(&key) {
                        if !Arc::ptr_eq(&session.source, &plan.source) || session.cancellation.is_cancelled() {
                            continue;
                        }
                        let previous = session.route.borrow().clone();
                        if previous != plan.route {
                            session.route.send_replace(plan.route.clone());
                        }
                        for target in previous.targets.values() {
                            if !plan.route.targets.values().any(|current| current == target) {
                                metering.invalidate_managed(&target.name, session.owner)?;
                            }
                        }
                        if session.running || session.attempted_revision == plan.route.revision {
                            continue;
                        }
                    }
                    if !plan.supported || plan.route.targets.is_empty() {
                        self.signal_status(&key, &plan.source, json!({
                            "state": "unavailable", "error": "Managed signal identities cannot be matched unambiguously",
                            "unavailable_devices": plan.route.unavailable,
                        }))?;
                        continue;
                    }
                    if tasks.len() >= MAXIMUM_SESSIONS {
                        self.signal_status(&key, &plan.source, json!({
                            "state": "waiting_capacity", "maximum_sessions": MAXIMUM_SESSIONS,
                            "unavailable_devices": plan.route.unavailable,
                        }))?;
                        continue;
                    }
                    next_owner = next_owner.checked_add(1).context("managed signal owner identity exhausted")?;
                    let owner = next_owner;
                    let (route, received) = watch::channel(plan.route.clone());
                    let cancellation = lifetime.child_token();
                    let registry = Arc::clone(&self);
                    let task_key = key.clone();
                    let source = Arc::clone(&plan.source);
                    let task_metering = Arc::clone(&metering);
                    let task_cancellation = cancellation.clone();
                    tasks.spawn(async move {
                        let result = registry.signal_session(&task_key, &source, received, &task_metering, owner, task_cancellation).await;
                        (task_key, owner, result)
                    });
                    sessions.insert(key, Session {
                        source: plan.source, route, cancellation, owner, running: true,
                        attempted_revision: plan.route.revision,
                    });
                }
                tokio::select! {
                    biased;
                    _ = lifetime.cancelled() => return Ok(()),
                    result = tasks.join_next(), if !tasks.is_empty() => {
                        let (key, owner, result) = result.context("managed signal task disappeared")?
                            .context("managed signal task failed")?;
                        result?;
                        if let Some(session) = sessions.get_mut(&key).filter(|session| session.owner == owner) {
                            session.running = false;
                            session.attempted_revision = session.route.borrow().revision;
                        }
                    }
                    changed = changes.changed() => { changed.context("managed signal configuration observer stopped")?; }
                    event = observations.next() => {
                        if event.is_none() {
                            if self.model.is_closed()? { return Ok(()); }
                            anyhow::ensure!(observations.overflowed(), "managed device observation stream stopped");
                            observations = self.model.subscribe_device_observations()?;
                        }
                    }
                }
            }
        }.await;
        for session in sessions.values() {
            session.cancellation.cancel();
        }
        while let Some(finished) = tasks.join_next().await {
            let (_, _, finished) =
                finished.context("managed signal task failed during shutdown")?;
            finished?;
        }
        for session in sessions.values() {
            invalidate(&metering, &session.route.borrow(), session.owner)?;
        }
        result
    }

    fn signal_plans(&self) -> Result<BTreeMap<Key, Plan>> {
        let state = self.lock()?;
        let records = self.model.records()?;
        let mut identities = BTreeMap::new();
        for (name, record) in &records {
            if let (Some(server), Some(domain), Some(identifier)) = (
                record["ddm_server_profile"].as_str(),
                record["ddm_domain_id"].as_str(),
                record["ddm_device_id"].as_str(),
            ) {
                identities
                    .entry((server, domain, identifier))
                    .and_modify(|name| *name = None)
                    .or_insert(Some(name));
            }
        }
        let mut plans = BTreeMap::new();
        for (server, entry) in &state.servers {
            if !entry.source.configuration.enabled() || entry.source.client.is_none() {
                continue;
            }
            let membership = entry.inventory.signal_membership();
            for (domain, devices) in &membership.domains {
                if devices.is_empty() {
                    continue;
                }
                let mut targets = BTreeMap::new();
                let mut unavailable = BTreeSet::new();
                let mut repeated = BTreeSet::new();
                for (inventory_id, identifier) in devices {
                    let Some(identifier) = identifier else {
                        unavailable.insert(inventory_id.clone());
                        continue;
                    };
                    let Some(Some(name)) =
                        identities.get(&(server.as_str(), domain.as_str(), inventory_id.as_str()))
                    else {
                        unavailable.insert(inventory_id.clone());
                        continue;
                    };
                    if let Some(previous) = targets.insert(
                        identifier.clone(),
                        Target {
                            name: (*name).clone(),
                            inventory_id: inventory_id.clone(),
                        },
                    ) {
                        repeated.insert(identifier.clone());
                        unavailable.insert(previous.inventory_id);
                        unavailable.insert(inventory_id.clone());
                    }
                }
                for identifier in repeated {
                    targets.remove(&identifier);
                }
                plans.insert(
                    (server.clone(), domain.clone()),
                    Plan {
                        source: Arc::clone(&entry.source),
                        route: Route {
                            revision: membership.revision,
                            targets,
                            unavailable: unavailable.into_iter().collect(),
                        },
                        supported: normalize(DeviceIdentityRequest::ManagedDomain(json!(domain)))
                            .is_some(),
                    },
                );
            }
        }
        Ok(plans)
    }

    fn retain_signal_statuses(&self, desired: &BTreeMap<Key, Plan>) -> Result<()> {
        let mut state = self.lock()?;
        let mut changed = false;
        for (server, entry) in &mut state.servers {
            let previous = entry.signals.len();
            entry
                .signals
                .retain(|domain, _| desired.contains_key(&(server.clone(), domain.clone())));
            changed |= previous != entry.signals.len();
            for (domain, status) in &mut entry.signals {
                if let Some(plan) = desired.get(&(server.clone(), domain.clone())) {
                    let unavailable = json!(plan.route.unavailable);
                    changed |= status["unavailable_devices"] != unavailable;
                    status["unavailable_devices"] = unavailable;
                }
            }
        }
        if changed {
            self.publish_locked(&mut state)?;
        }
        Ok(())
    }

    fn signal_status(&self, key: &Key, source: &Arc<Source>, status: Value) -> Result<()> {
        let mut state = self.lock()?;
        let Some(entry) = state
            .servers
            .get_mut(&key.0)
            .filter(|entry| Arc::ptr_eq(&entry.source, source))
        else {
            return Ok(());
        };
        if !entry
            .inventory
            .signal_membership()
            .domains
            .get(&key.1)
            .is_some_and(|targets| !targets.is_empty())
        {
            return Ok(());
        }
        if entry.signals.get(&key.1) != Some(&status) {
            entry.signals.insert(key.1.clone(), status);
            self.publish_locked(&mut state)?;
        }
        Ok(())
    }

    async fn signal_session(
        &self,
        key: &Key,
        source: &Arc<Source>,
        route: watch::Receiver<Route>,
        metering: &Metering,
        owner: u64,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let session = async {
            for attempt in 1..=MAXIMUM_ATTEMPTS {
                if attempt > 1 {
                    tokio::time::sleep(Duration::from_secs(u64::from(attempt - 1))).await;
                }
                self.signal_status(key, source, json!({"state": "connecting", "attempt": attempt,
                    "maximum_attempts": MAXIMUM_ATTEMPTS, "unavailable_devices": route.borrow().unavailable}))?;
                let result = self
                    .receive_signals(key, source, &route, metering, owner, attempt)
                    .await;
                invalidate(metering, &route.borrow(), owner)?;
                let error = match result {
                    Ok(()) => "Managed signal session ended".to_owned(),
                    Err(error) => error.to_string(),
                };
                tracing::warn!(server = %key.0, domain = %key.1, attempt, %error, "managed signal stream disconnected");
                self.signal_status(key, source, json!({
                    "state": if attempt == MAXIMUM_ATTEMPTS { "exhausted" } else { "reconnecting" },
                    "attempt": attempt, "maximum_attempts": MAXIMUM_ATTEMPTS, "error": error,
                    "recovery": "Refresh managed inventory to retry an exhausted session",
                    "unavailable_devices": route.borrow().unavailable,
                }))?;
            }
            Ok(())
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(()),
            result = session => result,
        };
        invalidate(metering, &route.borrow(), owner)?;
        result
    }

    async fn receive_signals(
        &self,
        key: &Key,
        source: &Arc<Source>,
        route: &watch::Receiver<Route>,
        metering: &Metering,
        owner: u64,
        attempt: u32,
    ) -> Result<(), Error> {
        let configuration = &source.configuration;
        let endpoint = super::super::client::endpoint(
            configuration
                .url
                .as_deref()
                .ok_or(Error::Invalid("DDM URL is missing"))?,
        )?;
        let host = endpoint
            .host_str()
            .ok_or(Error::Invalid("DDM hostname is missing"))?;
        let transport = source
            .transport
            .as_ref()
            .ok_or(Error::Transport)?
            .independent_capacity(NonZeroUsize::new(2).expect("positive capacity"));
        let controller = Controller::new(
            host,
            NonZeroU16::new(netaudio_core::controller_api::AUTH_PORT)
                .expect("nonzero Controller port"),
            transport,
            Limits::default(),
        )?;
        let endpoints = controller.endpoints().await?;
        let session = controller.connect(endpoints.service_port).await?;
        let credential = source
            .credential
            .clone()
            .ok_or(Error::Credential("DDM credential is unavailable"))?;
        session.monitor(&credential, key.1.clone(), |event| {
            match event {
                StreamEvent::Ready => self.signal_status(key, source, json!({
                    "state": "ready", "attempt": attempt, "maximum_attempts": MAXIMUM_ATTEMPTS,
                    "unavailable_devices": route.borrow().unavailable,
                })).map_err(|_| Error::Conflict("Managed signal status could not be published"))?,
                StreamEvent::Signal(signal) => {
                    let route = route.borrow();
                    let state = self.lock()?;
                    let current = state.servers.get(&key.0).is_some_and(|entry| {
                        Arc::ptr_eq(&entry.source, source) && entry.inventory.signal_membership().revision == route.revision
                    });
                    if !current { return Ok(ControlFlow::Continue(())); }
                    if let Some(target) = route.targets.get(&signal.publication.device_id) {
                        let record = self.model.device(&target.name)
                            .map_err(|_| Error::Conflict("Managed signal target could not be read"))?;
                        if record.is_some_and(|record| record["ddm_server_profile"] == key.0
                            && record["ddm_domain_id"] == key.1 && record["ddm_device_id"] == target.inventory_id) {
                            metering.observe_managed(&target.name, &signal, owner)
                                .map_err(|_| Error::Conflict("Managed signal observation could not be published"))?;
                        }
                    }
                }
            }
            Ok(ControlFlow::Continue(()))
        }).await
    }
}

fn invalidate(metering: &Metering, route: &Route, owner: u64) -> Result<()> {
    for target in route.targets.values() {
        metering.invalidate_managed(&target.name, owner)?;
    }
    Ok(())
}
