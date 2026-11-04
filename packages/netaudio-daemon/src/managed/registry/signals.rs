use super::{Registry, Source};
use crate::managed::{
    Error, Limits,
    controller::{Controller, Lookup, StreamEvent},
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
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const HEALTHY_SESSION: Duration = Duration::from_secs(60);
const MAXIMUM_ATTEMPTS: u32 = 8;
const MAXIMUM_BACKOFF: Duration = Duration::from_secs(30);
const MAXIMUM_SESSIONS: usize = 8;
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
        let mut refreshes = JoinSet::new();
        let mut next_owner = 0u64;
        let result = async {
            loop {
                let desired = self.signal_plans()?;
                for (key, session) in &sessions {
                    if desired.get(key).is_none_or(|plan| !Arc::ptr_eq(&plan.source, &session.source) || !plan.supported) {
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
                for server in self.discovery_refreshes()? {
                    let registry = Arc::clone(&self);
                    refreshes.spawn(async move {
                        if let Err(error) = registry.refresh_queued(&server).await {
                            tracing::warn!(%server, %error, "managed inventory read for a newly advertised device failed");
                        }
                    });
                }
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
                    if !plan.supported {
                        self.signal_status(&key, &plan.source, json!({
                            "state": "unavailable", "error": "Managed domain identifier is not supported",
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
                    result = refreshes.join_next(), if !refreshes.is_empty() => {
                        result.context("managed discovery refresh task disappeared")?
                            .context("managed discovery refresh task failed")?;
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
        refreshes.shutdown().await;
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
            if !entry.source.enabled() || entry.source.client.is_none() {
                continue;
            }
            let membership = entry.inventory.signal_membership();
            let mut domains: BTreeMap<String, String> = entry
                .inventory
                .domain_ids()
                .map(|domain| (domain.to_ascii_lowercase(), domain.clone()))
                .collect();
            for context in state.snapshot.settings.contexts.values() {
                if &context.server == server {
                    domains
                        .entry(context.domain_id.to_ascii_lowercase())
                        .or_insert_with(|| context.domain_id.clone());
                }
            }
            for domain in domains.into_values() {
                let mut targets = BTreeMap::new();
                let mut unavailable = BTreeSet::new();
                let mut repeated = BTreeSet::new();
                for (inventory_id, identifier) in membership.domains.get(&domain).into_iter().flatten() {
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
                let supported =
                    normalize(DeviceIdentityRequest::ManagedDomain(json!(domain))).is_some();
                plans.insert(
                    (server.clone(), domain),
                    Plan {
                        source: Arc::clone(&entry.source),
                        route: Route {
                            revision: membership.revision,
                            targets,
                            unavailable: unavailable.into_iter().collect(),
                        },
                        supported,
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
            let domains = desired
                .keys()
                .filter(|(planned, _)| planned == server)
                .map(|(_, domain)| domain.clone())
                .collect();
            changed |= entry.discovery.retain(&domains);
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
        if entry.signals.get(&key.1) != Some(&status) {
            entry.signals.insert(key.1.clone(), status);
            self.publish_locked(&mut state)?;
        }
        Ok(())
    }

    fn discovery_refreshes(&self) -> Result<Vec<String>> {
        let mut state = self.lock()?;
        let mut servers = Vec::new();
        for (name, entry) in &mut state.servers {
            if !entry.inventory.attempted() || entry.source.refresh.available_permits() == 0 {
                continue;
            }
            let domains: Vec<_> = entry.discovery.domains().cloned().collect();
            let mut requested = false;
            for domain in domains {
                let known = entry.inventory.native_identities(&domain);
                requested |= entry.discovery.request_unknown(&domain, &known);
            }
            if requested {
                servers.push(name.clone());
            }
        }
        Ok(servers)
    }

    fn discovery<T>(
        &self,
        key: &Key,
        source: &Arc<Source>,
        change: impl FnOnce(&mut super::Entry) -> (T, bool),
    ) -> Result<Option<T>, Error> {
        let mut state = self.lock()?;
        let Some(entry) = state
            .servers
            .get_mut(&key.0)
            .filter(|entry| Arc::ptr_eq(&entry.source, source))
        else {
            return Ok(None);
        };
        let (result, changed) = change(entry);
        if changed {
            self.publish_locked(&mut state)
                .map_err(|_| Error::Conflict("Managed discovery could not be published"))?;
        }
        Ok(Some(result))
    }

    fn apply_lookup(
        &self,
        key: &Key,
        source: &Arc<Source>,
        generation: u64,
        lookup: &Lookup,
    ) -> Result<bool, Error> {
        let applied = self.discovery(key, source, |entry| {
            let known = entry.inventory.native_identities(&key.1);
            let applied =
                entry
                    .discovery
                    .apply(&key.1, generation, &lookup.event, lookup.received_at, &known);
            let changed = applied.changed;
            (applied, changed)
        })?;
        let Some(applied) = applied else {
            return Ok(false);
        };
        if applied.unknown {
            self.signal_changes.send_replace(());
        }
        Ok(applied.terminal)
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
            let mut failures = 0u32;
            loop {
                if failures > 0 {
                    let delay = Duration::from_secs(1 << (failures - 1).min(5)).min(MAXIMUM_BACKOFF);
                    tokio::time::sleep(delay).await;
                }
                let attempt = failures + 1;
                let Some(generation) =
                    self.discovery(key, source, |entry| (entry.discovery.connecting(&key.1), true))?
                else {
                    return Ok(());
                };
                self.signal_status(key, source, json!({"state": "connecting", "attempt": attempt,
                    "maximum_attempts": MAXIMUM_ATTEMPTS, "unavailable_devices": route.borrow().unavailable}))?;
                let mut ready = None;
                let result = self
                    .receive_signals(key, source, &route, metering, owner, attempt, generation, &mut ready)
                    .await;
                self.discovery(key, source, |entry| ((), entry.discovery.lost(&key.1, generation)))?;
                invalidate(metering, &route.borrow(), owner)?;
                failures = match ready {
                    Some(ready) if ready.elapsed() >= HEALTHY_SESSION => 1,
                    _ => failures + 1,
                };
                let permanent = matches!(
                    result,
                    Err(Error::Authentication | Error::Credential(_) | Error::Forbidden(_) | Error::Session(_) | Error::Tls(_))
                );
                let error = match result {
                    Ok(()) => "Managed signal session ended".to_owned(),
                    Err(error) => error.to_string(),
                };
                let exhausted = permanent || failures >= MAXIMUM_ATTEMPTS;
                tracing::warn!(server = %key.0, domain = %key.1, attempt, %error, "managed signal stream disconnected");
                self.signal_status(key, source, json!({
                    "state": if exhausted { "exhausted" } else { "reconnecting" },
                    "attempt": attempt, "maximum_attempts": MAXIMUM_ATTEMPTS, "error": error,
                    "recovery": "Refresh managed inventory to retry an exhausted session",
                    "unavailable_devices": route.borrow().unavailable,
                }))?;
                if exhausted {
                    return Ok(());
                }
            }
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(()),
            result = session => result,
        };
        invalidate(metering, &route.borrow(), owner)?;
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn receive_signals(
        &self,
        key: &Key,
        source: &Arc<Source>,
        route: &watch::Receiver<Route>,
        metering: &Metering,
        owner: u64,
        attempt: u32,
        generation: u64,
        ready: &mut Option<Instant>,
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
                StreamEvent::Lookup(lookup) => {
                    if self.apply_lookup(key, source, generation, &lookup)? {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                StreamEvent::LookupRejected(error) => {
                    tracing::warn!(server = %key.0, domain = %key.1, %error, "managed device lookup update rejected");
                    self.discovery(key, source, |entry| ((), { entry.discovery.reject(&key.1, generation); true }))?;
                }
                StreamEvent::Ready => {
                    *ready = Some(Instant::now());
                    self.discovery(key, source, |entry| ((), entry.discovery.observing(&key.1, generation)))?;
                    self.signal_status(key, source, json!({
                        "state": "ready", "attempt": attempt, "maximum_attempts": MAXIMUM_ATTEMPTS,
                        "unavailable_devices": route.borrow().unavailable,
                    })).map_err(|_| Error::Conflict("Managed signal status could not be published"))?
                }
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
