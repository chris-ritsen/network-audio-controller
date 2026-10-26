use super::model::{EventFilter, JournalEvent, OperationRequest, Thresholds, device_identity};
use super::observe::Observer;
use super::remembered;
use super::store::{EventPage, JournalDatabase, Overview};
use crate::events::{
    DEVICE_DISCOVERED, DEVICE_REMOVED, DEVICE_UPDATED, Event, SNAPSHOT, TELEMETRY_UPDATED,
};
use crate::monitoring::issues;
use crate::state::{ReadModel, Subscription};
use anyhow::{Context, Result, bail};
use chrono::{SecondsFormat, Utc};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

const REQUEST_CAPACITY: usize = 128;
const MONITORING_EVENT: &str = "monitoring_event";

enum Request {
    Overview(oneshot::Sender<Result<Arc<Overview>>>),
    Page(EventFilter, oneshot::Sender<Result<EventPage>>),
    Issues(
        issues::store::Filter,
        oneshot::Sender<Result<issues::store::Page>>,
    ),
    Remembered(oneshot::Sender<remembered::Devices>),
    Wireless(oneshot::Sender<Result<Value>>),
    RememberWireless(Value, oneshot::Sender<Result<()>>),
    Forget(Forget),
    Operation(Box<OperationRequest>, oneshot::Sender<Result<Value>>),
    HostAudio(Box<JournalEvent>, oneshot::Sender<Result<()>>),
    Clear(oneshot::Sender<Result<Value>>),
    Source(Event, String),
    Gap(String, String),
    Stop(oneshot::Sender<()>),
}

pub struct Journal {
    requests: mpsc::Sender<Request>,
}

struct Forget {
    identity: String,
    through_sequence: u64,
    reply: oneshot::Sender<Result<()>>,
}

pub struct Worker {
    requests: mpsc::Receiver<Request>,
    database: JournalDatabase,
    observer: Observer,
    issues: issues::Engine,
    remembered: remembered::Inventory,
    model: Arc<ReadModel>,
    source_sequence: Option<u64>,
    device_sequences: BTreeMap<String, u64>,
    pending_forgets: Vec<Forget>,
}

impl Journal {
    pub async fn overview(&self) -> Result<Arc<Overview>> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Overview(reply))
            .await
            .context("event journal stopped")?;
        result
            .await
            .context("journal overview request ended without a result")?
    }

    pub async fn host_audio(&self, event: JournalEvent) -> Result<()> {
        anyhow::ensure!(
            event.kind == super::model::EventKind::HostAudioChanged,
            "Host audio journal entry has an invalid event kind"
        );
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::HostAudio(Box::new(event), reply))
            .await
            .context("Event journal stopped")?;
        result
            .await
            .context("Host audio journal request ended without a result")?
    }

    pub async fn wireless_devices(&self) -> Result<Value> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Wireless(reply))
            .await
            .context("Event journal stopped")?;
        result
            .await
            .context("Wireless inventory request ended without a result")?
    }

    pub async fn remember_wireless(&self, records: Value) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::RememberWireless(records, reply))
            .await
            .context("Event journal stopped")?;
        result
            .await
            .context("Wireless inventory save ended without a result")?
    }

    pub async fn open(
        path: &Path,
        model: Arc<ReadModel>,
        view_limit: usize,
        thresholds: Thresholds,
    ) -> Result<(Arc<Self>, Worker)> {
        let path = path.to_owned();
        let (database, restored, remembered) = tokio::task::spawn_blocking(move || {
            let mut database = JournalDatabase::open(&path, view_limit)?;
            let restored = database.restore_issues()?;
            let remembered = remembered::Inventory::load(&database)?;
            Ok::<_, anyhow::Error>((database, restored, remembered))
        })
        .await
        .context("event journal initialization worker failed")??;
        let (requests, receiver) = mpsc::channel(REQUEST_CAPACITY);
        Ok((
            Arc::new(Self { requests }),
            Worker {
                requests: receiver,
                database,
                observer: Observer::new(thresholds),
                issues: issues::Engine::new(thresholds, restored),
                remembered,
                model,
                source_sequence: None,
                device_sequences: BTreeMap::new(),
                pending_forgets: Vec::new(),
            },
        ))
    }

    pub async fn page(&self, filter: EventFilter) -> Result<EventPage> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Page(filter, reply))
            .await
            .context("event journal stopped")?;
        result
            .await
            .context("event journal request ended without a result")?
    }

    pub async fn operation(&self, request: OperationRequest) -> Result<Value> {
        request.event(&now())?;
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Operation(Box::new(request), reply))
            .await
            .context("event journal stopped")?;
        result
            .await
            .context("event journal request ended without a result")?
    }

    pub async fn issues(&self, filter: issues::store::Filter) -> Result<issues::store::Page> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Issues(filter, reply))
            .await
            .context("event journal stopped")?;
        result
            .await
            .context("issue request ended without a result")?
    }

    pub async fn remembered(&self) -> Result<remembered::Devices> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Remembered(reply))
            .await
            .context("event journal stopped")?;
        result
            .await
            .context("remembered inventory request ended without a result")
    }

    pub async fn forget(&self, identity: String, through_sequence: u64) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Forget(Forget {
                identity,
                through_sequence,
                reply,
            }))
            .await
            .context("event journal stopped")?;
        result
            .await
            .context("device cache removal ended without a result")?
    }

    pub async fn clear(&self) -> Result<Value> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Clear(reply))
            .await
            .context("event journal stopped")?;
        result
            .await
            .context("event journal request ended without a result")?
    }

    pub async fn stop(&self) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Stop(reply))
            .await
            .context("event journal stopped before shutdown")?;
        result.await.context("event journal shutdown failed")
    }

    pub async fn observe(
        self: Arc<Self>,
        model: Arc<ReadModel>,
        mut subscription: Subscription,
    ) -> Result<()> {
        let mut last_observed = subscription
            .opened_at()
            .to_rfc3339_opts(SecondsFormat::Micros, true);

        loop {
            match subscription.next().await {
                Some(event) => {
                    let timestamp = now();
                    self.requests
                        .send(Request::Source(event, timestamp.clone()))
                        .await
                        .context("event journal stopped")?;
                    last_observed = timestamp;
                }
                None => {
                    let stopped = model.is_closed()?;

                    if stopped && !subscription.overflowed() {
                        return Ok(());
                    }

                    let timestamp = now();
                    self.requests
                        .send(Request::Gap(last_observed, timestamp.clone()))
                        .await
                        .context("event journal stopped")?;

                    if stopped {
                        return Ok(());
                    }

                    subscription = model.subscribe_device_observations()?;
                    last_observed = timestamp;
                    tracing::warn!(
                        "event journal recorded an observation gap and acquired a new snapshot"
                    );
                }
            }
        }
    }
}

impl Worker {
    pub fn run(mut self) -> Result<()> {
        let mut stopping = Vec::new();

        while let Some(request) = self.requests.blocking_recv() {
            match request {
                Request::Page(filter, reply) => {
                    let result = self.database.page(&filter);
                    let failed = result.as_ref().err().map(ToString::to_string);
                    let _ = reply.send(result);

                    if let Some(error) = failed {
                        bail!("event journal read failed: {error}");
                    }
                }
                Request::Overview(reply) => {
                    let result = self.database.overview();
                    let failed = result.as_ref().err().map(ToString::to_string);
                    let _ = reply.send(result);

                    if let Some(error) = failed {
                        bail!("journal overview read failed: {error}");
                    }
                }
                Request::Operation(request, reply) => {
                    let result = self.operation(*request);
                    let failed = result.as_ref().err().map(ToString::to_string);
                    let _ = reply.send(result);

                    if let Some(error) = failed {
                        bail!("event journal append failed: {error}");
                    }
                }
                Request::HostAudio(event, reply) => {
                    let result = self.persist_changes(vec![*event], &[]).map(|_| ());
                    let failed = result.as_ref().err().map(ToString::to_string);
                    let _ = reply.send(result);
                    if let Some(error) = failed {
                        bail!("Host audio journal save failed: {error}");
                    }
                }
                Request::Issues(filter, reply) => {
                    let result = self.database.issues(&filter);
                    let failed = result.as_ref().err().map(ToString::to_string);
                    let _ = reply.send(result);
                    if let Some(error) = failed {
                        bail!("issue history read failed: {error}");
                    }
                }
                Request::Remembered(reply) => {
                    let _ = reply.send(self.remembered.snapshot());
                }
                Request::Wireless(reply) => {
                    let _ = reply.send(self.database.wireless_devices());
                }
                Request::RememberWireless(records, reply) => {
                    let result = self.database.remember_wireless(&records);
                    let failure = result.as_ref().err().map(ToString::to_string);
                    let _ = reply.send(result);
                    if let Some(error) = failure {
                        bail!("Wireless inventory save failed: {error}");
                    }
                }
                Request::Forget(request) => {
                    self.pending_forgets.push(request);
                    self.finish_forgets()?;
                }
                Request::Clear(reply) => {
                    let result = self.database.clear();
                    let failed = result.as_ref().err().map(ToString::to_string);
                    let _ = reply.send(result);

                    if let Some(error) = failed {
                        bail!("event journal clear failed: {error}");
                    }
                }
                Request::Source(event, timestamp) => {
                    self.source(event.clone(), &timestamp)?;
                    self.source_sequence = Some(event.sequence);
                    self.finish_forgets()?;
                }
                Request::Gap(stopped, started) => {
                    self.database.record_gap(&stopped, &started)?;
                    self.observer.reset();
                    let changes = self.issues.reset(&started)?;
                    self.persist_changes(Vec::new(), &changes)?;
                }
                Request::Stop(reply) => {
                    self.requests.close();
                    stopping.push(reply);
                }
            }
        }

        self.remembered.flush(&mut self.database)?;
        self.database.stop()?;

        for reply in stopping {
            let _ = reply.send(());
        }

        Ok(())
    }

    fn finish_forgets(&mut self) -> Result<()> {
        let pending = std::mem::take(&mut self.pending_forgets);

        for request in pending {
            if self
                .source_sequence
                .is_none_or(|sequence| sequence < request.through_sequence)
            {
                self.pending_forgets.push(request);
                continue;
            }

            let result = if self
                .device_sequences
                .get(&request.identity)
                .is_some_and(|sequence| *sequence > request.through_sequence)
            {
                Ok(())
            } else {
                self.remembered
                    .forget(&mut self.database, &request.identity)
            };
            let failed = result.as_ref().err().map(ToString::to_string);

            if result.is_ok()
                && self
                    .device_sequences
                    .get(&request.identity)
                    .is_none_or(|sequence| *sequence <= request.through_sequence)
            {
                self.observer.forget(&request.identity);
                self.device_sequences.remove(&request.identity);
            }

            let _ = request.reply.send(result);

            if let Some(error) = failed {
                bail!("device cache removal failed: {error}");
            }
        }

        Ok(())
    }

    fn operation(&mut self, request: OperationRequest) -> Result<Value> {
        let timestamp = now();
        let operation = request.event(&timestamp)?;
        let mut events = vec![operation];
        let mut changes = Vec::new();

        if request.observe_device {
            let record = Arc::new(request.device_snapshot.clone());
            events.extend(self.observer.observe(Arc::clone(&record), &timestamp)?);
            changes = self.issues.observe(Arc::clone(&record), &timestamp)?;
            let identity = device_identity(&record)?;
            self.remembered.observe(&identity, record, &timestamp);
        }

        let mut events = self.persist_changes(events, &changes)?;
        if request.observe_device {
            self.remembered.save(
                &mut self.database,
                &device_identity(&request.device_snapshot)?,
            )?;
        }
        let event = events.remove(0);
        Ok(json!({"event": event, "observed_events": events}))
    }

    fn source(&mut self, event: Event, timestamp: &str) -> Result<()> {
        let payload = &event.value;
        let records = match payload["event"].as_str() {
            Some(SNAPSHOT) => payload["devices"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(_, record)| Arc::new(record.clone()))
                .collect(),
            Some(DEVICE_DISCOVERED | DEVICE_UPDATED) => vec![Arc::new(payload["device"].clone())],
            Some(TELEMETRY_UPDATED) => {
                let name = payload["server_name"]
                    .as_str()
                    .context("telemetry event has no device identity")?;
                let mut record = self
                    .observer
                    .snapshot(name)
                    .context("telemetry arrived before its device snapshot")?
                    .as_ref()
                    .clone();
                let fields = record
                    .as_object_mut()
                    .context("device snapshot must be an object")?;

                for (field, value) in payload["telemetry"]
                    .as_object()
                    .context("telemetry must be an object")?
                {
                    if value.is_null() {
                        fields.remove(field);
                    } else {
                        fields.insert(field.clone(), value.clone());
                    }
                }

                vec![Arc::new(record)]
            }
            Some(DEVICE_REMOVED) => {
                let name = payload["server_name"]
                    .as_str()
                    .context("removed event has no device identity")?;
                self.device_sequences
                    .insert(name.to_owned(), event.sequence);
                let events = self.observer.remove(name, timestamp)?;
                let changes = self.issues.remove(name, timestamp)?;
                self.persist_changes(events, &changes)?;

                if let Some(record) = self.observer.snapshot(name) {
                    self.remembered.observe(name, record, timestamp);
                    self.remembered.save(&mut self.database, name)?;
                }

                Vec::new()
            }
            _ => Vec::new(),
        };

        for record in records {
            let identity = device_identity(&record)?;
            self.device_sequences
                .insert(identity.clone(), event.sequence);
            let first = self.observer.snapshot(&identity).is_none();
            let events = self.observer.observe(Arc::clone(&record), timestamp)?;
            let changed = !events.is_empty();
            let changes = self.issues.observe(Arc::clone(&record), timestamp)?;
            self.persist_changes(events, &changes)?;

            self.remembered.observe(&identity, record, timestamp);
            if first || changed || payload["event"] != TELEMETRY_UPDATED {
                self.remembered.save(&mut self.database, &identity)?;
            }
        }

        Ok(())
    }

    fn persist_changes(
        &mut self,
        mut events: Vec<JournalEvent>,
        changes: &[issues::Change],
    ) -> Result<Vec<JournalEvent>> {
        events.extend(changes.iter().filter_map(|change| change.event.clone()));
        let events = self.database.append_observation(events, changes)?;

        for event in &events {
            self.model
                .publish(json!({"event": MONITORING_EVENT, "journal_event": event}))?;
        }

        Ok(events)
    }
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}
