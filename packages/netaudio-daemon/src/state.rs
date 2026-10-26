use crate::events::{
    DEVICE_DISCOVERED, DEVICE_REMOVED, DEVICE_UPDATED, DeviceView, Event, EventOptions,
    EventReceiver, EventSender, MANAGED_STATUS, SNAPSHOT, TELEMETRY_UPDATED, event_queue,
};
use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures_core::Stream;
use netaudio_core::managed_api::inventory::{self as managed_inventory, Observation};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context as TaskContext, Poll};

pub const API_VERSION: u32 = 2;

struct Subscriber {
    sender: EventSender,
    view: Option<DeviceView>,
    devices_only: bool,
}

struct State {
    sequence: u64,
    next_subscriber: u64,
    devices: BTreeMap<String, Arc<Value>>,
    direct_devices: BTreeMap<String, Arc<Value>>,
    inventory_projection: managed_inventory::Projection,
    managed_observations: Vec<Observation>,
    managed_status: Vec<crate::managed::status::observations::Projection>,
    snapshot_fields: Map<String, Value>,
    devices_encoded: Option<Bytes>,
    external_flows_encoded: Option<Bytes>,
    external_flow_deadlines: BTreeMap<String, tokio::time::Instant>,
    snapshot: Option<Event>,
    subscribers: BTreeMap<u64, Subscriber>,
    closed: bool,
}

pub struct ReadModel {
    state: Arc<Mutex<State>>,
}

pub struct Subscription {
    identifier: u64,
    opened_at: chrono::DateTime<chrono::Utc>,
    receiver: EventReceiver,
    state: Weak<Mutex<State>>,
}

impl Subscription {
    pub fn opened_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.opened_at
    }

    pub fn overflowed(&self) -> bool {
        self.receiver.overflowed()
    }
}

impl Stream for Subscription {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Option<Event>> {
        Pin::new(&mut self.get_mut().receiver).poll_next(context)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            match state.lock() {
                Ok(mut state) => {
                    state.subscribers.remove(&self.identifier);
                }
                Err(error) => tracing::error!(%error, "could not release event subscription"),
            }
        }
    }
}

impl ReadModel {
    pub fn new() -> Result<Self> {
        let mut snapshot_fields = Map::new();
        snapshot_fields.insert("event".to_owned(), json!(SNAPSHOT));
        snapshot_fields.insert("api_version".to_owned(), json!(API_VERSION));
        snapshot_fields.insert(
            "public_api".to_owned(),
            crate::http::versioning::descriptor(),
        );
        snapshot_fields.insert(
            "source_generation".to_owned(),
            json!(rand::random::<u128>().to_string()),
        );
        snapshot_fields.insert("external_flows".to_owned(), json!({}));
        snapshot_fields.insert("shure_devices".to_owned(), json!({}));
        snapshot_fields.insert("metering".to_owned(), json!({}));
        snapshot_fields.insert("managed".to_owned(), Value::Null);
        snapshot_fields.insert("managed_domains".to_owned(), json!([]));
        snapshot_fields.insert(
            "metering_scale".to_owned(),
            serde_json::to_value(netaudio_core::metering::metering_scale())?,
        );

        Ok(Self {
            state: Arc::new(Mutex::new(State {
                sequence: 0,
                next_subscriber: 0,
                devices: BTreeMap::new(),
                direct_devices: BTreeMap::new(),
                inventory_projection: managed_inventory::Projection::default(),
                managed_observations: Vec::new(),
                managed_status: Vec::new(),
                snapshot_fields,
                devices_encoded: None,
                external_flows_encoded: None,
                external_flow_deadlines: BTreeMap::new(),
                snapshot: None,
                subscribers: BTreeMap::new(),
                closed: false,
            })),
        })
    }

    pub fn devices(&self) -> Result<Bytes> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;

        if let Some(encoded) = &state.devices_encoded {
            return Ok(encoded.clone());
        }

        let encoded = encode(&state.devices)?;
        state.devices_encoded = Some(encoded.clone());
        Ok(encoded)
    }

    pub fn records(&self) -> Result<BTreeMap<String, Arc<Value>>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        Ok(state.devices.clone())
    }

    pub fn external_flows(&self) -> Result<Bytes> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;

        if let Some(encoded) = &state.external_flows_encoded {
            return Ok(encoded.clone());
        }

        let encoded = encode(&state.snapshot_fields["external_flows"])?;
        state.external_flows_encoded = Some(encoded.clone());
        Ok(encoded)
    }

    pub fn external_flow_records(&self) -> Result<Value> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        Ok(state.snapshot_fields["external_flows"].clone())
    }

    pub fn external_flow(&self, change: crate::sap::inventory::Change) -> Result<()> {
        let mut flow = change.flow.unwrap_or(Value::Null);
        crate::representation::browser_values(&mut flow);
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        let flows = state.snapshot_fields["external_flows"]
            .as_object_mut()
            .context("external flow inventory is not an object")?;
        let key = change.identity.key();

        if flow.is_null() {
            flows.remove(&key);
            state.external_flow_deadlines.remove(&key);
        } else {
            let expires = change.expires_monotonic.context("SAP flow has no expiry")?;
            flows.insert(key.clone(), flow.clone());
            state.external_flow_deadlines.insert(key, expires);
        }

        state.external_flows_encoded = None;
        state.snapshot = None;
        state.publish(json!({
            "event": crate::events::EXTERNAL_FLOW_CHANGED,
            "change": change.kind,
            "identity": change.identity,
            "flow": flow,
        }))
    }

    pub fn fresh_external_flow(
        &self,
        identity: &crate::sap::inventory::Identity,
    ) -> Result<Option<Value>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        let key = identity.key();

        if state
            .external_flow_deadlines
            .get(&key)
            .is_none_or(|deadline| *deadline <= tokio::time::Instant::now())
        {
            return Ok(None);
        }

        Ok(state.snapshot_fields["external_flows"].get(&key).cloned())
    }

    pub fn subscriber_count(&self) -> Result<usize> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        Ok(state.subscribers.len())
    }

    pub fn device(&self, name: &str) -> Result<Option<Arc<Value>>> {
        self.device_in_context(name, None)
    }

    pub fn devices_in_context(&self, context: Option<&str>) -> Result<Bytes> {
        let Some(context) = context else {
            return self.devices();
        };
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        let records: BTreeMap<_, _> = state
            .devices
            .iter()
            .filter(|(_, record)| in_context(record, context))
            .collect();
        encode(&records)
    }

    pub fn device_in_context(
        &self,
        name: &str,
        context: Option<&str>,
    ) -> Result<Option<Arc<Value>>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;

        Ok(resolve_record(&state.devices, name, context)?.cloned())
    }

    pub fn replace_device(&self, name: String, mut record: Value) -> Result<bool> {
        if !record.is_object() {
            bail!("device record must be an object");
        }

        crate::representation::browser_values(&mut record);
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        let previous = state.direct_devices.get(&name);

        if previous.is_some_and(|previous| previous.as_ref() == &record) {
            return Ok(false);
        }

        state.direct_devices.insert(name, Arc::new(record));
        state.reconcile_devices()
    }

    pub fn remove_device(&self, name: &str) -> Result<u64> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;

        state.direct_devices.remove(name);
        state.reconcile_devices()?;
        Ok(state.sequence)
    }

    pub fn update_telemetry(&self, name: &str, fields: Map<String, Value>) -> Result<bool> {
        let mut telemetry = Value::Object(fields);
        crate::representation::browser_values(&mut telemetry);
        let fields = telemetry
            .as_object()
            .context("telemetry is not an object")?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        state.inventory_projection.invalidate(name);
        if let Some(direct) = state.direct_devices.get_mut(name) {
            let direct = Arc::make_mut(direct)
                .as_object_mut()
                .context("direct device record is not an object")?;
            for (field, value) in fields {
                if value.is_null() {
                    direct.remove(field);
                } else {
                    direct.insert(field.clone(), value.clone());
                }
            }
        }
        let Some(record) = state.devices.get_mut(name) else {
            bail!("telemetry device is absent from inventory");
        };
        let record = Arc::make_mut(record)
            .as_object_mut()
            .context("device record is not an object")?;
        let mut changed = Map::new();

        for (field, value) in fields {
            if record.get(field) == Some(value) || value.is_null() && !record.contains_key(field) {
                continue;
            }

            if value.is_null() {
                record.remove(field);
            } else {
                record.insert(field.clone(), value.clone());
            }

            changed.insert(field.clone(), value.clone());
        }

        if changed.is_empty() {
            return Ok(false);
        }

        state.devices_encoded = None;
        state.snapshot = None;
        state.publish(json!({
            "event": TELEMETRY_UPDATED,
            "server_name": name,
            "fields": changed.keys().collect::<Vec<_>>(),
            "telemetry": changed,
        }))?;
        Ok(true)
    }

    pub fn metering(&self, name: &str, sample: Option<Arc<Value>>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        let metering = state
            .snapshot_fields
            .get_mut("metering")
            .and_then(Value::as_object_mut)
            .context("metering snapshot is not an object")?;
        let mut event = match sample {
            Some(sample) => {
                metering.insert(name.to_owned(), sample.as_ref().clone());
                sample.as_ref().clone()
            }
            None => {
                if metering.remove(name).is_none() {
                    return Ok(());
                }

                json!({"tx": {}, "rx": {}, "metering_source": null, "expired": true})
            }
        };
        state.inventory_projection.invalidate(name);
        if let Some(timestamp) = event.get("wall_time")
            && let Some(record) = state.devices.get_mut(name)
        {
            Arc::make_mut(record)["last_seen"] = timestamp.clone();
            state.devices_encoded = None;
        }
        if let Some(timestamp) = event.get("wall_time")
            && let Some(record) = state.direct_devices.get_mut(name)
        {
            Arc::make_mut(record)["last_seen"] = timestamp.clone();
        }

        event["event"] = json!(crate::events::METER_VALUES);
        event["server_name"] = json!(name);
        state.snapshot = None;
        state.publish(event)
    }

    pub fn managed(&self, mut managed: Value) -> Result<bool> {
        crate::representation::browser_values(&mut managed);
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;

        if state.snapshot_fields.get("managed") == Some(&managed) {
            return Ok(false);
        }

        state
            .snapshot_fields
            .insert("managed".to_owned(), managed.clone());
        state.snapshot = None;
        state.publish(json!({"event": MANAGED_STATUS, "managed": managed}))?;
        Ok(true)
    }

    pub(crate) fn managed_inventory(
        &self,
        mut managed: Value,
        domains: Vec<Value>,
        observations: Vec<Observation>,
        controller_status: Vec<crate::managed::status::observations::Projection>,
    ) -> Result<()> {
        crate::representation::browser_values(&mut managed);
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        let domains = json!(domains);
        let changed = state.snapshot_fields.get("managed") != Some(&managed)
            || state.snapshot_fields.get("managed_domains") != Some(&domains);
        state
            .snapshot_fields
            .insert("managed_domains".into(), domains.clone());
        state
            .snapshot_fields
            .insert("managed".into(), managed.clone());
        if state.managed_observations != observations || state.managed_status != controller_status {
            state.managed_observations = observations;
            state.managed_status = controller_status;
            state.reconcile_devices()?;
        }
        if changed {
            state.snapshot = None;
            state.publish(
                json!({"event": MANAGED_STATUS, "managed": managed, "managed_domains": domains}),
            )?;
        }
        Ok(())
    }

    pub fn shure_device(&self, mac: &str, mut device: Value, event: Option<&str>) -> Result<()> {
        crate::representation::browser_values(&mut device);
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        state.snapshot_fields["shure_devices"]
            .as_object_mut()
            .context("Wireless inventory is not an object")?
            .insert(mac.to_owned(), device.clone());
        state.snapshot = None;
        if let Some(event) = event {
            state.publish(json!({"event": event, "mac": mac, "device": device}))?;
        }
        Ok(())
    }

    pub fn observe(&self, snapshot_field: &str, mut value: Value, event: Value) -> Result<()> {
        if matches!(
            snapshot_field,
            "event"
                | "api_version"
                | "public_api"
                | "source_generation"
                | "devices"
                | "metering_scale"
                | "external_flows"
                | "shure_devices"
        ) {
            bail!("snapshot field {snapshot_field} has a dedicated owner");
        }

        crate::representation::browser_values(&mut value);
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        state
            .snapshot_fields
            .insert(snapshot_field.to_owned(), value);
        state.snapshot = None;
        state.publish(event)
    }

    pub fn publish(&self, event: Value) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        state.publish(event)
    }

    pub fn subscribe(&self, options: Option<EventOptions>) -> Result<Subscription> {
        self.subscribe_selected(options, false)
    }

    pub fn subscribe_device_observations(&self) -> Result<Subscription> {
        self.subscribe_selected(None, true)
    }

    fn subscribe_selected(
        &self,
        options: Option<EventOptions>,
        devices_only: bool,
    ) -> Result<Subscription> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;

        if state.closed {
            bail!("device event service is stopping");
        }

        state
            .subscribers
            .retain(|_, subscriber| subscriber.sender.is_connected());
        let snapshot = state.snapshot()?;
        let (sender, receiver) = event_queue();
        let mut view = options.map(DeviceView::new);
        let initial = match view.as_mut() {
            Some(view) => Event::new(snapshot.sequence, view.initial_snapshot(&snapshot.value))?,
            None => snapshot,
        };

        if !sender.send(initial) {
            bail!("initial event snapshot could not be queued");
        }

        let identifier = state.next_subscriber;
        state.next_subscriber = identifier
            .checked_add(1)
            .context("subscriber identity exhausted")?;
        state.subscribers.insert(
            identifier,
            Subscriber {
                sender,
                view,
                devices_only,
            },
        );
        Ok(Subscription {
            identifier,
            opened_at: chrono::Utc::now(),
            receiver,
            state: Arc::downgrade(&self.state),
        })
    }

    pub fn snapshot(&self) -> Result<Event> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        state.snapshot()
    }

    pub fn close(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;
        state.closed = true;
        state.subscribers.clear();
        Ok(())
    }

    pub fn is_closed(&self) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?
            .closed)
    }
}

impl State {
    fn reconcile_devices(&mut self) -> Result<bool> {
        let mut next = self
            .inventory_projection
            .merge(&self.direct_devices, &self.managed_observations)
            .map_err(anyhow::Error::msg)?;
        for record in next.values_mut() {
            for status in &self.managed_status {
                status.apply(record)?;
            }
        }
        let removed: Vec<_> = self
            .devices
            .keys()
            .filter(|name| !next.contains_key(*name))
            .cloned()
            .collect();
        let mut events = Vec::new();
        for name in removed {
            self.devices.remove(&name);
            events.push(json!({"event": DEVICE_REMOVED, "server_name": name}));
        }
        for (name, record) in next {
            let previous = self.devices.get(&name);
            if previous.is_some_and(|previous| {
                Arc::ptr_eq(previous, &record) || previous.as_ref() == record.as_ref()
            }) {
                continue;
            }
            let kind = if previous.is_some() {
                DEVICE_UPDATED
            } else {
                DEVICE_DISCOVERED
            };
            events.push(json!({"event": kind, "server_name": name, "device": record}));
            self.devices.insert(name, record);
        }
        if events.is_empty() {
            return Ok(false);
        }
        self.devices_encoded = None;
        self.snapshot = None;
        for event in events {
            self.publish(event)?;
        }
        Ok(true)
    }

    fn snapshot(&mut self) -> Result<Event> {
        if let Some(snapshot) = &self.snapshot {
            return Ok(snapshot.clone());
        }

        let mut snapshot = self.snapshot_fields.clone();
        snapshot.insert("devices".to_owned(), serde_json::to_value(&self.devices)?);
        let event = Event::new(self.sequence, Value::Object(snapshot))?;
        self.snapshot = Some(event.clone());
        Ok(event)
    }

    fn publish(&mut self, value: Value) -> Result<()> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .context("event sequence exhausted")?;
        let event = Event::new(self.sequence, value)?;
        self.subscribers.retain(|_, subscriber| {
            if subscriber.devices_only && !matches!(
                event.value["event"].as_str(),
                Some(DEVICE_DISCOVERED | DEVICE_UPDATED | DEVICE_REMOVED | TELEMETRY_UPDATED)
            ) {
                return subscriber.sender.is_connected();
            }

            match subscriber.view.as_mut() {
            Some(view) => {
                for value in view.events_for(&event.value) {
                    match Event::new(event.sequence, value) {
                        Ok(projected) => {
                            if !subscriber.sender.send(projected) {
                                return false;
                            }
                        }
                        Err(error) => {
                            tracing::error!(%error, "could not serialize projected device event");
                            return false;
                        }
                    }
                }

                subscriber.sender.is_connected()
            }
            None => subscriber.sender.send(event.clone()),
            }
        });
        Ok(())
    }
}

#[derive(Debug)]
pub struct AmbiguousDevice;

impl std::fmt::Display for AmbiguousDevice {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "multiple devices matched; use the context-qualified inventory ID or server name",
        )
    }
}

impl std::error::Error for AmbiguousDevice {}

pub(crate) fn resolve_record<'a>(
    records: &'a BTreeMap<String, Arc<Value>>,
    selector: &str,
    context: Option<&str>,
) -> Result<Option<&'a Arc<Value>>> {
    if let Some(record) = records.get(selector)
        && context.is_none_or(|context| in_context(record, context))
    {
        return Ok(Some(record));
    }

    let mut matches = records.iter().filter(|(key, record)| {
        context.is_none_or(|context| in_context(record, context))
            && (caseless::default_caseless_match_str(key, selector)
                || [
                    "name",
                    "server_name",
                    "ddm_device_id",
                    "inventory_id",
                    "ipv4",
                ]
                .iter()
                .filter_map(|field| record[*field].as_str())
                .any(|value| caseless::default_caseless_match_str(value, selector)))
    });
    let first = matches.next().map(|(_, record)| record);

    if matches.next().is_some() {
        return Err(AmbiguousDevice.into());
    }

    Ok(first)
}

fn in_context(record: &Value, context: &str) -> bool {
    record
        .get("ddm_device_id")
        .is_none_or(|identifier| identifier.is_null() || identifier.as_str() == Some(""))
        || record.get("ddm_context").and_then(Value::as_str) == Some(context)
}

fn encode(value: &impl Serialize) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(value)?))
}
