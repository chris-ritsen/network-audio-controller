use super::{
    DEVICE_DISCOVERED, DEVICE_PATCH, DEVICE_REMOVED, DEVICE_UPDATED, MANAGED_STATUS, METER_VALUES,
    SHURE_METER_VALUES, SNAPSHOT, TELEMETRY_UPDATED,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const TELEMETRY_FIELDS: [&str; 5] = [
    "clock_frequency_offset_parts_per_billion",
    "last_seen",
    "network_interface_traffic",
    "receiver_flow_connection_health",
    "clock_observations",
];
const NOTIFIED_TELEMETRY_FIELD: &str = "receiver_flow_connection_health";
const VOLATILE_RECORD_FIELDS: [&str; 2] = ["ddm_last_sync", "clock_observed_at"];
const CLOCK_TELEMETRY_FIELDS: [&str; 2] =
    ["clock_frequency_offset_parts_per_billion", "raw_record"];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventOptions {
    pub patches: bool,
    pub meters: bool,
    pub notices: bool,
    pub telemetry: bool,
    pub hidden_fields: BTreeSet<String>,
}

impl EventOptions {
    pub fn from_query(query: &str) -> Option<Self> {
        let parameters: BTreeMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let patches = parameters.get("patches").is_some_and(|value| value == "1");
        let meters = parameters.get("meters").is_none_or(|value| value != "0");
        let requested = parameters.get("telemetry").map_or("", String::as_str);

        if !patches && meters && matches!(requested, "" | "1") {
            return None;
        }

        let visible: BTreeSet<&str> = match requested {
            "" | "1" => TELEMETRY_FIELDS.into_iter().collect(),
            "0" | "none" => BTreeSet::new(),
            _ => requested.split(',').map(str::trim).collect(),
        };

        Some(Self {
            patches,
            meters,
            notices: requested != "none",
            telemetry: !matches!(requested, "0" | "none"),
            hidden_fields: TELEMETRY_FIELDS
                .into_iter()
                .filter(|field| !visible.contains(field))
                .map(str::to_owned)
                .collect(),
        })
    }
}

pub struct DeviceView {
    options: EventOptions,
    devices: BTreeMap<String, Map<String, Value>>,
    notified: BTreeMap<String, Map<String, Value>>,
    managed: Value,
}

impl DeviceView {
    pub fn new(options: EventOptions) -> Self {
        Self {
            options,
            devices: BTreeMap::new(),
            notified: BTreeMap::new(),
            managed: Value::Null,
        }
    }

    pub fn initial_snapshot(&mut self, snapshot: &Value) -> Value {
        self.devices.clear();
        self.notified.clear();

        if let Some(records) = snapshot.get("devices").and_then(Value::as_object) {
            for (name, record) in records {
                if let Some(record) = record.as_object() {
                    self.devices.insert(name.clone(), self.visible(record));
                    self.notified
                        .insert(name.clone(), self.notified_fields(record));
                }
            }
        }

        self.managed = snapshot.get("managed").cloned().unwrap_or_default();
        let mut result = snapshot.clone();
        result["devices"] = json!(self.devices);
        result
    }

    pub fn events_for(&mut self, data: &Value) -> Vec<Value> {
        match data
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            METER_VALUES | SHURE_METER_VALUES => {
                if self.options.meters {
                    vec![data.clone()]
                } else {
                    Vec::new()
                }
            }
            TELEMETRY_UPDATED => self.telemetry_events(data),
            DEVICE_DISCOVERED | DEVICE_UPDATED => {
                let Some(record) = data.get("device").and_then(Value::as_object) else {
                    return vec![data.clone()];
                };

                let Some(name) = data.get("server_name").and_then(Value::as_str) else {
                    return vec![data.clone()];
                };

                self.device_events(name, record, data["event"].as_str().unwrap_or_default())
            }
            DEVICE_REMOVED => {
                if let Some(name) = data.get("server_name").and_then(Value::as_str) {
                    self.devices.remove(name);
                    self.notified.remove(name);
                }

                vec![data.clone()]
            }
            SNAPSHOT => self.snapshot_events(data),
            MANAGED_STATUS => {
                let managed = data.get("managed").cloned().unwrap_or_default();

                if managed == self.managed {
                    return Vec::new();
                }

                self.managed = managed;
                vec![data.clone()]
            }
            _ => vec![data.clone()],
        }
    }

    fn visible(&self, record: &Map<String, Value>) -> Map<String, Value> {
        record
            .iter()
            .filter(|(key, _)| !self.options.hidden_fields.contains(*key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    fn notified_fields(&self, record: &Map<String, Value>) -> Map<String, Value> {
        let mut fields = Map::new();

        if self
            .options
            .hidden_fields
            .contains(NOTIFIED_TELEMETRY_FIELD)
            && let Some(value) = record.get(NOTIFIED_TELEMETRY_FIELD)
        {
            fields.insert(NOTIFIED_TELEMETRY_FIELD.to_owned(), value.clone());
        }

        fields
    }

    fn device_events(&mut self, name: &str, record: &Map<String, Value>, kind: &str) -> Vec<Value> {
        let visible = self.visible(record);
        let previous = self.devices.insert(name.to_owned(), visible.clone());
        let mut events = Vec::new();
        let unchanged = previous.as_ref().is_some_and(|previous| {
            previous == &visible
                || (!self.options.patches
                    && substantive_record(previous) == substantive_record(&visible))
        });

        if !unchanged {
            events.push(match previous {
                Some(previous) if self.options.patches => patch(name, &previous, &visible),
                _ => json!({"event": kind, "server_name": name, "device": visible}),
            });
        }

        if self.options.notices {
            let current = self.notified_fields(record);

            if let Some(previous) = self.notified.insert(name.to_owned(), current.clone()) {
                let fields: BTreeSet<&String> = current
                    .keys()
                    .chain(previous.keys())
                    .filter(|key| current.get(*key) != previous.get(*key))
                    .collect();

                if !fields.is_empty() {
                    events.push(
                        json!({"event": TELEMETRY_UPDATED, "server_name": name, "fields": fields}),
                    );
                }
            }
        }

        events
    }

    fn telemetry_events(&mut self, data: &Value) -> Vec<Value> {
        let name = data
            .get("server_name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let fields = data
            .get("fields")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        if !self.options.telemetry {
            return if self.options.notices {
                vec![json!({"event": TELEMETRY_UPDATED, "server_name": name, "fields": fields})]
            } else {
                Vec::new()
            };
        }

        let notified: BTreeSet<&str> = fields
            .iter()
            .filter_map(Value::as_str)
            .filter(|field| {
                *field == NOTIFIED_TELEMETRY_FIELD && self.options.hidden_fields.contains(*field)
            })
            .collect();
        let mut events = Vec::new();

        if self.options.notices && !notified.is_empty() {
            events
                .push(json!({"event": TELEMETRY_UPDATED, "server_name": name, "fields": notified}));
        }

        let Some(previous) = self.devices.get(name) else {
            return events;
        };
        let Some(telemetry) = data.get("telemetry").and_then(Value::as_object) else {
            return events;
        };
        let mut current = previous.clone();

        for (key, value) in telemetry {
            if self.options.hidden_fields.contains(key) {
                continue;
            }

            if value.is_null() {
                current.remove(key);
            } else {
                current.insert(key.clone(), value.clone());
            }
        }

        if &current == previous {
            return events;
        }

        let change = if self.options.patches {
            patch(name, previous, &current)
        } else {
            json!({"event": DEVICE_UPDATED, "server_name": name, "device": current})
        };

        self.devices.insert(name.to_owned(), current);
        events.insert(0, change);
        events
    }

    fn snapshot_events(&mut self, data: &Value) -> Vec<Value> {
        if !self.options.patches {
            return vec![self.initial_snapshot(data)];
        }

        let empty = Map::new();
        let records = data
            .get("devices")
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        let removed: Vec<String> = self
            .devices
            .keys()
            .filter(|name| !records.contains_key(*name))
            .cloned()
            .collect();
        let mut events = Vec::new();

        for name in removed {
            self.devices.remove(&name);
            self.notified.remove(&name);
            events.push(json!({"event": DEVICE_REMOVED, "server_name": name}));
        }

        for (name, record) in records {
            if let Some(record) = record.as_object() {
                events.extend(self.device_events(name, record, DEVICE_DISCOVERED));
            }
        }

        let managed = data.get("managed").cloned().unwrap_or_default();

        if managed != self.managed {
            self.managed = managed.clone();
            events.push(json!({"event": MANAGED_STATUS, "managed": managed}));
        }

        events
    }
}

fn patch(name: &str, previous: &Map<String, Value>, current: &Map<String, Value>) -> Value {
    let changed: Map<String, Value> = current
        .iter()
        .filter(|(key, value)| previous.get(*key) != Some(*value))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let removed: BTreeSet<&String> = previous
        .keys()
        .filter(|key| !current.contains_key(*key))
        .collect();

    json!({"event": DEVICE_PATCH, "server_name": name, "changed": changed, "removed": removed})
}

pub fn substantive_record(record: &Map<String, Value>) -> Map<String, Value> {
    let mut result: Map<String, Value> = record
        .iter()
        .filter(|(key, _)| !VOLATILE_RECORD_FIELDS.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

    if let Some(clock) = result
        .get_mut("clock_status")
        .and_then(Value::as_object_mut)
    {
        clock.retain(|key, _| !CLOCK_TELEMETRY_FIELDS.contains(&key.as_str()));
    }

    if let Some(channels) = result.get_mut("channels").and_then(Value::as_object_mut) {
        for direction in channels.values_mut().filter_map(Value::as_object_mut) {
            for channel in direction.values_mut().filter_map(Value::as_object_mut) {
                channel.remove("ddm_signal_presence");
            }
        }
    }

    result
}
