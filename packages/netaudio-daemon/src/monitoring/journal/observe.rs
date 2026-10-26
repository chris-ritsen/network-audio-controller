use super::model::{EventKind, JournalEvent, Severity, device_identity};
use anyhow::Result;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;

const CONFIGURATION_FIELDS: &[&str] = &[
    "name",
    "sample_rate_hz",
    "encoding",
    "latency_ms",
    "preferred_leader",
    "is_locked",
    "aes67_configured",
];
const CLOCK_FIELDS: &[&str] = &[
    "synchronization",
    "servo_state_code",
    "clock_state_code",
    "mute_flags",
    "clock_source_code",
    "ptpv1_grandmaster_uuid",
    "ptpv2_domain",
    "clock_capabilities",
    "extension_flags",
    "clock_subdomain",
];
const PRESENCE_FIELDS: &[&str] = &[
    "online",
    "availability_state",
    "last_seen",
    "name",
    "mac_address",
];

#[derive(Default)]
pub struct Observer {
    snapshots: BTreeMap<String, Arc<Value>>,
    conditions: BTreeMap<(String, threshold::Condition, String), bool>,
    thresholds: super::model::Thresholds,
}

pub struct Observation<'a> {
    pub current: &'a Value,
    pub previous: &'a Value,
    pub timestamp: &'a str,
    pub events: Vec<JournalEvent>,
}

impl Observation<'_> {
    pub fn event(
        &self,
        kind: EventKind,
        severity: Severity,
        previous: Value,
        current: Value,
        source: &str,
    ) -> Result<JournalEvent> {
        let mut event = JournalEvent::observed(
            self.current,
            self.timestamp,
            kind,
            severity,
            previous,
            current,
            source,
        )?;
        let mut context = selected(
            self.current,
            &["failed_queries", "field_sources", "ddm_status"],
        );
        context.retain(|_, value| !value.is_null() && value != &json!([]) && value != &json!({}));

        if !self.current["error"].is_null() {
            context.insert("device_error".to_owned(), self.current["error"].clone());
        }

        if !context.is_empty() {
            event
                .raw
                .insert("observation_context".to_owned(), Value::Object(context));
        }

        Ok(event)
    }
}

impl Observer {
    pub fn new(thresholds: super::model::Thresholds) -> Self {
        Self {
            thresholds,
            ..Self::default()
        }
    }

    pub fn observe(&mut self, current: Arc<Value>, timestamp: &str) -> Result<Vec<JournalEvent>> {
        let identity = device_identity(&current)?;
        let previous = self
            .snapshots
            .insert(identity.clone(), Arc::clone(&current));
        let replaced = previous.as_ref().is_some_and(|previous| {
            matches!((previous["mac_address"].as_str(), current["mac_address"].as_str()), (Some(before), Some(after)) if before != after)
        });
        if replaced || previous.is_none() {
            self.conditions
                .retain(|(device, _, _), _| device != &identity);
        }

        let previous = previous.filter(|_| !replaced);
        let mut observation = Observation {
            current: &current,
            previous: previous.as_deref().unwrap_or(&Value::Null),
            timestamp,
            events: Vec::new(),
        };
        if previous.is_some() {
            presence(&mut observation)?;
            clocks(&mut observation)?;
            controls(&mut observation)?;
            channels(&mut observation)?;
            configuration(&mut observation)?;
            subscriptions(&mut observation)?;
            counters::observe(&mut observation)?;
        }

        threshold::observe(
            &mut observation,
            &identity,
            &mut self.conditions,
            self.thresholds,
        )?;
        Ok(observation.events)
    }

    pub fn remove(&mut self, identity: &str, timestamp: &str) -> Result<Vec<JournalEvent>> {
        let Some(previous) = self.snapshots.get(identity) else {
            return Ok(Vec::new());
        };
        let mut current = previous.as_ref().clone();
        current["online"] = json!(false);
        self.observe(Arc::new(current), timestamp)
    }

    pub fn snapshot(&self, identity: &str) -> Option<Arc<Value>> {
        self.snapshots.get(identity).cloned()
    }

    pub fn forget(&mut self, identity: &str) {
        self.snapshots.remove(identity);
        self.conditions
            .retain(|(device, _, _), _| device != identity);
    }

    pub fn reset(&mut self) {
        self.snapshots.clear();
        self.conditions.clear();
    }
}

fn presence(observation: &mut Observation<'_>) -> Result<()> {
    let before = observation.previous["online"].as_bool();
    let after = observation.current["online"].as_bool();
    let kind = match (before, after) {
        (Some(true), Some(false)) => EventKind::DeviceDisappeared,
        (Some(false), Some(true)) => EventKind::DeviceReappeared,
        _ => return Ok(()),
    };
    let mut event = observation.event(
        kind,
        Severity::Info,
        json!(before),
        json!(after),
        "device_lifecycle",
    )?;
    event.raw.extend([
        (
            "previous_observation".to_owned(),
            json!(selected(observation.previous, PRESENCE_FIELDS)),
        ),
        (
            "current_observation".to_owned(),
            json!(selected(observation.current, PRESENCE_FIELDS)),
        ),
    ]);
    observation.events.push(event);
    Ok(())
}

fn clocks(observation: &mut Observation<'_>) -> Result<()> {
    let before_status = &observation.previous["clock_status"];
    let after_status = &observation.current["clock_status"];
    let fresh = after_status["observation_state"] == "fresh";
    let mut before = selected(before_status, CLOCK_FIELDS);
    let mut after = selected(after_status, CLOCK_FIELDS);

    for fields in [&mut before, &mut after] {
        if let Some(identifier) = fields["ptpv1_grandmaster_uuid"].as_str() {
            fields.insert(
                "ptpv1_grandmaster_uuid".to_owned(),
                json!(identifier.replace([':', '-', '.'], "").to_lowercase()),
            );
        }
    }

    if before_status.is_object() && fresh && before != after {
        let severity = if after_status["synchronization"] == "lost"
            || after_status["mute_flags"]
                .as_u64()
                .is_some_and(|flags| flags != 0)
        {
            Severity::Warning
        } else {
            Severity::Info
        };
        let mut event = observation.event(
            EventKind::ClockStatusChanged,
            severity,
            json!(before),
            json!(after),
            "device_clock_status",
        )?;
        event
            .raw
            .insert("clock_status".to_owned(), after_status.clone());
        observation.events.push(event);
    }

    if !fresh {
        return Ok(());
    }

    for (field, kind, severity) in [
        ("clock_role", EventKind::ClockRoleChanged, Severity::Info),
        (
            "ptpv1_master_uuid",
            EventKind::LeaderIdentityChanged,
            Severity::Warning,
        ),
    ] {
        let before = &observation.previous[field];
        let after = &observation.current[field];

        if before.is_null() || after.is_null() || before == after {
            continue;
        }

        let mut event = observation.event(
            kind,
            severity,
            before.clone(),
            after.clone(),
            "device_clock_status",
        )?;
        event.raw.extend([
            ("previous_clock_state".to_owned(), before_status.clone()),
            ("current_clock_state".to_owned(), after_status.clone()),
        ]);
        observation.events.push(event);
    }

    let before = port_map(&observation.previous["clock_port_records"]);
    let after = port_map(&observation.current["clock_port_records"]);
    const PORT_FIELDS: &[&str] = &[
        "state_code",
        "role",
        "link_down",
        "user_disabled",
        "unicast_delay_requests",
        "network_interface_index",
    ];

    for (identity, current) in after {
        let Some(previous) = before.get(&identity) else {
            continue;
        };
        let before = selected(previous, PORT_FIELDS);
        let after = selected(current, PORT_FIELDS);

        if before["state_code"].is_null() || after["state_code"].is_null() || before == after {
            continue;
        }

        let severity = if current["link_down"] == true {
            Severity::Warning
        } else {
            Severity::Info
        };
        let mut event = observation.event(
            EventKind::PtpPortStateChanged,
            severity,
            json!(before),
            json!(after),
            "device_clock_status",
        )?;
        event.interface_identity = Some(identity);
        event.raw.extend([
            ("previous_port".to_owned(), (*previous).clone()),
            ("current_port".to_owned(), current.clone()),
        ]);
        observation.events.push(event);
    }

    Ok(())
}

fn controls(observation: &mut Observation<'_>) -> Result<()> {
    if observation.current["online"] == false {
        return Ok(());
    }

    let before = &observation.previous["device_controls"]["observations"];
    let after = &observation.current["device_controls"]["observations"];

    for (category, current) in after.as_object().into_iter().flatten() {
        let mut old = before[category]["value"].clone();
        let mut value = current["value"].clone();

        if current["fresh"] != true || old.is_null() {
            continue;
        }

        if category == "bluetooth_connection" {
            old = json!({"state": old["state"]});
            value = json!({"state": value["state"]});
        }

        if old == value {
            continue;
        }

        let mut event = observation.event(
            EventKind::DeviceControlChanged,
            Severity::Info,
            old,
            value,
            "device_control_status",
        )?;
        event.raw.extend([
            ("category".to_owned(), json!(category)),
            (
                "record_revision".to_owned(),
                current["record_revision"].clone(),
            ),
        ]);
        observation.events.push(event);
    }

    Ok(())
}

fn channels(observation: &mut Observation<'_>) -> Result<()> {
    for (direction, inventory) in [("rx", "receivers"), ("tx", "transmitters")] {
        let before = &observation.previous["channels"][inventory];
        let after = &observation.current["channels"][inventory];

        for (number, current) in after.as_object().into_iter().flatten() {
            let previous = &before[number];

            if !previous.is_object() {
                continue;
            }

            if let (Some(old), Some(new)) =
                (previous["muted"].as_bool(), current["muted"].as_bool())
                && old != new
            {
                let mut event = observation.event(
                    EventKind::MuteStateChanged,
                    if new {
                        Severity::Warning
                    } else {
                        Severity::Info
                    },
                    json!(old),
                    json!(new),
                    "device_channel_status",
                )?;
                event.channel_identity = Some(format!("{direction}:{number}"));
                event.raw.extend([
                    ("previous_channel".to_owned(), previous.clone()),
                    ("current_channel".to_owned(), current.clone()),
                ]);
                observation.events.push(event);
            }

            if observation.previous["online"] == false || observation.current["online"] == false {
                continue;
            }

            let before = channel_label(previous);
            let after = channel_label(current);

            if before != after
                && let (Some(before), Some(after)) = (before, after)
            {
                let mut event = observation.event(
                    EventKind::ChannelRenamed,
                    Severity::Info,
                    json!({"label": before}),
                    json!({"label": after}),
                    "device_state",
                )?;
                event.channel_identity = Some(format!("{direction}:{number}"));
                event.raw.insert("direction".to_owned(), json!(inventory));
                observation.events.push(event);
            }
        }
    }

    Ok(())
}

fn configuration(observation: &mut Observation<'_>) -> Result<()> {
    if observation.previous["online"] == false || observation.current["online"] == false {
        return Ok(());
    }

    for field in CONFIGURATION_FIELDS {
        let before = &observation.previous[*field];
        let after = &observation.current[*field];

        if before.is_null() || after.is_null() || before == "" || after == "" || before == after {
            continue;
        }

        let mut event = observation.event(
            EventKind::SettingChanged,
            Severity::Info,
            json!({*field: before}),
            json!({*field: after}),
            "device_state",
        )?;
        event.raw.insert("field".to_owned(), json!(field));
        observation.events.push(event);
    }

    Ok(())
}

fn subscriptions(observation: &mut Observation<'_>) -> Result<()> {
    let before = subscription_map(&observation.previous["subscriptions"]);
    let after = subscription_map(&observation.current["subscriptions"]);

    for (identity, current) in &after {
        let Some(previous) = before.get(identity) else {
            continue;
        };
        let (Some(old), Some(new)) = (
            subscription_failure(previous),
            subscription_failure(current),
        ) else {
            continue;
        };

        if old == new {
            continue;
        }

        let (kind, severity) = if new {
            (
                EventKind::SubscriptionFailed,
                if current["status"]["severity"] == "error" {
                    Severity::Error
                } else {
                    Severity::Warning
                },
            )
        } else {
            (EventKind::SubscriptionRecovered, Severity::Info)
        };
        let mut event = observation.event(
            kind,
            severity,
            previous["status"].clone(),
            current["status"].clone(),
            "receiver_subscription_status",
        )?;
        event.channel_identity = Some(identity.clone());
        event.raw.extend([
            ("previous_subscription".to_owned(), (*previous).clone()),
            ("current_subscription".to_owned(), (*current).clone()),
        ]);
        observation.events.push(event);
    }

    if !observation.previous["subscriptions"].is_array()
        || !observation.current["subscriptions"].is_array()
        || observation.previous["online"] == false
        || observation.current["online"] == false
    {
        return Ok(());
    }

    let previous_channels = &observation.previous["channels"]["receivers"];
    let current_channels = &observation.current["channels"]["receivers"];

    for (number, _) in current_channels.as_object().into_iter().flatten() {
        if !previous_channels[number].is_object() {
            continue;
        }

        let identity = format!("rx:{number}");
        let old = before.get(&identity).and_then(|value| route_source(value));
        let new = after.get(&identity).and_then(|value| route_source(value));

        if old != new {
            let mut event = observation.event(
                EventKind::RouteChanged,
                Severity::Info,
                json!({"source": old.unwrap_or_else(|| "nothing".to_owned())}),
                json!({"source": new.unwrap_or_else(|| "nothing".to_owned())}),
                "device_state",
            )?;
            event.channel_identity = Some(identity);
            observation.events.push(event);
        }
    }

    Ok(())
}

pub(super) fn selected(value: &Value, fields: &[&str]) -> Map<String, Value> {
    fields
        .iter()
        .map(|field| ((*field).to_owned(), value[*field].clone()))
        .collect()
}

fn channel_label(value: &Value) -> Option<&str> {
    value["friendly_name"]
        .as_str()
        .filter(|value| !value.is_empty())
        .or_else(|| value["name"].as_str().filter(|value| !value.is_empty()))
}

fn route_source(value: &Value) -> Option<String> {
    let device = value["tx_device"]
        .as_str()
        .filter(|value| !value.is_empty())?;

    if value["status"]["state"] == "none" {
        return None;
    }

    let channel = value["tx_channel"].as_str().unwrap_or("None");
    Some(format!("{device}:{channel}"))
}

pub(crate) fn subscription_map(value: &Value) -> BTreeMap<String, &Value> {
    let mut subscriptions = BTreeMap::new();

    for (index, subscription) in value.as_array().into_iter().flatten().enumerate() {
        let identity = if let Some(number) = subscription["rx_channel_number"].as_u64() {
            format!("rx:{number}")
        } else if let Some(channel) = subscription["rx_channel"]
            .as_str()
            .filter(|value| !value.is_empty())
        {
            format!(
                "rx:{}/{channel}",
                subscription["rx_device"].as_str().unwrap_or_default()
            )
        } else {
            continue;
        };
        let identity = if subscriptions.contains_key(&identity) {
            format!("{identity}/entry:{index}")
        } else {
            identity
        };
        subscriptions.insert(identity, subscription);
    }

    subscriptions
}

pub(crate) fn subscription_failure(value: &Value) -> Option<bool> {
    let status = &value["status"];

    if matches!(status["severity"].as_str(), Some("error" | "warning")) {
        Some(true)
    } else if status["severity"] == "ok" || status["state"] == "connected" {
        Some(false)
    } else {
        None
    }
}

fn port_map(value: &Value) -> BTreeMap<String, &Value> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|port| {
            let record = port["record_number"].as_u64()?;
            let version = port["ptp_version"].as_u64()?;
            let transport = port
                .get("transport_path")
                .filter(|value| !value.is_null())
                .or_else(|| port.get("transport_path_code"))?;
            let transport = transport
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| transport.to_string());
            Some((format!("ptp:{version}/{transport}/record:{record}"), port))
        })
        .collect()
}

mod counters;
pub(crate) use counters::flow_map;
mod threshold;
