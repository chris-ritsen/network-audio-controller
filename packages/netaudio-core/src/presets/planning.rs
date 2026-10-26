use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "PresetActionState"))]
#[serde(rename_all = "snake_case")]
pub enum ActionState {
    Change,
    Unchanged,
    Unsupported,
    Unavailable,
    Ambiguous,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "PresetActionKind"))]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    DeviceName,
    RedundancyMode,
    SampleRate,
    Encoding,
    Latency,
    ReceiveFlowPerformance,
    TransmitFlowPerformance,
    UnicastPerformance,
    ReceiveFlowDefaultSlots,
    SampleRatePullup,
    DeviceControl,
    ClockConfiguration,
    TransmitterChannelNames,
    ReceiverChannelNames,
    TransmitFlow,
    ReceiverSubscriptions,
    ExternalReceiverSubscriptions,
    CodecGain,
    Interface,
    Preserved,
}

#[derive(Clone, Debug, Serialize)]
pub struct RequestedAction {
    pub kind: ActionKind,
    pub requested: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "PresetAction"))]
pub struct Action {
    pub kind: ActionKind,
    pub requested: Value,
    pub current: Value,
    pub state: ActionState,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub details: Value,
}

impl RequestedAction {
    pub fn result(&self, state: ActionState, current: Value, reason: impl Into<String>) -> Action {
        Action {
            kind: self.kind,
            requested: self.requested.clone(),
            current,
            state,
            reason: reason.into(),
            details: Value::Null,
        }
    }

    pub fn compare(&self, current: Value, matches: bool) -> Action {
        self.result(
            if matches {
                ActionState::Unchanged
            } else {
                ActionState::Change
            },
            current,
            if matches {
                "Fresh readback matches the requested value."
            } else {
                "Fresh readback differs from the requested value."
            },
        )
    }
}

pub fn requested_actions(
    configuration: &Map<String, Value>,
) -> Result<Vec<RequestedAction>, String> {
    let mut result = Vec::new();
    let mut known = BTreeSet::from(["name", "device_identity"]);
    let mut field = |name, kind| {
        known.insert(name);
        if let Some(requested) = configuration.get(name) {
            result.push(RequestedAction {
                kind,
                requested: requested.clone(),
            });
        }
    };
    for (name, kind) in [
        ("device_name", ActionKind::DeviceName),
        ("redundancy_mode", ActionKind::RedundancyMode),
        ("sample_rate", ActionKind::SampleRate),
        ("encoding", ActionKind::Encoding),
        ("latency", ActionKind::Latency),
        (
            "receive_flow_performance",
            ActionKind::ReceiveFlowPerformance,
        ),
        (
            "transmit_flow_performance",
            ActionKind::TransmitFlowPerformance,
        ),
        ("unicast_performance", ActionKind::UnicastPerformance),
        (
            "receive_flow_default_slots",
            ActionKind::ReceiveFlowDefaultSlots,
        ),
        ("sample_rate_pullup", ActionKind::SampleRatePullup),
    ] {
        field(name, kind);
    }
    known.insert("device_controls");
    if let Some(controls) = configuration.get("device_controls") {
        let controls = controls
            .as_object()
            .ok_or("device_controls must be an object")?;
        if let Some(settings) = controls.get("settings") {
            for (category, requested) in settings
                .as_object()
                .ok_or("device_controls settings must be an object")?
            {
                if category.to_lowercase().contains("pair") {
                    return Err("Pairing-list clearing is excluded from presets.".into());
                }
                result.push(RequestedAction {
                    kind: ActionKind::DeviceControl,
                    requested: json!({"category": category, "requested": requested}),
                });
            }
        }
        let unknown: Map<_, _> = controls
            .iter()
            .filter(|(field, _)| {
                !matches!(
                    field.as_str(),
                    "settings" | "identity" | "observed_extensions"
                )
            })
            .map(|(field, value)| (field.clone(), value.clone()))
            .collect();
        if !unknown.is_empty() {
            result.push(RequestedAction {
                kind: ActionKind::Preserved,
                requested: json!({"field": "device_controls", "value": unknown}),
            });
        }
    }
    let mut clock = Map::new();
    for (source, target) in [
        ("preferred_leader", "preferred_leader"),
        ("clock_source_code", "clock_source"),
        ("clock_subdomain", "subdomain"),
        (
            "global_unicast_delay_requests",
            "global_unicast_delay_requests",
        ),
        (
            "aggregate_ptpv1_unicast_delay_requests",
            "aggregate_ptpv1_unicast_delay_requests",
        ),
    ] {
        known.insert(source);
        if let Some(value) = configuration.get(source) {
            clock.insert(target.into(), value.clone());
        }
    }
    known.insert("clock_configuration");
    if let Some(extended) = configuration.get("clock_configuration") {
        for (name, value) in extended
            .as_object()
            .ok_or("clock_configuration must be an object")?
        {
            if clock.insert(name.clone(), value.clone()).is_some() {
                return Err(format!(
                    "Clock configuration contains duplicate setting {name}."
                ));
            }
        }
    }
    if !clock.is_empty() {
        result.push(RequestedAction {
            kind: ActionKind::ClockConfiguration,
            requested: clock.into(),
        });
    }
    for (field, kind) in [
        (
            "transmitter_channel_names",
            ActionKind::TransmitterChannelNames,
        ),
        ("receiver_channel_names", ActionKind::ReceiverChannelNames),
    ] {
        known.insert(field);
        if let Some(requested) = configuration
            .get(field)
            .filter(|value| value.as_object().is_some_and(|value| !value.is_empty()))
        {
            result.push(RequestedAction {
                kind,
                requested: requested.clone(),
            });
        }
    }
    known.insert("transmit_flows");
    for requested in configuration
        .get("transmit_flows")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        result.push(RequestedAction {
            kind: ActionKind::TransmitFlow,
            requested: requested.clone(),
        });
    }
    known.insert("rx_subscriptions");
    if let Some(subscriptions) = configuration
        .get("rx_subscriptions")
        .and_then(Value::as_object)
    {
        let mut native = Map::new();
        let mut external = Map::new();
        for (number, subscription) in subscriptions {
            if subscription["kind"] == "external_rtp" {
                external.insert(number.clone(), subscription.clone());
            } else {
                native.insert(number.clone(), subscription.clone());
            }
        }
        for (requested, kind) in [
            (native, ActionKind::ReceiverSubscriptions),
            (external, ActionKind::ExternalReceiverSubscriptions),
        ] {
            if !requested.is_empty() {
                result.push(RequestedAction {
                    kind,
                    requested: requested.into(),
                });
            }
        }
    }
    for (field, kind) in [
        ("codec_gain", ActionKind::CodecGain),
        ("interfaces", ActionKind::Interface),
    ] {
        known.insert(field);
        for requested in configuration
            .get(field)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            result.push(RequestedAction {
                kind,
                requested: requested.clone(),
            });
        }
    }
    for (field, value) in configuration {
        if !known.contains(field.as_str()) {
            result.push(RequestedAction {
                kind: ActionKind::Preserved,
                requested: json!({"field": field, "value": value}),
            });
        }
    }
    Ok(result)
}
