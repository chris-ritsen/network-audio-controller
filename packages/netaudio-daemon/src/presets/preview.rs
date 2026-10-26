use super::{ParsedPreset, matches_record};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) fn presentation(
    parsed: &ParsedPreset,
    identifiers: &[String],
    records: &BTreeMap<String, Arc<Value>>,
) -> Value {
    let mut devices = Vec::new();

    for (name, device) in parsed.document.devices() {
        let configuration = device.configuration();
        let mut targets = Vec::new();

        for identifier in identifiers {
            let Some(record) = records.get(identifier) else {
                continue;
            };

            if matches_record(configuration, identifier, record) {
                targets.push(json!({
                    "id": identifier,
                    "name": record["name"],
                    "address": record["ipv4"],
                    "context": record["ddm_context"],
                    "online": record["online"].as_bool().unwrap_or(false),
                }));
            }
        }

        let preserved: Vec<_> = [
            ("external_word_clock", "External word-clock selection"),
            ("ha_bridge", "HA bridge"),
            ("unknown_fields", "Unknown extension fields"),
        ]
        .into_iter()
        .filter_map(|(field, label)| configuration.contains_key(field).then_some(label))
        .collect();
        devices.push(json!({
            "name": name,
            "settings": settings(configuration),
            "preserved": preserved,
            "targets": targets,
        }));
    }

    json!({"name": parsed.document.name, "digest": parsed.digest, "devices": devices})
}

fn settings(configuration: &Map<String, Value>) -> Vec<Value> {
    let mut entries = Vec::new();

    for (field, label, suffix) in [
        ("sample_rate", "Sample rate", " Hz"),
        ("encoding", "Encoding", "-bit"),
        ("latency", "Latency", " ms"),
    ] {
        if let Some(value) = configuration.get(field) {
            entries.push(json!({"label": label, "value": format!("{}{suffix}", display(value))}));
        }
    }

    for (field, label) in [
        ("preferred_leader", "Preferred leader"),
        ("device_name", "Device name"),
        ("sample_rate_pullup", "Sample-rate pull-up"),
    ] {
        if let Some(value) = configuration.get(field) {
            entries.push(json!({"label": label, "value": display(value)}));
        }
    }

    for (field, label) in [
        ("receive_flow_performance", "Receive-flow performance"),
        ("transmit_flow_performance", "Transmit-flow performance"),
        ("unicast_performance", "Unicast performance"),
    ] {
        if let Some(value) = configuration.get(field) {
            entries.push(json!({
                "label": label,
                "value": format!("{} µs, {} frames/packet", value["latency_microseconds"], value["frames_per_packet"]),
            }));
        }
    }

    if let Some(value) = configuration.get("receive_flow_default_slots") {
        entries.push(json!({"label": "Receive-flow default slots", "value": display(value)}));
    }

    if let Some(value) = configuration
        .get("clock_source_code")
        .and_then(Value::as_u64)
    {
        entries.push(json!({"label": "Clock source", "value": format!("0x{value:04X}")}));
    }

    if let Some(value) = configuration.get("clock_subdomain") {
        let presentation = netaudio_core::clock_configuration::subdomain_presentation(value);
        entries.push(json!({"label": "Clock subdomain", "value": presentation.label}));
    }

    for (field, label) in [
        (
            "global_unicast_delay_requests",
            "Global unicast delay requests",
        ),
        (
            "aggregate_ptpv1_unicast_delay_requests",
            "PTPv1 unicast delay requests",
        ),
        ("external_word_clock", "External word clock"),
        ("redundancy_mode", "Redundancy"),
    ] {
        if let Some(value) = configuration.get(field) {
            entries.push(json!({"label": label, "value": display(value)}));
        }
    }

    if let Some(names) = configuration
        .get("transmitter_channel_names")
        .and_then(Value::as_object)
    {
        entries.push(
            json!({"label": "Transmitter names", "value": format!("{} channels", names.len())}),
        );
    }

    if let Some(routes) = configuration
        .get("rx_subscriptions")
        .and_then(Value::as_object)
    {
        let subscribed = routes.values().filter(|value| !value.is_null()).count();
        entries.push(json!({"label": "Receiver routing", "value": format!("{subscribed} subscribed, {} unsubscribed", routes.len() - subscribed)}));
    }

    if let Some(names) = configuration
        .get("receiver_channel_names")
        .and_then(Value::as_object)
    {
        entries
            .push(json!({"label": "Receiver names", "value": format!("{} channels", names.len())}));
    }

    if let Some(flows) = configuration
        .get("transmit_flows")
        .and_then(Value::as_array)
    {
        entries.push(json!({"label": "Transmit flows", "value": flows.len().to_string()}));
    }

    if let Some(gains) = configuration.get("codec_gain").and_then(Value::as_array) {
        entries.push(json!({"label": "Codec gain", "value": format!("{} channels", gains.len())}));
    }

    for interface in configuration
        .get("interfaces")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let mode = interface["mode"].as_str().unwrap_or_default();
        let value = if mode == "static" {
            format!(
                "Static {}",
                interface["ip_address"].as_str().unwrap_or_default()
            )
        } else {
            mode.to_uppercase()
        };
        entries.push(json!({"label": format!("Network ({})", interface["identity"].as_str().unwrap_or_default()), "value": value}));
    }

    entries
}

fn display(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Bool(value) => if *value { "On" } else { "Off" }.into(),
        _ => value.to_string(),
    }
}
