use anyhow::Result;
use netaudio_core::capabilities::{Availability, AvailabilityRequest, availability};
use netaudio_core::network::{NetworkControlState, network_control_state, redundancy_control};
use serde_json::{Map, Value, json};

#[derive(serde::Serialize, schemars::JsonSchema)]
pub struct OperationAvailability<'a> {
    supported: Option<bool>,
    readable: bool,
    writable: bool,
    reasons: &'a [&'static str],
}

const OPERATIONS: [(&str, &str, Option<&str>); 9] = [
    ("identify", "identify_supported", None),
    (
        "sample_rate",
        "sample_rate_configuration_supported",
        Some("sample_rate_hz"),
    ),
    (
        "encoding",
        "encoding_configuration_supported",
        Some("encoding"),
    ),
    (
        "sample_rate_pullup",
        "sample_rate_pullup_configuration_supported",
        Some("sample_rate_pullup_raw_value"),
    ),
    (
        "aes67",
        "aes67_configuration_supported",
        Some("aes67_current"),
    ),
    (
        "static_ipv4",
        "static_ipv4_configuration_supported",
        Some("interfaces"),
    ),
    (
        "redundancy",
        "switch_redundancy_supported",
        Some("dante_redundancy"),
    ),
    (
        "codec_control",
        "generic_codec_control_supported",
        Some("codec_parameters"),
    ),
    ("locking", "device_locking_supported", Some("is_locked")),
];

pub fn operation(record: &Value, operation: &str) -> Result<Availability> {
    let (_, capability, current) = OPERATIONS
        .iter()
        .find(|(name, _, _)| *name == operation)
        .ok_or_else(|| anyhow::anyhow!("unknown device capability {operation}"))?;
    let mut facts = json!({
        "operation": operation,
        "supported": record[*capability],
        "readable": current.is_some_and(|current| !record[current].is_null()) || operation == "aes67" && !record["aes67_configured"].is_null(),
        "read_only": match operation {
            "static_ipv4" => &record["static_ipv4_configuration_read_only"],
            "redundancy" => &record["switch_redundancy_read_only"],
            _ => &Value::Null,
        },
        "locked": record["is_locked"],
        "transport_available": true,
        "has_adapter": !record["gain_adapter"].is_null(),
        "managed": record["requires_managed_control"] == true,
        "permission": record["managed_operation_permissions"][operation],
    });

    if operation == "redundancy" {
        facts["supported_source"] = record["redundancy_advertised_support_source"].clone();
        facts["read_only_source"] = record["redundancy_read_only_source"].clone();
        facts["redundancy"] = json!({"state": record["dante_redundancy"]});
        facts["transport_available"] =
            json!(network_state(record, None, false, None)?.transport_available);
    }

    if let Some(choices) = match operation {
        "sample_rate" => Some("supported_sample_rates"),
        "encoding" => Some("supported_encodings"),
        "sample_rate_pullup" => Some("supported_sample_rate_pullup_raw_values"),
        _ => None,
    } {
        facts["audio"] = json!({
            "update_mode": record[format!("{operation}_update_mode")],
            "available_values": record[choices],
            "host_disabled": if operation == "sample_rate_pullup" { &record["sample_rate_pullup_host_disabled"] } else { &Value::Null },
        });
    }

    let facts: AvailabilityRequest = serde_json::from_value(facts)?;
    Ok(availability(facts))
}

pub fn presentation(availability: &Availability) -> Value {
    json!(OperationAvailability {
        supported: availability.supported,
        readable: availability.readable,
        writable: availability.writable,
        reasons: &availability.reasons,
    })
}

pub fn operations(record: &Value) -> Result<Value> {
    let mut result = Map::new();

    for (name, _, _) in OPERATIONS {
        result.insert(name.to_owned(), presentation(&operation(record, name)?));
    }

    let managed = record["requires_managed_control"] == true;
    result.insert(
        "metering".to_owned(),
        json!({
            "supported": !managed,
            "readable": !managed,
            "writable": !managed,
            "reasons": if managed { vec!["managed_metering_not_implemented"] } else { Vec::new() },
        }),
    );
    Ok(Value::Object(result))
}

fn network_state(
    record: &Value,
    entry: Option<&Value>,
    writable: bool,
    supported: Option<bool>,
) -> Result<NetworkControlState> {
    Ok(network_control_state(serde_json::from_value(json!({
        "entry": entry,
        "interfaces": record["interfaces"],
        "writable": writable,
        "redundancy_supported": supported,
        "managed": record["requires_managed_control"] == true,
        "transports": record["control_transports"],
        "address_available": !record["ipv4"].is_null(),
    }))?))
}

pub fn redundancy(record: &Value) -> Result<Value> {
    let availability = operation(record, "redundancy")?;
    let state = &record["dante_redundancy"];
    let network = network_state(record, None, false, availability.supported)?;
    let control = redundancy_control(serde_json::from_value(json!({"state": state}))?);
    let mut result = json!({
        "advertised_support": availability.supported,
        "advertised_support_source": record["redundancy_advertised_support_source"],
        "read_only": availability.read_only,
        "read_only_source": record["redundancy_read_only_source"],
        "current_mode": state["current"],
        "configured_mode": state["configured"],
        "state_source": state["state_source"],
        "state_fresh": state["state_fresh"] == true,
        "available_modes": state["available_modes"].as_array(),
        "available_modes_source": state["available_modes_source"],
        "available_modes_fresh": state["available_modes_fresh"] == true,
        "interface_inventory": {"reported_count": network.reported_count, "completeness": network.inventory_completeness},
        "licensed_redundancy": {"enabled": record["licensed_redundancy_enabled"].as_bool(), "source": record["licensed_redundancy_enabled"].as_bool().map(|_| "diagnostic_log_export")},
        "serializer_cohort": control.serializer_cohort,
        "probe_outcomes": record["redundancy_probe_outcomes"].as_object().cloned().unwrap_or_default(),
        "reboot_required": state["reboot_required"] == true,
        "operation_availability": presentation(&availability),
    });

    for field in ["current_mode_evidence", "configured_mode_evidence"] {
        result[field] = state
            .get(field)
            .filter(|value| value.is_object())
            .cloned()
            .unwrap_or_else(|| json!({"status": "unavailable", "mode": null}));
    }

    Ok(result)
}

pub fn network(record: &Value) -> Result<Value> {
    let interface_availability = operation(record, "static_ipv4")?;
    let redundancy = redundancy(record)?;
    let interfaces = record["interfaces"].as_array().cloned().unwrap_or_default();
    let mut modes = Map::new();

    for interface in &interfaces {
        if let Some(name @ ("primary" | "secondary")) = interface["interface"].as_str()
            && !interface["configured"].is_null()
        {
            modes.insert(
                name.to_owned(),
                json!(
                    network_state(
                        record,
                        Some(interface),
                        interface_availability.writable,
                        None
                    )?
                    .configuration_modes
                ),
            );
        }
    }

    Ok(json!({
        "device": record["server_name"],
        "interfaces": interfaces,
        "interface_configuration_modes": modes,
        "reboot_required": record["interface_reboot_required"] == true || redundancy["reboot_required"] == true,
        "link_speed_mbps": record["link_speed_mbps"],
        "operation_availability": {"static_ipv4": presentation(&interface_availability), "redundancy": redundancy["operation_availability"]},
        "redundancy": redundancy,
    }))
}
