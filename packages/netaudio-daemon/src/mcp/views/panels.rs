use super::compact;
use serde_json::{Map, Value, json};

fn meaning(category: &str, value: &Value) -> Value {
    match category {
        "bluetooth_connection" => {
            let state = match value["state"].as_u64() {
                Some(0) => "undefined",
                Some(1) => "connected",
                Some(2) => "disconnected",
                Some(3) => "connected but link lost",
                _ => return value.clone(),
            };
            let peer = value["peer_name"].as_str().unwrap_or_default();
            json!(if state.starts_with("connected") && !peer.is_empty() {
                format!("{state} to {peer}")
            } else {
                state.to_owned()
            })
        }
        "bluetooth_identification" => match value["name_source"].as_u64() {
            Some(1) => json!("the Dante device name"),
            Some(2) if value["custom_name"].is_string() => json!(format!(
                "custom name {:?}",
                value["custom_name"].as_str().unwrap()
            )),
            _ => value.clone(),
        },
        "bluetooth_discovery" => match value.as_u64() {
            Some(1) => json!("discoverable"),
            Some(2) => json!("not discoverable"),
            _ => value.clone(),
        },
        "bluetooth_pairing" => value.as_u64().map_or_else(
            || value.clone(),
            |count| {
                json!(format!(
                    "{count} remembered paired device{}",
                    if count == 1 { "" } else { "s" }
                ))
            },
        ),
        "analog_level" if value.get("channel_levels").is_some() => {
            json!({"levels": value["channel_levels"]})
        }
        "analog_level" if value.get("level").is_some() => compact(
            json!({"channel": value["channel"], "level": value["level"], "direction": value["direction"]}),
        ),
        _ => value.clone(),
    }
}

pub fn plan(payload: &Value) -> Value {
    let category = payload["category"].as_str().unwrap_or_default();
    let action = payload["action"].as_str().unwrap_or_default();
    compact(json!({
        "category": category,
        "result": match action { "change" => "can be applied", "unchanged" => "already set; nothing to change", "unsupported" => "refused", "unavailable" => "cannot be checked right now", _ => action },
        "from": meaning(category, &payload["before"]),
        "to": if matches!(action, "change" | "unchanged") { meaning(category, payload.get("expected").unwrap_or(&payload["requested"])) } else { Value::Null },
        "reason": payload["reason"],
        "next": (action == "change").then_some("apply_device_control with the same arguments applies it after the user confirms"),
    }))
}

pub fn applied(payload: &Value) -> Value {
    let plan = &payload["plan"];
    let category = plan["category"].as_str().unwrap_or_default();
    let verified = payload["effective_state_confirmed"] == true;
    compact(json!({
        "category": category,
        "changed": verified && plan["action"] == "change",
        "verified": verified,
        "from": meaning(category, &plan["before"]),
        "now": verified.then(|| meaning(category, payload.get("status").unwrap_or(&plan["before"]))),
        "requested": (!verified).then(|| meaning(category, plan.get("expected").unwrap_or(&plan["requested"]))),
        "sent": payload["request_sent"],
        "reason": payload.get("reason").filter(|value| !value.is_null()).unwrap_or(&plan["reason"]),
        "note": (payload["request_sent"] == true && !verified).then_some("The request was sent but the device has not confirmed the new value; read inspect_device_controls again before retrying."),
    }))
}

fn hexadecimal(value: &Value) -> Value {
    match value {
        Value::Array(values) => json!(values.iter().map(hexadecimal).collect::<Vec<_>>()),
        Value::Object(fields) => {
            let mut result = Map::new();
            for (key, value) in fields {
                let bytes = matches!(
                    key.as_str(),
                    "raw_record" | "application_payload" | "source_identifier" | "raw_response"
                )
                .then(|| {
                    value.as_array().and_then(|values| {
                        values
                            .iter()
                            .map(|value| value.as_u64().and_then(|value| u8::try_from(value).ok()))
                            .collect::<Option<Vec<_>>>()
                    })
                })
                .flatten();
                if let Some(bytes) = bytes {
                    result.insert(format!("{key}_hexadecimal"), json!(hex::encode(bytes)));
                } else {
                    result.insert(key.clone(), hexadecimal(value));
                }
            }
            json!(result)
        }
        _ => value.clone(),
    }
}

fn analog(payload: &Value) -> Value {
    if !payload.is_object()
        || payload["read_unavailable_reason"] == "Codec-control support is unavailable."
    {
        return Value::Null;
    }
    let choices: Map<_, _> = payload["choices"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|choice| {
            Some((
                choice["value"].as_u64()?.to_string(),
                choice["label"].clone(),
            ))
        })
        .collect();
    compact(json!({
        "direction": payload["direction"], "levels": payload["levels"],
        "choices": (!choices.is_empty()).then_some(choices),
        "unavailable": payload["read_unavailable_reason"], "not_writable": payload["write_unavailable_reason"],
        "change_with": (!payload["levels"].is_null() && payload["write_unavailable_reason"].is_null()).then_some("category analog_level and requested {channel: N, level: one of the choices}"),
    }))
}

pub fn controls(payload: &Value, arguments: &Value) -> Value {
    if arguments["detail"] == "debug" {
        return hexadecimal(payload);
    }
    if payload["family"].is_null() {
        return compact(
            json!({"panel": "none known", "why": payload["read_unavailable_reason"], "analog": analog(&payload["analog"]), "detail": "detail=debug returns the raw panel records"}),
        );
    }
    let editors = &payload["presentation"]["editors"];
    let mut settings = Map::new();
    for (category, observation) in payload["observations"].as_object().into_iter().flatten() {
        let labels: Map<_, _> = editors[category]["initial_fields"]
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(name, field)| {
                field
                    .get("label")
                    .map(|label| (name.clone(), label.clone()))
            })
            .collect();
        let interpretation = if category.starts_with("bluetooth_") {
            meaning(category, &observation["value"])
        } else if labels.is_empty() {
            Value::Null
        } else {
            json!(labels)
        };
        settings.insert(category.clone(), compact(json!({"value": observation["value"], "meaning": interpretation, "current": (observation["available"] != true || observation["fresh"] != true).then_some(false)})));
    }
    let mut changeable = Map::new();
    for category in payload["categories"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        let input = match category {
            "bluetooth_identification" => {
                Some("a custom Bluetooth name (up to 32 UTF-8 bytes), or Dante device name")
            }
            "bluetooth_discovery" => Some("discoverable or not discoverable (on/off also work)"),
            "bluetooth_pairing" => Some(
                "clear forgets every remembered phone; it needs confirm_clear=true and cannot be undone",
            ),
            _ => None,
        };
        if let Some(input) = input {
            changeable.insert(category.to_owned(), json!(input));
        }
    }
    for (category, editor) in editors.as_object().into_iter().flatten() {
        if !changeable.contains_key(category) {
            changeable.insert(
                category.clone(),
                json!({"initial": editor["initial"], "choices": editor["variants"]}),
            );
        }
    }
    compact(json!({
        "panel": payload["family"], "readable": payload["readable"], "writable": payload["writable"],
        "unavailable": payload.get("write_unavailable_reason").filter(|value| !value.is_null()).unwrap_or(&payload["read_unavailable_reason"]),
        "summary": payload["presentation"]["summary"], "settings": settings,
        "changeable": (payload["writable"] != false).then_some(changeable), "analog": analog(&payload["analog"]),
        "detail": "detail=debug returns the raw panel records",
    }))
}
