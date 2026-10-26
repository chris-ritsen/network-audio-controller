use super::compact;
use serde_json::{Map, Value, json};

pub fn domains(payload: Vec<Value>, arguments: &Value) -> Value {
    if arguments["detail"] == "debug" {
        return json!(payload);
    }
    let domains: Vec<_> = payload.iter().map(|domain| {
        let devices: Vec<_> = domain["devices"].as_array().into_iter().flatten()
            .filter(|device| device.is_object())
            .map(|device| compact(json!({
                "name": device["name"], "product": device["identity"]["product_model_name"],
                "enrollment": device["enrolment_state"], "connection": device["connection"]["state"],
                "status": device["status"]["summary"], "problems": problems(&device["status"]),
            }))).collect();
        compact(json!({
            "name": domain["name"], "id": domain["id"], "server": domain["ddm_server_profile"],
            "context": domain["ddm_context"], "status": domain["status"]["summary"],
            "problems": problems(&domain["status"]), "devices": devices,
            "fresh": domain["ddm_fresh"],
        }))
    }).collect();
    json!({"domains": domains})
}

fn problems(status: &Value) -> Value {
    let Some(status) = status.as_object() else {
        return Value::Null;
    };
    let mut problems: Map<_, _> = status
        .iter()
        .filter(|(key, value)| {
            !matches!(key.as_str(), "id" | "summary" | "alert_message") && *value != "OK"
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if let Some(alert) = status.get("alert_message").and_then(Value::as_object) {
        problems.extend(
            alert
                .iter()
                .filter(|(key, value)| key.as_str() != "id" && super::truthy(value))
                .map(|(key, value)| (format!("{key}_alert"), value.clone())),
        );
    }
    if problems.is_empty() {
        Value::Null
    } else {
        Value::Object(problems)
    }
}
