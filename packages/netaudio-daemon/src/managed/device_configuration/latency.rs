use crate::devices::operations::ControlError;
use serde_json::{Value, json};

pub(super) fn command(milliseconds: f64, record: &Value) -> Result<Option<Value>, ControlError> {
    netaudio_core::latency_configuration::requested_nanoseconds(milliseconds)
        .map_err(|error| ControlError::Invalid(error.to_string()))?;
    if record["ddm_fresh"] != true || record["ddm_connection_state"] != "READY" {
        return Err(ControlError::ObservationUnavailable(
            "The selected managed device is not currently ready".into(),
        ));
    }
    match record["is_locked"].as_bool() {
        Some(false) => {}
        Some(true) => return Err(ControlError::Conflict("device is locked".into())),
        None => {
            return Err(ControlError::ObservationUnavailable(
                "Device lock state is unknown; no latency command was sent".into(),
            ));
        }
    }
    Ok(Some(
        json!({"command": "set_latency", "latency": milliseconds}),
    ))
}

pub(super) fn completion(
    milliseconds: f64,
    after: Option<&Value>,
    acknowledged: Option<bool>,
) -> Result<Value, ControlError> {
    let settings = after.map(|record| record["latency_settings"].clone());
    let request: netaudio_core::latency_configuration::LatencyControl =
        serde_json::from_value(json!({
            "requested_milliseconds": milliseconds,
            "settings": settings,
            "acknowledged": acknowledged,
        }))
        .map_err(anyhow::Error::from)?;
    let mut result = serde_json::to_value(
        request
            .resolve()
            .map_err(|error| ControlError::Invalid(error.to_string()))?,
    )
    .map_err(anyhow::Error::from)?;
    result["latency_state"] = result["state"].take();
    result["configuration_confirmed"] = json!(
        result["configured_latency_ns"]
            .as_u64()
            .map(|configured| Some(configured) == result["requested_latency_ns"].as_u64())
    );
    if acknowledged.is_none() || result["configured_latency_ns"].is_null() {
        result["effective_state_confirmed"] = Value::Null;
    }
    result["persistence_confirmed"] = Value::Null;
    Ok(result)
}
