use super::{ApiError, DeviceService, RequestBody, control_error, json_value, parameters, text};
use crate::devices::audio_configuration::{AudioConfiguration, AudioKind, parse_pullup_selection};
use crate::devices::operations::ControlOperation;
use axum::Router;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde_json::{Value, json};
use std::sync::Arc;

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    [
        ("/set-encoding", AudioKind::Encoding),
        ("/set-sample-rate-pullup", AudioKind::SampleRatePullup),
    ]
    .into_iter()
    .fold(router, |router, (path, kind)| {
        router.route(path, post(apply).layer(Extension(kind)))
    })
}

async fn apply(
    State(service): State<DeviceService>,
    Extension(kind): Extension<AudioKind>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let mut value = parameters
        .get(kind.parameter())
        .cloned()
        .unwrap_or(Value::Null);

    if matches!(kind, AudioKind::SampleRatePullup)
        && value.is_null()
        && let Some(selection) = parameters.get("value").filter(|value| !value.is_null())
    {
        value = match parse_pullup_selection(selection) {
            Ok(value) => value,
            Err(error) => return Ok(control_error(error)),
        };
    }

    let operation = match AudioConfiguration::new(kind, &value) {
        Ok(operation) => operation,
        Err(error) => return Ok(control_error(error)),
    };
    let completion = match service
        .registry
        .apply(device, ControlOperation::AudioConfiguration(operation))
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let mut observed = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("audio configuration completion is unavailable"))?;

    if observed["transport"] == "ddm" {
        observed["success"] = json!(true);
        return Ok(json_value(Arc::new(observed)));
    }

    if observed["state"] == "unavailable" {
        return Err(ApiError {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: format!("{} readback was unavailable", kind.label()),
        });
    }

    if observed["effective_state_confirmed"] != true {
        let mut response = json_value(Arc::new(json!({
            "error": format!("{} change was not applied", kind.label()),
            "observed": observed["current_value"],
            "supported": observed["status"]["available_values"],
        })));
        *response.status_mut() = StatusCode::CONFLICT;
        return Ok(response);
    }

    let mut result = json!({"success": true});

    if matches!(kind, AudioKind::SampleRatePullup) {
        result["raw_value"] = observed["current_value"].clone();
        result["supported"] = observed["status"]["available_values"].clone();
    }

    Ok(json_value(Arc::new(result)))
}
