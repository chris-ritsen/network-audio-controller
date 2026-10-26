use super::{ApiError, DeviceService, RequestBody, control_error, json_value, parameters, text};
use crate::devices::analog::Configuration;
use crate::devices::operations::{ControlError, ControlOperation};
use crate::devices::status_configuration;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde_json::{Value, json};
use std::sync::Arc;

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router.route("/set-gain", post(configure))
}

async fn configure(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let field = |name| parameters.get(name).unwrap_or(&Value::Null);
    let configuration = match Configuration::new(
        field("channel_number"),
        field("gain_level"),
        field("device_type"),
    ) {
        Ok(configuration) => configuration,
        Err(error) => return Ok(control_error(ControlError::Conflict(error.to_string()))),
    };
    let channel = configuration.channel;
    let completion = match service
        .registry
        .apply(
            device,
            ControlOperation::StatusConfiguration(status_configuration::Configuration::Analog(
                configuration,
            )),
        )
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let mut observation = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("gain readback was unavailable"))?;
    if observation["transport"] == "ddm" {
        observation["success"] = json!(true);
        return Ok(json_value(Arc::new(observation)));
    }
    let (status, result) = match observation["effective_state_confirmed"].as_bool() {
        Some(true) => (StatusCode::OK, json!({"success": true})),
        Some(false) => (
            StatusCode::CONFLICT,
            json!({
                "error": "gain change was not applied",
                "observed_device_type": observation["status"]["device_type"],
                "observed_level": observation["status"]["channel_levels"].get(usize::from(channel - 1)),
            }),
        ),
        None => (
            StatusCode::GATEWAY_TIMEOUT,
            json!({"error": "gain readback was unavailable"}),
        ),
    };
    let mut response = json_value(Arc::new(result));
    *response.status_mut() = status;
    Ok(response)
}
