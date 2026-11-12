use super::{
    control_error, json_value, parameters, text, with_cause, ApiError, DeviceService, RequestBody,
};
use crate::devices::aes67::Configuration;
use crate::devices::operations::ControlOperation;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use serde_json::{json, Value};
use std::sync::Arc;

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router
        .route("/set-aes67", post(configure))
        .route("/set-aes67-multicast-prefix", post(configure_prefix))
}

async fn configure(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let configuration = match Configuration::new(parameters.get("enabled").unwrap_or(&Value::Null))
    {
        Ok(configuration) => configuration,
        Err(error) => return Ok(control_error(error)),
    };
    let completion = match service
        .registry
        .apply(
            device,
            ControlOperation::StatusConfiguration(
                crate::devices::status_configuration::Configuration::Aes67(configuration),
            ),
        )
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let readback_errors = completion.readback_errors;
    let mut observed = completion.observation.ok_or_else(|| {
        ApiError::unavailable(with_cause(
            "AES67 readback was unavailable",
            &readback_errors,
        ))
    })?;
    if observed["transport"] == "ddm" {
        observed["success"] = json!(true);
        return Ok(json_value(Arc::new(observed)));
    }
    let (status, body) = match observed["configuration_confirmed"].as_bool() {
        Some(true) => (StatusCode::OK, json!({"success": true})),
        Some(false) => (
            StatusCode::CONFLICT,
            json!({"error": "AES67 change was not applied", "observed": observed["configured"]}),
        ),
        None => (
            StatusCode::GATEWAY_TIMEOUT,
            json!({"error": with_cause("AES67 readback was unavailable", &readback_errors)}),
        ),
    };
    let mut response = json_value(Arc::new(body));
    *response.status_mut() = status;
    Ok(response)
}

async fn configure_prefix(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let configuration = match crate::devices::multicast_prefix::Configuration::new(
        parameters.get("prefix").unwrap_or(&Value::Null),
    ) {
        Ok(configuration) => configuration,
        Err(error) => return Ok(control_error(error)),
    };
    let completion = match service
        .registry
        .apply(device, ControlOperation::MulticastPrefix(configuration))
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let readback_errors = completion.readback_errors;
    let observation = completion.observation.ok_or_else(|| {
        ApiError::unavailable(with_cause(
            "multicast prefix readback unavailable",
            &readback_errors,
        ))
    })?;
    if observation["transport"] == "ddm" {
        let mut result = observation;
        result["success"] = json!(true);
        return Ok(json_value(Arc::new(result)));
    }
    let (status, body) = match observation["configuration_confirmed"].as_bool() {
        Some(true) => (
            StatusCode::OK,
            json!({"success": true, "prefix": observation["prefix"]}),
        ),
        Some(false) => (
            StatusCode::CONFLICT,
            json!({"error": "AES67 multicast prefix change was not applied", "observed": observation["prefix"]}),
        ),
        None => (
            StatusCode::GATEWAY_TIMEOUT,
            json!({"error": with_cause("AES67 multicast prefix readback was unavailable", &readback_errors)}),
        ),
    };
    let mut response = json_value(Arc::new(body));
    *response.status_mut() = status;
    Ok(response)
}
