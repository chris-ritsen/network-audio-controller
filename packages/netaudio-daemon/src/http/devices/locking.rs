use super::{ApiError, DeviceService, RequestBody, control_error, parameters, text};
use crate::devices::locking::{Operation, response_status};
use crate::devices::operations::ControlOperation;
use crate::http::inventory::json_value;
use axum::Router;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde_json::json;
use std::sync::Arc;

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router
        .route("/lock", post(configure).layer(Extension(true)))
        .route("/unlock", post(configure).layer(Extension(false)))
        .route("/device-lock-key", post(export_key))
}

async fn export_key(State(service): State<DeviceService>) -> Result<Response, ApiError> {
    Ok(match service.registry.lock_key() {
        Ok(key) => json_value(Arc::new(json!({"key": key}))),
        Err(error) => control_error(error),
    })
}

async fn configure(
    State(service): State<DeviceService>,
    Extension(locking): Extension<bool>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let reference = text(&parameters, "device")?;
    let operation = match Operation::new(&json!(parameters), locking) {
        Ok(operation) => operation,
        Err(error) => return Ok(control_error(error)),
    };
    let completion = match service
        .registry
        .apply(reference, ControlOperation::Locking(operation))
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let result = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("lock operation result unavailable"))?;
    let status =
        StatusCode::from_u16(response_status(&result)).expect("valid lock response status");
    let mut response = json_value(Arc::new(result));
    *response.status_mut() = status;
    Ok(response)
}
