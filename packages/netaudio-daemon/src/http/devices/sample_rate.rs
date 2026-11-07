use super::{control_error, parameters, text, ApiError, DeviceService, RequestBody};
use crate::devices::operations::ControlOperation;
use crate::devices::sample_rate::{response_status, Operation};
use crate::http::inventory::json_value;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use serde_json::json;
use std::sync::Arc;

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router.route("/set-sample-rate", post(set_sample_rate))
}

async fn set_sample_rate(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let reference = text(&parameters, "device")?;
    let operation = match Operation::new(&json!(parameters)) {
        Ok(operation) => operation,
        Err(error) => return Ok(control_error(error)),
    };
    let completion = match service
        .registry
        .apply(reference, ControlOperation::SampleRate(operation))
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let result = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("sample-rate completion unavailable"))?;
    let status =
        StatusCode::from_u16(response_status(&result)).expect("valid sample-rate response status");
    let mut response = json_value(Arc::new(result));
    *response.status_mut() = status;
    Ok(response)
}
