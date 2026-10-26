use super::{ApiError, DeviceService, RequestBody, control_error, parameters, text};
use crate::devices::external_subscription::{ExternalSubscription, Outcome, response};
use crate::devices::operations::ControlOperation;
use crate::http::inventory::json_value;
use axum::extract::State;
use axum::response::Response;
use serde_json::Value;
use std::sync::Arc;

pub(super) async fn subscribe(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = Value::Object(parameters(&body, false)?);
    let reference = text(
        parameters.as_object().expect("parameters are an object"),
        "rx_device",
    )?;
    let operation = match ExternalSubscription::prepare(&parameters, &service.model) {
        Ok(operation) => operation,
        Err(error) => return Ok(control_error(error)),
    };
    let completion = match service
        .registry
        .apply(reference, ControlOperation::ExternalSubscription(operation))
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let result = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("external subscription completion is unavailable"))?;
    let (status, result) = response(result);
    let mut response = json_value(Arc::new(result));
    *response.status_mut() = match status {
        Outcome::Confirmed => axum::http::StatusCode::OK,
        Outcome::Pending => axum::http::StatusCode::ACCEPTED,
        Outcome::Rejected => axum::http::StatusCode::CONFLICT,
        Outcome::Contradicted => axum::http::StatusCode::BAD_GATEWAY,
        Outcome::Unavailable => axum::http::StatusCode::GATEWAY_TIMEOUT,
    };
    Ok(response)
}
