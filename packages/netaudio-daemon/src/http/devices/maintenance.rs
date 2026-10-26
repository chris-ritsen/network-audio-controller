use super::{ApiError, DeviceService, RequestBody, control_error, parameters, text};
use crate::devices::maintenance::{Operation, response_status};
use crate::devices::operations::ControlOperation;
use crate::http::inventory::json_value;
use axum::Router;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde_json::json;
use std::sync::Arc;

pub(super) fn routes(mut router: Router<DeviceService>) -> Router<DeviceService> {
    for (path, operation) in [
        ("/reboot", "reboot"),
        ("/factory-reset", "factory_reset"),
        ("/clear-configuration", "clear_configuration"),
    ] {
        router = router.route(path, post(maintain).layer(Extension(operation)));
    }

    router
}

async fn maintain(
    State(service): State<DeviceService>,
    Extension(operation): Extension<&'static str>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let reference = text(&parameters, "device")?;
    let operation = match Operation::new(operation, &json!(parameters)) {
        Ok(operation) => operation,
        Err(error) => return Ok(control_error(error)),
    };
    let completion = match service
        .registry
        .apply(reference, ControlOperation::Maintenance(operation))
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let result = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("maintenance result unavailable"))?;
    let status =
        StatusCode::from_u16(response_status(&result)).expect("valid maintenance response status");
    let mut response = json_value(Arc::new(result));
    *response.status_mut() = status;
    Ok(response)
}
