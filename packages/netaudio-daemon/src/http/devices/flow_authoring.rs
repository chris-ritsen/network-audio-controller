use super::{ApiError, DeviceService, RequestBody, control_error, json_value, parameters, text};
use crate::devices::flow_authoring::Operation;
use crate::devices::operations::ControlOperation;
use axum::Router;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum RequestKind {
    Plan,
    Create,
    Delete,
}

pub(super) fn routes(mut router: Router<DeviceService>) -> Router<DeviceService> {
    for (path, kind) in [
        ("/transmit-flows/plan", RequestKind::Plan),
        ("/transmit-flows/create", RequestKind::Create),
        ("/transmit-flows/delete", RequestKind::Delete),
    ] {
        router = router.route(path, post(author).layer(Extension(kind)));
    }

    router
}

async fn author(
    State(service): State<DeviceService>,
    Extension(kind): Extension<RequestKind>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;

    if !matches!(kind, RequestKind::Plan) && parameters.get("confirmed") != Some(&json!(true)) {
        return Err(ApiError::invalid("confirmed must be true"));
    }

    let device = text(&parameters, "device")?;
    let operation = match kind {
        RequestKind::Plan | RequestKind::Create => Operation::create(
            parameters.get("specification").unwrap_or(&Value::Null),
            matches!(kind, RequestKind::Plan),
        ),
        RequestKind::Delete => Operation::delete(parameters.get("flow_id").unwrap_or(&Value::Null)),
    };
    let operation = match operation {
        Ok(operation) => operation,
        Err(error) => return Ok(control_error(error)),
    };
    let execution = if matches!(kind, RequestKind::Plan) {
        service
            .registry
            .plan_transmit_flow(device, &operation)
            .await
    } else {
        service
            .registry
            .apply(device, ControlOperation::FlowAuthoring(operation))
            .await
    };
    let completion = match execution {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let mut observation = completion.observation.ok_or_else(|| {
        ApiError::unavailable("flow result unavailable; do not replay the command")
    })?;
    let status = if matches!(kind, RequestKind::Plan) {
        StatusCode::OK
    } else {
        match observation["state"].as_str() {
            Some("confirmed" | "deleted") => StatusCode::OK,
            Some("partial" | "pending") => StatusCode::ACCEPTED,
            Some("inconsistent") => StatusCode::BAD_GATEWAY,
            Some("rejected" | "unsupported") => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    };

    if status.is_client_error() || status.is_server_error() {
        observation["error"] = observation["message"].clone();
    }

    let mut response = json_value(Arc::new(observation));
    *response.status_mut() = status;
    Ok(response)
}
