use super::{ApiError, DeviceService, RequestBody, control_error, json_value, parameters, text};
use crate::devices::operations::{ControlError, ControlOperation};
use crate::devices::panels::{Change, Operation};
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde_json::{Value, json};
use std::sync::Arc;

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router.route("/device-controls", post(control))
}

async fn control(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let action = match parameters.get("action") {
        None => "inspect",
        Some(Value::String(action)) => action.as_str(),
        _ => {
            return Ok(control_error(ControlError::Conflict(
                "Action must be inspect, plan or apply.".into(),
            )));
        }
    };
    let operation = match action {
        "inspect" => Operation::Inspect,
        "plan" | "apply" => {
            let change = match Change::parse(&parameters) {
                Ok(change) => change,
                Err(error) => return Ok(control_error(error)),
            };
            if action == "plan" {
                Operation::Plan(change)
            } else {
                Operation::Apply(change)
            }
        }
        _ => {
            return Ok(control_error(ControlError::Conflict(
                "Action must be inspect, plan or apply.".into(),
            )));
        }
    };
    let completion = match service
        .registry
        .apply(device, ControlOperation::Panel(operation))
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let mut observation = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("panel operation result was unavailable"))?;
    if action == "plan" {
        observation = observation["plan"].take();
    }
    let success = action != "apply" || observation["effective_state_confirmed"] == true;
    observation["success"] = json!(success);
    let mut response = json_value(Arc::new(observation));
    if !success {
        *response.status_mut() = StatusCode::CONFLICT;
    }
    Ok(response)
}
