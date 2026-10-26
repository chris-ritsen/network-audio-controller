use super::{ApiError, DeviceService, RequestBody, control_error, json_value, parameters, text};
use crate::devices::network::Change;
use crate::devices::network_configuration::Configuration;
use crate::devices::operations::{ControlError, ControlOperation};
use crate::devices::redundancy;
use axum::Router;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum RequestKind {
    Interface,
    Redundancy,
}

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router
        .route(
            "/interface",
            post(configure).layer(Extension(RequestKind::Interface)),
        )
        .route(
            "/redundancy",
            post(configure).layer(Extension(RequestKind::Redundancy)),
        )
}

async fn configure(
    State(service): State<DeviceService>,
    Extension(kind): Extension<RequestKind>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let configuration = match kind {
        RequestKind::Redundancy => {
            redundancy::Configuration::new(parameters.get("mode").unwrap_or(&Value::Null))
                .map(Change::Redundancy)
        }
        RequestKind::Interface => {
            Configuration::new(&Value::Object(parameters.clone())).map(Change::Interface)
        }
    };
    let configuration = match configuration {
        Ok(configuration) => configuration,
        Err(error) => return Ok(network_error(error)),
    };
    let completion = match service
        .registry
        .apply(
            device,
            ControlOperation::NetworkConfiguration(configuration),
        )
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(network_error(error)),
    };
    let mut observation = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("network readback was unavailable"))?;

    if observation["transport"] == "ddm" {
        observation["success"] = json!(true);
        return Ok(json_value(Arc::new(observation)));
    }

    if observation["configuration_confirmed"] == true {
        if matches!(kind, RequestKind::Redundancy) {
            return Ok(json_value(Arc::new(json!({
                "success": true,
                "redundancy": observation["effective_readback"],
                "mutation": observation,
            }))));
        }

        let mut network = observation["network"].clone();
        network
            .as_object_mut()
            .expect("network snapshot object")
            .remove("device");
        network["success"] = json!(true);
        return Ok(json_value(Arc::new(network)));
    }

    let unavailable = match kind {
        RequestKind::Interface => "Interface change could not be verified",
        RequestKind::Redundancy => "Dante redundancy change could not be verified",
    };
    let mut response = json_value(Arc::new(json!({
        "error": observation["verification_error"].as_str().unwrap_or(unavailable),
        "mutation": observation,
    })));
    *response.status_mut() = StatusCode::BAD_GATEWAY;
    Ok(response)
}

fn network_error(error: ControlError) -> Response {
    let observation_unavailable = matches!(error, ControlError::ObservationUnavailable(_));
    let mut response = control_error(error);

    if observation_unavailable {
        *response.status_mut() = StatusCode::CONFLICT;
    }

    response
}
