use super::{ApiError, DeviceService, RequestBody, control_error, json_value, parameters, text};
use crate::devices::clock::{ClockOperation, PROFILE_PARAMETER_ERROR, parse_subdomain};
use crate::devices::operations::ControlOperation;
use axum::Router;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde_json::{Map, Value, json};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum Endpoint {
    Configuration,
    Source,
    Subdomain,
    PreferredLeader,
    Refresh,
}

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    [
        ("/set-clock-configuration", Endpoint::Configuration),
        ("/set-clock-source", Endpoint::Source),
        ("/set-clock-subdomain", Endpoint::Subdomain),
        ("/set-preferred-leader", Endpoint::PreferredLeader),
        ("/refresh-clock", Endpoint::Refresh),
    ]
    .into_iter()
    .fold(router, |router, (path, operation)| {
        router.route(path, post(apply).layer(Extension(operation)))
    })
}

async fn apply(
    State(service): State<DeviceService>,
    Extension(endpoint): Extension<Endpoint>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;

    if parameters.contains_key("record_revision") {
        return Err(ApiError::invalid(PROFILE_PARAMETER_ERROR));
    }

    let profile = parameters
        .get("control_profile")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| u16::try_from(value).ok())
                .ok_or_else(|| ApiError::invalid("control_profile must be an integer"))
        })
        .transpose()?;
    let changes = match endpoint {
        Endpoint::Configuration => parameters
            .get("changes")
            .and_then(Value::as_object)
            .cloned()
            .ok_or_else(|| ApiError::invalid("changes must be an object"))?,
        Endpoint::Source => {
            let source = parameters
                .get("clock_source")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .ok_or_else(|| {
                    ApiError::invalid("clock_source must be an integer from 0 through 65535")
                })?;
            Map::from_iter([("clock_source".to_owned(), json!(source))])
        }
        Endpoint::Subdomain => {
            let selection = text(&parameters, "subdomain")?;
            let subdomain = match parse_subdomain(selection) {
                Ok(value) => value,
                Err(error) => return Ok(control_error(error)),
            };
            Map::from_iter([("subdomain".to_owned(), subdomain)])
        }
        Endpoint::PreferredLeader => {
            let preferred = parameters
                .get("preferred")
                .and_then(Value::as_bool)
                .ok_or_else(|| ApiError::invalid("preferred must be a boolean"))?;
            Map::from_iter([("preferred_leader".to_owned(), json!(preferred))])
        }
        Endpoint::Refresh => Map::new(),
    };
    let clock = match ClockOperation::new(changes, profile) {
        Ok(clock) => clock,
        Err(error) => return Ok(control_error(error)),
    };
    let operation = if matches!(endpoint, Endpoint::Refresh) {
        ControlOperation::RefreshClock {
            profile: clock.profile,
        }
    } else {
        ControlOperation::Clock(clock)
    };
    let completion = match service.registry.apply(device, operation).await {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let mut observed = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("clock readback was unavailable"))?;
    let confirmed = observed["effective_state_confirmed"] == true;

    if matches!(endpoint, Endpoint::Configuration) {
        observed["success"] = json!(confirmed);
        let mut response = json_value(Arc::new(observed));

        if !confirmed {
            *response.status_mut() = StatusCode::CONFLICT;
        }

        return Ok(response);
    }

    let status = match observed["status"].as_object() {
        Some(status) => status,
        None if observed["transport"] == "ddm" => {
            observed["error"] = json!("Managed clock readback was unavailable");
            return Ok(control_error(
                crate::devices::operations::ControlError::Managed {
                    status: 504,
                    result: observed,
                },
            ));
        }
        None => {
            return Err(ApiError {
                status: StatusCode::GATEWAY_TIMEOUT,
                message: "clock readback was unavailable".to_owned(),
            });
        }
    };

    if matches!(endpoint, Endpoint::Refresh) {
        let mut result = status.clone();
        result.insert("success".to_owned(), json!(true));
        return Ok(json_value(Arc::new(Value::Object(result))));
    }

    let (field, label) = match endpoint {
        Endpoint::Source => ("clock_source_code", "clock source"),
        Endpoint::Subdomain => ("clock_subdomain", "clock subdomain"),
        Endpoint::PreferredLeader => ("preferred_leader", "preferred leader"),
        _ => unreachable!(),
    };
    let value = status
        .get(field)
        .filter(|value| !value.is_null())
        .ok_or_else(|| ApiError {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: format!("{label} readback was unavailable"),
        })?;
    let value = if matches!(endpoint, Endpoint::Subdomain) {
        json!(netaudio_core::clock_configuration::subdomain_presentation(value).label)
    } else {
        value.clone()
    };

    if !confirmed {
        let mut result = json!({
            "error": format!("{label} change was not applied"),
            "observed": value,
        });
        if observed["transport"] == "ddm" {
            result["managed"] = crate::managed::control::evidence(&observed);
        }
        let mut response = json_value(Arc::new(result));
        *response.status_mut() = StatusCode::CONFLICT;
        return Ok(response);
    }

    let mut result = json!({"success": true});
    if observed["transport"] == "ddm" {
        result["managed"] = crate::managed::control::evidence(&observed);
    }

    match endpoint {
        Endpoint::Source => result["clock_source"] = value,
        Endpoint::Subdomain => result["subdomain"] = value,
        _ => {}
    }

    Ok(json_value(Arc::new(result)))
}
