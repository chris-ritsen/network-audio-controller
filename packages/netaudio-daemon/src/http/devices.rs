use super::inventory::json_value;
use super::{ApiError, RequestBody};
use crate::devices::operations::{ChannelDirection, ControlError, ControlOperation};
use crate::devices::performance::PerformanceOperation;
use crate::devices::registry::DeviceRegistry;
use crate::devices::subscriptions::SubscriptionRequest;
use crate::devices::subscriptions::batch::BatchRequest;
use crate::devices::transport::TransportError;
use crate::state::ReadModel;
use axum::Router;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{delete, get, post};
use futures_util::{StreamExt, stream};
use serde_json::{Map, Value, json};
use std::sync::Arc;

const REFRESH_CONCURRENCY: usize = 16;

mod aes67;
mod analog;
mod audio;
mod clock;
mod diagnostics;
mod external_subscription;
mod flow_authoring;
mod forgetting;
mod inspection;
mod locking;
mod maintenance;
mod metering;
mod network_configuration;
mod panels;
mod sample_rate;

#[derive(Clone)]
struct DeviceService {
    registry: Arc<DeviceRegistry>,
    model: Arc<ReadModel>,
    metering: Arc<crate::monitoring::metering_streams::MeteringStreams>,
}

pub fn router(
    registry: Arc<DeviceRegistry>,
    model: Arc<ReadModel>,
    metering: Arc<crate::monitoring::metering_streams::MeteringStreams>,
) -> Router {
    let mut router = Router::new()
        .route("/devices", delete(forgetting::devices))
        .route("/devices/{device}", delete(forgetting::device))
        .route("/rename-device", post(rename_device))
        .route("/rename-channel", post(rename_channel))
        .route("/set-latency", post(set_latency))
        .route("/identify", post(identify))
        .route("/transmit-flows/{device}", get(transmit_flows))
        .route("/subscribe", post(subscribe))
        .route(
            "/external-flows/subscribe",
            post(external_subscription::subscribe),
        )
        .route("/subscriptions/apply", post(apply_routes))
        .route("/unsubscribe", post(unsubscribe))
        .route("/refresh", post(refresh));

    for (path, operation) in [
        ("/set-receive-flow-performance", "receive_flow_performance"),
        (
            "/set-transmit-flow-performance",
            "transmit_flow_performance",
        ),
        ("/set-unicast-performance", "unicast_performance"),
        (
            "/set-receive-flow-default-slots",
            "receive_flow_default_slots",
        ),
        (
            "/store-current-configuration",
            "store_current_configuration",
        ),
    ] {
        router = router.route(path, post(performance).layer(Extension(operation)));
    }

    let router = clock::routes(router);
    let router = audio::routes(router);
    let router = diagnostics::routes(router);
    let router = metering::routes(router);
    let router = inspection::routes(router);
    let router = locking::routes(router);
    let router = maintenance::routes(router);
    let router = aes67::routes(router);
    let router = analog::routes(router);
    let router = flow_authoring::routes(router);
    let router = sample_rate::routes(router);
    let router = panels::routes(router);
    network_configuration::routes(router).with_state(DeviceService {
        registry,
        model,
        metering,
    })
}

async fn transmit_flows(
    State(service): State<DeviceService>,
    Path(device): Path<String>,
) -> Result<Response, ApiError> {
    match service
        .registry
        .apply(&device, ControlOperation::InspectTransmitFlows)
        .await
    {
        Ok(completion) => {
            let inventory = completion.observation.ok_or_else(|| {
                ApiError::unavailable("fresh transmitter flow readback was unavailable")
            })?;
            Ok(json_value(Arc::new(inventory)))
        }
        Err(error) => Ok(control_error(error)),
    }
}

async fn performance(
    State(service): State<DeviceService>,
    Extension(operation): Extension<&'static str>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let mut requested = json!({"operation": operation});

    for field in ["latency_microseconds", "frames_per_packet", "default_slots"] {
        if let Some(value) = parameters.get(field) {
            requested[field] = value.clone();
        }
    }

    let operation: PerformanceOperation =
        serde_json::from_value(requested).map_err(|error| ApiError::invalid(error.to_string()))?;
    let completion = match service
        .registry
        .apply(device, ControlOperation::Performance(operation))
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let result = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("performance completion is unavailable"))?;
    let status = match result["state"].as_str() {
        Some("confirmed") => StatusCode::OK,
        Some("request_acknowledged") => StatusCode::ACCEPTED,
        Some("contradicted" | "rejected") => StatusCode::CONFLICT,
        Some("unverified") => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut response = json_value(Arc::new(result));
    *response.status_mut() = status;
    Ok(response)
}

async fn rename_channel(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let name = text(&parameters, "name")?;
    let direction: ChannelDirection = serde_json::from_value(
        parameters
            .get("channel_type")
            .cloned()
            .unwrap_or(Value::Null),
    )
    .map_err(|_| ApiError::invalid("channel_type must be \"rx\" or \"tx\""))?;
    let number = parameters
        .get("channel_number")
        .and_then(Value::as_u64)
        .and_then(|number| u16::try_from(number).ok())
        .filter(|number| *number > 0)
        .ok_or_else(|| {
            ApiError::invalid("channel_number must be an integer from 1 through 65535")
        })?;
    let result = service
        .registry
        .apply(
            device,
            ControlOperation::RenameChannel {
                direction,
                number,
                name: name.to_owned(),
            },
        )
        .await;
    Ok(completion(result, None))
}

async fn set_latency(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let milliseconds = parameters
        .get("latency")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or_else(|| {
            ApiError::invalid("latency must be a finite, nonnegative number of milliseconds")
        })?;
    let completion = match service
        .registry
        .apply(device, ControlOperation::SetLatency { milliseconds })
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let observed = completion.observation.ok_or_else(|| {
        ApiError::unavailable("latency readback was unavailable; refresh before retrying")
    })?;
    let body = if observed["effective_state_confirmed"] == true {
        if observed["control_transport"] == "controller" {
            let mut result = observed;
            result["success"] = json!(true);
            result
        } else {
            json!({"success": true})
        }
    } else if observed["state"] == "unavailable" {
        return Err(ApiError {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: "latency readback was unavailable; refresh before retrying".to_owned(),
        });
    } else {
        let mut response = json_value(Arc::new(json!({
            "error": "latency change was not applied",
            "configured_latency_ms": observed["configured_latency_ms"],
        })));
        *response.status_mut() = StatusCode::CONFLICT;
        return Ok(response);
    };
    Ok(json_value(Arc::new(body)))
}

async fn identify(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;

    let completion = match service
        .registry
        .apply(device, ControlOperation::Identify)
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };

    let body = completion
        .observation
        .filter(|value| value["transport"] == "ddm")
        .unwrap_or_else(|| json!({"accepted": true, "verified": false}));
    let mut response = json_value(Arc::new(body));
    *response.status_mut() = StatusCode::ACCEPTED;
    Ok(response)
}

async fn rename_device(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let name = text(&parameters, "name")?;
    Ok(completion(
        service
            .registry
            .apply(
                device,
                ControlOperation::RenameDevice {
                    name: name.to_owned(),
                },
            )
            .await,
        None,
    ))
}

async fn subscribe(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = Value::Object(parameters(&body, false)?);
    Ok(apply_subscription(SubscriptionRequest::set(&parameters), &service.registry).await)
}

async fn apply_routes(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let request = match BatchRequest::parse(parameters.get("routes").unwrap_or(&Value::Null)) {
        Ok(request) => request,
        Err(error) => return Ok(control_error(error)),
    };
    let result = match request.apply(&service.registry, &service.model).await {
        Ok(result) => result,
        Err(error) => return Ok(control_error(error)),
    };
    let status = if result["applied"].as_u64().is_some_and(|count| count > 0) {
        StatusCode::OK
    } else {
        StatusCode::CONFLICT
    };
    let mut response = json_value(Arc::new(result));
    *response.status_mut() = status;
    Ok(response)
}

async fn unsubscribe(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = Value::Object(parameters(&body, false)?);
    Ok(apply_subscription(SubscriptionRequest::clear(&parameters), &service.registry).await)
}

async fn apply_subscription(
    request: Result<SubscriptionRequest, ControlError>,
    registry: &Arc<DeviceRegistry>,
) -> Response {
    let result = match request {
        Ok(request) => request.apply(registry).await,
        Err(error) => Err(error),
    };

    match result {
        Ok((applied, count)) => completion(Ok(applied), count),
        Err(error) => control_error(error),
    }
}

async fn refresh(
    State(service): State<DeviceService>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, true)?;
    let names = match parameters.get("device") {
        None | Some(Value::Null) => service.model.records()?.keys().cloned().collect::<Vec<_>>(),
        Some(Value::String(name)) if !name.is_empty() => {
            if service.model.device(name)?.is_none() {
                return Err(ApiError::not_found("device not found"));
            }

            vec![name.clone()]
        }
        Some(_) => return Err(ApiError::invalid("device must be a string")),
    };
    let mut results = stream::iter(names.into_iter().map(|name| {
        let registry = Arc::clone(&service.registry);
        async move { registry.refresh(&name).await }
    }))
    .buffer_unordered(REFRESH_CONCURRENCY);

    while let Some(result) = results.next().await {
        result?;
    }

    Ok(json_value(Arc::new(json!({"success": true}))))
}

fn parameters(body: &[u8], optional: bool) -> Result<Map<String, Value>, ApiError> {
    if body.is_empty() {
        return if optional {
            Ok(Map::new())
        } else {
            Err(ApiError::invalid("missing body"))
        };
    }

    let value: Value = serde_json::from_slice(body)
        .map_err(|error| ApiError::invalid(format!("invalid json: {error}")))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| ApiError::invalid("body must be a json object"))
}

fn text<'a>(parameters: &'a Map<String, Value>, field: &str) -> Result<&'a str, ApiError> {
    parameters
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::invalid(format!("{field} must be a string")))
}

fn completion(
    result: Result<crate::devices::operations::ControlCompletion, ControlError>,
    count: Option<usize>,
) -> Response {
    match result {
        Ok(completion) => {
            let mut result = json!({"success": true});

            if let Some(observed) = completion.observation
                && observed["transport"] == "ddm"
            {
                result.as_object_mut().expect("completion object").extend(
                    observed
                        .as_object()
                        .expect("managed outcome object")
                        .clone(),
                );
            }

            if let Some(count) = count {
                result["count"] = json!(count);
            }

            json_value(Arc::new(result))
        }
        Err(error) => control_error(error),
    }
}

pub fn control_failure(error: &ControlError) -> (StatusCode, Value) {
    let mut body = json!({"error": error.to_string()});
    let status = match error {
        ControlError::Managed { status, result } => {
            return (
                StatusCode::from_u16(*status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                result.clone(),
            );
        }
        ControlError::Invalid(_) => StatusCode::BAD_REQUEST,
        ControlError::NotFound(_) => StatusCode::NOT_FOUND,
        ControlError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        ControlError::ObservationUnavailable(_) => StatusCode::GATEWAY_TIMEOUT,
        ControlError::Subscription(failure) => {
            let (status, mut body) = control_failure(&failure.cause);
            body["acknowledged_receiver_channels"] = json!(failure.acknowledged_receivers);
            body["attempted_receiver_channels"] = json!(failure.attempted_receivers);
            body["readback_completed"] = json!(failure.readback_completed);
            return (status, body);
        }
        ControlError::Conflict(_) => StatusCode::CONFLICT,
        ControlError::Rejected {
            result_code,
            acknowledged_commands,
        } => {
            body["result_code"] = json!(result_code);
            body["acknowledged_commands"] = json!(acknowledged_commands);
            StatusCode::CONFLICT
        }
        ControlError::Transport {
            error,
            acknowledged_commands,
        } => {
            body["acknowledged_commands"] = json!(acknowledged_commands);

            match error {
                TransportError::InvalidCommand(_) => StatusCode::BAD_REQUEST,
                TransportError::ReadTimeout => StatusCode::GATEWAY_TIMEOUT,
                TransportError::WriteOutcomeUnknown(_) => {
                    body["outcome_unknown"] = json!(true);
                    StatusCode::GATEWAY_TIMEOUT
                }
                TransportError::Network(_) => StatusCode::SERVICE_UNAVAILABLE,
                TransportError::MalformedResponse(_) => StatusCode::BAD_GATEWAY,
            }
        }
        ControlError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, body)
}

pub fn control_error(error: ControlError) -> Response {
    let (status, body) = control_failure(&error);
    let mut response = json_value(Arc::new(body));
    *response.status_mut() = status;
    response
}
