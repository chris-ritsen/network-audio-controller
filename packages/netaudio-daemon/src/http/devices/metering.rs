use super::{ApiError, DeviceService, RequestBody, json_value, parameters, text};
use axum::Router;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{get, post};
use serde_json::{Map, Value, json};
use std::sync::Arc;

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router
        .route("/metering/cache", get(cached))
        .route("/metering/status", get(status))
        .route("/metering/snapshot/{device}", get(snapshot))
        .route("/metering/start", post(change).layer(Extension(true)))
        .route("/metering/stop", post(change).layer(Extension(false)))
}

async fn status(State(service): State<DeviceService>) -> Result<Response, ApiError> {
    Ok(json_value(Arc::new(service.metering.status()?)))
}

async fn change(
    State(service): State<DeviceService>,
    Extension(start): Extension<bool>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let record = service
        .model
        .device(text(&parameters, "device")?)?
        .ok_or_else(|| ApiError::not_found("device not found"))?;
    let name = record
        .get("server_name")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::unavailable("device has no server name"))?;
    let client = parameters
        .get("client_id")
        .map(|client| {
            client
                .as_str()
                .ok_or_else(|| ApiError::invalid("client_id must be a string"))
        })
        .transpose()?
        .unwrap_or("daemon_http");
    let result = if start {
        service.metering.start(name, client)
    } else {
        service.metering.stop(name, client)
    };
    result.map_err(|error| ApiError {
        status: StatusCode::CONFLICT,
        message: error.to_string(),
    })?;
    Ok(json_value(Arc::new(json!({"success": true}))))
}

async fn snapshot(
    State(service): State<DeviceService>,
    Path(device): Path<String>,
) -> Result<Response, ApiError> {
    let record = service
        .model
        .device(&device)?
        .ok_or_else(|| ApiError::not_found("device not found"))?;
    let name = record
        .get("server_name")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::unavailable("device has no server name"))?;
    let sample = service
        .metering
        .snapshot(name)
        .await
        .map_err(|error| ApiError {
            status: StatusCode::CONFLICT,
            message: error.to_string(),
        })?
        .ok_or_else(|| ApiError {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: "no metering data".to_owned(),
        })?;
    let mut result = json!({
        "wall_time": sample["wall_time"],
        "source_ip": sample["source_ip"],
        "source_port": sample["source_port"],
        "metering_source": sample["metering_source"],
    });

    for (direction, inventory) in [("tx", "transmitters"), ("rx", "receivers")] {
        let mut channels = Map::new();

        if let Some(levels) = sample[direction].as_object() {
            for (number, raw) in levels {
                let channel = &record["channels"][inventory][number];
                let label = channel
                    .get("friendly_name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .or_else(|| channel.get("name").and_then(Value::as_str))
                    .unwrap_or("");
                let mut reading = json!({"name": label, "level": raw});

                if let Some(state) = sample[format!("{direction}_signal_presence")].get(number) {
                    reading["signal_presence"] = state.clone();
                }

                channels.insert(number.clone(), reading);
            }
        }

        result[direction] = Value::Object(channels);
    }

    Ok(json_value(Arc::new(result)))
}

async fn cached(State(service): State<DeviceService>) -> Result<Response, ApiError> {
    Ok(json_value(Arc::new(service.registry.metering.cached()?)))
}
