use super::{ApiError, MAXIMUM_REQUEST_BYTES, RequestBody, inventory::json_value};
use crate::host_audio::{manager::Manager, provider::Outcome};
use axum::{
    Router,
    extract::{DefaultBodyLimit, OriginalUri, RawQuery, State},
    http::StatusCode,
    response::Response,
    routing::{get, post},
};
use netaudio_core::host_audio::control::Command;
use serde_json::{Value, json};
use std::sync::Arc;

pub fn signal_router(service: Option<Arc<crate::host_audio::signal::Service>>) -> Router {
    Router::new()
        .route("/host-audio/trace", get(trace_query).post(trace))
        .route("/host-audio/levels", post(point_levels))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
        .with_state(service)
}

fn signal_service(
    service: &Option<Arc<crate::host_audio::signal::Service>>,
) -> Result<&Arc<crate::host_audio::signal::Service>, ApiError> {
    service.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: "Host audio support is not running".to_owned(),
    })
}

fn signal_error(error: anyhow::Error) -> ApiError {
    if error.is::<tokio::sync::TryAcquireError>() {
        ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: error.to_string(),
        }
    } else {
        ApiError::from(error)
    }
}

async fn traced(
    service: &Option<Arc<crate::host_audio::signal::Service>>,
    request: crate::host_audio::signal::TraceRequest,
) -> Result<Response, ApiError> {
    request
        .validate()
        .map_err(|error| ApiError::invalid(error.to_string()))?;
    let service = signal_service(service)?;
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), service.trace(request))
        .await
        .map_err(|_| ApiError {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: "Signal trace exceeded its request deadline".to_owned(),
        })?
        .map_err(signal_error)?;
    response(outcome)
}

async fn trace(
    State(service): State<Option<Arc<crate::host_audio::signal::Service>>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request =
        serde_json::from_slice(&body).map_err(|error| ApiError::invalid(error.to_string()))?;
    traced(&service, request).await
}

async fn trace_query(
    State(service): State<Option<Arc<crate::host_audio::signal::Service>>>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let mut arguments = serde_json::Map::new();
    for (name, value) in
        url::form_urlencoded::parse(query.as_deref().unwrap_or_default().as_bytes())
    {
        if arguments.contains_key(name.as_ref()) {
            return Err(ApiError::invalid("Duplicate signal trace parameter"));
        }
        let value = match name.as_ref() {
            "levels" => match value.as_ref() {
                "0" | "false" | "no" => json!(false),
                "1" | "true" | "yes" => json!(true),
                _ => return Err(ApiError::invalid("levels must be true or false")),
            },
            "seconds" => json!(
                value
                    .parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| ApiError::invalid("seconds must be a finite number"))?
            ),
            _ => json!(value),
        };
        arguments.insert(name.into_owned(), value);
    }
    let request = serde_json::from_value(Value::Object(arguments))
        .map_err(|error| ApiError::invalid(error.to_string()))?;
    traced(&service, request).await
}

async fn point_levels(
    State(service): State<Option<Arc<crate::host_audio::signal::Service>>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request: crate::host_audio::signal::PointsRequest =
        serde_json::from_slice(&body).map_err(|error| ApiError::invalid(error.to_string()))?;
    request
        .validate()
        .map_err(|error| ApiError::invalid(error.to_string()))?;
    let service = signal_service(&service)?;
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        service.points(request, None),
    )
    .await
    .map_err(|_| ApiError {
        status: StatusCode::GATEWAY_TIMEOUT,
        message: "Signal measurement exceeded its request deadline".to_owned(),
    })?
    .map_err(signal_error)?;
    response(outcome)
}

pub fn router(manager: Option<Arc<Manager>>) -> Router {
    Router::new()
        .route("/host-audio", get(snapshot))
        .route("/host-audio/cards", post(card))
        .route("/host-audio/meter", post(measure))
        .route("/host-audio/history", post(history))
        .route("/host-audio/recordings", post(recordings))
        .route("/host-audio/jack/connect", post(execute))
        .route("/host-audio/jack/disconnect", post(execute))
        .route("/host-audio/pulse/volume", post(execute))
        .route("/host-audio/pulse/mute", post(execute))
        .route("/host-audio/pulse/default", post(execute))
        .route("/host-audio/pulse/move", post(execute))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
        .with_state(manager)
}

fn required(manager: &Option<Arc<Manager>>) -> Result<&Arc<Manager>, ApiError> {
    manager.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: "Host audio support is not running".to_owned(),
    })
}

fn response(outcome: Outcome) -> Result<Response, ApiError> {
    let mut response = json_value(Arc::new(outcome.body));
    *response.status_mut() = StatusCode::from_u16(outcome.status).map_err(anyhow::Error::from)?;
    Ok(response)
}

async fn snapshot(
    State(manager): State<Option<Arc<Manager>>>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let host = query.as_ref().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == "host")
            .map(|(_, host)| host.into_owned())
    });
    response(required(&manager)?.read(host.as_deref()).await?)
}

pub fn command(path: &str, mut body: Value) -> Result<Command, ApiError> {
    let object = body
        .as_object_mut()
        .ok_or_else(|| ApiError::invalid("Host audio command must be a JSON object"))?;
    let action = match path {
        "/host-audio/jack/connect" => {
            object.insert("connect".to_owned(), json!(true));
            "jack_connection"
        }
        "/host-audio/jack/disconnect" => {
            object.insert("connect".to_owned(), json!(false));
            "jack_connection"
        }
        "/host-audio/pulse/volume" => "pulse_volume",
        "/host-audio/pulse/mute" => "pulse_mute",
        "/host-audio/pulse/default" => "pulse_default",
        "/host-audio/pulse/move" => "pulse_move",
        _ => return Err(ApiError::invalid("Unknown host audio command")),
    };
    if object.contains_key("action") {
        return Err(ApiError::invalid(
            "The request path selects the host audio action",
        ));
    }
    for field in ["target", "stream", "destination"] {
        if let Some(value) = object.get_mut(field).filter(|value| value.is_u64()) {
            *value = json!(value.as_u64().expect("checked integer").to_string());
        }
    }
    object.insert("action".to_owned(), json!(action));
    serde_json::from_value(body).map_err(|error| ApiError::invalid(error.to_string()))
}

async fn execute(
    State(manager): State<Option<Arc<Manager>>>,
    OriginalUri(uri): OriginalUri,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let body: Value = serde_json::from_slice(&body)
        .map_err(|_| ApiError::invalid("Host audio command must be JSON"))?;
    let command = command(uri.path(), body)?;
    response(required(&manager)?.execute(command, None).await?)
}

async fn card(
    State(manager): State<Option<Arc<Manager>>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let command =
        serde_json::from_slice(&body).map_err(|error| ApiError::invalid(error.to_string()))?;
    response(required(&manager)?.execute_card(command, None).await?)
}

async fn measure(
    State(manager): State<Option<Arc<Manager>>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request =
        serde_json::from_slice(&body).map_err(|error| ApiError::invalid(error.to_string()))?;
    response(required(&manager)?.measure(request).await?)
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "HostHistoryRequest")]
pub(crate) struct HistoryRequest {
    targets: std::collections::BTreeMap<String, Vec<[String; 2]>>,
    start: f64,
    end: f64,
}

#[derive(serde::Serialize, schemars::JsonSchema)]
#[schemars(rename = "HostHistorySummary")]
pub(crate) struct HistoryReading {
    #[serde(flatten)]
    summary: crate::monitoring::level_history::Summary,
    #[serde(skip_serializing_if = "Option::is_none")]
    not_recording: Option<String>,
}

async fn history(
    State(manager): State<Option<Arc<Manager>>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request: HistoryRequest =
        serde_json::from_slice(&body).map_err(|error| ApiError::invalid(error.to_string()))?;
    if request.targets.len() > 32
        || request.targets.values().any(|keys| keys.len() > 64)
        || !request.start.is_finite()
        || !request.end.is_finite()
        || request.start < 0.0
        || request.end <= request.start
        || request.end >= i64::MAX as f64
    {
        return Err(ApiError::invalid(
            "Provide up to 32 history targets, at most 64 source/point pairs per target, and valid start and end timestamps",
        ));
    }
    let first = request.start.floor() as i64;
    let last = (request.end.floor() as i64).max(first + 1);
    let period = crate::monitoring::level_history::Period::new(first, last)
        .map_err(|error| ApiError::invalid(error.to_string()))?;
    let manager = required(&manager)?;
    let mut summaries = serde_json::Map::new();
    let recorder = manager.snapshot()?["level_recorder"].clone();
    for (name, keys) in request.targets {
        let references = keys
            .iter()
            .map(|key| (key[0].as_str(), key[1].as_str()))
            .collect::<Vec<_>>();
        let summary =
            manager
                .level_history
                .summary(&references, period, chrono::Utc::now().timestamp())?;
        let not_recording =
            if keys.iter().any(|key| key[0] == "jack") && recorder["recording"] != true {
                recorder["reason"].as_str().map(str::to_owned)
            } else {
                None
            };
        let summary = serde_json::to_value(HistoryReading {
            summary,
            not_recording,
        })
        .map_err(anyhow::Error::from)?;
        summaries.insert(name, summary);
    }
    Ok(json_value(Arc::new(Value::Object(summaries))))
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "HostRecordingsRequest")]
pub(crate) struct RecordingsRequest {
    #[serde(default)]
    after: u64,
    #[serde(default = "recording_page_size")]
    limit: u32,
}

fn recording_page_size() -> u32 {
    100
}

async fn recordings(
    State(manager): State<Option<Arc<Manager>>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request: RecordingsRequest =
        serde_json::from_slice(&body).map_err(|error| ApiError::invalid(error.to_string()))?;
    if request.after > i64::MAX as u64 || !(1..=1000).contains(&request.limit) {
        return Err(ApiError::invalid(
            "Provide a valid recording cursor and a limit from 1 to 1000",
        ));
    }
    let value = required(&manager)?
        .history_store
        .host_audio_records(request.after, request.limit)
        .await
        .map_err(|error| {
            if error
                .downcast_ref::<tokio::sync::TryAcquireError>()
                .is_some()
            {
                ApiError {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    message: "Recording history read capacity is busy".to_owned(),
                }
            } else {
                ApiError::from(error)
            }
        })?;
    Ok(json_value(Arc::new(value)))
}
