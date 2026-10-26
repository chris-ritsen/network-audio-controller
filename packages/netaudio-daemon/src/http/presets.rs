use super::inventory::json_value;
use super::{ApiError, MAXIMUM_REQUEST_BYTES, RequestBody};
use crate::devices::registry::DeviceRegistry;
use crate::presets::{Error, Service};
use crate::state::ReadModel;
use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde_json::Value;
use std::sync::Arc;

struct PresetState {
    presets: Arc<Service>,
    model: Arc<ReadModel>,
    devices: Arc<DeviceRegistry>,
}

pub fn router(
    presets: Arc<Service>,
    model: Arc<ReadModel>,
    devices: Arc<DeviceRegistry>,
) -> Router {
    Router::new()
        .route("/presets/preview", post(preview))
        .route("/presets/save", post(save))
        .route("/presets/plan", post(plan))
        .route("/presets/load", post(load))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
        .with_state(Arc::new(PresetState {
            presets,
            model,
            devices,
        }))
}

async fn preview(
    State(state): State<Arc<PresetState>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request: Value = serde_json::from_slice(&body)
        .map_err(|error| ApiError::invalid(format!("invalid json: {error}")))?;
    let records = state.model.records()?;
    let result = state.presets.preview(&request, &records).await?;
    Ok(json_value(Arc::new(result)))
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        match error {
            Error::Invalid(message) => Self::invalid(message),
            Error::Conflict(message) => Self {
                status: StatusCode::CONFLICT,
                message,
            },
            Error::Busy => Self::unavailable(Error::Busy.to_string()),
            Error::Internal(error) => error.into(),
        }
    }
}

async fn save(
    State(state): State<Arc<PresetState>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request: Value = serde_json::from_slice(&body)
        .map_err(|error| ApiError::invalid(format!("invalid json: {error}")))?;
    let result = state.presets.capture(&request, &state.devices).await?;
    Ok(json_value(Arc::new(result)))
}

async fn plan(
    State(state): State<Arc<PresetState>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request: Value = serde_json::from_slice(&body)
        .map_err(|error| ApiError::invalid(format!("invalid json: {error}")))?;
    let result = state.presets.plan_load(&request, &state.devices).await?;
    Ok(json_value(Arc::new(result)))
}

async fn load(
    State(state): State<Arc<PresetState>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request: Value = serde_json::from_slice(&body)
        .map_err(|error| ApiError::invalid(format!("invalid json: {error}")))?;
    let result = state.presets.load(&request, &state.devices).await?;
    Ok(json_value(Arc::new(result)))
}
