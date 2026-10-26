use super::inventory::json_value;
use super::{ApiError, RequestBody};
use crate::monitoring::settings::Settings;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{get, post};
use serde_json::Value;
use std::sync::Arc;

pub fn router(settings: Arc<Settings>) -> Router {
    Router::new()
        .route("/settings", get(read))
        .route("/settings/monitoring", post(configure))
        .with_state(settings)
}

async fn read(State(settings): State<Arc<Settings>>) -> Result<Response, ApiError> {
    Ok(json_value(Arc::new(settings.snapshot()?)))
}

async fn configure(
    State(settings): State<Arc<Settings>>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request: Value = serde_json::from_slice(&body)
        .map_err(|error| ApiError::invalid(format!("invalid json: {error}")))?;
    let port = request
        .get("port")
        .and_then(Value::as_u64)
        .and_then(|port| u16::try_from(port).ok())
        .filter(|port| *port >= 1024)
        .ok_or_else(|| {
            ApiError::invalid("Monitoring port must be an integer between 1024 and 65535")
        })?;
    let settings = settings.configure(port).await.map_err(|error| ApiError {
        status: StatusCode::CONFLICT,
        message: error.to_string(),
    })?;
    Ok(json_value(Arc::new(settings)))
}
