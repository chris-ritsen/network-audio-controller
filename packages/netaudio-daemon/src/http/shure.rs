use super::ApiError;
use crate::shure::Registry;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;
use std::sync::Arc;

pub fn router(registry: Arc<Registry>) -> Router {
    Router::new()
        .route("/shure/devices", get(devices))
        .route("/shure/devices/{device}", get(device))
        .route("/shure/set", post(set))
        .with_state(registry)
}

async fn devices(State(registry): State<Arc<Registry>>) -> Result<Json<Value>, ApiError> {
    Ok(Json(registry.records()?))
}

async fn device(
    State(registry): State<Arc<Registry>>,
    Path(selector): Path<String>,
) -> Result<Json<Value>, ApiError> {
    registry
        .device(&selector)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("wireless device not found"))
}

async fn set(
    State(registry): State<Arc<Registry>>,
    Json(arguments): Json<Value>,
) -> Result<Response, ApiError> {
    registry.prepare(&arguments).map_err(|error| ApiError {
        status: StatusCode::BAD_REQUEST,
        message: error.to_string(),
    })?;
    let outcome = registry.execute(arguments, None).await?;
    Ok((
        StatusCode::from_u16(outcome.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(outcome.value),
    )
        .into_response())
}
