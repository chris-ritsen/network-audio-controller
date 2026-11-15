use super::inventory::json_value;
use super::{ApiError, RequestBody};
use crate::monitoring::settings::Settings;
use crate::network::selection::Selection;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
struct Owners {
    monitoring: Arc<Settings>,
    selection: Arc<Selection>,
}

pub fn router(monitoring: Arc<Settings>, selection: Arc<Selection>) -> Router {
    Router::new()
        .route("/settings", get(read))
        .route("/settings/monitoring", post(configure))
        .route("/settings/network", post(choose))
        .with_state(Owners {
            monitoring,
            selection,
        })
}

fn combined(owners: &Owners) -> Result<Value, ApiError> {
    let mut settings = owners.monitoring.snapshot()?;

    if let (Value::Object(settings), Value::Object(network)) =
        (&mut settings, owners.selection.snapshot()?)
    {
        settings.extend(network);
    }

    Ok(settings)
}

async fn read(State(owners): State<Owners>) -> Result<Response, ApiError> {
    Ok(json_value(Arc::new(combined(&owners)?)))
}

async fn configure(
    State(owners): State<Owners>,
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
    owners
        .monitoring
        .configure(port)
        .await
        .map_err(|error| ApiError {
            status: StatusCode::CONFLICT,
            message: error.to_string(),
        })?;
    Ok(json_value(Arc::new(combined(&owners)?)))
}

async fn choose(
    State(owners): State<Owners>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let request: Value = serde_json::from_slice(&body)
        .map_err(|error| ApiError::invalid(format!("invalid json: {error}")))?;
    let interfaces = request
        .get("interfaces")
        .and_then(Value::as_array)
        .and_then(|interfaces| {
            interfaces
                .iter()
                .map(|interface| interface.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
        })
        .ok_or_else(|| ApiError::invalid("interfaces must be a list of interface names"))?;
    let selection = Arc::clone(&owners.selection);
    tokio::task::spawn_blocking(move || selection.choose(interfaces))
        .await
        .map_err(|error| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        })?
        .map_err(|error| ApiError {
            status: StatusCode::CONFLICT,
            message: error.to_string(),
        })?;
    Ok(json_value(Arc::new(combined(&owners)?)))
}
