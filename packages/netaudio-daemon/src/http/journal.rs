use super::{ApiError, RequestBody};
use crate::devices::registry::DeviceRegistry;
use crate::monitoring::journal::model::{EventFilter, EventKind, LifecyclePhase, OperationRequest};
use crate::monitoring::journal::service::Journal;
use axum::extract::{RawQuery, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Map, Value, json};
use std::sync::Arc;

#[derive(Clone)]
struct JournalService {
    journal: Arc<Journal>,
    registry: Arc<DeviceRegistry>,
}

pub fn router(journal: Arc<Journal>, registry: Arc<DeviceRegistry>) -> Router {
    Router::new()
        .route("/event-journal", get(read).delete(clear))
        .route("/event-journal/operations", post(operation))
        .route("/issues", get(issues))
        .with_state(JournalService { journal, registry })
}

async fn read(
    State(service): State<JournalService>,
    RawQuery(query): RawQuery,
) -> Result<Json<Value>, ApiError> {
    let arguments = query_arguments(query.as_deref().unwrap_or_default())?;
    let filter =
        EventFilter::parse(&arguments).map_err(|error| ApiError::invalid(error.to_string()))?;
    Ok(Json(
        serde_json::to_value(service.journal.page(filter).await?).map_err(anyhow::Error::from)?,
    ))
}

async fn issues(
    State(service): State<JournalService>,
    RawQuery(query): RawQuery,
) -> Result<Json<Value>, ApiError> {
    let arguments = query_arguments(query.as_deref().unwrap_or_default())?;
    let filter = crate::monitoring::issues::store::Filter::parse(&arguments)
        .map_err(|error| ApiError::invalid(error.to_string()))?;
    Ok(Json(
        serde_json::to_value(service.journal.issues(filter).await?).map_err(anyhow::Error::from)?,
    ))
}

fn query_arguments(query: &str) -> Result<Value, ApiError> {
    let mut arguments = Map::new();

    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if arguments.contains_key(name.as_ref()) {
            continue;
        }

        let value =
            if matches!(name.as_ref(), "limit" | "before_sequence") {
                json!(value.parse::<u64>().map_err(|_| ApiError::invalid(format!(
                    "{name} must be a non-negative integer"
                )))?)
            } else {
                json!(value)
            };
        arguments.insert(name.into_owned(), value);
    }

    Ok(Value::Object(arguments))
}

async fn clear(State(service): State<JournalService>) -> Result<Json<Value>, ApiError> {
    Ok(Json(service.journal.clear().await?))
}

async fn operation(
    State(service): State<JournalService>,
    RequestBody(body): RequestBody,
) -> Result<Json<Value>, ApiError> {
    let request: OperationRequest =
        serde_json::from_slice(&body).map_err(|error| ApiError::invalid(error.to_string()))?;
    request
        .event(&chrono::Utc::now().to_rfc3339())
        .map_err(|error| ApiError::invalid(error.to_string()))?;
    let refresh = request.kind == EventKind::ConfigurationOperation
        && matches!(
            request.lifecycle_phase,
            LifecyclePhase::RequestAcknowledged
                | LifecyclePhase::EffectiveStateConfirmed
                | LifecyclePhase::PartialUnobservable
                | LifecyclePhase::Inconsistent
                | LifecyclePhase::PersistenceConfirmed,
        );
    let device = request.device_snapshot["server_name"]
        .as_str()
        .map(str::to_owned);
    let result = service.journal.operation(request).await?;

    if refresh && let Some(device) = device {
        service.registry.request_refresh(&device)?;
    }

    Ok(Json(result))
}
