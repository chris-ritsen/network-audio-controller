use super::{
    ApiError, MAXIMUM_REQUEST_BYTES, RequestBody,
    inventory::{json_value, query_context},
};
use crate::managed::{Error, operations, registry::Registry};
use crate::state::ReadModel;
use axum::{
    Router,
    extract::{DefaultBodyLimit, OriginalUri, RawQuery, State},
    http::StatusCode,
    response::Response,
    routing::{get, post},
};
use serde_json::{Map, Value, json};
use std::sync::Arc;

struct ManagedState {
    managed: Arc<Registry>,
    model: Arc<ReadModel>,
}

pub fn router(managed: Arc<Registry>, model: Arc<ReadModel>) -> Router {
    Router::new()
        .route("/ddm/status", get(status))
        .route("/ddm/connections", get(saved_connections))
        .route("/ddm/domains", get(domains).post(execute))
        .route("/ddm/domains/update", post(execute))
        .route("/ddm/enrollment", post(execute))
        .route("/ddm/devices", get(devices))
        .route("/ddm/refresh", post(execute))
        .route("/ddm/graphql", post(execute))
        .route("/ddm/login", post(execute))
        .route("/ddm/logout", post(execute))
        .route("/ddm/context", post(execute))
        .route("/ddm/profile", post(execute))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
        .with_state(Arc::new(ManagedState { managed, model }))
}

async fn status(State(state): State<Arc<ManagedState>>) -> Result<Response, ApiError> {
    Ok(json_value(Arc::new(state.managed.status()?)))
}

async fn saved_connections(State(state): State<Arc<ManagedState>>) -> Result<Response, ApiError> {
    Ok(json_value(Arc::new(state.managed.connections()?)))
}

async fn domains(
    State(state): State<Arc<ManagedState>>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let context = query_context(query.as_deref());
    Ok(json_value(Arc::new(json!(
        state.managed.domains(context.as_deref())?
    ))))
}

async fn devices(
    State(state): State<Arc<ManagedState>>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let context = query_context(query.as_deref());
    let values = state.model.records()?;
    let filtered: Map<_, _> = values
        .iter()
        .filter(|(_, device)| {
            device["inventory_sources"]
                .as_array()
                .is_some_and(|sources| sources.iter().any(|source| source == "ddm"))
                && context
                    .as_deref()
                    .is_none_or(|context| device["ddm_context"] == context)
        })
        .map(|(name, record)| (name.clone(), record.as_ref().clone()))
        .collect();
    Ok(json_value(Arc::new(json!(filtered))))
}

async fn execute(
    State(state): State<Arc<ManagedState>>,
    OriginalUri(uri): OriginalUri,
    RequestBody(bytes): RequestBody,
) -> Result<Response, ApiError> {
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::invalid("Managed request must be a JSON object"))?;
    let operation = operations::parse(uri.path().strip_prefix("/ddm/").unwrap_or_default(), value)?;
    let output = state.managed.execute(operation).await?;
    let mut response = json_value(Arc::new(output.value));
    *response.status_mut() = StatusCode::from_u16(output.status).map_err(anyhow::Error::from)?;
    Ok(response)
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        let status = match &error {
            Error::Invalid(_) | Error::Credential(_) => StatusCode::BAD_REQUEST,
            Error::Authentication => StatusCode::UNAUTHORIZED,
            Error::Conflict(_) => StatusCode::CONFLICT,
            Error::Busy => StatusCode::SERVICE_UNAVAILABLE,
            Error::Persistence(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_GATEWAY,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }
}
