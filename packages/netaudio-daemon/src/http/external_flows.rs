use super::{ApiError, inventory};
use crate::sap::Diagnostics;
use crate::state::ReadModel;
use anyhow::Context;
use axum::Router;
use axum::extract::State;
use axum::response::Response;
use axum::routing::get;
use std::sync::{Arc, Mutex};

struct ExternalFlows {
    model: Arc<ReadModel>,
    diagnostics: Arc<Mutex<Diagnostics>>,
}

pub fn router(model: Arc<ReadModel>, diagnostics: Arc<Mutex<Diagnostics>>) -> Router {
    Router::new()
        .route("/external-flows", get(flows))
        .route("/external-flows/diagnostics", get(status))
        .with_state(Arc::new(ExternalFlows { model, diagnostics }))
}

async fn flows(State(state): State<Arc<ExternalFlows>>) -> Result<Response, ApiError> {
    Ok(inventory::json_bytes(state.model.external_flows()?))
}

async fn status(State(state): State<Arc<ExternalFlows>>) -> Result<Response, ApiError> {
    let diagnostics = state
        .diagnostics
        .lock()
        .map_err(|_| anyhow::anyhow!("SAP diagnostics lock poisoned"))?
        .clone();
    let encoded = serde_json::to_vec(&diagnostics).context("serialize SAP diagnostics")?;
    Ok(inventory::json_bytes(encoded.into()))
}
