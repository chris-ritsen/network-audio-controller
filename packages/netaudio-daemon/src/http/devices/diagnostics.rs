use super::{ApiError, DeviceService, RequestBody, control_error, json_value, parameters, text};
use crate::devices::registry::DiagnosticOptions;
use axum::Router;
use axum::extract::{Extension, Path, RawQuery, State};
use axum::response::Response;
use axum::routing::{get, post};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router
        .route("/diagnostics/{device}", get(read))
        .route("/diagnostics/reset", post(update).layer(Extension(true)))
        .route("/diagnostics/policy", post(update).layer(Extension(false)))
}

async fn read(
    State(service): State<DeviceService>,
    Path(device): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    let query: BTreeMap<_, _> =
        url::form_urlencoded::parse(query.as_deref().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    let options = DiagnosticOptions {
        include_clock: query.get("clock").is_none_or(|value| value != "0"),
        receiver_history: query
            .get("receiver_history")
            .is_none_or(|value| value != "0"),
        points: query.get("history").is_some_and(|value| value == "points"),
        ..DiagnosticOptions::default()
    };
    snapshot(&service, &device, options)
}

async fn update(
    State(service): State<DeviceService>,
    Extension(reset): Extension<bool>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let device = text(&parameters, "device")?;
    let warning = if reset {
        None
    } else {
        Some(
            parameters
                .get("clock_variation_warnings")
                .and_then(Value::as_bool)
                .ok_or_else(|| ApiError::invalid("clock_variation_warnings must be a boolean"))?,
        )
    };
    Ok(snapshot(
        &service,
        device,
        DiagnosticOptions {
            reset,
            warning,
            points: parameters.get("history").and_then(Value::as_str) == Some("points"),
            ..DiagnosticOptions::default()
        },
    ))
}

fn snapshot(service: &DeviceService, device: &str, options: DiagnosticOptions) -> Response {
    match service.registry.diagnostics_snapshot(device, options) {
        Ok(value) => json_value(Arc::new(value)),
        Err(error) => control_error(error),
    }
}
