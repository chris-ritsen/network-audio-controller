use super::{ApiError, DeviceService, control_error, json_value};
use axum::extract::{Path, RawQuery, State};
use axum::response::Response;
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::Arc;

pub(super) async fn device(
    State(service): State<DeviceService>,
    Path(reference): Path<String>,
) -> Result<Response, ApiError> {
    let records = service.registry.cached_records().await?;
    let record = crate::state::resolve_record(&records, &reference, None)?
        .cloned()
        .ok_or_else(|| ApiError::not_found("device not found"))?;
    match service.registry.forget(vec![record]).await {
        Ok(forgotten) => Ok(json_value(Arc::new(json!({"forgotten": forgotten})))),
        Err(error) => Ok(control_error(error)),
    }
}

pub(super) async fn devices(
    State(service): State<DeviceService>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let selections: BTreeSet<_> =
        url::form_urlencoded::parse(query.as_deref().unwrap_or_default().as_bytes())
            .filter(|(key, _)| key == "selection")
            .flat_map(|(_, value)| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .collect();
    if selections.is_empty()
        || selections
            .iter()
            .any(|value| !matches!(value.as_str(), "offline" | "emulated"))
    {
        return Err(ApiError::invalid(
            "selection must be one or more of: emulated, offline",
        ));
    }

    let records = service.registry.cached_records().await?;
    let selected = records
        .into_values()
        .filter(|record| {
            selections.contains("offline") && record["online"] == false
                || selections.contains("emulated") && record["kind"] == "emulated"
        })
        .collect();
    match service.registry.forget(selected).await {
        Ok(forgotten) => Ok(json_value(Arc::new(json!({"forgotten": forgotten})))),
        Err(error) => Ok(control_error(error)),
    }
}
