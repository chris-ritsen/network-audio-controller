use super::{control_error, parameters, text, ApiError, DeviceService, RequestBody};
use crate::devices::locking::{response_status, Operation};
use crate::devices::operations::ControlOperation;
use crate::http::inventory::json_value;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use qrcodegen::{QrCode, QrCodeEcc};
use serde_json::json;
use std::sync::Arc;

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router
        .route("/lock", post(configure).layer(Extension(true)))
        .route("/unlock", post(configure).layer(Extension(false)))
        .route("/device-lock-key", post(export_key))
        .route(
            "/device-lock-key/dante-controller",
            post(read_installed_key),
        )
}

async fn read_installed_key() -> Result<Response, ApiError> {
    let search = tokio::task::spawn_blocking(|| netaudio_core::lock_key::search(None))
        .await
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    let (Some(key), Some(source)) = (search.key, search.source) else {
        return Err(ApiError::not_found(search.failures.join("; ")));
    };
    let import_link = format!("netaudio://lock-key/import?version=1&key={key}");
    let code = QrCode::encode_text(&import_link, QrCodeEcc::Medium)
        .map_err(|error| ApiError::unavailable(error.to_string()))?;
    let modules: Vec<String> = (0..code.size())
        .map(|row| {
            (0..code.size())
                .map(|column| {
                    if code.get_module(column, row) {
                        '1'
                    } else {
                        '0'
                    }
                })
                .collect()
        })
        .collect();
    Ok(json_value(Arc::new(json!({
        "import_link": import_link,
        "key": key,
        "modules": modules,
        "source": source,
    }))))
}

async fn export_key(State(service): State<DeviceService>) -> Result<Response, ApiError> {
    Ok(match service.registry.lock_key() {
        Ok(key) => json_value(Arc::new(json!({"key": key}))),
        Err(error) => control_error(error),
    })
}

async fn configure(
    State(service): State<DeviceService>,
    Extension(locking): Extension<bool>,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let parameters = parameters(&body, false)?;
    let reference = text(&parameters, "device")?;
    let operation = match Operation::new(&json!(parameters), locking) {
        Ok(operation) => operation,
        Err(error) => return Ok(control_error(error)),
    };
    let completion = match service
        .registry
        .apply(reference, ControlOperation::Locking(operation))
        .await
    {
        Ok(completion) => completion,
        Err(error) => return Ok(control_error(error)),
    };
    let result = completion
        .observation
        .ok_or_else(|| ApiError::unavailable("lock operation result unavailable"))?;
    let status =
        StatusCode::from_u16(response_status(&result)).expect("valid lock response status");
    let mut response = json_value(Arc::new(result));
    *response.status_mut() = status;
    Ok(response)
}
