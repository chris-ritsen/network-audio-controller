use super::{ApiError, DeviceService};
use crate::http::inventory::json_value;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use futures_util::StreamExt;
use netaudio_core::lock_key::{self, Search};
use qrcodegen::{QrCode, QrCodeEcc};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

const MAXIMUM_INSTALLER_LENGTH: u64 = 2 << 30;
const UPLOAD_IDLE_DEADLINE: Duration = Duration::from_secs(30);

static INSTALLER: Semaphore = Semaphore::const_new(1);

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router
        .route("/device-lock-key/dante-controller", post(read_installed))
        .route("/device-lock-key/installer", post(read_installer))
}

fn failed(error: impl std::fmt::Display) -> ApiError {
    ApiError::unavailable(error.to_string())
}

fn respond(search: Search, missing: fn(String) -> ApiError) -> Result<Response, ApiError> {
    let Some(key) = search.key else {
        return Err(missing(search.failures.join("; ")));
    };
    let import_link = format!("netaudio://lock-key/import?version=1&key={key}");
    let code = QrCode::encode_text(&import_link, QrCodeEcc::Medium).map_err(failed)?;
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
    let mut body = json!({
        "import_link": import_link,
        "key": key,
        "modules": modules,
    });

    if let Some(source) = search.source {
        body["source"] = json!(source);
    }

    if let Some(release) = search.release {
        body["platform"] = json!(release.platform);
        body["version"] = json!(release.version);
    }

    Ok(json_value(Arc::new(body)))
}

async fn read_installed() -> Result<Response, ApiError> {
    let search = tokio::task::spawn_blocking(|| lock_key::search(None))
        .await
        .map_err(failed)?;
    respond(search, ApiError::not_found)
}

async fn read_installer(request: Request) -> Result<Response, ApiError> {
    let _permit = INSTALLER.try_acquire().map_err(|_| ApiError {
        status: StatusCode::CONFLICT,
        message: "another installer is being read".to_owned(),
    })?;
    let spool =
        tokio::task::spawn_blocking(|| tempfile::tempfile_in(lock_key::scratch_directory()))
            .await
            .map_err(failed)?
            .map_err(failed)?;
    let mut spool = tokio::fs::File::from_std(spool);
    let mut body = request.into_body().into_data_stream();
    let mut length = 0_u64;

    loop {
        let chunk = match tokio::time::timeout(UPLOAD_IDLE_DEADLINE, body.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            Ok(Some(Err(error))) => return Err(ApiError::invalid(error.to_string())),
            Ok(None) => break,
            Err(_) => {
                return Err(ApiError {
                    status: StatusCode::REQUEST_TIMEOUT,
                    message: "request body timed out".to_owned(),
                });
            }
        };
        length += chunk.len() as u64;

        if length > MAXIMUM_INSTALLER_LENGTH {
            return Err(ApiError {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                message: "the installer is larger than 2 GiB".to_owned(),
            });
        }

        spool.write_all(&chunk).await.map_err(failed)?;
    }

    spool.flush().await.map_err(failed)?;
    let mut spool = spool.into_std().await;
    let search =
        tokio::task::spawn_blocking(move || lock_key::search_installer(&mut spool, "the file"))
            .await
            .map_err(failed)?;
    respond(search, ApiError::invalid)
}
