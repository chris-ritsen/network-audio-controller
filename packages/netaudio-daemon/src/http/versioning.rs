use super::ApiError;
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

pub const MAJOR: u32 = 1;
pub const CONTRACT_VERSION: &str = "1.0.0";
const HEADER: &str = "netaudio-api-version";

pub fn descriptor() -> Value {
    json!({
        "major": MAJOR,
        "contract_version": CONTRACT_VERSION,
        "stability": "stable",
        "version_header": "NetAudio-API-Version",
        "openapi": "/openapi.json",
        "reference": "/api/docs",
    })
}

pub async fn select(request: Request, next: Next) -> Response {
    let mut versions = request.headers().get_all(HEADER).iter();
    let selected = versions.next();
    let mut response = if versions.next().is_some() {
        ApiError::invalid("provide exactly one NetAudio-API-Version header").into_response()
    } else if selected.is_some_and(|version| version.as_bytes() != b"1") {
        ApiError {
            status: StatusCode::NOT_ACCEPTABLE,
            message: "unsupported NetAudio-API-Version; this server supports 1".to_owned(),
        }
        .into_response()
    } else {
        next.run(request).await
    };
    response
        .headers_mut()
        .insert(HEADER, HeaderValue::from_static("1"));
    response.headers_mut().append(
        header::VARY,
        HeaderValue::from_static("NetAudio-API-Version"),
    );
    response.headers_mut().append(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("NetAudio-API-Version"),
    );
    response
}
