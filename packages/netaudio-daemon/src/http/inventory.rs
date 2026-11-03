use super::ApiError;
use super::visibility::{EventFilter, Filtered};
use crate::events::EventOptions;
use crate::managed::sessions::Caller;
use crate::state::{AmbiguousDevice, ReadModel};
use axum::body::{Body, Bytes};
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::StreamExt;
use serde_json::Value;
use std::convert::Infallible;
use std::sync::Arc;

pub fn router(model: Arc<ReadModel>) -> Router {
    Router::new()
        .route("/api", get(contract))
        .route("/devices", get(devices))
        .route("/devices/{device}", get(device))
        .route("/events", get(events))
        .with_state(model)
}

async fn contract() -> Response {
    static CONTRACT: std::sync::LazyLock<Arc<Value>> = std::sync::LazyLock::new(|| {
        let mut contract: Value = serde_json::from_str(include_str!("../../assets/api.json"))
            .expect("embedded API descriptor must be valid JSON");
        contract["public_api"] = super::versioning::descriptor();
        Arc::new(contract)
    });
    json_value(Arc::clone(&CONTRACT))
}

async fn devices(
    State(model): State<Arc<ReadModel>>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let context = query_context(query.as_deref());
    let caller = Caller::current();
    if caller.is_owner() {
        return Ok(json_bytes(model.devices_in_context(context.as_deref())?));
    }
    Ok(json_bytes(model.devices_where(context.as_deref(), |record| {
        caller.sees_device(record)
    })?))
}

async fn device(
    State(model): State<Arc<ReadModel>>,
    Path(device): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let context = query_context(query.as_deref());
    let device = model
        .device_in_context(&device, context.as_deref())
        .map_err(|error| {
            if error.is::<AmbiguousDevice>() {
                ApiError {
                    status: StatusCode::CONFLICT,
                    message: error.to_string(),
                }
            } else {
                ApiError::from(error)
            }
        })?
        .filter(|device| Caller::current().sees_device(device))
        .ok_or_else(|| ApiError::not_found("device not found"))?;

    Ok(json_value(device))
}

async fn events(
    State(model): State<Arc<ReadModel>>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let options = EventOptions::from_query(query.as_deref().unwrap_or_default());
    let receiver = model.subscribe(options)?;
    let caller = Caller::current();
    let body = if caller.is_owner() {
        Body::from_stream(receiver.map(|event| Ok::<_, Infallible>(event.encoded)))
    } else {
        let ended: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            match &caller {
                Caller::Session(session) => {
                    let mut changes = session.subscribe();
                    let expires = session.expires();
                    Box::pin(async move {
                        tokio::select! {
                            _ = changes.changed() => {}
                            _ = tokio::time::sleep_until(expires) => {}
                        }
                    })
                }
                _ => Box::pin(std::future::pending()),
            };
        let mut filter = EventFilter::new(caller);
        Body::from_stream(
            receiver
                .map(move |event| match filter.apply(&event) {
                    Filtered::Keep => Some(Some(event.encoded)),
                    Filtered::Replace(encoded) => Some(Some(encoded)),
                    Filtered::Resynchronize => None,
                    Filtered::Skip => Some(None),
                })
                .take_while(|item| std::future::ready(item.is_some()))
                .filter_map(|item| std::future::ready(item.flatten()))
                .take_until(ended)
                .map(Ok::<_, Infallible>),
        )
    };
    let mut response = body.into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    Ok(response)
}

pub fn json_bytes(bytes: Bytes) -> Response {
    let mut response = bytes.into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

pub fn json_value(value: Arc<Value>) -> Response {
    let mut response = Json(value).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

pub(super) fn query_context(query: Option<&str>) -> Option<String> {
    url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .find(|(name, value)| name == "context" && !value.is_empty())
        .map(|(_, value)| value.into_owned())
}
