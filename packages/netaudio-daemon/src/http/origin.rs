use super::{ApiError, ConnectionContext};
use axum::extract::Request;
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use url::Url;

pub async fn protect(request: Request, next: Next) -> Response {
    if matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) || same_origin(&request)
    {
        return next.run(request).await;
    }
    ApiError {
        status: StatusCode::FORBIDDEN,
        message: "Requests from other sites are refused".into(),
    }
    .into_response()
}

fn same_origin(request: &Request) -> bool {
    let headers = request.headers();
    match headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
    {
        Some("same-origin" | "none") => return true,
        Some(_) => return false,
        None => {}
    }
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Some(base) = request
        .extensions()
        .get::<ConnectionContext>()
        .and_then(|context| context.request_base_url(headers, request.uri()).ok())
    else {
        return false;
    };
    origin
        .to_str()
        .ok()
        .and_then(|origin| Url::parse(origin).ok())
        .is_some_and(|origin| origin.origin() == base.origin())
}
