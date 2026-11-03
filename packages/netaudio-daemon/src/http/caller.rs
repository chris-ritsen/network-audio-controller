use super::ConnectionContext;
use crate::managed::sessions::{Caller, Sessions, COOKIE};
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;

pub async fn identify(
    State(sessions): State<Arc<Sessions>>,
    request: Request,
    next: Next,
) -> Response {
    let caller = caller(&sessions, &request);
    caller.scope(next.run(request)).await
}

fn caller(sessions: &Sessions, request: &Request) -> Caller {
    let headers = request.headers();
    if request
        .extensions()
        .get::<ConnectionContext>()
        .is_some_and(|context| context.client_address(headers).to_canonical().is_loopback())
    {
        return Caller::Owner;
    }
    session_token(headers)
        .and_then(|token| sessions.find(token))
        .map_or(Caller::Anonymous, Caller::Session)
}

fn session_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(name, _)| *name == COOKIE)
        .map(|(_, value)| value)
}

pub fn session_cookie(token: &str) -> Option<HeaderValue> {
    HeaderValue::from_str(&format!(
        "{COOKIE}={token}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=604800"
    ))
    .ok()
}

pub fn cleared_cookie() -> HeaderValue {
    HeaderValue::from_static(
        "__Host-netaudio-session=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0",
    )
}
