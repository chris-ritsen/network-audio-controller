use super::{
    ApiError, ConnectionContext, MAXIMUM_REQUEST_BYTES, RequestBody,
    inventory::{json_value, query_context},
};
use crate::managed::{
    Error,
    connections::Login,
    operations::{self, Operation},
    registry::Registry,
    sessions::{Caller, Sessions},
};
use crate::state::ReadModel;
use axum::{
    Router,
    extract::{DefaultBodyLimit, Extension, OriginalUri, RawQuery, State},
    http::{HeaderMap, StatusCode, Uri, header},
    response::Response,
    routing::{get, post},
};
use serde_json::{Map, Value, json};
use std::sync::Arc;

struct ManagedState {
    managed: Arc<Registry>,
    model: Arc<ReadModel>,
    sessions: Arc<Sessions>,
}

pub fn router(managed: Arc<Registry>, model: Arc<ReadModel>, sessions: Arc<Sessions>) -> Router {
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
        .with_state(Arc::new(ManagedState {
            managed,
            model,
            sessions,
        }))
}

async fn status(State(state): State<Arc<ManagedState>>) -> Result<Response, ApiError> {
    let mut status = state.managed.status()?;
    let caller = Caller::current();
    if !caller.is_owner() && status.is_object() {
        status["default_context"] = json!(match &caller {
            Caller::Session(session) => session.context(),
            _ => None,
        });
    }
    Ok(json_value(Arc::new(status)))
}

async fn saved_connections(State(state): State<Arc<ManagedState>>) -> Result<Response, ApiError> {
    Ok(json_value(Arc::new(
        state.managed.connections_for(&Caller::current())?,
    )))
}

async fn domains(
    State(state): State<Arc<ManagedState>>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let context = query_context(query.as_deref());
    let caller = Caller::current();
    let domains: Vec<_> = state
        .managed
        .domains(context.as_deref())?
        .into_iter()
        .filter(|domain| caller.sees_domain_record(domain))
        .collect();
    Ok(json_value(Arc::new(json!(domains))))
}

async fn devices(
    State(state): State<Arc<ManagedState>>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let context = query_context(query.as_deref());
    let caller = Caller::current();
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
                && caller.sees_device(device)
        })
        .map(|(name, record)| (name.clone(), record.as_ref().clone()))
        .collect();
    Ok(json_value(Arc::new(json!(filtered))))
}

async fn execute(
    State(state): State<Arc<ManagedState>>,
    Extension(connection): Extension<ConnectionContext>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    RequestBody(bytes): RequestBody,
) -> Result<Response, ApiError> {
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::invalid("Managed request must be a JSON object"))?;
    let operation = operations::parse(uri.path().strip_prefix("/ddm/").unwrap_or_default(), value)?;
    let caller = Caller::current();
    let operation = match operation {
        Operation::Login(request) if !caller.is_owner() => {
            return session_login(&state, caller, &connection, &headers, &uri, request).await;
        }
        Operation::Logout(server) if !caller.is_owner() => {
            return session_logout(&state, caller, &server);
        }
        operation => operation,
    };
    let output = state.managed.execute(operation, caller).await?;
    let mut response = json_value(Arc::new(output.value));
    *response.status_mut() = StatusCode::from_u16(output.status).map_err(anyhow::Error::from)?;
    Ok(response)
}

async fn session_login(
    state: &ManagedState,
    caller: Caller,
    connection: &ConnectionContext,
    headers: &HeaderMap,
    uri: &Uri,
    request: Login,
) -> Result<Response, ApiError> {
    if connection.request_base_url(headers, uri)?.scheme() != "https" {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            message: "Open the daemon over HTTPS to log in to DDM".into(),
        });
    }
    let (server, login) = state.managed.authenticate(request).await?;
    let (session, token) = match caller {
        Caller::Session(session) => (session, None),
        _ => {
            let (token, session) = state.sessions.open()?;
            (session, Some(token))
        }
    };
    session.sign_in(server, login);
    let mut response = json_value(Arc::new(
        state.managed.connections_for(&Caller::Session(session))?,
    ));
    if let Some(cookie) = token.as_deref().and_then(super::caller::session_cookie) {
        response.headers_mut().append(header::SET_COOKIE, cookie);
    }
    Ok(response)
}

fn session_logout(
    state: &ManagedState,
    caller: Caller,
    server: &str,
) -> Result<Response, ApiError> {
    let Caller::Session(session) = caller else {
        return Ok(json_value(Arc::new(
            state.managed.connections_for(&Caller::Anonymous)?,
        )));
    };
    if session.sign_out(server) {
        return Ok(json_value(Arc::new(
            state.managed.connections_for(&Caller::Session(session))?,
        )));
    }
    state.sessions.end(&session);
    let mut response = json_value(Arc::new(
        state.managed.connections_for(&Caller::Anonymous)?,
    ));
    response
        .headers_mut()
        .append(header::SET_COOKIE, super::caller::cleared_cookie());
    Ok(response)
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        let status = match &error {
            Error::Invalid(_) | Error::Credential(_) => StatusCode::BAD_REQUEST,
            Error::Authentication => StatusCode::UNAUTHORIZED,
            Error::Forbidden(_) => StatusCode::FORBIDDEN,
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
