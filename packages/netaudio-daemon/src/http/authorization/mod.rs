mod form;
mod page;

use super::{ConnectionContext, MAXIMUM_REQUEST_BYTES, RequestBody};
use crate::authorization::{AuthorizationError, AuthorizationStore, SCOPES, normalize_scope};
use axum::extract::{DefaultBodyLimit, Extension, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use form::{Form, basic_credentials, parse_body, parse_json, parse_query};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Semaphore;
use url::Url;

const METADATA_PATH: &str = "/.well-known/oauth-authorization-server";
const RESOURCE_METADATA_PATH: &str = "/.well-known/oauth-protected-resource";
const AUTHORIZE_PATH: &str = "/oauth/authorize";
const REGISTER_PATH: &str = "/oauth/register";
const REVOKE_PATH: &str = "/oauth/revoke";
const TOKEN_PATH: &str = "/oauth/token";
const MAXIMUM_CONCURRENT_STORE_OPERATIONS: usize = 4;
const CHALLENGE_BYTES: usize = 32;

pub struct AuthorizationService {
    store: Arc<AuthorizationStore>,
    workers: Arc<Semaphore>,
}

struct ConsentRequest {
    client_id: String,
    client_name: String,
    challenge: String,
    redirect: String,
    scope: String,
    state: String,
}

impl AuthorizationService {
    pub fn new(store: Arc<AuthorizationStore>) -> Arc<Self> {
        Arc::new(Self {
            store,
            workers: Arc::new(Semaphore::new(MAXIMUM_CONCURRENT_STORE_OPERATIONS)),
        })
    }

    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route(METADATA_PATH, get(metadata))
            .route(
                "/.well-known/oauth-authorization-server/{*resource}",
                get(metadata),
            )
            .route(RESOURCE_METADATA_PATH, get(resource_metadata))
            .route(
                "/.well-known/oauth-protected-resource/{*resource}",
                get(resource_metadata),
            )
            .route(REGISTER_PATH, post(register))
            .route(AUTHORIZE_PATH, get(authorize_page).post(authorize_decision))
            .route(TOKEN_PATH, post(exchange_token))
            .route(REVOKE_PATH, post(revoke))
            .method_not_allowed_fallback(|| async {
                response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    Json(json!({"error": "not found"})),
                )
            })
            .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
            .with_state(Arc::clone(self))
    }

    async fn execute<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&AuthorizationStore) -> Result<T, AuthorizationError> + Send + 'static,
    ) -> Result<T, AuthorizationError> {
        let permit =
            Arc::clone(&self.workers)
                .try_acquire_owned()
                .map_err(|_| AuthorizationError {
                    status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                    error: "temporarily_unavailable",
                    description: "authorization service is busy; try again shortly".to_owned(),
                })?;
        let store = Arc::clone(&self.store);

        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(&store)
        })
        .await
        .map_err(|error| AuthorizationError::from(anyhow::Error::from(error)))?
    }

    pub(crate) fn base_url(
        &self,
        connection: &ConnectionContext,
        headers: &HeaderMap,
    ) -> Result<Url, AuthorizationError> {
        if let Some(configured) = self.store.public_url()? {
            let url = Url::parse(&configured).map_err(|error| {
                AuthorizationError::from(anyhow::anyhow!("invalid configured public URL: {error}"))
            })?;

            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(anyhow::anyhow!("invalid configured public URL").into());
            }

            return Ok(url);
        }

        connection
            .base_url(headers)
            .map_err(|error| AuthorizationError::new("invalid_request", error.message))
    }

    fn consent(&self, fields: &Form) -> Result<ConsentRequest, AuthorizationError> {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

        let (client_id, record) = self.store.client(value(fields, "client_id"))?;
        let redirect = value(fields, "redirect_uri").ok_or_else(|| {
            AuthorizationError::new(
                "invalid_request",
                "redirect_uri is not registered for this client",
            )
        })?;

        if !record["redirect_uris"]
            .as_array()
            .is_some_and(|uris| uris.iter().any(|uri| uri.as_str() == Some(redirect)))
        {
            return Err(AuthorizationError::new(
                "invalid_request",
                "redirect_uri is not registered for this client",
            ));
        }

        if value(fields, "response_type") != Some("code") {
            return Err(AuthorizationError::new(
                "unsupported_response_type",
                "only the code response type is supported",
            ));
        }

        let challenge = value(fields, "code_challenge").unwrap_or_default();

        if value(fields, "code_challenge_method") != Some("S256") || challenge.is_empty() {
            return Err(AuthorizationError::new(
                "invalid_request",
                "PKCE with S256 is required",
            ));
        }

        if !URL_SAFE_NO_PAD
            .decode(challenge)
            .is_ok_and(|bytes| bytes.len() == CHALLENGE_BYTES)
        {
            return Err(AuthorizationError::new(
                "invalid_request",
                "invalid S256 code challenge",
            ));
        }

        Ok(ConsentRequest {
            client_id,
            client_name: record["client_name"]
                .as_str()
                .unwrap_or("MCP client")
                .to_owned(),
            challenge: challenge.to_owned(),
            redirect: redirect.to_owned(),
            scope: normalize_scope(value(fields, "scope"))?,
            state: value(fields, "state").unwrap_or_default().to_owned(),
        })
    }

    fn render_consent(
        &self,
        connection: &ConnectionContext,
        headers: &HeaderMap,
        request: &ConsentRequest,
        error: Option<&str>,
    ) -> Result<Response, AuthorizationError> {
        if !self.store.login_secret_configured()? {
            return Err(AuthorizationError {
                status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                error: "temporarily_unavailable",
                description:
                    "no login secret is configured; run `netaudio daemon mcp-login-secret`"
                        .to_owned(),
            });
        }

        let base = self.base_url(connection, headers)?;
        let page = page::render(
            request,
            &base[url::Position::BeforeHost..url::Position::AfterPort],
            error,
        );
        let mut response = response(StatusCode::OK, axum::response::Html(page));
        let redirect = Url::parse(&request.redirect)
            .map_err(|error| AuthorizationError::from(anyhow::Error::from(error)))?;
        let policy = format!(
            "default-src 'none'; style-src 'unsafe-inline'; form-action 'self' {}; frame-ancestors 'none'",
            redirect.origin().ascii_serialization()
        );
        response.headers_mut().insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_str(&policy)
                .map_err(|error| AuthorizationError::from(anyhow::Error::from(error)))?,
        );
        Ok(response)
    }
}

async fn metadata(
    State(service): State<Arc<AuthorizationService>>,
    Extension(connection): Extension<ConnectionContext>,
    headers: HeaderMap,
) -> Result<Response, AuthorizationError> {
    let base = service
        .base_url(&connection, &headers)?
        .as_str()
        .trim_end_matches('/')
        .to_owned();
    Ok(response(
        StatusCode::OK,
        Json(json!({
            "authorization_endpoint": format!("{base}{AUTHORIZE_PATH}"),
            "code_challenge_methods_supported": ["S256"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "issuer": base,
            "registration_endpoint": format!("{base}{REGISTER_PATH}"),
            "response_types_supported": ["code"],
            "revocation_endpoint": format!("{base}{REVOKE_PATH}"),
            "scopes_supported": SCOPES,
            "token_endpoint": format!("{base}{TOKEN_PATH}"),
            "token_endpoint_auth_methods_supported": ["none", "client_secret_post", "client_secret_basic"],
        })),
    ))
}

async fn resource_metadata(
    State(service): State<Arc<AuthorizationService>>,
    Extension(connection): Extension<ConnectionContext>,
    headers: HeaderMap,
) -> Result<Response, AuthorizationError> {
    let base = service
        .base_url(&connection, &headers)?
        .as_str()
        .trim_end_matches('/')
        .to_owned();
    Ok(response(
        StatusCode::OK,
        Json(json!({
            "authorization_servers": [base],
            "bearer_methods_supported": ["header"],
            "resource": format!("{base}/mcp"),
            "scopes_supported": SCOPES,
        })),
    ))
}

async fn register(
    State(service): State<Arc<AuthorizationService>>,
    RequestBody(body): RequestBody,
) -> Result<Response, AuthorizationError> {
    let metadata = parse_json(&body)?;
    let client = service
        .execute(move |store| store.register_client(&metadata))
        .await?;
    Ok(response(StatusCode::CREATED, Json(client)))
}

async fn authorize_page(
    State(service): State<Arc<AuthorizationService>>,
    Extension(connection): Extension<ConnectionContext>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, AuthorizationError> {
    let request = service.consent(&parse_query(query.as_deref().unwrap_or_default()))?;
    service.render_consent(&connection, &headers, &request, None)
}

async fn authorize_decision(
    State(service): State<Arc<AuthorizationService>>,
    Extension(connection): Extension<ConnectionContext>,
    headers: HeaderMap,
    RequestBody(body): RequestBody,
) -> Result<Response, AuthorizationError> {
    let fields = parse_body(&body, &headers)?;
    let request = service.consent(&fields)?;

    if value(&fields, "decision") != Some("allow") {
        return redirect(
            &request.redirect,
            &[("error", "access_denied"), ("state", &request.state)],
        );
    }

    let address = connection.client_address(&headers).to_string();

    if !service.store.reserve_login_attempt(&address)? {
        return Err(AuthorizationError {
            status: StatusCode::TOO_MANY_REQUESTS.as_u16(),
            error: "access_denied",
            description: "too many login attempts; try again in a few minutes".to_owned(),
        });
    }

    let secret = value(&fields, "secret").unwrap_or_default().to_owned();
    let accepted = service
        .execute(move |store| store.verify_login_secret(&secret))
        .await?;

    if !accepted {
        tracing::warn!(%address, client = %request.client_name, "MCP login failed");
        return service.render_consent(
            &connection,
            &headers,
            &request,
            Some("That login secret is not right."),
        );
    }

    service.store.login_succeeded(&address)?;
    let code = service.store.issue_code(
        request.client_id,
        request.redirect.clone(),
        request.challenge,
        request.scope.clone(),
    )?;
    tracing::info!(%address, client = %request.client_name, scope = %request.scope, "MCP client authorized");
    redirect(
        &request.redirect,
        &[("code", &code), ("state", &request.state)],
    )
}

async fn exchange_token(
    State(service): State<Arc<AuthorizationService>>,
    headers: HeaderMap,
    RequestBody(body): RequestBody,
) -> Result<Response, AuthorizationError> {
    let fields = parse_body(&body, &headers)?;
    let basic = basic_credentials(&headers)?;
    let result = service
        .execute(move |store| {
            let (client, record) = store.authenticate_client(
                value(&fields, "client_id")
                    .or_else(|| basic.as_ref().map(|(client, _)| client.as_str())),
                value(&fields, "client_secret")
                    .or_else(|| basic.as_ref().map(|(_, secret)| secret.as_str())),
            )?;

            match value(&fields, "grant_type") {
                Some("authorization_code") => {
                    let scope = store.redeem_code(
                        value(&fields, "code"),
                        &client,
                        value(&fields, "redirect_uri"),
                        value(&fields, "code_verifier"),
                    )?;
                    store.issue_tokens(
                        &client,
                        record["client_name"].as_str().unwrap_or_default(),
                        &scope,
                    )
                }
                Some("refresh_token") => store.refresh(value(&fields, "refresh_token"), &client),
                _ => Err(AuthorizationError::new(
                    "unsupported_grant_type",
                    "grant_type must be authorization_code or refresh_token",
                )),
            }
        })
        .await?;
    Ok(response(StatusCode::OK, Json(result)))
}

async fn revoke(
    State(service): State<Arc<AuthorizationService>>,
    headers: HeaderMap,
    RequestBody(body): RequestBody,
) -> Result<Response, AuthorizationError> {
    let fields = parse_body(&body, &headers)?;
    service
        .execute(move |store| store.revoke(value(&fields, "token")))
        .await?;
    Ok(response(StatusCode::OK, Json(json!({}))))
}

fn value<'a>(fields: &'a Form, name: &str) -> Option<&'a str> {
    fields.get(name).map(String::as_str)
}

fn redirect(
    destination: &str,
    parameters: &[(&str, &str)],
) -> Result<Response, AuthorizationError> {
    let mut location = Url::parse(destination)
        .map_err(|_| AuthorizationError::new("invalid_request", "invalid redirect URI"))?;

    {
        let mut query = location.query_pairs_mut();

        for (name, value) in parameters {
            if !value.is_empty() {
                query.append_pair(name, value);
            }
        }
    }

    let mut response = response(StatusCode::FOUND, ());
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(location.as_str())
            .map_err(|error| AuthorizationError::from(anyhow::Error::from(error)))?,
    );
    Ok(response)
}

fn response(status: StatusCode, body: impl IntoResponse) -> Response {
    let mut response = (status, body).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );

    if status == StatusCode::UNAUTHORIZED {
        headers.insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"netaudio\""),
        );
    }

    response
}

impl IntoResponse for AuthorizationError {
    fn into_response(self) -> Response {
        response(
            StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(json!({"error": self.error, "error_description": self.description})),
        )
    }
}
