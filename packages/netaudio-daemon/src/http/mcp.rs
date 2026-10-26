use super::{
    ApiError, AuthorizationService, ConnectionContext, MAXIMUM_REQUEST_BYTES, RequestBody,
};
use crate::authorization::AuthorizationStore;
use crate::mcp::{protocol, service::Service};
use axum::extract::{DefaultBodyLimit, Extension, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::{Json, Router};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

struct Endpoint {
    service: Arc<Service>,
    authorization: Arc<AuthorizationStore>,
    oauth: Arc<AuthorizationService>,
    cancellation: CancellationToken,
}

pub fn router(
    service: Arc<Service>,
    authorization: Arc<AuthorizationStore>,
    oauth: Arc<AuthorizationService>,
    cancellation: CancellationToken,
) -> Router {
    Router::new()
        .route("/mcp", any(handle))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
        .with_state(Arc::new(Endpoint {
            service,
            authorization,
            oauth,
            cancellation,
        }))
}

async fn handle(
    State(endpoint): State<Arc<Endpoint>>,
    Extension(connection): Extension<ConnectionContext>,
    method: Method,
    headers: HeaderMap,
    RequestBody(body): RequestBody,
) -> Result<Response, ApiError> {
    let grant = endpoint
        .authorization
        .grant(
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
        )
        .map_err(|error| ApiError::from(anyhow::Error::from(error)))?;
    let Some(grant) = grant else {
        let base = endpoint
            .oauth
            .base_url(&connection, &headers)
            .map_err(|error| ApiError::from(anyhow::Error::from(error)))?;
        let metadata = format!(
            "{}/.well-known/oauth-protected-resource",
            base.as_str().trim_end_matches('/')
        );
        let challenge = format!("Bearer realm=\"netaudio\", resource_metadata=\"{metadata}\"");
        let mut result = response(
            StatusCode::UNAUTHORIZED,
            Some(json!({
                "error": "a bearer token is required; connect through OAuth or run `netaudio daemon mcp-token`",
            })),
        );
        result.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_str(&challenge).map_err(anyhow::Error::from)?,
        );
        return Ok(result);
    };

    if method == Method::DELETE {
        return Ok(response(StatusCode::NO_CONTENT, None));
    }

    if method != Method::POST {
        return Ok(response(
            StatusCode::METHOD_NOT_ALLOWED,
            Some(json!({"error": "MCP requests are POST only"})),
        ));
    }

    let message = if body.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice::<Value>(&body) {
            Ok(message) => message,
            Err(error) => {
                return Ok(response(
                    StatusCode::OK,
                    Some(protocol::error(
                        Value::Null,
                        protocol::PARSE_ERROR,
                        format!("invalid json: {error}"),
                    )),
                ));
            }
        }
    };
    let modern = protocol::modern_request(&message, &headers);

    if modern && let Err(rejection) = protocol::validate_modern(&message, &headers) {
        return Ok(response(rejection.status, Some(rejection.body)));
    }

    if modern && message["method"] == "subscriptions/listen" && !message["id"].is_null() {
        let stream = endpoint.service.subscriptions.listen(
            message["id"].clone(),
            &message["params"]["notifications"],
            endpoint.cancellation.clone(),
            grant.expires_at,
        );
        let mut response = stream.into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
            .headers_mut()
            .insert("x-accel-buffering", HeaderValue::from_static("no"));
        return Ok(response);
    }

    let empty = Map::new();
    let answer = async |message: &Value| {
        let identifier = message.get("id").cloned().unwrap_or(Value::Null);
        let Some(method) = message["method"]
            .as_str()
            .filter(|_| message["jsonrpc"] == "2.0")
        else {
            return Some(protocol::error(
                identifier,
                protocol::INVALID_REQUEST,
                "invalid request",
            ));
        };
        let parameters = match message.get("params") {
            None | Some(Value::Null) => &empty,
            Some(Value::Object(parameters)) => parameters,
            _ => {
                return Some(protocol::error(
                    identifier,
                    protocol::INVALID_PARAMS,
                    "params must be an object",
                ));
            }
        };

        if identifier.is_null() {
            return None;
        }

        let result = endpoint.service.call(method, parameters, &grant).await;
        Some(match result {
            Ok(result) => {
                let result = if modern {
                    protocol::complete_result(method, result, endpoint.service.identity())
                } else {
                    result
                };
                json!({"jsonrpc": "2.0", "id": identifier, "result": result})
            }
            Err(error) => protocol::error(identifier, error.code, error.message),
        })
    };

    if let Value::Array(messages) = &message {
        let mut responses = Vec::new();

        for message in messages {
            if let Some(response) = answer(message).await {
                responses.push(response);
            }
        }

        return Ok(if responses.is_empty() {
            response(StatusCode::ACCEPTED, None)
        } else {
            response(StatusCode::OK, Some(json!(responses)))
        });
    }

    let answer = answer(&message).await;
    let status = match &answer {
        None => StatusCode::ACCEPTED,
        Some(answer) if modern && answer["error"]["code"] == protocol::METHOD_NOT_FOUND => {
            StatusCode::NOT_FOUND
        }
        Some(_) => StatusCode::OK,
    };
    Ok(response(status, answer))
}

fn response(status: StatusCode, body: Option<Value>) -> Response {
    let mut response = match body {
        Some(body) => (status, Json(body)).into_response(),
        None => status.into_response(),
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}
