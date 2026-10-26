use axum::http::{HeaderMap, StatusCode};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

pub const LEGACY_VERSION: &str = "2025-06-18";
pub const MODERN_VERSION: &str = "2026-07-28";
pub const META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
pub const META_CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
pub const META_SERVER_INFO: &str = "io.modelcontextprotocol/serverInfo";
pub const META_SUBSCRIPTION_ID: &str = "io.modelcontextprotocol/subscriptionId";
pub const PARSE_ERROR: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;
pub const HEADER_MISMATCH: i32 = -32020;
pub const UNSUPPORTED_PROTOCOL_VERSION: i32 = -32022;
const LIST_CACHE_MILLISECONDS: u32 = 60_000;

pub struct Rejection {
    pub status: StatusCode,
    pub body: Value,
}

impl Rejection {
    fn invalid(identifier: &Value, code: i32, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: error(identifier.clone(), code, message),
        }
    }
}

pub fn modern_request(message: &Value, headers: &HeaderMap) -> bool {
    if message.is_array() {
        return header(headers, "mcp-protocol-version") == Some(MODERN_VERSION);
    }

    message.is_object()
        && (message
            .pointer("/params/_meta")
            .and_then(Value::as_object)
            .is_some_and(|metadata| metadata.contains_key(META_PROTOCOL_VERSION))
            || header(headers, "mcp-protocol-version") == Some(MODERN_VERSION))
}

pub fn validate_modern(message: &Value, headers: &HeaderMap) -> Result<(), Rejection> {
    let identifier = message.get("id").unwrap_or(&Value::Null);
    let Some(method) = message
        .get("method")
        .and_then(Value::as_str)
        .filter(|_| message.get("jsonrpc").and_then(Value::as_str) == Some("2.0"))
    else {
        return Err(Rejection::invalid(
            identifier,
            INVALID_REQUEST,
            "the body must be a single JSON-RPC request",
        ));
    };
    let metadata = message.pointer("/params/_meta");
    let Some(body_version) = metadata
        .and_then(|metadata| metadata.get(META_PROTOCOL_VERSION))
        .and_then(Value::as_str)
    else {
        return Err(Rejection::invalid(
            identifier,
            INVALID_PARAMS,
            format!("_meta.{META_PROTOCOL_VERSION} is required"),
        ));
    };
    let header_version = header(headers, "mcp-protocol-version").ok_or_else(|| {
        Rejection::invalid(
            identifier,
            HEADER_MISMATCH,
            "MCP-Protocol-Version header is required",
        )
    })?;

    if header_version != body_version {
        return Err(Rejection::invalid(
            identifier,
            HEADER_MISMATCH,
            format!(
                "MCP-Protocol-Version header {header_version:?} does not match body version {body_version:?}"
            ),
        ));
    }

    if body_version != MODERN_VERSION {
        let mut rejection = Rejection::invalid(
            identifier,
            UNSUPPORTED_PROTOCOL_VERSION,
            "Unsupported protocol version",
        );
        rejection.body["error"]["data"] = json!({
            "supported": [MODERN_VERSION, LEGACY_VERSION],
            "requested": body_version,
        });
        return Err(rejection);
    }

    if !metadata
        .and_then(|metadata| metadata.get(META_CLIENT_CAPABILITIES))
        .is_some_and(Value::is_object)
    {
        return Err(Rejection::invalid(
            identifier,
            INVALID_PARAMS,
            format!("_meta.{META_CLIENT_CAPABILITIES} is required"),
        ));
    }

    let header_method = header(headers, "mcp-method");

    if header_method != Some(method) {
        return Err(Rejection::invalid(
            identifier,
            HEADER_MISMATCH,
            format!("Mcp-Method header {header_method:?} does not match body method {method:?}"),
        ));
    }

    let name_field = match method {
        "tools/call" | "prompts/get" => Some("name"),
        "resources/read" => Some("uri"),
        _ => None,
    };

    if let Some(field) = name_field {
        let body_name = message.get("params").and_then(|params| params.get(field));
        let raw_name = header(headers, "mcp-name");
        let name = raw_name.and_then(decode_name);

        if name.is_none() || name.as_deref() != body_name.and_then(Value::as_str) {
            return Err(Rejection::invalid(
                identifier,
                HEADER_MISMATCH,
                format!("Mcp-Name header {raw_name:?} does not match body {field} {body_name:?}"),
            ));
        }
    }

    Ok(())
}

pub fn complete_result(method: &str, mut result: Value, identity: Value) -> Value {
    result["resultType"] = json!("complete");

    if !result.get("_meta").is_some_and(Value::is_object) {
        result["_meta"] = json!({});
    }

    result["_meta"][META_SERVER_INFO] = identity;
    let lifetime = match method {
        "server/discover"
        | "tools/list"
        | "prompts/list"
        | "resources/list"
        | "resources/templates/list" => Some(LIST_CACHE_MILLISECONDS),
        "resources/read" => Some(0),
        _ => None,
    };

    if let Some(lifetime) = lifetime {
        result["ttlMs"] = json!(lifetime);
        result["cacheScope"] = json!("private");
    }

    result
}

pub fn error(identifier: Value, code: i32, message: impl Into<String>) -> Value {
    json!({"jsonrpc": "2.0", "id": identifier, "error": {"code": code, "message": message.into()}})
}

pub fn tool_result(payload: Value, is_error: bool) -> Value {
    json!({
        "content": [{"type": "text", "text": payload.to_string()}],
        "isError": is_error,
    })
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|header| header.to_str().ok())
}

fn decode_name(value: &str) -> Option<String> {
    match value
        .strip_prefix("=?base64?")
        .and_then(|value| value.strip_suffix("?="))
    {
        Some(encoded) => String::from_utf8(STANDARD.decode(encoded).ok()?).ok(),
        None => Some(value.to_owned()),
    }
}
