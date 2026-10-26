use crate::authorization::AuthorizationError;
use axum::http::{HeaderMap, header};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;
use std::collections::BTreeMap;
use url::form_urlencoded;

pub type Form = BTreeMap<String, String>;

pub fn parse_query(query: &str) -> Form {
    let mut fields = Form::new();

    for (name, value) in form_urlencoded::parse(query.as_bytes()) {
        fields
            .entry(name.into_owned())
            .or_insert_with(|| value.into_owned());
    }

    fields
}

pub fn parse_json(body: &[u8]) -> Result<Value, AuthorizationError> {
    let value: Value = if body.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_slice(body).map_err(|error| {
            AuthorizationError::new("invalid_request", format!("invalid json: {error}"))
        })?
    };

    if !value.is_object() {
        return Err(AuthorizationError::new(
            "invalid_request",
            "body must be a json object",
        ));
    }

    Ok(value)
}

pub fn parse_body(body: &[u8], headers: &HeaderMap) -> Result<Form, AuthorizationError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());

    if content_type.is_some_and(|value| value.contains("application/json")) {
        return parse_json(body)?
            .as_object()
            .expect("validated JSON object")
            .iter()
            .map(|(name, value)| {
                let value = value.as_str().ok_or_else(|| {
                    AuthorizationError::new("invalid_request", format!("{name} must be a string"))
                })?;
                Ok((name.clone(), value.to_owned()))
            })
            .collect();
    }

    let text = std::str::from_utf8(body)
        .map_err(|_| AuthorizationError::new("invalid_request", "form body must be UTF-8"))?;
    Ok(parse_query(text))
}

pub fn basic_credentials(
    headers: &HeaderMap,
) -> Result<Option<(String, String)>, AuthorizationError> {
    let Some(header) = headers.get(header::AUTHORIZATION) else {
        return Ok(None);
    };
    let header = header
        .to_str()
        .map_err(|_| AuthorizationError::unauthorized("malformed basic credentials"))?;
    let Some((scheme, credentials)) = header.split_once(' ') else {
        return Ok(None);
    };

    if !scheme.eq_ignore_ascii_case("basic") {
        return Ok(None);
    }

    let decoded = STANDARD
        .decode(credentials.trim())
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .ok_or_else(|| AuthorizationError::unauthorized("malformed basic credentials"))?;
    let (identifier, secret) = decoded
        .split_once(':')
        .ok_or_else(|| AuthorizationError::unauthorized("malformed basic credentials"))?;

    Ok(Some((
        decode_component(identifier)?,
        decode_component(secret)?,
    )))
}

fn decode_component(value: &str) -> Result<String, AuthorizationError> {
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| AuthorizationError::unauthorized("malformed basic credentials"))
}
