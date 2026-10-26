mod contract;
mod render;

use super::ApiError;
use anyhow::{Context, Result};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use flate2::{Compression, write::GzEncoder};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;

struct Representation {
    bytes: Bytes,
    tag: HeaderValue,
}

impl Representation {
    fn new(bytes: Vec<u8>) -> Result<Self> {
        let tag = HeaderValue::from_str(&format!("\"{:x}\"", Sha256::digest(&bytes)))?;
        Ok(Self {
            bytes: Bytes::from(bytes),
            tag,
        })
    }
}

struct Document {
    identity: Representation,
    compressed: Representation,
    content_type: &'static str,
}

impl Document {
    fn new(bytes: Vec<u8>, content_type: &'static str) -> Result<Self> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&bytes)?;
        Ok(Self {
            identity: Representation::new(bytes)?,
            compressed: Representation::new(encoder.finish()?)?,
            content_type,
        })
    }

    fn respond(&self, method: &Method, headers: &HeaderMap) -> Response {
        let compressed = super::web::accepts_gzip(headers);
        let representation = if compressed {
            &self.compressed
        } else {
            &self.identity
        };
        let unmodified = headers.get_all(header::IF_NONE_MATCH).iter().any(|value| {
            value.to_str().is_ok_and(|value| {
                value.split(',').any(|candidate| {
                    let candidate = candidate.trim();
                    candidate == "*"
                        || candidate.strip_prefix("W/").unwrap_or(candidate)
                            == representation.tag.to_str().unwrap_or_default()
                })
            })
        });
        let mut response = if unmodified {
            StatusCode::NOT_MODIFIED.into_response()
        } else if method == Method::HEAD {
            Body::empty().into_response()
        } else {
            representation.bytes.clone().into_response()
        };
        let headers = response.headers_mut();
        headers.insert(header::ETAG, representation.tag.clone());
        headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(self.content_type),
        );
        headers.insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(
                "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
            ),
        );
        headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );

        if compressed {
            headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        }

        if !unmodified {
            headers.insert(
                header::CONTENT_LENGTH,
                HeaderValue::from(representation.bytes.len()),
            );
        }

        response
    }
}

type Documents = BTreeMap<String, Document>;

pub fn router() -> Result<Router> {
    let contract = contract::generate().context("generate HTTP API contract")?;
    let mut documents = Documents::new();
    documents.insert(
        "/openapi.json".to_owned(),
        Document::new(serde_json::to_vec_pretty(&contract)?, "application/json")?,
    );

    for (path, html) in render::pages(&contract)? {
        documents.insert(
            percent_encoding::percent_decode_str(&path)
                .decode_utf8()?
                .into_owned(),
            Document::new(html.into_bytes(), "text/html; charset=utf-8")?,
        );
    }

    Ok(Router::new()
        .route("/openapi.json", get(read))
        .route("/api/docs", get(read))
        .route("/api/docs/operations/{operation}", get(read))
        .route("/api/docs/schemas/{schema}", get(read))
        .route("/api/docs/guides/{guide}", get(read))
        .with_state(Arc::new(documents)))
}

async fn read(
    State(documents): State<Arc<Documents>>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let path = percent_encoding::percent_decode_str(uri.path())
        .decode_utf8()
        .map_err(|_| ApiError::invalid("path must be UTF-8"))?;
    documents
        .get(path.as_ref())
        .map(|document| document.respond(&method, &headers))
        .ok_or_else(|| ApiError::not_found("documentation not found"))
}
