use super::ApiError;
use anyhow::{Context, Result, ensure};
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header, request::Parts};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

const ENTRY_DOCUMENT: &str = "index.html";
const ASSET_PREFIX: &str = "/_assets/";
const ARCHIVE_MANIFEST: &str = "manifest.json";
const MAXIMUM_ARCHIVE_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAXIMUM_ASSET_BYTES: u64 = 16 * 1024 * 1024;
const IMMUTABLE_CACHE: &str = "public, max-age=31536000, immutable";

struct EmbeddedAsset {
    path: &'static str,
    content_type: &'static str,
    identity: &'static [u8],
    compressed: &'static [u8],
    identity_digest: &'static str,
    compressed_digest: &'static str,
}

include!(concat!(env!("OUT_DIR"), "/web_assets.rs"));

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ArchivedAsset {
    content_type: String,
    identity_digest: String,
    compressed_digest: String,
    identity_size: u64,
    compressed_size: u64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Manifest {
    generation: String,
    assets: BTreeMap<String, ArchivedAsset>,
}

pub struct WebAssets {
    archive: PathBuf,
    current: BTreeMap<&'static str, &'static EmbeddedAsset>,
}

impl WebAssets {
    pub fn open(archive: PathBuf) -> Result<Arc<Self>> {
        let generation_directory = archive.join(GENERATION);
        std::fs::create_dir_all(&generation_directory)?;
        let mut manifest = Manifest {
            generation: GENERATION.to_owned(),
            assets: BTreeMap::new(),
        };

        for asset in ASSETS {
            for (digest, bytes) in [
                (asset.identity_digest, asset.identity),
                (asset.compressed_digest, asset.compressed),
            ] {
                ensure!(
                    bytes.len() as u64 <= MAXIMUM_ASSET_BYTES,
                    "web asset exceeds size limit"
                );
                store(&generation_directory.join(digest), bytes)?;
            }

            manifest.assets.insert(
                asset.path.to_owned(),
                ArchivedAsset {
                    content_type: asset.content_type.to_owned(),
                    identity_digest: asset.identity_digest.to_owned(),
                    compressed_digest: asset.compressed_digest.to_owned(),
                    identity_size: asset.identity.len() as u64,
                    compressed_size: asset.compressed.len() as u64,
                },
            );
        }

        store(
            &generation_directory.join(ARCHIVE_MANIFEST),
            &serde_json::to_vec(&manifest)?,
        )?;
        Ok(Arc::new(Self {
            archive,
            current: ASSETS.iter().map(|asset| (asset.path, asset)).collect(),
        }))
    }

    async fn respond(&self, request: &Parts) -> Result<Option<Response>, ApiError> {
        if !matches!(request.method, Method::GET | Method::HEAD) {
            return Ok(None);
        }

        let path = percent_decode_str(request.uri.path())
            .decode_utf8()
            .map_err(|_| ApiError::invalid("path must be UTF-8"))?;

        if !safe_path(&path) {
            return Err(ApiError::not_found("not found"));
        }

        if let Some(versioned) = path.strip_prefix(ASSET_PREFIX) {
            let (generation, asset) = versioned
                .split_once('/')
                .filter(|(generation, _)| is_digest(generation))
                .ok_or_else(|| ApiError::not_found("not found"))?;

            if generation == GENERATION {
                return self.embedded(asset, request, true).map(Some);
            }

            return self.archived(generation, asset, request).await.map(Some);
        }

        if path == "/" || (application_route(&path) && prefers_document(&request.headers)) {
            return self.embedded(ENTRY_DOCUMENT, request, false).map(Some);
        }

        let relative = path.trim_start_matches('/');

        if self.current.contains_key(relative) {
            return self.embedded(relative, request, false).map(Some);
        }

        Ok(None)
    }

    fn embedded(&self, path: &str, request: &Parts, immutable: bool) -> Result<Response, ApiError> {
        let asset = self
            .current
            .get(path)
            .ok_or_else(|| ApiError::not_found("not found"))?;
        let compressed = accepts_gzip(&request.headers);
        let (body, digest) = if compressed {
            (asset.compressed, asset.compressed_digest)
        } else {
            (asset.identity, asset.identity_digest)
        };
        response(
            request,
            Representation {
                content_type: asset.content_type,
                digest,
                size: body.len() as u64,
                compressed,
                immutable,
            },
            Bytes::from_static(body),
        )
    }

    async fn archived(
        &self,
        generation: &str,
        path: &str,
        request: &Parts,
    ) -> Result<Response, ApiError> {
        let directory = self.archive.join(generation);
        let manifest = match bounded_read(
            &directory.join(ARCHIVE_MANIFEST),
            MAXIMUM_ARCHIVE_MANIFEST_BYTES,
        )
        .await?
        {
            Some(bytes) => serde_json::from_slice::<Manifest>(&bytes)
                .context("invalid archived web manifest")?,
            None => return Err(ApiError::not_found("not found")),
        };
        if manifest.generation != generation {
            return Err(anyhow::anyhow!("archived web generation is inconsistent").into());
        }
        let asset = manifest
            .assets
            .get(path)
            .ok_or_else(|| ApiError::not_found("not found"))?;
        let compressed = accepts_gzip(&request.headers);
        let (digest, size) = if compressed {
            (&asset.compressed_digest, asset.compressed_size)
        } else {
            (&asset.identity_digest, asset.identity_size)
        };
        if !is_digest(digest) || size > MAXIMUM_ASSET_BYTES {
            return Err(anyhow::anyhow!("archived web asset metadata is invalid").into());
        }
        let body = bounded_read(&directory.join(digest), MAXIMUM_ASSET_BYTES)
            .await?
            .context("archived web asset is missing")?;
        if body.len() as u64 != size || format!("{:x}", Sha256::digest(&body)) != *digest {
            return Err(anyhow::anyhow!("archived web asset is corrupt").into());
        }
        response(
            request,
            Representation {
                content_type: &asset.content_type,
                digest,
                size,
                compressed,
                immutable: true,
            },
            body.into(),
        )
    }
}

pub async fn serve(State(assets): State<Arc<WebAssets>>, request: Request, next: Next) -> Response {
    let (parts, body) = request.into_parts();

    match assets.respond(&parts).await {
        Ok(Some(response)) => response,
        Ok(None) => next.run(Request::from_parts(parts, body)).await,
        Err(error) => error.into_response(),
    }
}

struct Representation<'a> {
    content_type: &'a str,
    digest: &'a str,
    size: u64,
    compressed: bool,
    immutable: bool,
}

fn response(
    request: &Parts,
    representation: Representation<'_>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let entity_tag = format!("\"{}\"", representation.digest);
    let unmodified = request
        .headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .any(|value| {
            value.to_str().is_ok_and(|value| {
                value.split(',').any(|candidate| {
                    let candidate = candidate.trim();
                    candidate == "*"
                        || candidate.strip_prefix("W/").unwrap_or(candidate) == entity_tag
                })
            })
        });
    let mut response = if unmodified {
        StatusCode::NOT_MODIFIED.into_response()
    } else if request.method == Method::HEAD {
        Body::empty().into_response()
    } else {
        body.into_response()
    };
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(representation.content_type).context("invalid asset content type")?,
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&entity_tag).context("invalid asset entity tag")?,
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if representation.immutable {
            IMMUTABLE_CACHE
        } else {
            "no-cache"
        }),
    );
    headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("frame-ancestors 'none'"),
    );

    if representation.compressed {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }

    if !unmodified {
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from(representation.size),
        );
    }

    Ok(response)
}

pub(super) fn accepts_gzip(headers: &HeaderMap) -> bool {
    let mut wildcard = None;
    let mut gzip = None;

    for value in headers.get_all(header::ACCEPT_ENCODING) {
        let Ok(value) = value.to_str() else {
            continue;
        };

        for option in value.split(',') {
            let mut parts = option.split(';');
            let name = parts.next().unwrap_or_default().trim();
            let quality = parts
                .find_map(|part| part.trim().strip_prefix("q="))
                .map_or(1.0, |quality| quality.parse::<f32>().unwrap_or(0.0));
            let accepted = quality > 0.0 && quality <= 1.0;

            if name.eq_ignore_ascii_case("gzip") {
                gzip = Some(accepted);
            } else if name == "*" {
                wildcard = Some(accepted);
            }
        }
    }

    gzip.or(wildcard).unwrap_or(false)
}

fn prefers_document(headers: &HeaderMap) -> bool {
    headers.get_all(header::ACCEPT).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value.split(',').any(|candidate| {
                let mut parts = candidate.trim().split(';');
                parts
                    .next()
                    .is_some_and(|name| name.trim().eq_ignore_ascii_case("text/html"))
                    && parts
                        .find_map(|part| part.trim().strip_prefix("q="))
                        .is_none_or(|quality| quality.parse::<f32>().is_ok_and(|value| value > 0.0))
            })
        })
    })
}

fn application_route(path: &str) -> bool {
    let parts: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();

    match parts.as_slice() {
        ["devices"] | ["devices", _] | ["devices", _, _] | ["shure"] | ["shure", _] => true,
        [view] => matches!(
            *view,
            "routing"
                | "clock-status"
                | "network-status"
                | "subscriptions"
                | "ddm"
                | "events"
                | "settings"
                | "presets"
        ),
        _ => false,
    }
}

fn safe_path(path: &str) -> bool {
    !path.contains(['\\', '\0'])
        && !path.split('/').any(|part| matches!(part, "." | ".."))
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn store(path: &Path, bytes: &[u8]) -> Result<()> {
    match std::fs::read(path) {
        Ok(existing) if existing == bytes => return Ok(()),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("read retained web asset"),
    }

    let parent = path.parent().context("web asset path has no parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).context("retain web asset")?;
    Ok(())
}

async fn bounded_read(path: &Path, maximum: u64) -> Result<Option<Vec<u8>>> {
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("open retained web asset"),
    };
    let mut body = Vec::new();
    file.take(maximum + 1).read_to_end(&mut body).await?;
    ensure!(
        body.len() as u64 <= maximum,
        "retained web asset exceeds size limit"
    );
    Ok(Some(body))
}
