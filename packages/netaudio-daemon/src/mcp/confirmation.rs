use anyhow::{Context, Result};
use rand::RngCore;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const PREVIEW_LIFETIME: Duration = Duration::from_secs(600);
const PREVIEW_LIMIT: usize = 256;
const TOKEN_BYTES: usize = 16;

struct Preview {
    client: String,
    digest: [u8; 32],
    issued: Instant,
}

#[derive(Default)]
pub struct PreviewTokens {
    previews: Mutex<BTreeMap<String, Preview>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Redemption {
    Missing,
    Unknown,
    Changed { age: Duration },
    Accepted { age: Duration },
}

impl PreviewTokens {
    pub fn issue(&self, client: &str, digest: [u8; 32]) -> Result<String> {
        let now = Instant::now();
        let mut previews = self
            .previews
            .lock()
            .map_err(|_| anyhow::anyhow!("preview lock poisoned"))?;
        previews.retain(|_, preview| now.duration_since(preview.issued) < PREVIEW_LIFETIME);

        if previews.len() >= PREVIEW_LIMIT {
            let oldest = previews
                .iter()
                .min_by_key(|(_, preview)| preview.issued)
                .map(|(token, _)| token.clone())
                .context("preview capacity has no entries")?;
            previews.remove(&oldest);
        }

        let mut bytes = [0; TOKEN_BYTES];
        rand::rngs::OsRng.try_fill_bytes(&mut bytes)?;
        let token = netaudio_core::bytes::hexadecimal(&bytes);
        anyhow::ensure!(!previews.contains_key(&token), "preview token collision");
        previews.insert(
            token.clone(),
            Preview {
                client: client.to_owned(),
                digest,
                issued: now,
            },
        );
        Ok(token)
    }

    pub fn redeem(
        &self,
        token: Option<&str>,
        client: &str,
        digest: [u8; 32],
    ) -> Result<Redemption> {
        let Some(token) = token.filter(|token| !token.is_empty()) else {
            return Ok(Redemption::Missing);
        };
        let mut previews = self
            .previews
            .lock()
            .map_err(|_| anyhow::anyhow!("preview lock poisoned"))?;
        let Some(preview) = previews.get(token) else {
            return Ok(Redemption::Unknown);
        };

        if preview.client != client {
            return Ok(Redemption::Unknown);
        }

        let preview = previews
            .remove(token)
            .context("preview disappeared while locked")?;
        let age = preview.issued.elapsed();

        if age >= PREVIEW_LIFETIME {
            return Ok(Redemption::Unknown);
        }

        if preview.digest != digest {
            return Ok(Redemption::Changed { age });
        }

        Ok(Redemption::Accepted { age })
    }
}

pub fn preview_digest(operation: &str, request: &Value, preview: &Value) -> [u8; 32] {
    let bound: BTreeMap<_, _> = preview
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(key, _)| !["preview_token", "next", "confirmation"].contains(&key.as_str()))
        .collect();
    let mut value =
        serde_json::json!({"operation": operation, "request": request, "preview": bound});
    value.sort_all_objects();
    Sha256::digest(value.to_string().as_bytes()).into()
}
