use super::credentials::{
    CLIENT_IDENTIFIER_BYTES, TOKEN_BYTES, constant_time_equal, decode_hexadecimal, digest,
    secret_digest, secret_record, token, valid_redirect_uri, verify_challenge,
};
use super::{
    ACCESS_TOKEN_LIFETIME_SECONDS, AUTHORIZATION_CODE_LIFETIME_SECONDS, AuthorizationError,
    CLIENTS_PREFERENCE, Grant, LOGIN_ATTEMPT_LIMIT, LOGIN_ATTEMPT_WINDOW_SECONDS,
    LOGIN_SECRET_PREFERENCE, PUBLIC_URL_PREFERENCE, REFRESH_TOKEN_LIFETIME_SECONDS,
    STATIC_TOKEN_PREFERENCE, TOKENS_PREFERENCE, WRITE_SCOPE,
};
use crate::preferences::Preferences;
use anyhow::{Context, Result};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use url::Url;

struct AuthorizationCode {
    client: String,
    redirect: String,
    challenge: String,
    scope: String,
    expires: Instant,
}

#[derive(Default)]
struct TransientState {
    codes: BTreeMap<String, AuthorizationCode>,
    attempts: BTreeMap<String, Vec<Instant>>,
}

pub struct AuthorizationStore {
    preferences: Arc<Preferences>,
    transient: Mutex<TransientState>,
}

impl AuthorizationStore {
    pub fn new(preferences: Arc<Preferences>) -> Self {
        Self {
            preferences,
            transient: Mutex::new(TransientState::default()),
        }
    }

    pub fn public_url(&self) -> Result<Option<String>, AuthorizationError> {
        Ok(self
            .preferences
            .snapshot()?
            .get(PUBLIC_URL_PREFERENCE)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| value.trim_end_matches('/').to_owned()))
    }

    pub fn client(&self, client_id: Option<&str>) -> Result<(String, Value), AuthorizationError> {
        let client_id =
            client_id.ok_or_else(|| AuthorizationError::unauthorized("unknown client"))?;
        let preferences = self.preferences.snapshot()?;
        let client = preferences
            .get(CLIENTS_PREFERENCE)
            .and_then(Value::as_object)
            .and_then(|clients| clients.get(client_id))
            .filter(|client| client.is_object())
            .cloned()
            .ok_or_else(|| AuthorizationError::unauthorized("unknown client"))?;

        Ok((client_id.to_owned(), client))
    }

    pub fn authenticate_client(
        &self,
        client_id: Option<&str>,
        client_secret: Option<&str>,
    ) -> Result<(String, Value), AuthorizationError> {
        let (client_id, record) = self.client(client_id)?;

        if let Some(expected) = record.get("secret_hash").and_then(Value::as_str)
            && !client_secret.is_some_and(|secret| {
                constant_time_equal(digest(secret).as_bytes(), expected.as_bytes())
            })
        {
            return Err(AuthorizationError::unauthorized(
                "client authentication failed",
            ));
        }

        Ok((client_id, record))
    }

    pub fn register_client(&self, metadata: &Value) -> Result<Value, AuthorizationError> {
        let redirects = metadata
            .get("redirect_uris")
            .and_then(Value::as_array)
            .filter(|uris| {
                !uris.is_empty()
                    && uris
                        .iter()
                        .all(|uri| uri.as_str().is_some_and(valid_redirect_uri))
            })
            .ok_or_else(|| {
                AuthorizationError::new(
                    "invalid_redirect_uri",
                    "redirect_uris must be https URLs or http://localhost URLs",
                )
            })?;
        let name = metadata
            .get("client_name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .or_else(|| {
                Url::parse(redirects[0].as_str()?)
                    .ok()?
                    .host_str()
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "MCP client".to_owned());
        let method = metadata
            .get("token_endpoint_auth_method")
            .and_then(Value::as_str)
            .filter(|method| !method.is_empty())
            .unwrap_or("none");

        if !matches!(
            method,
            "none" | "client_secret_post" | "client_secret_basic"
        ) {
            return Err(AuthorizationError::new(
                "invalid_client_metadata",
                "unsupported token_endpoint_auth_method",
            ));
        }

        let identifier = token(CLIENT_IDENTIFIER_BYTES);
        let secret = (method != "none").then(|| token(TOKEN_BYTES));
        let issued = chrono::Utc::now().timestamp();
        let record = json!({
            "client_name": name,
            "created_at": issued,
            "redirect_uris": redirects,
            "secret_hash": secret.as_deref().map(digest),
            "token_endpoint_auth_method": method,
        });
        self.preferences.update(|document| {
            table(document, CLIENTS_PREFERENCE)?.insert(identifier.clone(), record);
            Ok(())
        })?;
        let mut response = json!({
            "client_id": identifier,
            "client_id_issued_at": issued,
            "client_name": name,
            "grant_types": ["authorization_code", "refresh_token"],
            "redirect_uris": redirects,
            "response_types": ["code"],
            "token_endpoint_auth_method": method,
        });

        if let Some(secret) = secret {
            response["client_secret"] = json!(secret);
            response["client_secret_expires_at"] = json!(0);
        }

        Ok(response)
    }

    pub fn login_secret_configured(&self) -> Result<bool, AuthorizationError> {
        Ok(self
            .preferences
            .snapshot()?
            .get(LOGIN_SECRET_PREFERENCE)
            .and_then(|record| record.get("hash"))
            .is_some_and(Value::is_string))
    }

    pub fn set_login_secret(&self, secret: &str) -> Result<(), AuthorizationError> {
        self.preferences
            .set(LOGIN_SECRET_PREFERENCE.to_owned(), secret_record(secret)?)?;
        Ok(())
    }

    pub fn verify_login_secret(&self, secret: &str) -> Result<bool, AuthorizationError> {
        let preferences = self.preferences.snapshot()?;
        let Some(record) = preferences.get(LOGIN_SECRET_PREFERENCE) else {
            return Ok(false);
        };
        let salt = record
            .get("salt")
            .and_then(Value::as_str)
            .and_then(decode_hexadecimal);
        let expected = record
            .get("hash")
            .and_then(Value::as_str)
            .and_then(decode_hexadecimal);

        match (salt, expected) {
            (Some(salt), Some(expected)) => Ok(constant_time_equal(
                &secret_digest(secret, &salt),
                &expected,
            )),
            _ => Ok(false),
        }
    }

    pub fn reserve_login_attempt(&self, address: &str) -> Result<bool, AuthorizationError> {
        let now = Instant::now();
        let window = Duration::from_secs(LOGIN_ATTEMPT_WINDOW_SECONDS);
        let mut transient = self
            .transient
            .lock()
            .map_err(|_| anyhow::anyhow!("authorization lock poisoned"))?;
        transient.attempts.retain(|_, attempts| {
            attempts.retain(|attempt| now.duration_since(*attempt) < window);
            !attempts.is_empty()
        });
        let attempts = transient.attempts.entry(address.to_owned()).or_default();

        if attempts.len() >= LOGIN_ATTEMPT_LIMIT {
            return Ok(false);
        }

        attempts.push(now);
        Ok(true)
    }

    pub fn login_succeeded(&self, address: &str) -> Result<(), AuthorizationError> {
        self.transient
            .lock()
            .map_err(|_| anyhow::anyhow!("authorization lock poisoned"))?
            .attempts
            .remove(address);
        Ok(())
    }

    pub fn issue_code(
        &self,
        client: String,
        redirect: String,
        challenge: String,
        scope: String,
    ) -> Result<String, AuthorizationError> {
        let code = token(TOKEN_BYTES);
        let now = Instant::now();
        let mut transient = self
            .transient
            .lock()
            .map_err(|_| anyhow::anyhow!("authorization lock poisoned"))?;
        transient.codes.retain(|_, entry| entry.expires > now);
        transient.codes.insert(
            digest(&code),
            AuthorizationCode {
                client,
                redirect,
                challenge,
                scope,
                expires: now + Duration::from_secs(AUTHORIZATION_CODE_LIFETIME_SECONDS),
            },
        );
        Ok(code)
    }

    pub fn redeem_code(
        &self,
        code: Option<&str>,
        client: &str,
        redirect: Option<&str>,
        verifier: Option<&str>,
    ) -> Result<String, AuthorizationError> {
        let mut transient = self
            .transient
            .lock()
            .map_err(|_| anyhow::anyhow!("authorization lock poisoned"))?;
        let record = code
            .and_then(|code| transient.codes.remove(&digest(code)))
            .filter(|record| record.expires > Instant::now())
            .ok_or_else(|| {
                AuthorizationError::new("invalid_grant", "authorization code is invalid or expired")
            })?;

        if record.client != client || Some(record.redirect.as_str()) != redirect {
            return Err(AuthorizationError::new(
                "invalid_grant",
                "authorization code was issued to a different client or redirect URI",
            ));
        }

        if !verifier.is_some_and(|verifier| verify_challenge(verifier, &record.challenge)) {
            return Err(AuthorizationError::new(
                "invalid_grant",
                "PKCE verification failed",
            ));
        }

        Ok(record.scope)
    }

    pub fn issue_tokens(
        &self,
        client: &str,
        name: &str,
        scope: &str,
    ) -> Result<Value, AuthorizationError> {
        Ok(self.preferences.update(|document| {
            Ok(insert_tokens(
                table(document, TOKENS_PREFERENCE)?,
                client,
                name,
                scope,
            ))
        })?)
    }

    pub fn refresh(
        &self,
        refresh: Option<&str>,
        client: &str,
    ) -> Result<Value, AuthorizationError> {
        let key = refresh.map(digest);
        Ok(self.preferences.update(|document| {
            let tokens = table(document, TOKENS_PREFERENCE)?;
            let record = key
                .as_ref()
                .and_then(|key| tokens.get(key))
                .filter(|record| {
                    record["kind"] == "refresh"
                        && record["expires_at"]
                            .as_f64()
                            .is_some_and(|expires| expires > now_seconds())
                })
                .cloned()
                .ok_or_else(|| {
                    AuthorizationError::new("invalid_grant", "refresh token is invalid or expired")
                })?;

            if record["client_id"].as_str() != Some(client) {
                return Err(AuthorizationError::new(
                    "invalid_grant",
                    "refresh token belongs to a different client",
                )
                .into());
            }

            let scope = record["scope"]
                .as_str()
                .context("stored refresh token has no scope")?;
            let name = record["client_name"].as_str().unwrap_or_default();

            if let Some(key) = key {
                tokens.remove(&key);
            }

            Ok(insert_tokens(tokens, client, name, scope))
        })?)
    }

    pub fn revoke(&self, presented: Option<&str>) -> Result<(), AuthorizationError> {
        let Some(presented) = presented else {
            return Ok(());
        };

        self.preferences.update(|document| {
            table(document, TOKENS_PREFERENCE)?.remove(&digest(presented));
            Ok(())
        })?;
        Ok(())
    }

    pub fn revoke_all(&self) -> Result<usize, AuthorizationError> {
        Ok(self.preferences.update(|document| {
            let count = table(document, TOKENS_PREFERENCE)?.len();
            document.insert(TOKENS_PREFERENCE.to_owned(), json!({}));
            document.insert(CLIENTS_PREFERENCE.to_owned(), json!({}));
            Ok(count)
        })?)
    }

    pub fn grant(&self, header: Option<&str>) -> Result<Option<Grant>, AuthorizationError> {
        let Some((scheme, presented)) = header.and_then(|value| value.trim().split_once(' '))
        else {
            return Ok(None);
        };
        let presented = presented.trim();

        if !scheme.eq_ignore_ascii_case("bearer") || presented.is_empty() {
            return Ok(None);
        }

        let preferences = self.preferences.snapshot()?;

        if preferences
            .get(STATIC_TOKEN_PREFERENCE)
            .and_then(Value::as_str)
            .is_some_and(|expected| {
                !expected.is_empty()
                    && constant_time_equal(expected.as_bytes(), presented.as_bytes())
            })
        {
            return Ok(Some(Grant {
                client_id: "local-token".to_owned(),
                client_name: "netaudio daemon mcp-token".to_owned(),
                scope: WRITE_SCOPE.to_owned(),
                expires_at: f64::INFINITY,
            }));
        }

        let Some(record) = preferences
            .get(TOKENS_PREFERENCE)
            .and_then(|tokens| tokens.get(digest(presented)))
            .filter(|record| {
                record["kind"] == "access"
                    && record["expires_at"]
                        .as_f64()
                        .is_some_and(|expires| expires > now_seconds())
            })
        else {
            return Ok(None);
        };
        let grant =
            serde_json::from_value(record.clone()).context("stored access grant is invalid")?;
        Ok(Some(grant))
    }

    pub fn ensure_static_token(&self) -> Result<String, AuthorizationError> {
        Ok(self.preferences.update(|document| {
            if let Some(existing) = document
                .get(STATIC_TOKEN_PREFERENCE)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                return Ok(existing.to_owned());
            }

            let value = token(TOKEN_BYTES);
            document.insert(STATIC_TOKEN_PREFERENCE.to_owned(), json!(value));
            Ok(value)
        })?)
    }
}

fn table<'a>(
    document: &'a mut Map<String, Value>,
    key: &str,
) -> Result<&'a mut Map<String, Value>> {
    document
        .entry(key.to_owned())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .with_context(|| format!("preference {key} must be an object"))
}

fn now_seconds() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1_000.0
}

fn insert_tokens(tokens: &mut Map<String, Value>, client: &str, name: &str, scope: &str) -> Value {
    let now = now_seconds();
    let access = token(TOKEN_BYTES);
    let refresh = token(TOKEN_BYTES);
    tokens.retain(|_, record| {
        record["expires_at"]
            .as_f64()
            .is_some_and(|expires| expires > now)
    });

    for (value, kind, lifetime) in [
        (&access, "access", ACCESS_TOKEN_LIFETIME_SECONDS),
        (&refresh, "refresh", REFRESH_TOKEN_LIFETIME_SECONDS),
    ] {
        tokens.insert(
            digest(value),
            json!({
                "client_id": client,
                "client_name": name,
                "expires_at": now + lifetime as f64,
                "kind": kind,
                "scope": scope,
            }),
        );
    }

    json!({
        "access_token": access,
        "expires_in": ACCESS_TOKEN_LIFETIME_SECONDS,
        "refresh_token": refresh,
        "scope": scope,
        "token_type": "Bearer",
    })
}
