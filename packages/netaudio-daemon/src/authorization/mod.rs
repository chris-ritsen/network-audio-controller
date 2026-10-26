mod credentials;
mod store;

pub use credentials::{constant_time_equal, normalize_scope, valid_redirect_uri, verify_challenge};
pub use store::AuthorizationStore;

use serde::{Deserialize, Serialize};
use std::fmt;

pub const READ_SCOPE: &str = "netaudio:read";
pub const WRITE_SCOPE: &str = "netaudio:write";
pub const SCOPES: [&str; 2] = [READ_SCOPE, WRITE_SCOPE];
pub const CLIENTS_PREFERENCE: &str = "mcp_oauth_clients";
pub const TOKENS_PREFERENCE: &str = "mcp_oauth_tokens";
pub const LOGIN_SECRET_PREFERENCE: &str = "mcp_login_secret";
pub const STATIC_TOKEN_PREFERENCE: &str = "mcp_token";
pub const PUBLIC_URL_PREFERENCE: &str = "mcp_public_url";
pub const ACCESS_TOKEN_LIFETIME_SECONDS: u64 = 3_600;
pub const REFRESH_TOKEN_LIFETIME_SECONDS: u64 = 2_592_000;
pub const AUTHORIZATION_CODE_LIFETIME_SECONDS: u64 = 600;
pub const LOGIN_ATTEMPT_LIMIT: usize = 5;
pub const LOGIN_ATTEMPT_WINDOW_SECONDS: u64 = 300;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Grant {
    pub client_id: String,
    pub client_name: String,
    pub scope: String,
    pub expires_at: f64,
}

impl Grant {
    pub fn can_write(&self) -> bool {
        self.scope
            .split_whitespace()
            .any(|scope| scope == WRITE_SCOPE)
    }
}

#[derive(Clone, Debug)]
pub struct AuthorizationError {
    pub status: u16,
    pub error: &'static str,
    pub description: String,
}

impl AuthorizationError {
    pub fn new(error: &'static str, description: impl Into<String>) -> Self {
        Self {
            status: 400,
            error,
            description: description.into(),
        }
    }

    pub fn unauthorized(description: impl Into<String>) -> Self {
        Self {
            status: 401,
            error: "invalid_client",
            description: description.into(),
        }
    }
}

impl fmt::Display for AuthorizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.description)
    }
}

impl std::error::Error for AuthorizationError {}

impl From<anyhow::Error> for AuthorizationError {
    fn from(error: anyhow::Error) -> Self {
        if let Some(authorization) = error.downcast_ref::<Self>() {
            return authorization.clone();
        }

        tracing::error!(%error, "authorization operation failed");

        Self {
            status: 503,
            error: "temporarily_unavailable",
            description: "authorization store is unavailable".to_owned(),
        }
    }
}
