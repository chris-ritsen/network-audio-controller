from __future__ import annotations

import hashlib
import secrets
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

from netaudio.common.preferences import read_preferences, save_preference, update_preferences

ACCESS_TOKEN_LIFETIME = 60 * 60
AUTHORIZATION_CODE_LIFETIME = 10 * 60
CLIENTS_PREFERENCE = "mcp_oauth_clients"
LOGIN_ATTEMPT_LIMIT = 5
LOGIN_ATTEMPT_WINDOW = 5 * 60
LOGIN_SECRET_PREFERENCE = "mcp_login_secret"
PUBLIC_URL_PREFERENCE = "mcp_public_url"
REFRESH_TOKEN_LIFETIME = 30 * 24 * 60 * 60
SCOPE_READ = "netaudio:read"
SCOPE_WRITE = "netaudio:write"
SCOPES = (SCOPE_READ, SCOPE_WRITE)
TOKENS_PREFERENCE = "mcp_oauth_tokens"


class OAuthError(Exception):
    def __init__(self, error: str, description: str, status: int = 400):
        super().__init__(description)
        self.error = error
        self.description = description
        self.status = status


@dataclass(frozen=True)
class Grant:
    client_id: str
    client_name: str
    scope: str
    expires_at: float


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def read_public_url() -> str | None:
    value = read_preferences().get(PUBLIC_URL_PREFERENCE)
    return value.rstrip("/") if isinstance(value, str) and value else None


def save_public_url(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.netloc or parts.query or parts.fragment:
        raise ValueError("The public URL must look like https://example.com")
    normalized = f"{parts.scheme}://{parts.netloc}{parts.path.rstrip('/')}"
    save_preference(PUBLIC_URL_PREFERENCE, normalized)
    return normalized


def set_login_secret(secret: str) -> None:
    if len(secret) < 12:
        raise ValueError("The login secret must be at least 12 characters")
    salt = secrets.token_bytes(16)
    save_preference(LOGIN_SECRET_PREFERENCE, {"salt": salt.hex(), "hash": _secret_digest(secret, salt).hex()})


def _secret_digest(secret: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", secret.encode(), salt, 600_000)


class OAuthStore:
    def __init__(self):
        self._codes: dict[str, dict] = {}
        self._attempts: dict[str, list[float]] = {}

    def clients(self) -> dict:
        clients = read_preferences().get(CLIENTS_PREFERENCE)
        return dict(clients) if isinstance(clients, dict) else {}

    def client(self, client_id: str | None) -> tuple[str, dict]:
        clients = self.clients()
        if not isinstance(client_id, str) or client_id not in clients:
            raise OAuthError("invalid_client", "unknown client", 401)
        return client_id, clients[client_id]

    def tokens(self) -> dict:
        tokens = read_preferences().get(TOKENS_PREFERENCE)
        return dict(tokens) if isinstance(tokens, dict) else {}

    def issue_tokens(self, client_id: str, client_name: str, scope: str) -> dict:
        now = time.time()
        access = secrets.token_urlsafe(32)
        refresh = secrets.token_urlsafe(32)
        tokens = {key: value for key, value in self.tokens().items() if value.get("expires_at", 0) > now}
        tokens[_hash(access)] = {
            "client_id": client_id,
            "client_name": client_name,
            "expires_at": now + ACCESS_TOKEN_LIFETIME,
            "kind": "access",
            "scope": scope,
        }
        tokens[_hash(refresh)] = {
            "client_id": client_id,
            "client_name": client_name,
            "expires_at": now + REFRESH_TOKEN_LIFETIME,
            "kind": "refresh",
            "scope": scope,
        }
        save_preference(TOKENS_PREFERENCE, tokens)
        return {
            "access_token": access,
            "expires_in": ACCESS_TOKEN_LIFETIME,
            "refresh_token": refresh,
            "scope": scope,
            "token_type": "Bearer",
        }

    def refresh(self, refresh_token: str | None, client_id: str) -> dict:
        tokens = self.tokens()
        key = _hash(refresh_token) if isinstance(refresh_token, str) else None
        record = tokens.get(key) if key else None
        if record is None or record.get("kind") != "refresh" or record.get("expires_at", 0) < time.time():
            raise OAuthError("invalid_grant", "refresh token is invalid or expired")
        if record.get("client_id") != client_id:
            raise OAuthError("invalid_grant", "refresh token belongs to a different client")
        del tokens[key]
        save_preference(TOKENS_PREFERENCE, tokens)
        return self.issue_tokens(client_id, record.get("client_name", ""), record["scope"])

    def revoke(self, token: str | None) -> None:
        if not isinstance(token, str):
            return
        tokens = self.tokens()
        if tokens.pop(_hash(token), None) is not None:
            save_preference(TOKENS_PREFERENCE, tokens)

    def revoke_all(self) -> int:
        def revoke(preferences: dict) -> int:
            tokens = preferences.get(TOKENS_PREFERENCE)
            count = len(tokens) if isinstance(tokens, dict) else 0
            preferences[TOKENS_PREFERENCE] = {}
            preferences[CLIENTS_PREFERENCE] = {}
            return count

        return update_preferences(revoke)
