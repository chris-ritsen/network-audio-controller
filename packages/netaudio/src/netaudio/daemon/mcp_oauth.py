from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

from netaudio.common.preferences import read_preferences, save_preference

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

    @property
    def can_write(self) -> bool:
        return SCOPE_WRITE in self.scope.split()


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


def login_secret_configured() -> bool:
    stored = read_preferences().get(LOGIN_SECRET_PREFERENCE)
    return isinstance(stored, dict) and isinstance(stored.get("hash"), str)


def verify_login_secret(secret: str) -> bool:
    stored = read_preferences().get(LOGIN_SECRET_PREFERENCE)
    if not isinstance(stored, dict):
        return False
    try:
        salt = bytes.fromhex(stored["salt"])
        expected = bytes.fromhex(stored["hash"])
    except (KeyError, TypeError, ValueError):
        return False
    return hmac.compare_digest(_secret_digest(secret, salt), expected)


def valid_redirect_uri(uri: str) -> bool:
    parts = urlsplit(uri)
    if parts.fragment or not parts.netloc:
        return False
    if parts.scheme == "https":
        return True
    return parts.scheme == "http" and parts.hostname in {"localhost", "127.0.0.1", "::1"}


def pkce_matches(verifier: str, challenge: str) -> bool:
    if not 43 <= len(verifier) <= 128:
        return False
    computed = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    return hmac.compare_digest(computed, challenge)


def normalize_scope(requested: str | None) -> str:
    if not requested:
        return SCOPE_WRITE
    scopes = [scope for scope in requested.split() if scope in SCOPES]
    if not scopes:
        raise OAuthError("invalid_scope", f"scope must be one of {', '.join(SCOPES)}")
    return SCOPE_WRITE if SCOPE_WRITE in scopes else SCOPE_READ


class OAuthStore:
    def __init__(self):
        self._codes: dict[str, dict] = {}
        self._attempts: dict[str, list[float]] = {}

    def clients(self) -> dict:
        clients = read_preferences().get(CLIENTS_PREFERENCE)
        return dict(clients) if isinstance(clients, dict) else {}

    def register_client(self, metadata: dict) -> dict:
        redirect_uris = metadata.get("redirect_uris")
        if (
            not isinstance(redirect_uris, list)
            or not redirect_uris
            or not all(isinstance(uri, str) and valid_redirect_uri(uri) for uri in redirect_uris)
        ):
            raise OAuthError("invalid_redirect_uri", "redirect_uris must be https URLs or http://localhost URLs")
        name = metadata.get("client_name")
        if not isinstance(name, str) or not name.strip():
            name = urlsplit(redirect_uris[0]).hostname or "MCP client"
        auth_method = metadata.get("token_endpoint_auth_method") or "none"
        if auth_method not in {"none", "client_secret_post", "client_secret_basic"}:
            raise OAuthError("invalid_client_metadata", "unsupported token_endpoint_auth_method")
        client_id = secrets.token_urlsafe(16)
        client_secret = secrets.token_urlsafe(32) if auth_method != "none" else None
        record = {
            "client_name": name.strip()[:80],
            "created_at": int(time.time()),
            "redirect_uris": redirect_uris,
            "secret_hash": _hash(client_secret) if client_secret else None,
            "token_endpoint_auth_method": auth_method,
        }
        clients = self.clients()
        clients[client_id] = record
        save_preference(CLIENTS_PREFERENCE, clients)
        response = {
            "client_id": client_id,
            "client_id_issued_at": record["created_at"],
            "client_name": record["client_name"],
            "grant_types": ["authorization_code", "refresh_token"],
            "redirect_uris": redirect_uris,
            "response_types": ["code"],
            "token_endpoint_auth_method": auth_method,
        }
        if client_secret:
            response["client_secret"] = client_secret
            response["client_secret_expires_at"] = 0
        return response

    def client(self, client_id: str | None) -> tuple[str, dict]:
        clients = self.clients()
        if not isinstance(client_id, str) or client_id not in clients:
            raise OAuthError("invalid_client", "unknown client", 401)
        return client_id, clients[client_id]

    def authenticate_client(self, client_id: str | None, client_secret: str | None) -> tuple[str, dict]:
        client_id, record = self.client(client_id)
        secret_hash = record.get("secret_hash")
        if secret_hash and not (
            isinstance(client_secret, str) and hmac.compare_digest(_hash(client_secret), secret_hash)
        ):
            raise OAuthError("invalid_client", "client authentication failed", 401)
        return client_id, record

    def login_allowed(self, address: str, now: float | None = None) -> bool:
        now = now or time.time()
        attempts = [stamp for stamp in self._attempts.get(address, []) if now - stamp < LOGIN_ATTEMPT_WINDOW]
        self._attempts[address] = attempts
        return len(attempts) < LOGIN_ATTEMPT_LIMIT

    def record_login_failure(self, address: str, now: float | None = None) -> None:
        self._attempts.setdefault(address, []).append(now or time.time())

    def issue_code(self, client_id: str, redirect_uri: str, challenge: str, scope: str) -> str:
        code = secrets.token_urlsafe(32)
        self._codes[code] = {
            "client_id": client_id,
            "code_challenge": challenge,
            "expires_at": time.time() + AUTHORIZATION_CODE_LIFETIME,
            "redirect_uri": redirect_uri,
            "scope": scope,
        }
        return code

    def redeem_code(self, code: str | None, client_id: str, redirect_uri: str | None, verifier: str | None) -> str:
        record = self._codes.pop(code, None) if isinstance(code, str) else None
        if record is None or record["expires_at"] < time.time():
            raise OAuthError("invalid_grant", "authorization code is invalid or expired")
        if record["client_id"] != client_id or record["redirect_uri"] != redirect_uri:
            raise OAuthError("invalid_grant", "authorization code was issued to a different client or redirect URI")
        if not isinstance(verifier, str) or not pkce_matches(verifier, record["code_challenge"]):
            raise OAuthError("invalid_grant", "PKCE verification failed")
        return record["scope"]

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

    def grant_for(self, token: str) -> Grant | None:
        record = self.tokens().get(_hash(token))
        if record is None or record.get("kind") != "access" or record.get("expires_at", 0) < time.time():
            return None
        return Grant(
            client_id=record["client_id"],
            client_name=record.get("client_name", ""),
            scope=record["scope"],
            expires_at=record["expires_at"],
        )

    def revoke_all(self) -> int:
        count = len(self.tokens())
        save_preference(TOKENS_PREFERENCE, {})
        save_preference(CLIENTS_PREFERENCE, {})
        return count


def json_bytes(value) -> bytes:
    return json.dumps(value, default=str).encode()
