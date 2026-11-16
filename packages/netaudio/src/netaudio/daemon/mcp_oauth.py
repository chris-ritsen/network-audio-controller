from __future__ import annotations

import hashlib
import secrets
from urllib.parse import urlsplit

from netaudio.common.preferences import read_preferences, save_preference, update_preferences

CLIENTS_PREFERENCE = "mcp_oauth_clients"
LOGIN_SECRET_PREFERENCE = "mcp_login_secret"
PUBLIC_URL_PREFERENCE = "mcp_public_url"
TOKENS_PREFERENCE = "mcp_oauth_tokens"


def read_public_url() -> str | None:
    value = read_preferences().get(PUBLIC_URL_PREFERENCE)
    return value.rstrip("/") if isinstance(value, str) and value else None


def revoke_all() -> int:
    def revoke(preferences: dict) -> int:
        tokens = preferences.get(TOKENS_PREFERENCE)
        count = len(tokens) if isinstance(tokens, dict) else 0
        preferences[TOKENS_PREFERENCE] = {}
        preferences[CLIENTS_PREFERENCE] = {}
        return count

    return update_preferences(revoke)


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
    digest = hashlib.pbkdf2_hmac("sha256", secret.encode(), salt, 600_000)
    save_preference(LOGIN_SECRET_PREFERENCE, {"salt": salt.hex(), "hash": digest.hex()})
