from __future__ import annotations

import hmac
import secrets

from netaudio.common.preferences import read_preferences, save_preference

TOKEN_PREFERENCE = "mcp_token"


def read_mcp_token() -> str | None:
    token = read_preferences().get(TOKEN_PREFERENCE)
    return token if isinstance(token, str) and token else None


def ensure_mcp_token() -> str:
    token = read_mcp_token()
    if token is None:
        token = rotate_mcp_token()
    return token


def rotate_mcp_token() -> str:
    token = secrets.token_urlsafe(32)
    save_preference(TOKEN_PREFERENCE, token)
    return token


def authorization_matches(header_value: str | None, token: str | None) -> bool:
    if not token or not isinstance(header_value, str):
        return False
    scheme, _, presented = header_value.strip().partition(" ")
    if scheme.lower() != "bearer":
        return False
    return hmac.compare_digest(presented.strip(), token)
