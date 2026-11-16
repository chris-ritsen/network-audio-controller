from __future__ import annotations

import secrets

from netaudio.common.preferences import save_preference, update_preferences

TOKEN_PREFERENCE = "mcp_token"


def ensure_mcp_token() -> str:
    def ensure(preferences: dict) -> str:
        token = preferences.get(TOKEN_PREFERENCE)

        if not isinstance(token, str) or not token:
            token = secrets.token_urlsafe(32)
            preferences[TOKEN_PREFERENCE] = token

        return token

    return update_preferences(ensure)


def rotate_mcp_token() -> str:
    token = secrets.token_urlsafe(32)
    save_preference(TOKEN_PREFERENCE, token)
    return token
