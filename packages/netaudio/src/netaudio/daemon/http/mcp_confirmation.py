from __future__ import annotations

import hashlib
import json
import secrets
import time
from contextvars import ContextVar
from dataclasses import dataclass

INTERNAL_CONFIRMATION: ContextVar[bool] = ContextVar("mcp_internal_confirmation", default=False)
PREVIEW_TOKEN_SECONDS = 600
PREVIEW_TOKEN_LIMIT = 256
UNBOUND_PREVIEW_KEYS = frozenset({"preview_token", "next", "confirmation"})
REFUSALS = {
    "missing": (
        "Not changed: confirmed=true needs the preview_token from a preview of this exact change, shown to the user "
        "first. Tell the user what the preview says will change and ask; if they agree, call again without "
        "confirmed to get a preview_token, then confirm with it."
    ),
    "unknown": (
        "Not changed: that preview_token is unknown, already used or expired. Tell the user what this preview says "
        "will change and ask again; if they agree, confirm with the new preview_token."
    ),
    "changed": (
        "Not changed: the arguments or the situation are different from the preview that preview_token came from. "
        "Tell the user what this new preview says will change and ask again; if they agree, confirm with the new "
        "preview_token."
    ),
}


@dataclass(frozen=True)
class _Preview:
    client: str
    digest: str
    issued: float
    expires: float


def preview_digest(name: str, request: dict, preview: dict) -> str:
    bound = {key: value for key, value in preview.items() if key not in UNBOUND_PREVIEW_KEYS}
    text = json.dumps({"operation": name, "request": request, "preview": bound}, sort_keys=True, default=str)
    return hashlib.sha256(text.encode()).hexdigest()


class PreviewTokens:
    def __init__(self) -> None:
        self._previews: dict[str, _Preview] = {}

    def issue(self, client: str, digest: str) -> str:
        now = time.monotonic()
        self._previews = {token: entry for token, entry in self._previews.items() if entry.expires > now}
        while len(self._previews) >= PREVIEW_TOKEN_LIMIT:
            self._previews.pop(next(iter(self._previews)))
        token = secrets.token_hex(4)
        self._previews[token] = _Preview(client, digest, now, now + PREVIEW_TOKEN_SECONDS)
        return token

    def redeem(self, token: object, client: str, digest: str) -> tuple[str, float | None]:
        if not isinstance(token, str) or not token:
            return "missing", None
        entry = self._previews.pop(token, None)
        now = time.monotonic()
        age = now - entry.issued if entry is not None else None
        if entry is None or entry.expires <= now or entry.client != client:
            return "unknown", age
        if entry.digest != digest:
            return "changed", age
        return "accepted", age
