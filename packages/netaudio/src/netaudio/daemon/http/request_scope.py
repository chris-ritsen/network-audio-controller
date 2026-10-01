from __future__ import annotations

import contextvars

REQUEST_INVENTORY: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "netaudio_request_inventory", default=None
)
