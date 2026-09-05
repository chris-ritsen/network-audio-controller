from __future__ import annotations

from netaudio.core import subscription_status


def default_status_entry(code: int, receiver_status_code: int | None = None) -> dict[str, object]:
    entry = subscription_status(code, receiver_status_code)
    entry["labels"] = (entry["label"],)
    return entry
