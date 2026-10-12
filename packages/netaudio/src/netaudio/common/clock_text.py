from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _moment(value: Any) -> datetime | None:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        moment = datetime.fromtimestamp(value, timezone.utc)
    elif isinstance(value, str):
        try:
            moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)


def clock_text(value: Any = None, *, seconds: bool = False) -> str | None:
    moment = _moment(value)
    if moment is None:
        return None
    local = moment.astimezone()
    pattern = "%H:%M:%S" if seconds else "%H:%M"
    if local.date() == datetime.now().astimezone().date():
        return local.strftime(pattern)
    return local.strftime(f"%Y-%m-%d {pattern}")
