from __future__ import annotations

from datetime import datetime, timezone

from netaudio import core
from netaudio.core import _types

CLOCK_STATUS_MAX_AGE_SECONDS = 10.0


def clock_status_fresh(snapshot, now: datetime | None = None) -> bool:
    status = snapshot.get("clock_status")

    if snapshot.get("online") is False or not isinstance(status, dict) or status.get("status_supported") is not True:
        return False

    observed_at = snapshot.get("clock_observed_at")

    if not isinstance(observed_at, str):
        return False

    try:
        observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        age = ((now or datetime.now(timezone.utc)) - observed).total_seconds()
    except (KeyError, TypeError, ValueError):
        return False

    return 0 <= age <= CLOCK_STATUS_MAX_AGE_SECONDS


def _revision_facts(device, override=None) -> dict:
    status = getattr(device, "clock_status", None) or {}

    return {
        "clock_revision": status.get("record_revision"),
        "explicit_revision": override,
        "model_revision": getattr(device, "dante_model_record_protocol_version", None),
        "interface_revision": getattr(device, "interface_status_protocol", None),
    }


def clock_record_revision(device, override: int | None = None) -> int:
    return core.clock_record_revision(_revision_facts(device, override))


def preview_clock_configuration(device, status: dict, changes: dict, revision: int | None = None) -> _types.ClockPlan:
    return core.plan_clock_configuration(
        {
            "status": status,
            "changes": changes,
            "revisions": _revision_facts(device, revision),
            "supported_clock_sources": getattr(device, "supported_clock_sources", None) or [],
        }
    )
