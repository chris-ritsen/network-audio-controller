from __future__ import annotations

from datetime import datetime, timezone

from netaudio import core
from netaudio.core import _requests, _types

CLOCK_STATUS_MAX_AGE_SECONDS = 10.0

EXTENDED_CLOCK_FIELDS = (
    "follower_only",
    "priority_mapping",
    "preferred_protocol",
    "ptpv2_clock_class",
    "ptpv2_domain",
    "ptpv2_priority1",
    "ptpv2_priority2",
    "multicast_dscp",
)


def observed_clock_configuration(status: dict) -> dict:
    allowed = core.clock_control_availability(status)
    changes = {
        name: status[name] for name in EXTENDED_CLOCK_FIELDS if allowed.get(name) and status.get(name) is not None
    }
    ports = []
    for port in status.get("extended_ports", []):
        if port.get("port_id") is None:
            continue
        fields = {
            name: value
            for name, value in port.items()
            if name in _requests.ClockPortControl.__annotations__ and value is not None
        }
        if len(fields) > 1:
            ports.append(fields)
    if ports:
        changes["ports"] = ports
    return changes


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


def clock_control_profile(override: int | None = None) -> int:
    return core.clock_control_profile({"control_profile": override})


def preview_clock_configuration(
    device, status: dict, changes: dict, control_profile: int | None = None
) -> _types.ClockPlan:
    return core.plan_clock_configuration(
        {
            "status": status,
            "changes": changes,
            "control_profile": control_profile,
            "supported_clock_sources": getattr(device, "supported_clock_sources", None) or [],
        }
    )
