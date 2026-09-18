from __future__ import annotations

from datetime import datetime, timezone

from netaudio.dante.clock_config import clock_subdomain_bytes

CLOCK_STATUS_MAX_AGE_SECONDS = 10.0
CLOCK_CONFIGURATION_FIELDS = frozenset(
    {
        "clock_source",
        "preferred_leader",
        "subdomain",
        "global_unicast_delay_requests",
        "aggregate_ptpv1_unicast_delay_requests",
    }
)


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


def clock_record_revision(device, override: int | None = None) -> int:
    status = getattr(device, "clock_status", None) or {}
    known = status.get("record_revision")
    if override is not None and known is not None and override != known:
        raise ValueError("The explicit clock revision differs from the device clock status.")
    revision = override if override is not None else known
    if revision is None:
        revision = getattr(device, "dante_model_record_protocol_version", None)
    if revision is None:
        revision = getattr(device, "interface_status_protocol", None)
    if isinstance(revision, bool) or not isinstance(revision, int) or not 0 < revision <= 0xFFFF:
        raise RuntimeError("Clock record revision is unavailable; supply an explicit record revision.")
    return revision


def preview_clock_configuration(device, status: dict, changes: dict, revision: int | None = None) -> dict:
    unknown = changes.keys() - CLOCK_CONFIGURATION_FIELDS
    if unknown:
        raise ValueError(f"Unsupported clock settings: {', '.join(sorted(unknown))}")
    if status.get("status_supported") is not True:
        raise RuntimeError("A fresh supported clock status is required before changing clock settings.")
    requested = {key: value for key, value in changes.items() if value is not None}
    caps = status.get("clock_capabilities")
    flags = status.get("extension_flags")
    revision = clock_record_revision(device, revision)
    for name, value in requested.items():
        if name == "clock_source" and (
            isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 0xFFFF
        ):
            raise ValueError("Clock source must be an unsigned 16-bit integer.")
        if name not in {"clock_source", "subdomain"} and not isinstance(value, bool):
            raise ValueError(f"{name} must be true or false")
    if "subdomain" in requested:
        raw = clock_subdomain_bytes(requested["subdomain"])
        if raw is None or b"\0" not in raw or any(raw[raw.index(0) :]):
            raise ValueError("Clock subdomain must fit in 15 bytes followed by a NUL.")
        requested["subdomain"] = list(raw)
    before = {
        key: status.get({"clock_source": "clock_source_code", "subdomain": "clock_subdomain"}.get(key, key))
        for key in requested
    }
    before = {key: list(value) if isinstance(value, bytes) else value for key, value in before.items()}
    changed = {key: value for key, value in requested.items() if before[key] != value}
    if "clock_source" in changed:
        source = changed["clock_source"]
        if isinstance(source, bool) or not isinstance(source, int) or source not in (0, 1, 2):
            raise ValueError("Clock source must be 0 (internal), 1 (external/BNC), or 2 (AES).")
        if source and source not in (getattr(device, "supported_clock_sources", None) or []):
            raise RuntimeError("The device has not independently reported support for that clock source.")
    if "preferred_leader" in changed:
        if caps is None or caps & 0x0120 or (revision >= 0x072E and (flags is None or flags & 0x1000)):
            raise RuntimeError("The device does not permit changing preferred leader.")
    if "subdomain" in changed and (caps is None or not caps & 4):
        raise RuntimeError("The device does not support named clock subdomains.")
    if "global_unicast_delay_requests" in changed and (
        revision < 0x0603 or caps is None or not caps & 8 or caps & 0x0200
    ):
        raise RuntimeError("The device does not support the global unicast-delay setting.")
    if "aggregate_ptpv1_unicast_delay_requests" in changed and (revision < 0x071F or caps is None or not caps & 0x0200):
        raise RuntimeError("The device does not support the PTPv1 per-port unicast-delay setting.")
    return {
        "requested": requested,
        "before": before,
        "changes": changed,
        "control": {
            "record_revision": revision,
            "clock_capabilities": caps,
            "extension_flags": flags,
            "supported_clock_sources": getattr(device, "supported_clock_sources", None) or [],
            **changed,
        },
    }


def clock_configuration_matches(status: dict, requested: dict) -> bool:
    for key, value in requested.items():
        actual = status.get({"clock_source": "clock_source_code", "subdomain": "clock_subdomain"}.get(key, key))
        if isinstance(actual, bytes):
            actual = list(actual)
        if actual != value:
            return False
    return status.get("status_supported") is True
