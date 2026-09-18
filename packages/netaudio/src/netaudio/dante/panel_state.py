"""Panel identity and timestamped observations; no wire-format interpretation."""

from __future__ import annotations

from datetime import datetime, timezone
from copy import deepcopy

MAX_AGE = 10.0
FAMILIES = {"DIOBT": "bluetooth", "DanteAV": "dante_av"}
QUERY_SELECTORS = {
    "bluetooth": {
        "bluetooth_connection": 1,
        "bluetooth_identification": 2,
        "bluetooth_discovery": 3,
        "bluetooth_pairing": 4,
    },
    "dante_av": {
        "video_format": 1,
        "codec_format": 2,
        "video_channel": 3,
        "serial": 4,
        "bandwidth": 5,
        "hdcp": 6,
        "visca": 7,
    },
}


def observation_time():
    # Match the journal clock, including its microsecond resolution on Windows.
    return datetime.now(timezone.utc).timestamp()


def panel_family(device):
    versions = getattr(device, "platform_versions_record", None) or {}
    advertised = versions.get("plugin_identifiers") or []
    if advertised:
        families = {FAMILIES.get(value) for value in advertised}
        known = families - {None}
        return next(iter(known)) if len(known) == 1 and None not in families else None
    identifiers = (
        versions.get("platform_model_identifier"),
        getattr(device, "platform_model_identifier", None),
        getattr(device, "model_id", None),
    )
    families = {FAMILIES[value] for value in identifiers if isinstance(value, str) and value in FAMILIES}
    return next(iter(families)) if len(families) == 1 else None


def fresh(device, observation, now=None):
    if (
        getattr(device, "online", None) is False
        or not isinstance(observation, dict)
        or observation.get("available") is not True
    ):
        return False
    stamp = observation.get("observed_at_unix")
    return isinstance(stamp, (float, int)) and 0 <= (observation_time() if now is None else now) - stamp <= MAX_AGE


def observe_panel(device, status):
    before = deepcopy(getattr(device, "device_controls", {}))
    state = deepcopy(before)
    state["family"] = panel_family(device)
    state["identity"] = {
        **deepcopy(getattr(device, "platform_versions_record", None) or {}),
        "advertised_model_id": getattr(device, "model_id", None),
    }
    state["latest_diagnostic"] = status
    if not status.get("observations"):
        state["unknown_diagnostics"] = [*(state.get("unknown_diagnostics") or [])[-15:], status]
    observations = state.setdefault("observations", {})
    if status.get("diagnostic_error"):
        for observation in observations.values():
            observation["available"] = False
    for item in status.get("observations", []):
        category = item["category"]
        observation = {
            "value": item["value"],
            "available": True,
            "observed_at_unix": status["observed_at_unix"],
            "source": "conmon_panel_status",
            "correlated": status.get("correlated", False),
            "requester": status.get("requester"),
            "sequence": status.get("sequence"),
            "record_revision": status.get("record_revision"),
            "raw_record": status.get("raw_record"),
            "application_payload": status.get("application_payload"),
            "source_identifier": status.get("source_identifier"),
            "common_word": status.get("common_word"),
            "envelope_offset": status.get("envelope_offset"),
            "envelope_length": status.get("envelope_length"),
        }
        observations[category] = observation
        store = state.setdefault("correlated_status" if observation["correlated"] else "unsolicited_status", {})
        store[category] = deepcopy(observation)
        if category == "bluetooth_connection":
            connection = item["value"]
            device.bluetooth_connected = {1: True, 2: False}.get(connection["state"])
            device.bluetooth_device = connection["peer_name"]
    device.device_controls = state
    return before != state


def panel_snapshot(device):
    state = deepcopy(getattr(device, "device_controls", {}))
    state["family"] = panel_family(device)
    for observation in state.get("observations", {}).values():
        observation["fresh"] = fresh(device, observation)
    state["readable"] = panel_permission(device, write=False) is None
    state["write_unavailable_reason"] = panel_permission(device, write=True)
    state["writable"] = state["write_unavailable_reason"] is None
    return state


def panel_permission(device, *, write):
    if getattr(device, "requires_managed_control", False):
        return "Managed panel transport is not established."
    if not getattr(device, "ipv4", None):
        return "Device address is unavailable."
    if getattr(device, "online", None) is False:
        return "Device is offline."
    if panel_family(device) is None:
        return "Device panel identity is unknown or ambiguous."
    if getattr(device, "virtual_panel_supported", None) is not True:
        return "Device has not advertised panel support."
    if getattr(device, "panel_read_allowed", True) is not True:
        return "Panel read permission is unavailable."
    if write:
        if getattr(device, "panel_write_allowed", True) is not True:
            return "Panel write permission is unavailable."
        if getattr(device, "is_locked", None) is not False:
            return "Device is locked or its lock state is unknown."
    return None
