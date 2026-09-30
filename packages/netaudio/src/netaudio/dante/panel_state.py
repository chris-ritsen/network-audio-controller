"""Panel identity and timestamped observations; no wire-format interpretation."""

from __future__ import annotations

from datetime import datetime, timezone
from copy import deepcopy

from netaudio import core
from netaudio.core import _requests, _types

MAX_AGE = 10.0


def observation_time():
    # Match the journal clock, including its microsecond resolution on Windows.
    return datetime.now(timezone.utc).timestamp()


def panel_facts(device) -> _requests.PanelProfileRequest:
    versions = getattr(device, "platform_versions_record", None) or {}
    return {
        "plugins": versions.get("plugin_identifiers") or [],
        "platform_model_identifier_hexadecimal": versions.get("platform_model_identifier_hexadecimal"),
        "managed": bool(getattr(device, "requires_managed_control", False)),
        "address_available": bool(getattr(device, "ipv4", None)),
        "online": getattr(device, "online", None),
        "virtual_panel_supported": getattr(device, "virtual_panel_supported", None) is True,
        "read_allowed": getattr(device, "panel_read_allowed", True) is True,
        "write_allowed": getattr(device, "panel_write_allowed", True) is True,
        "locked": getattr(device, "is_locked", None),
        "video_transmission_supported": getattr(device, "video_transmission_supported", None) is True,
    }


def panel_profile(device) -> _types.PanelProfile:
    return core.panel_profile(panel_facts(device))


def panel_family(device) -> _types.PanelFamily | None:
    return panel_profile(device)["family"]


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
            device.bluetooth_connected = connection.get("connected")
            device.bluetooth_device = connection["peer_name"]
    device.device_controls = state
    return before != state


def panel_snapshot(device):
    profile = panel_profile(device)
    state = deepcopy(getattr(device, "device_controls", {}))
    state["family"] = profile["family"]
    state["panels"] = profile["panels"]
    state["categories"] = profile["categories"]
    state["selection"] = profile["selection"]
    state["unrecognized_panels"] = profile["unrecognized_panels"]
    state["read_unavailable_reason"] = profile["read_unavailable_reason"]
    for observation in state.get("observations", {}).values():
        observation["fresh"] = fresh(device, observation)
    state["readable"] = profile["read_unavailable_reason"] is None
    state["write_unavailable_reason"] = profile["write_unavailable_reason"]
    state["writable"] = state["write_unavailable_reason"] is None
    observations = state.get("observations", {})
    state["presentation"] = core.panel_presentation(
        {
            "profile": panel_facts(device),
            "values": {name: observation["value"] for name, observation in observations.items()},
            "fresh_values": {
                name: observation["value"] for name, observation in observations.items() if observation["fresh"]
            },
        }
    )
    return state


def panel_permission(device, *, write):
    profile = panel_profile(device)
    return profile["write_unavailable_reason"] if write else profile["read_unavailable_reason"]
