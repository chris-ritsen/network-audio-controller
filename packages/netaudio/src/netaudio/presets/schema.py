from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from netaudio import core
from netaudio.dante.transmit_flow import parse_transmit_flow_specification


PRESET_SCHEMA_VERSION = 3
PRESET_EXTENSION_TAG = "netaudio_configuration"
PRESET_EXTENSION_CONTENT_TAG = "json"


class ParsedPresetDevices(dict[str, dict[str, Any]]):
    """Dictionary-compatible parsed device set with preserved document metadata."""

    def __init__(
        self,
        *args,
        source_version: str | None = None,
        root_attributes: dict[str, str] | None = None,
        unknown_root_elements: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_version = source_version
        self.root_attributes = root_attributes or {}
        self.unknown_root_elements = unknown_root_elements or []


def _channel_number(value: Any, existing: dict, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{label} channel identifiers must be integers")

    try:
        number = int(value)
    except ValueError as exception:
        raise ValueError(f"{label} channel identifiers must be integers") from exception

    if number in existing:
        raise ValueError(f"{label} contains duplicate channel identifier {number}")

    return number


def _name_map(value: Any, label: str) -> dict[int, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    result = {}
    for raw_number, raw_name in value.items():
        number = _channel_number(raw_number, result, label)
        if not isinstance(raw_name, str):
            raise ValueError(f"{label} channel names must be strings")
        result[number] = raw_name
    return result


def _interfaces(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("interfaces must be a list")
    result = []
    identities = set()
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise ValueError("interface entries must be objects")
        entry = copy.deepcopy(dict(raw))
        identity = entry.get("identity", "primary" if index == 0 else "secondary" if index == 1 else str(index))
        if not isinstance(identity, str) or not identity or identity in identities:
            raise ValueError("interface identities must be distinct non-empty strings")
        identities.add(identity)
        mode = entry.get("mode")
        if mode not in ("dynamic", "dhcp", "static"):
            raise ValueError(f"interface {identity}: mode must be dynamic, dhcp or static")
        entry["identity"] = identity
        if mode == "static":
            for name in ("ip_address", "netmask", "gateway", "dns_server"):
                if not isinstance(entry.get(name), str) or not entry[name]:
                    raise ValueError(f"interface {identity}: static interface is missing {name}")
        result.append(entry)
    return result


def _subscriptions(value: Any) -> dict[int, dict[str, Any] | None]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("rx_subscriptions must be an object")
    result = {}
    for raw_channel, raw_subscription in value.items():
        channel = _channel_number(raw_channel, result, "receiver subscription")
        if raw_subscription is None:
            result[channel] = None
        elif isinstance(raw_subscription, Mapping):
            entry = copy.deepcopy(dict(raw_subscription))
            kind = entry.get("kind", "native_dante")
            entry["kind"] = kind
            result[channel] = entry
        else:
            raise ValueError("receiver subscriptions must be objects or null")
    return result


def normalize_device_config(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("preset device configuration must be an object")
    result = copy.deepcopy(dict(value))
    name = result.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("preset device name must be a non-empty string")
    if "device_name" in result and (not isinstance(result["device_name"], str) or not result["device_name"]):
        raise ValueError("device_name must be a non-empty string")
    if "device_identity" in result:
        identity = result["device_identity"]
        if not isinstance(identity, Mapping) or not any(
            identity.get(key) for key in ("server_name", "mac_address", "inventory_id")
        ):
            raise ValueError("device_identity must contain server_name, mac_address, or inventory_id")
        result["device_identity"] = copy.deepcopy(dict(identity))
    if "interfaces" in result:
        result["interfaces"] = _interfaces(result["interfaces"])
    for field_name in ("transmitter_channel_names", "receiver_channel_names"):
        if field_name in result:
            result[field_name] = _name_map(result[field_name], field_name)
    if "rx_subscriptions" in result:
        result["rx_subscriptions"] = _subscriptions(result["rx_subscriptions"])
    if "transmit_flows" in result:
        raw_flows = result["transmit_flows"]
        if not isinstance(raw_flows, list):
            raise ValueError("transmit_flows must be a list")
        result["transmit_flows"] = [parse_transmit_flow_specification(item) for item in raw_flows]
    if "device_controls" in result:
        from netaudio.presets.device_controls import validate_device_controls

        result["device_controls"] = validate_device_controls(result["device_controls"])
    if "clock_subdomain" in result:
        try:
            result["clock_subdomain"] = list(core.normalize_clock_subdomain(result["clock_subdomain"]))
        except core.NetaudioCoreError as error:
            raise ValueError(error.detail or str(error)) from error
    if "unknown_fields" in result and not isinstance(result["unknown_fields"], Mapping):
        raise ValueError("unknown_fields must be an object")

    try:
        core.validate_configuration(result)
    except core.NetaudioCoreError as error:
        raise ValueError(error.detail or str(error)) from error

    return result


def json_ready_config(config: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(config))
    for field_name in ("transmitter_channel_names", "receiver_channel_names", "rx_subscriptions"):
        if isinstance(value.get(field_name), Mapping):
            value[field_name] = {str(key): item for key, item in value[field_name].items()}
    return value
