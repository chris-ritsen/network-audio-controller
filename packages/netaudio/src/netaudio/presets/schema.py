from __future__ import annotations

import copy
from collections.abc import Mapping
from ipaddress import IPv4Address, IPv4Network
from typing import Any

from netaudio.dante.transmit_flow import TransmitFlowSpecification


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


def _positive_integer(value: Any, label: str, maximum: int = 0xFFFF) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{label} must be an integer from 1 through {maximum}")
    return value


def _optional_boolean(value: Any, label: str) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    raise ValueError(f"{label} must be Boolean or null")


def _unsigned_integer(value: Any, label: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise ValueError(f"{label} must be an integer from 0 through {maximum}")
    return value


def _performance_pair(value: Any, label: str) -> dict[str, int]:
    if not isinstance(value, Mapping) or set(value) != {"latency_microseconds", "frames_per_packet"}:
        raise ValueError(f"{label} must contain latency_microseconds and frames_per_packet")
    return {
        "latency_microseconds": _unsigned_integer(
            value["latency_microseconds"], f"{label}.latency_microseconds", 0xFFFFFFFF // 1_000
        ),
        "frames_per_packet": _unsigned_integer(value["frames_per_packet"], f"{label}.frames_per_packet", 0xFFFF),
    }


def _name_map(value: Any, label: str) -> dict[int, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    result = {}
    for raw_number, raw_name in value.items():
        try:
            number = int(raw_number)
        except (TypeError, ValueError) as exception:
            raise ValueError(f"{label} channel identifiers must be integers") from exception
        _positive_integer(number, f"{label} channel identifier")
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
        try:
            channel = int(raw_channel)
        except (TypeError, ValueError) as exception:
            raise ValueError("receiver subscription channel identifiers must be integers") from exception
        _positive_integer(channel, "receiver subscription channel identifier")
        if raw_subscription is None:
            result[channel] = None
        elif isinstance(raw_subscription, Mapping):
            entry = copy.deepcopy(dict(raw_subscription))
            kind = entry.get("kind", "native_dante")
            if kind not in ("native_dante", "external_rtp"):
                raise ValueError("receiver subscription kind must be native_dante or external_rtp")
            entry["kind"] = kind
            if kind == "native_dante":
                if not isinstance(entry.get("tx_channel"), str) or not entry["tx_channel"]:
                    raise ValueError("native receiver subscriptions require tx_channel")
                if not isinstance(entry.get("tx_device"), str) or not entry["tx_device"]:
                    raise ValueError("native receiver subscriptions require tx_device")
            else:
                identity = entry.get("flow_identity")
                slot = entry.get("flow_slot")
                if not isinstance(identity, Mapping):
                    raise ValueError("external RTP subscriptions require flow_identity")
                if not isinstance(identity.get("source_ipv4"), str):
                    raise ValueError("external RTP flow identity requires source_ipv4")
                IPv4Address(identity["source_ipv4"])
                _positive_integer(identity.get("session_id"), "external RTP session_id", maximum=0xFFFFFFFFFFFFFFFF)
                if isinstance(slot, bool) or not isinstance(slot, int) or not 1 <= slot <= 0xFFFF:
                    raise ValueError("external RTP subscriptions require a positive 16-bit flow_slot")
                endpoints = entry.get("interface_endpoints")
                if endpoints is not None:
                    if not isinstance(endpoints, list) or not 1 <= len(endpoints) <= 2:
                        raise ValueError("external RTP interface_endpoints must contain one or two destinations")
                    for endpoint in endpoints:
                        if not isinstance(endpoint, Mapping):
                            raise ValueError("external RTP interface endpoints must be objects")
                        address = endpoint.get("ipv4_address")
                        if address is not None:
                            if not isinstance(address, str):
                                raise ValueError("external RTP endpoint IPv4 address must be a string or null")
                            IPv4Address(address)
                        _positive_integer(endpoint.get("udp_port"), "external RTP endpoint UDP port")
                multiple_interfaces = entry.get("receiver_supports_multiple_interfaces")
                if multiple_interfaces is not None and not isinstance(multiple_interfaces, bool):
                    raise ValueError("receiver_supports_multiple_interfaces must be Boolean")
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
    for field_name in ("preferred_leader", "external_word_clock"):
        if field_name in result:
            result[field_name] = _optional_boolean(result[field_name], field_name)
    if "interfaces" in result:
        result["interfaces"] = _interfaces(result["interfaces"])
        for interface in result["interfaces"]:
            if interface["mode"] == "static":
                try:
                    for field_name in ("ip_address", "netmask", "gateway", "dns_server"):
                        IPv4Address(interface[field_name])
                    IPv4Network(f"{interface['ip_address']}/{interface['netmask']}", strict=False)
                except ValueError as exception:
                    raise ValueError(
                        f"interface {interface['identity']}: invalid static IPv4 configuration"
                    ) from exception
    for field_name in ("transmitter_channel_names", "receiver_channel_names"):
        if field_name in result:
            result[field_name] = _name_map(result[field_name], field_name)
    if "rx_subscriptions" in result:
        result["rx_subscriptions"] = _subscriptions(result["rx_subscriptions"])
    if "transmit_flows" in result:
        raw_flows = result["transmit_flows"]
        if not isinstance(raw_flows, list):
            raise ValueError("transmit_flows must be a list")
        result["transmit_flows"] = [TransmitFlowSpecification.from_dict(item).to_dict() for item in raw_flows]
    if "codec_gain" in result:
        gains = result["codec_gain"]
        if not isinstance(gains, list):
            raise ValueError("codec_gain must be a list")
        normalized_gains = []
        for gain in gains:
            if not isinstance(gain, Mapping):
                raise ValueError("codec_gain entries must be objects")
            entry = copy.deepcopy(dict(gain))
            _positive_integer(entry.get("channel"), "codec gain channel")
            _positive_integer(entry.get("level"), "codec gain level", maximum=255)
            if entry.get("device_type") not in ("input", "output"):
                raise ValueError("codec gain device_type must be input or output")
            normalized_gains.append(entry)
        result["codec_gain"] = normalized_gains
    if "sample_rate_pullup" in result:
        pullup = result["sample_rate_pullup"]
        if isinstance(pullup, bool) or not isinstance(pullup, int) or not 0 <= pullup <= 0xFFFFFFFF:
            raise ValueError("sample_rate_pullup must be an unsigned 32-bit integer")
    for field_name in (
        "receive_flow_performance",
        "transmit_flow_performance",
        "unicast_performance",
    ):
        if field_name in result:
            result[field_name] = _performance_pair(result[field_name], field_name)
    if "receive_flow_default_slots" in result:
        result["receive_flow_default_slots"] = _unsigned_integer(
            result["receive_flow_default_slots"], "receive_flow_default_slots", 0xFFFF
        )
    if "clock_source_code" in result:
        value = result["clock_source_code"]
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 0xFFFF:
            raise ValueError("clock_source_code must be an unsigned 16-bit integer")
    if "redundancy_mode" in result and result["redundancy_mode"] not in (
        "switched",
        "redundant",
        "split_redundant",
    ):
        raise ValueError("redundancy_mode must be switched, redundant, or split_redundant")
    for numeric_name in ("sample_rate", "encoding"):
        if numeric_name in result:
            _positive_integer(result[numeric_name], numeric_name, maximum=0xFFFFFFFF)
    if "unknown_fields" in result and not isinstance(result["unknown_fields"], Mapping):
        raise ValueError("unknown_fields must be an object")
    return result


def json_ready_config(config: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(config))
    for field_name in ("transmitter_channel_names", "receiver_channel_names", "rx_subscriptions"):
        if isinstance(value.get(field_name), Mapping):
            value[field_name] = {str(key): item for key, item in value[field_name].items()}
    return value
