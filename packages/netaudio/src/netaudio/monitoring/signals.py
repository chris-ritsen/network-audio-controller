from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any
from typing_extensions import TypeGuard

from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.monitoring.model import _json_safe


def snapshot_from_device(device) -> dict[str, Any]:
    serialized = DanteDeviceSerializer.to_json(device)
    error = getattr(device, "error", None)
    serialized["error"] = None if error is None else str(error)
    serialized["failed_queries"] = sorted(str(item) for item in (getattr(device, "failed_queries", None) or ()))
    return _json_safe(serialized)


def _device_identity(snapshot: Mapping[str, Any]) -> str:
    device_identity = snapshot.get("device_identity")
    if isinstance(device_identity, str) and device_identity:
        return device_identity
    server_name = snapshot.get("server_name")
    if isinstance(server_name, str) and server_name:
        return server_name
    inventory_id = snapshot.get("inventory_id")
    if isinstance(inventory_id, str) and inventory_id:
        return inventory_id
    mac_address = snapshot.get("mac_address")
    if isinstance(mac_address, str) and mac_address:
        return mac_address.casefold().replace(":", "").replace("-", "")
    raise ValueError("monitoring snapshots require server_name, inventory_id, or mac_address")


def _observation_context(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    context = {}
    if snapshot.get("error") is not None:
        context["device_error"] = snapshot.get("error")
    failed_queries = snapshot.get("failed_queries")
    if failed_queries:
        context["failed_queries"] = failed_queries
    field_sources = snapshot.get("field_sources")
    if field_sources:
        context["field_sources"] = field_sources
    ddm_status = snapshot.get("ddm_status")
    if ddm_status:
        context["ddm_status"] = ddm_status
    return context


def _clock_evidence(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "clock_status": snapshot.get("clock_status"),
        "ptpv1_grandmaster_uuid": snapshot.get("ptpv1_grandmaster_uuid"),
        "clock_role": snapshot.get("clock_role"),
        "clock_port_state_code": snapshot.get("clock_port_state_code"),
        "ptpv1_device_uuid": snapshot.get("ptpv1_device_uuid"),
        "ptpv1_master_uuid": snapshot.get("ptpv1_master_uuid"),
        "clock_frequency_offset_parts_per_billion": snapshot.get("clock_frequency_offset_parts_per_billion"),
    }


def _ptp_port_map(records: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(records, list):
        return {}
    mapped = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        record_number = record.get("record_number")
        network_index = record.get("network_interface_index")
        ptp_version = record.get("ptp_version")
        transport = record.get("transport_path") or record.get("transport_path_code")
        if not _unsigned_integer(record_number) or (network_index is not None and not _unsigned_integer(network_index)):
            continue
        identity = f"ptp:{ptp_version}/{transport}/record:{record_number}"
        mapped[identity] = record
    return mapped


def _ptp_state(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: record.get(key)
        for key in (
            "state_code",
            "role",
            "link_down",
            "user_disabled",
            "unicast_delay_requests",
            "network_interface_index",
        )
    }


def _channel_map(channels: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(channels, dict):
        return {}
    mapped = {}
    for direction, direction_channels in (("rx", channels.get("receivers")), ("tx", channels.get("transmitters"))):
        if not isinstance(direction_channels, dict):
            continue
        for channel_number, channel in direction_channels.items():
            if isinstance(channel, dict):
                mapped[f"{direction}:{channel_number}"] = channel
    return mapped


def _subscription_map(subscriptions: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(subscriptions, list):
        return {}
    mapped = {}
    for index, subscription in enumerate(subscriptions):
        if not isinstance(subscription, dict):
            continue
        channel_number = subscription.get("rx_channel_number")
        if _unsigned_integer(channel_number):
            identity = f"rx:{channel_number}"
        else:
            receiver = subscription.get("rx_channel")
            receiver_device = subscription.get("rx_device")
            if not isinstance(receiver, str) or not receiver:
                continue
            identity = f"rx:{receiver_device or ''}/{receiver}"
        if identity in mapped:
            identity = f"{identity}/entry:{index}"
        mapped[identity] = subscription
    return mapped


def _subscription_failure_state(subscription: Mapping[str, Any]) -> bool | None:
    status = subscription.get("status")
    if not isinstance(status, dict):
        return None
    severity = status.get("severity")
    state = status.get("state")
    if severity in ("error", "warning"):
        return True
    if severity == "ok" or state == "connected":
        return False
    return None


def _flow_map(health: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(health, dict) or not isinstance(health.get("paths"), list):
        return {}
    mapped = {}
    for flow in health["paths"]:
        if not isinstance(flow, dict):
            continue
        slot = flow.get("audio_receiver_flow_id")
        network = flow.get("network_interface_index")
        if (
            flow.get("attribution_status") != "resolved"
            or not _positive_integer(slot)
            or not _unsigned_integer(network)
        ):
            continue
        identity = f"audio-receiver:{slot}/network:{network}/attribution:{flow.get('attribution_epoch', 0)}"
        mapped[identity] = flow
    return mapped


def _configured_flow_latency(snapshot: Mapping[str, Any], health_flow: Mapping[str, Any]) -> int | None:
    if snapshot.get("receiver_flow_completeness") in ("partial", "unknown"):
        return None
    if health_flow.get("attribution_status") != "resolved":
        return None
    observation = (health_flow.get("latency") or {}).get("current") or {}
    evidence = observation.get("evidence") or {}
    if evidence.get("comparison_key") != (health_flow.get("evidence") or {}).get("comparison_key"):
        return None
    configured = evidence.get("configured_latency_nanoseconds")
    return configured if _positive_integer(configured) else None


def _interface_map(traffic: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(traffic, dict) or not isinstance(traffic.get("interfaces"), list):
        return {}
    return {
        f"interface:{index + 1}": interface
        for index, interface in enumerate(traffic["interfaces"])
        if isinstance(interface, dict)
    }


def _unsigned_integer(value: Any) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _positive_integer(value: Any) -> TypeGuard[int]:
    return _unsigned_integer(value) and value > 0


def _unsigned_number(value: Any) -> TypeGuard[int | float]:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def _positive_number(value: Any) -> TypeGuard[int | float]:
    return _unsigned_number(value) and value > 0
