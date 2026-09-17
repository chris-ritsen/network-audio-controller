from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.monitoring.model import _json_safe


def snapshot_from_device(device) -> dict[str, Any]:
    serialized = DanteDeviceSerializer.to_json(device)
    for subscription, entry in zip(device.subscriptions, serialized["subscriptions"]):
        rx_channel = getattr(subscription, "rx_channel", None)
        channel_number = getattr(rx_channel, "number", None)
        if not _unsigned_integer(channel_number):
            channel_number = getattr(subscription, "_netaudio_rx_channel_number", None)
        if _unsigned_integer(channel_number):
            entry["rx_channel_number"] = channel_number
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
        "clock_role": snapshot.get("clock_role"),
        "clock_port_state_code": snapshot.get("clock_port_state_code"),
        "clock_identity": snapshot.get("clock_identity"),
        "leader_clock_identity": snapshot.get("leader_clock_identity"),
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
        if not _unsigned_integer(record_number) or not _unsigned_integer(network_index):
            continue
        identity = f"network:{network_index}/ptp:{ptp_version}/{transport}/record:{record_number}"
        mapped[identity] = record
    return mapped


def _ptp_state(record: Mapping[str, Any]) -> dict[str, Any]:
    return {"state_code": record.get("state_code"), "role": record.get("role"), "link_down": record.get("link_down")}


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
    if not isinstance(health, dict) or not isinstance(health.get("flows"), list):
        return {}
    mapped = {}
    for flow in health["flows"]:
        if not isinstance(flow, dict):
            continue
        slot = flow.get("receiver_flow_slot")
        index = flow.get("receiver_flow_index")
        if _positive_integer(slot):
            identity = f"receiver-flow:{slot}"
        elif _unsigned_integer(index):
            identity = f"receiver-flow-index:{index}"
        else:
            continue
        mapped[identity] = flow
    return mapped


def _configured_flow_latency(snapshot: Mapping[str, Any], health_flow: Mapping[str, Any]) -> int | None:
    if snapshot.get("receiver_flow_completeness") in ("partial", "unknown"):
        return None
    slot = health_flow.get("receiver_flow_slot")
    receiver_flows = snapshot.get("receiver_flows")
    if _positive_integer(slot) and isinstance(receiver_flows, list):
        for flow in receiver_flows:
            if not isinstance(flow, dict) or flow.get("global_flow_id", flow.get("flow_number")) != slot:
                continue
            configured = flow.get("latency_nanoseconds")
            if _positive_integer(configured):
                return configured
    configured = snapshot.get("receiver_flow_latency_ns")
    return configured if _positive_integer(configured) else None


def _interface_map(traffic: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(traffic, dict) or not isinstance(traffic.get("interfaces"), list):
        return {}
    return {
        f"interface:{index + 1}": interface
        for index, interface in enumerate(traffic["interfaces"])
        if isinstance(interface, dict)
    }


def _unsigned_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _positive_integer(value: Any) -> bool:
    return _unsigned_integer(value) and value > 0


def _unsigned_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def _positive_number(value: Any) -> bool:
    return _unsigned_number(value) and value > 0
