from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from typing import Any

from netaudio import core
from netaudio.dante.channel_status_paging import advertised_arc_protocol_identifier_for_device
from netaudio.dante.const import ARC_SUCCESS_RESULT_CODES


PROPERTY_TX_FLOW_LATENCY_NS = 0x8204
PROPERTY_UNICAST_CONFIGURED_LATENCY_NS = 0x8205
PROPERTY_TX_FLOW_FRAMES_PER_PACKET = 0x0210
PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET = 0x0211
PROPERTY_RX_FLOW_LATENCY_NS = 0x8301
PROPERTY_RX_FLOW_FRAMES_PER_PACKET = 0x0310
PROPERTY_RX_FLOW_DEFAULT_SLOTS = 0x0303
PROPERTY_PRE_3_COMPATIBILITY = 0x8304

PERFORMANCE_PROPERTY_IDS = frozenset(
    {
        PROPERTY_TX_FLOW_LATENCY_NS,
        PROPERTY_UNICAST_CONFIGURED_LATENCY_NS,
        PROPERTY_TX_FLOW_FRAMES_PER_PACKET,
        PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET,
        PROPERTY_RX_FLOW_LATENCY_NS,
        PROPERTY_RX_FLOW_FRAMES_PER_PACKET,
        PROPERTY_RX_FLOW_DEFAULT_SLOTS,
        PROPERTY_PRE_3_COMPATIBILITY,
    }
)
_VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


@dataclass(frozen=True)
class PerformanceOperationResult:
    operation: str
    state: str
    requested_properties: dict[int, int]
    effective_properties: dict[int, int]
    request_acknowledgement: dict[str, Any] | None
    device_confirmation: bool | None
    effective_state_confirmation: bool | None
    persistence_request_acknowledgement: dict[str, Any] | None
    persistence_confirmation: bool | None
    message: str
    verification_observations: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "state": self.state,
            "requested_properties": _serialize_properties(self.requested_properties),
            "effective_properties": _serialize_properties(self.effective_properties),
            "request_acknowledgement": copy.deepcopy(self.request_acknowledgement),
            "device_confirmation": self.device_confirmation,
            "effective_state_confirmation": self.effective_state_confirmation,
            "persistence_request_acknowledgement": copy.deepcopy(self.persistence_request_acknowledgement),
            "persistence_confirmation": self.persistence_confirmation,
            "message": self.message,
            "verification_observations": copy.deepcopy(list(self.verification_observations)),
        }


def _serialize_properties(properties: dict[int, int]) -> dict[str, int]:
    return {f"0x{property_id:04x}": value for property_id, value in sorted(properties.items())}


def _acknowledgement(response: bytes | None) -> dict[str, Any] | None:
    if response is None:
        return None
    try:
        result_code = core.parse_response("result_code", response)
    except core.NetaudioCoreError:
        return {"received": True, "parseable": False, "raw_response_hexadecimal": response.hex()}
    availability = {
        "received": True,
        "parseable": True,
        "result_code": result_code,
        "accepted": result_code in ARC_SUCCESS_RESULT_CODES,
        "raw_response_hexadecimal": response.hex(),
    }
    return availability


def _require_direct_device(device) -> None:
    if getattr(device, "requires_managed_control", False):
        raise RuntimeError("performance and configuration-storage writes have no established managed transport")


def _negotiated_protocol_id(device) -> int:
    protocol_id = advertised_arc_protocol_identifier_for_device(device)
    if protocol_id is None:
        raise RuntimeError("device has no observed ARC protocol identifier")
    return protocol_id


def _platform_software_version(device) -> tuple[int, int, int]:
    value = getattr(device, "platform_software_version", None)
    match = _VERSION_PATTERN.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise RuntimeError("platform software version is unavailable or is not an x.y.z version")
    version = tuple(int(part) for part in match.groups())
    if any(part > 0xFFFF for part in version):
        raise RuntimeError("platform software version component exceeds the supported range")
    return version  # type: ignore[return-value]


def advertised_performance_property_ids(device) -> frozenset[int]:
    entries = getattr(device, "settings_properties", None)
    if not isinstance(entries, list):
        raise RuntimeError("device property directory has not been observed")
    property_ids = {
        entry["property_id"]
        for entry in entries
        if isinstance(entry, dict)
        and isinstance(entry.get("property_id"), int)
        and not isinstance(entry.get("property_id"), bool)
    }
    return frozenset(property_ids & PERFORMANCE_PROPERTY_IDS)


def performance_operation_availability(device) -> dict[str, dict[str, Any]]:
    managed = getattr(device, "requires_managed_control", False)
    try:
        protocol_id = _negotiated_protocol_id(device)
    except RuntimeError:
        protocol_id = None
    try:
        supported = advertised_performance_property_ids(device)
    except RuntimeError:
        supported = frozenset()
    try:
        version = _platform_software_version(device)
    except RuntimeError:
        version = None

    def entry(required: set[int], *, needs_version: bool = False, any_of: bool = False) -> dict[str, Any]:
        reasons = []
        if managed:
            reasons.append("managed_transport_unavailable")
        if protocol_id is None:
            reasons.append("protocol_unknown")
        elif protocol_id < 0x2601 and protocol_id < 0x280A:
            reasons.append("protocol_unsupported")
        if getattr(device, "settings_properties", None) is None:
            reasons.append("property_directory_unknown")
        elif not supported.intersection(required) if any_of else not required.issubset(supported):
            reasons.append("properties_not_advertised")
        if needs_version and version is None:
            reasons.append("platform_software_version_unknown")
        elif needs_version and version < (3, 0, 0) and PROPERTY_PRE_3_COMPATIBILITY not in supported:
            reasons.append("compatibility_property_not_advertised")
        return {
            "supported": not reasons,
            "readable": bool(supported.intersection(required)),
            "writable": not reasons,
            "reasons": list(dict.fromkeys(reasons)),
        }

    availability = {
        "receive_flow_performance": entry(
            {PROPERTY_RX_FLOW_LATENCY_NS, PROPERTY_RX_FLOW_FRAMES_PER_PACKET}, needs_version=True
        ),
        "transmit_flow_performance": entry({PROPERTY_TX_FLOW_LATENCY_NS, PROPERTY_TX_FLOW_FRAMES_PER_PACKET}),
        "unicast_performance": entry(
            {
                PROPERTY_UNICAST_CONFIGURED_LATENCY_NS,
                PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET,
                PROPERTY_RX_FLOW_LATENCY_NS,
                PROPERTY_RX_FLOW_FRAMES_PER_PACKET,
            },
            needs_version=True,
            any_of=True,
        ),
        "receive_flow_default_slots": entry({PROPERTY_RX_FLOW_DEFAULT_SLOTS}),
        "store_current_configuration": {
            "supported": not managed and protocol_id is not None,
            "readable": False,
            "writable": not managed and protocol_id is not None,
            "reasons": (["managed_transport_unavailable"] if managed else [])
            + (["protocol_unknown"] if protocol_id is None else []),
        },
    }
    if version is not None and version < (3, 0, 0) and PROPERTY_PRE_3_COMPATIBILITY in supported:
        unicast = availability["unicast_performance"]
        unicast["reasons"] = [reason for reason in unicast["reasons"] if reason != "properties_not_advertised"]
        unicast["supported"] = not unicast["reasons"]
        unicast["writable"] = not unicast["reasons"]
    return availability


def _integer(name: str, value: object, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise ValueError(f"{name} must be an integer from 0 through {maximum}")
    return value


def _latency_nanoseconds(latency_microseconds: object) -> int:
    value = _integer("latency_microseconds", latency_microseconds, 0xFFFFFFFF)
    nanoseconds = value * 1_000
    if nanoseconds > 0xFFFFFFFF:
        raise ValueError("latency_microseconds is too large to encode as u32 nanoseconds")
    return nanoseconds


def _require_advertised(device, required: set[int]) -> None:
    missing = sorted(required - advertised_performance_property_ids(device))
    if missing:
        labels = ", ".join(f"0x{property_id:04x}" for property_id in missing)
        raise RuntimeError(f"device does not advertise required performance properties: {labels}")


def requested_performance_properties(device, operation: str, payload: dict[str, int] | int) -> dict[int, int]:
    _require_direct_device(device)
    if operation == "receive_flow_default_slots":
        slots = _integer("default_slots", payload, 0xFFFF)
        requested = {PROPERTY_RX_FLOW_DEFAULT_SLOTS: slots}
    else:
        if not isinstance(payload, dict):
            raise ValueError(f"{operation} requires latency and frames per packet")
        latency_ns = _latency_nanoseconds(payload.get("latency_microseconds"))
        frames = _integer("frames_per_packet", payload.get("frames_per_packet"), 0xFFFF)
        if operation == "receive_flow_performance":
            requested = {
                PROPERTY_RX_FLOW_LATENCY_NS: latency_ns,
                PROPERTY_RX_FLOW_FRAMES_PER_PACKET: frames,
            }
            if _platform_software_version(device) < (3, 0, 0):
                requested[PROPERTY_PRE_3_COMPATIBILITY] = 1
        elif operation == "transmit_flow_performance":
            requested = {
                PROPERTY_TX_FLOW_LATENCY_NS: latency_ns,
                PROPERTY_TX_FLOW_FRAMES_PER_PACKET: frames,
            }
        elif operation == "unicast_performance":
            supported = advertised_performance_property_ids(device)
            requested = {
                property_id: value
                for property_id, value in (
                    (PROPERTY_UNICAST_CONFIGURED_LATENCY_NS, latency_ns),
                    (PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET, frames),
                    (PROPERTY_RX_FLOW_LATENCY_NS, latency_ns),
                    (PROPERTY_RX_FLOW_FRAMES_PER_PACKET, frames),
                )
                if property_id in supported
            }
            if _platform_software_version(device) < (3, 0, 0):
                requested[PROPERTY_PRE_3_COMPATIBILITY] = 1_000
            if not requested:
                raise RuntimeError("device advertises no supported unicast performance properties")
        else:
            raise ValueError(f"unknown performance operation {operation!r}")
    _require_advertised(device, set(requested))
    return requested


def performance_settings_from_response(settings: dict[str, Any]) -> dict[int, int]:
    values: dict[int, int] = {}
    for entry in settings.get("inline_values", []):
        if isinstance(entry, dict) and isinstance(entry.get("info_code"), int) and isinstance(entry.get("value"), int):
            values[entry["info_code"]] = entry["value"]
    for entry in settings.get("referenced_values", []):
        if not isinstance(entry, dict) or not isinstance(entry.get("info_code"), int):
            continue
        hexadecimal = entry.get("value_hexadecimal")
        if isinstance(hexadecimal, str) and len(hexadecimal) >= 8:
            try:
                values[entry["info_code"]] = int(hexadecimal[:8], 16)
            except ValueError:
                continue
    return values


async def get_performance_settings(device, property_ids) -> dict[int, int]:
    _require_direct_device(device)
    protocol_id = _negotiated_protocol_id(device)
    requested_ids = [_integer("property_id", value, 0xFFFF) for value in property_ids]
    response = await _send_once(
        device,
        {
            "command": "query_performance_settings",
            "negotiated_protocol_id": protocol_id,
            "property_ids": requested_ids,
        },
    )
    if response is None:
        raise RuntimeError("fresh performance property readback was unavailable")
    settings = core.parse_response("device_settings", response)
    values = performance_settings_from_response(settings)
    device.performance_settings = {**(getattr(device, "performance_settings", None) or {}), **values}
    return {property_id: values[property_id] for property_id in requested_ids if property_id in values}


async def _send_once(device, specification: dict[str, Any]) -> bytes | None:
    return await device.call_core(lambda client: client.execute(specification), request_attempts=1)


async def _apply_and_verify(
    device,
    *,
    operation: str,
    command: str,
    requested_properties: dict[int, int],
    command_fields: dict[str, Any],
) -> PerformanceOperationResult:
    _require_direct_device(device)
    protocol_id = _negotiated_protocol_id(device)
    supported_property_ids = sorted(advertised_performance_property_ids(device))
    specification = {
        "command": command,
        "negotiated_protocol_id": protocol_id,
        "supported_property_ids": supported_property_ids,
        **command_fields,
    }
    async with device.topology_mutation_lock:
        response = await _send_once(device, specification)
        acknowledgement = _acknowledgement(response)
        query_response = await _send_once(
            device,
            {
                "command": "query_performance_settings",
                "negotiated_protocol_id": protocol_id,
                "property_ids": list(requested_properties),
            },
        )

    rejected = (
        acknowledgement is not None
        and acknowledgement.get("parseable") is True
        and acknowledgement.get("accepted") is False
    )
    if query_response is None:
        acknowledged = acknowledgement is not None and acknowledgement.get("accepted") is True
        return PerformanceOperationResult(
            operation,
            "rejected" if rejected else "request_acknowledged" if acknowledged else "unverified",
            requested_properties,
            {},
            acknowledgement,
            None,
            None,
            None,
            None,
            (
                "device rejected the performance request; fresh property readback was unavailable"
                if rejected
                else "request was acknowledged; fresh property readback was unavailable"
                if acknowledged
                else "request was sent once without an accepted acknowledgement; fresh readback was unavailable"
            ),
            ({"phase": "readback", "outcome": "no_response"},),
        )
    try:
        settings = core.parse_response("device_settings", query_response)
    except core.NetaudioCoreError:
        acknowledged = acknowledgement is not None and acknowledgement.get("accepted") is True
        return PerformanceOperationResult(
            operation,
            "rejected" if rejected else "request_acknowledged" if acknowledged else "unverified",
            requested_properties,
            {},
            acknowledgement,
            None,
            None,
            None,
            None,
            (
                "device rejected the performance request; fresh property readback was not parseable"
                if rejected
                else "request was acknowledged; fresh property readback was not parseable"
                if acknowledged
                else "request was sent once without an accepted acknowledgement; fresh readback was not parseable"
            ),
            ({"phase": "readback", "outcome": "unparseable"},),
        )
    effective = performance_settings_from_response(settings)
    device.performance_settings = {**(getattr(device, "performance_settings", None) or {}), **effective}
    missing = sorted(set(requested_properties) - set(effective))
    mismatched = {
        property_id: {"requested": requested_properties[property_id], "effective": effective[property_id]}
        for property_id in requested_properties.keys() & effective.keys()
        if requested_properties[property_id] != effective[property_id]
    }
    complete = not missing
    confirmed = complete and not mismatched
    contradicted = bool(mismatched)
    acknowledged = acknowledgement is not None and acknowledgement.get("accepted") is True
    observation = {
        "phase": "readback",
        "outcome": "matched" if confirmed else "mismatch" if contradicted else "incomplete",
        "missing_property_ids": [f"0x{property_id:04x}" for property_id in missing],
        "mismatched_properties": _serialize_properties(
            {property_id: values["effective"] for property_id, values in mismatched.items()}
        ),
    }
    return PerformanceOperationResult(
        operation,
        (
            "rejected"
            if rejected
            else "confirmed"
            if confirmed
            else "contradicted"
            if contradicted
            else "request_acknowledged"
            if acknowledged
            else "unverified"
        ),
        requested_properties,
        {property_id: effective[property_id] for property_id in requested_properties.keys() & effective.keys()},
        acknowledgement,
        None,
        True if confirmed else False if contradicted else None,
        None,
        None,
        (
            "device rejected the performance request; fresh readback matched every affected property"
            if rejected and confirmed
            else "device rejected the performance request; fresh readback found a correlated value mismatch"
            if rejected and contradicted
            else "device rejected the performance request; fresh readback omitted one or more affected properties"
            if rejected
            else "fresh readback matched every affected property"
            if confirmed and acknowledged
            else "fresh readback matched every affected property; request acknowledgement was not established"
            if confirmed
            else "fresh readback found a correlated value mismatch"
            if contradicted
            else "fresh readback omitted one or more affected properties"
        ),
        (observation,),
    )


async def set_receive_flow_performance(device, latency_microseconds: int, frames_per_packet: int):
    _require_direct_device(device)
    latency_ns = _latency_nanoseconds(latency_microseconds)
    frames = _integer("frames_per_packet", frames_per_packet, 0xFFFF)
    version = _platform_software_version(device)
    requested = {PROPERTY_RX_FLOW_LATENCY_NS: latency_ns, PROPERTY_RX_FLOW_FRAMES_PER_PACKET: frames}
    if version < (3, 0, 0):
        requested[PROPERTY_PRE_3_COMPATIBILITY] = 1
    _require_advertised(device, set(requested))
    return await _apply_and_verify(
        device,
        operation="set_receive_flow_performance",
        command="set_receive_flow_performance",
        requested_properties=requested,
        command_fields={
            "latency_microseconds": latency_microseconds,
            "frames_per_packet": frames,
            "platform_software_version": list(version),
        },
    )


async def set_transmit_flow_performance(device, latency_microseconds: int, frames_per_packet: int):
    _require_direct_device(device)
    latency_ns = _latency_nanoseconds(latency_microseconds)
    frames = _integer("frames_per_packet", frames_per_packet, 0xFFFF)
    _require_advertised(device, {PROPERTY_TX_FLOW_LATENCY_NS, PROPERTY_TX_FLOW_FRAMES_PER_PACKET})
    return await _apply_and_verify(
        device,
        operation="set_transmit_flow_performance",
        command="set_transmit_flow_performance",
        requested_properties={PROPERTY_TX_FLOW_LATENCY_NS: latency_ns, PROPERTY_TX_FLOW_FRAMES_PER_PACKET: frames},
        command_fields={"latency_microseconds": latency_microseconds, "frames_per_packet": frames},
    )


async def set_unicast_performance(device, latency_microseconds: int, frames_per_packet: int):
    _require_direct_device(device)
    latency_ns = _latency_nanoseconds(latency_microseconds)
    frames = _integer("frames_per_packet", frames_per_packet, 0xFFFF)
    version = _platform_software_version(device)
    supported = advertised_performance_property_ids(device)
    requested = {}
    for property_id, value in (
        (PROPERTY_UNICAST_CONFIGURED_LATENCY_NS, latency_ns),
        (PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET, frames),
        (PROPERTY_RX_FLOW_LATENCY_NS, latency_ns),
        (PROPERTY_RX_FLOW_FRAMES_PER_PACKET, frames),
    ):
        if property_id in supported:
            requested[property_id] = value
    if version < (3, 0, 0):
        requested[PROPERTY_PRE_3_COMPATIBILITY] = 1_000
    if not requested:
        raise RuntimeError("device advertises no supported unicast performance properties")
    _require_advertised(device, set(requested))
    return await _apply_and_verify(
        device,
        operation="set_unicast_performance",
        command="set_unicast_performance",
        requested_properties=requested,
        command_fields={
            "latency_microseconds": latency_microseconds,
            "frames_per_packet": frames,
            "platform_software_version": list(version),
        },
    )


async def set_receive_flow_default_slots(device, default_slots: int):
    _require_direct_device(device)
    slots = _integer("default_slots", default_slots, 0xFFFF)
    _require_advertised(device, {PROPERTY_RX_FLOW_DEFAULT_SLOTS})
    return await _apply_and_verify(
        device,
        operation="set_receive_flow_default_slots",
        command="set_receive_flow_default_slots",
        requested_properties={PROPERTY_RX_FLOW_DEFAULT_SLOTS: slots},
        command_fields={"default_slots": slots},
    )


async def store_current_configuration(device) -> PerformanceOperationResult:
    _require_direct_device(device)
    protocol_id = _negotiated_protocol_id(device)
    async with device.topology_mutation_lock:
        response = await _send_once(
            device,
            {"command": "store_current_configuration", "negotiated_protocol_id": protocol_id},
        )
    acknowledgement = _acknowledgement(response)
    accepted = acknowledgement is not None and acknowledgement.get("accepted") is True
    rejected = (
        acknowledgement is not None
        and acknowledgement.get("parseable") is True
        and acknowledgement.get("accepted") is False
    )
    return PerformanceOperationResult(
        "store_current_configuration",
        "request_acknowledged" if accepted else "rejected" if rejected else "unverified",
        {},
        {},
        None,
        None,
        None,
        acknowledgement,
        None,
        "storage request acknowledged; persistence requires an independent signal or post-reboot readback"
        if accepted
        else "configuration storage was not acknowledged",
    )
