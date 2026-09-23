from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from netaudio import core
from netaudio.core import _types
from netaudio.dante.arc_protocol import advertised_arc_protocol_identifier_for_device


@dataclass(frozen=True)
class PerformanceOperationResult:
    operation: str
    state: str
    requested_properties: dict[int, int]
    effective_properties: dict[int, int]
    request_acknowledgement: _types.CommandReceipt | None
    device_confirmation: bool | None
    effective_state_confirmation: bool | None
    persistence_request_acknowledgement: _types.CommandReceipt | None
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


def _require_direct_device(device) -> None:
    if getattr(device, "requires_managed_control", False):
        raise RuntimeError("performance and configuration-storage writes have no established managed transport")


def _negotiated_protocol_id(device) -> int:
    protocol_id = advertised_arc_protocol_identifier_for_device(device)
    if protocol_id is None:
        raise RuntimeError("device has no observed ARC protocol identifier")
    return protocol_id


def _platform_software_version(device) -> tuple[int, int, int]:
    version = _performance_capabilities(device)["platform_software_version"]

    if version is None:
        raise RuntimeError("platform software version is unavailable or is not an x.y.z version")

    return version[0], version[1], version[2]


def advertised_performance_property_ids(device) -> frozenset[int]:
    property_ids = _performance_capabilities(device)["supported_property_ids"]

    if property_ids is None:
        raise RuntimeError("device property directory has not been observed")

    return frozenset(property_ids)


def _performance_capabilities(device) -> _types.PerformanceCapabilities:
    try:
        protocol_id = _negotiated_protocol_id(device)
    except RuntimeError:
        protocol_id = None

    entries = getattr(device, "settings_properties", None)
    version = getattr(device, "platform_software_version", None)

    return core.performance_capabilities(
        {
            "protocol_id": protocol_id,
            "managed": bool(getattr(device, "requires_managed_control", False)),
            "property_ids": (
                [entry.get("property_id") for entry in entries if isinstance(entry, dict)]
                if isinstance(entries, list)
                else None
            ),
            "platform_software_version": version if isinstance(version, str) else None,
        }
    )


def performance_operation_availability(device) -> dict[str, _types.PerformanceAvailability]:
    return _performance_capabilities(device)["operations"]


def observed_performance_configuration(device) -> _types.PerformanceSnapshot:
    entries = getattr(device, "settings_properties", None)

    return core.performance_snapshot(
        {
            "property_ids": (
                [entry.get("property_id") for entry in entries if isinstance(entry, dict)]
                if isinstance(entries, list)
                else []
            ),
            "values": {str(key): value for key, value in (getattr(device, "performance_settings", None) or {}).items()},
        }
    )


def _performance_command(device, operation: str, payload: dict[str, int] | int) -> dict:
    _require_direct_device(device)

    if operation == "receive_flow_default_slots":
        fields = {"default_slots": payload}
    else:
        if not isinstance(payload, dict):
            raise ValueError(f"{operation} requires latency and frames per packet")

        if payload.keys() - {"latency_microseconds", "frames_per_packet"}:
            raise ValueError(f"{operation} accepts only latency_microseconds and frames_per_packet")

        fields = dict(payload)

    version_fields = (
        {"platform_software_version": list(_platform_software_version(device))}
        if operation in ("receive_flow_performance", "unicast_performance")
        else {}
    )

    return {
        "command": f"set_{operation}",
        "negotiated_protocol_id": _negotiated_protocol_id(device),
        "supported_property_ids": sorted(advertised_performance_property_ids(device)),
        **fields,
        **version_fields,
    }


def _planned_properties(specification: dict) -> dict[int, int]:
    return {entry["property_id"]: entry["value"] for entry in core.plan_performance_command(specification)}


def requested_performance_properties(device, operation: str, payload: dict[str, int] | int) -> dict[int, int]:
    return _planned_properties(_performance_command(device, operation, payload))


def performance_settings_from_response(settings: dict[str, Any]) -> dict[int, int]:
    return {entry["property_id"]: entry["value"] for entry in settings["performance_values"]}


def _invalidate_performance_properties(device, property_ids) -> None:
    affected = set(property_ids)
    device.performance_settings = {
        property_id: value
        for property_id, value in (getattr(device, "performance_settings", None) or {}).items()
        if property_id not in affected
    }


async def get_performance_settings(device, property_ids) -> dict[int, int]:
    _require_direct_device(device)
    protocol_id = _negotiated_protocol_id(device)
    requested_ids = list(property_ids)
    specification = {
        "command": "query_performance_settings",
        "negotiated_protocol_id": protocol_id,
        "property_ids": requested_ids,
    }
    core.build_command(specification)
    _invalidate_performance_properties(device, requested_ids)

    response = await _send_once(device, specification)
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
    specification: dict[str, Any],
    requested_properties: dict[int, int],
) -> PerformanceOperationResult:
    operation = specification["command"]
    protocol_id = specification["negotiated_protocol_id"]

    async with device.topology_mutation_lock:
        _invalidate_performance_properties(device, requested_properties)

        response = await _send_once(device, specification)
        acknowledgement = core.command_acknowledgement(response)
        query_response = await _send_once(
            device,
            {
                "command": "query_performance_settings",
                "negotiated_protocol_id": protocol_id,
                "property_ids": list(requested_properties),
            },
        )

    completion = core.performance_completion(
        {
            "kind": "configuration",
            "requested": {str(key): value for key, value in requested_properties.items()},
            "acknowledgement": list(response) if response is not None else None,
            "readback": list(query_response) if query_response is not None else None,
        }
    )
    observed = {int(key): value for key, value in completion["observed_properties"].items()}
    device.performance_settings = {**(getattr(device, "performance_settings", None) or {}), **observed}
    observation = {
        "phase": "readback",
        "outcome": completion["readback_outcome"],
        "missing_property_ids": [f"0x{property_id:04x}" for property_id in completion["missing_property_ids"]],
        "mismatched_properties": _serialize_properties(
            {int(key): value for key, value in completion["mismatched_properties"].items()}
        ),
    }
    return PerformanceOperationResult(
        operation=operation,
        state=completion["state"],
        requested_properties=requested_properties,
        effective_properties={int(key): value for key, value in completion["effective_properties"].items()},
        request_acknowledgement=acknowledgement,
        device_confirmation=None,
        effective_state_confirmation=completion["effective_state_confirmation"],
        persistence_request_acknowledgement=None,
        persistence_confirmation=completion["persistence_confirmation"],
        message=completion["message"],
        verification_observations=(observation,),
    )


async def set_receive_flow_performance(device, latency_microseconds: int, frames_per_packet: int):
    return await _set_performance(
        device,
        "receive_flow_performance",
        {"latency_microseconds": latency_microseconds, "frames_per_packet": frames_per_packet},
    )


async def set_transmit_flow_performance(device, latency_microseconds: int, frames_per_packet: int):
    return await _set_performance(
        device,
        "transmit_flow_performance",
        {"latency_microseconds": latency_microseconds, "frames_per_packet": frames_per_packet},
    )


async def set_unicast_performance(device, latency_microseconds: int, frames_per_packet: int):
    return await _set_performance(
        device,
        "unicast_performance",
        {"latency_microseconds": latency_microseconds, "frames_per_packet": frames_per_packet},
    )


async def set_receive_flow_default_slots(device, default_slots: int):
    return await _set_performance(device, "receive_flow_default_slots", default_slots)


async def _set_performance(device, operation: str, payload):
    specification = _performance_command(device, operation, payload)
    return await _apply_and_verify(
        device,
        specification=specification,
        requested_properties=_planned_properties(specification),
    )


async def store_current_configuration(device) -> PerformanceOperationResult:
    _require_direct_device(device)
    protocol_id = _negotiated_protocol_id(device)
    async with device.topology_mutation_lock:
        response = await _send_once(
            device,
            {"command": "store_current_configuration", "negotiated_protocol_id": protocol_id},
        )
    acknowledgement = core.command_acknowledgement(response)
    completion = core.performance_completion(
        {"kind": "storage", "acknowledgement": list(response) if response is not None else None}
    )
    return PerformanceOperationResult(
        operation="store_current_configuration",
        state=completion["state"],
        requested_properties={},
        effective_properties={},
        request_acknowledgement=None,
        device_confirmation=None,
        effective_state_confirmation=completion["effective_state_confirmation"],
        persistence_request_acknowledgement=acknowledgement,
        persistence_confirmation=completion["persistence_confirmation"],
        message=completion["message"],
    )
