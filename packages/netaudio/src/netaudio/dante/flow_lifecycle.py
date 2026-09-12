from __future__ import annotations

import asyncio
import copy
import json
from typing import Any

from netaudio import core
from netaudio.dante import flows
from netaudio.dante.channel_status_paging import advertised_arc_protocol_identifier_for_device
from netaudio.dante.const import RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED
from netaudio.dante.transmit_flow import (
    FlowComparison,
    FlowLifecycleState,
    FlowOperationPlan,
    FlowOperationResult,
    FlowType,
    MediaMode,
    RedundancyConstraint,
    TransmitFlowSpecification,
    compare_transmit_flows,
)


LEGACY_CREATE_PROTOCOL_ID = 0x2729
LEGACY_READBACK_PROTOCOL_IDS = frozenset({0x2729, 0x2801})
MODERN_ALLOCATION_PROTOCOL_ID = 0x2809
SUCCESS_RESULT_CODES = frozenset({RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED})
VERIFICATION_TIMEOUT_SECONDS = 5.0
VERIFICATION_POLL_INTERVAL_SECONDS = 0.25


def _transport(device) -> str:
    return "ddm" if getattr(device, "requires_managed_control", False) else "direct"


def _protocol_id(device, specification: TransmitFlowSpecification | None = None) -> int | None:
    required = specification.protocol.protocol_id if specification is not None else None
    advertised = getattr(device, "flow_protocol_id", None)
    if advertised is None:
        try:
            advertised = advertised_arc_protocol_identifier_for_device(device)
        except (AttributeError, RuntimeError):
            advertised = None
    return required if required is not None else advertised


def _required_capability_reasons(device, specification: TransmitFlowSpecification) -> list[str]:
    reasons = []
    for capability in specification.protocol.required_capabilities:
        value = getattr(device, capability, None)
        if value is not True:
            reasons.append(f"required capability {capability!r} is {'unknown' if value is None else 'not advertised'}")
    return reasons


def _device_protocol_version(device) -> str | None:
    for field_name in ("flow_protocol_version", "protocol_version"):
        value = getattr(device, field_name, None)
        if isinstance(value, str) and value:
            return value
    return None


def _common_create_reasons(device, specification: TransmitFlowSpecification, protocol_id: int | None) -> list[str]:
    reasons = _required_capability_reasons(device, specification)
    if _transport(device) == "ddm":
        reasons.append("managed transmit-flow writes have no documented or independently observed transport")
    if getattr(device, "is_locked", None) is True:
        reasons.append("device is locked")
    elif getattr(device, "is_locked", None) is None:
        reasons.append("device lock state is unknown")
    if protocol_id is None:
        reasons.append("flow protocol is unknown")
    advertised = getattr(device, "flow_protocol_id", None)
    if advertised is not None and specification.protocol.protocol_id is not None and advertised != protocol_id:
        reasons.append(f"requested protocol 0x{protocol_id:04X} does not match device protocol 0x{advertised:04X}")
    required_version = specification.protocol.protocol_version
    if required_version is not None:
        observed_version = _device_protocol_version(device)
        if observed_version is None:
            reasons.append("device protocol version is unknown")
        elif observed_version != required_version:
            reasons.append(f"required protocol version {required_version!r} does not match {observed_version!r}")
    if specification.media_mode is MediaMode.RTP_AES67:
        reasons.append("RTP/AES67 transmit-flow serialization is unsupported by retained evidence")
    if specification.flow_type is FlowType.UNICAST:
        reasons.append("explicit unicast transmit-flow creation is unsupported by retained evidence")
    if specification.name is not None:
        reasons.append("the supported serializers do not encode a flow name")
    if specification.primary_destination is not None or specification.secondary_destination is not None:
        reasons.append("the supported serializers do not encode caller-selected destination sockets")
    if specification.frames_per_packet is not None:
        reasons.append("the supported serializers do not encode frames per packet")
    if specification.redundancy is not RedundancyConstraint.DEVICE_DEFAULT:
        reasons.append("the supported serializers do not encode an interface or redundancy constraint")
    if specification.sample_rate_hz is not None:
        current = getattr(device, "sample_rate", None)
        if current is None:
            reasons.append("current device sample rate is unknown")
        elif current != specification.sample_rate_hz:
            reasons.append("requested sample rate differs from the current device-wide sample rate")
    if specification.encoding_bits is not None:
        current = getattr(device, "encoding", None)
        if current is None:
            reasons.append("current device encoding is unknown")
        elif current != specification.encoding_bits:
            reasons.append("requested encoding differs from the current device-wide encoding")
    available_channels = getattr(device, "tx_channels", None)
    if not isinstance(available_channels, dict):
        reasons.append("transmitter channel inventory is unavailable")
    else:
        missing = sorted(set(specification.channels) - {int(number) for number in available_channels})
        if missing:
            reasons.append(f"transmitter channels are unavailable: {', '.join(map(str, missing))}")
    return reasons


def plan_create_transmit_flow(device, specification: TransmitFlowSpecification) -> FlowOperationPlan:
    protocol_id = _protocol_id(device, specification)
    reasons = _common_create_reasons(device, specification, protocol_id)
    serializer_cohort = None
    wire_options: dict[str, Any] = {}
    if protocol_id == LEGACY_CREATE_PROTOCOL_ID:
        serializer_cohort = "legacy_2729_explicit_slot_multicast"
        if specification.identity.global_flow_id is None:
            reasons.append("legacy 0x2729 creation requires an explicit global flow identifier")
        elif specification.identity.global_flow_id > 32:
            reasons.append("legacy 0x2729 global flow identifier must be from 1 through 32")
        request_options = specification.raw_fields.get("request_options_word")
        if request_options is not None:
            reasons.append("legacy 0x2729 creation does not accept request_options_word")
    elif protocol_id == MODERN_ALLOCATION_PROTOCOL_ID:
        serializer_cohort = "modern_2809_device_allocated_multicast"
        if specification.identity.global_flow_id is not None:
            reasons.append("ARC 2.8.9 allocation assigns the global flow identifier")
        request_options = specification.raw_fields.get("request_options_word", 0)
        if type(request_options) is not int or request_options not in (0, 1, 0x71):
            reasons.append("ARC 2.8.9 request_options_word must be an observed value: 0, 1 or 113")
        else:
            wire_options["request_options_word"] = request_options
        if specification.protocol.cohort not in (None, "modern_2809"):
            reasons.append("requested protocol cohort does not match ARC 2.8.9")
    elif protocol_id == 0x2801:
        reasons.append("0x2801 creation has no digest-bound request/acknowledgement fixture")
    elif protocol_id is not None:
        reasons.append(f"flow protocol 0x{protocol_id:04X} has no supported create serializer")
    supported = not reasons
    return FlowOperationPlan(
        operation="create",
        state=FlowLifecycleState.PLANNED if supported else FlowLifecycleState.UNSUPPORTED,
        transport=_transport(device),
        protocol_id=protocol_id,
        serializer_cohort=serializer_cohort,
        supported=supported,
        reasons=tuple(dict.fromkeys(reasons)),
        specification=specification,
        flow_id=specification.identity.global_flow_id,
        wire_options=wire_options,
    )


def plan_delete_transmit_flow(device, flow_id: int) -> FlowOperationPlan:
    flow_id = flows.validate_flow_slot(flow_id)
    protocol_id = _protocol_id(device)
    reasons = []
    serializer_cohort = None
    if _transport(device) == "ddm":
        reasons.append("managed transmit-flow deletion has no documented or independently observed transport")
    if getattr(device, "is_locked", None) is True:
        reasons.append("device is locked")
    elif getattr(device, "is_locked", None) is None:
        reasons.append("device lock state is unknown")
    if protocol_id == LEGACY_CREATE_PROTOCOL_ID:
        serializer_cohort = "legacy_2729_explicit_slot_delete"
    elif protocol_id == MODERN_ALLOCATION_PROTOCOL_ID and flow_id == 2:
        serializer_cohort = "modern_2809_global_flow_2_delete"
    elif protocol_id == MODERN_ALLOCATION_PROTOCOL_ID:
        reasons.append("ARC 2.8.9 deletion is verified only for global flow identifier 2")
    elif protocol_id == 0x2801:
        reasons.append("0x2801 deletion has no digest-bound request/acknowledgement fixture")
    elif protocol_id is None:
        reasons.append("flow protocol is unknown")
    else:
        reasons.append(f"flow protocol 0x{protocol_id:04X} has no supported delete serializer")
    supported = not reasons
    return FlowOperationPlan(
        operation="delete",
        state=FlowLifecycleState.PLANNED if supported else FlowLifecycleState.UNSUPPORTED,
        transport=_transport(device),
        protocol_id=protocol_id,
        serializer_cohort=serializer_cohort,
        supported=supported,
        reasons=tuple(dict.fromkeys(reasons)),
        flow_id=flow_id,
    )


def canonical_inventory(flow_inventory: dict, protocol_id: int) -> dict[str, Any]:
    records = flow_inventory.get("flows")
    if not isinstance(records, list):
        raise flows.FlowValidationError("transmitter flow inventory is malformed", status=502)
    specifications = []
    errors = []
    for index, record in enumerate(records):
        try:
            specifications.append(
                TransmitFlowSpecification.from_inventory_record(record, protocol_id=protocol_id).to_dict()
            )
        except (TypeError, ValueError) as exception:
            errors.append({"record_index": index, "error": str(exception), "raw_record": copy.deepcopy(record)})
    return {
        "schema_version": 1,
        "flow_protocol_id": protocol_id,
        "max_flow_slots": flow_inventory.get("max_flow_slots"),
        "reported_flow_count": flow_inventory.get("reported_flow_count", len(records)),
        "flows": specifications,
        "unparsed_records": errors,
    }


async def inspect_transmit_flows(device) -> dict[str, Any]:
    protocol_id = _protocol_id(device)
    if protocol_id is None:
        raise flows.FlowValidationError("flow protocol is unknown", status=409)
    inventory = await flows.query_preferred_tx_flow_inventory(
        str(device.ipv4), device._arc_port(), protocol_id, device=device
    )
    if inventory is None:
        raise flows.FlowValidationError("fresh transmitter flow readback was unavailable", status=504)
    readback_protocol = inventory.get("flow_protocol_id", protocol_id)
    return canonical_inventory(inventory, readback_protocol)


def _record_id(record: dict) -> int | None:
    value = record.get("global_flow_id", record.get("flow_number"))
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _find_record(inventory: dict, flow_id: int) -> dict | None:
    return next((record for record in inventory.get("flows", ()) if _record_id(record) == flow_id), None)


def _stable_configuration_projection(record: dict, protocol_id: int) -> dict[str, Any]:
    """Return only parser-established identity and durable flow configuration."""
    flow_id = _record_id(record)
    try:
        specification = TransmitFlowSpecification.from_inventory_record(record, protocol_id=protocol_id)
    except (TypeError, ValueError):
        # Unknown record semantics are retained in verification observations but
        # are never promoted into stable equality fields.
        return {"identity": {"global_flow_id": flow_id}, "parsed": False}
    value = specification.to_dict()
    return {
        "identity": value["identity"],
        "media_mode": value["media_mode"],
        "flow_type": value["flow_type"],
        "name": value["name"],
        "channel_slots": value["channel_slots"],
        "sample_rate_hz": value["sample_rate_hz"],
        "encoding_bits": value["encoding_bits"],
        "frames_per_packet": value["frames_per_packet"],
        "primary_destination": value["primary_destination"],
        "secondary_destination": value["secondary_destination"],
        "redundancy": value["redundancy"],
        "protocol": {
            "protocol_id": value["protocol"]["protocol_id"],
            "protocol_version": value["protocol"]["protocol_version"],
            "cohort": value["protocol"]["cohort"],
        },
        "parsed": True,
    }


def _stable_inventory_projection(
    inventory: dict,
    protocol_id: int,
    *,
    excluded_flow_id: int | None = None,
) -> list[dict[str, Any]]:
    projected = [
        _stable_configuration_projection(record, protocol_id)
        for record in inventory.get("flows", ())
        if excluded_flow_id is None or _record_id(record) != excluded_flow_id
    ]
    return sorted(projected, key=lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")))


def _concurrent_topology_activity(
    before: dict,
    after: dict,
    protocol_id: int,
    *,
    target_flow_id: int | None,
) -> dict[str, Any] | None:
    before_projection = _stable_inventory_projection(before, protocol_id, excluded_flow_id=target_flow_id)
    after_projection = _stable_inventory_projection(after, protocol_id, excluded_flow_id=target_flow_id)
    if before_projection == after_projection:
        return None
    return {
        "before": before_projection,
        "after": after_projection,
    }


async def _read_inventory(device, protocol_id: int) -> dict | None:
    return await flows.query_tx_flow_inventory(str(device.ipv4), device._arc_port(), protocol_id, device=device)


async def _send_once(device, command_specification: dict) -> bytes | None:
    return await device.call_core(lambda client: client.execute(command_specification), request_attempts=1)


def _acknowledgement(response: bytes | None) -> dict[str, Any] | None:
    if response is None:
        return None
    try:
        result_code = core.parse_response("result_code", response)
    except core.NetaudioCoreError:
        return {"received": True, "parseable": False, "raw_response_hexadecimal": response.hex()}
    return {
        "received": True,
        "parseable": True,
        "result_code": result_code,
        "accepted": result_code in SUCCESS_RESULT_CODES,
        "raw_response_hexadecimal": response.hex(),
    }


def _observation(
    *,
    phase: str,
    attempt: int,
    inventory: dict | None,
    outcome: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    value = {
        "phase": phase,
        "attempt": attempt,
        "outcome": outcome,
        "inventory": copy.deepcopy(inventory),
    }
    if details:
        value["details"] = copy.deepcopy(details)
    return value


def _operation_result(
    *,
    operation: str,
    state: FlowLifecycleState,
    transport: str,
    acknowledgement: dict[str, Any] | None,
    effective_confirmation: bool | None,
    requested: TransmitFlowSpecification | None,
    effective: TransmitFlowSpecification | None,
    comparison: FlowComparison | None,
    message: str,
    observations: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> FlowOperationResult:
    return FlowOperationResult(
        operation=operation,
        state=state,
        transport=transport,
        request_acknowledgement=acknowledgement,
        # ARC request responses and inventory readback provide no separate
        # device-side confirmation signal.
        device_confirmation=None,
        persistence_confirmation=None,
        effective_state_confirmation=effective_confirmation,
        requested=requested,
        effective=effective,
        comparison=comparison,
        message=message,
        verification_observations=tuple(copy.deepcopy(list(observations))),
    )


def _accepted(acknowledgement: dict[str, Any] | None) -> bool:
    return acknowledgement is not None and acknowledgement.get("accepted") is True


def _rejected(acknowledgement: dict[str, Any] | None) -> bool:
    return bool(
        acknowledgement is not None
        and acknowledgement.get("parseable") is True
        and acknowledgement.get("accepted") is False
    )


async def _wait_for_next_poll(deadline: float) -> bool:
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        return False
    await asyncio.sleep(min(VERIFICATION_POLL_INTERVAL_SECONDS, remaining))
    return True


def _allocated_flow_id(acknowledgement: dict[str, Any] | None) -> int | None:
    allocation = acknowledgement.get("allocation") if acknowledgement is not None else None
    value = allocation.get("global_flow_id") if isinstance(allocation, dict) else None
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _creation_candidate(
    *,
    before: dict,
    after: dict,
    requested: TransmitFlowSpecification,
    protocol_id: int,
    correlated_flow_id: int | None,
) -> tuple[int | None, dict | None, FlowComparison | None]:
    if correlated_flow_id is not None:
        record = _find_record(after, correlated_flow_id)
        if record is None:
            return correlated_flow_id, None, None
        effective = TransmitFlowSpecification.from_inventory_record(record, protocol_id=protocol_id)
        return correlated_flow_id, record, compare_transmit_flows(requested, effective)

    before_ids = {_record_id(record) for record in before.get("flows", ())}
    candidates: list[tuple[int, dict, FlowComparison]] = []
    for record in after.get("flows", ()):
        flow_id = _record_id(record)
        if flow_id is None or flow_id in before_ids:
            continue
        try:
            effective = TransmitFlowSpecification.from_inventory_record(record, protocol_id=protocol_id)
        except (TypeError, ValueError):
            continue
        comparison = compare_transmit_flows(requested, effective)
        if comparison.matches:
            candidates.append((flow_id, record, comparison))
    if len(candidates) == 1:
        return candidates[0]
    return None, None, None


async def create_transmit_flow(device, specification: TransmitFlowSpecification) -> FlowOperationResult:
    plan = plan_create_transmit_flow(device, specification)
    if not plan.supported:
        return _operation_result(
            operation="create",
            state=FlowLifecycleState.UNSUPPORTED,
            transport=plan.transport,
            acknowledgement=None,
            effective_confirmation=None,
            requested=specification,
            effective=None,
            comparison=None,
            message="; ".join(plan.reasons),
        )
    assert plan.protocol_id is not None
    protocol_id = plan.protocol_id
    command_specification: dict[str, Any]
    if protocol_id == MODERN_ALLOCATION_PROTOCOL_ID:
        command_specification = {
            "command": "create_multicast_flow_2809",
            "channels": specification.channels,
            **plan.wire_options,
        }
    else:
        command_specification = {
            "command": "create_tx_flow",
            "flow_protocol_id": protocol_id,
            "flow_slot": specification.identity.global_flow_id,
            "channels": specification.channels,
        }
    core.build_command(command_specification)
    observations: list[dict[str, Any]] = []
    async with device.topology_mutation_lock:
        before = await _read_inventory(device, protocol_id)
        observations.append(
            _observation(
                phase="preflight",
                attempt=1,
                inventory=before,
                outcome="available" if before is not None else "unavailable",
            )
        )
        if before is None:
            return _operation_result(
                operation="create",
                state=FlowLifecycleState.PENDING,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=None,
                requested=specification,
                effective=None,
                comparison=None,
                message="fresh preflight inventory was unavailable; no request was sent",
                observations=observations,
            )
        if len(before.get("flows", ())) >= before.get("max_flow_slots", 0):
            return _operation_result(
                operation="create",
                state=FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=False,
                requested=specification,
                effective=None,
                comparison=None,
                message="all transmitter flow slots are in use; no request was sent",
                observations=observations,
            )
        maximum_flow_slots = before.get("max_flow_slots")
        requested_flow_id = specification.identity.global_flow_id
        if (
            requested_flow_id is not None
            and isinstance(maximum_flow_slots, int)
            and not isinstance(maximum_flow_slots, bool)
            and requested_flow_id > maximum_flow_slots
        ):
            return _operation_result(
                operation="create",
                state=FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=False,
                requested=specification,
                effective=None,
                comparison=None,
                message=(
                    f"flow {requested_flow_id} exceeds the device capacity of {maximum_flow_slots}; no request was sent"
                ),
                observations=observations,
            )
        if requested_flow_id is not None and _find_record(before, requested_flow_id):
            return _operation_result(
                operation="create",
                state=FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=False,
                requested=specification,
                effective=None,
                comparison=None,
                message=f"flow {requested_flow_id} is already active; no request was sent",
                observations=observations,
            )

        response = await _send_once(device, command_specification)
        acknowledgement = _acknowledgement(response)
        if protocol_id == MODERN_ALLOCATION_PROTOCOL_ID and acknowledgement is not None:
            try:
                acknowledgement["allocation"] = core.parse_response("multicast_flow_creation_2809", response)
            except core.NetaudioCoreError:
                acknowledgement["allocation"] = None
        if _rejected(acknowledgement):
            return _operation_result(
                operation="create",
                state=FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=acknowledgement,
                effective_confirmation=None,
                requested=specification,
                effective=None,
                comparison=None,
                message="device rejected the transmit-flow request",
                observations=observations,
            )

        correlated_flow_id = requested_flow_id or _allocated_flow_id(acknowledgement)
        deadline = asyncio.get_running_loop().time() + VERIFICATION_TIMEOUT_SECONDS
        attempt = 0
        last_effective: TransmitFlowSpecification | None = None
        last_comparison: FlowComparison | None = None
        while True:
            attempt += 1
            after = await _read_inventory(device, protocol_id)
            if after is None:
                observations.append(
                    _observation(
                        phase="post_write",
                        attempt=attempt,
                        inventory=None,
                        outcome="unavailable",
                    )
                )
            else:
                try:
                    flow_id, record, comparison = _creation_candidate(
                        before=before,
                        after=after,
                        requested=specification,
                        protocol_id=protocol_id,
                        correlated_flow_id=correlated_flow_id,
                    )
                except (TypeError, ValueError) as exception:
                    flow_id, record, comparison = correlated_flow_id, None, None
                    details: dict[str, Any] = {"parse_error": str(exception)}
                else:
                    details = {}
                target_flow_id = flow_id if flow_id is not None else correlated_flow_id
                concurrent_activity = _concurrent_topology_activity(
                    before,
                    after,
                    protocol_id,
                    target_flow_id=target_flow_id,
                )
                if concurrent_activity is not None:
                    details["concurrent_topology_activity"] = concurrent_activity
                if record is not None:
                    last_effective = TransmitFlowSpecification.from_inventory_record(record, protocol_id=protocol_id)
                    last_comparison = comparison
                if record is not None and comparison is not None and not comparison.matches:
                    outcome = "contradiction"
                elif record is not None and comparison is not None and comparison.matches:
                    outcome = "confirmed"
                else:
                    outcome = "not_yet_visible"
                observations.append(
                    _observation(
                        phase="post_write",
                        attempt=attempt,
                        inventory=after,
                        outcome=outcome,
                        details=details,
                    )
                )
                if outcome == "confirmed":
                    return _operation_result(
                        operation="create",
                        state=FlowLifecycleState.CONFIRMED,
                        transport=plan.transport,
                        acknowledgement=acknowledgement,
                        effective_confirmation=True,
                        requested=specification,
                        effective=last_effective,
                        comparison=last_comparison,
                        message=(
                            "effective flow matches the request; acknowledgement was not received"
                            if acknowledgement is None
                            else "flow creation was acknowledged and verified by fresh readback"
                        ),
                        observations=observations,
                    )
                if outcome == "contradiction":
                    return _operation_result(
                        operation="create",
                        state=FlowLifecycleState.INCONSISTENT,
                        transport=plan.transport,
                        acknowledgement=acknowledgement,
                        effective_confirmation=False,
                        requested=specification,
                        effective=last_effective,
                        comparison=last_comparison,
                        message="fresh readback of the correlated flow contradicts the request",
                        observations=observations,
                    )
            if not await _wait_for_next_poll(deadline):
                break

    accepted = _accepted(acknowledgement)
    return _operation_result(
        operation="create",
        state=FlowLifecycleState.PARTIAL if accepted else FlowLifecycleState.PENDING,
        transport=plan.transport,
        acknowledgement=acknowledgement,
        effective_confirmation=None,
        requested=specification,
        effective=last_effective,
        comparison=last_comparison,
        message=(
            "request was acknowledged but bounded fresh readback did not confirm the effective flow"
            if accepted
            else "request outcome remains unknown after bounded fresh readback; no retry was sent"
        ),
        observations=observations,
    )


async def delete_transmit_flow(device, flow_id: int) -> FlowOperationResult:
    plan = plan_delete_transmit_flow(device, flow_id)
    if not plan.supported:
        return _operation_result(
            operation="delete",
            state=FlowLifecycleState.UNSUPPORTED,
            transport=plan.transport,
            acknowledgement=None,
            effective_confirmation=None,
            requested=None,
            effective=None,
            comparison=None,
            message="; ".join(plan.reasons),
        )
    assert plan.protocol_id is not None
    protocol_id = plan.protocol_id
    command_specification = {
        "command": "delete_tx_flow",
        "flow_protocol_id": protocol_id,
        "flow_slot": flow_id,
    }
    core.build_command(command_specification)
    observations: list[dict[str, Any]] = []
    async with device.topology_mutation_lock:
        before = await _read_inventory(device, protocol_id)
        observations.append(
            _observation(
                phase="preflight",
                attempt=1,
                inventory=before,
                outcome="available" if before is not None else "unavailable",
            )
        )
        if before is None:
            return _operation_result(
                operation="delete",
                state=FlowLifecycleState.PENDING,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=None,
                requested=None,
                effective=None,
                comparison=None,
                message="fresh preflight inventory was unavailable; no request was sent",
                observations=observations,
            )
        record = _find_record(before, flow_id)
        if record is None:
            return _operation_result(
                operation="delete",
                state=FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=False,
                requested=None,
                effective=None,
                comparison=None,
                message=f"flow {flow_id} is not active; no request was sent",
                observations=observations,
            )
        reported_flow_type = record.get("flow_type")
        if isinstance(reported_flow_type, str) and reported_flow_type.casefold() != FlowType.MULTICAST.value:
            return _operation_result(
                operation="delete",
                state=FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=False,
                requested=None,
                effective=None,
                comparison=None,
                message="only multicast transmit-flow deletion is supported; no request was sent",
                observations=observations,
            )
        requested = TransmitFlowSpecification.from_inventory_record(record, protocol_id=protocol_id)
        if requested.flow_type is not FlowType.MULTICAST:
            return _operation_result(
                operation="delete",
                state=FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=False,
                requested=requested,
                effective=requested,
                comparison=None,
                message="only multicast transmit-flow deletion is supported; no request was sent",
                observations=observations,
            )

        response = await _send_once(device, command_specification)
        acknowledgement = _acknowledgement(response)
        if _rejected(acknowledgement):
            return _operation_result(
                operation="delete",
                state=FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=acknowledgement,
                effective_confirmation=None,
                requested=requested,
                effective=None,
                comparison=None,
                message="device rejected the transmit-flow deletion request",
                observations=observations,
            )

        deadline = asyncio.get_running_loop().time() + VERIFICATION_TIMEOUT_SECONDS
        attempt = 0
        last_effective: TransmitFlowSpecification | None = requested
        last_comparison: FlowComparison | None = None
        while True:
            attempt += 1
            after = await _read_inventory(device, protocol_id)
            if after is None:
                observations.append(
                    _observation(
                        phase="post_write",
                        attempt=attempt,
                        inventory=None,
                        outcome="unavailable",
                    )
                )
            else:
                remaining = _find_record(after, flow_id)
                concurrent_activity = _concurrent_topology_activity(
                    before,
                    after,
                    protocol_id,
                    target_flow_id=flow_id,
                )
                details = (
                    {"concurrent_topology_activity": concurrent_activity} if concurrent_activity is not None else None
                )
                if remaining is not None:
                    last_effective = TransmitFlowSpecification.from_inventory_record(remaining, protocol_id=protocol_id)
                    last_comparison = compare_transmit_flows(requested, last_effective)
                else:
                    last_effective = None
                    last_comparison = None
                if remaining is None:
                    outcome = "confirmed"
                elif last_comparison is not None and not last_comparison.matches:
                    outcome = "contradiction"
                else:
                    outcome = "not_yet_visible"
                observations.append(
                    _observation(
                        phase="post_write",
                        attempt=attempt,
                        inventory=after,
                        outcome=outcome,
                        details=details,
                    )
                )
                if outcome == "confirmed":
                    return _operation_result(
                        operation="delete",
                        state=FlowLifecycleState.DELETED,
                        transport=plan.transport,
                        acknowledgement=acknowledgement,
                        effective_confirmation=True,
                        requested=requested,
                        effective=None,
                        comparison=None,
                        message=(
                            "fresh readback confirms deletion; acknowledgement was not received"
                            if acknowledgement is None
                            else "flow deletion was acknowledged and verified by fresh readback"
                        ),
                        observations=observations,
                    )
                if outcome == "contradiction":
                    return _operation_result(
                        operation="delete",
                        state=FlowLifecycleState.INCONSISTENT,
                        transport=plan.transport,
                        acknowledgement=acknowledgement,
                        effective_confirmation=False,
                        requested=requested,
                        effective=last_effective,
                        comparison=last_comparison,
                        message="fresh readback shows that the correlated flow changed instead of being deleted",
                        observations=observations,
                    )
            if not await _wait_for_next_poll(deadline):
                break

    accepted = _accepted(acknowledgement)
    return _operation_result(
        operation="delete",
        state=FlowLifecycleState.PARTIAL if accepted else FlowLifecycleState.PENDING,
        transport=plan.transport,
        acknowledgement=acknowledgement,
        effective_confirmation=None,
        requested=requested,
        effective=last_effective,
        comparison=last_comparison,
        message=(
            "deletion was acknowledged but bounded fresh readback did not confirm absence"
            if accepted
            else "deletion outcome remains unknown after bounded fresh readback; no retry was sent"
        ),
        observations=observations,
    )
