from __future__ import annotations

import asyncio
import copy
from typing import Any

from netaudio import core
from netaudio.core import _requests, _types
from netaudio.dante import flows
from netaudio.dante.performance_configuration import observed_performance_configuration
from netaudio.dante.flow_preconditions import refresh_flow_state
from netaudio.dante.arc_protocol import (
    advertised_arc_protocol_identifier_for_device,
    flow_inventory_protocol_identifier_for_device,
)
from netaudio.dante.transmit_flow import (
    FlowLifecycleState,
    FlowOperationPlan,
    FlowOperationResult,
)


VERIFICATION_TIMEOUT_SECONDS = 5.0
VERIFICATION_POLL_INTERVAL_SECONDS = 0.25


def _transport(device) -> str:
    return "ddm" if getattr(device, "requires_managed_control", False) else "direct"


def _readback_protocol_id(device) -> int | None:
    try:
        return flow_inventory_protocol_identifier_for_device(device)
    except (AttributeError, RuntimeError):
        return None


def _protocol_id(device, specification: _requests.TransmitFlowSpecification | None = None) -> int | None:
    try:
        return advertised_arc_protocol_identifier_for_device(device)
    except (AttributeError, RuntimeError):
        return None


def _device_protocol_version(device) -> str | None:
    for field_name in ("flow_protocol_version", "protocol_version"):
        value = getattr(device, field_name, None)
        if isinstance(value, str) and value:
            return value
    return None


def _flow_device_facts(device, required_capabilities=()) -> _requests.FlowDeviceFacts:
    channels = getattr(device, "tx_channels", None)
    performance = observed_performance_configuration(device).get("transmit_flow_performance")
    return {
        "managed": bool(getattr(device, "requires_managed_control", False)),
        "locked": getattr(device, "is_locked", None),
        "capability_word": getattr(device, "transmit_flow_authoring_capability_word", None),
        "advertised_protocol": _protocol_id(device),
        "protocol_version": _device_protocol_version(device),
        "sample_rate": getattr(device, "sample_rate", None),
        "encoding": getattr(device, "encoding", None),
        "channels": [int(number) for number in channels] if isinstance(channels, dict) else None,
        "channel_capacity": getattr(device, "routing_capacity_transmit_channel_count", None),
        "capabilities": {
            **{name: getattr(device, name, None) for name in required_capabilities},
            "aes67_configuration_supported": getattr(device, "aes67_configuration_supported", None),
            "aes67_current": getattr(device, "aes67_current", None),
            "redundancy_supported": getattr(device, "switch_redundancy_supported", None),
            "transmit_performance": {"frames_per_packet": performance["frames_per_packet"]}
            if performance is not None
            else None,
        },
    }


def plan_create_transmit_flow(device, specification: _requests.TransmitFlowSpecification) -> FlowOperationPlan:
    protocol_id = _protocol_id(device, specification)
    native = core.plan_transmit_flow_create(
        {
            "protocol_id": protocol_id,
            "specification": specification,
            "device": _flow_device_facts(device, specification.get("protocol", {}).get("required_capabilities", [])),
        }
    )
    reasons = native["reasons"]

    supported = not reasons

    return FlowOperationPlan(
        operation="create",
        state=FlowLifecycleState.PLANNED if supported else FlowLifecycleState.UNSUPPORTED,
        transport=_transport(device),
        protocol_id=protocol_id,
        serializer_cohort=native["serializer_cohort"],
        supported=supported,
        reasons=tuple(dict.fromkeys(reasons)),
        specification=specification,
        flow_id=specification.get("identity", {}).get("global_flow_id"),
        command_specification=native["command"] if supported else None,
        wire_authored_fields=tuple(native["wire_authored_fields"]),
        state_preconditions={
            key: value
            for key, value in (
                ("sample_rate_hz", specification.get("sample_rate_hz")),
                ("encoding_bits", specification.get("encoding_bits")),
            )
            if value is not None
        },
    )


def plan_delete_transmit_flow(device, flow_id: int) -> FlowOperationPlan:
    flow_id = flows.validate_flow_identifier(flow_id)
    protocol_id = _protocol_id(device)
    try:
        native = core.plan_transmit_flow_delete(
            {"protocol_id": protocol_id, "flow_id": flow_id, "device": _flow_device_facts(device)}
        )
    except core.NetaudioCoreError as error:
        raise flows.FlowValidationError(str(error)) from error

    reasons = native["reasons"]

    supported = not reasons

    return FlowOperationPlan(
        operation="delete",
        state=FlowLifecycleState.PLANNED if supported else FlowLifecycleState.UNSUPPORTED,
        transport=_transport(device),
        protocol_id=protocol_id,
        serializer_cohort=native["serializer_cohort"],
        supported=supported,
        reasons=tuple(dict.fromkeys(reasons)),
        flow_id=flow_id,
        command_specification=native["command"] if supported else None,
    )


def canonical_inventory(flow_inventory: dict, protocol_id: int) -> dict[str, Any]:
    if not core.flow_inventory_complete(flow_inventory):
        raise flows.FlowValidationError("complete flow inventory is unavailable", status=409)
    records = flow_inventory.get("flows")
    if not isinstance(records, list):
        raise flows.FlowValidationError("transmitter flow inventory is malformed", status=502)
    specifications = []
    errors = []
    for index, record in enumerate(records):
        try:
            specifications.append(core.transmit_flow_specification(record, protocol_id=protocol_id))
        except (TypeError, ValueError, core.NetaudioCoreError) as exception:
            errors.append({"record_index": index, "error": str(exception), "raw_record": copy.deepcopy(record)})
    return {
        "schema_version": 1,
        "flow_protocol_id": protocol_id,
        "maximum_flow_slots": flow_inventory.get("maximum_flow_slots"),
        "reported_flow_count": flow_inventory.get("reported_flow_count", len(records)),
        "flows": specifications,
        "unparsed_records": errors,
    }


async def inspect_transmit_flows(device) -> dict[str, Any]:
    protocol_id = _readback_protocol_id(device)
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


def _inventory_evidence(inventory: dict) -> _requests.FlowInventoryEvidence:
    records = inventory.get("flows")

    if not isinstance(records, list):
        raise ValueError("flow inventory requires a record list")

    return {
        "flows": records,
        "page_disposition": inventory.get("page_disposition"),
        "reported_flow_count": inventory.get("reported_flow_count"),
    }


def _concurrent_topology_activity(
    before: dict,
    after: dict,
    protocol_id: int,
    *,
    target_flow_id: int | None,
) -> _types.FlowTopologyChange | None:
    return core.flow_topology_change(
        {
            "before": _inventory_evidence(before),
            "after": _inventory_evidence(after),
            "protocol_id": protocol_id,
            "target_flow_id": target_flow_id,
        }
    )


async def _read_inventory(device, protocol_id: int) -> dict | None:
    inventory_protocol = _readback_protocol_id(device)
    if inventory_protocol is None:
        return None

    return await flows.query_tx_flow_inventory(str(device.ipv4), device._arc_port(), inventory_protocol, device=device)


async def _fresh_authoring_protocol(device) -> tuple[int, int] | None:
    protocol_id = _protocol_id(device)
    if protocol_id is None:
        return None
    try:
        response = await device.execute({"command": "channel_count", "protocol_id": protocol_id})
    except (OSError, RuntimeError, TimeoutError, core.NetaudioCoreError):
        return None
    if not response:
        return None
    try:
        channel_count = core.parse_response("channel_count", response)
    except core.NetaudioCoreError:
        return None
    capability_word = channel_count.get("transmit_flow_authoring_capability_word")
    if isinstance(capability_word, bool) or not isinstance(capability_word, int):
        return None
    capabilities = core.flow_authoring_capabilities(capability_word)
    device.transmit_flow_authoring_capability_word = capability_word

    for name, value in capabilities.items():
        setattr(device, name, value)

    return capability_word, protocol_id


async def _refresh_authoring_family(device) -> dict | None:
    try:
        return await flows.query_preferred_receiver_flow_inventory(device)
    except (AttributeError, OSError, RuntimeError, TimeoutError, core.NetaudioCoreError):
        return None


async def _fresh_format_preconditions(
    device,
    specification: _requests.TransmitFlowSpecification,
) -> tuple[str, dict[str, Any]]:
    requested = (
        ("sample_rate_hz", specification.get("sample_rate_hz"), "probe_sample_rate_status"),
        ("encoding_bits", specification.get("encoding_bits"), "probe_encoding_status"),
    )
    requested = tuple((field, value, probe) for field, value, probe in requested if value is not None)

    if not requested:
        return "confirmed", {}

    application = getattr(device, "application", None)
    details: dict[str, Any] = {"requested": {field: value for field, value, _ in requested}}

    if application is None:
        details["reason"] = "device has no attached application for fresh format probes"
        return "unavailable", details

    observed = {}

    for field, expected, probe_name in requested:
        probe = getattr(application, probe_name, None)

        if probe is None:
            details["reason"] = f"{probe_name} is unavailable"
            return "unavailable", details

        try:
            status = await probe(device, timeout=2.0)
        except Exception as exception:
            details["reason"] = f"{probe_name} failed: {exception}"
            return "unavailable", details

        readback = core.flow_format_readback(status if isinstance(status, dict) else None, expected)

        if readback["state"] == "unavailable":
            details["reason"] = f"{probe_name} returned no valid current value"
            return "unavailable", details

        observed[field] = readback["current_value"]

        if readback["state"] == "unverified":
            details["observed"] = observed
            details["mismatch"] = field
            return "contradiction", details

    details["observed"] = observed
    return "confirmed", details


async def _send_once(device, command_specification: dict) -> bytes | None:
    return await device.call_core(lambda client: client.execute(command_specification), request_attempts=1)


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
    acknowledgement: _types.CommandReceipt | None,
    effective_confirmation: bool | None,
    requested: _requests.TransmitFlowSpecification | _types.ObservedTransmitFlowSpecification | None,
    effective: _types.ObservedTransmitFlowSpecification | None,
    comparison: _types.FlowComparison | None,
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


def _accepted(acknowledgement: _types.CommandReceipt | None) -> bool:
    return acknowledgement is not None and acknowledgement.get("accepted") is True


def _rejected(acknowledgement: _types.CommandReceipt | None) -> bool:
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


def _verification_outcome(operation: _requests.FlowMutation, record, comparison, authoring_refresh, details) -> str:
    verification = core.flow_verification(
        {
            "operation": operation,
            "record_present": record is not None,
            "comparison": comparison,
            "authoring_refreshed": authoring_refresh is not None,
        }
    )

    if verification["unavailable_fields"]:
        details["unavailable_fields"] = verification["unavailable_fields"]

    if verification["authoring_refresh_missing"]:
        details["authoring_family_refresh"] = "unavailable"

    return verification["outcome"]


def _allocated_flow_id(acknowledgement: _types.CommandReceipt | None) -> int | None:
    allocation = acknowledgement.get("allocation") if acknowledgement is not None else None
    value = allocation.get("global_flow_id") if isinstance(allocation, dict) else None
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _creation_candidate(
    *,
    before: dict,
    after: dict,
    requested: _requests.TransmitFlowSpecification,
    protocol_id: int,
    correlated_flow_id: int | None,
) -> tuple[int | None, dict | None, _types.FlowComparison | None]:
    try:
        result = core.flow_creation_candidate(
            {
                "before": _inventory_evidence(before),
                "after": _inventory_evidence(after),
                "requested": requested,
                "protocol_id": protocol_id,
                "correlated_flow_id": correlated_flow_id,
            }
        )
    except core.NetaudioCoreError as error:
        raise ValueError(error.detail or str(error)) from error

    return result["flow_id"], result["record"], result["comparison"]


async def create_transmit_flow(device, specification: _requests.TransmitFlowSpecification) -> FlowOperationResult:
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
    command_specification = plan.command_specification
    assert command_specification is not None

    observations: list[dict[str, Any]] = []
    planned_capability = getattr(device, "transmit_flow_authoring_capability_word", None)
    async with device.topology_mutation_lock:
        reason = await refresh_flow_state(device, rtp=specification.get("media_mode") == "rtp_aes67")
        if reason is not None:
            return _operation_result(
                operation="create",
                state=FlowLifecycleState.PENDING,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=None,
                requested=specification,
                effective=None,
                comparison=None,
                message=f"{reason}; no request was sent",
                observations=[
                    _observation(
                        phase="authoring_preconditions",
                        attempt=1,
                        inventory=None,
                        outcome="unavailable",
                        details={"reason": reason},
                    )
                ],
            )
        fresh_authoring = await _fresh_authoring_protocol(device)
        if fresh_authoring is None or fresh_authoring != (planned_capability, protocol_id):
            return _operation_result(
                operation="create",
                state=FlowLifecycleState.PENDING,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=None,
                requested=specification,
                effective=None,
                comparison=None,
                message=(
                    "fresh transmit-flow authoring capability was unavailable; no request was sent"
                    if fresh_authoring is None
                    else "fresh transmit-flow authoring capability selected a different protocol; no request was sent"
                ),
                observations=[
                    _observation(
                        phase="authoring_capability",
                        attempt=1,
                        inventory=None,
                        outcome="unavailable" if fresh_authoring is None else "contradiction",
                        details={
                            "planned_protocol_id": protocol_id,
                            "fresh_capability_word": fresh_authoring[0] if fresh_authoring else None,
                            "fresh_protocol_id": fresh_authoring[1] if fresh_authoring else None,
                        },
                    )
                ],
            )
        before = await _read_inventory(device, protocol_id)
        preflight = core.flow_create_preflight(
            {"inventory": before, "requested_flow_id": specification.get("identity", {}).get("global_flow_id")}
        )
        observations.append(
            _observation(
                phase="preflight",
                attempt=1,
                inventory=before,
                outcome="unavailable" if preflight["state"] == "unavailable" else "available",
            )
        )
        if preflight["state"] != "ready":
            unavailable = preflight["state"] == "unavailable"

            return _operation_result(
                operation="create",
                state=FlowLifecycleState.PENDING if unavailable else FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=None if unavailable else False,
                requested=specification,
                effective=None,
                comparison=None,
                message=f"{preflight['reason']}; no request was sent",
                observations=observations,
            )

        assert before is not None, "ready native preflight requires an inventory"

        if specification.get("sample_rate_hz") is not None or specification.get("encoding_bits") is not None:
            precondition_outcome, precondition_details = await _fresh_format_preconditions(device, specification)
            observations.append(
                _observation(
                    phase="format_precondition",
                    attempt=1,
                    inventory=None,
                    outcome=precondition_outcome,
                    details=precondition_details,
                )
            )
            if precondition_outcome == "unavailable":
                return _operation_result(
                    operation="create",
                    state=FlowLifecycleState.PENDING,
                    transport=plan.transport,
                    acknowledgement=None,
                    effective_confirmation=None,
                    requested=specification,
                    effective=None,
                    comparison=None,
                    message="fresh sample-rate or encoding precondition readback was unavailable; no request was sent",
                    observations=observations,
                )
            if precondition_outcome == "contradiction":
                return _operation_result(
                    operation="create",
                    state=FlowLifecycleState.REJECTED,
                    transport=plan.transport,
                    acknowledgement=None,
                    effective_confirmation=False,
                    requested=specification,
                    effective=None,
                    comparison=None,
                    message="fresh device state contradicts the requested sample-rate or encoding precondition; no request was sent",
                    observations=observations,
                )

        refreshed_plan = plan_create_transmit_flow(device, specification)
        if not refreshed_plan.supported or refreshed_plan.command_specification != command_specification:
            return _operation_result(
                operation="create",
                state=FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=None,
                requested=specification,
                effective=None,
                comparison=None,
                message="; ".join(refreshed_plan.reasons) or "flow facts changed; validate the request again",
                observations=observations,
            )

        response = await _send_once(device, command_specification)
        acknowledgement = core.command_acknowledgement(response)

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

        authoring_refresh = await _refresh_authoring_family(device)
        observations.append(
            _observation(
                phase="authoring_family_refresh",
                attempt=1,
                inventory=authoring_refresh,
                outcome="available" if authoring_refresh is not None else "unavailable",
                details={"receiver_flow_inventory_family": getattr(device, "receiver_flow_inventory_family", None)},
            )
        )

        correlated_flow_id = specification.get("identity", {}).get("global_flow_id") or _allocated_flow_id(
            acknowledgement
        )
        deadline = asyncio.get_running_loop().time() + VERIFICATION_TIMEOUT_SECONDS
        attempt = 0
        last_effective: _types.ObservedTransmitFlowSpecification | None = None
        last_comparison: _types.FlowComparison | None = None
        while True:
            attempt += 1
            after = await _read_inventory(device, protocol_id)
            if after is None or not core.flow_inventory_complete(after):
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
                    last_effective = core.transmit_flow_specification(record, protocol_id=protocol_id)
                    last_comparison = comparison
                outcome = _verification_outcome("create", record, comparison, authoring_refresh, details)
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
            "request was acknowledged and correlated fresh readback was found, but some requested fields were not exposed"
            if accepted and last_comparison is not None and last_comparison["unavailable_fields"]
            else (
                "request was acknowledged but bounded fresh readback did not confirm the effective flow"
                if accepted
                else "request outcome remains unknown after bounded fresh readback; no retry was sent"
            )
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
    command_specification = plan.command_specification
    assert command_specification is not None

    observations: list[dict[str, Any]] = []
    async with device.topology_mutation_lock:
        fresh_authoring = await _fresh_authoring_protocol(device)
        if fresh_authoring is None or fresh_authoring[1] != protocol_id:
            return _operation_result(
                operation="delete",
                state=FlowLifecycleState.PENDING,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=None,
                requested=None,
                effective=None,
                comparison=None,
                message=(
                    "fresh transmit-flow authoring capability was unavailable; no request was sent"
                    if fresh_authoring is None
                    else "fresh transmit-flow authoring capability selected a different protocol; no request was sent"
                ),
                observations=[
                    _observation(
                        phase="authoring_capability",
                        attempt=1,
                        inventory=None,
                        outcome="unavailable" if fresh_authoring is None else "contradiction",
                        details={
                            "planned_protocol_id": protocol_id,
                            "fresh_capability_word": fresh_authoring[0] if fresh_authoring else None,
                            "fresh_protocol_id": fresh_authoring[1] if fresh_authoring else None,
                        },
                    )
                ],
            )
        before = await _read_inventory(device, protocol_id)
        preflight = core.flow_delete_preflight({"inventory": before, "flow_id": flow_id, "protocol_id": protocol_id})
        observations.append(
            _observation(
                phase="preflight",
                attempt=1,
                inventory=before,
                outcome=preflight["state"],
            )
        )

        if preflight["state"] != "ready":
            unavailable = preflight["state"] == "unavailable"

            return _operation_result(
                operation="delete",
                state=FlowLifecycleState.PENDING if unavailable else FlowLifecycleState.REJECTED,
                transport=plan.transport,
                acknowledgement=None,
                effective_confirmation=None if unavailable else False,
                requested=None,
                effective=None,
                comparison=None,
                message=f"{preflight['reason']}; no request was sent",
                observations=observations,
            )

        requested = preflight["specification"]
        assert requested is not None
        assert before is not None

        response = await _send_once(device, command_specification)
        acknowledgement = core.command_acknowledgement(response)
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

        authoring_refresh = await _refresh_authoring_family(device)
        observations.append(
            _observation(
                phase="authoring_family_refresh",
                attempt=1,
                inventory=authoring_refresh,
                outcome="available" if authoring_refresh is not None else "unavailable",
                details={"receiver_flow_inventory_family": getattr(device, "receiver_flow_inventory_family", None)},
            )
        )

        deadline = asyncio.get_running_loop().time() + VERIFICATION_TIMEOUT_SECONDS
        attempt = 0
        last_effective: _types.ObservedTransmitFlowSpecification | None = requested
        last_comparison: _types.FlowComparison | None = None
        while True:
            attempt += 1
            after = await _read_inventory(device, protocol_id)
            if after is None or not core.flow_inventory_complete(after):
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
                    {"concurrent_topology_activity": concurrent_activity} if concurrent_activity is not None else {}
                )
                if remaining is not None:
                    last_effective = core.transmit_flow_specification(remaining, protocol_id=protocol_id)
                    last_comparison = core.compare_transmit_flows(requested, last_effective)
                else:
                    last_effective = None
                    last_comparison = None
                outcome = _verification_outcome("delete", remaining, last_comparison, authoring_refresh, details)
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
