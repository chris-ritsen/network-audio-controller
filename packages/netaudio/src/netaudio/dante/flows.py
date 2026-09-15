from __future__ import annotations

import asyncio
import copy
import logging
from collections.abc import Awaitable, Callable
from typing import cast

from netaudio.common.app_config import settings as app_settings
from netaudio.network_path import source_address_for
from netaudio.dante.channel_status_paging import (
    advertised_arc_protocol_identifier_for_device,
    modern_arc_protocol_identifier_for_device,
)
from netaudio.dante.const import (
    FLOW_CREATE_PROTOCOL_IDS,
    FLOW_DELETE_PROTOCOL_IDS,
    FLOW_QUERY_PROTOCOL_IDS,
    MODERN_ARC_PROTOCOL_IDS,
    PROTOCOL_ARC_2809,
    RESULT_CODE_SUCCESS,
    RESULT_CODE_SUCCESS_EXTENDED,
)

logger = logging.getLogger("netaudio")

EXTERNAL_RTP_DEFAULT_PORT = 4321


class FlowValidationError(ValueError):
    def __init__(self, message: str, *, status: int = 400):
        super().__init__(message)
        self.status = status


def external_receiver_subscription_specification(
    commands,
    device,
    flow,
    receiver_channel_ids,
    flow_slot_assignments,
    *,
    receiver_supports_multiple_interfaces: bool,
) -> dict:
    if not getattr(flow, "routable", False):
        reasons = "; ".join(getattr(flow, "routability_errors", ()) or ("flow is not routable",))
        raise FlowValidationError(f"external flow is not routable: {reasons}")
    if not isinstance(receiver_channel_ids, (list, tuple)) or not isinstance(flow_slot_assignments, (list, tuple)):
        raise FlowValidationError("receiver channels and flow-slot assignments must be lists")
    if not isinstance(receiver_supports_multiple_interfaces, bool):
        raise FlowValidationError("receiver multiple-interface support must be Boolean")
    receiver_channel_ids = list(receiver_channel_ids)
    flow_slot_assignments = list(flow_slot_assignments)
    if not receiver_channel_ids or len(receiver_channel_ids) != len(flow_slot_assignments):
        raise FlowValidationError("receiver channels and flow-slot assignments must be parallel non-empty lists")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535
        for value in receiver_channel_ids
    ):
        raise FlowValidationError("receiver channel IDs must be positive 16-bit integers")
    if len(receiver_channel_ids) != len(set(receiver_channel_ids)):
        raise FlowValidationError("receiver channel IDs must be unique")
    advertised_slot_count = getattr(flow, "channel_count", None)
    if (
        isinstance(advertised_slot_count, bool)
        or not isinstance(advertised_slot_count, int)
        or advertised_slot_count <= 0
    ):
        raise FlowValidationError("external flow has no positive advertised slot count")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= advertised_slot_count
        for value in flow_slot_assignments
    ):
        raise FlowValidationError("flow-slot assignments must be zero or within the advertised slot count")
    receiver_channels = getattr(device, "rx_channels", None)
    if not isinstance(receiver_channels, dict):
        raise FlowValidationError("receiver channel inventory is unavailable", status=409)
    known_receiver_ids = set(receiver_channels)
    unavailable = sorted(set(receiver_channel_ids) - known_receiver_ids)
    if unavailable:
        raise FlowValidationError(f"receiver channel not found: {', '.join(map(str, unavailable))}", status=404)

    device_protocol = advertised_arc_protocol_identifier_for_device(device)
    if device_protocol is None:
        raise FlowValidationError("device has no ARC protocol metadata")
    if getattr(device, "requires_managed_control", False):
        raise FlowValidationError(
            "direct external RTP subscription is unavailable for managed-only devices", status=409
        )

    primary_address = getattr(flow, "primary_destination_address", None)
    primary_port = getattr(flow, "primary_destination_port", None)
    if not primary_address or not primary_port:
        raise FlowValidationError("external flow has no primary destination socket")
    secondary_address = getattr(flow, "secondary_destination_address", None)
    secondary_port = getattr(flow, "secondary_destination_port", None)
    advertisement_supports_multiple_interfaces = secondary_address is not None and secondary_port is not None
    secondary_destination = None
    if advertisement_supports_multiple_interfaces and receiver_supports_multiple_interfaces:
        secondary_destination = {"address": secondary_address, "port": secondary_port}

    return commands.subscribe_external_rtp(
        device_protocol=device_protocol,
        receiver_channel_ids=receiver_channel_ids,
        flow_slot_assignments=flow_slot_assignments,
        advertised_flow_slot_count=advertised_slot_count,
        source_address=flow.source_ipv4,
        session_id=flow.session_id,
        clock_offset=flow.clock_offset or 0,
        primary_destination={"address": primary_address, "port": primary_port},
        secondary_destination=secondary_destination,
        advertisement_supports_multiple_interfaces=advertisement_supports_multiple_interfaces,
        receiver_supports_multiple_interfaces=bool(receiver_supports_multiple_interfaces),
    )


async def subscribe_external_rtp(
    application,
    device,
    flow,
    receiver_channel_ids,
    flow_slot_assignments,
    *,
    receiver_supports_multiple_interfaces: bool,
) -> dict:
    specification = external_receiver_subscription_specification(
        application.commands,
        device,
        flow,
        receiver_channel_ids,
        flow_slot_assignments,
        receiver_supports_multiple_interfaces=receiver_supports_multiple_interfaces,
    )
    expected_endpoints = [
        {
            "ipv4_address": destination["address"],
            "udp_port": destination["port"] or EXTERNAL_RTP_DEFAULT_PORT,
        }
        for destination in (
            specification["primary_destination"],
            specification.get("secondary_destination"),
        )
        if destination is not None
    ]
    expected_identities = [
        {
            "receiver_channel": receiver_channel,
            "flow_slot": flow_slot,
            "source_ipv4": flow.source_ipv4,
            "session_id": flow.session_id,
            "interface_endpoints": expected_endpoints,
        }
        for receiver_channel, flow_slot in zip(receiver_channel_ids, flow_slot_assignments)
        if flow_slot != 0
    ]
    async with device.topology_mutation_lock:
        before = await query_preferred_receiver_flow_inventory(device)
        if before is None:
            return {
                "result_code": None,
                "request_acknowledged": False,
                "arc_effective_state_confirmed": None,
                "sdp_correlation_confirmed": None,
                "rtp_packet_reception_confirmed": None,
                "clock_lock_confirmed": None,
                "persistence_confirmed": None,
                "decoded_audio_confirmed": None,
                "mutation_sent": False,
                "message": "complete fresh receiver-flow baseline was unavailable; no request was sent",
                "requested_effective_identities": expected_identities,
                "receiver_flow_before": None,
                "receiver_flow_after": None,
            }
        response = await device.execute(specification)
        result_code = _parsed_response("result_code", response) if response else None
        acknowledged = result_code in {RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED}
        after = await query_preferred_receiver_flow_inventory(device) if acknowledged else None
    observed_index = effective_external_subscription_index(after or {})
    observed_identities = [
        identity
        for receiver_channel in receiver_channel_ids
        for identity in observed_index.get(receiver_channel, ())
        if identity.get("source_ipv4") == flow.source_ipv4 and identity.get("session_id") == flow.session_id
    ]
    expected_projection = sorted(
        (_effective_identity_projection(identity) for identity in expected_identities),
        key=repr,
    )
    observed_projection = sorted(
        (_effective_identity_projection(identity) for identity in observed_identities),
        key=repr,
    )
    arc_confirmed = expected_projection == observed_projection if after is not None else None
    sdp_confirmed = (
        all(_identity_has_sdp_correlation(after, identity) for identity in observed_identities)
        if arc_confirmed is True and after is not None and observed_identities
        else None
    )
    if after is not None:
        apply_page = getattr(device, "apply_receiver_flow_status_page", None)
        if apply_page is not None:
            apply_page(after)
    return {
        "result_code": result_code,
        "request_acknowledged": acknowledged,
        "arc_effective_state_confirmed": arc_confirmed,
        "sdp_correlation_confirmed": sdp_confirmed,
        "rtp_packet_reception_confirmed": None,
        "clock_lock_confirmed": None,
        "persistence_confirmed": None,
        "decoded_audio_confirmed": None,
        "mutation_sent": True,
        "message": (
            "external receiver subscription confirmed by complete fresh ARC readback"
            if arc_confirmed is True
            else (
                "request acknowledged but complete fresh ARC readback was unavailable"
                if acknowledged and after is None
                else (
                    "request acknowledged but fresh ARC readback contradicted the requested identity"
                    if acknowledged
                    else "external receiver subscription was not acknowledged"
                )
            )
        ),
        "flow_identity": {"source_ipv4": flow.source_ipv4, "session_id": flow.session_id},
        "receiver_channel_ids": list(receiver_channel_ids),
        "flow_slot_assignments": list(flow_slot_assignments),
        "requested_effective_identities": expected_identities,
        "observed_effective_identities": observed_identities,
        "receiver_flow_before": before,
        "receiver_flow_after": after,
    }


def _effective_identity_projection(identity: dict) -> tuple:
    endpoints = tuple(
        (endpoint.get("ipv4_address"), endpoint.get("udp_port"))
        for endpoint in identity.get("interface_endpoints") or ()
        if isinstance(endpoint, dict)
    )
    return (
        identity.get("receiver_channel"),
        identity.get("flow_slot"),
        identity.get("source_ipv4"),
        identity.get("session_id"),
        endpoints,
    )


def _identity_has_sdp_correlation(inventory: dict, identity: dict) -> bool:
    for flow in inventory.get("flows") or ():
        if not isinstance(flow, dict):
            continue
        external = flow.get("external_identity")
        if not isinstance(external, dict):
            continue
        if external.get("source_ipv4") == identity.get("source_ipv4") and external.get("session_id") == identity.get(
            "session_id"
        ):
            correlation = flow.get("sdp_correlation")
            return isinstance(correlation, dict) and correlation.get("matched") is True
    return False


def validate_flow_slot(flow_slot) -> int:
    if isinstance(flow_slot, bool) or not isinstance(flow_slot, int) or not 1 <= flow_slot <= 32:
        raise FlowValidationError("flow_slot must be an integer from 1 to 32")
    return flow_slot


def validate_flow_channels(channel_numbers) -> list[int]:
    if not isinstance(channel_numbers, list) or not channel_numbers:
        raise FlowValidationError("channels must be a non-empty list")
    if any(isinstance(number, bool) or not isinstance(number, int) or number < 1 for number in channel_numbers):
        raise FlowValidationError("channels must contain positive integers")
    if len(set(channel_numbers)) != len(channel_numbers):
        raise FlowValidationError("channels must not contain duplicates")
    return channel_numbers


def require_available_tx_channels(channel_numbers, available_channels) -> None:
    unavailable_channels = sorted(set(channel_numbers) - set(available_channels))
    if unavailable_channels:
        unavailable = ", ".join(str(number) for number in unavailable_channels)
        raise FlowValidationError(f"tx channel not found: {unavailable}", status=404)


def require_available_flow_slot(device_flows, flow_slot: int) -> None:
    if any(_flow_slot(flow) == flow_slot for flow in device_flows):
        raise FlowValidationError(f"flow slot {flow_slot} is already in use", status=409)


def _flow_slot(flow: dict) -> int | None:
    # Legacy inventory and modern status pages use different field names.
    return flow.get("global_flow_id") if "global_flow_id" in flow else flow.get("flow_number")


def require_supported_flow_slot(flow_slot: int, max_flow_slots: int) -> None:
    if flow_slot > max_flow_slots:
        raise FlowValidationError(
            f"flow slot {flow_slot} exceeds the device capacity of {max_flow_slots}",
            status=409,
        )


def require_multicast_flow(device_flows, flow_slot: int) -> dict:
    flow = next(
        (entry for entry in device_flows if _flow_slot(entry) == flow_slot),
        None,
    )
    if flow is None:
        raise FlowValidationError(f"flow slot {flow_slot} is not active", status=404)
    if str(flow.get("flow_type", "")).lower() != "multicast":
        raise FlowValidationError(f"flow slot {flow_slot} is not multicast", status=409)
    return flow


def require_creatable_flow_protocol(flow_protocol_id: int) -> None:
    if flow_protocol_id not in FLOW_CREATE_PROTOCOL_IDS:
        raise FlowValidationError(
            f"flow protocol 0x{flow_protocol_id:04X} has no verified create format",
            status=409,
        )


def require_deletable_flow_protocol(flow_protocol_id: int, flow_slot: int) -> None:
    if flow_protocol_id not in FLOW_DELETE_PROTOCOL_IDS:
        raise FlowValidationError(
            f"flow protocol 0x{flow_protocol_id:04X} has no verified delete format",
            status=409,
        )
    if flow_protocol_id == PROTOCOL_ARC_2809 and flow_slot != 2:
        raise FlowValidationError(
            "flow protocol 0x2809 has a verified delete format only for flow slot 2",
            status=409,
        )


def _parsed_response(kind: str, response: bytes):
    from netaudio import core

    try:
        return core.parse_response(kind, response)
    except core.NetaudioCoreError as exception:
        logger.debug(f"Discarding unparseable {kind} response: {exception}")
        return None


async def _request(
    device_ip: str,
    arc_port: int,
    command_specification: dict,
    timeout_ms: int,
    attempts: int,
    device=None,
) -> bytes | None:
    if device is not None:
        return await device.execute(command_specification)
    from netaudio import core

    def _send():
        local_ip = source_address_for(device_ip, app_settings.interface)
        client = core.CoreClient(
            device_ip, arc_port=arc_port, timeout_ms=timeout_ms, attempts=attempts, local_ip=local_ip
        )
        try:
            packet = core.build_command(command_specification)
            return client.request(packet, arc_port)
        finally:
            client.close()

    try:
        return await asyncio.to_thread(_send)
    except core.NetaudioCoreError:
        return None


async def _request_with_optional_device(
    device_ip: str,
    arc_port: int,
    command_specification: dict,
    timeout_ms: int,
    attempts: int,
    device,
) -> bytes | None:
    if device is None:
        return await _request(
            device_ip,
            arc_port,
            command_specification,
            timeout_ms=timeout_ms,
            attempts=attempts,
        )
    return await _request(
        device_ip,
        arc_port,
        command_specification,
        timeout_ms,
        attempts,
        device=device,
    )


async def _query_tx_inventory_with_optional_device(
    device_ip: str,
    arc_port: int,
    flow_protocol_id: int,
    device,
) -> dict | None:
    if device is None:
        return await query_tx_flow_inventory(device_ip, arc_port, flow_protocol_id)
    return await query_tx_flow_inventory(device_ip, arc_port, flow_protocol_id, device=device)


async def detect_flow_protocol(device_ip: str, arc_port: int, *, device=None) -> int | None:
    if device is not None:
        try:
            protocol_ids = (modern_arc_protocol_identifier_for_device(device),)
        except RuntimeError:
            protocol_ids = FLOW_QUERY_PROTOCOL_IDS
    else:
        protocol_ids = FLOW_QUERY_PROTOCOL_IDS
    for flow_protocol_id in protocol_ids:
        command_specification = {
            "command": "query_tx_flows",
            "flow_protocol_id": flow_protocol_id,
            "starting_flow": 1,
        }
        response = await _request_with_optional_device(
            device_ip,
            arc_port,
            command_specification,
            timeout_ms=500,
            attempts=1,
            device=device,
        )
        if response and _parsed_response("result_code", response) in (
            RESULT_CODE_SUCCESS,
            RESULT_CODE_SUCCESS_EXTENDED,
        ):
            return flow_protocol_id
    return None


async def query_tx_flow_inventory(device_ip: str, arc_port: int, flow_protocol_id: int, *, device=None) -> dict | None:
    if flow_protocol_id in MODERN_ARC_PROTOCOL_IDS:
        response = await _request_with_optional_device(
            device_ip,
            arc_port,
            {
                "command": "query_tx_flows",
                "flow_protocol_id": flow_protocol_id,
                "starting_flow": 1,
            },
            timeout_ms=1000,
            attempts=2,
            device=device,
        )
        if not response or _parsed_response("result_code", response) != RESULT_CODE_SUCCESS:
            return None
        flow_page = _parsed_response("transmitter_flow_status_page", response)
        if not isinstance(flow_page, dict):
            return None
        maximum_flow_slots = flow_page.get("maximum_flow_slots")
        reported_flow_count = flow_page.get("reported_flow_count")
        status_flows = flow_page.get("flows")
        if (
            isinstance(maximum_flow_slots, bool)
            or not isinstance(maximum_flow_slots, int)
            or not 1 <= maximum_flow_slots <= 32
            or isinstance(reported_flow_count, bool)
            or not isinstance(reported_flow_count, int)
            or not isinstance(status_flows, list)
            or reported_flow_count != len(status_flows)
            or reported_flow_count > maximum_flow_slots
        ):
            return None
        flow_numbers = set()
        for status_flow in status_flows:
            if not isinstance(status_flow, dict):
                return None
            flow_number = status_flow.get("global_flow_id")
            if (
                isinstance(flow_number, bool)
                or not isinstance(flow_number, int)
                or not 1 <= flow_number <= maximum_flow_slots
                or flow_number in flow_numbers
            ):
                return None
            flow_numbers.add(flow_number)
        return {
            "max_flow_slots": maximum_flow_slots,
            "reported_flow_count": reported_flow_count,
            "flows": status_flows,
        }

    if flow_protocol_id not in FLOW_CREATE_PROTOCOL_IDS:
        return None

    device_flows = []
    seen_flow_numbers = set()
    starting_flow = 1
    max_flow_slots = None

    while True:
        command_specification = {
            "command": "query_tx_flows",
            "flow_protocol_id": flow_protocol_id,
            "starting_flow": starting_flow,
        }
        response = await _request_with_optional_device(
            device_ip,
            arc_port,
            command_specification,
            timeout_ms=1000,
            attempts=2,
            device=device,
        )
        if not response:
            return None

        result_code = _parsed_response("result_code", response)
        if result_code not in (RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED):
            return None
        flow_page = _parsed_response("tx_flow_page", response)
        if not isinstance(flow_page, dict):
            return None
        page_max_flow_slots = flow_page.get("max_flow_slots")
        if (
            isinstance(page_max_flow_slots, bool)
            or not isinstance(page_max_flow_slots, int)
            or not 1 <= page_max_flow_slots <= 32
        ):
            return None
        if max_flow_slots is None:
            max_flow_slots = page_max_flow_slots
        elif page_max_flow_slots != max_flow_slots:
            return None
        page_flows = flow_page.get("flows")
        if not isinstance(page_flows, list):
            return None
        page_flow_numbers = []
        for flow in page_flows:
            if not isinstance(flow, dict):
                return None
            flow_number = flow.get("flow_number")
            if (
                isinstance(flow_number, bool)
                or not isinstance(flow_number, int)
                or not 1 <= flow_number <= max_flow_slots
                or flow_number in seen_flow_numbers
            ):
                return None
            page_flow_numbers.append(flow_number)
            seen_flow_numbers.add(flow_number)
            device_flows.append(flow)

        if result_code == RESULT_CODE_SUCCESS:
            return {"max_flow_slots": max_flow_slots, "flows": device_flows}
        if not page_flow_numbers:
            return None

        next_starting_flow = max(page_flow_numbers) + 1
        if next_starting_flow <= starting_flow or next_starting_flow > max_flow_slots:
            return None
        starting_flow = next_starting_flow


async def query_preferred_tx_flow_inventory(
    device_ip: str,
    arc_port: int,
    mutation_protocol_id: int,
    *,
    device=None,
) -> dict | None:
    status_protocol_id = PROTOCOL_ARC_2809
    if device is not None:
        advertised_protocol_id = advertised_arc_protocol_identifier_for_device(device)
        if advertised_protocol_id is None or advertised_protocol_id in MODERN_ARC_PROTOCOL_IDS:
            status_protocol_id = modern_arc_protocol_identifier_for_device(device)
    status_inventory = await _query_tx_inventory_with_optional_device(
        device_ip,
        arc_port,
        status_protocol_id,
        device=device,
    )
    if status_inventory is not None:
        return {**status_inventory, "flow_protocol_id": status_protocol_id}
    if mutation_protocol_id in MODERN_ARC_PROTOCOL_IDS:
        return None
    inventory = await _query_tx_inventory_with_optional_device(device_ip, arc_port, mutation_protocol_id, device)
    if inventory is None:
        return None
    return {**inventory, "flow_protocol_id": mutation_protocol_id}


def inventory_from_receiver_flow_status_page(page: dict) -> dict:
    receiver_flows = []
    for flow in page.get("flows") or []:
        if not isinstance(flow, dict):
            continue
        local_receiver_channel_count = flow.get("local_receiver_channel_count") or 0
        flow_type = flow.get("flow_type")
        if flow_type is None:
            flow_type_code = flow.get("flow_type_code")
            flow_type = f"0x{flow_type_code:04X}" if isinstance(flow_type_code, int) else None
        normalized = copy.deepcopy(flow)
        normalized.update(
            {
                "flow_number": flow.get("flow_number", flow.get("global_flow_id")),
                "flow_type": flow_type,
                "local_receiver_channel_count": local_receiver_channel_count,
                "destination_internet_protocol_version_four_address": flow.get(
                    "destination_internet_protocol_version_four_address"
                )
                or "",
            }
        )
        receiver_flows.append(normalized)
    inventory = copy.deepcopy(page)
    inventory["page_disposition"] = page.get("page_disposition", "unknown")
    inventory["status_page"] = copy.deepcopy(page)
    inventory["flows"] = receiver_flows
    return inventory


def correlate_receiver_flow_inventory(inventory: dict, sap_inventory) -> dict:
    correlated = copy.deepcopy(inventory)
    for flow in correlated.get("flows") or []:
        if not isinstance(flow, dict):
            continue
        identity = flow.get("external_identity")
        source_ipv4 = identity.get("source_ipv4") if isinstance(identity, dict) else None
        session_id = identity.get("session_id") if isinstance(identity, dict) else None
        if not isinstance(source_ipv4, str) or isinstance(session_id, bool) or not isinstance(session_id, int):
            continue
        advertised = None
        if sap_inventory is not None:
            try:
                advertised = sap_inventory.get(source_ipv4, session_id)
            except ValueError:
                advertised = None
        flow["sdp_correlation"] = {
            "matched": advertised is not None,
            "source_ipv4": source_ipv4,
            "session_id": session_id,
            "advertisement": advertised.to_dict() if advertised is not None else None,
        }
        for effective_identity in flow.get("effective_subscription_identities") or ():
            if isinstance(effective_identity, dict):
                effective_identity["sdp_correlation_confirmed"] = advertised is not None
    return correlated


def effective_external_subscription_index(inventory: dict) -> dict[int, list[dict]]:
    index: dict[int, list[dict]] = {}
    for flow in inventory.get("flows") or []:
        if not isinstance(flow, dict):
            continue
        for identity in flow.get("effective_subscription_identities") or []:
            receiver_channel = identity.get("receiver_channel") if isinstance(identity, dict) else None
            if isinstance(receiver_channel, int) and not isinstance(receiver_channel, bool):
                index.setdefault(receiver_channel, []).append(copy.deepcopy(identity))
    return index


async def query_preferred_receiver_flow_inventory(device, *, require_complete: bool = True) -> dict | None:
    application = device.application
    status_page = None
    inventory_opcode = getattr(device, "receiver_flow_inventory_opcode", None)
    modern_query = (
        getattr(application, "query_modern_arc_receiver_flow_status", None) if application is not None else None
    )
    if inventory_opcode == 0x3600 or (inventory_opcode is None and callable(modern_query)):
        if not callable(modern_query):
            return None
        try:
            query = cast("Callable[[object], Awaitable[dict | None]]", modern_query)
            status_page = await query(device)
        except RuntimeError:
            status_page = None
    if status_page is not None:
        apply_page = getattr(device, "apply_receiver_flow_status_page", None)
        if apply_page is not None:
            apply_page(status_page)
        if require_complete and status_page.get("page_disposition") != "complete":
            return None
        return correlate_receiver_flow_inventory(
            inventory_from_receiver_flow_status_page(status_page),
            getattr(application, "external_flows", None),
        )
    if inventory_opcode == 0x3600:
        return None
    if getattr(device, "requires_managed_control", False):
        return None
    from netaudio.cli_support.execution import _get_arc_port

    arc_port = device._arc_port() if callable(getattr(device, "_arc_port", None)) else _get_arc_port(device)
    inventory = await query_receiver_flow_inventory(str(device.ipv4), arc_port, device=device)
    if inventory is None:
        return None
    return correlate_receiver_flow_inventory(inventory, getattr(application, "external_flows", None))


async def query_receiver_flow_inventory(device_ip: str, arc_port: int, *, device=None) -> dict | None:
    starting_flow = 1
    maximum_flow_slots = None
    flow_numbers = set()
    receiver_flows = []
    pages = []
    while True:
        response = await _request_with_optional_device(
            device_ip,
            arc_port,
            {"command": "query_receiver_flows", "starting_flow": starting_flow},
            timeout_ms=1000,
            attempts=2,
            device=device,
        )
        if not response:
            return None
        result_code = _parsed_response("result_code", response)
        if result_code not in (RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED):
            return None
        flow_page = _parsed_response("receiver_flow_page", response)
        if not isinstance(flow_page, dict):
            return None
        page_maximum = flow_page.get("maximum_flow_slots")
        page_flows = flow_page.get("flows")
        if (
            isinstance(page_maximum, bool)
            or not isinstance(page_maximum, int)
            or not 1 <= page_maximum <= 32
            or not isinstance(page_flows, list)
            or flow_page.get("reported_flow_count") != len(page_flows)
        ):
            return None
        if maximum_flow_slots is None:
            maximum_flow_slots = page_maximum
        elif page_maximum != maximum_flow_slots:
            return None
        page_flow_numbers = []
        for receiver_flow in page_flows:
            flow_number = receiver_flow.get("flow_number") if isinstance(receiver_flow, dict) else None
            if (
                isinstance(flow_number, bool)
                or not isinstance(flow_number, int)
                or not starting_flow <= flow_number <= maximum_flow_slots
                or flow_number in flow_numbers
            ):
                return None
            flow_numbers.add(flow_number)
            page_flow_numbers.append(flow_number)
            receiver_flows.append(receiver_flow)
        pages.append(flow_page)
        if result_code == RESULT_CODE_SUCCESS and flow_page.get("page_disposition") == "complete":
            aggregate = dict(flow_page)
            aggregate["reported_flow_count"] = len(receiver_flows)
            aggregate["flows"] = receiver_flows
            aggregate["pages"] = pages
            return aggregate
        if result_code != RESULT_CODE_SUCCESS_EXTENDED or flow_page.get("page_disposition") != "more_pages":
            return None
        if not page_flow_numbers:
            return None
        next_starting_flow = max(page_flow_numbers) + 1
        if next_starting_flow <= starting_flow or next_starting_flow > maximum_flow_slots:
            return None
        starting_flow = next_starting_flow


async def query_receiver_port_ranges(device_ip: str, arc_port: int, *, device=None) -> dict | None:
    response = await _request_with_optional_device(
        device_ip,
        arc_port,
        {"command": "query_receiver_port_ranges"},
        timeout_ms=1000,
        attempts=2,
        device=device,
    )
    if not response or _parsed_response("result_code", response) != RESULT_CODE_SUCCESS:
        return None
    port_ranges = _parsed_response("receiver_port_ranges", response)
    return port_ranges if isinstance(port_ranges, dict) else None


async def query_transmit_channel_capabilities(
    device_ip: str,
    arc_port: int,
    starting_channel_identifier: int = 1,
    maximum_channel_count: int = 0,
    *,
    device=None,
) -> dict | None:
    response = await _request_with_optional_device(
        device_ip,
        arc_port,
        {
            "command": "query_transmit_channel_capabilities",
            "starting_channel_identifier": starting_channel_identifier,
            "maximum_channel_count": maximum_channel_count,
        },
        timeout_ms=1000,
        attempts=2,
        device=device,
    )
    if not response or _parsed_response("result_code", response) != RESULT_CODE_SUCCESS:
        return None
    capabilities = _parsed_response("transmit_channel_capabilities", response)
    return capabilities if isinstance(capabilities, dict) else None


async def create_tx_flow(
    device_ip: str,
    arc_port: int,
    flow_protocol_id: int,
    flow_slot: int,
    channels: list[int],
    *,
    device=None,
) -> int | None:
    require_creatable_flow_protocol(flow_protocol_id)
    command_specification = {
        "command": "create_tx_flow",
        "flow_protocol_id": flow_protocol_id,
        "flow_slot": flow_slot,
        "channels": list(channels),
    }
    if device is None:
        return await _result_code(device_ip, arc_port, command_specification)
    return await _result_code(device_ip, arc_port, command_specification, device=device)


async def delete_tx_flow(
    device_ip: str,
    arc_port: int,
    flow_protocol_id: int,
    flow_slot: int,
    *,
    device=None,
) -> int | None:
    require_deletable_flow_protocol(flow_protocol_id, flow_slot)
    command_specification = {
        "command": "delete_tx_flow",
        "flow_protocol_id": flow_protocol_id,
        "flow_slot": flow_slot,
    }
    if device is None:
        return await _result_code(device_ip, arc_port, command_specification)
    return await _result_code(device_ip, arc_port, command_specification, device=device)


async def _result_code(device_ip: str, arc_port: int, command_specification: dict, *, device=None) -> int | None:
    response = await _request_with_optional_device(device_ip, arc_port, command_specification, 2000, 2, device)
    if not response:
        return None
    return _parsed_response("result_code", response)
