from __future__ import annotations

import asyncio
import logging

from netaudio.dante.const import (
    FLOW_CREATE_PROTOCOL_IDS,
    FLOW_DELETE_PROTOCOL_IDS,
    FLOW_QUERY_PROTOCOL_IDS,
    PROTOCOL_ARC_2809,
    RESULT_CODE_SUCCESS,
    RESULT_CODE_SUCCESS_EXTENDED,
)

logger = logging.getLogger("netaudio")


class FlowValidationError(ValueError):
    def __init__(self, message: str, *, status: int = 400):
        super().__init__(message)
        self.status = status


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
    if any(flow.get("flow_number") == flow_slot for flow in device_flows):
        raise FlowValidationError(f"flow slot {flow_slot} is already in use", status=409)


def require_supported_flow_slot(flow_slot: int, max_flow_slots: int) -> None:
    if flow_slot > max_flow_slots:
        raise FlowValidationError(
            f"flow slot {flow_slot} exceeds the device capacity of {max_flow_slots}",
            status=409,
        )


def require_multicast_flow(device_flows, flow_slot: int) -> dict:
    flow = next(
        (entry for entry in device_flows if entry.get("flow_number") == flow_slot),
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
        client = core.CoreClient(device_ip, arc_port=arc_port, timeout_ms=timeout_ms, attempts=attempts)
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
    protocol_ids = (
        (PROTOCOL_ARC_2809,) if getattr(device, "requires_managed_control", False) else FLOW_QUERY_PROTOCOL_IDS
    )
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
    if flow_protocol_id == PROTOCOL_ARC_2809:
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
    status_inventory = await _query_tx_inventory_with_optional_device(
        device_ip,
        arc_port,
        PROTOCOL_ARC_2809,
        device=device,
    )
    if status_inventory is not None:
        return status_inventory
    if mutation_protocol_id == PROTOCOL_ARC_2809:
        return None
    return await _query_tx_inventory_with_optional_device(device_ip, arc_port, mutation_protocol_id, device)


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
        receiver_flows.append(
            {
                "flow_number": flow.get("flow_number"),
                "flow_type": flow_type,
                "local_receiver_channel_count": local_receiver_channel_count,
                "receiver_mapping_descriptor_hexadecimal": flow.get("receiver_mapping_descriptor_hexadecimal"),
                "status_code_at_record_offset_62": flow.get("status_code_at_record_offset_62"),
                "destination_internet_protocol_version_four_address": flow.get(
                    "destination_internet_protocol_version_four_address"
                )
                or "",
                "destination_user_datagram_port": flow.get("destination_user_datagram_port"),
                "sample_rate": flow.get("sample_rate"),
                "encoding": flow.get("encoding"),
                "frames_per_packet": flow.get("frames_per_packet"),
                "latency_nanoseconds": flow.get("latency_nanoseconds"),
            }
        )
    return {
        "maximum_flow_slots": page.get("maximum_flow_slots"),
        "flows": receiver_flows,
    }


async def query_preferred_receiver_flow_inventory(device) -> dict | None:
    application = device.application
    status_page = None
    if application is not None:
        try:
            status_page = await application.query_receiver_flow_status_2809(device)
        except RuntimeError:
            status_page = None
    if status_page is not None:
        return inventory_from_receiver_flow_status_page(status_page)
    if getattr(device, "requires_managed_control", False):
        return None
    from netaudio.cli_support.execution import _get_arc_port

    return await query_receiver_flow_inventory(str(device.ipv4), _get_arc_port(device))


async def query_receiver_flow_inventory(device_ip: str, arc_port: int) -> dict | None:
    response = await _request(
        device_ip,
        arc_port,
        {"command": "query_receiver_flows", "starting_flow": 1},
        timeout_ms=1000,
        attempts=2,
    )
    if not response or _parsed_response("result_code", response) != RESULT_CODE_SUCCESS:
        return None
    flow_page = _parsed_response("receiver_flow_page", response)
    if not isinstance(flow_page, dict):
        return None
    maximum_flow_slots = flow_page.get("maximum_flow_slots")
    if (
        isinstance(maximum_flow_slots, bool)
        or not isinstance(maximum_flow_slots, int)
        or not 1 <= maximum_flow_slots <= 32
    ):
        return None
    receiver_flows = flow_page.get("flows")
    if not isinstance(receiver_flows, list) or len(receiver_flows) > maximum_flow_slots:
        return None
    flow_numbers = set()
    for receiver_flow in receiver_flows:
        if not isinstance(receiver_flow, dict):
            return None
        flow_number = receiver_flow.get("flow_number")
        if (
            isinstance(flow_number, bool)
            or not isinstance(flow_number, int)
            or not 1 <= flow_number <= maximum_flow_slots
            or flow_number in flow_numbers
        ):
            return None
        flow_numbers.add(flow_number)
    return flow_page


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
