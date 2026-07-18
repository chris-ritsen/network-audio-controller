from __future__ import annotations

import asyncio

from netaudio.dante.const import FLOW_PROTOCOL_IDS, RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED


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


async def _request(
    device_ip: str,
    arc_port: int,
    command_specification: dict,
    timeout_ms: int,
    attempts: int,
) -> bytes | None:
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


async def detect_flow_protocol(device_ip: str, arc_port: int) -> int | None:
    from netaudio import core

    for flow_protocol_id in FLOW_PROTOCOL_IDS:
        command_specification = {
            "command": "query_tx_flows",
            "flow_protocol_id": flow_protocol_id,
            "starting_flow": 1,
        }
        response = await _request(device_ip, arc_port, command_specification, timeout_ms=500, attempts=1)
        if response and core.parse_response("result_code", response) in (
            RESULT_CODE_SUCCESS,
            RESULT_CODE_SUCCESS_EXTENDED,
        ):
            return flow_protocol_id
    return None


async def query_tx_flows(device_ip: str, arc_port: int, flow_protocol_id: int) -> list[dict] | None:
    from netaudio import core

    device_flows = []
    seen_flow_numbers = set()
    starting_flow = 1

    while True:
        command_specification = {
            "command": "query_tx_flows",
            "flow_protocol_id": flow_protocol_id,
            "starting_flow": starting_flow,
        }
        response = await _request(device_ip, arc_port, command_specification, timeout_ms=1000, attempts=2)
        if not response:
            return None

        result_code = core.parse_response("result_code", response)
        if result_code not in (RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED):
            return None
        page_flows = core.parse_response("tx_flows", response)
        page_flow_numbers = []
        for flow in page_flows:
            flow_number = flow.get("flow_number")
            if (
                isinstance(flow_number, bool)
                or not isinstance(flow_number, int)
                or not 1 <= flow_number <= 32
                or flow_number in seen_flow_numbers
            ):
                return None
            page_flow_numbers.append(flow_number)
            seen_flow_numbers.add(flow_number)
            device_flows.append(flow)

        if result_code == RESULT_CODE_SUCCESS:
            return device_flows
        if not page_flow_numbers:
            return None

        next_starting_flow = max(page_flow_numbers) + 1
        if next_starting_flow <= starting_flow or next_starting_flow > 32:
            return None
        starting_flow = next_starting_flow


async def create_tx_flow(
    device_ip: str,
    arc_port: int,
    flow_protocol_id: int,
    flow_slot: int,
    channels: list[int],
) -> int | None:
    command_specification = {
        "command": "create_tx_flow",
        "flow_protocol_id": flow_protocol_id,
        "flow_slot": flow_slot,
        "channels": list(channels),
    }
    return await _result_code(device_ip, arc_port, command_specification)


async def delete_tx_flow(
    device_ip: str,
    arc_port: int,
    flow_protocol_id: int,
    flow_slot: int,
) -> int | None:
    command_specification = {
        "command": "delete_tx_flow",
        "flow_protocol_id": flow_protocol_id,
        "flow_slot": flow_slot,
    }
    return await _result_code(device_ip, arc_port, command_specification)


async def _result_code(device_ip: str, arc_port: int, command_specification: dict) -> int | None:
    from netaudio import core

    response = await _request(device_ip, arc_port, command_specification, timeout_ms=2000, attempts=2)
    if not response:
        return None
    return core.parse_response("result_code", response)
