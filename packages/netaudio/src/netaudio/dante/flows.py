import asyncio

from netaudio.dante.const import FLOW_PROTOCOL_IDS, RESULT_CODE_SUCCESS


async def _request(device_ip: str, arc_port: int, spec: dict, timeout_ms: int, attempts: int) -> bytes | None:
    from netaudio import core

    def _send():
        client = core.CoreClient(device_ip, arc_port=arc_port, timeout_ms=timeout_ms, attempts=attempts)
        try:
            packet = core.build_command(spec)
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
        spec = {"command": "query_tx_flows", "flow_protocol_id": flow_protocol_id}
        response = await _request(device_ip, arc_port, spec, timeout_ms=500, attempts=1)
        if response and core.parse_response("result_code", response) == RESULT_CODE_SUCCESS:
            return flow_protocol_id
    return None


async def query_tx_flows(device_ip: str, arc_port: int, flow_protocol_id: int) -> list[dict] | None:
    from netaudio import core

    spec = {"command": "query_tx_flows", "flow_protocol_id": flow_protocol_id}
    response = await _request(device_ip, arc_port, spec, timeout_ms=1000, attempts=2)
    if not response:
        return None
    return core.parse_response("tx_flows", response)


async def create_tx_flow(
    device_ip: str,
    arc_port: int,
    flow_protocol_id: int,
    flow_slot: int,
    channels: list[int],
) -> int | None:
    spec = {
        "command": "create_tx_flow",
        "flow_protocol_id": flow_protocol_id,
        "flow_slot": flow_slot,
        "channels": list(channels),
    }
    return await _result_code(device_ip, arc_port, spec)


async def delete_tx_flow(
    device_ip: str,
    arc_port: int,
    flow_protocol_id: int,
    flow_slot: int,
) -> int | None:
    spec = {
        "command": "delete_tx_flow",
        "flow_protocol_id": flow_protocol_id,
        "flow_slot": flow_slot,
    }
    return await _result_code(device_ip, arc_port, spec)


async def _result_code(device_ip: str, arc_port: int, spec: dict) -> int | None:
    from netaudio import core

    response = await _request(device_ip, arc_port, spec, timeout_ms=2000, attempts=2)
    if not response:
        return None
    return core.parse_response("result_code", response)
