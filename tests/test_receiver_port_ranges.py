import ipaddress
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante import flows
from tests.protocol_test_fixtures import load_protocol_packet


def _packet(opcode: int, packet_identifier: int) -> bytes:
    return load_protocol_packet(
        "receiver_port_ranges",
        f"protocol_2729_opcode_{opcode:04x}_id_{packet_identifier}.bin",
    )


def _fallback_member(name: str) -> bytes:
    return load_protocol_packet("receiver_port_ranges_fallback", name)






def test_authentic_fallback_setup_uses_the_same_endpoint_descriptor_layout():
    request = _fallback_member("protocol_1102_opcode_0100_id_15.bin")
    transport_descriptor_pointer = int.from_bytes(request[24:26], "big")
    transport_descriptor_count = int.from_bytes(request[26:28], "big")
    endpoint_descriptor = request[transport_descriptor_pointer : transport_descriptor_pointer + 8]

    assert transport_descriptor_count == 1
    assert endpoint_descriptor == bytes.fromhex("080238010afe4e0b")
    assert int.from_bytes(endpoint_descriptor[2:4], "big") == 0x3801
    assert str(ipaddress.ip_address(endpoint_descriptor[4:8])) == "10.254.78.11"


@pytest.mark.asyncio
async def test_receiver_port_range_product_query_uses_the_controller_request():
    device = SimpleNamespace(execute=AsyncMock(return_value=_packet(0x3300, 8176)))

    assert await flows.query_receiver_port_ranges("192.0.2.10", 4440, device=device) == {
        "first_port_range_start": 0x3800,
        "first_port_range_end": 0x397F,
        "second_port_range_start": 0x3980,
        "second_port_range_end": 0x39FF,
        "second_port_range_available": True,
    }
    device.execute.assert_awaited_once()
    specification = device.execute.call_args.args[0]
    assert core.build_command({**specification, "message_id": 0x033C}) == _packet(0x3300, 8175)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "empty", "truncated", "rejected", "wrong_opcode"])
async def test_product_query_rejects_missing_malformed_and_failed_responses(failure):
    packet = _packet(0x3300, 8176)
    response = {
        "missing": None,
        "empty": b"",
        "truncated": packet[:9],
        "rejected": packet[:8] + b"\x00\x02" + packet[10:],
        "wrong_opcode": packet[:6] + b"\xff\xff" + packet[8:],
    }[failure]
    device = SimpleNamespace(execute=AsyncMock(return_value=response))

    assert await flows.query_receiver_port_ranges("192.0.2.10", 4440, device=device) is None
