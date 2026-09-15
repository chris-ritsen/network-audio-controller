import pytest

from netaudio import core
from netaudio.dante import flows


CONTROLLER_REQUEST = bytes.fromhex("27290010032920320000000100010000")
PHYSICAL_A32_RESPONSE = bytes.fromhex("272900120329203200010001000100807fff")
VIRTUAL_A32_RESPONSE = bytes.fromhex("272900120329203200010001000100207fff")
AVIO_SHORT_SUCCESS_RESPONSE = bytes.fromhex("2729000c0000203200010000")


def test_transmit_channel_capability_command_matches_shipping_controller():
    assert (
        core.build_command(
            {
                "command": "query_transmit_channel_capabilities",
                "transaction_id": 0x0329,
            }
        )
        == CONTROLLER_REQUEST
    )


def test_transmit_channel_capability_parser_preserves_reported_ranges():
    assert core.parse_response("transmit_channel_capabilities", PHYSICAL_A32_RESPONSE) == {
        "record_count": 1,
        "ranges": [{"first_transmit_channel": 1, "last_transmit_channel": 128, "unknown_value": 0x7FFF}],
    }
    assert core.parse_response("transmit_channel_capabilities", VIRTUAL_A32_RESPONSE) == {
        "record_count": 1,
        "ranges": [{"first_transmit_channel": 1, "last_transmit_channel": 32, "unknown_value": 0x7FFF}],
    }


@pytest.mark.asyncio
async def test_product_query_uses_the_proven_controller_request(monkeypatch):
    command_specifications = []

    async def request(device_ip, arc_port, command_specification, timeout_ms, attempts):
        command_specifications.append(
            {
                "device_ip": device_ip,
                "arc_port": arc_port,
                "command_specification": command_specification,
                "timeout_ms": timeout_ms,
                "attempts": attempts,
            }
        )
        return VIRTUAL_A32_RESPONSE

    monkeypatch.setattr(flows, "_request", request)

    assert await flows.query_transmit_channel_capabilities(
        "192.0.2.10",
        4440,
        starting_channel_identifier=1,
        maximum_channel_count=32,
    ) == {
        "record_count": 1,
        "ranges": [{"first_transmit_channel": 1, "last_transmit_channel": 32, "unknown_value": 0x7FFF}],
    }
    assert command_specifications == [
        {
            "device_ip": "192.0.2.10",
            "arc_port": 4440,
            "command_specification": {
                "command": "query_transmit_channel_capabilities",
                "starting_channel_identifier": 1,
                "maximum_channel_count": 32,
            },
            "timeout_ms": 1000,
            "attempts": 2,
        }
    ]


@pytest.mark.asyncio
async def test_product_query_treats_empty_range_inventory_as_valid(monkeypatch):
    async def request(device_ip, arc_port, command_specification, timeout_ms, attempts):
        return AVIO_SHORT_SUCCESS_RESPONSE

    monkeypatch.setattr(flows, "_request", request)

    assert await flows.query_transmit_channel_capabilities(
        "192.0.2.10",
        4440,
        starting_channel_identifier=1,
        maximum_channel_count=0,
    ) == {"record_count": 0, "ranges": []}
