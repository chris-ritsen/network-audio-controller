from types import SimpleNamespace
from unittest.mock import AsyncMock

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
                "message_id": 0x0329,
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
async def test_product_query_uses_the_proven_controller_request():
    device = SimpleNamespace(execute=AsyncMock(return_value=VIRTUAL_A32_RESPONSE))

    assert await flows.query_transmit_channel_capabilities(
        "192.0.2.10",
        4440,
        starting_channel_identifier=1,
        maximum_channel_count=32,
        device=device,
    ) == {
        "record_count": 1,
        "ranges": [{"first_transmit_channel": 1, "last_transmit_channel": 32, "unknown_value": 0x7FFF}],
    }
    device.execute.assert_awaited_once()
    specification = device.execute.call_args.args[0]
    assert core.build_command({**specification, "message_id": 0x0329}) == CONTROLLER_REQUEST[:-2] + b"\x00\x20"


@pytest.mark.asyncio
async def test_product_query_treats_empty_range_inventory_as_valid():
    device = SimpleNamespace(execute=AsyncMock(return_value=AVIO_SHORT_SUCCESS_RESPONSE))

    assert await flows.query_transmit_channel_capabilities(
        "192.0.2.10",
        4440,
        starting_channel_identifier=1,
        maximum_channel_count=0,
        device=device,
    ) == {"record_count": 0, "ranges": []}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        None,
        b"",
        VIRTUAL_A32_RESPONSE[:9],
        VIRTUAL_A32_RESPONSE[:8] + b"\x00\x02" + VIRTUAL_A32_RESPONSE[10:],
        VIRTUAL_A32_RESPONSE[:6] + b"\xff\xff" + VIRTUAL_A32_RESPONSE[8:],
    ],
)
async def test_product_query_rejects_missing_malformed_and_failed_responses(response):
    device = SimpleNamespace(execute=AsyncMock(return_value=response))

    assert await flows.query_transmit_channel_capabilities("192.0.2.10", 4440, device=device) is None
