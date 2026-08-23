from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante import flows

BROOKLYN_CREATE_REQUEST = "2729003a220122010000010100100000000200020000000000000000000000010002002600000a00000000000000000000000000000000010000"
BROOKLYN_DELETE_REQUEST = "27290010220222020000000100000002"
CONTROLLER_DELETE_REQUEST = "28090022160226020000000000000000000100030000000200000000000000000000"
EMPTY_BEFORE = "28090016260026000001000000000000020000000000"
AFTER_BROOKLYN_CREATE = "2809007426002600000100000000000002010020000032000000bb8000000018142600020000000300020000000000020000000000160018000f424000000000000000000000000008120000000000000001000000000000040a0101006c0000040600010002000002000010080210e1efffd392"
AFTER_BROOKLYN_DELETE = "28090016260026000001000000000000020000200000"
AFTER_CONTROLLER_CREATE = "2809007426002600000100000000000002010020120032000000bb8000000018142600020000000300020000000000020000000000160018000f424000000000000000000000000008120000000000000001000000000000040a0101006c00000406000100020000020000100802000000000000"
AFTER_CONTROLLER_DELETE = "28090016260026000001000000000000020000340037"


def test_brooklyn_create_builder_matches_live_aes3_request():
    packet = core.build_command(
        {
            "command": "create_tx_flow",
            "flow_protocol_id": 0x2729,
            "flow_slot": 2,
            "channels": [2],
            "transaction_id": 0x2201,
        }
    )
    assert packet.hex() == BROOKLYN_CREATE_REQUEST


def test_brooklyn_delete_builder_matches_live_aes3_request():
    packet = core.build_command(
        {
            "command": "delete_tx_flow",
            "flow_protocol_id": 0x2729,
            "flow_slot": 2,
            "transaction_id": 0x2202,
        }
    )
    assert packet.hex() == BROOKLYN_DELETE_REQUEST


def test_controller_delete_builder_matches_live_aes3_request():
    packet = core.build_command(
        {
            "command": "delete_tx_flow",
            "flow_protocol_id": 0x2809,
            "flow_slot": 2,
            "transaction_id": 0x1602,
        }
    )
    assert packet.hex() == CONTROLLER_DELETE_REQUEST


def test_live_aes3_2600_pages_parse_create_and_clear():
    before = core.parse_response(
        "transmitter_flow_status_page",
        bytes.fromhex(EMPTY_BEFORE),
    )
    assert before["reported_flow_count"] == 0
    assert before["flows"] == []

    after_create = core.parse_response(
        "transmitter_flow_status_page",
        bytes.fromhex(AFTER_BROOKLYN_CREATE),
    )
    assert after_create["reported_flow_count"] == 1
    flow = after_create["flows"][0]
    assert flow["flow_number"] == 2
    assert flow["flow_type"] == "multicast"
    assert flow["destination_internet_protocol_version_four_address"] == "239.255.211.146"
    assert flow["destination_user_datagram_port"] == 4321

    after_delete = core.parse_response(
        "transmitter_flow_status_page",
        bytes.fromhex(AFTER_BROOKLYN_DELETE),
    )
    assert after_delete["reported_flow_count"] == 0
    assert after_delete["flows"] == []

    after_controller_create = core.parse_response(
        "transmitter_flow_status_page",
        bytes.fromhex(AFTER_CONTROLLER_CREATE),
    )
    assert after_controller_create["flows"][0]["flow_number"] == 2
    assert after_controller_create["flows"][0]["flow_type"] == "multicast"

    after_controller_delete = core.parse_response(
        "transmitter_flow_status_page",
        bytes.fromhex(AFTER_CONTROLLER_DELETE),
    )
    assert after_controller_delete["reported_flow_count"] == 0


@pytest.mark.asyncio
async def test_2809_create_remains_fail_closed_while_slot_two_delete_is_enabled(monkeypatch):
    request = AsyncMock(return_value=bytes.fromhex("2809000a160226020001"))
    monkeypatch.setattr(flows, "_request", request)

    with pytest.raises(flows.FlowValidationError) as create_error:
        await flows.create_tx_flow("192.0.2.10", 4440, 0x2809, 2, [2])
    result_code = await flows.delete_tx_flow("192.0.2.10", 4440, 0x2809, 2)

    assert create_error.value.status == 409
    assert result_code == 1
    request.assert_awaited_once_with(
        "192.0.2.10",
        4440,
        {
            "command": "delete_tx_flow",
            "flow_protocol_id": 0x2809,
            "flow_slot": 2,
        },
        timeout_ms=2000,
        attempts=2,
    )
