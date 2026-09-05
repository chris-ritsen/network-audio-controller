from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante import flows
from tests.protocol_test_fixtures import load_protocol_packet


def _packet(protocol_identifier: int, opcode: int, packet_identifier: int) -> bytes:
    return load_protocol_packet(
        "transmitter_flow_status",
        f"protocol_{protocol_identifier:04x}_opcode_{opcode:04x}_id_{packet_identifier}.bin",
    )


def test_query_builder_is_byte_identical_to_the_controller_zero_tail_form():
    request = _packet(0x2809, 0x2600, 7185)
    built = core.build_command(
        {
            "command": "query_tx_flows",
            "flow_protocol_id": 0x2809,
            "starting_flow": 1,
            "transaction_id": 0x0225,
        }
    )

    assert built == request
    assert built.hex() == "28090022022526000000000000000000000100010001000000000000000000000000"


def test_parser_preserves_zero_unicast_and_multicast_status_records():
    zero_page = core.parse_response(
        "transmitter_flow_status_page",
        _packet(0x2809, 0x2600, 7196),
    )
    causal_pre_action_page = core.parse_response(
        "transmitter_flow_status_page",
        _packet(0x2809, 0x2600, 29605),
    )
    unicast_page = core.parse_response(
        "transmitter_flow_status_page",
        _packet(0x2809, 0x2600, 29630),
    )
    multicast_page = core.parse_response(
        "transmitter_flow_status_page",
        _packet(0x2809, 0x2600, 7675),
    )

    assert zero_page["maximum_flow_slots"] == 2
    assert zero_page["reported_flow_count"] == 0
    assert zero_page["flows"] == []
    assert causal_pre_action_page["reported_flow_count"] == 0

    assert unicast_page["maximum_flow_slots"] == 2
    assert unicast_page["reported_flow_count"] == 1
    assert unicast_page["flows"] == [
        {
            "record_pointer": 32,
            "record_length_bytes": 76,
            "global_flow_id": 1,
            "media_type_code": 3,
            "media_local_flow_id": 1,
            "flow_name_pointer": 22,
            "flow_name": "1",
            "flow_type_code": 0x0011,
            "flow_type": "unicast",
            "format_pointer": 24,
            "format_descriptor_hexadecimal": "0000bb8000000018",
            "sample_rate": 48_000,
            "encoding": 24,
            "channel_slot_segment_header": 0x0507,
            "channel_slot_count": 2,
            "transmitter_channel_ids_by_slot": [1, 2],
            "populated_transmitter_channel_ids": [1, 2],
            "populated_slot_count": 2,
            "endpoint_descriptor_pointer": 112,
            "endpoint_descriptor_hexadecimal": "08023805c0a8016c",
            "destination_user_datagram_port": 14_341,
            "destination_internet_protocol_version_four_address": "192.168.1.108",
            "subscriber_device_name_pointer": 120,
            "subscriber_device_name": "lx-dante",
            "subscriber_flow_name_pointer": 129,
            "subscriber_flow_name": "3",
            "raw_record_hexadecimal": _packet(0x2809, 0x2600, 29630)[32:108].hex(),
        }
    ]
    assert len(unicast_page["flows"][0]["raw_record_hexadecimal"]) == 152

    assert multicast_page["maximum_flow_slots"] == 2
    assert multicast_page["reported_flow_count"] == 1
    assert multicast_page["flows"][0] == {
        "record_pointer": 32,
        "record_length_bytes": 76,
        "global_flow_id": 2,
        "media_type_code": 3,
        "media_local_flow_id": 2,
        "flow_name_pointer": 22,
        "flow_name": "2",
        "flow_type_code": 0x0002,
        "flow_type": "multicast",
        "format_pointer": 24,
        "format_descriptor_hexadecimal": "0000bb8000000018",
        "sample_rate": 48_000,
        "encoding": 24,
        "channel_slot_segment_header": 0x0507,
        "channel_slot_count": 2,
        "transmitter_channel_ids_by_slot": [1, 2],
        "populated_transmitter_channel_ids": [1, 2],
        "populated_slot_count": 2,
        "endpoint_descriptor_pointer": 112,
        "endpoint_descriptor_hexadecimal": "080210e1efffff38",
        "destination_user_datagram_port": 4_321,
        "destination_internet_protocol_version_four_address": "239.255.255.56",
        "subscriber_device_name_pointer": 0,
        "subscriber_device_name": None,
        "subscriber_flow_name_pointer": 0,
        "subscriber_flow_name": None,
        "raw_record_hexadecimal": _packet(0x2809, 0x2600, 7675)[32:108].hex(),
    }


@pytest.mark.asyncio
async def test_product_inventory_uses_the_typed_2809_status_parser(monkeypatch):
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
        return _packet(0x2809, 0x2600, 29630)

    monkeypatch.setattr(flows, "_request", request)

    inventory = await flows.query_tx_flow_inventory("192.0.2.10", 4440, 0x2809)

    assert inventory["max_flow_slots"] == 2
    assert inventory["reported_flow_count"] == 1
    assert inventory["flows"][0]["subscriber_device_name"] == "lx-dante"
    assert command_specifications == [
        {
            "device_ip": "192.0.2.10",
            "arc_port": 4440,
            "command_specification": {
                "command": "query_tx_flows",
                "flow_protocol_id": 0x2809,
                "starting_flow": 1,
            },
            "timeout_ms": 1000,
            "attempts": 2,
        }
    ]


@pytest.mark.asyncio
async def test_detection_accepts_2809_after_earlier_protocol_identifiers_fail(monkeypatch):
    attempted_protocol_identifiers = []

    async def request(device_ip, arc_port, command_specification, timeout_ms, attempts):
        attempted_protocol_identifiers.append(command_specification["flow_protocol_id"])
        if command_specification["flow_protocol_id"] == 0x2809:
            return _packet(0x2809, 0x2600, 29630)
        return None

    monkeypatch.setattr(flows, "_request", request)

    assert await flows.detect_flow_protocol("192.0.2.10", 4440) == 0x2809
    assert attempted_protocol_identifiers == [0x2729, 0x2801, 0x2809]


@pytest.mark.asyncio
async def test_2809_mutations_fail_before_network_access(monkeypatch):
    request = AsyncMock()
    monkeypatch.setattr(flows, "_request", request)

    with pytest.raises(flows.FlowValidationError) as create_error:
        await flows.create_tx_flow("192.0.2.10", 4440, 0x2809, 1, [1])
    with pytest.raises(flows.FlowValidationError) as delete_error:
        await flows.delete_tx_flow("192.0.2.10", 4440, 0x2809, 1)

    assert create_error.value.status == 409
    assert delete_error.value.status == 409
    request.assert_not_awaited()
