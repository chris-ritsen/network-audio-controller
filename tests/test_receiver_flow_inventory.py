import pytest

from netaudio import core
from netaudio.dante import flows
from tests.protocol_test_fixtures import load_protocol_packet


def _packet(opcode: int, packet_identifier: int) -> bytes:
    return load_protocol_packet(
        "receiver_flow_inventory",
        f"protocol_2729_opcode_{opcode:04x}_id_{packet_identifier}.bin",
    )


def test_receiver_flow_query_is_byte_identical_to_shipping_controller():
    built = core.build_command(
        {
            "command": "query_receiver_flows",
            "starting_flow": 1,
            "transaction_id": 0x033A,
        }
    )

    assert built == _packet(0x3200, 8171)
    assert built.hex() == "27290010033a32000000000100010000"


def test_receiver_flow_parser_preserves_controller_visible_flow_state():
    page = core.parse_response("receiver_flow_page", _packet(0x3200, 8172))

    assert page["maximum_flow_slots"] == 16
    assert [flow["flow_number"] for flow in page["flows"]] == [1, 2, 3, 5]
    assert [flow["subscription_status_code"] for flow in page["flows"]] == [9, 9, 9, 10]
    assert [flow["latency_nanoseconds"] for flow in page["flows"]] == [
        1_000_000,
        1_000_000,
        2_000_000,
        1_000_000,
    ]
    assert [flow["destination_internet_protocol_version_four_address"] for flow in page["flows"]] == [
        "192.168.1.108",
        "192.168.1.108",
        "192.168.1.108",
        "239.255.255.56",
    ]
    assert [flow["destination_user_datagram_port"] for flow in page["flows"]] == [
        0x3813,
        0x3803,
        0x3829,
        0x10E1,
    ]
    assert [flow["flow_type"] for flow in page["flows"]] == [
        "unicast",
        "unicast",
        "unicast",
        "multicast",
    ]
    assert all(flow["sample_rate"] == 48_000 for flow in page["flows"])
    assert all(flow["encoding"] == 24 for flow in page["flows"])
    assert all(flow["channel_count"] == 2 for flow in page["flows"])
    assert all(len(flow["raw_record_hexadecimal"]) == 168 for flow in page["flows"])
    assert [flow["receiver_channel_numbers_by_flow_channel"] for flow in page["flows"]] == [
        [[21], [22]],
        [[17], [18]],
        [[15], [16]],
        [[11], [12]],
    ]


def test_receiver_flow_statuses_match_the_correlated_subscription_inventory():
    receiver_records = [
        *core.parse_page("rx", _packet(0x3000, 8125), 1),
        *core.parse_page("rx", _packet(0x3000, 8137), 17),
    ]
    flows = core.parse_response("receiver_flow_page", _packet(0x3200, 8172))["flows"]

    receiver_channels_by_status = {
        status_code: [
            record["number"] for record in receiver_records if record["subscription_status_code"] == status_code
        ]
        for status_code in (9, 10)
    }
    flow_receiver_channels_by_status = {
        status_code: sorted(
            receiver_channel_number
            for flow in flows
            if flow["subscription_status_code"] == status_code
            for receiver_channel_numbers in flow["receiver_channel_numbers_by_flow_channel"]
            for receiver_channel_number in receiver_channel_numbers
        )
        for status_code in (9, 10)
    }

    assert receiver_channels_by_status == {
        9: [15, 16, 17, 18, 21, 22],
        10: [11, 12],
    }
    assert flow_receiver_channels_by_status == receiver_channels_by_status


def test_receiver_flow_parser_preserves_multiple_and_empty_channel_mappings():
    multiple_receiver_page = core.parse_response("receiver_flow_page", _packet(0x3200, 29625))
    multiple_receiver_flow = next(flow for flow in multiple_receiver_page["flows"] if flow["flow_number"] == 3)
    assert multiple_receiver_flow["receiver_channel_numbers_by_flow_channel"] == [
        [1, 21],
        [22],
    ]
    assert multiple_receiver_flow["subscription_status_code"] == 0x0015

    empty_mapping_page = core.parse_response("receiver_flow_page", _packet(0x3200, 52343))
    empty_mapping_flow = next(flow for flow in empty_mapping_page["flows"] if flow["flow_number"] == 5)
    assert empty_mapping_flow["receiver_channel_numbers_by_flow_channel"] == [
        [2],
        [],
        [],
        [],
    ]
    assert empty_mapping_flow["subscription_status_code"] == 0x0009
    assert empty_mapping_flow["latency_nanoseconds"] == 250_000


@pytest.mark.asyncio
async def test_receiver_flow_inventory_uses_the_controller_query(monkeypatch):
    command_specifications = []

    async def request(
        device_ip,
        arc_port,
        command_specification,
        timeout_ms,
        attempts,
    ):
        command_specifications.append(
            {
                "device_ip": device_ip,
                "arc_port": arc_port,
                "command_specification": command_specification,
                "timeout_ms": timeout_ms,
                "attempts": attempts,
            }
        )
        return _packet(0x3200, 8172)

    monkeypatch.setattr(flows, "_request", request)

    inventory = await flows.query_receiver_flow_inventory("192.0.2.10", 4440)

    assert inventory == core.parse_response("receiver_flow_page", _packet(0x3200, 8172))
    assert command_specifications == [
        {
            "device_ip": "192.0.2.10",
            "arc_port": 4440,
            "command_specification": {
                "command": "query_receiver_flows",
                "starting_flow": 1,
            },
            "timeout_ms": 1000,
            "attempts": 2,
        }
    ]
