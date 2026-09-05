from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.application import DanteApplication
from tests.protocol_test_fixtures import load_protocol_packet


def _packet(opcode: int, packet_identifier: int) -> bytes:
    return load_protocol_packet(
        "receiver_flow_status",
        f"protocol_2809_opcode_{opcode:04x}_id_{packet_identifier}.bin",
    )


def test_query_builder_and_command_factory_are_byte_identical_to_shipping_controller():
    expected = _packet(0x3600, 11)
    built = core.build_command(
        {
            "command": "query_modern_arc_receiver_flow_status",
            "transaction_id": 0x2856,
        }
    )
    command, service = DanteDeviceCommands().command_query_modern_arc_receiver_flow_status(0x2856)

    assert built == expected
    assert command == expected
    assert service is not None
    assert expected == _packet(0x3600, 13)
    assert expected.hex() == "28090022285636000000000000000000000100010001000000000000830283060310"


def test_parser_exposes_flow_format_latency_endpoint_and_receiver_mapping():
    page = core.parse_response("modern_arc_receiver_flow_status_page", _packet(0x3600, 4))

    assert page["maximum_flow_slots"] == 2
    assert page["reported_flow_count"] == 1
    assert page["flows"] == [
        {
            "record_pointer": 32,
            "record_length_bytes": 84,
            "record_type_code": 0x1422,
            "global_flow_id": 1,
            "media_type_code": 3,
            "media_local_flow_id": 1,
            "flow_type_code": 1,
            "flow_name_pointer": 22,
            "flow_name": "1",
            "format_pointer": 24,
            "format_descriptor_hexadecimal": "0000bb8000000018",
            "sample_rate": 48_000,
            "encoding": 24,
            "latency_nanoseconds": 1_000_000,
            "local_receiver_channel_count": 1,
            "receiver_mapping_descriptor_pointer": 108,
            "receiver_mapping_descriptor_hexadecimal": "0001000200000100",
            "status_flags": 0x0400,
            "status_code": 0x0101,
            "endpoint_descriptor_hexadecimal": "08023801c0a8013d",
            "destination_user_datagram_port": 0x3801,
            "destination_internet_protocol_version_four_address": "192.168.1.61",
            "raw_record_hexadecimal": _packet(0x3600, 4)[32:116].hex(),
        }
    ]
    assert len(page["flows"][0]["raw_record_hexadecimal"]) == 168

    two_receiver_page = core.parse_response("modern_arc_receiver_flow_status_page", _packet(0x3600, 10))
    assert two_receiver_page["flows"][0]["local_receiver_channel_count"] == 2
    assert two_receiver_page["flows"][0]["receiver_mapping_descriptor_hexadecimal"] == ("0001000200000101")

    empty_page = core.parse_response("modern_arc_receiver_flow_status_page", _packet(0x3600, 12))
    assert empty_page["maximum_flow_slots"] == 2
    assert empty_page["reported_flow_count"] == 0
    assert empty_page["flows"] == []


def test_parser_fails_closed_on_structural_corruption_and_a32_rejection():
    successful_response = _packet(0x3600, 4)
    corruptions = [
        (16, 17, bytes([0])),
        (17, 18, bytes([3])),
        (18, 20, (18).to_bytes(2, "big")),
        (34, 36, (0).to_bytes(2, "big")),
        (40, 42, (0).to_bytes(2, "big")),
        (52, 54, (0).to_bytes(2, "big")),
        (54, 56, (0xFFFF).to_bytes(2, "big")),
        (84, 86, (0).to_bytes(2, "big")),
        (86, 88, (0xFFFF).to_bytes(2, "big")),
    ]
    for start, end, replacement in corruptions:
        malformed = bytearray(successful_response)
        malformed[start:end] = replacement
        with pytest.raises(core.NetaudioCoreError):
            core.parse_response("modern_arc_receiver_flow_status_page", bytes(malformed))

    a32_response = _packet(0x3600, 14)
    assert a32_response == bytes.fromhex("2809000a285636000030")
    assert core.parse_response("result_code", a32_response) == 0x0030
    with pytest.raises(core.NetaudioCoreError):
        core.parse_response("modern_arc_receiver_flow_status_page", a32_response)


@pytest.mark.asyncio
async def test_device_operation_returns_page_and_fails_loud_on_a32_frontend_rejection():
    services = {"arc": {"type": "_netaudio-arc._udp.local.", "properties": {"arcp_vers": "2.8.9"}}}
    successful_device = SimpleNamespace(execute=AsyncMock(return_value=_packet(0x3600, 4)), services=services)
    successful_operation = DanteApplication()

    page = await successful_operation.query_modern_arc_receiver_flow_status(successful_device)

    assert page["flows"][0]["destination_internet_protocol_version_four_address"] == "192.168.1.61"
    successful_device.execute.assert_awaited_once_with(
        {"command": "query_modern_arc_receiver_flow_status", "protocol_id": 0x2809}
    )

    rejected_device = SimpleNamespace(execute=AsyncMock(return_value=_packet(0x3600, 14)), services=services)
    with pytest.raises(RuntimeError, match="result 0x0030"):
        await DanteApplication().query_modern_arc_receiver_flow_status(rejected_device)
