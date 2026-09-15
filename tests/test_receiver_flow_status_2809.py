from types import SimpleNamespace
from unittest.mock import AsyncMock, call

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


def test_query_builder_and_command_factory_encode_open_ended_pagination_range():
    built = core.build_command(
        {
            "command": "query_modern_arc_receiver_flow_status",
            "transaction_id": 0x2856,
        }
    )
    command, service = DanteDeviceCommands().command_query_modern_arc_receiver_flow_status(0x2856)

    assert built == command
    assert service is not None
    assert built.hex() == "28090022285636000000000000000000000100010000000000000000830283060310"


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
        {"command": "query_modern_arc_receiver_flow_status", "protocol_id": 0x2809, "starting_flow": 1}
    )

    rejected_device = SimpleNamespace(execute=AsyncMock(return_value=_packet(0x3600, 14)), services=services)
    with pytest.raises(RuntimeError, match="result 0x0030"):
        await DanteApplication().query_modern_arc_receiver_flow_status(rejected_device)


def test_issue_59_partial_receiver_flow_records_and_result_survive_ffi():
    from tests.issue_59_fixtures import packet

    response = packet("receiver_flow_partial.bin")
    page = core.parse_response("modern_arc_receiver_flow_status_page", response)
    assert len(response) == 1400
    assert page["result_code"] == 0x8112
    assert page["page_disposition"] == "more_pages"
    assert page["maximum_flow_slots"] == 16
    assert page["reported_flow_count"] == len(page["flows"]) == 15
    assert page["raw_body_hexadecimal"] == response[10:].hex()
    complete = bytearray(response)
    complete[8:10] = (1).to_bytes(2, "big")
    completed = core.parse_response("modern_arc_receiver_flow_status_page", bytes(complete))
    assert completed["page_disposition"] == "complete"
    assert completed["result_code"] == 1
    assert completed["flows"] == page["flows"]
    for code in (0, 2, 0x30, 65535):
        complete[8:10] = code.to_bytes(2, "big")
        with pytest.raises(core.NetaudioCoreError):
            core.parse_response("modern_arc_receiver_flow_status_page", bytes(complete))


@pytest.mark.asyncio
async def test_partial_flow_query_requests_maximum_returned_identifier_plus_one():
    from tests.issue_59_fixtures import packet

    terminal = bytearray(_packet(0x3600, 4))
    terminal[16] = 16
    terminal[34:36] = (16).to_bytes(2, "big")
    device = SimpleNamespace(
        execute=AsyncMock(side_effect=[packet("receiver_flow_partial.bin"), bytes(terminal)]),
        services={"arc": {"type": "_netaudio-arc._udp.local.", "properties": {"arcp_vers": "2.8.9"}}},
    )
    page = await DanteApplication().query_modern_arc_receiver_flow_status(device)
    assert page["page_disposition"] == "complete"
    assert len(page["flows"]) == 16
    assert device.execute.await_args_list == [
        call({"command": "query_modern_arc_receiver_flow_status", "protocol_id": 0x2809, "starting_flow": 1}),
        call({"command": "query_modern_arc_receiver_flow_status", "protocol_id": 0x2809, "starting_flow": 16}),
    ]


@pytest.mark.asyncio
async def test_partial_flow_readback_is_unavailable_for_effective_state_decisions(monkeypatch):
    from netaudio.dante import flows
    from netaudio.dante.device import DanteDevice
    from tests.issue_59_fixtures import packet

    device = DanteDevice(server_name="receiver.local.")
    page = core.parse_response("modern_arc_receiver_flow_status_page", packet("receiver_flow_partial.bin"))
    device._app = SimpleNamespace(query_modern_arc_receiver_flow_status=AsyncMock(return_value=page))
    fallback = AsyncMock()
    monkeypatch.setattr(flows, "query_receiver_flow_inventory", fallback)
    assert await flows.query_preferred_receiver_flow_inventory(device) is None
    assert device.receiver_flow_completeness == "partial"
    assert device.receiver_flow_status_page is None
    diagnostic = await flows.query_preferred_receiver_flow_inventory(device, require_complete=False)
    assert len(diagnostic["flows"]) == 15
    assert diagnostic["page_disposition"] == "more_pages"
    assert diagnostic["result_code"] == 0x8112
    assert diagnostic["status_page"] == page
    fallback.assert_not_awaited()
