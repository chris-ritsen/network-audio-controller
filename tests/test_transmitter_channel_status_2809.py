from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest

from netaudio import core
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.application import DanteApplication
from netaudio.dante.commands import channel_status_query_specification


def _rename_specification(name: str, protocol_id: int) -> dict:
    return {
        "channel_number": 1,
        "channel_type": "tx",
        "command": "set_channel_name",
        "name": name,
        "protocol_id": protocol_id,
    }


from tests.protocol_test_fixtures import load_protocol_packet


def _packet(packet_identifier: int) -> bytes:
    return load_protocol_packet(
        "transmitter_channel_status",
        f"protocol_2809_opcode_2400_id_{packet_identifier}.bin",
    )


def test_query_builder_and_command_factory_are_byte_identical_to_shipping_controller():
    expected = _packet(1)
    built = core.build_command(
        {
            "command": "query_transmitter_channel_status_2809",
            "transaction_id": 0x2852,
        }
    )
    command, service = DanteDeviceCommands().command_query_transmitter_channel_status_2809(0x2852)

    assert built == expected
    assert command == expected
    assert service is not None
    assert expected.hex() == "28090022285224000000000000000000000100010001000000000000830283060310"
    assert _packet(7) == expected


def test_parser_exposes_transmitter_names_friendly_names_and_format():
    page = core.parse_response("transmitter_channel_status_page_2809", _packet(2))

    assert page["maximum_transmitter_channels"] == 2
    assert page["reported_record_count"] == 2
    assert page["records"] == [
        {
            "record_pointer": 60,
            "record_type_code": 0x1414,
            "channel_number": 1,
            "media_type": 3,
            "media_local_channel_id": 1,
            "channel_name_pointer": 40,
            "channel_name": "bluetooth:left",
            "format_pointer": 24,
            "format_descriptor_hexadecimal": "0000bb80010100180400001800180004",
            "sample_rate": 48_000,
            "encoding": 24,
            "friendly_channel_name_pointer": 55,
            "friendly_channel_name": "Left",
            "raw_record_hexadecimal": page["records"][0]["raw_record_hexadecimal"],
        },
        {
            "record_pointer": 124,
            "record_type_code": 0x1414,
            "channel_number": 2,
            "media_type": 3,
            "media_local_channel_id": 2,
            "channel_name_pointer": 100,
            "channel_name": "bluetooth:right",
            "format_pointer": 24,
            "format_descriptor_hexadecimal": "0000bb80010100180400001800180004",
            "sample_rate": 48_000,
            "encoding": 24,
            "friendly_channel_name_pointer": 116,
            "friendly_channel_name": "Right",
            "raw_record_hexadecimal": page["records"][1]["raw_record_hexadecimal"],
        },
    ]
    assert all(len(record["raw_record_hexadecimal"]) == 80 for record in page["records"])


def test_parser_handles_all_five_preserved_device_layouts():
    expected_names = {
        2: [("bluetooth:left", "Left"), ("bluetooth:right", "Right")],
        3: [("vrroom:left", "CH1"), ("vrroom:right", "CH2")],
        4: [("macbook-work:left", "Left"), ("macbook-work:right", "Right")],
        5: [("macbook-personal:left", "Left"), ("macbook-personal:right", "Right")],
        6: [("Left", "Left"), ("Right", "Right")],
    }

    for packet_identifier, names in expected_names.items():
        page = core.parse_response("transmitter_channel_status_page_2809", _packet(packet_identifier))
        assert [(record["channel_name"], record["friendly_channel_name"]) for record in page["records"]] == names
        assert [record["channel_number"] for record in page["records"]] == [1, 2]
        assert {record["sample_rate"] for record in page["records"]} == {48_000}
        assert {record["encoding"] for record in page["records"]} == {24}


def test_parser_fails_closed_on_structural_corruption_and_a32_rejection():
    successful_response = _packet(2)
    corruptions = [
        (16, 17, bytes([1])),
        (20, 22, (60).to_bytes(2, "big")),
        (20, 22, (80).to_bytes(2, "big")),
        (62, 64, (0).to_bytes(2, "big")),
        (80, 82, (17).to_bytes(2, "big")),
        (82, 84, (159).to_bytes(2, "big")),
        (90, 92, (17).to_bytes(2, "big")),
    ]
    for start, end, replacement in corruptions:
        malformed = bytearray(successful_response)
        malformed[start:end] = replacement
        with pytest.raises(core.NetaudioCoreError):
            core.parse_response("transmitter_channel_status_page_2809", bytes(malformed))

    a32_response = _packet(8)
    assert a32_response == bytes.fromhex("2809000a285224000030")
    assert core.parse_response("result_code", a32_response) == 0x0030
    with pytest.raises(core.NetaudioCoreError):
        core.parse_response("transmitter_channel_status_page_2809", a32_response)


@pytest.mark.asyncio
async def test_device_operation_returns_page_and_fails_loud_on_a32_frontend_rejection():
    successful_device = SimpleNamespace(execute=AsyncMock(return_value=_packet(2)), services={})
    successful_operation = DanteApplication()

    page = await successful_operation.query_transmitter_channel_status_2809(successful_device)

    assert [record["channel_name"] for record in page["records"]] == ["bluetooth:left", "bluetooth:right"]
    assert successful_device.transmitter_channel_name_protocol_identifier == 0x2809
    successful_device.execute.assert_awaited_once_with(channel_status_query_specification("tx"))

    rejected_device = SimpleNamespace(execute=AsyncMock(return_value=_packet(8)), services={})
    with pytest.raises(RuntimeError, match="result 0x0030"):
        await DanteApplication().query_transmitter_channel_status_2809(rejected_device)


@pytest.mark.asyncio
async def test_transmitter_rename_selects_and_caches_2809_after_successful_status_probe():
    rename_response = bytes.fromhex("2809000c0302201300010000")
    device = SimpleNamespace(
        execute=AsyncMock(side_effect=[_packet(2), rename_response, rename_response]),
        transmitter_channel_name_protocol_identifier=None,
    )
    operation = DanteApplication()

    first_response = await operation.send_set_channel_name(device, "tx", 1, "bluetooth:left")
    second_response = await operation.send_set_channel_name(device, "tx", 1, "bluetooth:left")

    assert first_response == rename_response
    assert second_response == rename_response
    assert device.transmitter_channel_name_protocol_identifier == 0x2809
    rename_specification = _rename_specification("bluetooth:left", 0x2809)
    assert device.execute.await_args_list == [
        call(channel_status_query_specification("tx")),
        call(rename_specification),
        call(rename_specification),
    ]


@pytest.mark.asyncio
async def test_transmitter_rename_selects_2729_after_authentic_a32_frontend_rejection():
    rename_response = bytes.fromhex("2729000c0302201300010000")
    device = SimpleNamespace(
        execute=AsyncMock(side_effect=[_packet(8), rename_response]),
        transmitter_channel_name_protocol_identifier=None,
    )
    operation = DanteApplication()

    response = await operation.send_set_channel_name(device, "tx", 1, "Output-1")

    assert response == rename_response
    assert device.transmitter_channel_name_protocol_identifier == 0x2729
    assert device.execute.await_args_list[-1] == call(_rename_specification("Output-1", 0x2729))
