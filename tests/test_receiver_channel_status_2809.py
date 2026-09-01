from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest

from netaudio import core
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_operations import DanteDeviceOperations
from tests.protocol_test_fixtures import load_protocol_packet


def _packet(protocol_identifier: int, opcode: int, packet_identifier: int) -> bytes:
    return load_protocol_packet(
        "receiver_channel_status",
        f"protocol_{protocol_identifier:04x}_opcode_{opcode:04x}_id_{packet_identifier}.bin",
    )


def _frontend_boundary_packet(opcode: int, packet_identifier: int) -> bytes:
    return load_protocol_packet(
        "receiver_channel_frontend",
        f"protocol_2809_opcode_{opcode:04x}_id_{packet_identifier}.bin",
    )


def test_query_and_rename_builders_are_byte_identical_to_controller_requests():
    query = core.build_command(
        {
            "command": "query_receiver_channel_status_2809",
            "transaction_id": 0x284A,
        }
    )
    first_rename = core.build_command(
        {
            "command": "set_channel_name",
            "channel_type": "rx",
            "channel_number": 1,
            "name": "01",
            "protocol_id": 0x2809,
            "transaction_id": 0x2849,
        }
    )
    second_rename = core.build_command(
        {
            "command": "set_channel_name",
            "channel_type": "rx",
            "channel_number": 1,
            "name": "mic-mix",
            "protocol_id": 0x2809,
            "transaction_id": 0x284C,
        }
    )

    assert query == _packet(0x2809, 0x3400, 28728)
    assert first_rename == _packet(0x2809, 0x3401, 28726)
    assert second_rename == _packet(0x2809, 0x3401, 28735)


def test_command_factory_exposes_the_verified_2809_frontend():
    commands = DanteDeviceCommands()
    query, query_service = commands.command_query_receiver_channel_status_2809(0x284A)
    rename, rename_service = commands.command_set_channel_name(
        "rx",
        1,
        "mic-mix",
        protocol_id=0x2809,
        transaction_id=0x284C,
    )

    assert query == _packet(0x2809, 0x3400, 28728)
    assert rename == _packet(0x2809, 0x3401, 28735)
    assert query_service == rename_service


def test_parser_exposes_causal_local_name_readback_and_separate_status_fields():
    first_page = core.parse_response(
        "receiver_channel_status_page_2809",
        _packet(0x2809, 0x3400, 28729),
    )
    second_page = core.parse_response(
        "receiver_channel_status_page_2809",
        _packet(0x2809, 0x3400, 28738),
    )

    assert first_page["maximum_receiver_channels"] == 1
    assert first_page["reported_record_count"] == 1
    assert first_page["records"][0] == {
        "record_pointer": 68,
        "record_type_code": 0x141C,
        "channel_number": 1,
        "media_type": 3,
        "media_local_channel_id": 1,
        "local_channel_name_pointer": 60,
        "local_channel_name": "01",
        "format_pointer": 44,
        "format_descriptor_hexadecimal": "0000bb80010100180400001800180004",
        "sample_rate": 48_000,
        "encoding": 24,
        "friendly_channel_name_pointer": 63,
        "friendly_channel_name": "Left",
        "source_channel_name_pointer": 20,
        "source_channel_name": "mic-mix-high",
        "source_device_name_pointer": 33,
        "source_device_name": "lx-dante",
        "subscription_status_code": 0x0010,
        "receiver_status_code": 0x0000,
        "status_flags": 0x0202,
        "raw_record_hexadecimal": first_page["records"][0]["raw_record_hexadecimal"],
    }
    assert len(first_page["records"][0]["raw_record_hexadecimal"]) == 112

    first_record = first_page["records"][0]
    second_record = second_page["records"][0]
    assert second_record["local_channel_name"] == "mic-mix"
    assert second_record["friendly_channel_name"] == "Left"
    for field in (
        "record_type_code",
        "channel_number",
        "format_descriptor_hexadecimal",
        "sample_rate",
        "encoding",
        "friendly_channel_name",
        "source_channel_name",
        "source_device_name",
        "subscription_status_code",
        "receiver_status_code",
        "status_flags",
    ):
        assert second_record[field] == first_record[field]


def test_parser_handles_subscribed_and_unsubscribed_two_channel_pages():
    unsubscribed = core.parse_response(
        "receiver_channel_status_page_2809",
        _packet(0x2809, 0x3400, 7013),
    )
    subscribed = core.parse_response(
        "receiver_channel_status_page_2809",
        _packet(0x2809, 0x3400, 7019),
    )

    assert unsubscribed["maximum_receiver_channels"] == 2
    assert unsubscribed["reported_record_count"] == 2
    assert [record["channel_number"] for record in unsubscribed["records"]] == [1, 2]
    assert [record["source_channel_name"] for record in unsubscribed["records"]] == [None, None]
    assert [record["subscription_status_code"] for record in unsubscribed["records"]] == [0, 0]

    assert subscribed["maximum_receiver_channels"] == 2
    assert subscribed["reported_record_count"] == 2
    assert [record["local_channel_name"] for record in subscribed["records"]] == [
        "mic-mix-1",
        "mic-mix-2",
    ]
    assert [record["subscription_status_code"] for record in subscribed["records"]] == [9, 9]
    assert [record["receiver_status_code"] for record in subscribed["records"]] == [0x0101, 0x0101]


def test_2809_transmit_rename_matches_the_causal_avio_request():
    assert core.build_command(
        {
            "command": "set_channel_name",
            "channel_type": "tx",
            "channel_number": 2,
            "name": "tv-probe2",
            "protocol_id": 0x2809,
            "transaction_id": 0x0411,
        }
    ) == bytes.fromhex("28090022041120130000020100000002001800000000000074762d70726f62653200")


@pytest.mark.asyncio
async def test_device_operation_returns_typed_receiver_status_page():
    response = _packet(0x2809, 0x3400, 28738)
    device = SimpleNamespace(
        commands=DanteDeviceCommands(),
        dante_command=AsyncMock(return_value=response),
    )
    operation = DanteDeviceOperations(device)

    page = await operation.query_receiver_channel_status_2809()

    assert page["records"][0]["local_channel_name"] == "mic-mix"
    command_arguments = device.commands.command_query_receiver_channel_status_2809()
    device.dante_command.assert_awaited_once_with(
        *command_arguments,
        logical_command_name="query_receiver_channel_status_2809",
    )


@pytest.mark.asyncio
async def test_receiver_rename_selects_and_caches_2809_after_successful_status_probe():
    status_response = _packet(0x2809, 0x3400, 28729)
    rename_response = _packet(0x2809, 0x3401, 28727)
    device = SimpleNamespace(
        commands=DanteDeviceCommands(),
        dante_command=AsyncMock(side_effect=[status_response, rename_response, rename_response]),
        receiver_channel_name_protocol_identifier=None,
    )
    operation = DanteDeviceOperations(device)

    first_response = await operation.set_channel_name("rx", 1, "mic-mix")
    second_response = await operation.set_channel_name("rx", 1, "mic-mix")

    assert first_response == rename_response
    assert second_response == rename_response
    assert device.receiver_channel_name_protocol_identifier == 0x2809
    query_arguments = device.commands.command_query_receiver_channel_status_2809()
    rename_arguments = device.commands.command_set_channel_name("rx", 1, "mic-mix", protocol_id=0x2809)
    assert device.dante_command.await_args_list == [
        call(*query_arguments, logical_command_name="query_receiver_channel_status_2809"),
        call(*rename_arguments, logical_command_name="set_channel_name"),
        call(*rename_arguments, logical_command_name="set_channel_name"),
    ]


@pytest.mark.asyncio
async def test_receiver_rename_selects_2729_after_authentic_a32_frontend_rejection():
    status_response = _frontend_boundary_packet(0x3400, 4)
    rename_response = bytes.fromhex("2729000a000030010001")
    device = SimpleNamespace(
        commands=DanteDeviceCommands(),
        dante_command=AsyncMock(side_effect=[status_response, rename_response]),
        receiver_channel_name_protocol_identifier=None,
    )
    operation = DanteDeviceOperations(device)

    response = await operation.set_channel_name("rx", 1, "Input-1")

    assert response == rename_response
    assert device.receiver_channel_name_protocol_identifier == 0x2729
    rename_arguments = device.commands.command_set_channel_name("rx", 1, "Input-1", protocol_id=0x2729)
    assert device.dante_command.await_args_list[-1] == call(
        *rename_arguments,
        logical_command_name="set_channel_name",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("probe_response", "message"),
    [
        (None, "did not receive a response"),
        (b"invalid", "invalid response"),
        (bytes.fromhex("2809000a000034000600"), "result 0x0600"),
    ],
)
async def test_receiver_rename_does_not_guess_after_an_indeterminate_frontend_probe(probe_response, message):
    device = SimpleNamespace(
        commands=DanteDeviceCommands(),
        dante_command=AsyncMock(return_value=probe_response),
        receiver_channel_name_protocol_identifier=None,
    )
    operation = DanteDeviceOperations(device)

    with pytest.raises(RuntimeError, match=message):
        await operation.set_channel_name("rx", 1, "Input-1")

    assert device.dante_command.await_count == 1
    assert device.receiver_channel_name_protocol_identifier is None
