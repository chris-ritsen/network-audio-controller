from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest

from netaudio import core
from netaudio.dante.application import DanteApplication
from netaudio.dante.const import SERVICE_ARC


def _rename_specification(channel_type: str, name: str, protocol_id: int) -> dict:
    return {
        "channel_number": 1,
        "channel_type": channel_type,
        "command": "set_channel_name",
        "name": name,
        "protocol_id": protocol_id,
    }


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


def _arc_services() -> dict:
    return {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.8.9"}}}


@pytest.mark.asyncio
@pytest.mark.parametrize("channel_type", ["", "receive", None])
@pytest.mark.parametrize("cached", [None, 0x2809])
async def test_channel_name_probe_rejects_invalid_direction_before_io(channel_type, cached):
    device = SimpleNamespace(
        services=_arc_services(),
        execute=AsyncMock(return_value=_frontend_boundary_packet(0x3400, 4)),
        transmitter_channel_name_protocol_identifier=cached,
    )

    with pytest.raises(core.NetaudioCoreError):
        await DanteApplication().resolve_channel_name_protocol_identifier(device, channel_type)

    device.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("channel_type", ["rx", "tx"])
@pytest.mark.parametrize("version", [None, "2.8.16", "invalid"])
@pytest.mark.parametrize("cached", [None, 0x2809])
async def test_channel_rename_does_not_guess_revision_even_with_a_cached_frontend(channel_type, version, cached):
    device = SimpleNamespace(
        services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": version}}},
        execute=AsyncMock(return_value=_frontend_boundary_packet(0x3400, 4)),
        receiver_channel_name_protocol_identifier=cached,
        transmitter_channel_name_protocol_identifier=cached,
    )

    with pytest.raises(RuntimeError, match="ARC"):
        await DanteApplication().send_set_channel_name(device, channel_type, 1, "Input-1")

    device.execute.assert_not_awaited()


def test_query_and_rename_builders_are_byte_identical_to_controller_requests():
    query = core.build_command(
        {
            "command": "query_modern_arc_receiver_channel_status",
            "protocol_id": 0x2809,
            "message_id": 0x284A,
        }
    )
    first_rename = core.build_command(
        {
            "command": "set_channel_name",
            "channel_type": "rx",
            "channel_number": 1,
            "name": "01",
            "protocol_id": 0x2809,
            "message_id": 0x2849,
        }
    )
    second_rename = core.build_command(
        {
            "command": "set_channel_name",
            "channel_type": "rx",
            "channel_number": 1,
            "name": "mic-mix",
            "protocol_id": 0x2809,
            "message_id": 0x284C,
        }
    )

    assert query == _packet(0x2809, 0x3400, 28728)
    assert first_rename == _packet(0x2809, 0x3401, 28726)
    assert second_rename == _packet(0x2809, 0x3401, 28735)


def test_parser_exposes_causal_local_name_readback_and_separate_status_fields():
    first_page = core.parse_response(
        "modern_arc_receiver_channel_status_page",
        _packet(0x2809, 0x3400, 28729),
    )
    second_page = core.parse_response(
        "modern_arc_receiver_channel_status_page",
        _packet(0x2809, 0x3400, 28738),
    )

    unchanged_fields = {
        "channel_number": 1,
        "media_type": "audio",
        "media_local_channel_id": 1,
        "sample_rate": 48_000,
        "encoding": 24,
        "friendly_channel_name": "Left",
        "source_channel_name": "mic-mix-high",
        "source_device_name": "lx-dante",
        "subscription_status_code": 0x0010,
        "is_self_connection": False,
        "receiver_status_code": 0x0000,
        "can_subscribe_self": False,
        "can_rename": True,
    }

    for page, name in [(first_page, "01"), (second_page, "mic-mix")]:
        assert page["page_capacity"] == page["reported_record_count"] == 1
        [record] = page["records"]
        assert record["local_channel_name"] == name
        assert {field: record[field] for field in unchanged_fields} == unchanged_fields


def test_parser_handles_subscribed_and_unsubscribed_two_channel_pages():
    unsubscribed = core.parse_response(
        "modern_arc_receiver_channel_status_page",
        _packet(0x2809, 0x3400, 7013),
    )
    subscribed = core.parse_response(
        "modern_arc_receiver_channel_status_page",
        _packet(0x2809, 0x3400, 7019),
    )

    assert unsubscribed["page_capacity"] == 2
    assert unsubscribed["reported_record_count"] == 2
    assert [record["channel_number"] for record in unsubscribed["records"]] == [1, 2]
    assert [record["source_channel_name"] for record in unsubscribed["records"]] == [None, None]
    assert [record["subscription_status_code"] for record in unsubscribed["records"]] == [0, 0]

    assert subscribed["page_capacity"] == 2
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
            "message_id": 0x0411,
        }
    ) == bytes.fromhex("28090022041120130000020100000002001800000000000074762d70726f62653200")


@pytest.mark.asyncio
async def test_device_operation_returns_typed_receiver_status_page():
    response = _packet(0x2809, 0x3400, 28738)
    device = SimpleNamespace(execute=AsyncMock(return_value=response), services=_arc_services())
    operation = DanteApplication()

    page = await operation.query_modern_arc_receiver_channel_status(device)

    assert page["records"][0]["local_channel_name"] == "mic-mix"
    device.execute.assert_awaited_once()
    specification = device.execute.await_args.args[0]
    assert core.build_command({**specification, "message_id": 0x284A}) == _packet(0x2809, 0x3400, 28728)


@pytest.mark.asyncio
async def test_receiver_rename_selects_and_caches_2809_after_successful_status_probe():
    status_response = _packet(0x2809, 0x3400, 28729)
    rename_response = _packet(0x2809, 0x3401, 28727)
    device = SimpleNamespace(
        execute=AsyncMock(side_effect=[status_response, rename_response, rename_response]),
        receiver_channel_name_protocol_identifier=None,
        services=_arc_services(),
    )
    operation = DanteApplication()

    first_response = await operation.send_set_channel_name(device, "rx", 1, "mic-mix")
    second_response = await operation.send_set_channel_name(device, "rx", 1, "mic-mix")

    assert first_response == rename_response
    assert second_response == rename_response
    assert device.receiver_channel_name_protocol_identifier == 0x2809
    rename_specification = _rename_specification("rx", "mic-mix", 0x2809)
    specification = device.execute.await_args_list[0].args[0]
    assert core.build_command({**specification, "message_id": 0x284A}) == _packet(0x2809, 0x3400, 28728)
    assert device.execute.await_args_list[1:] == [
        call(rename_specification),
        call(rename_specification),
    ]


@pytest.mark.asyncio
async def test_receiver_rename_selects_2729_after_authentic_a32_frontend_rejection():
    status_response = _frontend_boundary_packet(0x3400, 4)
    rename_response = bytes.fromhex("2729000a000030010001")
    device = SimpleNamespace(
        execute=AsyncMock(side_effect=[status_response, rename_response]),
        receiver_channel_name_protocol_identifier=None,
        services=_arc_services(),
    )
    operation = DanteApplication()

    response = await operation.send_set_channel_name(device, "rx", 1, "Input-1")

    assert response == rename_response
    assert device.receiver_channel_name_protocol_identifier == 0x2729
    assert device.execute.await_args_list[-1] == call(_rename_specification("rx", "Input-1", 0x2729))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("probe_response", "message"),
    [
        (None, "did not receive a response"),
        (b"invalid", "invalid response"),
        (bytes.fromhex("2809000a000034000600"), "invalid response"),
        (bytes.fromhex("2809000a000024000030"), "invalid response"),
        (bytes.fromhex("2729000a000034000030"), "invalid response"),
    ],
)
async def test_receiver_rename_does_not_guess_after_an_indeterminate_frontend_probe(probe_response, message):
    device = SimpleNamespace(
        execute=AsyncMock(return_value=probe_response),
        receiver_channel_name_protocol_identifier=None,
        services=_arc_services(),
    )
    operation = DanteApplication()

    with pytest.raises(RuntimeError, match=message):
        await operation.send_set_channel_name(device, "rx", 1, "Input-1")

    assert device.execute.await_count == 1
    assert device.receiver_channel_name_protocol_identifier is None


def test_native_channel_name_protocol_selection_uses_captured_frontend_evidence():
    assert core.parse_response("receiver_channel_name_protocol", _packet(0x2809, 0x3400, 28729)) == 0x2809
    assert core.parse_response("receiver_channel_name_protocol", _frontend_boundary_packet(0x3400, 4)) == 0x2729

    with pytest.raises(core.NetaudioCoreError):
        core.parse_response("transmitter_channel_name_protocol", _frontend_boundary_packet(0x3400, 4))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("capability", "message"),
    [(False, "prohibits renaming"), (None, "rename capability is unavailable")],
)
async def test_receiver_rename_and_reset_fail_closed_before_mutation(capability, message):
    device = SimpleNamespace(
        requires_managed_control=False,
        rx_channels={1: SimpleNamespace(number=1, can_rename=capability)},
        execute=AsyncMock(),
    )
    operation = DanteApplication()

    with pytest.raises(ValueError, match=message):
        await operation.set_channel_name(device, "rx", 1, "Input-1")
    with pytest.raises(ValueError, match=message):
        await operation.reset_channel_name(device, "rx", 1)

    device.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate", [False, True])
async def test_receiver_rename_permission_uses_actual_channel_identity(duplicate):
    device = SimpleNamespace(
        requires_managed_control=False,
        rx_channels={
            1: SimpleNamespace(number=1 if duplicate else 2, can_rename=True),
            2: SimpleNamespace(number=1, can_rename=False),
        },
        execute=AsyncMock(),
    )
    operation = DanteApplication()

    with pytest.raises((RuntimeError, ValueError), match="conflicting identities|prohibits renaming"):
        await operation.reset_channel_name(device, "rx", 1)

    device.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_receiver_reset_accepts_rekeyed_channel_inventory():
    device = SimpleNamespace(
        requires_managed_control=False,
        rx_channels={"input": SimpleNamespace(number=1, can_rename=True)},
        execute=AsyncMock(return_value=b"acknowledged"),
    )

    assert await DanteApplication().reset_channel_name(device, "rx", 1) == b"acknowledged"
    device.execute.assert_awaited_once_with(
        {"command": "reset_channel_name", "channel_number": 1, "channel_type": "rx"}
    )
