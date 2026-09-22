import pytest
from copy import copy
from unittest.mock import AsyncMock

from netaudio.cli_support.selection import parse_channel_reference
from netaudio.commands import channel as channel_commands
from netaudio.dante.application import DanteApplication
from netaudio.dante.commands import DanteCommands

from tests.cli_test_support import FakeApplication, FakeChannelDevice, invoke


class ExecutingChannelDevice(FakeChannelDevice):
    def __init__(self, *, channel_type, responses):
        super().__init__(channel_reads="New", channel_type=channel_type)
        self.services = {"arc": {"type": "_netaudio-arc._udp.local.", "properties": {"arcp_vers": "2.8.9"}}}
        self.receiver_channel_name_protocol_identifier = None
        self.transmitter_channel_name_protocol_identifier = None
        self.responses = list(responses)
        self.executed = []

    async def execute(self, specification):
        self.executed.append(specification)
        return self.responses.pop(0)


class ExecutingApplication(FakeApplication):
    commands = DanteCommands()
    resolve_channel_name_protocol_identifier = DanteApplication.resolve_channel_name_protocol_identifier
    send_set_channel_name = DanteApplication.send_set_channel_name

    async def set_channel_name(self, device, channel_type, channel_number, name):
        return await self.send_set_channel_name(device, channel_type, channel_number, name)


def _rename(device, channel):
    application = ExecutingApplication({"device.local.": device})
    return invoke(
        channel_commands.run_channel_name,
        application,
        application.devices,
        parse_channel_reference(channel),
        "New",
    )


@pytest.mark.parametrize("direction", ["rx", "tx"])
@pytest.mark.parametrize("inventory", ["rekeyed", "wrong_channel", "duplicate"])
def test_rename_readback_checks_identity_not_dictionary_key(reset_cli_state, direction, inventory):
    response = bytes.fromhex("2729000a000030010001" if direction == "rx" else "2729000c0302201300010000")
    device = ExecutingChannelDevice(channel_type=direction, responses=[response])
    device.receiver_channel_name_protocol_identifier = 0x2729
    device.transmitter_channel_name_protocol_identifier = 0x2729
    original = getattr(device, f"{direction}_channels")[1]

    async def refresh():
        channel = copy(original)
        channel.name = channel.friendly_name = "New"

        if inventory == "rekeyed":
            channels = {"New": channel}
        elif inventory == "wrong_channel":
            channel.number = 2
            channels = {1: channel}
        else:
            channels = {1: channel, 2: copy(channel)}

        setattr(device, f"{direction}_channels", channels)

    setattr(device, f"get_{direction}_channels", AsyncMock(side_effect=refresh))
    result = _rename(device, f"{direction}:1")

    assert result.exit_code == (0 if inventory == "rekeyed" else 1)
    assert ("(verified)" in result.output) == (inventory == "rekeyed")


@pytest.mark.parametrize(
    "response,detail",
    [
        (None, "did not receive a response"),
        (b"invalid", "invalid response"),
        (bytes.fromhex("2729000c0302201300070000"), "not acknowledged"),
    ],
)
def test_unacknowledged_rename_is_not_verified_even_with_matching_inventory(reset_cli_state, response, detail):
    device = ExecutingChannelDevice(channel_type="tx", responses=[bytes.fromhex("2809000a285224000030"), response])

    result = _rename(device, "tx:1")

    assert result.exit_code == 1
    assert detail in result.output
    assert "0x" not in result.output
    assert "(verified)" not in result.output
    assert device.channel_read_calls == 0


def test_receiver_channel_name_uses_2809_after_successful_frontend_probe(reset_cli_state):
    status_response = bytes.fromhex(
        "2809007c284a34000001000000000000010100446d69632d6d69782d68696768006c782d64616e74650000000000bb800101001804000018001800043031004c65667400141c000100000003000100000000000600000000003c002c000000000000003f000000000000000006080000001400210010000002020000"
    )
    rename_response = bytes.fromhex("2809001d2849340100010000000000000600010100010003001a303100")
    device = ExecutingChannelDevice(channel_type="rx", responses=[status_response, rename_response])

    result = _rename(device, "rx:1")

    assert result.exit_code == 0
    assert [specification["command"] for specification in device.executed] == [
        "query_modern_arc_receiver_channel_status",
        "set_channel_name",
    ]
    assert device.executed[1]["protocol_id"] == 0x2809
    assert device.receiver_channel_name_protocol_identifier == 0x2809


def test_receiver_channel_name_uses_2729_after_authentic_a32_frontend_rejection(reset_cli_state):
    status_response = bytes.fromhex("2809000a284a34000030")
    rename_response = bytes.fromhex("2729000a000030010001")
    device = ExecutingChannelDevice(channel_type="rx", responses=[status_response, rename_response])

    result = _rename(device, "rx:1")

    assert result.exit_code == 0
    assert [specification["command"] for specification in device.executed] == [
        "query_modern_arc_receiver_channel_status",
        "set_channel_name",
    ]
    assert device.executed[1]["protocol_id"] == 0x2729
    assert device.receiver_channel_name_protocol_identifier == 0x2729


def test_transmitter_channel_name_uses_2809_after_successful_frontend_probe(reset_cli_state):
    status_response = bytes.fromhex(
        "280900a42852240000010000000000000202003c007c00030000bb80010100180400001800180004626c7565746f6f74683a6c656674004c6566740014140001000000030001000000000007000000000028001800000000000000370000000000000000626c7565746f6f74683a726967687400526967687400000014140002000000030002000000000007000000000064001800000000000000740000000000000000"
    )
    rename_response = bytes.fromhex("2809000c0302201300010000")
    device = ExecutingChannelDevice(channel_type="tx", responses=[status_response, rename_response])

    result = _rename(device, "tx:1")

    assert result.exit_code == 0
    assert [specification["command"] for specification in device.executed] == [
        "query_modern_arc_transmitter_channel_status",
        "set_channel_name",
    ]
    assert device.executed[1]["protocol_id"] == 0x2809
    assert device.transmitter_channel_name_protocol_identifier == 0x2809


def test_transmitter_channel_name_uses_2729_after_authentic_a32_frontend_rejection(reset_cli_state):
    status_response = bytes.fromhex("2809000a285224000030")
    rename_response = bytes.fromhex("2729000c0302201300010000")
    device = ExecutingChannelDevice(channel_type="tx", responses=[status_response, rename_response])

    result = _rename(device, "tx:1")

    assert result.exit_code == 0
    assert [specification["command"] for specification in device.executed] == [
        "query_modern_arc_transmitter_channel_status",
        "set_channel_name",
    ]
    assert device.executed[1]["protocol_id"] == 0x2729
    assert device.transmitter_channel_name_protocol_identifier == 0x2729
