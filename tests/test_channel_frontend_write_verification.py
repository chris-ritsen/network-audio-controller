from netaudio._common_selection import parse_channel_reference
from netaudio.commands import channel as channel_commands
from netaudio.dante.device_operations import DanteDeviceOperations
from tests.cli_test_support import FakeApplication, FakeChannelDevice, invoke


class ExecutingChannelDevice(FakeChannelDevice):
    def __init__(self, *, channel_type, responses):
        super().__init__(channel_reads="New", channel_type=channel_type)
        self.receiver_channel_name_protocol_identifier = None
        self.transmitter_channel_name_protocol_identifier = None
        self.responses = list(responses)
        self.executed = []
        self.operations = DanteDeviceOperations(self)

    async def execute(self, specification):
        self.executed.append(specification)
        return self.responses.pop(0)


class ExecutingApplication(FakeApplication):
    async def set_channel_name(self, device, channel_type, channel_number, name):
        return await device.operations.set_channel_name(channel_type, channel_number, name)


def _rename(device, channel):
    application = ExecutingApplication({"device.local.": device})
    return invoke(
        channel_commands.run_channel_name,
        application,
        application.devices,
        parse_channel_reference(channel),
        "New",
    )


def test_receiver_channel_name_uses_2809_after_successful_frontend_probe(reset_cli_state):
    status_response = bytes.fromhex(
        "2809007c284a34000001000000000000010100446d69632d6d69782d68696768006c782d64616e74650000000000bb800101001804000018001800043031004c65667400141c000100000003000100000000000600000000003c002c000000000000003f000000000000000006080000001400210010000002020000"
    )
    rename_response = bytes.fromhex("2809001d2849340100010000000000000600010100010003001a303100")
    device = ExecutingChannelDevice(channel_type="rx", responses=[status_response, rename_response])

    result = _rename(device, "rx:1")

    assert result.exit_code == 0
    assert [specification["command"] for specification in device.executed] == [
        "query_receiver_channel_status_2809",
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
        "query_receiver_channel_status_2809",
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
        "query_transmitter_channel_status_2809",
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
        "query_transmitter_channel_status_2809",
        "set_channel_name",
    ]
    assert device.executed[1]["protocol_id"] == 0x2729
    assert device.transmitter_channel_name_protocol_identifier == 0x2729
