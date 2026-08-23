from tests.test_cli_write_verification import (
    FakeChannelDevice,
    _install_context,
    channel_commands,
    reset_cli_state,
    runner,
)


def test_receiver_channel_name_uses_2809_after_successful_frontend_probe(monkeypatch, reset_cli_state):
    device = FakeChannelDevice(channel_reads="New", channel_type="rx")
    status_response = bytes.fromhex(
        "2809007c284a34000001000000000000010100446d69632d6d69782d68696768006c782d64616e74650000000000bb800101001804000018001800043031004c65667400141c000100000003000100000000000600000000003c002c000000000000003f000000000000000006080000001400210010000002020000"
    )
    rename_response = bytes.fromhex("2809001d2849340100010000000000000600010100010003001a303100")
    sent = _install_context(
        monkeypatch,
        channel_commands,
        {"avio.local.": device},
        send_responses=[status_response, rename_response],
    )

    result = runner.invoke(channel_commands.app, ["name", "1", "New", "--type", "rx"])

    assert result.exit_code == 0
    assert [int.from_bytes(packet[0:2], "big") for _, _, packet, _ in sent] == [0x2809, 0x2809]
    assert [int.from_bytes(packet[6:8], "big") for _, _, packet, _ in sent] == [0x3400, 0x3401]
    assert device.receiver_channel_name_protocol_identifier == 0x2809


def test_receiver_channel_name_uses_2729_after_authentic_a32_frontend_rejection(monkeypatch, reset_cli_state):
    device = FakeChannelDevice(channel_reads="New", channel_type="rx")
    status_response = bytes.fromhex("2809000a284a34000030")
    rename_response = bytes.fromhex("2729000a000030010001")
    sent = _install_context(
        monkeypatch,
        channel_commands,
        {"a32.local.": device},
        send_responses=[status_response, rename_response],
    )

    result = runner.invoke(channel_commands.app, ["name", "1", "New", "--type", "rx"])

    assert result.exit_code == 0
    assert [int.from_bytes(packet[0:2], "big") for _, _, packet, _ in sent] == [0x2809, 0x2729]
    assert [int.from_bytes(packet[6:8], "big") for _, _, packet, _ in sent] == [0x3400, 0x3001]
    assert device.receiver_channel_name_protocol_identifier == 0x2729


def test_transmitter_channel_name_uses_2809_after_successful_frontend_probe(monkeypatch, reset_cli_state):
    device = FakeChannelDevice(channel_reads="New")
    device.transmitter_channel_name_protocol_identifier = None
    status_response = bytes.fromhex(
        "280900a42852240000010000000000000202003c007c00030000bb80010100180400001800180004626c7565746f6f74683a6c656674004c6566740014140001000000030001000000000007000000000028001800000000000000370000000000000000626c7565746f6f74683a726967687400526967687400000014140002000000030002000000000007000000000064001800000000000000740000000000000000"
    )
    rename_response = bytes.fromhex("2809000c0302201300010000")
    sent = _install_context(
        monkeypatch,
        channel_commands,
        {"avio.local.": device},
        send_responses=[status_response, rename_response],
    )

    result = runner.invoke(channel_commands.app, ["name", "1", "New", "--type", "tx"])

    assert result.exit_code == 0
    assert [int.from_bytes(packet[0:2], "big") for _, _, packet, _ in sent] == [0x2809, 0x2809]
    assert [int.from_bytes(packet[6:8], "big") for _, _, packet, _ in sent] == [0x2400, 0x2013]
    assert device.transmitter_channel_name_protocol_identifier == 0x2809


def test_transmitter_channel_name_uses_2729_after_authentic_a32_frontend_rejection(monkeypatch, reset_cli_state):
    device = FakeChannelDevice(channel_reads="New")
    device.transmitter_channel_name_protocol_identifier = None
    status_response = bytes.fromhex("2809000a285224000030")
    rename_response = bytes.fromhex("2729000c0302201300010000")
    sent = _install_context(
        monkeypatch,
        channel_commands,
        {"a32.local.": device},
        send_responses=[status_response, rename_response],
    )

    result = runner.invoke(channel_commands.app, ["name", "1", "New", "--type", "tx"])

    assert result.exit_code == 0
    assert [int.from_bytes(packet[0:2], "big") for _, _, packet, _ in sent] == [0x2809, 0x2729]
    assert [int.from_bytes(packet[6:8], "big") for _, _, packet, _ in sent] == [0x2400, 0x2013]
    assert device.transmitter_channel_name_protocol_identifier == 0x2729
