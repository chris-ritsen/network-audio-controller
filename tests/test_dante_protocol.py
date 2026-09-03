from __future__ import annotations

import struct

import pytest

from netaudio import _capture
from netaudio.dante.const import (
    OPCODE_TX_CHANNEL_INFO,
    OPCODE_TX_CHANNEL_NAMES,
    SERVICE_ARC,
)
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_parser import DanteDeviceParser


class TestCommandTransmitters:
    def test_friendly_names_generate_capture_backed_range(self):
        commands = DanteDeviceCommands()
        packet, service_type = commands.command_transmitter_names(channel_count=2)

        assert service_type == SERVICE_ARC
        assert packet[6:8] == struct.pack(">H", OPCODE_TX_CHANNEL_NAMES)
        assert packet[8:] == bytes.fromhex("0000000100010002")

    def test_raw_generates_correct_opcode(self):
        commands = DanteDeviceCommands()
        packet, service_type = commands.command_transmitters(page=0)

        assert service_type == SERVICE_ARC
        assert packet[6:8] == struct.pack(">H", OPCODE_TX_CHANNEL_INFO)

    def test_page_1_generates_correct_starting_channel(self):
        commands = DanteDeviceCommands()
        packet, _ = commands.command_transmitters(page=1)
        payload = packet[8:]
        starting_channel = struct.unpack(">H", payload[4:6])[0]
        assert starting_channel == 33


def test_instrumented_tx_fetch_uses_one_full_range_friendly_name_query(monkeypatch):
    command_specifications = []

    def query(client, command_specification, port, parse_kind=None, starting_channel=None):
        command_specifications.append((command_specification, parse_kind, starting_channel))
        if command_specification["command"] == "transmitter_names":
            return [(1, "Friendly One"), (2, "Friendly Two")]
        return [
            {"number": 1, "name": "Channel One"},
            {"number": 2, "name": "Channel Two"},
        ]

    monkeypatch.setattr(_capture, "_query", query)

    records = _capture.fetch_tx_records(object(), 4440, 2)

    assert records == [
        {"number": 1, "name": "Channel One", "friendly_name": "Friendly One"},
        {"number": 2, "name": "Channel Two", "friendly_name": "Friendly Two"},
    ]
    assert command_specifications == [
        ({"command": "transmitter_names", "channel_count": 2}, "tx_friendly", 1),
        ({"command": "transmitters", "page": 0}, "tx_info", 1),
    ]


def test_instrumented_tx_fetch_skips_friendly_query_for_zero_channels(monkeypatch):
    command_specifications = []

    def query(client, command_specification, port, parse_kind=None, starting_channel=None):
        command_specifications.append(command_specification)
        return []

    monkeypatch.setattr(_capture, "_query", query)

    assert _capture.fetch_tx_records(object(), 4440, 0) == []
    assert command_specifications == [{"command": "transmitters", "page": 0}]


class TestSettingsCommandPacketFormat:
    def _check_header(self, packet, expected_msg_type_byte0, expected_msg_type_byte1):
        assert packet[0:2] == b"\xff\xff"
        assert packet[2] == 0x00
        assert packet[3] == len(packet)
        assert packet[6:8] == b"\x00\x00"
        assert packet[14:16] == b"\x00\x00"
        assert packet[16:24] == b"Audinate"
        assert packet[26] == expected_msg_type_byte0
        assert packet[27] == expected_msg_type_byte1

    def test_identify_packet_format(self):
        commands = DanteDeviceCommands(settings_sequence=0x0BC8)
        packet, _, port = commands.command_identify()
        assert port == 8700
        assert len(packet) == 32
        assert packet[4:6] == b"\x0b\xc9"
        self._check_header(packet, 0x00, 0x63)
        assert packet[8:14] == b"\x00" * 6

    def test_bluetooth_status_packet_format(self):
        host_mac = b"\xaa\xbb\xcc\xdd\xee\xff"
        commands = DanteDeviceCommands()
        packet, _, port = commands.command_bluetooth_status(host_mac=host_mac)
        assert port == 8700
        assert len(packet) == 48
        self._check_header(packet, 0x10, 0x0D)
        assert packet[8:14] == host_mac

    def test_reboot_uses_incrementing_nonzero_sequence(self):
        commands = DanteDeviceCommands(settings_sequence=0)
        first, _, first_port = commands.command_reboot(host_mac=b"\xaa\xbb\xcc\xdd\xee\xff")
        second, _, second_port = commands.command_reboot(host_mac=b"\xaa\xbb\xcc\xdd\xee\xff")
        assert first_port == second_port == 8700
        assert first[4:6] == bytes.fromhex("0001")
        assert second[4:6] == bytes.fromhex("0002")

    def test_factory_reset_matches_authentic_selector_one_request(self):
        commands = DanteDeviceCommands(settings_sequence=0x18A3)
        packet, _, port = commands.command_factory_reset(host_mac=b">B'L\xff$")
        assert port == 8700
        assert packet.hex() == ("ffff002418a400003e42274cff240000417564696e617465073a00900000006400010001")

    def test_interface_writes_use_incrementing_nonzero_sequence(self):
        commands = DanteDeviceCommands(settings_sequence=0)
        host_mac = b"\xaa\xbb\xcc\xdd\xee\xff"
        dhcp, _, dhcp_port = commands.command_set_interface_dhcp(host_mac=host_mac)
        static, _, static_port = commands.command_set_interface_static(
            "192.0.2.10",
            "255.255.255.0",
            "192.0.2.53",
            "192.0.2.1",
            host_mac=host_mac,
        )
        assert dhcp_port == static_port == 8700
        assert dhcp[4:6] == bytes.fromhex("0001")
        assert static[4:6] == bytes.fromhex("0002")
        assert len(dhcp) == len(static) == 68

    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("command_reboot", ()),
            ("command_factory_reset", ()),
            ("command_enable_aes67", (True,)),
            ("command_probe_interface_status", ()),
            ("command_probe_link_status", ()),
            ("command_set_interface_dhcp", ()),
            (
                "command_set_interface_static",
                ("192.0.2.10", "255.255.255.0", "192.0.2.53", "192.0.2.1"),
            ),
            ("command_probe_aes67", ()),
            ("command_probe_lock_reset_status", ()),
            ("command_probe_sample_rate", ()),
            ("command_probe_encoding", ()),
            ("command_probe_sample_rate_pullup", ()),
            ("command_set_preferred_leader", (True,)),
            ("command_probe_preferred_leader", ()),
            ("command_refresh_clock_status", ()),
            ("command_bluetooth_status", ()),
        ],
    )
    def test_host_identified_settings_commands_use_discovered_mac(
        self,
        monkeypatch,
        method,
        args,
    ):
        discovered = b"\x10\x20\x30\x40\x50\x60"
        monkeypatch.setattr(
            "netaudio.dante.services.cmc._get_host_mac",
            lambda: discovered,
        )
        commands = DanteDeviceCommands()
        packet, _, _ = getattr(commands, method)(*args)
        assert packet[8:14] == discovered

    def test_settings_command_rejects_zero_host_mac(self):
        commands = DanteDeviceCommands(host_mac=b"\x00" * 6)
        with pytest.raises(ValueError, match="non-zero 6-byte host MAC"):
            commands.command_set_interface_dhcp()

    def test_enable_aes67_packet_format(self):
        host_mac = b"\xaa\xbb\xcc\xdd\xee\xff"
        commands = DanteDeviceCommands()
        packet_enable, _, port = commands.command_enable_aes67(True, host_mac=host_mac)
        packet_disable, _, _ = commands.command_enable_aes67(False, host_mac=host_mac)
        assert port == 8700
        assert len(packet_enable) == 36
        assert len(packet_disable) == 36
        self._check_header(packet_enable, 0x10, 0x06)
        assert packet_enable[8:14] == host_mac
        assert packet_enable[32:34] == b"\x00\x01"
        assert packet_enable[34:36] == b"\x00\x01"
        assert packet_disable[32:34] == b"\x00\x01"
        assert packet_disable[34:36] == b"\x00\x00"

    def test_set_encoding_packet_format(self):
        commands = DanteDeviceCommands()
        packet, _, port = commands.command_set_encoding(24)
        assert port == 8700
        self._check_header(packet, 0x00, 0x83)

    def test_set_sample_rate_packet_format(self):
        commands = DanteDeviceCommands(settings_sequence=0x18AF)
        packet, _, port = commands.command_set_sample_rate(48000)
        next_packet, _, _ = commands.command_set_sample_rate(192000)
        assert port == 8700
        self._check_header(packet, 0x00, 0x81)
        assert packet[4:6] == bytes.fromhex("18b0")
        assert next_packet[4:6] == bytes.fromhex("18b1")

    def test_sample_rate_pullup_packet_format(self):
        host_mac = bytes.fromhex("52550a000202")
        commands = DanteDeviceCommands(host_mac=host_mac, settings_sequence=0x0046)
        probe, _, probe_port = commands.command_probe_sample_rate_pullup(sequence=0x0047)
        write, _, write_port = commands.command_set_sample_rate_pullup(1)
        assert probe_port == write_port == 8700
        assert probe == bytes.fromhex(
            "ffff00380047000052550a0002020000417564696e617465073a0085"
            "00000000000000000000000000000000000000000000000000000000"
        )
        assert write == bytes.fromhex(
            "ffff00380047000052550a0002020000417564696e617465073a0085"
            "00000000000000010000000100000000000000000000000000000000"
        )

    def test_set_gain_level_packet_format(self):
        host_mac = bytes.fromhex("842f5774e86d")
        commands = DanteDeviceCommands(host_mac=host_mac)
        packet, _, port = commands.command_set_gain_level(1, 5, "input", sequence=0xC006)
        assert port == 8700
        self._check_header(packet, 0x10, 0x0A)
        assert packet[4:6] == bytes.fromhex("c006")
        assert packet[8:14] == host_mac
        assert packet[40:42] == bytes.fromhex("0102")
        assert packet[46:48] == bytes.fromhex("0001")
        assert packet[48:52] == bytes.fromhex("00000005")

    def test_probe_gain_level_packet_format(self):
        host_mac = bytes.fromhex("842f5774e86d")
        commands = DanteDeviceCommands(host_mac=host_mac)
        packet, _, port = commands.command_probe_gain_level(sequence=0x045A)
        assert port == 8700
        self._check_header(packet, 0x10, 0x0A)
        assert packet.hex() == "ffff0028045a0000842f5774e86d0000417564696e617465073a100a000000000000000000000000"


class TestParserBluetoothStatus:
    def test_connected_device_name(self, load_fixture):
        response = load_fixture("avio-bt-1_bluetooth_status_connected.bin")
        name = DanteDeviceParser.parse_bluetooth_status(response)
        assert name == "s00pcan-iphone-17"

    def test_disconnected_returns_none(self, load_fixture):
        response = load_fixture("avio-bt-1_bluetooth_status_disconnected.bin")
        name = DanteDeviceParser.parse_bluetooth_status(response)
        assert name is None
