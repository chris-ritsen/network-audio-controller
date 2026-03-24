import pathlib
import struct

import pytest

from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.device_commands import DanteDeviceCommands, Protocol, Opcode
from netaudio.dante.device_parser import DanteDeviceParser

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


def load_fixture(filename: str) -> bytes:
    path = FIXTURES_DIR / filename
    if not path.exists():
        pytest.skip(f"Fixture not found: {filename}")
    return path.read_bytes()


DEVICE_NAME_FIXTURES = [
    ("20250517_200646_215524_192_168_1_108_get_device_name", "lx-dante"),
    ("20250517_200646_356259_192_168_1_193_get_device_name", "avio-bt-1"),
    ("20250517_200646_390658_192_168_1_94_get_device_name", "avio-usb-3"),
    ("20250517_200646_412248_192_168_1_18_get_device_name", "avio-aes3-1"),
    ("20250517_200646_439405_192_168_1_247_get_device_name", "avio-usb-1"),
    ("20250517_200646_472412_192_168_1_36_get_device_name", "avio-usb-2"),
]

CHANNEL_COUNT_FIXTURES = [
    "20250517_200646_226874_lx-dante_get_channel_count",
    "20250517_200646_363064_avio-bt-1_get_channel_count",
    "20250517_200646_396138_avio-usb-3_get_channel_count",
    "20250517_200646_416392_avio-aes3-1_get_channel_count",
    "20250517_200646_445946_avio-usb-1_get_channel_count",
    "20250517_200646_478965_avio-usb-2_get_channel_count",
]

RECEIVER_FIXTURES = [
    "20250517_200646_289003_lx-dante_get_receivers",
    "20250517_200646_385043_avio-bt-1_get_receivers",
    "20250517_200646_408078_avio-usb-3_get_receivers",
    "20250517_200646_429145_avio-aes3-1_get_receivers",
    "20250517_200646_463580_avio-usb-1_get_receivers",
    "20250517_200646_499097_avio-usb-2_get_receivers",
]


class TestCommandDeviceName:
    @pytest.mark.parametrize("fixture_base,expected_name", DEVICE_NAME_FIXTURES)
    def test_command_generates_correct_payload(self, fixture_base, expected_name):
        commands = DanteDeviceCommands()
        packet, service_type = commands.command_device_name()
        expected = load_fixture(f"{fixture_base}_request.bin")

        assert service_type == SERVICE_ARC
        assert len(packet) == len(expected)
        assert packet[0:2] == struct.pack(">H", Protocol.CONTROL)
        assert packet[3] == 10
        assert packet[6:8] == struct.pack(">H", Opcode.DEVICE_NAME)
        assert packet[8:] == expected[8:]

    @pytest.mark.parametrize("fixture_base,expected_name", DEVICE_NAME_FIXTURES)
    def test_response_contains_device_name(self, fixture_base, expected_name):
        response = load_fixture(f"{fixture_base}_response.bin")
        name = DanteDeviceParser._get_string_at_offset(response, 10)
        assert name == expected_name


class TestCommandChannelCount:
    @pytest.mark.parametrize("fixture_base", CHANNEL_COUNT_FIXTURES)
    def test_command_generates_correct_payload(self, fixture_base):
        commands = DanteDeviceCommands()
        packet, service_type = commands.command_channel_count()
        expected = load_fixture(f"{fixture_base}_request.bin")

        assert service_type == SERVICE_ARC
        assert len(packet) == len(expected)
        assert packet[6:8] == struct.pack(">H", Opcode.CHANNEL_COUNT)
        assert packet[8:] == expected[8:]


class TestCommandReceivers:
    @pytest.mark.parametrize("fixture_base", RECEIVER_FIXTURES)
    def test_command_generates_correct_payload(self, fixture_base):
        commands = DanteDeviceCommands()
        packet, service_type = commands.command_receivers(page=0)
        expected = load_fixture(f"{fixture_base}_request.bin")

        assert service_type == SERVICE_ARC
        assert len(packet) == len(expected)
        assert packet[6:8] == struct.pack(">H", Opcode.RX_CHANNELS)
        assert packet[8:] == expected[8:]

    def test_page_1_generates_correct_starting_channel(self):
        commands = DanteDeviceCommands()
        packet, _ = commands.command_receivers(page=1)
        payload = packet[8:]
        starting_channel = struct.unpack(">H", payload[4:6])[0]
        assert starting_channel == 17

    def test_parse_avio_usb_3_rx_channels(self):
        response = load_fixture(
            "20250517_200646_408078_avio-usb-3_get_receivers_response.bin"
        )
        parser = DanteDeviceParser()
        body = response[10:]

        record_1 = body[2:22]
        assert struct.unpack(">H", record_1[0:2])[0] == 1
        assert parser._get_string_at_offset(response, struct.unpack(">H", record_1[6:8])[0]) == "mic-mix-high"
        assert parser._get_string_at_offset(response, struct.unpack(">H", record_1[8:10])[0]) == "lx-dante"
        assert parser._get_string_at_offset(response, struct.unpack(">H", record_1[10:12])[0]) == "mic-mix-1"
        assert struct.unpack(">H", record_1[14:16])[0] == 9

        record_2 = body[22:42]
        assert struct.unpack(">H", record_2[0:2])[0] == 2
        assert parser._get_string_at_offset(response, struct.unpack(">H", record_2[10:12])[0]) == "mic-mix-2"
        assert struct.unpack(">H", record_2[14:16])[0] == 9

    def test_parse_avio_bt_1_rx_channels(self):
        response = load_fixture(
            "20250517_200646_385043_avio-bt-1_get_receivers_response.bin"
        )
        parser = DanteDeviceParser()
        body = response[10:]

        record_1 = body[2:22]
        assert struct.unpack(">H", record_1[0:2])[0] == 1
        assert parser._get_string_at_offset(response, struct.unpack(">H", record_1[6:8])[0]) == "shelford-channel"
        assert parser._get_string_at_offset(response, struct.unpack(">H", record_1[8:10])[0]) == "a32"
        assert parser._get_string_at_offset(response, struct.unpack(">H", record_1[10:12])[0]) == "mic-mix"


class TestCommandTransmitters:
    def test_friendly_generates_correct_opcode(self):
        commands = DanteDeviceCommands()
        packet, service_type = commands.command_transmitters(page=0, friendly_names=True)

        assert service_type == SERVICE_ARC
        assert packet[6:8] == struct.pack(">H", Opcode.TX_CHANNEL_NAMES)

    def test_raw_generates_correct_opcode(self):
        commands = DanteDeviceCommands()
        packet, service_type = commands.command_transmitters(page=0, friendly_names=False)

        assert service_type == SERVICE_ARC
        assert packet[6:8] == struct.pack(">H", Opcode.TX_CHANNELS)

    def test_page_1_generates_correct_starting_channel(self):
        commands = DanteDeviceCommands()
        packet, _ = commands.command_transmitters(page=1, friendly_names=True)
        payload = packet[8:]
        starting_channel = struct.unpack(">H", payload[4:6])[0]
        assert starting_channel == 33


class TestSettingsCommandPacketFormat:

    def _check_header(self, packet, expected_msg_type_byte0, expected_msg_type_byte1):
        assert packet[0:2] == b'\xff\xff'
        assert packet[2] == 0x00
        assert packet[3] == len(packet)
        assert packet[6:8] == b'\x00\x00'
        assert packet[14:16] == b'\x00\x00'
        assert packet[16:24] == b'Audinate'
        assert packet[26] == expected_msg_type_byte0
        assert packet[27] == expected_msg_type_byte1

    def test_identify_packet_format(self):
        commands = DanteDeviceCommands()
        packet, _, port = commands.command_identify()
        assert port == 8700
        assert len(packet) == 32
        self._check_header(packet, 0x00, 0x63)
        assert packet[8:14] == b'\x00' * 6

    def test_bluetooth_status_packet_format(self):
        host_mac = b'\xaa\xbb\xcc\xdd\xee\xff'
        commands = DanteDeviceCommands()
        packet, _, port = commands.command_bluetooth_status(host_mac=host_mac)
        assert port == 8700
        assert len(packet) == 48
        self._check_header(packet, 0x10, 0x0d)
        assert packet[8:14] == host_mac

    def test_bluetooth_status_without_mac_uses_zeros(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_bluetooth_status()
        assert packet[8:14] == b'\x00' * 6

    def test_enable_aes67_packet_format(self):
        host_mac = b'\xaa\xbb\xcc\xdd\xee\xff'
        commands = DanteDeviceCommands()
        packet_enable, _, port = commands.command_enable_aes67(True, host_mac=host_mac)
        packet_disable, _, _ = commands.command_enable_aes67(False, host_mac=host_mac)
        assert port == 8700
        assert len(packet_enable) == 36
        assert len(packet_disable) == 36
        self._check_header(packet_enable, 0x10, 0x06)
        assert packet_enable[8:14] == host_mac
        assert packet_enable[-1] == 0x01
        assert packet_disable[-1] == 0x00

    def test_set_encoding_packet_format(self):
        commands = DanteDeviceCommands()
        packet, _, port = commands.command_set_encoding(24)
        assert port == 8700
        self._check_header(packet, 0x00, 0x83)

    def test_set_sample_rate_packet_format(self):
        commands = DanteDeviceCommands()
        packet, _, port = commands.command_set_sample_rate(48000)
        assert port == 8700
        self._check_header(packet, 0x00, 0x81)

    def test_set_gain_level_packet_format(self):
        commands = DanteDeviceCommands()
        packet, _, port = commands.command_set_gain_level(1, 50, "input")
        assert port == 8700
        self._check_header(packet, 0x10, 0x01)


class TestParserBluetoothStatus:
    def test_connected_device_name(self):
        response = load_fixture("avio-bt-1_bluetooth_status_connected.bin")
        name = DanteDeviceParser.parse_bluetooth_status(response)
        assert name == "s00pcan-iphone-17"

    def test_disconnected_returns_none(self):
        response = load_fixture("avio-bt-1_bluetooth_status_disconnected.bin")
        name = DanteDeviceParser.parse_bluetooth_status(response)
        assert name is None
