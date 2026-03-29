import asyncio
import struct

import pytest

from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.services.notification import (
    AES67_CURRENT_NEW_MAP,
    CONMON_AES67_CURRENT_NEW_OFFSET,
    CONMON_OPCODE_AES67_CURRENT_NEW,
    DanteNotificationService,
    parse_aes67_current_new_byte,
)


class TestAES67ConfiguredFromARC1100:
    AES67_MODE_OFFSET = 0x53

    def _build_latency_config_response(self, aes67_byte):
        response = bytearray(148)
        struct.pack_into(">H", response, 0, 0x2809)
        struct.pack_into(">H", response, 2, 148)
        struct.pack_into(">H", response, 6, 0x1100)
        struct.pack_into(">H", response, 8, 0x0001)
        response[self.AES67_MODE_OFFSET] = aes67_byte
        return bytes(response)

    def test_byte_0x01_means_configured_disabled(self):
        response = self._build_latency_config_response(0x01)
        assert response[self.AES67_MODE_OFFSET] == 0x01

    def test_byte_0x03_means_configured_enabled(self):
        response = self._build_latency_config_response(0x03)
        assert response[self.AES67_MODE_OFFSET] == 0x03

    def test_response_parsing_disabled(self):
        response = self._build_latency_config_response(0x01)
        aes67_byte = response[self.AES67_MODE_OFFSET]
        assert aes67_byte == 0x01
        configured = aes67_byte == 0x03
        assert configured is False

    def test_response_parsing_enabled(self):
        response = self._build_latency_config_response(0x03)
        aes67_byte = response[self.AES67_MODE_OFFSET]
        assert aes67_byte == 0x03
        configured = aes67_byte == 0x03
        assert configured is True

    def test_response_too_short_returns_none(self):
        response = bytes(0x53)
        assert len(response) <= self.AES67_MODE_OFFSET

    def test_query_latency_config_packet_structure(self):
        commands = DanteDeviceCommands()
        packet, service, port = commands.command_query_latency_config(transaction_id=0x0745)
        assert struct.unpack(">H", packet[0:2])[0] == 0x2809
        assert struct.unpack(">H", packet[2:4])[0] == 58
        assert struct.unpack(">H", packet[4:6])[0] == 0x0745
        assert struct.unpack(">H", packet[6:8])[0] == 0x1100
        assert struct.unpack(">H", packet[8:10])[0] == 0x0000
        assert port is None

    def test_query_latency_config_packet_length(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_query_latency_config()
        assert len(packet) == 58

    def test_query_latency_config_matches_captured(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_query_latency_config(transaction_id=0x0745)
        captured = bytes.fromhex(
            "2809003a07451100000000170201820482050210"
            "021182188219830183028306031003110303802100"
            "f080600022006300640065022202128321"
        )
        assert packet == captured


class TestAES67CurrentNewFromConmon1007:
    def test_0x00_means_disabled_disabled(self):
        current, configured = parse_aes67_current_new_byte(0x00)
        assert current is False
        assert configured is False

    def test_0x02_means_disabled_enabled(self):
        current, configured = parse_aes67_current_new_byte(0x02)
        assert current is False
        assert configured is True

    def test_0x03_means_enabled_enabled(self):
        current, configured = parse_aes67_current_new_byte(0x03)
        assert current is True
        assert configured is True

    def test_0x01_means_enabled_disabled(self):
        current, configured = parse_aes67_current_new_byte(0x01)
        assert current is True
        assert configured is False

    def test_unknown_byte_returns_none_none(self):
        current, configured = parse_aes67_current_new_byte(0xFF)
        assert current is None
        assert configured is None

    def test_map_has_four_values(self):
        assert len(AES67_CURRENT_NEW_MAP) == 4
        assert set(AES67_CURRENT_NEW_MAP.keys()) == {0x00, 0x01, 0x02, 0x03}

    def test_conmon_opcode_constant(self):
        assert CONMON_OPCODE_AES67_CURRENT_NEW == 0x1007

    def test_offset_constant(self):
        assert CONMON_AES67_CURRENT_NEW_OFFSET == 0x21

    def _build_conmon_1007_packet(self, state_byte):
        packet = bytearray(36)
        struct.pack_into(">H", packet, 0, 0xFFFF)
        struct.pack_into(">H", packet, 2, 36)
        packet[0x10:0x18] = b'Audinate'
        struct.pack_into(">H", packet, 0x1A, 0x1007)
        packet[CONMON_AES67_CURRENT_NEW_OFFSET] = state_byte
        return bytes(packet)

    def test_parse_from_real_packet_structure_disabled_disabled(self):
        packet = self._build_conmon_1007_packet(0x00)
        state_byte = packet[CONMON_AES67_CURRENT_NEW_OFFSET]
        current, configured = parse_aes67_current_new_byte(state_byte)
        assert current is False
        assert configured is False

    def test_parse_from_real_packet_structure_disabled_enabled(self):
        packet = self._build_conmon_1007_packet(0x02)
        state_byte = packet[CONMON_AES67_CURRENT_NEW_OFFSET]
        current, configured = parse_aes67_current_new_byte(state_byte)
        assert current is False
        assert configured is True

    def test_parse_from_real_packet_structure_enabled_enabled(self):
        packet = self._build_conmon_1007_packet(0x03)
        state_byte = packet[CONMON_AES67_CURRENT_NEW_OFFSET]
        current, configured = parse_aes67_current_new_byte(state_byte)
        assert current is True
        assert configured is True


class TestAES67RebootRequired:
    def _reboot_required(self, device):
        return (
            device.aes67_current is not None
            and device.aes67_configured is not None
            and device.aes67_current != device.aes67_configured
        )

    def test_reboot_required_off_to_on(self):
        device = DanteDevice()
        device.aes67_current = False
        device.aes67_configured = True
        assert self._reboot_required(device) is True

    def test_reboot_required_on_to_off(self):
        device = DanteDevice()
        device.aes67_current = True
        device.aes67_configured = False
        assert self._reboot_required(device) is True

    def test_no_reboot_when_both_off(self):
        device = DanteDevice()
        device.aes67_current = False
        device.aes67_configured = False
        assert self._reboot_required(device) is False

    def test_no_reboot_when_both_on(self):
        device = DanteDevice()
        device.aes67_current = True
        device.aes67_configured = True
        assert self._reboot_required(device) is False

    def test_no_reboot_when_current_unknown(self):
        device = DanteDevice()
        device.aes67_current = None
        device.aes67_configured = True
        assert self._reboot_required(device) is False


class TestDanteDeviceAES67Model:
    def test_default_state_is_none(self):
        device = DanteDevice()
        assert device.aes67_configured is None
        assert device.aes67_current is None

    def test_serializer_includes_aes67_fields(self):
        device = DanteDevice()
        device.name = "test"
        device.server_name = "test"
        device.ipv4 = "192.168.1.1"
        device.aes67_configured = True
        device.aes67_current = False
        json_data = device.to_json()
        assert json_data["aes67_configured"] is True
        assert json_data["aes67_current"] is False

    def test_serializer_omits_none_aes67(self):
        device = DanteDevice()
        device.name = "test"
        device.server_name = "test"
        device.ipv4 = "192.168.1.1"
        json_data = device.to_json()
        assert "aes67_configured" not in json_data
        assert "aes67_current" not in json_data


class TestAES67ProbePacket:
    def test_probe_packet_length(self):
        commands = DanteDeviceCommands()
        packet, _, port = commands.command_probe_aes67()
        assert len(packet) == 36
        assert port == DEVICE_SETTINGS_PORT

    def test_probe_packet_structure(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_probe_aes67()
        assert struct.unpack(">H", packet[0:2])[0] == 0xFFFF
        magic_offset = packet.find(b'Audinate')
        assert magic_offset == 0x10
        message_type = struct.unpack(">H", packet[0x1A:0x1C])[0]
        assert message_type == 0x1006

    def test_probe_has_zero_presence_and_enable_flags(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_probe_aes67()
        assert packet[32:34] == b'\x00\x00'
        assert packet[34:36] == b'\x00\x00'

    def test_probe_has_nonzero_sequence(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_probe_aes67()
        sequence = struct.unpack(">H", packet[4:6])[0]
        assert sequence != 0

    def test_probe_with_custom_mac(self):
        commands = DanteDeviceCommands()
        mac = b'\xaa\xbb\xcc\xdd\xee\xff'
        packet, _, _ = commands.command_probe_aes67(host_mac=mac)
        assert packet[8:14] == mac

    def test_probe_differs_from_enable(self):
        commands = DanteDeviceCommands()
        probe, _, _ = commands.command_probe_aes67()
        enable, _, _ = commands.command_enable_aes67(True)
        assert probe[32:36] == b'\x00\x00\x00\x00'
        assert enable[32:36] == b'\x00\x01\x00\x01'


class TestAES67Waiter:
    def _build_conmon_1007_packet(self, state_byte, source_eui64=None):
        packet = bytearray(36)
        struct.pack_into(">H", packet, 0, 0xFFFF)
        struct.pack_into(">H", packet, 2, 36)
        if source_eui64:
            packet[8:16] = source_eui64
        packet[0x10:0x18] = b'Audinate'
        struct.pack_into(">H", packet, 0x18, 0x073A)
        struct.pack_into(">H", packet, 0x1A, 0x1007)
        packet[CONMON_AES67_CURRENT_NEW_OFFSET] = state_byte
        return bytes(packet)

    def test_register_and_notify_waiter(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.247"

        waiter = service.register_aes67_waiter(device_ip)
        assert not waiter.is_set()

        packet = self._build_conmon_1007_packet(0x03)
        service._on_packet(packet, (device_ip, 8700))

        assert waiter.is_set()
        result = service.get_aes67_result(device_ip)
        assert result == (True, True)

        service.unregister_aes67_waiter(device_ip)

    def test_waiter_captures_pending_state(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.36"

        waiter = service.register_aes67_waiter(device_ip)

        packet = self._build_conmon_1007_packet(0x02)
        service._on_packet(packet, (device_ip, 8700))

        assert waiter.is_set()
        result = service.get_aes67_result(device_ip)
        assert result == (False, True)

        service.unregister_aes67_waiter(device_ip)

    def test_waiter_captures_on_to_off_pending(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.108"

        waiter = service.register_aes67_waiter(device_ip)

        packet = self._build_conmon_1007_packet(0x01)
        service._on_packet(packet, (device_ip, 8700))

        assert waiter.is_set()
        result = service.get_aes67_result(device_ip)
        assert result == (True, False)

        service.unregister_aes67_waiter(device_ip)

    def test_no_waiter_no_crash(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)

        packet = self._build_conmon_1007_packet(0x03)
        service._on_packet(packet, ("192.168.1.247", 8700))

    def test_unregister_clears_waiter(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.247"

        service.register_aes67_waiter(device_ip)
        assert device_ip in service._aes67_waiters
        service.unregister_aes67_waiter(device_ip)
        assert device_ip not in service._aes67_waiters
