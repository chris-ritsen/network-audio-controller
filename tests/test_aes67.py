import struct

import pytest

from netaudio import core
from netaudio.commands.config import cli as config_cli
from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.events import DanteEventDispatcher, EventType
from netaudio.dante.services.notification import (
    DanteNotificationService,
)
from netaudio.dante.services.notification_packet_handlers import (
    STATUS_KIND_AES67,
    _parse_aes67_current_new,
)


class TestAES67ConfiguredFromARC1100:
    def _build_latency_config_response(self, records):
        response = bytearray(12 + len(records) * 4)
        struct.pack_into(">H", response, 0, 0x2809)
        struct.pack_into(">H", response, 2, len(response))
        struct.pack_into(">H", response, 6, 0x1100)
        struct.pack_into(">H", response, 8, 0x0001)
        response[11] = len(records)
        for record_index, (info_code, inline_value) in enumerate(records):
            struct.pack_into(">HH", response, 12 + record_index * 4, info_code, inline_value)
        return bytes(response)

    def test_property_identity_survives_record_reordering(self):
        enabled = self._build_latency_config_response([(0x0211, 4), (0x0063, 3), (0x0310, 4)])
        disabled = self._build_latency_config_response([(0x0063, 1), (0x0211, 4)])

        assert core.parse_response("aes67_configured", enabled) is True
        assert core.parse_response("aes67_configured", disabled) is False

    def test_zero_placeholder_reports_unavailable(self):
        response = self._build_latency_config_response([(0x0000, 0x0063), (0x0211, 4)])

        assert core.parse_response("aes67_configured", response) is None

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
    def _build_conmon_1007_packet(self, state_byte):
        packet = bytearray(36)
        struct.pack_into(">H", packet, 0, 0xFFFF)
        struct.pack_into(">H", packet, 2, 36)
        packet[0x10:0x18] = b"Audinate"
        packet[0x18] = 0x07
        struct.pack_into(">H", packet, 0x1A, 0x1007)
        packet[0x21] = state_byte
        return bytes(packet)

    def _parse(self, state_byte):
        return core.parse_response("aes67_status", self._build_conmon_1007_packet(state_byte))

    def test_0x00_means_disabled_disabled(self):
        assert self._parse(0x00) == {"aes67_configured": False, "aes67_current": False}

    def test_0x02_means_disabled_enabled(self):
        assert self._parse(0x02) == {"aes67_configured": True, "aes67_current": False}

    def test_0x03_means_enabled_enabled(self):
        assert self._parse(0x03) == {"aes67_configured": True, "aes67_current": True}

    def test_0x01_means_enabled_disabled(self):
        assert self._parse(0x01) == {"aes67_configured": False, "aes67_current": True}

    def test_unknown_byte_is_rejected(self):
        with pytest.raises(core.NetaudioCoreError):
            self._parse(0xFF)

    def test_packet_handler_returns_status_and_waiter_result(self):
        parsed = _parse_aes67_current_new(self._build_conmon_1007_packet(0x02), "192.0.2.10", None)
        assert parsed.kind == STATUS_KIND_AES67
        assert parsed.status == {"aes67_configured": True, "aes67_current": False}
        assert parsed.waiter_result == (False, True)

    def test_packet_handler_rejects_unknown_byte(self):
        assert _parse_aes67_current_new(self._build_conmon_1007_packet(0xFF), "192.0.2.10", None) is None


@pytest.mark.parametrize(
    ("current", "configured", "expected"),
    [
        (False, True, True),
        (True, False, True),
        (False, False, False),
        (True, True, False),
        (None, True, False),
        (True, None, False),
        (None, None, False),
    ],
)
def test_aes67_reboot_required(current, configured, expected):
    device = DanteDevice()
    device.aes67_current = current
    device.aes67_configured = configured
    assert config_cli._aes67_reboot_required(device) is expected


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
        magic_offset = packet.find(b"Audinate")
        assert magic_offset == 0x10
        message_type = struct.unpack(">H", packet[0x1A:0x1C])[0]
        assert message_type == 0x1006

    def test_probe_has_zero_presence_and_enable_flags(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_probe_aes67()
        assert packet[32:34] == b"\x00\x00"
        assert packet[34:36] == b"\x00\x00"

    def test_probe_has_nonzero_sequence(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_probe_aes67()
        sequence = struct.unpack(">H", packet[4:6])[0]
        assert sequence != 0

    def test_probe_with_custom_mac(self):
        commands = DanteDeviceCommands()
        mac = b"\xaa\xbb\xcc\xdd\xee\xff"
        packet, _, _ = commands.command_probe_aes67(host_mac=mac)
        assert packet[8:14] == mac

    def test_probe_differs_from_enable(self):
        commands = DanteDeviceCommands()
        probe, _, _ = commands.command_probe_aes67()
        enable, _, _ = commands.command_enable_aes67(True)
        assert probe[32:36] == b"\x00\x00\x00\x00"
        assert enable[32:36] == b"\x00\x01\x00\x01"


class TestAES67Waiter:
    def _build_conmon_1007_packet(self, state_byte, source_eui64=None):
        packet = bytearray(36)
        struct.pack_into(">H", packet, 0, 0xFFFF)
        struct.pack_into(">H", packet, 2, 36)
        if source_eui64:
            packet[8:16] = source_eui64
        packet[0x10:0x18] = b"Audinate"
        struct.pack_into(">H", packet, 0x18, 0x073A)
        struct.pack_into(">H", packet, 0x1A, 0x1007)
        packet[0x21] = state_byte
        return bytes(packet)

    def test_register_and_notify_waiter(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.247"

        waiter = service.register_waiter("aes67", device_ip)
        assert not waiter.is_set()

        packet = self._build_conmon_1007_packet(0x03)
        service._on_packet(packet, (device_ip, 8700))

        assert waiter.is_set()
        assert waiter.latest_result == (True, True)

        service.unregister_waiter(waiter)

    def test_waiter_captures_pending_state(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.36"

        waiter = service.register_waiter("aes67", device_ip)

        packet = self._build_conmon_1007_packet(0x02)
        service._on_packet(packet, (device_ip, 8700))

        assert waiter.is_set()
        assert waiter.latest_result == (False, True)

        service.unregister_waiter(waiter)

    def test_waiter_captures_on_to_off_pending(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.108"

        waiter = service.register_waiter("aes67", device_ip)

        packet = self._build_conmon_1007_packet(0x01)
        service._on_packet(packet, (device_ip, 8700))

        assert waiter.is_set()
        assert waiter.latest_result == (True, False)

        service.unregister_waiter(waiter)

    def test_unsolicited_status_is_published_without_a_waiter(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)

        packet = self._build_conmon_1007_packet(0x03)
        service._on_packet(packet, ("192.168.1.247", 8700))

        [event] = dispatcher._pending_events
        assert event.type is EventType.DEVICE_STATUS_RECEIVED
        assert event.data == {
            "kind": "aes67",
            "notification_id": 0x1007,
            "raw": packet,
            "source_ip": "192.168.1.247",
            "status": {"aes67_current": True, "aes67_configured": True},
        }

    def test_unregister_clears_waiter(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.247"

        waiter = service.register_waiter("aes67", device_ip)
        assert service.is_waiting("aes67", device_ip)
        service.unregister_waiter(waiter)
        assert not service.is_waiting("aes67", device_ip)
