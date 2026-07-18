import struct
from unittest.mock import MagicMock

from netaudio.dante.services.cmc import DanteCMCService
from netaudio.dante.services.notification import (
    CONMON_OPCODE_INTERFACE_STATUS,
    DanteNotificationService,
    NOTIFICATION_NAMES,
)
from netaudio.dante.services.settings import DanteSettingsService
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEventDispatcher, EventType


SAMPLE_RATE_STATUS_PACKET = bytes.fromhex(
    "ffff004816310000001dc10812580000417564696e6174650724008000000000"
    "001800060000ac4400000000000200000000ac440000bb800001588800017700"
    "0002b1100002ee00"
)


class TestDanteSettingsService:
    def test_instantiation(self):
        service = DanteSettingsService()
        assert service._commands is not None

    def test_identify_not_started(self):
        service = DanteSettingsService()
        service.identify("192.168.1.1")

    def test_probe_sample_rate_sends_typed_command(self):
        service = DanteSettingsService()
        service._commands.command_probe_sample_rate = MagicMock(return_value=(b"probe", None, 8700))
        service.send = MagicMock()

        service.probe_sample_rate("192.168.1.108", host_mac=b"\x10\x20\x30\x40\x50\x60")

        service._commands.command_probe_sample_rate.assert_called_once_with(host_mac=b"\x10\x20\x30\x40\x50\x60")
        service.send.assert_called_once_with(b"probe", "192.168.1.108", 8700)


class TestDanteCMCService:
    def test_instantiation(self):
        service = DanteCMCService()
        assert service._commands is not None

    def test_registration_packet_uses_typed_core_builder(self, monkeypatch):
        service = DanteCMCService()
        monkeypatch.setattr(service, "_host_mac", b"\x00\x1d\xc1\x50\x23\x68")

        packet = service._build_registration_packet(0x1234)

        assert packet == bytes.fromhex("120000141234100100000000001dc15023680000")


class TestDanteNotificationService:
    @staticmethod
    def _write_interface_record(
        packet,
        offset,
        mode,
        mac,
        ip_address,
        netmask,
        first_extra_address,
        second_extra_address,
    ):
        struct.pack_into(">H", packet, offset, mode)
        packet[offset + 2 : offset + 8] = mac
        packet[offset + 8 : offset + 12] = ip_address
        packet[offset + 12 : offset + 16] = netmask
        packet[offset + 16 : offset + 20] = first_extra_address
        packet[offset + 20 : offset + 24] = second_extra_address

    def _build_dual_interface_packet(self, secondary_mac):
        packet = bytearray(0x60)
        struct.pack_into(">H", packet, 0, 0xFFFF)
        struct.pack_into(">H", packet, 2, len(packet))
        packet[0x10:0x18] = b"Audinate"
        struct.pack_into(">H", packet, 0x18, 0x073A)
        struct.pack_into(">H", packet, 0x1A, CONMON_OPCODE_INTERFACE_STATUS)
        struct.pack_into(">H", packet, 0x20, 2)

        self._write_interface_record(
            packet,
            0x28,
            0x0001,
            bytes.fromhex("001DC1000001"),
            bytes((192, 168, 1, 10)),
            bytes((255, 255, 255, 0)),
            bytes((192, 168, 1, 1)),
            bytes((1, 1, 1, 1)),
        )
        packet[0x40:0x44] = bytes.fromhex("DEADBEEF")
        self._write_interface_record(
            packet,
            0x44,
            0x0003,
            secondary_mac,
            bytes((192, 168, 2, 20)),
            bytes((255, 255, 0, 0)),
            bytes((8, 8, 8, 8)),
            bytes((192, 168, 2, 1)),
        )
        packet[0x5C:0x60] = bytes.fromhex("FEEDFACE")
        return bytes(packet)

    def _parse_dual_interface_packet(self, secondary_mac):
        device_ip = "192.168.1.34"
        device = DanteDevice()
        device.ipv4 = device_ip
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip: device if ip == device_ip else None,
        )
        service._on_packet(
            self._build_dual_interface_packet(secondary_mac),
            (device_ip, 1030),
        )
        return device

    def test_instantiation(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        assert service._dispatcher is dispatcher
        assert service._multicast_group == "224.0.0.231"
        assert service._multicast_port == 8702

    def test_notification_names(self):
        assert NOTIFICATION_NAMES[128] == "Sample Rate Status"
        assert NOTIFICATION_NAMES[257] == "TX Channel Change"
        assert NOTIFICATION_NAMES[258] == "RX Channel Change"
        assert NOTIFICATION_NAMES[4103] == "AES67 Status"

    def test_set_device_lookup(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)

        def lookup(ip):
            return None

        service.set_device_lookup(lookup)
        assert service._device_lookup is lookup

    def test_on_packet_short_data(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        service._on_packet(b"\x00" * 10, ("192.168.1.1", 8702))

    def test_dual_interface_status_uses_28_byte_stride(self):
        device = self._parse_dual_interface_packet(bytes.fromhex("001DC1AABBCC"))

        assert device.interfaces == [
            {
                "mode": "dynamic",
                "mac_address": "00:1D:C1:00:00:01",
                "ip_address": "192.168.1.10",
                "netmask": "255.255.255.0",
                "gateway": "192.168.1.1",
                "dns_server": "1.1.1.1",
            },
            {
                "mode": "static",
                "mac_address": "00:1D:C1:AA:BB:CC",
                "ip_address": "192.168.2.20",
                "netmask": "255.255.0.0",
                "dns_server": "8.8.8.8",
                "gateway": "192.168.2.1",
            },
        ]
        assert device.interface_reboot_required is False
        assert device.interface_pending_config is None

    def test_dual_interface_mac_cannot_create_pending_dhcp_state(self):
        device = self._parse_dual_interface_packet(bytes.fromhex("02000004BBCC"))

        assert device.interfaces[1]["mac_address"] == "02:00:00:04:BB:CC"
        assert device.interfaces[1]["ip_address"] == "192.168.2.20"
        assert device.interface_reboot_required is False
        assert device.interface_pending_config is None

    def test_sample_rate_status_updates_device_and_emits_once(self):
        device = DanteDevice(server_name="lx-dante.local.")
        device.name = "lx-dante"
        device.ipv4 = "192.168.1.108"
        dispatcher = MagicMock()
        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip_address: device if ip_address == "192.168.1.108" else None,
        )

        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))

        assert device.sample_rate == 44_100
        assert device.supported_sample_rates == [44_100, 48_000, 88_200, 96_000, 176_400, 192_000]
        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        device_updated_events = [event for event in emitted_events if event.type == EventType.DEVICE_UPDATED]
        notification_events = [event for event in emitted_events if event.type == EventType.NOTIFICATION_RECEIVED]
        assert len(device_updated_events) == 1
        assert device_updated_events[0].server_name == "lx-dante.local."
        assert len(notification_events) == 1
        assert notification_events[0].data["notification_id"] == 128
        assert notification_events[0].data["state_applied"] is True
        assert notification_events[0].data["conmon_response"] is True

        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))
        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        assert sum(event.type == EventType.DEVICE_UPDATED for event in emitted_events) == 1
        assert sum(event.type == EventType.NOTIFICATION_RECEIVED for event in emitted_events) == 2

    def test_sample_rate_status_notifies_registered_waiter(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        waiter = service.register_notification_waiter("192.168.1.108", (128,))

        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))

        assert waiter.event.is_set()
        assert waiter.notification_id == 128

    def test_sample_rate_status_returns_typed_probe_result(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        waiter = service.register_sample_rate_waiter("192.168.1.108")

        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))

        assert waiter.is_set()
        assert service.get_sample_rate_result("192.168.1.108") == (
            44_100,
            [44_100, 48_000, 88_200, 96_000, 176_400, 192_000],
        )
        service.unregister_sample_rate_waiter("192.168.1.108")

    def test_sample_rate_status_does_not_repopulate_offline_device(self):
        device = DanteDevice(server_name="lx-dante.local.")
        device.ipv4 = "192.168.1.108"
        device.online = False
        service = DanteNotificationService(
            dispatcher=MagicMock(),
            device_lookup=lambda ip_address: device if ip_address == "192.168.1.108" else None,
        )

        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))

        assert device.sample_rate is None
        assert device.supported_sample_rates is None

    def test_sample_rate_status_is_applied_when_device_appears(self):
        dispatcher = MagicMock()
        service = DanteNotificationService(dispatcher=dispatcher)
        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))
        device = DanteDevice(server_name="lx-dante.local.")
        device.ipv4 = "192.168.1.108"

        service.apply_pending_for_device(device)

        assert device.sample_rate == 44_100
        assert device.supported_sample_rates == [44_100, 48_000, 88_200, 96_000, 176_400, 192_000]
        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        assert all(event.type != EventType.DEVICE_UPDATED for event in emitted_events)


class TestHeartbeatLockStateParsing:
    def test_locked_device(self):
        from netaudio.dante.services.heartbeat import _parse_lock_state

        payload = bytes.fromhex(
            "fffe00b82d8c0000001dc1fffe5279b6"
            "4175646963617465000800011000000000"
            "1c800100040010"
            "2c2400"
            "00ffff"
            "a27600"
            "000000"
            "000000"
            "000000"
            "000000"
            "00"
        )
        payload = bytes.fromhex("fffe00b82d8c0000001dc1fffe5279b641756469636e617465000800011000000000")
        locked_block = bytes.fromhex("001c8002000400102c240000000200000001000000180000fefe3400")
        header = b"\xff\xfe\x00\xb8" + b"\x00" * 28
        result = _parse_lock_state(header + locked_block)
        assert result is True

    def test_unlocked_device(self):
        from netaudio.dante.services.heartbeat import _parse_lock_state

        unlocked_block = bytes.fromhex("001c8002000400101b6d0000000200000002000000180000fefe7c7c")
        header = b"\xff\xfe\x00\x54" + b"\x00" * 28
        result = _parse_lock_state(header + unlocked_block)
        assert result is False

    def test_no_lock_block(self):
        from netaudio.dante.services.heartbeat import _parse_lock_state

        other_block = bytes.fromhex("001080010004000428360000ffff8a74")
        header = b"\xff\xfe\x00\x54" + b"\x00" * 28
        result = _parse_lock_state(header + other_block)
        assert result is None

    def test_short_packet(self):
        from netaudio.dante.services.heartbeat import _parse_lock_state

        result = _parse_lock_state(b"\x00" * 10)
        assert result is None


class TestKeyExtraction:
    def test_table_pattern_sequential(self):
        from netaudio.common.key_extract import _is_table_pattern

        sequential = bytes(range(32))
        assert _is_table_pattern(sequential) is True

    def test_table_pattern_stride2(self):
        from netaudio.common.key_extract import _is_table_pattern

        stride2 = bytes([i for i in range(0, 64, 2)])
        assert _is_table_pattern(stride2) is True

    def test_high_entropy_not_table(self):
        from netaudio.common.key_extract import _is_table_pattern
        import os

        random_key = os.urandom(32)
        assert _is_table_pattern(random_key) is False

    def test_extract_from_nonexistent(self):
        from pathlib import Path
        from netaudio.common.key_extract import extract_key_from_binary

        result = extract_key_from_binary(Path("/nonexistent/file"))
        assert result is None
