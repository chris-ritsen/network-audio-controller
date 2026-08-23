import struct
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.dante.services.cmc import DanteCMCService
from netaudio.dante.services.notification import (
    CONMON_OPCODE_GAIN_STATUS,
    CONMON_OPCODE_INTERFACE_STATUS,
    CONMON_OPCODE_ROUTING_CAPACITY_STATUS,
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

ENCODING_STATUS_PACKET = bytes.fromhex(
    "ffff003413870000001dc10812580000417564696e61746507240082000000000018000100000018000000000000000000000018"
)

ROUTING_CAPACITY_READY_PACKET = bytes.fromhex(
    "ffff002812870000001dc10812580000417564696e61746507240100000000000101000000800080"
)

ROUTING_CAPACITY_TRANSITION_PACKET = bytes.fromhex(
    "ffff002812870000001dc10812580000417564696e61746507240100000000000001000000000000"
)

SAMPLE_RATE_PULLUP_STATUS_PACKET = bytes.fromhex(
    "ffff005c002200000200000000010000417564696e6174650724008400000000"
    "0030000500000001000000010002000000000001000000000000000000000000"
    "00000000000000000000000000000001000000020000000300000004"
)

INPUT_GAIN_STATUS_PACKET = bytes.fromhex(
    "ffff003806110000001dc1fffe50692e417564696e6174650727100b00000000000000010008001001020002000400180000000500000001"
)

OUTPUT_GAIN_STATUS_PACKET = bytes.fromhex(
    "ffff003808100000001dc1fffe507b8d417564696e6174650727100b00000000000000010008001002010002000400180000000400000004"
)

LIVE_AVIO_INPUT_GAIN_STATUS_PACKET = bytes.fromhex(
    "ffff0038eee50000001dc1fffe50692e417564696e6174650738100b00000000000000010008001001020002000400180000000400000004"
)

LIVE_AVIO_OUTPUT_GAIN_STATUS_PACKET = bytes.fromhex(
    "ffff0038efb00000001dc1fffe507b8d417564696e6174650738100b00000000000000010008001002010002000400180000000400000004"
)

AVIO_APPLIED_DHCP_INTERFACE_STATUS_PACKET = bytes.fromhex(
    "ffff006c05920000001dc1fffe50692e417564696e6174650727001100000000"
    "00010000000000640001001dc150692ec0a8018bffffff00c0a80101c0a80101"
    "0018003000000000000400000000000000000000000000000000000000480000"
    "6c6f63616c646f6d61696e00"
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

    def test_refresh_clock_status_sends_typed_command(self):
        service = DanteSettingsService()
        service._commands.command_refresh_clock_status = MagicMock(return_value=(b"refresh", None, 8700))
        service.send = MagicMock()

        service.refresh_clock_status(
            "192.168.1.108",
            host_mac=b"\x10\x20\x30\x40\x50\x60",
            sequence=0x0021,
        )

        service._commands.command_refresh_clock_status.assert_called_once_with(
            host_mac=b"\x10\x20\x30\x40\x50\x60",
            sequence=0x0021,
        )
        service.send.assert_called_once_with(b"refresh", "192.168.1.108", 8700)

    def test_probe_encoding_sends_typed_command(self):
        service = DanteSettingsService()
        service._commands.command_probe_encoding = MagicMock(return_value=(b"probe", None, 8700))
        service.send = MagicMock()

        service.probe_encoding("192.168.1.108", host_mac=b"\x10\x20\x30\x40\x50\x60")

        service._commands.command_probe_encoding.assert_called_once_with(host_mac=b"\x10\x20\x30\x40\x50\x60")
        service.send.assert_called_once_with(b"probe", "192.168.1.108", 8700)

    def test_sample_rate_pullup_commands_send_typed_packets(self):
        service = DanteSettingsService()
        service._commands.command_probe_sample_rate_pullup = MagicMock(return_value=(b"probe", None, 8700))
        service._commands.command_set_sample_rate_pullup = MagicMock(return_value=(b"write", None, 8700))
        service.send = MagicMock()
        host_mac = b"\x10\x20\x30\x40\x50\x60"

        service.probe_sample_rate_pullup("192.168.1.108", host_mac=host_mac)
        service.set_sample_rate_pullup("192.168.1.108", 4, host_mac=host_mac)

        service._commands.command_probe_sample_rate_pullup.assert_called_once_with(host_mac=host_mac)
        service._commands.command_set_sample_rate_pullup.assert_called_once_with(4, host_mac=host_mac)
        assert service.send.call_args_list[0].args == (b"probe", "192.168.1.108", 8700)
        assert service.send.call_args_list[1].args == (b"write", "192.168.1.108", 8700)

    def test_probe_lock_reset_status_sends_typed_command(self):
        service = DanteSettingsService()
        service._commands.command_probe_lock_reset_status = MagicMock(return_value=(b"probe", None, 8700))
        service.send = MagicMock()

        service.probe_lock_reset_status(
            "192.168.1.108",
            host_mac=b"\x10\x20\x30\x40\x50\x60",
            request_value=100,
        )

        service._commands.command_probe_lock_reset_status.assert_called_once_with(
            host_mac=b"\x10\x20\x30\x40\x50\x60",
            request_value=100,
        )
        service.send.assert_called_once_with(b"probe", "192.168.1.108", 8700)

    def test_probe_gain_sends_typed_command(self):
        service = DanteSettingsService()
        service._commands.command_probe_gain_level = MagicMock(return_value=(b"probe", None, 8700))
        service.send = MagicMock()

        service.probe_gain_level("192.168.1.108", host_mac=b"\x10\x20\x30\x40\x50\x60")

        service._commands.command_probe_gain_level.assert_called_once_with(host_mac=b"\x10\x20\x30\x40\x50\x60")
        service.send.assert_called_once_with(b"probe", "192.168.1.108", 8700)

    def test_set_gain_sends_typed_command(self):
        service = DanteSettingsService()
        service._commands.command_set_gain_level = MagicMock(return_value=(b"set", None, 8700))
        service.send = MagicMock()

        service.set_gain_level(
            "192.168.1.108",
            2,
            5,
            "output",
            host_mac=b"\x10\x20\x30\x40\x50\x60",
        )

        service._commands.command_set_gain_level.assert_called_once_with(
            2,
            5,
            "output",
            host_mac=b"\x10\x20\x30\x40\x50\x60",
        )
        service.send.assert_called_once_with(b"set", "192.168.1.108", 8700)


class TestDanteCMCService:
    def test_instantiation(self):
        service = DanteCMCService()
        assert service._commands is not None

    def test_exposes_host_address_used_by_cmc_commands(self):
        host_address = b"\x00\x1d\xc1\x50\x23\x68"
        service = DanteCMCService(host_media_access_control_address=host_address)

        assert service.host_media_access_control_address == host_address

    def test_registration_packet_uses_typed_core_builder(self, monkeypatch):
        service = DanteCMCService(host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")

        packet = service._build_registration_packet(0x1234)

        assert packet == bytes.fromhex("120000141234100100000000001dc15023680000")

    @pytest.mark.asyncio
    async def test_registration_requires_matching_success_response(self):
        service = DanteCMCService(host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")
        successful_response = bytes.fromhex("120000200000100100010000020000000001000000010000c0a8013d21fc0000")
        service.request = AsyncMock(return_value=successful_response)

        response = await service.register_device("192.168.1.61")

        assert response == successful_response
        assert service._registered_devices == {"192.168.1.61"}

    @pytest.mark.asyncio
    async def test_registration_rejects_failure_response(self):
        service = DanteCMCService(host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")
        failed_response = bytes.fromhex("120000200000100100000000020000000001000000010000c0a8013d21fc0000")
        service.request = AsyncMock(return_value=failed_response)

        response = await service.register_device("192.168.1.61")

        assert response is None
        assert service._registered_devices == set()

    @pytest.mark.asyncio
    async def test_registration_rejects_mismatched_sequence_and_malformed_envelope(self):
        service = DanteCMCService(host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")
        service._sequence_counter = 0x1234
        mismatched_sequence = bytes.fromhex("120000200000100100010000020000000001000000010000c0a8013d21fc0000")
        service.request = AsyncMock(return_value=mismatched_sequence)

        assert await service.register_device("192.168.1.61") is None
        assert service._registered_devices == set()

        malformed_length = bytes.fromhex("1200001f1235100100010000020000000001000000010000c0a8013d21fc0000")
        service.request = AsyncMock(return_value=malformed_length)

        assert await service.register_device("192.168.1.61") is None
        assert service._registered_devices == set()

    @pytest.mark.asyncio
    async def test_required_registration_fails_loudly_on_timeout(self):
        service = DanteCMCService(host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")
        service.request = AsyncMock(return_value=None)

        with pytest.raises(RuntimeError, match="CMC registration failed for 192.168.1.61"):
            await service.require_registration("192.168.1.61")


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
        struct.pack_into(">I", packet, 0x24, 1000)

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
        assert NOTIFICATION_NAMES[130] == "Encoding Status"
        assert NOTIFICATION_NAMES[257] == "TX Channel Change"
        assert NOTIFICATION_NAMES[258] == "RX Channel Change"
        assert NOTIFICATION_NAMES[4103] == "AES67 Status"
        assert NOTIFICATION_NAMES[CONMON_OPCODE_GAIN_STATUS] == "Gain Status"

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

    def test_routing_capacity_ready_updates_capacity_and_active_counts(self):
        device = DanteDevice(server_name="lx-dante.local.")
        device.name = "lx-dante"
        device.ipv4 = "192.168.1.108"
        dispatcher = MagicMock()
        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip_address: device if ip_address == "192.168.1.108" else None,
        )

        service._on_packet(ROUTING_CAPACITY_READY_PACKET, ("192.168.1.108", 8702))

        assert device.routing_ready is True
        assert device.routing_ready_state_code == 0x0101
        assert device.routing_capacity_transmit_channel_count == 128
        assert device.routing_capacity_receive_channel_count == 128
        assert device.tx_count == device.tx_count_raw == 128
        assert device.rx_count == device.rx_count_raw == 128
        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        assert sum(event.type == EventType.DEVICE_UPDATED for event in emitted_events) == 1
        notification_event = next(event for event in emitted_events if event.type == EventType.NOTIFICATION_RECEIVED)
        assert notification_event.data["notification_id"] == CONMON_OPCODE_ROUTING_CAPACITY_STATUS
        assert notification_event.data["state_applied"] is True
        assert notification_event.data["conmon_response"] is True

    def test_routing_capacity_transition_preserves_active_counts(self):
        device = DanteDevice(server_name="a32.local.")
        device.name = "A32"
        device.ipv4 = "10.0.2.15"
        device.routing_ready = True
        device.routing_ready_state_code = 0x0101
        device.routing_capacity_transmit_channel_count = 64
        device.routing_capacity_receive_channel_count = 64
        device.tx_count = device.tx_count_raw = 64
        device.rx_count = device.rx_count_raw = 64
        dispatcher = MagicMock()
        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip_address: device if ip_address == "10.0.2.15" else None,
        )

        service._on_packet(ROUTING_CAPACITY_TRANSITION_PACKET, ("10.0.2.15", 8702))

        assert device.routing_ready is False
        assert device.routing_ready_state_code == 0x0001
        assert device.routing_capacity_transmit_channel_count == 0
        assert device.routing_capacity_receive_channel_count == 0
        assert device.tx_count == device.tx_count_raw == 64
        assert device.rx_count == device.rx_count_raw == 64
        notification_event = next(
            call.args[0]
            for call in dispatcher.emit_nowait.call_args_list
            if call.args[0].type == EventType.NOTIFICATION_RECEIVED
        )
        assert notification_event.data["state_applied"] is True
        assert notification_event.data["conmon_response"] is True

    def test_routing_capacity_ready_preserves_inventory_counts(self):
        device = DanteDevice(server_name="ad4d.local.")
        device.name = "ad4d"
        device.ipv4 = "192.168.1.108"
        device.tx_count = 2
        device.tx_count_raw = 64
        device.rx_count = 1
        device.rx_count_raw = 1
        dispatcher = MagicMock()
        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip_address: device if ip_address == "192.168.1.108" else None,
        )

        service._on_packet(ROUTING_CAPACITY_READY_PACKET, ("192.168.1.108", 8702))

        assert device.routing_capacity_transmit_channel_count == 128
        assert device.routing_capacity_receive_channel_count == 128
        assert device.tx_count == 2
        assert device.tx_count_raw == 64
        assert device.rx_count == device.rx_count_raw == 1

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
        assert device.link_speed_mbps == 1000

    def test_dual_interface_mac_cannot_create_pending_dhcp_state(self):
        device = self._parse_dual_interface_packet(bytes.fromhex("02000004BBCC"))

        assert device.interfaces[1]["mac_address"] == "02:00:00:04:BB:CC"
        assert device.interfaces[1]["ip_address"] == "192.168.2.20"
        assert device.interface_reboot_required is False
        assert device.interface_pending_config is None

    def test_applied_dhcp_target_clears_stale_avio_flag(self):
        device_ip = "192.168.1.139"
        device = DanteDevice(server_name="AVIOAI2-50692e.local.")
        device.ipv4 = device_ip
        service = DanteNotificationService(
            dispatcher=DanteEventDispatcher(),
            device_lookup=lambda ip_address: device if ip_address == device_ip else None,
        )

        service._on_packet(AVIO_APPLIED_DHCP_INTERFACE_STATUS_PACKET, (device_ip, 8700))

        assert device.interfaces[0]["mode"] == "dynamic"
        assert device.interfaces[0]["ip_address"] == device_ip
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
        assert notification_events[0].data["current_value_changed"] is False
        assert notification_events[0].data["supported_values_changed"] is True

        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))
        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        assert sum(event.type == EventType.DEVICE_UPDATED for event in emitted_events) == 1
        assert sum(event.type == EventType.NOTIFICATION_RECEIVED for event in emitted_events) == 2
        latest_notification = [event for event in emitted_events if event.type == EventType.NOTIFICATION_RECEIVED][-1]
        assert latest_notification.data["state_applied"] is False
        assert latest_notification.data["current_value_changed"] is False
        assert latest_notification.data["supported_values_changed"] is False

    def test_sample_rate_value_change_defers_device_update_for_settings_refresh(self):
        device = DanteDevice(server_name="lx-dante.local.")
        device.name = "lx-dante"
        device.ipv4 = "192.168.1.108"
        device.sample_rate = 48_000
        device.supported_sample_rates = [44_100, 48_000, 88_200, 96_000, 176_400, 192_000]
        dispatcher = MagicMock()
        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip_address: device if ip_address == "192.168.1.108" else None,
        )

        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))

        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        assert all(event.type != EventType.DEVICE_UPDATED for event in emitted_events)
        notification_event = next(event for event in emitted_events if event.type == EventType.NOTIFICATION_RECEIVED)
        assert notification_event.data["state_applied"] is True
        assert notification_event.data["current_value_changed"] is True
        assert notification_event.data["supported_values_changed"] is False

    def test_sample_rate_supported_list_change_emits_without_settings_refresh(self):
        device = DanteDevice(server_name="lx-dante.local.")
        device.name = "lx-dante"
        device.ipv4 = "192.168.1.108"
        device.sample_rate = 44_100
        device.supported_sample_rates = [44_100]
        dispatcher = MagicMock()
        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip_address: device if ip_address == "192.168.1.108" else None,
        )

        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))

        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        assert sum(event.type == EventType.DEVICE_UPDATED for event in emitted_events) == 1
        notification_event = next(event for event in emitted_events if event.type == EventType.NOTIFICATION_RECEIVED)
        assert notification_event.data["state_applied"] is True
        assert notification_event.data["current_value_changed"] is False
        assert notification_event.data["supported_values_changed"] is True

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

    def test_encoding_status_updates_device_and_emits_once(self):
        device = DanteDevice(server_name="lx-dante.local.")
        device.name = "lx-dante"
        device.ipv4 = "192.168.1.108"
        dispatcher = MagicMock()
        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip_address: device if ip_address == "192.168.1.108" else None,
        )

        service._on_packet(ENCODING_STATUS_PACKET, ("192.168.1.108", 1034))

        assert device.encoding == 24
        assert device.supported_encodings == [24]
        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        device_updated_events = [event for event in emitted_events if event.type == EventType.DEVICE_UPDATED]
        notification_events = [event for event in emitted_events if event.type == EventType.NOTIFICATION_RECEIVED]
        assert len(device_updated_events) == 1
        assert device_updated_events[0].server_name == "lx-dante.local."
        assert len(notification_events) == 1
        assert notification_events[0].data["notification_id"] == 130
        assert notification_events[0].data["state_applied"] is True
        assert notification_events[0].data["conmon_response"] is True

        service._on_packet(ENCODING_STATUS_PACKET, ("192.168.1.108", 1034))
        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        assert sum(event.type == EventType.DEVICE_UPDATED for event in emitted_events) == 1
        assert sum(event.type == EventType.NOTIFICATION_RECEIVED for event in emitted_events) == 2

    def test_sample_rate_pullup_status_updates_device_and_waiter(self):
        device = DanteDevice(server_name="a32.local.")
        device.name = "A32"
        device.ipv4 = "10.0.2.15"
        dispatcher = MagicMock()
        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip_address: device if ip_address == "10.0.2.15" else None,
        )
        waiter = service.register_sample_rate_pullup_waiter("10.0.2.15")

        service._on_packet(SAMPLE_RATE_PULLUP_STATUS_PACKET, ("10.0.2.15", 8702))

        assert waiter.is_set()
        assert service.get_sample_rate_pullup_result("10.0.2.15") == (1, [0, 1, 2, 3, 4])
        assert device.sample_rate_pullup_raw_value == 1
        assert device.requested_sample_rate_pullup_raw_value == 1
        assert device.supported_sample_rate_pullup_raw_values == [0, 1, 2, 3, 4]
        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        assert sum(event.type == EventType.DEVICE_UPDATED for event in emitted_events) == 1
        notification_event = next(event for event in emitted_events if event.type == EventType.NOTIFICATION_RECEIVED)
        assert notification_event.data["notification_id"] == 132
        assert notification_event.data["state_applied"] is True
        service.unregister_sample_rate_pullup_waiter("10.0.2.15")

    def test_encoding_status_returns_device_scoped_typed_probe_result(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        matching_waiter = service.register_encoding_waiter("192.168.1.108")
        unrelated_waiter = service.register_encoding_waiter("192.168.1.109")

        service._on_packet(ENCODING_STATUS_PACKET, ("192.168.1.108", 1034))

        assert matching_waiter.is_set()
        assert not unrelated_waiter.is_set()
        assert service.get_encoding_result("192.168.1.108") == (24, [24])
        assert service.get_encoding_result("192.168.1.109") is None
        service.unregister_encoding_waiter("192.168.1.108")
        service.unregister_encoding_waiter("192.168.1.109")

    def test_encoding_status_does_not_repopulate_offline_device(self):
        device = DanteDevice(server_name="lx-dante.local.")
        device.ipv4 = "192.168.1.108"
        device.online = False
        service = DanteNotificationService(
            dispatcher=MagicMock(),
            device_lookup=lambda ip_address: device if ip_address == "192.168.1.108" else None,
        )

        service._on_packet(ENCODING_STATUS_PACKET, ("192.168.1.108", 1034))

        assert device.encoding is None
        assert device.supported_encodings is None

    def test_encoding_status_is_applied_when_device_appears(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        service._on_packet(ENCODING_STATUS_PACKET, ("192.168.1.108", 1034))
        device = DanteDevice(server_name="lx-dante.local.")
        device.ipv4 = "192.168.1.108"

        service.apply_pending_for_device(device)

        assert device.encoding == 24
        assert device.supported_encodings == [24]

    def test_live_avio_gain_status_parses_when_unmapped_header_byte_changes(self):
        from netaudio import core

        assert core.parse_response("gain_status", LIVE_AVIO_INPUT_GAIN_STATUS_PACKET) == {
            "device_type": "input",
            "channel_levels": [4, 4],
        }
        assert core.parse_response("gain_status", LIVE_AVIO_OUTPUT_GAIN_STATUS_PACKET) == {
            "device_type": "output",
            "channel_levels": [4, 4],
        }

    def test_input_gain_status_updates_device_and_exposes_protocol_levels(self):
        device = DanteDevice(server_name="avio-input.local.")
        device.name = "avio-input"
        device.ipv4 = "192.168.1.108"
        dispatcher = MagicMock()
        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip_address: device if ip_address == "192.168.1.108" else None,
        )

        service._on_packet(INPUT_GAIN_STATUS_PACKET, ("192.168.1.108", 8700))

        assert device.gain_device_type == "input"
        assert device.gain_levels == [5, 1]
        assert device.supported_gain_levels == [1, 2, 3, 4, 5]
        assert device.gain_level_choices == [
            {"value": 1, "label": "+24 dBu"},
            {"value": 2, "label": "+4 dBu"},
            {"value": 3, "label": "0 dBu"},
            {"value": 4, "label": "0 dBV"},
            {"value": 5, "label": "-10 dBV"},
        ]
        emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
        assert sum(event.type == EventType.DEVICE_UPDATED for event in emitted_events) == 1
        notification_event = next(event for event in emitted_events if event.type == EventType.NOTIFICATION_RECEIVED)
        assert notification_event.data["notification_id"] == CONMON_OPCODE_GAIN_STATUS
        assert notification_event.data["state_applied"] is True

    def test_output_gain_status_notifies_only_matching_device_waiters(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        matching_waiter = service.register_gain_status_waiter(
            "192.168.1.108",
            channel_number=2,
            expected_level=4,
        )
        unrelated_waiter = service.register_gain_status_waiter(
            "192.168.1.109",
            channel_number=2,
            expected_level=4,
        )

        service._on_packet(OUTPUT_GAIN_STATUS_PACKET, ("192.168.1.108", 8700))

        assert matching_waiter.event.is_set()
        assert matching_waiter.latest_result == ("output", [4, 4])
        assert not unrelated_waiter.event.is_set()

    def test_gain_write_waiter_ignores_nonmatching_level_but_retains_readback(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        waiter = service.register_gain_status_waiter(
            "192.168.1.108",
            channel_number=1,
            expected_level=3,
        )

        service._on_packet(INPUT_GAIN_STATUS_PACKET, ("192.168.1.108", 8700))

        assert not waiter.event.is_set()
        assert waiter.latest_result == ("input", [5, 1])

    def test_gain_write_waiter_ignores_nonmatching_direction(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        waiter = service.register_gain_status_waiter(
            "192.168.1.108",
            expected_device_type="output",
            channel_number=1,
            expected_level=5,
        )

        service._on_packet(INPUT_GAIN_STATUS_PACKET, ("192.168.1.108", 8700))

        assert not waiter.event.is_set()
        assert waiter.latest_result == ("input", [5, 1])

    def test_gain_status_is_applied_when_device_appears(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        service._on_packet(INPUT_GAIN_STATUS_PACKET, ("192.168.1.108", 8700))
        device = DanteDevice(server_name="avio-input.local.")
        device.ipv4 = "192.168.1.108"

        service.apply_pending_for_device(device)

        assert device.gain_device_type == "input"
        assert device.gain_levels == [5, 1]
        assert device.supported_gain_levels == [1, 2, 3, 4, 5]


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
