import struct
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.dante.services.cmc import DanteCMCService
from netaudio.dante.const import (
    CONMON_OPCODE_GAIN_STATUS,
    CONMON_OPCODE_INTERFACE_STATUS,
    CONMON_OPCODE_ROUTING_CAPACITY_STATUS,
)
from netaudio.dante.services.notification import (
    DanteNotificationService,
)
from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEventDispatcher, EventType
from netaudio.dante.services.notification import _gain_status_accepts
from tests.status_test_support import (
    application_with_device,
    count_events,
    receive_packets,
    status_events,
)


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


def _recording_transport():
    transport = SimpleNamespace(execute=AsyncMock(return_value=None))
    return transport


def _executed(transport):
    return [(call.args[0], call.args[1]) for call in transport.execute.await_args_list]


class TestApplicationSettingsCommands:
    @pytest.mark.asyncio
    async def test_identify_executes_typed_command(self):
        transport = _recording_transport()
        application = DanteApplication()
        application.transport = transport

        await application.send_identify("192.168.1.1")

        [(address, specification)] = _executed(transport)
        assert address == "192.168.1.1"
        assert specification["command"] == "identify"
        assert 1 <= specification["sequence"] <= 0xFFFF

    @pytest.mark.asyncio
    async def test_probe_sample_rate_executes_typed_command(self):
        transport = _recording_transport()
        application = DanteApplication()
        application.transport = transport

        await application.send_probe_sample_rate("192.168.1.108", host_mac=b"\x10\x20\x30\x40\x50\x60")

        assert _executed(transport) == [("192.168.1.108", {"command": "probe_sample_rate", "host_mac": "102030405060"})]

    @pytest.mark.asyncio
    async def test_refresh_clock_status_executes_typed_command(self):
        transport = _recording_transport()
        application = DanteApplication()
        application.transport = transport

        await application.send_refresh_clock_status(
            "192.168.1.108",
            host_mac=b"\x10\x20\x30\x40\x50\x60",
            sequence=0x0021,
        )

        assert _executed(transport) == [
            (
                "192.168.1.108",
                {"command": "refresh_clock_status", "host_mac": "102030405060", "sequence": 0x0021},
            )
        ]

    @pytest.mark.asyncio
    async def test_probe_encoding_executes_typed_command(self):
        transport = _recording_transport()
        application = DanteApplication()
        application.transport = transport

        await application.send_probe_encoding("192.168.1.108", host_mac=b"\x10\x20\x30\x40\x50\x60")

        assert _executed(transport) == [("192.168.1.108", {"command": "probe_encoding", "host_mac": "102030405060"})]

    @pytest.mark.asyncio
    async def test_sample_rate_pullup_commands_execute_typed_specifications(self):
        transport = _recording_transport()
        application = DanteApplication()
        application.transport = transport
        host_mac = b"\x10\x20\x30\x40\x50\x60"

        await application.send_probe_sample_rate_pullup("192.168.1.108", host_mac=host_mac)
        await application.send_set_sample_rate_pullup("192.168.1.108", 4, host_mac=host_mac)

        executed = _executed(transport)
        assert executed[0] == ("192.168.1.108", {"command": "probe_sample_rate_pullup", "host_mac": "102030405060"})
        address, write_specification = executed[1]
        assert address == "192.168.1.108"
        assert write_specification["command"] == "set_sample_rate_pullup"
        assert write_specification["raw_value"] == 4
        assert write_specification["host_mac"] == "102030405060"
        assert "sequence" in write_specification

    @pytest.mark.asyncio
    async def test_probe_lock_reset_status_executes_typed_command(self):
        transport = _recording_transport()
        application = DanteApplication()
        application.transport = transport

        await application.send_probe_lock_reset_status(
            "192.168.1.108",
            host_mac=b"\x10\x20\x30\x40\x50\x60",
            request_value=100,
        )

        [(address, specification)] = _executed(transport)
        assert address == "192.168.1.108"
        assert specification["command"] == "probe_lock_reset_status"
        assert specification["request_value"] == 100
        assert specification["host_mac"] == "102030405060"

    @pytest.mark.asyncio
    async def test_probe_gain_executes_typed_command(self):
        transport = _recording_transport()
        application = DanteApplication()
        application.transport = transport

        await application.send_probe_gain_level("192.168.1.108", host_mac=b"\x10\x20\x30\x40\x50\x60")

        [(address, specification)] = _executed(transport)
        assert specification["command"] == "probe_gain_level"
        assert specification["host_mac"] == "102030405060"

    @pytest.mark.asyncio
    async def test_set_gain_executes_typed_command(self):
        transport = _recording_transport()
        application = DanteApplication()
        application.transport = transport

        await application.send_set_gain_level(
            "192.168.1.108",
            2,
            5,
            "output",
            host_mac=b"\x10\x20\x30\x40\x50\x60",
        )

        [(address, specification)] = _executed(transport)
        assert address == "192.168.1.108"
        assert specification["command"] == "set_gain_level"
        assert specification["channel_number"] == 2
        assert specification["gain_level"] == 5
        assert specification["device_type"] == "output"
        assert specification["host_mac"] == "102030405060"


class TestDanteCMCService:
    def test_instantiation(self):
        transport = _recording_transport()
        service = DanteCMCService(transport, host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")
        assert service.registered_devices == frozenset()

    def test_exposes_host_address_used_by_cmc_commands(self):
        host_address = b"\x00\x1d\xc1\x50\x23\x68"
        service = DanteCMCService(_recording_transport(), host_media_access_control_address=host_address)

        assert service.host_media_access_control_address == host_address

    @pytest.mark.asyncio
    async def test_registration_executes_typed_specification(self):
        transport = _recording_transport()
        service = DanteCMCService(transport, host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")
        service._sequence_counter = 0x1234

        await service.register_device("192.168.1.61")

        assert _executed(transport) == [
            ("192.168.1.61", {"command": "cmc_register", "host_mac": "001dc1502368", "sequence": 0x1234})
        ]

    @pytest.mark.asyncio
    async def test_registration_requires_matching_success_response(self):
        transport = _recording_transport()
        service = DanteCMCService(transport, host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")
        successful_response = bytes.fromhex("120000200000100100010000020000000001000000010000c0a8013d21fc0000")
        transport.execute.return_value = successful_response

        response = await service.register_device("192.168.1.61")

        assert response == successful_response
        assert service.registered_devices == {"192.168.1.61"}

    @pytest.mark.asyncio
    async def test_registration_rejects_failure_response(self):
        transport = _recording_transport()
        service = DanteCMCService(transport, host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")
        transport.execute.return_value = bytes.fromhex(
            "120000200000100100000000020000000001000000010000c0a8013d21fc0000"
        )

        response = await service.register_device("192.168.1.61")

        assert response is None
        assert service.registered_devices == frozenset()

    @pytest.mark.asyncio
    async def test_registration_rejects_mismatched_sequence_and_malformed_envelope(self):
        transport = _recording_transport()
        service = DanteCMCService(transport, host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")
        service._sequence_counter = 0x1234
        transport.execute.return_value = bytes.fromhex(
            "120000200000100100010000020000000001000000010000c0a8013d21fc0000"
        )

        assert await service.register_device("192.168.1.61") is None
        assert service.registered_devices == frozenset()

        transport.execute.return_value = bytes.fromhex(
            "1200001f1235100100010000020000000001000000010000c0a8013d21fc0000"
        )

        assert await service.register_device("192.168.1.61") is None
        assert service.registered_devices == frozenset()

    @pytest.mark.asyncio
    async def test_required_registration_fails_loudly_on_timeout(self):
        from netaudio import core

        transport = _recording_transport()
        transport.execute.side_effect = core.NetaudioCoreError(core.STATUS_TIMEOUT, "execute cmc_register")
        service = DanteCMCService(transport, host_media_access_control_address=b"\x00\x1d\xc1\x50\x23\x68")

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
        application, device = application_with_device("device.local.", device_ip)
        receive_packets(application, [self._build_dual_interface_packet(secondary_mac)], (device_ip, 1030))
        return device

    def test_instantiation(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        assert service._dispatcher is dispatcher
        assert service._multicast_group == "224.0.0.231"
        assert service._multicast_port == 8702

    @pytest.mark.parametrize(
        "notification_id,name",
        [
            (128, "Sample Rate Status"),
            (130, "Encoding Status"),
            (257, "TX Channel Change"),
            (258, "RX Channel Change"),
            (4103, "AES67 Status"),
            (4107, "Gain Status"),
            (0xABCD, "Unknown(0xABCD)"),
        ],
    )
    def test_notification_event_preserves_identity_payload_and_label(self, notification_id, name):
        application, device = application_with_device("device.local.", "192.0.2.1", name="Test Device")
        packet = struct.pack(">HH", 0x27FF, 28) + bytes(22) + struct.pack(">H", notification_id)

        [event] = receive_packets(application, [packet], ("192.0.2.1", 8702))

        assert event.type is EventType.NOTIFICATION_RECEIVED
        assert event.device_name == "Test Device"
        assert event.server_name == "device.local."
        assert event.data == {
            "notification_id": notification_id,
            "notification_name": name,
            "raw": packet,
            "source_ip": "192.0.2.1",
        }

    @pytest.mark.parametrize("protocol", [0x27FF, 0xFFFF])
    @pytest.mark.parametrize("length", [0, 1, 3, 4, 10, 26, 27])
    def test_short_packet_does_not_emit_an_event(self, protocol, length):
        dispatcher = MagicMock(spec=DanteEventDispatcher)
        service = DanteNotificationService(dispatcher=dispatcher)
        packet = (struct.pack(">HH", protocol, length) + bytes(24))[:length]

        service._on_packet(packet, ("192.0.2.1", 8702))

        dispatcher.emit_nowait.assert_not_called()

    def test_routing_capacity_ready_updates_capacity_and_active_counts(self):
        application, device = application_with_device("lx-dante.local.", "192.168.1.108")

        events = receive_packets(application, [ROUTING_CAPACITY_READY_PACKET], ("192.168.1.108", 8702))

        assert device.routing_ready is True
        assert device.routing_ready_state_code == 0x0101
        assert device.routing_capacity_transmit_channel_count == 128
        assert device.routing_capacity_receive_channel_count == 128
        assert device.tx_count == device.tx_count_raw == 128
        assert device.rx_count == device.rx_count_raw == 128
        assert count_events(events, EventType.DEVICE_UPDATED) == 1
        [status_event] = status_events(events)
        assert status_event.data["notification_id"] == CONMON_OPCODE_ROUTING_CAPACITY_STATUS
        assert status_event.data["kind"] == "routing_capacity"

    def test_routing_capacity_transition_preserves_active_counts(self):
        application, device = application_with_device("a32.local.", "10.0.2.15", name="A32")
        device.routing_ready = True
        device.routing_ready_state_code = 0x0101
        device.routing_capacity_transmit_channel_count = 64
        device.routing_capacity_receive_channel_count = 64
        device.tx_count = device.tx_count_raw = 64
        device.rx_count = device.rx_count_raw = 64

        events = receive_packets(application, [ROUTING_CAPACITY_TRANSITION_PACKET], ("10.0.2.15", 8702))

        assert device.routing_ready is False
        assert device.routing_ready_state_code == 0x0001
        assert device.routing_capacity_transmit_channel_count == 0
        assert device.routing_capacity_receive_channel_count == 0
        assert device.tx_count == device.tx_count_raw == 64
        assert device.rx_count == device.rx_count_raw == 64
        assert count_events(events, EventType.DEVICE_UPDATED) == 1

    def test_routing_capacity_ready_preserves_inventory_counts(self):
        application, device = application_with_device("ad4d.local.", "192.168.1.108")
        device.tx_count = 2
        device.tx_count_raw = 64
        device.rx_count = 1
        device.rx_count_raw = 1

        receive_packets(application, [ROUTING_CAPACITY_READY_PACKET], ("192.168.1.108", 8702))

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
        application, device = application_with_device("AVIOAI2-50692e.local.", device_ip)

        receive_packets(application, [AVIO_APPLIED_DHCP_INTERFACE_STATUS_PACKET], (device_ip, 8700))

        assert device.interfaces[0]["mode"] == "dynamic"
        assert device.interfaces[0]["ip_address"] == device_ip
        assert device.interface_reboot_required is False
        assert device.interface_pending_config is None

    def test_sample_rate_status_updates_device_and_emits_once(self):
        application, device = application_with_device("lx-dante.local.", "192.168.1.108")

        events = receive_packets(application, [SAMPLE_RATE_STATUS_PACKET], ("192.168.1.108", 1032))

        assert device.sample_rate == 44_100
        assert device.supported_sample_rates == [44_100, 48_000, 88_200, 96_000, 176_400, 192_000]
        assert count_events(events, EventType.DEVICE_UPDATED) == 1
        [status_event] = status_events(events)
        assert status_event.server_name == "lx-dante.local."
        assert status_event.data["notification_id"] == 128
        assert status_event.data["kind"] == "sample_rate"
        assert count_events(events, EventType.NOTIFICATION_RECEIVED) == 0

        repeated_events = receive_packets(application, [SAMPLE_RATE_STATUS_PACKET], ("192.168.1.108", 1032))
        assert count_events(repeated_events, EventType.DEVICE_UPDATED) == 0
        assert len(status_events(repeated_events)) == 1

    @pytest.mark.asyncio
    async def test_sample_rate_value_change_refreshes_controls_and_emits_once_when_refetching(self):
        application, device = application_with_device("lx-dante.local.", "192.168.1.108")
        device.sample_rate = 48_000
        device.supported_sample_rates = [44_100, 48_000, 88_200, 96_000, 176_400, 192_000]
        device.fetch_controls_data = AsyncMock(return_value={"sample_rate": 44_100})
        application.state.register()

        application.notifications._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))
        from tests.status_test_support import deliver_status_events

        events = await deliver_status_events(application)

        assert device.sample_rate == 44_100
        device.fetch_controls_data.assert_awaited_once()
        assert count_events(events, EventType.DEVICE_UPDATED) == 1

    def test_sample_rate_supported_list_change_emits_without_settings_refresh(self):
        application, device = application_with_device("lx-dante.local.", "192.168.1.108")
        device.sample_rate = 44_100
        device.supported_sample_rates = [44_100]
        device.fetch_controls_data = AsyncMock()

        events = receive_packets(application, [SAMPLE_RATE_STATUS_PACKET], ("192.168.1.108", 1032))

        assert count_events(events, EventType.DEVICE_UPDATED) == 1
        device.fetch_controls_data.assert_not_awaited()

    def test_sample_rate_status_notifies_registered_waiter(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        waiter = service.register_notification_waiter("192.168.1.108", (128,))

        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))

        assert waiter.is_set()
        assert waiter.latest_result == 128

    def test_sample_rate_status_returns_typed_probe_result(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        waiter = service.register_waiter("sample_rate", "192.168.1.108")

        service._on_packet(SAMPLE_RATE_STATUS_PACKET, ("192.168.1.108", 1032))

        assert waiter.is_set()
        assert waiter.latest_result == (
            44_100,
            [44_100, 48_000, 88_200, 96_000, 176_400, 192_000],
        )
        service.unregister_waiter(waiter)
        assert not service.is_waiting("sample_rate", "192.168.1.108")

    def test_sample_rate_status_does_not_repopulate_offline_device(self):
        application, device = application_with_device("lx-dante.local.", "192.168.1.108")
        device.online = False

        receive_packets(application, [SAMPLE_RATE_STATUS_PACKET], ("192.168.1.108", 1032))

        assert device.sample_rate is None
        assert device.supported_sample_rates is None

    def test_sample_rate_status_is_applied_when_device_appears(self):
        application = DanteApplication()
        events = receive_packets(application, [SAMPLE_RATE_STATUS_PACKET], ("192.168.1.108", 1032))
        device = DanteDevice(server_name="lx-dante.local.")
        device.ipv4 = "192.168.1.108"

        application.register_device(device.server_name, device)

        assert device.sample_rate == 44_100
        assert device.supported_sample_rates == [44_100, 48_000, 88_200, 96_000, 176_400, 192_000]
        assert count_events(events, EventType.DEVICE_UPDATED) == 0
        assert count_events(list(application.dispatcher._pending_events), EventType.DEVICE_UPDATED) == 0

    def test_encoding_status_updates_device_and_emits_once(self):
        application, device = application_with_device("lx-dante.local.", "192.168.1.108")

        events = receive_packets(application, [ENCODING_STATUS_PACKET], ("192.168.1.108", 1034))

        assert device.encoding == 24
        assert device.supported_encodings == [24]
        assert count_events(events, EventType.DEVICE_UPDATED) == 1
        [status_event] = status_events(events)
        assert status_event.server_name == "lx-dante.local."
        assert status_event.data["notification_id"] == 130

        repeated_events = receive_packets(application, [ENCODING_STATUS_PACKET], ("192.168.1.108", 1034))
        assert count_events(repeated_events, EventType.DEVICE_UPDATED) == 0
        assert len(status_events(repeated_events)) == 1

    def test_sample_rate_pullup_status_updates_device_and_waiter(self):
        application, device = application_with_device("a32.local.", "10.0.2.15", name="A32")
        waiter = application.notifications.register_waiter("sample_rate_pullup", "10.0.2.15")

        events = receive_packets(application, [SAMPLE_RATE_PULLUP_STATUS_PACKET], ("10.0.2.15", 8702))

        assert waiter.is_set()
        assert waiter.latest_result == (1, [0, 1, 2, 3, 4])
        assert device.sample_rate_pullup_raw_value == 1
        assert device.requested_sample_rate_pullup_raw_value == 1
        assert device.supported_sample_rate_pullup_raw_values == [0, 1, 2, 3, 4]
        assert count_events(events, EventType.DEVICE_UPDATED) == 1
        [status_event] = status_events(events)
        assert status_event.data["notification_id"] == 132
        application.notifications.unregister_waiter(waiter)

    def test_encoding_status_returns_device_scoped_typed_probe_result(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        matching_waiter = service.register_waiter("encoding", "192.168.1.108")
        unrelated_waiter = service.register_waiter("encoding", "192.168.1.109")

        service._on_packet(ENCODING_STATUS_PACKET, ("192.168.1.108", 1034))

        assert matching_waiter.is_set()
        assert not unrelated_waiter.is_set()
        assert matching_waiter.latest_result == (24, [24])
        assert unrelated_waiter.latest_result is None
        service.unregister_waiter(matching_waiter)
        service.unregister_waiter(unrelated_waiter)

    def test_encoding_status_does_not_repopulate_offline_device(self):
        application, device = application_with_device("lx-dante.local.", "192.168.1.108")
        device.online = False

        receive_packets(application, [ENCODING_STATUS_PACKET], ("192.168.1.108", 1034))

        assert device.encoding is None
        assert device.supported_encodings is None

    def test_encoding_status_is_applied_when_device_appears(self):
        application = DanteApplication()
        receive_packets(application, [ENCODING_STATUS_PACKET], ("192.168.1.108", 1034))
        device = DanteDevice(server_name="lx-dante.local.")
        device.ipv4 = "192.168.1.108"

        application.register_device(device.server_name, device)

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
        application, device = application_with_device("avio-input.local.", "192.168.1.108")

        events = receive_packets(application, [INPUT_GAIN_STATUS_PACKET], ("192.168.1.108", 8700))

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
        assert count_events(events, EventType.DEVICE_UPDATED) == 1
        [status_event] = status_events(events)
        assert status_event.data["notification_id"] == CONMON_OPCODE_GAIN_STATUS

    def test_output_gain_status_notifies_only_matching_device_waiters(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        matching_waiter = service.register_waiter(
            "gain",
            "192.168.1.108",
            accept=_gain_status_accepts(None, 2, 4),
        )
        unrelated_waiter = service.register_waiter(
            "gain",
            "192.168.1.109",
            accept=_gain_status_accepts(None, 2, 4),
        )

        service._on_packet(OUTPUT_GAIN_STATUS_PACKET, ("192.168.1.108", 8700))

        assert matching_waiter.is_set()
        assert matching_waiter.latest_result == ("output", [4, 4])
        assert not unrelated_waiter.is_set()

    def test_gain_write_waiter_ignores_nonmatching_level_but_retains_readback(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        waiter = service.register_waiter("gain", "192.168.1.108", accept=_gain_status_accepts(None, 1, 3))

        service._on_packet(INPUT_GAIN_STATUS_PACKET, ("192.168.1.108", 8700))

        assert not waiter.is_set()
        assert waiter.latest_result == ("input", [5, 1])

    def test_gain_write_waiter_ignores_nonmatching_direction(self):
        service = DanteNotificationService(dispatcher=MagicMock())
        waiter = service.register_waiter("gain", "192.168.1.108", accept=_gain_status_accepts("output", 1, 5))

        service._on_packet(INPUT_GAIN_STATUS_PACKET, ("192.168.1.108", 8700))

        assert not waiter.is_set()
        assert waiter.latest_result == ("input", [5, 1])

    def test_gain_status_is_applied_when_device_appears(self):
        application = DanteApplication()
        receive_packets(application, [INPUT_GAIN_STATUS_PACKET], ("192.168.1.108", 8700))
        device = DanteDevice(server_name="avio-input.local.")
        device.ipv4 = "192.168.1.108"

        application.register_device(device.server_name, device)

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
