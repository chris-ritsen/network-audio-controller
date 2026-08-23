import asyncio
import struct
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.services.notification import (
    CLOCK_PORT_ROLE_MAP,
    CLOCK_PORT_STATE_FOLLOWER,
    CLOCK_PORT_STATE_LEADER,
    CONMON_CLOCK_FREQUENCY_OFFSET_PARTS_PER_BILLION_OFFSET,
    CONMON_CLOCK_PORT_STATE_OFFSET,
    CONMON_OPCODE_PTP_CLOCK_STATUS,
    CONMON_PREFERRED_LEADER_OFFSET,
    DanteNotificationService,
)


class TestPreferredLeaderSetPacket:
    def test_packet_length(self):
        commands = DanteDeviceCommands()
        packet, _, port = commands.command_set_preferred_leader(True)
        assert len(packet) == 92
        assert port == DEVICE_SETTINGS_PORT

    def test_packet_message_type(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_set_preferred_leader(True)
        message_type = struct.unpack(">H", packet[0x1A:0x1C])[0]
        assert message_type == 0x0021

    def test_presence_bitmask_set_on_write(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_set_preferred_leader(True)
        presence = struct.unpack(">H", packet[0x20:0x22])[0]
        assert presence == 0x0002

    def test_preferred_leader_on(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_set_preferred_leader(True)
        assert packet[0x24] == 0x01

    def test_preferred_leader_off(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_set_preferred_leader(False)
        assert packet[0x24] == 0x00

    def test_clock_source_passed_through(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_set_preferred_leader(True, clock_source=0xDED4)
        clock_source = struct.unpack(">H", packet[0x22:0x24])[0]
        assert clock_source == 0xDED4

    def test_set_clock_source_uses_mask_bit_zero(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_set_clock_source(0xDED4)
        assert struct.unpack(">H", packet[0x20:0x22])[0] == 0x0001
        assert struct.unpack(">H", packet[0x22:0x24])[0] == 0xDED4
        assert packet[0x24] == 0x00

    def test_nonzero_sequence(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_set_preferred_leader(True)
        sequence = struct.unpack(">H", packet[4:6])[0]
        assert sequence != 0

    def test_conmon_header_structure(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_set_preferred_leader(True)
        assert struct.unpack(">H", packet[0:2])[0] == 0xFFFF
        magic_offset = packet.find(b"Audinate")
        assert magic_offset == 0x10


class TestPreferredLeaderProbePacket:
    def test_probe_packet_length(self):
        commands = DanteDeviceCommands()
        packet, _, port = commands.command_probe_preferred_leader()
        assert len(packet) == 92
        assert port == DEVICE_SETTINGS_PORT

    def test_probe_presence_zero(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_probe_preferred_leader()
        presence = struct.unpack(">H", packet[0x20:0x22])[0]
        assert presence == 0x0000

    def test_probe_preferred_leader_zero(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_probe_preferred_leader()
        assert packet[0x24] == 0x00

    def test_probe_message_type(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_probe_preferred_leader()
        message_type = struct.unpack(">H", packet[0x1A:0x1C])[0]
        assert message_type == 0x0021


class TestPreferredLeaderFromConmon0x0020:
    def _build_conmon_0020_packet(self, preferred_leader_byte):
        packet = bytearray(CONMON_CLOCK_PORT_STATE_OFFSET + 2)
        struct.pack_into(">H", packet, 0, 0xFFFF)
        struct.pack_into(">H", packet, 2, len(packet))
        packet[0x10:0x18] = b"Audinate"
        struct.pack_into(">H", packet, 0x18, 0x073A)
        struct.pack_into(">H", packet, 0x1A, 0x0020)
        packet[CONMON_PREFERRED_LEADER_OFFSET] = preferred_leader_byte
        return bytes(packet)

    @pytest.mark.asyncio
    async def test_preferred_leader_on(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.34"

        waiter = service.register_preferred_leader_waiter(device_ip)
        packet = self._build_conmon_0020_packet(0x01)
        service._on_packet(packet, (device_ip, 1030))

        await asyncio.wait_for(waiter.wait(), timeout=1)
        result = service.get_preferred_leader_result(device_ip)
        assert result is True
        service.unregister_preferred_leader_waiter(device_ip)

    @pytest.mark.asyncio
    async def test_preferred_leader_off(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.34"

        waiter = service.register_preferred_leader_waiter(device_ip)
        packet = self._build_conmon_0020_packet(0x00)
        service._on_packet(packet, (device_ip, 1030))

        await asyncio.wait_for(waiter.wait(), timeout=1)
        result = service.get_preferred_leader_result(device_ip)
        assert result is False
        service.unregister_preferred_leader_waiter(device_ip)

    @pytest.mark.asyncio
    async def test_clock_status_refresh_updates_device_from_paired_publication(self, monkeypatch):
        device = DanteDevice()
        device.ipv4 = "192.168.1.34"
        calls = []
        parsed_response = {
            "preferred_leader": True,
            "clock_source_code": 0xDED4,
            "clock_subdomain": [0] * 16,
            "clock_frequency_offset_parts_per_billion": -12,
            "clock_port_state_code": 6,
            "clock_role": "Leader",
            "clock_port_records": [],
            "clock_identity": [0, 29, 193, 80, 105, 46],
            "leader_clock_identity": [0, 29, 193, 80, 105, 46],
        }

        def refresh(host_mac, sequence):
            calls.append((host_mac, sequence))
            return b"refresh", None, 8700

        async def run_query(_query):
            return parsed_response

        device.commands.command_refresh_clock_status = refresh
        monkeypatch.setattr(asyncio, "to_thread", run_query)

        result = await device.get_clocking_status(
            host_mac=b"\x10\x20\x30\x40\x50\x60",
            sequence=0x0021,
        )

        assert result == parsed_response
        assert calls == [(b"\x10\x20\x30\x40\x50\x60", 0x0021)]
        assert device.preferred_leader is True
        assert device.clock_source_code == 0xDED4
        assert device.clock_subdomain == bytes(16)
        assert device.clock_frequency_offset_parts_per_billion == -12
        assert device.clock_port_state_code == 6
        assert device.clock_role == "Leader"
        assert device.clock_port_records == []
        assert device.clock_identity == "001dc150692e"
        assert device.leader_clock_identity == "001dc150692e"

    @pytest.mark.asyncio
    async def test_probe_clocking_status_uses_settings_refresh_and_publication(self):
        from netaudio._common import CoreCommandSender

        device = DanteDevice()
        device.ipv4 = "192.168.1.61"
        device.name = "avio-bt-1"
        sender = CoreCommandSender(devices={"bt": device})

        class FakeWaiter:
            def __init__(self):
                self.wait = AsyncMock()

        waiter = FakeWaiter()
        notifications = SimpleNamespace(
            register_preferred_leader_waiter=MagicMock(return_value=waiter),
            unregister_preferred_leader_waiter=MagicMock(),
        )
        settings_service = SimpleNamespace(refresh_clock_status=MagicMock())

        async def ensure_notifications():
            return notifications

        async def ensure_settings():
            return settings_service

        sender._ensure_notifications = ensure_notifications
        sender._ensure_settings_service = ensure_settings

        def refresh(_device_ip):
            device.clock_source_code = 0
            device.clock_subdomain = bytes(16)

        settings_service.refresh_clock_status.side_effect = refresh

        parsed = await sender.probe_clocking_status(device)

        assert parsed["clock_source_code"] == 0
        assert parsed["clock_identity"] is None
        assert parsed["leader_clock_identity"] is None
        settings_service.refresh_clock_status.assert_called_once_with("192.168.1.61")
        notifications.register_preferred_leader_waiter.assert_called_once_with("192.168.1.61")
        waiter.wait.assert_awaited()
        notifications.unregister_preferred_leader_waiter.assert_called_once_with("192.168.1.61")

    def test_live_avio_bluetooth_clock_publication_parses_raw_source(self):
        from netaudio import core

        packet = bytes.fromhex(
            "ffff00dce1190000001dc1fffe5279b6417564696e6174650738002000000000000300030000009fffff9baf001dc15279b60000001dc10812580000001dc1081258000000010034000900000294000000030d4000000002000000000000000000000000000000000000000100600c000000000c0098002000030000006810000000000101020100000000020009000700010002020202000000000200030003000100030202010000000002000300070003000700b80004001dc1fffe5279b6001dc1fffe081258001dc1fffe081258000100000001000000010000"
        )
        parsed = core.parse_response("ptp_clock_status", packet)
        assert parsed["clock_source_code"] == 0
        assert parsed["preferred_leader"] is False
        assert parsed["clock_role"] == "Follower"
        assert parsed["clock_port_records"] is None

    def test_clock_status_refresh_does_not_bind_the_settings_port(self, monkeypatch):
        device = DanteDevice()
        device.ipv4 = "192.168.1.61"
        bound_addresses = []
        sent = []

        class FakeSocket:
            def __init__(self, *_arguments, **_keyword_arguments):
                self.timeout = None

            def setsockopt(self, *_arguments, **_keyword_arguments):
                return None

            def bind(self, address):
                bound_addresses.append(address)

            def settimeout(self, timeout):
                self.timeout = timeout

            def sendto(self, packet, address):
                sent.append((packet, address))

            def recvfrom(self, _size):
                return (b"publication", ("192.168.1.61", 8702))

            def close(self):
                return None

        monkeypatch.setattr("netaudio.dante.device.socket.socket", FakeSocket)
        parsed = device._receive_solicited_control_publication(
            b"refresh",
            "192.168.1.61",
            lambda data: {"ok": data == b"publication"},
        )

        assert parsed == {"ok": True}
        assert sent == [(b"refresh", ("192.168.1.61", 8700))]
        assert ("", 8700) not in bound_addresses
        assert ("0.0.0.0", 8700) not in bound_addresses

    def test_updates_device_directly(self):
        dispatcher = DanteEventDispatcher()
        device = DanteDevice()
        device.name = "test"
        device.ipv4 = "192.168.1.34"
        device.preferred_leader = None

        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip: device if ip == "192.168.1.34" else None,
        )

        packet = self._build_conmon_0020_packet(0x01)
        service._on_packet(packet, ("192.168.1.34", 1030))
        assert device.preferred_leader is True

    def test_opcode_constant(self):
        assert CONMON_OPCODE_PTP_CLOCK_STATUS == 0x0020

    def test_offset_constant(self):
        assert CONMON_PREFERRED_LEADER_OFFSET == 0x26


class TestPreferredLeaderDeviceModel:
    def test_default_is_none(self):
        device = DanteDevice()
        assert device.preferred_leader is None

    def test_serializer_includes_preferred_leader(self):
        device = DanteDevice()
        device.name = "test"
        device.server_name = "test"
        device.ipv4 = "192.168.1.1"
        device.preferred_leader = True
        json_data = device.to_json()
        assert json_data["preferred_leader"] is True

    def test_serializer_omits_none(self):
        device = DanteDevice()
        device.name = "test"
        device.server_name = "test"
        device.ipv4 = "192.168.1.1"
        json_data = device.to_json()
        assert "preferred_leader" not in json_data
