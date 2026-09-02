import asyncio
import struct
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.application import CapabilityProbeTimeout
from tests.status_test_support import application_with_device, deliver_status_events, receive_packets

from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.const import (
    CONMON_CLOCK_PORT_STATE_OFFSET,
    CONMON_OPCODE_PTP_CLOCK_STATUS,
    CONMON_PREFERRED_LEADER_OFFSET,
)
from netaudio.dante.services.notification import (
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

        waiter = service.register_waiter("preferred_leader", device_ip)
        packet = self._build_conmon_0020_packet(0x01)
        service._on_packet(packet, (device_ip, 1030))

        await asyncio.wait_for(waiter.wait(), timeout=1)
        assert waiter.latest_result is True
        service.unregister_waiter(waiter)

    @pytest.mark.asyncio
    async def test_preferred_leader_off(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.34"

        waiter = service.register_waiter("preferred_leader", device_ip)
        packet = self._build_conmon_0020_packet(0x00)
        service._on_packet(packet, (device_ip, 1030))

        await asyncio.wait_for(waiter.wait(), timeout=1)
        assert waiter.latest_result is False
        service.unregister_waiter(waiter)

    @pytest.mark.asyncio
    async def test_probe_clocking_status_applies_the_paired_publication(self):
        application, device = application_with_device("avio-bt-1.local.", "192.168.1.34")
        packet = (
            Path(__file__).parent / "fixtures" / "clock_leader_association" / "follower-domain-a-0020.bin"
        ).read_bytes()
        calls = []

        async def refresh(device_ip_address, host_mac=None, sequence=0x0021):
            calls.append((device_ip_address, sequence))
            application.notifications._on_packet(packet, (device_ip_address, 8702))
            await deliver_status_events(application)

        application.settings.refresh_clock_status = refresh

        parsed = await application.probe_clocking_status(device)

        expected = core.parse_response("ptp_clock_status", packet)
        assert calls == [("192.168.1.34", 0x0021)]
        assert parsed["clock_source_code"] == expected["clock_source_code"]
        assert parsed["preferred_leader"] == expected["preferred_leader"]
        assert parsed["clock_role"] == expected["clock_role"]
        assert device.clock_source_code == expected["clock_source_code"]
        assert device.clock_identity == "001dc1510295"
        assert device.leader_clock_identity == "001dc150692e"

    @pytest.mark.asyncio
    async def test_probe_clocking_status_times_out_without_any_publication(self):
        application, device = application_with_device("avio-bt-1.local.", "192.168.1.61")
        application.settings.refresh_clock_status = AsyncMock()

        with pytest.raises(CapabilityProbeTimeout, match="clock status probe timed out"):
            await application.probe_clocking_status(device, timeout=0.01)

        application.settings.refresh_clock_status.assert_awaited_once_with("192.168.1.61")
        assert not application.notifications.is_waiting("preferred_leader", "192.168.1.61")

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

    def test_updates_device_through_the_state_service(self):
        application, device = application_with_device("test.local.", "192.168.1.34", name="test")
        device.preferred_leader = None

        packet = self._build_conmon_0020_packet(0x01)
        receive_packets(application, [packet], ("192.168.1.34", 1030))
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
