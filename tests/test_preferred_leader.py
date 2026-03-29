import struct

import pytest

from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.services.notification import (
    CONMON_OPCODE_PTP_CLOCK_STATUS,
    CONMON_PREFERRED_LEADER_OFFSET,
    CONMON_PTP_V1_ROLE_OFFSET,
    PTP_V1_ROLE_MAP,
    PTP_V1_ROLE_MASTER,
    PTP_V1_ROLE_SLAVE,
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

    def test_nonzero_sequence(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_set_preferred_leader(True)
        sequence = struct.unpack(">H", packet[4:6])[0]
        assert sequence != 0

    def test_conmon_header_structure(self):
        commands = DanteDeviceCommands()
        packet, _, _ = commands.command_set_preferred_leader(True)
        assert struct.unpack(">H", packet[0:2])[0] == 0xFFFF
        magic_offset = packet.find(b'Audinate')
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
        packet = bytearray(184)
        struct.pack_into(">H", packet, 0, 0xFFFF)
        struct.pack_into(">H", packet, 2, 184)
        packet[0x10:0x18] = b'Audinate'
        struct.pack_into(">H", packet, 0x18, 0x073A)
        struct.pack_into(">H", packet, 0x1A, 0x0020)
        packet[CONMON_PREFERRED_LEADER_OFFSET] = preferred_leader_byte
        return bytes(packet)

    def test_preferred_leader_on(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.34"

        waiter = service.register_preferred_leader_waiter(device_ip)
        packet = self._build_conmon_0020_packet(0x01)
        service._on_packet(packet, (device_ip, 1030))

        assert waiter.is_set()
        result = service.get_preferred_leader_result(device_ip)
        assert result is True
        service.unregister_preferred_leader_waiter(device_ip)

    def test_preferred_leader_off(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        device_ip = "192.168.1.34"

        waiter = service.register_preferred_leader_waiter(device_ip)
        packet = self._build_conmon_0020_packet(0x00)
        service._on_packet(packet, (device_ip, 1030))

        assert waiter.is_set()
        result = service.get_preferred_leader_result(device_ip)
        assert result is False
        service.unregister_preferred_leader_waiter(device_ip)

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


class TestPTPv1RoleFromConmon0x0020:
    def _build_conmon_0020_packet(self, v1_role_value, preferred_leader_byte=0x00):
        packet = bytearray(184)
        struct.pack_into(">H", packet, 0, 0xFFFF)
        struct.pack_into(">H", packet, 2, 184)
        packet[0x10:0x18] = b'Audinate'
        struct.pack_into(">H", packet, 0x18, 0x073A)
        struct.pack_into(">H", packet, 0x1A, 0x0020)
        packet[CONMON_PREFERRED_LEADER_OFFSET] = preferred_leader_byte
        struct.pack_into(">H", packet, CONMON_PTP_V1_ROLE_OFFSET, v1_role_value)
        return bytes(packet)

    def test_master_role_decode(self):
        assert PTP_V1_ROLE_MAP[PTP_V1_ROLE_MASTER] == "Leader"

    def test_slave_role_decode(self):
        assert PTP_V1_ROLE_MAP[PTP_V1_ROLE_SLAVE] == "Follower"

    def test_master_constant(self):
        assert PTP_V1_ROLE_MASTER == 0x0006

    def test_slave_constant(self):
        assert PTP_V1_ROLE_SLAVE == 0x0009

    def test_role_offset_constant(self):
        assert CONMON_PTP_V1_ROLE_OFFSET == 0x48

    def test_only_two_known_roles(self):
        assert len(PTP_V1_ROLE_MAP) == 2

    def test_unknown_role_value_not_mapped(self):
        assert PTP_V1_ROLE_MAP.get(0x0001) is None

    def test_device_gets_master_from_0x0020(self):
        dispatcher = DanteEventDispatcher()
        device = DanteDevice()
        device.name = "test"
        device.ipv4 = "192.168.1.108"

        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip: device if ip == "192.168.1.108" else None,
        )

        packet = self._build_conmon_0020_packet(PTP_V1_ROLE_MASTER, preferred_leader_byte=0x01)
        service._on_packet(packet, ("192.168.1.108", 1030))
        assert device.ptp_v1_role == "Leader"
        assert device.preferred_leader is True

    def test_device_gets_slave_from_0x0020(self):
        dispatcher = DanteEventDispatcher()
        device = DanteDevice()
        device.name = "test"
        device.ipv4 = "192.168.1.34"

        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip: device if ip == "192.168.1.34" else None,
        )

        packet = self._build_conmon_0020_packet(PTP_V1_ROLE_SLAVE, preferred_leader_byte=0x00)
        service._on_packet(packet, ("192.168.1.34", 1030))
        assert device.ptp_v1_role == "Follower"
        assert device.preferred_leader is False

    def test_unknown_role_leaves_none(self):
        dispatcher = DanteEventDispatcher()
        device = DanteDevice()
        device.name = "test"
        device.ipv4 = "192.168.1.1"

        service = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=lambda ip: device if ip == "192.168.1.1" else None,
        )

        packet = self._build_conmon_0020_packet(0x0001)
        service._on_packet(packet, ("192.168.1.1", 1030))
        assert device.ptp_v1_role is None

    def test_no_v2_role_field_on_device(self):
        device = DanteDevice()
        assert not hasattr(device, "ptp_v2_role")


class TestPTPv1RoleDeviceModel:
    def test_default_is_none(self):
        device = DanteDevice()
        assert device.ptp_v1_role is None

    def test_serializer_includes_role(self):
        device = DanteDevice()
        device.name = "test"
        device.server_name = "test"
        device.ipv4 = "192.168.1.1"
        device.ptp_v1_role = "Leader"
        json_data = device.to_json()
        assert json_data["ptp_v1_role"] == "Leader"

    def test_serializer_omits_none(self):
        device = DanteDevice()
        device.name = "test"
        device.server_name = "test"
        device.ipv4 = "192.168.1.1"
        json_data = device.to_json()
        assert "ptp_v1_role" not in json_data
