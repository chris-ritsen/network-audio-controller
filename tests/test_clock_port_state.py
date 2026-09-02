import struct

from netaudio.dante.device import DanteDevice
from netaudio.dante.const import (
    CLOCK_PORT_ROLE_MAP,
    CLOCK_PORT_STATE_FOLLOWER,
    CLOCK_PORT_STATE_LEADER,
    CONMON_CLOCK_FREQUENCY_OFFSET_PARTS_PER_BILLION_OFFSET,
    CONMON_CLOCK_PORT_STATE_OFFSET,
    CONMON_PREFERRED_LEADER_OFFSET,
)
from tests.status_test_support import application_with_device, receive_packets


class TestClockPortStateFromConmon0x0020:
    def _build_conmon_0020_packet(
        self,
        state_code,
        preferred_leader_byte=0x00,
        clock_frequency_offset_parts_per_billion=-25_473,
    ):
        packet = bytearray(CONMON_CLOCK_PORT_STATE_OFFSET + 2)
        struct.pack_into(">H", packet, 0, 0xFFFF)
        struct.pack_into(">H", packet, 2, len(packet))
        packet[0x10:0x18] = b"Audinate"
        struct.pack_into(">H", packet, 0x18, 0x073A)
        struct.pack_into(">H", packet, 0x1A, 0x0020)
        packet[CONMON_PREFERRED_LEADER_OFFSET] = preferred_leader_byte
        struct.pack_into(
            ">i",
            packet,
            CONMON_CLOCK_FREQUENCY_OFFSET_PARTS_PER_BILLION_OFFSET,
            clock_frequency_offset_parts_per_billion,
        )
        struct.pack_into(">H", packet, CONMON_CLOCK_PORT_STATE_OFFSET, state_code)
        return bytes(packet)

    def test_leader_role_decode(self):
        assert CLOCK_PORT_ROLE_MAP[CLOCK_PORT_STATE_LEADER] == "Leader"

    def test_follower_role_decode(self):
        assert CLOCK_PORT_ROLE_MAP[CLOCK_PORT_STATE_FOLLOWER] == "Follower"

    def test_leader_constant(self):
        assert CLOCK_PORT_STATE_LEADER == 0x0006

    def test_follower_constant(self):
        assert CLOCK_PORT_STATE_FOLLOWER == 0x0009

    def test_state_offset_constant(self):
        assert CONMON_CLOCK_PORT_STATE_OFFSET == 0x48

    def test_only_two_known_roles(self):
        assert len(CLOCK_PORT_ROLE_MAP) == 2

    def test_unknown_role_value_not_mapped(self):
        assert CLOCK_PORT_ROLE_MAP.get(0x0001) is None

    def test_device_gets_leader_from_0x0020(self):
        application, device = application_with_device("test.local.", "192.168.1.108", name="test")

        packet = self._build_conmon_0020_packet(CLOCK_PORT_STATE_LEADER, preferred_leader_byte=0x01)
        receive_packets(application, [packet], ("192.168.1.108", 1030))
        assert device.clock_frequency_offset_parts_per_billion == -25_473
        assert device.clock_port_state_code == 0x0006
        assert device.clock_role == "Leader"
        assert device.preferred_leader is True
        assert device.clock_source_code == 0

    def test_device_gets_follower_from_0x0020(self):
        application, device = application_with_device("test.local.", "192.168.1.34", name="test")

        packet = self._build_conmon_0020_packet(CLOCK_PORT_STATE_FOLLOWER, preferred_leader_byte=0x00)
        receive_packets(application, [packet], ("192.168.1.34", 1030))
        assert device.clock_frequency_offset_parts_per_billion == -25_473
        assert device.clock_port_state_code == 0x0009
        assert device.clock_role == "Follower"
        assert device.preferred_leader is False

    def test_unknown_state_preserves_raw_code_and_clears_derived_role(self):
        application, device = application_with_device("test.local.", "192.168.1.1", name="test")
        device.clock_port_state_code = CLOCK_PORT_STATE_LEADER
        device.clock_role = "Leader"

        packet = self._build_conmon_0020_packet(0x0001)
        receive_packets(application, [packet], ("192.168.1.1", 1030))
        assert device.clock_port_state_code == 0x0001
        assert device.clock_role is None

    def test_no_v2_role_field_on_device(self):
        device = DanteDevice()
        assert not hasattr(device, "ptp_v2_role")


class TestClockPortStateDeviceModel:
    def test_default_is_none(self):
        device = DanteDevice()
        assert device.clock_frequency_offset_parts_per_billion is None
        assert device.clock_port_state_code is None
        assert device.clock_role is None

    def test_serializer_includes_role(self):
        device = DanteDevice()
        device.name = "test"
        device.server_name = "test"
        device.ipv4 = "192.168.1.1"
        device.clock_frequency_offset_parts_per_billion = -25_473
        device.clock_port_state_code = 0x0006
        device.clock_role = "Leader"
        json_data = device.to_json()
        assert json_data["clock_frequency_offset_parts_per_billion"] == -25_473
        assert json_data["clock_port_state_code"] == 0x0006
        assert json_data["clock_role"] == "Leader"

    def test_serializer_omits_none(self):
        device = DanteDevice()
        device.name = "test"
        device.server_name = "test"
        device.ipv4 = "192.168.1.1"
        json_data = device.to_json()
        assert "clock_frequency_offset_parts_per_billion" not in json_data
        assert "clock_port_state_code" not in json_data
        assert "clock_role" not in json_data
