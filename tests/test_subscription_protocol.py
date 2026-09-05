"""Tests for subscription add/remove command building plus header mechanics of
captured Controller 0x2809/0x3410 frames.

netaudio subscribes via protocol 0x27FF, opcodes 0x3010 (add, causally
verified) and 0x3014 (remove). The captured Controller frames on
0x2809/0x3410 were taken during subscription changes, but the fact registry
records that form as subscription_clear_prep (quarantined): its causal effect
is not established, and these tests assert wire structure only.

Both protocols are sent to the ARC service port (dynamic, discovered via mDNS).
"""

import struct

import pytest
from netaudio.dante.const import OPCODE_SUBSCRIPTION_ADD, OPCODE_SUBSCRIPTION_REMOVE, PROTOCOL_ID, SERVICE_ARC
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.dissection.header import parse_packet_header
from netaudio.dante.packet_store import PacketRecord

FIXTURES_DIR = "subscription"


@pytest.fixture
def cmds():
    return DanteDeviceCommands()


@pytest.fixture
def load_sub_fixture(load_fixture):
    def _load(name):
        return load_fixture(f"{FIXTURES_DIR}/{name}")

    return _load


# ---------------------------------------------------------------------------
# Test the actual command builders
# ---------------------------------------------------------------------------


class TestAddSubscriptionCommand:
    def test_produces_valid_header(self, cmds):
        pkt, svc = cmds.command_add_subscription(1, "mic-mix-high", "lx-dante")
        h = parse_packet_header(pkt)

        assert h["protocol_id"] == PROTOCOL_ID
        assert h["opcode"] == OPCODE_SUBSCRIPTION_ADD

    def test_routes_to_arc_service(self, cmds):
        _, svc = cmds.command_add_subscription(1, "mic-mix-high", "lx-dante")
        assert svc == SERVICE_ARC

    def test_length_field_matches_packet(self, cmds):
        pkt, _ = cmds.command_add_subscription(1, "mic-mix-high", "lx-dante")
        stated_length = struct.unpack(">H", pkt[2:4])[0]
        assert stated_length == len(pkt)

    @pytest.mark.parametrize(
        ("channel", "tx_name", "device_name"),
        [(1, "ch1", "dev1"), (2, "mic-mix-high", "lx-dante"), (255, "a", "long-device-name")],
    )
    def test_receiver_and_name_pointers(self, cmds, channel, tx_name, device_name):
        packet, _ = cmds.command_add_subscription(channel, tx_name, device_name)
        receiver, tx_pointer, device_pointer = struct.unpack_from(">HHH", packet, 12)
        assert receiver == channel
        assert tx_pointer == 52
        assert device_pointer == 52 + len(tx_name.encode()) + 1
        assert packet[tx_pointer:device_pointer] == tx_name.encode() + b"\0"
        assert packet[device_pointer:] == device_name.encode() + b"\0"


class TestRemoveSubscriptionCommand:
    def test_produces_valid_header(self, cmds):
        pkt, svc = cmds.command_remove_subscription(1)
        h = parse_packet_header(pkt)

        assert h["protocol_id"] == PROTOCOL_ID
        assert h["opcode"] == OPCODE_SUBSCRIPTION_REMOVE

    def test_routes_to_arc_service(self, cmds):
        _, svc = cmds.command_remove_subscription(1)
        assert svc == SERVICE_ARC

    def test_length_field_matches_packet(self, cmds):
        pkt, _ = cmds.command_remove_subscription(1)
        stated_length = struct.unpack(">H", pkt[2:4])[0]
        assert stated_length == len(pkt)

    @pytest.mark.parametrize("channel", [1, 2, 257])
    def test_channel_number_encoded(self, cmds, channel):
        packet, _ = cmds.command_remove_subscription(channel)
        assert len(packet) == 16
        assert packet[10:] == struct.pack(">HHH", 1, 0, channel)


# ---------------------------------------------------------------------------
# Test that captured Dante Controller packets parse correctly
# ---------------------------------------------------------------------------


class TestCapturedSubscriptionHeaders:
    """Verify parse_packet_header handles the Dante Controller protocol variant."""

    def test_captured_remove_header(self, load_sub_fixture):
        from netaudio.dante.debug_formatter import get_opcode_name

        data = load_sub_fixture("subscription_remove_request.bin")
        h = parse_packet_header(data)

        assert h["protocol_id"] == 0x2809
        assert h["opcode"] == 0x3410
        assert h["opcode_name"] == get_opcode_name(0x2809, 0x3410)
        assert h["result_code"] == 0x0000

    def test_captured_remove_response(self, load_sub_fixture):
        data = load_sub_fixture("subscription_remove_response.bin")
        h = parse_packet_header(data)

        assert h["result_code"] == 0x0001
        assert h["result_name"] == "RESULT_CODE_SUCCESS"

    def test_captured_add_header(self, load_sub_fixture):
        data = load_sub_fixture("subscription_add_request.bin")
        h = parse_packet_header(data)

        assert h["protocol_id"] == 0x2809
        assert h["opcode"] == 0x3410

    def test_rx_channel_status_opcode(self, load_sub_fixture):
        from netaudio.dante.debug_formatter import get_opcode_name

        data = load_sub_fixture("rx_channel_status_request.bin")
        h = parse_packet_header(data)
        assert h["opcode"] == 0x3400
        assert h["opcode_name"] == get_opcode_name(0x2809, 0x3400)

    def test_rx_flow_status_opcode(self, load_sub_fixture):
        from netaudio.dante.debug_formatter import get_opcode_name

        data = load_sub_fixture("rx_flow_status_request.bin")
        h = parse_packet_header(data)
        assert h["opcode"] == 0x3600
        assert h["opcode_name"] == get_opcode_name(0x2809, 0x3600)


# ---------------------------------------------------------------------------
# Verify PacketStore correlation works with captured subscription traffic
# ---------------------------------------------------------------------------


class TestSubscriptionCorrelation:
    """Feed real captured packets through PacketStore and verify
    request/response correlation by transaction_id."""

    def test_remove_request_response_correlated(self, load_sub_fixture, tmp_path):
        from netaudio.dante.packet_store import PacketRecord, PacketStore

        store = PacketStore(db_path=str(tmp_path / "test.sqlite"))

        req_data = load_sub_fixture("subscription_remove_request.bin")
        resp_data = load_sub_fixture("subscription_remove_response.bin")

        req_id = store.store_packet(
            PacketRecord(
                payload=req_data,
                source_type="tshark",
                device_ip="192.168.1.94",
                direction="request",
                timestamp_ns=1000,
            )
        )
        resp_id = store.store_packet(
            PacketRecord(
                payload=resp_data,
                source_type="tshark",
                device_ip="192.168.1.94",
                direction="response",
                timestamp_ns=2000,
            )
        )

        req_row = store.get_packet(req_id)
        resp_row = store.get_packet(resp_id)

        assert req_row["correlated_packet_id"] == resp_id
        assert resp_row["correlated_packet_id"] == req_id
        store.close()

    def test_multicast_temporally_correlated_to_request(self, load_sub_fixture, tmp_path):
        from netaudio.dante.packet_store import PacketStore

        store = PacketStore(db_path=str(tmp_path / "test.sqlite"))

        req_data = load_sub_fixture("subscription_remove_request.bin")
        mc_data = load_sub_fixture("multicast_rx_channel_change.bin")

        now = 1_000_000_000_000

        store.store_packet(
            PacketRecord(
                payload=req_data,
                source_type="tshark",
                device_ip="192.168.1.94",
                direction="request",
                timestamp_ns=now,
            )
        )
        mc_id = store.store_packet(
            PacketRecord(
                payload=mc_data,
                source_type="multicast",
                src_ip="192.168.1.94",
                device_ip="192.168.1.94",
                timestamp_ns=now + 80_000_000,  # 80ms later
            )
        )

        mc_row = store.get_packet(mc_id)
        assert mc_row["correlated_packet_id"] is not None
        store.close()

    def test_stored_opcode_names(self, load_sub_fixture, tmp_path):
        from netaudio.dante.debug_formatter import get_opcode_name
        from netaudio.dante.packet_store import PacketStore

        store = PacketStore(db_path=str(tmp_path / "test.sqlite"))

        for name, expected_opcode_name in [
            ("subscription_remove_request.bin", get_opcode_name(0x2809, 0x3410)),
            ("rx_channel_status_request.bin", get_opcode_name(0x2809, 0x3400)),
            ("rx_flow_status_request.bin", get_opcode_name(0x2809, 0x3600)),
        ]:
            data = load_sub_fixture(name)
            pid = store.store_packet(
                PacketRecord(
                    payload=data,
                    source_type="tshark",
                    device_ip="192.168.1.94",
                    direction="request",
                    timestamp_ns=1000,
                )
            )
            row = store.get_packet(pid)
            assert row["opcode_name"] == expected_opcode_name, (
                f"{name}: expected {expected_opcode_name}, got {row['opcode_name']}"
            )

        store.close()
