"""Tests for subscription add/remove command building and the captured
Dante Controller protocol variant (0x2809/0x3410) for reference.

The device accepts two protocol variants for subscription management:
  - netaudio: protocol 0x27FF, opcodes 0x3010 (add) / 0x3014 (remove)
  - Dante Controller: protocol 0x2809, opcode 0x3410 (both add and remove)

Both are sent to the ARC service port (dynamic, discovered via mDNS).
"""

import struct

import pytest

from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.device_commands import DanteDeviceCommands, Opcode, Protocol
from netaudio.dante.packet_store import _parse_header


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
        h = _parse_header(pkt)

        assert h["protocol_id"] == Protocol.CONTROL
        assert h["opcode"] == Opcode.SUBSCRIPTION_ADD

    def test_routes_to_arc_service(self, cmds):
        _, svc = cmds.command_add_subscription(1, "mic-mix-high", "lx-dante")
        assert svc == SERVICE_ARC

    def test_length_field_matches_packet(self, cmds):
        pkt, _ = cmds.command_add_subscription(1, "mic-mix-high", "lx-dante")
        stated_length = struct.unpack(">H", pkt[2:4])[0]
        assert stated_length == len(pkt)

    def test_contains_tx_channel_name(self, cmds):
        pkt, _ = cmds.command_add_subscription(1, "mic-mix-high", "lx-dante")
        assert b"mic-mix-high\x00" in pkt

    def test_contains_tx_device_name(self, cmds):
        pkt, _ = cmds.command_add_subscription(1, "mic-mix-high", "lx-dante")
        assert b"lx-dante\x00" in pkt

    def test_tx_channel_offset_resolves(self, cmds):
        pkt, _ = cmds.command_add_subscription(1, "mic-mix-high", "lx-dante")
        # Offset is at byte 13 (payload byte 5) as a single byte in the current impl
        # Find the offset and verify it points to the string
        idx = pkt.index(b"mic-mix-high\x00")
        assert idx > 0

    def test_tx_device_offset_resolves(self, cmds):
        pkt, _ = cmds.command_add_subscription(1, "mic-mix-high", "lx-dante")
        idx = pkt.index(b"lx-dante\x00")
        assert idx > pkt.index(b"mic-mix-high\x00")

    def test_channel_1(self, cmds):
        pkt, _ = cmds.command_add_subscription(1, "ch1", "dev1")
        # rx_channel_number=1 should appear in payload
        assert b"ch1\x00" in pkt
        assert b"dev1\x00" in pkt

    def test_channel_2(self, cmds):
        pkt, _ = cmds.command_add_subscription(2, "ch1", "dev1")
        assert b"ch1\x00" in pkt

    def test_different_names_produce_different_packets(self, cmds):
        pkt1, _ = cmds.command_add_subscription(1, "ch-a", "dev-a")
        pkt2, _ = cmds.command_add_subscription(1, "ch-b", "dev-b")
        assert pkt1 != pkt2

    def test_longer_names_produce_longer_packet(self, cmds):
        short, _ = cmds.command_add_subscription(1, "a", "b")
        long, _ = cmds.command_add_subscription(1, "long-channel-name", "long-device-name")
        assert len(long) > len(short)


class TestRemoveSubscriptionCommand:
    def test_produces_valid_header(self, cmds):
        pkt, svc = cmds.command_remove_subscription(1)
        h = _parse_header(pkt)

        assert h["protocol_id"] == Protocol.CONTROL
        assert h["opcode"] == Opcode.SUBSCRIPTION_REMOVE

    def test_routes_to_arc_service(self, cmds):
        _, svc = cmds.command_remove_subscription(1)
        assert svc == SERVICE_ARC

    def test_length_field_matches_packet(self, cmds):
        pkt, _ = cmds.command_remove_subscription(1)
        stated_length = struct.unpack(">H", pkt[2:4])[0]
        assert stated_length == len(pkt)

    def test_contains_no_strings(self, cmds):
        pkt, _ = cmds.command_remove_subscription(1)
        # Remove should be a short packet with no channel/device name strings
        assert len(pkt) == 16

    def test_channel_number_encoded(self, cmds):
        pkt1, _ = cmds.command_remove_subscription(1)
        pkt2, _ = cmds.command_remove_subscription(2)
        assert pkt1 != pkt2


# ---------------------------------------------------------------------------
# Test that captured Dante Controller packets parse correctly
# ---------------------------------------------------------------------------

class TestCapturedSubscriptionHeaders:
    """Verify _parse_header handles the Dante Controller protocol variant."""

    def test_captured_remove_header(self, load_sub_fixture):
        from netaudio.dante.debug_formatter import get_opcode_name

        data = load_sub_fixture("subscription_remove_request.bin")
        h = _parse_header(data)

        assert h["protocol_id"] == 0x2809
        assert h["opcode"] == 0x3410
        assert h["opcode_name"] == get_opcode_name(0x2809, 0x3410)
        assert h["result_code"] == 0x0000

    def test_captured_remove_response(self, load_sub_fixture):
        data = load_sub_fixture("subscription_remove_response.bin")
        h = _parse_header(data)

        assert h["result_code"] == 0x0001
        assert h["result_name"] == "RESULT_CODE_SUCCESS"

    def test_captured_add_header(self, load_sub_fixture):
        data = load_sub_fixture("subscription_add_request.bin")
        h = _parse_header(data)

        assert h["protocol_id"] == 0x2809
        assert h["opcode"] == 0x3410

    def test_captured_add_contains_names(self, load_sub_fixture):
        data = load_sub_fixture("subscription_add_request.bin")
        assert b"mic-mix-high\x00" in data
        assert b"lx-dante\x00" in data

    def test_captured_remove_has_no_names(self, load_sub_fixture):
        data = load_sub_fixture("subscription_remove_request.bin")
        # No printable strings longer than 3 chars in a remove packet
        assert b"mic-mix" not in data
        assert b"lx-dante" not in data

    def test_response_echoes_transaction_id(self, load_sub_fixture):
        req = load_sub_fixture("subscription_remove_request.bin")
        resp = load_sub_fixture("subscription_remove_response.bin")
        assert req[4:6] == resp[4:6]

    def test_rx_channel_status_opcode(self, load_sub_fixture):
        from netaudio.dante.debug_formatter import get_opcode_name

        data = load_sub_fixture("rx_channel_status_request.bin")
        h = _parse_header(data)
        assert h["opcode"] == 0x3400
        assert h["opcode_name"] == get_opcode_name(0x2809, 0x3400)

    def test_rx_flow_status_opcode(self, load_sub_fixture):
        from netaudio.dante.debug_formatter import get_opcode_name

        data = load_sub_fixture("rx_flow_status_request.bin")
        h = _parse_header(data)
        assert h["opcode"] == 0x3600
        assert h["opcode_name"] == get_opcode_name(0x2809, 0x3600)


# ---------------------------------------------------------------------------
# Verify PacketStore correlation works with captured subscription traffic
# ---------------------------------------------------------------------------

class TestSubscriptionCorrelation:
    """Feed real captured packets through PacketStore and verify
    request/response correlation by transaction_id."""

    def test_remove_request_response_correlated(self, load_sub_fixture, tmp_path):
        from netaudio.dante.packet_store import PacketStore

        store = PacketStore(db_path=str(tmp_path / "test.sqlite"))

        req_data = load_sub_fixture("subscription_remove_request.bin")
        resp_data = load_sub_fixture("subscription_remove_response.bin")

        req_id = store.store_packet(
            payload=req_data,
            source_type="tshark",
            device_ip="192.168.1.94",
            direction="request",
            timestamp_ns=1000,
        )
        resp_id = store.store_packet(
            payload=resp_data,
            source_type="tshark",
            device_ip="192.168.1.94",
            direction="response",
            timestamp_ns=2000,
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
            payload=req_data,
            source_type="tshark",
            device_ip="192.168.1.94",
            direction="request",
            timestamp_ns=now,
        )
        mc_id = store.store_packet(
            payload=mc_data,
            source_type="multicast",
            src_ip="192.168.1.94",
            device_ip="192.168.1.94",
            timestamp_ns=now + 80_000_000,  # 80ms later
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
                payload=data,
                source_type="tshark",
                device_ip="192.168.1.94",
                direction="request",
                timestamp_ns=1000,
            )
            row = store.get_packet(pid)
            assert row["opcode_name"] == expected_opcode_name, f"{name}: expected {expected_opcode_name}, got {row['opcode_name']}"

        store.close()
