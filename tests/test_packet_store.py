import os
import struct
import tempfile

import pytest

from netaudio_lib.dante.debug_formatter import OPCODE_NAMES, PROTOCOL_NAMES
from netaudio_lib.dante.packet_store import PacketStore, _parse_header


def _make_packet(protocol=0x27FF, opcode=0x1002, transaction_id=0x0042, body=b""):
    """Build a minimal Dante request packet."""
    header = struct.pack(">HH", protocol, 8 + len(body))
    header += struct.pack(">HH", transaction_id, opcode)
    return header + body


def _make_response(protocol=0x27FF, opcode=0x1002, transaction_id=0x0042, result=0x0001, body=b""):
    """Build a minimal Dante response packet."""
    header = struct.pack(">HH", protocol, 10 + len(body))
    header += struct.pack(">HH", transaction_id, opcode)
    header += struct.pack(">H", result)
    return header + body


@pytest.fixture
def store(tmp_path):
    db_path = str(tmp_path / "test_capture.sqlite")
    s = PacketStore(db_path=db_path)
    yield s
    s.close()


class TestParseHeader:
    def test_valid_request(self):
        data = _make_packet(opcode=0x1002, transaction_id=0x0042)
        h = _parse_header(data)
        assert h["protocol_id"] == 0x27FF
        assert h["transaction_id"] == 0x0042
        assert h["opcode"] == 0x1002
        assert h["opcode_name"] == "Device Name"
        assert h["protocol_name"] == "PROTOCOL_ARC"

    def test_valid_response(self):
        data = _make_response(opcode=0x3010, result=0x0001)
        h = _parse_header(data)
        assert h["opcode"] == 0x3010
        assert h["opcode_name"] == "Subscription Set"
        assert h["result_code"] == 0x0001
        assert h["result_name"] == "RESULT_CODE_SUCCESS"

    def test_too_short(self):
        assert _parse_header(b"\x00\x01\x02") is None

    def test_unknown_opcode(self):
        data = _make_packet(opcode=0x9999)
        h = _parse_header(data)
        assert h["opcode"] == 0x9999
        assert h["opcode_name"] == "0x9999"


class TestStorePacket:
    def test_store_and_retrieve(self, store):
        pkt = _make_packet()
        pid = store.store_packet(
            payload=pkt,
            source_type="netaudio_request",
            device_ip="192.168.1.10",
            direction="request",
        )
        assert pid is not None

        packets = store.get_packets(limit=10)
        assert len(packets) == 1
        assert packets[0]["device_ip"] == "192.168.1.10"
        assert packets[0]["opcode"] == 0x1002

    def test_stores_hex(self, store):
        pkt = _make_packet(body=b"\xDE\xAD")
        store.store_packet(payload=pkt, source_type="tshark")
        packets = store.get_packets()
        assert packets[0]["payload_hex"] == pkt.hex()


class TestCorrelation:
    def test_transaction_id_correlation(self, store):
        req = _make_packet(transaction_id=0x0099, opcode=0x3010)
        resp = _make_response(transaction_id=0x0099, opcode=0x3010)

        req_id = store.store_packet(
            payload=req,
            source_type="netaudio_request",
            device_ip="192.168.1.50",
            direction="request",
        )
        resp_id = store.store_packet(
            payload=resp,
            source_type="netaudio_response",
            device_ip="192.168.1.50",
            direction="response",
        )

        packets = store.get_packets(limit=10)
        req_pkt = next(p for p in packets if p["id"] == req_id)
        resp_pkt = next(p for p in packets if p["id"] == resp_id)
        assert req_pkt["correlated_packet_id"] == resp_id
        assert resp_pkt["correlated_packet_id"] == req_id

    def test_no_correlation_different_transaction_id(self, store):
        req = _make_packet(transaction_id=0x0001)
        resp = _make_response(transaction_id=0x0002)

        store.store_packet(
            payload=req,
            source_type="netaudio_request",
            device_ip="192.168.1.50",
            direction="request",
        )
        store.store_packet(
            payload=resp,
            source_type="netaudio_response",
            device_ip="192.168.1.50",
            direction="response",
        )

        packets = store.get_packets()
        assert all(p["correlated_packet_id"] is None for p in packets)

    def test_temporal_correlation(self, store):
        """Multicast packet from device within 100ms of request to that device."""
        req = _make_packet(transaction_id=0x0055)
        now = 1_000_000_000_000  # 1 second in nanoseconds

        store.store_packet(
            payload=req,
            source_type="netaudio_request",
            device_ip="192.168.1.20",
            direction="request",
            timestamp_ns=now,
        )

        # Multicast from same device 50ms later
        mc_pkt = _make_packet(opcode=0x1003)
        mc_id = store.store_packet(
            payload=mc_pkt,
            source_type="multicast",
            src_ip="192.168.1.20",
            device_ip="192.168.1.20",
            timestamp_ns=now + 50_000_000,  # 50ms later
        )

        packets = store.get_packets()
        mc = next(p for p in packets if p["id"] == mc_id)
        assert mc["correlated_packet_id"] is not None

    def test_get_correlated_pairs(self, store):
        req = _make_packet(transaction_id=0x0077, opcode=0x3010)
        resp = _make_response(transaction_id=0x0077, opcode=0x3010)

        store.store_packet(
            payload=req,
            source_type="netaudio_request",
            device_ip="192.168.1.50",
            direction="request",
            timestamp_ns=1000,
        )
        store.store_packet(
            payload=resp,
            source_type="netaudio_response",
            device_ip="192.168.1.50",
            direction="response",
            timestamp_ns=2000,
        )

        pairs = store.get_correlated_pairs()
        assert len(pairs) == 1
        assert pairs[0][0]["direction"] == "request"
        assert pairs[0][1]["direction"] == "response"

    def test_get_correlated_pairs_opcode_filter(self, store):
        for opcode in [0x3010, 0x1002]:
            req = _make_packet(transaction_id=opcode, opcode=opcode)
            resp = _make_response(transaction_id=opcode, opcode=opcode)
            store.store_packet(
                payload=req,
                source_type="netaudio_request",
                device_ip="192.168.1.50",
                direction="request",
                timestamp_ns=opcode * 1000,
            )
            store.store_packet(
                payload=resp,
                source_type="netaudio_response",
                device_ip="192.168.1.50",
                direction="response",
                timestamp_ns=opcode * 1000 + 1,
            )

        pairs = store.get_correlated_pairs(opcode=0x3010)
        assert len(pairs) == 1


class TestExport:
    def test_export_fixture(self, store, tmp_path):
        pkt = _make_packet(opcode=0x1002)
        pid = store.store_packet(
            payload=pkt,
            source_type="netaudio_request",
            device_name="avio-usb-1",
            device_ip="192.168.1.10",
            direction="request",
        )

        output_dir = str(tmp_path / "fixtures")
        path = store.export_fixture(pid, output_dir)
        assert path is not None
        assert os.path.exists(path)
        assert path.endswith("_request.bin")
        assert "avio-usb-1" in path
        assert "device_name" in path

        with open(path, "rb") as f:
            assert f.read() == pkt

    def test_export_correlated_pair(self, store, tmp_path):
        req = _make_packet(transaction_id=0x00AA, opcode=0x3010)
        resp = _make_response(transaction_id=0x00AA, opcode=0x3010)

        req_id = store.store_packet(
            payload=req,
            source_type="netaudio_request",
            device_name="test-device",
            device_ip="192.168.1.5",
            direction="request",
            timestamp_ns=1000,
        )
        store.store_packet(
            payload=resp,
            source_type="netaudio_response",
            device_name="test-device",
            device_ip="192.168.1.5",
            direction="response",
            timestamp_ns=2000,
        )

        output_dir = str(tmp_path / "pairs")
        result = store.export_correlated_pair(req_id, output_dir)
        assert result is not None
        assert len(result) == 2
        assert os.path.exists(result[0])
        assert os.path.exists(result[1])


class TestStats:
    def test_stats(self, store):
        for i in range(3):
            store.store_packet(
                payload=_make_packet(transaction_id=i),
                source_type="netaudio_request",
                direction="request",
                timestamp_ns=i * 1000,
            )
        store.store_packet(
            payload=_make_packet(opcode=0x1003, transaction_id=0xFF),
            source_type="multicast",
            timestamp_ns=99000,
        )

        stats = store.get_stats()
        assert stats["total"] == 4
        assert stats["by_source"]["netaudio_request"] == 3
        assert stats["by_source"]["multicast"] == 1

    def test_get_packets_by_opcode(self, store):
        store.store_packet(
            payload=_make_packet(opcode=0x3010),
            source_type="tshark",
            timestamp_ns=1000,
        )
        store.store_packet(
            payload=_make_packet(opcode=0x1002),
            source_type="tshark",
            timestamp_ns=2000,
        )

        results = store.get_packets_by_opcode(0x3010)
        assert len(results) == 1
        assert results[0]["opcode"] == 0x3010
