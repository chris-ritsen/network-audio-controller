import json
import sqlite3
import struct
import zlib

from netaudio.capture import provenance
from netaudio.capture.provenance import (
    _decode_packet_payload,
    _extract_subscription_status_codes,
    _extract_seed_samples,
    _query_observed_subscription_statuses,
    _write_seed_samples,
)
from netaudio.capture.packets import ARC_PROTOCOLS, TARGET_PROTOCOLS


def test_arc_extended_success_is_not_reported_as_failure():
    payload = struct.pack(">HHHHH", 0x2729, 10, 1, 0x3000, 0x8112)

    assert _decode_packet_payload(payload)["status_ok"] is True


def _subscription_status_packet(status_code: int) -> bytes:
    record = struct.pack(">HHHHHHHHI", 1, 0, 0, 0, 0, 0, 0, status_code, 0)
    body = bytes([1, 1]) + record
    return struct.pack(">HHHHH", 0x27FF, 10 + len(body), 1, 0x3000, 1) + body


def _packet_database(payload: bytes) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE packets (
            id INTEGER PRIMARY KEY,
            protocol_id INTEGER,
            opcode INTEGER,
            opcode_name TEXT,
            timestamp_iso TEXT,
            payload BLOB,
            session_id INTEGER,
            src_ip TEXT,
            dst_ip TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO packets (
            id, protocol_id, opcode, opcode_name, timestamp_iso, payload,
            session_id, src_ip, dst_ip
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            41,
            0x27FF,
            0x3000,
            "query_rx_channels",
            "2026-07-18T10:00:00",
            zlib.compress(payload),
            7,
            "192.168.1.34",
            "192.168.1.156",
        ),
    )
    return connection


def test_query_observed_subscription_statuses_decompresses_payload():
    payload = _subscription_status_packet(0x0004)
    connection = _packet_database(payload)

    try:
        rows = _query_observed_subscription_statuses(connection)
    finally:
        connection.close()

    assert rows == [{"status_code": 0x0004, "seen": 1, "sample_id": 41}]


def test_subscription_status_extraction_stops_at_declared_record_count():
    payload = _subscription_status_packet(0x0004) + struct.pack(">HHHHHHHHI", 2, 0, 0, 0, 0, 0, 0, 0x9999, 0)

    assert _extract_subscription_status_codes(payload) == {0x0004}


def test_subscription_status_extraction_rejects_invalid_record_header():
    payload = bytearray(_subscription_status_packet(0x0004))
    payload[10] = 2

    assert _extract_subscription_status_codes(bytes(payload)) == set()


def test_seed_samples_write_decompressed_packet_bytes(tmp_path):
    payload = _subscription_status_packet(0x0004)
    connection = _packet_database(payload)

    try:
        rows, status_samples = _extract_seed_samples(connection)
    finally:
        connection.close()

    manifest_path = _write_seed_samples(
        rows,
        status_samples,
        tmp_path,
        db_path=tmp_path / "capture.sqlite",
    )
    manifest = json.loads(manifest_path.read_text())
    samples_by_type = {sample["sample_type"]: sample for sample in manifest["samples"]}

    assert (tmp_path / samples_by_type["protocol_opcode"]["file"]).read_bytes() == payload
    assert (tmp_path / samples_by_type["subscription_status"]["file"]).read_bytes() == payload


def test_capture_classifies_all_captured_arc_protocol_variants():
    assert ARC_PROTOCOLS == (0x2729, 0x27FF, 0x2801, 0x2809)
    assert set(ARC_PROTOCOLS).issubset(TARGET_PROTOCOLS)


def test_opcode_label_proof_accepts_evidence_from_every_arc_variant(monkeypatch):
    monkeypatch.setattr(
        provenance,
        "OPCODE_NAMES_BY_PROTOCOL",
        {0x2801: {0x2200: "query_tx_flows"}},
    )

    assert provenance._check_opcode_labels({(0x2729, 0x2200)}, set()) == []
