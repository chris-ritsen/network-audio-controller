import hashlib
import json
import sqlite3
import struct
import zlib
from pathlib import Path

import pytest

from netaudio.capture import provenance
from netaudio.capture.provenance import (
    _decode_packet_payload,
    _extract_subscription_status_codes,
    _extract_seed_samples,
    _query_observed_subscription_statuses,
    _write_seed_samples,
)


def test_arc_extended_success_is_not_reported_as_failure():
    payload = struct.pack(">HHHHH", 0x2729, 10, 1, 0x3000, 0x8112)

    assert _decode_packet_payload(payload)["status_ok"] is True


def _subscription_status_packet(status_code: int) -> bytes:
    strings_offset = 10 + 2 + 20
    record = struct.pack(
        ">HHHHHHHHI",
        1,
        0,
        0,
        strings_offset,
        strings_offset + 4,
        strings_offset + 8,
        0,
        status_code,
        0,
    )
    body = bytes([1, 1]) + record + b"tx1\x00dev\x00rx1\x00"
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
    strings_offset = 10 + 2 + 40
    first_record = struct.pack(">HHHHHHHHI", 1, 0, 0, strings_offset, strings_offset + 4, strings_offset + 8, 0, 4, 0)
    second_record = struct.pack(
        ">HHHHHHHHI", 2, 0, 0, strings_offset, strings_offset + 4, strings_offset + 8, 0, 0x9999, 0
    )
    body = bytes([1, 1]) + first_record + second_record + b"tx1\x00dev\x00rx1\x00"
    payload = struct.pack(">HHHHH", 0x27FF, 10 + len(body), 1, 0x3000, 1) + body

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


@pytest.mark.parametrize("protocol", [0x2729, 0x27FF, 0x2801, 0x2809, 0x280F])
def test_capture_classifies_all_captured_arc_protocol_variants(tmp_path, protocol):
    packet = struct.pack(">HHHHH", protocol, 10, 1, 0x2200, 1)
    (tmp_path / "response.bin").write_bytes(packet)
    opcodes, messages, statuses = provenance._scan_observed_from_fixtures(tmp_path)
    assert opcodes == {(protocol, 0x2200)}
    assert messages == set()
    assert statuses == set()


def test_opcode_label_proof_accepts_evidence_from_every_arc_variant(monkeypatch):
    monkeypatch.setattr(
        provenance,
        "_external_labels",
        lambda: ({(0x2801, 0x2200): "query_tx_flows"}, {}),
    )

    assert provenance._check_opcode_labels({(0x2729, 0x2200)}, set()) == []


def test_historical_regression_fixtures_are_digest_bound_and_explicitly_not_evidence():
    fixtures_directory = Path(__file__).parent / "fixtures"
    manifest = json.loads((fixtures_directory / "historical_capture_provenance.json").read_text())
    historical_files = {path.name for path in fixtures_directory.glob("*.bin")}

    assert manifest["status"] == "regression-only"
    assert "must not be cited as protocol evidence" in manifest["notice"]
    assert set(manifest["fixtures"]) == historical_files
    for filename, provenance_record in manifest["fixtures"].items():
        assert len(provenance_record["source_tree_introduced_by_commit"]) == 40
        assert hashlib.sha256((fixtures_directory / filename).read_bytes()).hexdigest() == provenance_record["sha256"]
