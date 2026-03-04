from __future__ import annotations

import asyncio
import json
import socket
import sqlite3
import struct
import sys
import time
from pathlib import Path
from typing import Optional

import typer

from netaudio_lib.dante.clean_labels import (
    load_clean_labels,
    load_clean_subscription_status_labels,
    resolve_clean_labels_path,
    save_clean_labels,
)
from netaudio_lib.dante.debug_formatter import (
    OPCODE_NAMES_BY_PROTOCOL,
    SETTINGS_MESSAGE_TYPE_NAMES,
)
from netaudio_lib.dante.packet_store import PacketStore

from netaudio.commands.capture_helpers import (
    ARC_PROTOCOLS,
    TARGET_PROTOCOLS,
    _compact_hexdump,
    _default_fixture_root,
    _default_label_overrides_path,
    _default_provenance_output_dir,
    _hexdump,
    _label_packet,
    _load_capture_profile,
    _normalize_marker_label,
    _normalize_marker_type,
    _parse_field_spec,
    _parse_set_message,
    _parse_set_opcode,
    _parse_set_status,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_facts_path,
    _resolve_marker_window,
    _resolve_session_reference,
    _valid_label,
)


app = typer.Typer(help="Wire-observation provenance workflows.", no_args_is_help=True)


def _extract_subscription_status_codes(payload: bytes) -> set[int]:
    if len(payload) < 10:
        return set()

    protocol = struct.unpack(">H", payload[0:2])[0]
    if protocol not in ARC_PROTOCOLS:
        return set()

    opcode = struct.unpack(">H", payload[6:8])[0]
    if opcode != 0x3000:
        return set()

    body = payload[10:]
    if len(body) < 4:
        return set()

    record_size = 20
    record_start = 2
    record_index = 0
    statuses: set[int] = set()

    while record_start + record_size <= len(body):
        record = body[record_start : record_start + record_size]
        if len(record) < record_size:
            break

        (
            channel_number,
            _flags,
            _sample_rate_offset,
            _tx_channel_offset,
            _tx_device_offset,
            rx_channel_offset,
            _status,
            subscription_status_code,
        ) = struct.unpack(">HHHHHHHH", record[:16])

        if channel_number == 0 or rx_channel_offset > len(body) + 100:
            break

        statuses.add(subscription_status_code)
        record_start += record_size
        record_index += 1
        if record_index > 64:
            break

    return statuses


def _build_packet_scope(
    *,
    session_id: int | None,
    start_ns: int | None,
    end_ns: int | None,
    device_ip: str | None,
) -> tuple[str, list]:
    clauses: list[str] = []
    params: list = []

    if session_id is not None:
        clauses.append("session_id = ?")
        params.append(session_id)
    if device_ip:
        clauses.append("(src_ip = ? OR dst_ip = ?)")
        params.extend([device_ip, device_ip])
    if start_ns is not None:
        clauses.append("timestamp_ns >= ?")
        params.append(start_ns)
    if end_ns is not None:
        clauses.append("timestamp_ns <= ?")
        params.append(end_ns)

    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params


def _query_observed_opcodes(
    conn: sqlite3.Connection,
    *,
    session_id: int | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
    device_ip: str | None = None,
) -> list[sqlite3.Row]:
    protocol_csv = ",".join(str(v) for v in TARGET_PROTOCOLS if v != 0xFFFF)
    scope_sql, scope_params = _build_packet_scope(
        session_id=session_id,
        start_ns=start_ns,
        end_ns=end_ns,
        device_ip=device_ip,
    )
    query = f"""
        SELECT protocol_id, opcode, COUNT(*) AS seen, MIN(id) AS sample_id
        FROM packets
        WHERE protocol_id IN ({protocol_csv}) AND opcode IS NOT NULL
        {scope_sql}
        GROUP BY protocol_id, opcode
        ORDER BY protocol_id, opcode
    """
    return conn.execute(query, scope_params).fetchall()


def _query_observed_messages(
    conn: sqlite3.Connection,
    *,
    session_id: int | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
    device_ip: str | None = None,
) -> list[sqlite3.Row]:
    scope_sql, scope_params = _build_packet_scope(
        session_id=session_id,
        start_ns=start_ns,
        end_ns=end_ns,
        device_ip=device_ip,
    )
    query = f"""
        SELECT opcode AS message_type, COUNT(*) AS seen, MIN(id) AS sample_id
        FROM packets
        WHERE protocol_id = 65535 AND opcode IS NOT NULL
        {scope_sql}
        GROUP BY opcode
        ORDER BY opcode
    """
    return conn.execute(query, scope_params).fetchall()


def _query_observed_subscription_statuses(
    conn: sqlite3.Connection,
    *,
    session_id: int | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
    device_ip: str | None = None,
) -> list[dict[str, int]]:
    arc_protocol_csv = ",".join(str(v) for v in ARC_PROTOCOLS)
    scope_sql, scope_params = _build_packet_scope(
        session_id=session_id,
        start_ns=start_ns,
        end_ns=end_ns,
        device_ip=device_ip,
    )
    rows = conn.execute(
        f"""
        SELECT id, payload
        FROM packets
        WHERE protocol_id IN ({arc_protocol_csv})
          AND opcode = 12288
          AND payload IS NOT NULL
          {scope_sql}
        ORDER BY id
        """,
        scope_params,
    ).fetchall()

    stats: dict[int, dict[str, int]] = {}
    for row in rows:
        packet_id = int(row["id"])
        payload = row["payload"]
        codes = _extract_subscription_status_codes(payload)
        for code in codes:
            entry = stats.get(code)
            if entry is None:
                stats[code] = {"status_code": code, "seen": 1, "sample_id": packet_id}
            else:
                entry["seen"] += 1
                if packet_id < entry["sample_id"]:
                    entry["sample_id"] = packet_id
    return [stats[code] for code in sorted(stats)]


def _fixture_name(protocol_id: int, opcode: int, packet_id: int) -> str:
    kind = "message" if protocol_id == 0xFFFF else "opcode"
    return f"protocol_{protocol_id:04x}_{kind}_{opcode:04x}_id_{packet_id}.bin"


def _subscription_status_fixture_name(status_code: int, packet_id: int) -> str:
    return f"subscription_status_{status_code:04x}_id_{packet_id}.bin"


def _extract_seed_samples(
    conn: sqlite3.Connection,
    *,
    session_id: int | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
    device_ip: str | None = None,
) -> tuple[list[sqlite3.Row], list[dict[str, object]]]:
    protocol_csv = ",".join(str(p) for p in TARGET_PROTOCOLS)
    scope_sql, scope_params = _build_packet_scope(
        session_id=session_id,
        start_ns=start_ns,
        end_ns=end_ns,
        device_ip=device_ip,
    )

    query = f"""
        SELECT p.id, p.protocol_id, p.opcode, p.opcode_name, p.timestamp_iso, p.payload
        FROM packets p
        JOIN (
            SELECT protocol_id, opcode, MIN(id) AS sample_id
            FROM packets
            WHERE protocol_id IN ({protocol_csv}) AND opcode IS NOT NULL
            {scope_sql}
            GROUP BY protocol_id, opcode
        ) s ON p.id = s.sample_id
        ORDER BY p.protocol_id, p.opcode
    """
    rows = conn.execute(query, scope_params).fetchall()

    arc_protocol_csv = ",".join(str(p) for p in ARC_PROTOCOLS)
    status_rows = conn.execute(
        f"""
        SELECT id, protocol_id, opcode, opcode_name, timestamp_iso, payload
        FROM packets
        WHERE protocol_id IN ({arc_protocol_csv})
          AND opcode = 12288
          AND payload IS NOT NULL
          {scope_sql}
        ORDER BY id
        """,
        scope_params,
    ).fetchall()

    status_samples_by_code: dict[int, dict[str, object]] = {}
    for row in status_rows:
        packet_id = int(row["id"])
        codes = _extract_subscription_status_codes(row["payload"])
        for status_code in sorted(codes):
            if status_code in status_samples_by_code:
                continue
            status_samples_by_code[status_code] = {
                "status_code": status_code,
                "packet_id": packet_id,
                "protocol_id": int(row["protocol_id"]),
                "opcode": int(row["opcode"]),
                "opcode_name": row["opcode_name"],
                "timestamp_iso": row["timestamp_iso"],
                "payload": row["payload"],
            }

    status_samples = [status_samples_by_code[code] for code in sorted(status_samples_by_code)]
    return rows, status_samples


def _write_seed_samples(
    rows: list[sqlite3.Row],
    status_samples: list[dict[str, object]],
    output_dir: Path,
    *,
    db_path: Path,
    session_id: int | None = None,
    from_label: str | None = None,
    to_label: str | None = None,
    device_ip: str | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "db_path": str(db_path),
        "count": len(rows) + len(status_samples),
        "scope": {
            "session_id": session_id,
            "from_label": from_label,
            "to_label": to_label,
            "device_ip": device_ip,
        },
        "samples": [],
    }

    for row in rows:
        protocol_id = int(row["protocol_id"])
        opcode = int(row["opcode"])
        packet_id = int(row["id"])
        payload = row["payload"]

        file_name = _fixture_name(protocol_id, opcode, packet_id)
        (output_dir / file_name).write_bytes(payload)
        manifest["samples"].append(
            {
                "file": file_name,
                "packet_id": packet_id,
                "protocol_id": protocol_id,
                "protocol_hex": f"0x{protocol_id:04X}",
                "opcode": opcode,
                "opcode_hex": f"0x{opcode:04X}",
                "opcode_name": row["opcode_name"],
                "timestamp_iso": row["timestamp_iso"],
                "sample_type": "protocol_opcode",
            }
        )

    for sample in status_samples:
        status_code = int(sample["status_code"])
        packet_id = int(sample["packet_id"])
        protocol_id = int(sample["protocol_id"])
        opcode = int(sample["opcode"])

        file_name = _subscription_status_fixture_name(status_code, packet_id)
        (output_dir / file_name).write_bytes(sample["payload"])
        manifest["samples"].append(
            {
                "file": file_name,
                "packet_id": packet_id,
                "protocol_id": protocol_id,
                "protocol_hex": f"0x{protocol_id:04X}",
                "opcode": opcode,
                "opcode_hex": f"0x{opcode:04X}",
                "opcode_name": sample["opcode_name"],
                "status_code": status_code,
                "status_hex": f"0x{status_code:04X}",
                "timestamp_iso": sample["timestamp_iso"],
                "sample_type": "subscription_status",
            }
        )

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest_path


def _scan_observed_from_fixtures(fixture_root: Path) -> tuple[set[tuple[int, int]], set[int], set[int]]:
    import tarfile

    observed_opcodes: set[tuple[int, int]] = set()
    observed_messages: set[int] = set()
    observed_subscription_statuses: set[int] = set()

    def _process_payload(payload: bytes) -> None:
        if len(payload) < 2:
            return

        protocol = struct.unpack(">H", payload[0:2])[0]

        if protocol in (0x27FF, 0x2809, 0x1200) and len(payload) >= 8:
            opcode = struct.unpack(">H", payload[6:8])[0]
            observed_opcodes.add((protocol, opcode))

        if protocol == 0xFFFF and len(payload) >= 28:
            message_type = struct.unpack(">H", payload[26:28])[0]
            observed_messages.add(message_type)

        observed_subscription_statuses.update(_extract_subscription_status_codes(payload))

    for fixture in fixture_root.rglob("*.bin"):
        _process_payload(fixture.read_bytes())

    for archive in fixture_root.rglob("*.tar.gz"):
        try:
            with tarfile.open(archive, "r:gz") as tar:
                for member in tar.getmembers():
                    if member.name.endswith(".bin"):
                        f = tar.extractfile(member)
                        if f:
                            _process_payload(f.read())
        except Exception:
            pass

    return observed_opcodes, observed_messages, observed_subscription_statuses


def _load_label_overrides(overrides_path: Path) -> tuple[set[tuple[int, int]], set[int], set[int]]:
    if not overrides_path.exists():
        return set(), set(), set()

    data = json.loads(overrides_path.read_text())
    opcode_overrides = set()
    for key in data.get("opcode_overrides", []):
        protocol_hex, opcode_hex = key.split(":")
        opcode_overrides.add((int(protocol_hex, 16), int(opcode_hex, 16)))

    message_overrides = {int(v, 16) for v in data.get("message_overrides", [])}
    status_overrides = {int(v, 16) for v in data.get("subscription_status_overrides", [])}
    return opcode_overrides, message_overrides, status_overrides


def _check_opcode_labels(
    observed: set[tuple[int, int]],
    overrides: set[tuple[int, int]],
) -> list[str]:
    failures: list[str] = []
    arc_variant_protocols = {0x27FF, 0x2809}

    for protocol, mapping in OPCODE_NAMES_BY_PROTOCOL.items():
        for opcode, label in mapping.items():
            if not label or label == f"0x{opcode:04X}":
                continue
            key = (protocol, opcode)
            if key in observed or key in overrides:
                continue
            if protocol in arc_variant_protocols:
                if (0x27FF, opcode) in observed or (0x2809, opcode) in observed:
                    continue
            failures.append(f"unproven opcode label: protocol=0x{protocol:04X} opcode=0x{opcode:04X} label={label!r}")

    return failures


def _check_message_labels(
    observed: set[int],
    overrides: set[int],
) -> list[str]:
    failures: list[str] = []
    for message_type, label in SETTINGS_MESSAGE_TYPE_NAMES.items():
        if not label or label == f"msg:0x{message_type:04X}":
            continue
        if message_type in observed or message_type in overrides:
            continue
        failures.append(f"unproven settings message label: msg=0x{message_type:04X} label={label!r}")
    return failures


def _interactive_label_opcodes(
    rows: list[sqlite3.Row],
    opcode_labels: dict[tuple[int, int], str],
) -> bool:
    changed = False
    for index, row in enumerate(rows, start=1):
        protocol_id = int(row["protocol_id"])
        opcode = int(row["opcode"])
        seen = int(row["seen"])
        sample_id = int(row["sample_id"])
        key = (protocol_id, opcode)
        if key in opcode_labels:
            continue

        prompt = (
            f"[{index}/{len(rows)}] protocol=0x{protocol_id:04X} opcode=0x{opcode:04X} "
            f"seen={seen} sample_id={sample_id}\n"
            "label (blank=skip, q=quit): "
        )
        label = input(prompt).strip()
        if not label:
            continue
        if label.lower() in {"q", "quit", "exit"}:
            return changed
        if not _valid_label(label):
            print("invalid label, skipping")
            continue

        opcode_labels[key] = label
        changed = True
    return changed


def _interactive_label_messages(
    rows: list[sqlite3.Row],
    message_labels: dict[int, str],
) -> bool:
    changed = False
    for index, row in enumerate(rows, start=1):
        message_type = int(row["message_type"])
        seen = int(row["seen"])
        sample_id = int(row["sample_id"])
        if message_type in message_labels:
            continue

        prompt = (
            f"[{index}/{len(rows)}] message_type=0x{message_type:04X} "
            f"seen={seen} sample_id={sample_id}\n"
            "label (blank=skip, q=quit): "
        )
        label = input(prompt).strip()
        if not label:
            continue
        if label.lower() in {"q", "quit", "exit"}:
            return changed
        if not _valid_label(label):
            print("invalid label, skipping")
            continue

        message_labels[message_type] = label
        changed = True
    return changed


def _interactive_label_statuses(
    rows: list[dict[str, int]],
    status_labels: dict[int, dict[str, object]],
) -> bool:
    changed = False
    for index, row in enumerate(rows, start=1):
        status_code = int(row["status_code"])
        seen = int(row["seen"])
        sample_id = int(row["sample_id"])
        if status_code in status_labels:
            continue

        prompt = (
            f"[{index}/{len(rows)}] subscription_status=0x{status_code:04X} ({status_code}) "
            f"seen={seen} sample_id={sample_id}\n"
            "state,label (blank=skip, q=quit): "
        )
        value = input(prompt).strip()
        if not value:
            continue
        if value.lower() in {"q", "quit", "exit"}:
            return changed

        if "," in value:
            state, label = value.split(",", 1)
            state = state.strip() or "unknown"
            label = label.strip()
        else:
            state = "unknown"
            label = value

        if not _valid_label(label):
            print("invalid status label, skipping")
            continue

        status_labels[status_code] = {
            "state": state,
            "label": label,
            "detail": None,
            "labels": [label],
        }
        changed = True
    return changed


def _resolve_provenance_scope(
    store: PacketStore,
    *,
    id: int | None,
    session: str | None,
    from_label: str | None,
    to_label: str | None,
) -> tuple[int | None, int | None, int | None]:
    if id is None and not session and not from_label and not to_label:
        return None, None, None

    resolved_session_id, _ = _resolve_session_reference(
        store,
        session_id=id,
        session=session,
        default_selector="latest",
    )
    start_ns, end_ns = _resolve_marker_window(
        store,
        session_id=resolved_session_id,
        from_label=from_label,
        to_label=to_label,
    )
    return resolved_session_id, start_ns, end_ns


KNOWN_PROTOCOL_NAMES = {
    0x2729: "DEVICE_CONFIG",
    0x27FF: "CONTROL",
    0x2809: "AES67_CONFIG",
    0xFFFF: "SETTINGS",
    0x1200: "CMC",
}

KNOWN_OPCODE_NAMES = {
    0x1000: "CHANNEL_COUNT",
    0x1001: "DEVICE_NAME_SET",
    0x1002: "DEVICE_NAME",
    0x1003: "DEVICE_INFO",
    0x1100: "DEVICE_SETTINGS",
    0x1101: "SET_LATENCY",
}


def _verify_parse_header(data: bytes) -> dict | None:
    if len(data) < 8:
        return None

    protocol_id = struct.unpack(">H", data[0:2])[0]

    if protocol_id == 0xFFFF and len(data) >= 28:
        message_type = struct.unpack(">H", data[26:28])[0]
        return {
            "protocol_id": protocol_id,
            "protocol_name": "SETTINGS",
            "opcode": message_type,
            "transaction_id": None,
            "status": None,
        }

    transaction_id = struct.unpack(">H", data[4:6])[0]
    opcode = struct.unpack(">H", data[6:8])[0]
    status = struct.unpack(">H", data[8:10])[0] if len(data) >= 10 else None

    return {
        "protocol_id": protocol_id,
        "protocol_name": KNOWN_PROTOCOL_NAMES.get(protocol_id, f"0x{protocol_id:04X}"),
        "opcode": opcode,
        "opcode_name": KNOWN_OPCODE_NAMES.get(opcode, f"0x{opcode:04X}"),
        "transaction_id": transaction_id,
        "status": status,
    }


def _verify_single_bundle(bundle_path: Path) -> bool:
    manifest_path = bundle_path / "manifest.json"

    if not manifest_path.exists():
        print(f"FAIL: No manifest.json in {bundle_path}")
        return False

    with open(manifest_path) as manifest_file:
        manifest = json.load(manifest_file)

    print(f"Bundle: {bundle_path.name}")
    print(f"  Session: {manifest.get('session_name', 'unknown')} (id={manifest.get('session_id')})")
    print(f"  Scope: {json.dumps(manifest.get('scope', {}))}")
    print(f"  Packet count: {manifest.get('count', 0)}")

    markers = manifest.get("markers", [])
    samples = manifest.get("samples", [])

    if markers:
        print(f"\n  Markers ({len(markers)}):")
        for marker_item in markers:
            marker_type = marker_item.get("marker_type", "?")
            label = marker_item.get("label", "?")
            note = marker_item.get("note", "")
            data = marker_item.get("data")
            prefix = {"hypothesis": "H", "observation": "O", "system": "S", "step": ">"}.get(marker_type, "?")
            line = f"    [{prefix}] {label}"
            if note:
                line += f" — {note}"
            print(line)
            if data:
                print(f"        data: {json.dumps(data, default=str)}")

    files = {}
    for bin_file in bundle_path.glob("*.bin"):
        files[bin_file.name] = bin_file.read_bytes()

    all_ok = True
    verified_count = 0

    print(f"\n  Packets ({len(samples)}):")
    for sample in samples:
        filename = sample["file"]
        direction = sample.get("direction", "?")
        expected_protocol = sample.get("protocol_id")
        expected_opcode = sample.get("opcode")

        data = files.get(filename)
        if data is None:
            print(f"    MISSING: {filename}")
            all_ok = False
            continue

        if len(data) == 0:
            print(f"    EMPTY: {filename}")
            all_ok = False
            continue

        header = _verify_parse_header(data)
        if header is None:
            print(f"    UNPARSEABLE: {filename} ({len(data)} bytes)")
            all_ok = False
            continue

        protocol_match = expected_protocol is None or header["protocol_id"] == expected_protocol
        opcode_match = expected_opcode is None or header["opcode"] == expected_opcode

        if not protocol_match or not opcode_match:
            print(f"    MISMATCH: {filename}")
            if not protocol_match:
                print(f"      expected protocol 0x{expected_protocol:04X}, got 0x{header['protocol_id']:04X}")
            if not opcode_match:
                print(f"      expected opcode 0x{expected_opcode:04X}, got 0x{header['opcode']:04X}")
            all_ok = False
            continue

        status_str = ""
        if header.get("status") is not None:
            status_str = f" status=0x{header['status']:04X}"

        opcode_name = header.get("opcode_name", f"0x{header['opcode']:04X}")
        print(f"    OK: {filename} [{direction}] {header['protocol_name']} {opcode_name}{status_str} ({len(data)}B)")
        verified_count += 1

    hypotheses = [m for m in markers if m.get("marker_type") == "hypothesis"]
    observations = [m for m in markers if m.get("marker_type") == "observation"]

    print(f"\n  Verification Summary:")
    print(f"    Packets verified: {verified_count}/{len(samples)}")
    print(f"    Hypotheses: {len(hypotheses)}")
    print(f"    Observations: {len(observations)}")

    if all_ok and verified_count == len(samples):
        print(f"\n  RESULT: PASS — all {verified_count} packets verified, {len(observations)} observations recorded")
        return True

    print(f"\n  RESULT: FAIL — {len(samples) - verified_count} packet(s) could not be verified")
    return False


def _decode_packet_payload(data: bytes) -> dict:
    result = {}
    if len(data) < 8:
        result["raw_hex"] = data.hex()
        return result

    protocol_id = struct.unpack(">H", data[0:2])[0]
    length = struct.unpack(">H", data[2:4])[0]
    result["protocol"] = f"0x{protocol_id:04X}"
    result["protocol_name"] = KNOWN_PROTOCOL_NAMES.get(protocol_id, f"0x{protocol_id:04X}")
    result["declared_length"] = length
    result["actual_length"] = len(data)

    if protocol_id == 0xFFFF:
        if len(data) >= 28:
            message_type = struct.unpack(">H", data[26:28])[0]
            result["message_type"] = f"0x{message_type:04X}"
        result["raw_hex"] = data.hex()
        return result

    transaction_id = struct.unpack(">H", data[4:6])[0]
    opcode = struct.unpack(">H", data[6:8])[0]
    result["transaction_id"] = f"0x{transaction_id:04X}"
    result["opcode"] = f"0x{opcode:04X}"
    result["opcode_name"] = KNOWN_OPCODE_NAMES.get(opcode, f"0x{opcode:04X}")

    if len(data) >= 10:
        status = struct.unpack(">H", data[8:10])[0]
        result["status"] = f"0x{status:04X}"
        result["status_ok"] = status == 0x0001

    result["raw_hex"] = data.hex()

    if len(data) >= 12:
        result["payload_body_hex"] = data[10:].hex()

    words = []
    for offset in range(0, len(data), 4):
        chunk = data[offset : offset + 4]
        if len(chunk) == 4:
            val = struct.unpack(">I", chunk)[0]
            words.append({"offset": offset, "hex": chunk.hex(), "u32": val})
    result["words"] = words

    return result


def _format_audit_packet(data: bytes, indent: str = "      ") -> str:
    decoded = _decode_packet_payload(data)
    lines = []
    lines.append(f"{indent}Protocol: {decoded.get('protocol_name', '?')} ({decoded.get('protocol', '?')})")

    if "opcode" in decoded:
        lines.append(f"{indent}Opcode:   {decoded.get('opcode_name', '?')} ({decoded['opcode']})")

    if "transaction_id" in decoded:
        lines.append(f"{indent}TxnID:    {decoded['transaction_id']}")

    if "status" in decoded:
        ok = "OK" if decoded.get("status_ok") else "FAIL"
        lines.append(f"{indent}Status:   {decoded['status']} ({ok})")

    lines.append(f"{indent}Length:   {decoded.get('actual_length', '?')} bytes")
    lines.append(f"{indent}Hex:      {decoded.get('raw_hex', '')}")

    if decoded.get("words"):
        lines.append(f"{indent}Words:")
        for word in decoded["words"]:
            annotation = ""
            if word["u32"] > 0 and word["u32"] <= 10_000_000:
                us = word["u32"] / 1000
                ms = word["u32"] / 1_000_000
                if us == int(us) and 1 <= us <= 10000:
                    annotation = f"  ({us:.0f} us = {ms} ms)"
            lines.append(f"{indent}  [{word['offset']:3d}] 0x{word['hex']} = {word['u32']}{annotation}")

    return "\n".join(lines)


def _load_bundle(bundle_path: Path) -> tuple[dict, dict[str, bytes]]:
    import tarfile

    if bundle_path.suffix == ".gz" and bundle_path.stem.endswith(".tar"):
        with tarfile.open(bundle_path, "r:gz") as tar:
            manifest = None
            files = {}
            for member in tar.getmembers():
                name = Path(member.name).name
                data = tar.extractfile(member)
                if data is None:
                    continue
                content = data.read()
                if name == "manifest.json":
                    manifest = json.loads(content)
                else:
                    files[name] = content
            if manifest is None:
                raise typer.Exit(f"No manifest.json in {bundle_path}")
            return manifest, files

    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.exists():
        raise typer.Exit(f"No manifest.json in {bundle_path}")

    with open(manifest_path) as manifest_file:
        manifest = json.load(manifest_file)

    files = {}
    for bin_file in bundle_path.glob("*.bin"):
        files[bin_file.name] = bin_file.read_bytes()

    return manifest, files


def _audit_single_bundle(bundle_path: Path) -> bool:
    try:
        manifest, files = _load_bundle(bundle_path)
    except SystemExit:
        print(f"FAIL: No manifest.json in {bundle_path}")
        return False

    scope = manifest.get("scope", {})
    session_count = manifest.get("session_packet_count", manifest.get("count", 0))
    evidence_count = manifest.get("evidence_packet_count", 0)

    print(f"{'=' * 72}")
    print(f"PROVENANCE AUDIT: {manifest.get('session_name', 'unknown')}")
    print(f"{'=' * 72}")
    print(f"  Session ID:      {manifest.get('session_id')}")
    print(f"  Target Device:   {scope.get('device_name', '?')} ({scope.get('device_ip', '?')})")
    print(f"  Session Packets: {session_count}")
    print(f"  Evidence Packets: {evidence_count}")

    markers = manifest.get("markers", [])
    samples = manifest.get("samples", [])

    marker_type_icons = {
        "hypothesis": "HYPOTHESIS",
        "observation": "OBSERVATION",
        "step": "STEP",
        "evidence": "EVIDENCE",
        "system": "SYSTEM",
    }

    print(f"\n{'─' * 72}")
    print("TIMELINE")
    print(f"{'─' * 72}")

    all_events = []

    for marker_item in markers:
        all_events.append({
            "type": "marker",
            "timestamp_ns": marker_item.get("timestamp_ns", 0),
            "data": marker_item,
        })

    for sample in samples:
        all_events.append({
            "type": "packet",
            "timestamp_ns": sample.get("timestamp_ns", 0),
            "data": sample,
        })

    all_events.sort(key=lambda event: event["timestamp_ns"])

    for event in all_events:
        ts_iso = ""
        if event["type"] == "marker":
            marker_item = event["data"]
            ts_iso = marker_item.get("timestamp_iso", "")
            marker_type = marker_item.get("marker_type", "?")
            label = marker_item.get("label", "?")
            note = marker_item.get("note", "")
            icon = marker_type_icons.get(marker_type, marker_type.upper())

            if marker_type == "system":
                print(f"\n  [{ts_iso}] {icon}: {note}")
                continue

            print(f"\n  [{ts_iso}] {icon}: {label}")
            if note:
                print(f"    {note}")

            marker_data = marker_item.get("data")
            if marker_data:
                if marker_type == "hypothesis":
                    for key, value in marker_data.items():
                        print(f"    {key}: {value}")
                elif marker_type == "observation":
                    for key, value in marker_data.items():
                        if key == "response_hex":
                            continue
                        print(f"    {key}: {value}")
                elif marker_type == "evidence":
                    query = marker_data.get("query", {})
                    active_filters = {k: v for k, v in query.items() if v is not None}
                    if active_filters:
                        print(f"    Query: {json.dumps(active_filters)}")
                    filters = marker_data.get("filters", {})
                    if filters:
                        filter_parts = [f"{k}={v}" for k, v in filters.items()]
                        print(f"    Filters: {', '.join(filter_parts)}")
                    evidence_pids = marker_data.get("packet_ids", [])
                    if evidence_pids:
                        print(f"    Packets: {len(evidence_pids)}")
                        sample_lookup = {s.get("packet_id"): s for s in samples}
                        for pid in evidence_pids[:20]:
                            sample_entry = sample_lookup.get(pid)
                            if sample_entry:
                                direction = sample_entry.get("direction") or "multicast"
                                opcode_hex = sample_entry.get("opcode_hex", "?")
                                src = f"{sample_entry.get('src_ip', '?')}:{sample_entry.get('src_port', '?')}"
                                dst = f"{sample_entry.get('dst_ip', '?')}:{sample_entry.get('dst_port', '?')}"
                                filename = sample_entry.get("file", "")
                                print(f"      #{pid} {direction:9s} {opcode_hex} {src} -> {dst}")
                                payload = files.get(filename)
                                if payload:
                                    print(_hexdump(payload, indent="        "))
                        if len(evidence_pids) > 20:
                            print(f"      ... and {len(evidence_pids) - 20} more")
                    else:
                        print(f"    Matched: {marker_data.get('packet_count', 0)} packets")

        elif event["type"] == "packet":
            sample = event["data"]
            ts_iso = sample.get("timestamp_iso", "")
            direction = sample.get("direction", "?")
            filename = sample.get("file", "?")
            is_evidence = sample.get("evidence", False)
            source_label = "EVIDENCE PACKET" if is_evidence else "PACKET"
            source_session = f" (from session {sample['session_id']})" if is_evidence and sample.get("session_id") else ""

            src_dst = ""
            if sample.get("src_ip") and sample.get("dst_ip"):
                src_dst = f" {sample['src_ip']}:{sample.get('src_port', '?')} → {sample['dst_ip']}:{sample.get('dst_port', '?')}"

            print(f"\n  [{ts_iso}] {source_label} [{direction}]{src_dst}{source_session}")

            payload = files.get(filename)
            if payload:
                print(_format_audit_packet(payload))

    print(f"\n{'─' * 72}")
    print("VERIFICATION SUMMARY")
    print(f"{'─' * 72}")

    hypotheses = [m for m in markers if m.get("marker_type") == "hypothesis"]
    observations = [m for m in markers if m.get("marker_type") == "observation"]
    evidence_markers = [m for m in markers if m.get("marker_type") == "evidence"]

    verified_count = 0
    for sample in samples:
        data = files.get(sample["file"])
        if data and len(data) > 0:
            verified_count += 1

    print(f"  Packets present:  {verified_count}/{len(samples)}")
    print(f"  Hypotheses:       {len(hypotheses)}")
    print(f"  Observations:     {len(observations)}")
    print(f"  Evidence queries: {len(evidence_markers)}")

    if verified_count == len(samples):
        print(f"\n  RESULT: PASS")
        return True

    print(f"\n  RESULT: FAIL — {len(samples) - verified_count} packet(s) missing")
    return False


def _extract_field(payload: bytes, field: dict) -> dict | None:
    name = field.get("name", "?")
    offset = field.get("offset", 0)
    length = field.get("length", 0)
    dtype = field.get("dtype", "")

    if offset + length > len(payload):
        return None

    raw = payload[offset : offset + length]

    try:
        if dtype == "uint8" and length == 1:
            value = raw[0]
            display = str(value)
        elif dtype == "uint16_be" and length == 2:
            value = struct.unpack(">H", raw)[0]
            display = f"0x{value:04X}" if name in ("opcode", "protocol_id", "message_type", "status") else str(value)
        elif dtype == "uint32_be" and length == 4:
            value = struct.unpack(">I", raw)[0]
            display = str(value)
        elif dtype == "int32_be" and length == 4:
            value = struct.unpack(">i", raw)[0]
            display = str(value)
        elif dtype == "ascii":
            value = raw.rstrip(b"\x00").decode("ascii", errors="replace")
            display = value if value else "(empty)"
        elif dtype == "ipv4" and length == 4:
            display = f"{raw[0]}.{raw[1]}.{raw[2]}.{raw[3]}"
            value = display
        elif dtype == "hex":
            display = ":".join(f"{b:02x}" for b in raw)
            value = display
        else:
            display = raw.hex()
            value = display
    except (struct.error, UnicodeDecodeError):
        return None

    profile_key = None
    if name in ("current_name", "factory_name", "receiver_name"):
        profile_key = name
    elif "latency" in name:
        display = f"{value} ({value / 1000:.1f} us)" if isinstance(value, int) and value > 0 else display
        profile_key = name

    return {"name": name, "display": display, "value": value, "profile_key": profile_key}


@app.command("seed")
def provenance_seed(
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    out: Optional[str] = typer.Option(None, "--out", help="Output fixture directory."),
    clean: bool = typer.Option(False, "--clean", help="Delete existing .bin fixtures and manifest before writing."),
    id: Optional[int] = typer.Option(None, "--id", help="Session ID scope."),
    session: Optional[str] = typer.Option(None, "--session", help="Session reference (ID, name, latest, active)."),
    from_label: Optional[str] = typer.Option(
        None, "--from-label", help="Start at first marker label in scoped session."
    ),
    to_label: Optional[str] = typer.Option(None, "--to-label", help="End at last marker label in scoped session."),
    device_ip: Optional[str] = typer.Option(
        None, "--device-ip", help="Only include packets where src or dst matches this IP."
    ),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    db_path = Path(resolved_db).expanduser().resolve()
    if not db_path.exists():
        raise typer.Exit(f"capture database not found: {db_path}")

    output_dir = Path(out).expanduser().resolve() if out else _default_provenance_output_dir().resolve()
    if clean and output_dir.exists():
        for fixture in output_dir.glob("*.bin"):
            fixture.unlink()
        manifest_path = output_dir / "manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, start_ns, end_ns = _resolve_provenance_scope(
            store,
            id=id,
            session=session,
            from_label=from_label,
            to_label=to_label,
        )
    finally:
        store.close()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows, status_samples = _extract_seed_samples(
            conn,
            session_id=resolved_session_id,
            start_ns=start_ns,
            end_ns=end_ns,
            device_ip=device_ip,
        )
    finally:
        conn.close()

    if not rows and not status_samples:
        raise typer.Exit("no packets matched scope in capture database. capture traffic first, or loosen filters.")

    manifest_path = _write_seed_samples(
        rows,
        status_samples,
        output_dir,
        db_path=db_path,
        session_id=resolved_session_id,
        from_label=_normalize_marker_label(from_label) if from_label else None,
        to_label=_normalize_marker_label(to_label) if to_label else None,
        device_ip=device_ip,
    )

    print(f"Capture: Seeded fixtures: {len(rows) + len(status_samples)}")
    print(f"Capture: Output: {output_dir}")
    print(f"Capture: Manifest: {manifest_path}")


@app.command("label")
def provenance_label(
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    labels: Optional[str] = typer.Option(None, "--labels", help="Labels JSON path."),
    interactive: bool = typer.Option(
        True, "--interactive/--no-interactive", help="Prompt for unlabeled observed entries."
    ),
    set_opcode: Optional[list[str]] = typer.Option(None, "--set-opcode", help="Set label: protocol:opcode=label"),
    set_message: Optional[list[str]] = typer.Option(None, "--set-message", help="Set label: message_type=label"),
    set_status: Optional[list[str]] = typer.Option(None, "--set-status", help="Set label: status_code=state:label"),
    id: Optional[int] = typer.Option(None, "--id", help="Session ID scope."),
    session: Optional[str] = typer.Option(None, "--session", help="Session reference (ID, name, latest, active)."),
    from_label: Optional[str] = typer.Option(
        None, "--from-label", help="Start at first marker label in scoped session."
    ),
    to_label: Optional[str] = typer.Option(None, "--to-label", help="End at last marker label in scoped session."),
    device_ip: Optional[str] = typer.Option(
        None, "--device-ip", help="Only include packets where src or dst matches this IP."
    ),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    db_path = Path(resolved_db).expanduser().resolve()
    if not db_path.exists():
        raise typer.Exit(f"capture database not found: {db_path}")

    labels_path = resolve_clean_labels_path(labels)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, start_ns, end_ns = _resolve_provenance_scope(
            store,
            id=id,
            session=session,
            from_label=from_label,
            to_label=to_label,
        )
    finally:
        store.close()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        opcode_rows = _query_observed_opcodes(
            conn,
            session_id=resolved_session_id,
            start_ns=start_ns,
            end_ns=end_ns,
            device_ip=device_ip,
        )
        message_rows = _query_observed_messages(
            conn,
            session_id=resolved_session_id,
            start_ns=start_ns,
            end_ns=end_ns,
            device_ip=device_ip,
        )
        status_rows = _query_observed_subscription_statuses(
            conn,
            session_id=resolved_session_id,
            start_ns=start_ns,
            end_ns=end_ns,
            device_ip=device_ip,
        )
    finally:
        conn.close()

    if not opcode_rows and not message_rows and not status_rows:
        raise typer.Exit("no observed opcodes/messages/status values matched scope")

    opcode_labels, message_labels = load_clean_labels(labels_path)
    status_labels = load_clean_subscription_status_labels(labels_path)
    changed = False

    for token in set_opcode or []:
        try:
            key, value = _parse_set_opcode(token)
        except Exception as exception:
            raise typer.Exit(f"invalid --set-opcode {token!r}: {exception}")
        opcode_labels[key] = value
        changed = True

    for token in set_message or []:
        try:
            key, value = _parse_set_message(token)
        except Exception as exception:
            raise typer.Exit(f"invalid --set-message {token!r}: {exception}")
        message_labels[key] = value
        changed = True

    for token in set_status or []:
        try:
            key, value = _parse_set_status(token)
        except Exception as exception:
            raise typer.Exit(f"invalid --set-status {token!r}: {exception}")
        status_labels[key] = value
        changed = True

    observed_opcode_keys = {(int(row["protocol_id"]), int(row["opcode"])) for row in opcode_rows}
    observed_message_keys = {int(row["message_type"]) for row in message_rows}
    observed_status_keys = {int(row["status_code"]) for row in status_rows}
    unlabeled_opcodes = [
        row for row in opcode_rows if (int(row["protocol_id"]), int(row["opcode"])) not in opcode_labels
    ]
    unlabeled_messages = [row for row in message_rows if int(row["message_type"]) not in message_labels]
    unlabeled_statuses = [row for row in status_rows if int(row["status_code"]) not in status_labels]

    print(f"db: {db_path}")
    print(f"labels: {labels_path}")
    print(
        "observed opcodes/messages/statuses: "
        f"{len(observed_opcode_keys)}/{len(observed_message_keys)}/{len(observed_status_keys)} "
        f"unlabeled: {len(unlabeled_opcodes)}/{len(unlabeled_messages)}/{len(unlabeled_statuses)}"
    )

    if interactive:
        if unlabeled_opcodes:
            changed = _interactive_label_opcodes(unlabeled_opcodes, opcode_labels) or changed
        if unlabeled_messages:
            changed = _interactive_label_messages(unlabeled_messages, message_labels) or changed
        if unlabeled_statuses:
            changed = _interactive_label_statuses(unlabeled_statuses, status_labels) or changed

    if not changed:
        print("no label changes")
        return

    saved_path = save_clean_labels(
        opcode_labels,
        message_labels,
        status_labels,
        labels_path,
    )
    print(f"saved labels: {saved_path}")


@app.command("check")
def provenance_check(
    fixtures_root: Optional[str] = typer.Option(
        None, "--fixtures-root", help="Fixture root to scan for .bin payload samples."
    ),
    labels: Optional[str] = typer.Option(None, "--labels", help="Labels JSON path."),
    overrides: Optional[str] = typer.Option(None, "--overrides", help="Optional JSON override allowlist path."),
):
    fixture_root = Path(fixtures_root).expanduser().resolve() if fixtures_root else _default_fixture_root().resolve()
    if not fixture_root.exists():
        raise typer.Exit(f"fixture root not found: {fixture_root}")

    labels_path = resolve_clean_labels_path(labels)
    overrides_path = Path(overrides).expanduser().resolve() if overrides else _default_label_overrides_path().resolve()

    observed_opcodes, observed_messages, observed_statuses = _scan_observed_from_fixtures(fixture_root)
    opcode_overrides, message_overrides, status_overrides = _load_label_overrides(overrides_path)
    clean_opcode_labels, clean_message_labels = load_clean_labels(labels_path)
    clean_status_labels = load_clean_subscription_status_labels(labels_path)

    failures: list[str] = []
    failures.extend(_check_opcode_labels(observed_opcodes, opcode_overrides))
    failures.extend(_check_message_labels(observed_messages, message_overrides))

    arc_variant_protocols = {0x27FF, 0x2809}
    for protocol, opcode in sorted(clean_opcode_labels):
        label = clean_opcode_labels[(protocol, opcode)]
        if not label or label == f"0x{opcode:04X}":
            continue
        key = (protocol, opcode)
        if key in observed_opcodes or key in opcode_overrides:
            continue
        if protocol in arc_variant_protocols:
            if (0x27FF, opcode) in observed_opcodes or (0x2809, opcode) in observed_opcodes:
                continue
        failures.append(
            f"unproven clean opcode label: protocol=0x{protocol:04X} opcode=0x{opcode:04X} "
            f"label={label!r} file={labels_path}"
        )

    for message_type in sorted(clean_message_labels):
        label = clean_message_labels[message_type]
        if not label or label == f"msg:0x{message_type:04X}":
            continue
        if message_type in observed_messages or message_type in message_overrides:
            continue
        failures.append(f"unproven clean settings label: msg=0x{message_type:04X} label={label!r} file={labels_path}")

    for status_code in sorted(clean_status_labels):
        entry = clean_status_labels[status_code]
        label = entry.get("label")
        if not isinstance(label, str) or not label.strip():
            continue
        if status_code in observed_statuses or status_code in status_overrides:
            continue
        failures.append(
            f"unproven clean subscription-status label: status=0x{status_code:04X} label={label!r} file={labels_path}"
        )

    if failures:
        print("label provenance check failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        print(
            f"add vetted exceptions to {overrides_path} if a label is intentionally manual.",
            file=sys.stderr,
        )
        raise typer.Exit(1)

    print(
        "label provenance OK: "
        f"observed_opcodes={len(observed_opcodes)} "
        f"observed_messages={len(observed_messages)} "
        f"observed_subscription_statuses={len(observed_statuses)} "
        f"clean_labels={len(clean_opcode_labels) + len(clean_message_labels) + len(clean_status_labels)}"
    )


@app.command("verify")
def provenance_verify(
    bundle: Optional[str] = typer.Argument(None, help="Path to a specific bundle directory. Omit to scan all bundles."),
    fixtures_root: Optional[str] = typer.Option(
        None, "--fixtures-root", help="Fixture root containing provenance session dirs."
    ),
):
    if bundle:
        bundle_dirs = [Path(bundle).expanduser().resolve()]
    else:
        root = Path(fixtures_root).expanduser().resolve() if fixtures_root else _default_fixture_root().resolve()
        provenance_dir = root / "provenance" if root.name != "provenance" else root
        if not provenance_dir.exists():
            raise typer.Exit(f"Provenance directory not found: {provenance_dir}")
        bundle_dirs = sorted(provenance_dir.glob("session_*"))
        if not bundle_dirs:
            raise typer.Exit(f"No session bundles found in {provenance_dir}")

    results = {}
    for bundle_dir in bundle_dirs:
        if not bundle_dir.is_dir():
            continue
        result = _verify_single_bundle(bundle_dir)
        results[str(bundle_dir)] = result
        print()

    total = len(results)
    passed = sum(1 for v in results.values() if v)
    failed = total - passed

    if total > 1:
        print(f"{'=' * 60}")
        print(f"Total: {total} bundles, {passed} passed, {failed} failed")

    if failed > 0:
        raise typer.Exit(1)


@app.command("show")
def provenance_show(
    bundle: str = typer.Argument(..., help="Path to a bundle directory or .tar.gz file."),
):
    bundle_path = Path(bundle).expanduser().resolve()
    if not bundle_path.exists():
        print(f"Bundle not found: {bundle_path}", file=sys.stderr)
        raise typer.Exit(1)

    manifest, files = _load_bundle(bundle_path)

    scope = manifest.get("scope", {})
    markers = manifest.get("markers", [])
    samples = manifest.get("samples", [])

    print(f"Session #{manifest.get('session_id', '?')}")
    print(f"  Name:        {manifest.get('session_name', '')}")
    print(f"  Device:      {scope.get('device_name', '')} ({scope.get('device_ip', '')})")
    print(f"  Started:     {manifest.get('started_iso', '')}")
    print(f"  Ended:       {manifest.get('ended_iso', '')}")
    print(f"  Packets:     {manifest.get('session_packet_count', manifest.get('count', 0))} session, {manifest.get('evidence_packet_count', 0)} evidence")
    print(f"  Markers:     {len(markers)}")

    if not markers and not samples:
        return

    print("\nTimeline:")
    print(f"{'Type':14s}  {'Timestamp':26s}  {'Label':34s}")
    print("-" * 80)

    sample_by_id = {s.get("packet_id"): s for s in samples}

    for marker in markers:
        marker_type = marker.get("marker_type", "?")
        ts = marker.get("timestamp_iso", "")
        label = marker.get("label", "")
        note = marker.get("note")
        marker_data = marker.get("data")

        print(f"{marker_type:14s}  {ts:26s}  {label}")

        if note:
            print(f"{'':14s}  {'':26s}  note: {note}")

        if marker_data and marker_type == "evidence" and marker_data.get("packet_ids"):
            packet_ids = marker_data["packet_ids"]
            filters = marker_data.get("filters", {})
            if filters:
                filter_parts = [f"{k}={v}" for k, v in filters.items()]
                print(f"{'':14s}  {'':26s}  filters: {', '.join(filter_parts)}")
            for pid in packet_ids[:20]:
                sample_entry = sample_by_id.get(pid)
                if sample_entry:
                    direction = sample_entry.get("direction") or "multicast"
                    opcode_hex = sample_entry.get("opcode_hex", "?")
                    src = f"{sample_entry.get('src_ip', '?')}:{sample_entry.get('src_port', '?')}"
                    dst = f"{sample_entry.get('dst_ip', '?')}:{sample_entry.get('dst_port', '?')}"
                    filename = sample_entry.get("file", "")
                    payload = files.get(filename)
                    size = f"{len(payload)}B" if payload else "?"
                    print(f"{'':14s}  {'':26s}    #{pid} {direction:9s} {opcode_hex} {src} -> {dst} {size}")
                    if payload:
                        print(_hexdump(payload, indent=f"{'':14s}  {'':26s}    "))
            if len(packet_ids) > 20:
                print(f"{'':14s}  {'':26s}    ... and {len(packet_ids) - 20} more")


@app.command("audit")
def provenance_audit(
    bundle: Optional[str] = typer.Argument(None, help="Path to a bundle directory or .tar.gz. Omit to scan all bundles."),
    fixtures_root: Optional[str] = typer.Option(
        None, "--fixtures-root", help="Fixture root containing provenance session dirs."
    ),
):
    if bundle:
        bundle_paths = [Path(bundle).expanduser().resolve()]
    else:
        root = Path(fixtures_root).expanduser().resolve() if fixtures_root else _default_fixture_root().resolve()
        provenance_dir = root / "provenance" if root.name != "provenance" else root
        if not provenance_dir.exists():
            raise typer.Exit(f"Provenance directory not found: {provenance_dir}")
        bundle_paths = sorted(
            list(provenance_dir.glob("session_*/")) + list(provenance_dir.glob("session_*.tar.gz"))
        )
        if not bundle_paths:
            raise typer.Exit(f"No session bundles found in {provenance_dir}")

    results = {}
    for bundle_path in bundle_paths:
        if bundle_path.is_file() and bundle_path.name.endswith(".tar.gz"):
            pass
        elif not bundle_path.is_dir():
            continue
        result = _audit_single_bundle(bundle_path)
        results[str(bundle_path)] = result
        print()

    total = len(results)
    passed = sum(1 for v in results.values() if v)
    failed = total - passed

    if total > 1:
        print(f"{'=' * 72}")
        print(f"Total: {total} bundles, {passed} passed, {failed} failed")

    if failed > 0:
        raise typer.Exit(1)


@app.command("export")
def provenance_export(
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None, "--session", help="Session reference (ID, name, latest, active). Defaults to latest.",
    ),
    out: Optional[str] = typer.Option(None, "--out", help="Output directory for the bundle."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    from netaudio_lib.dante.protocol_verifier import export_session_bundle

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="latest",
        )
        bundle_path = export_session_bundle(store, resolved_session_id, output_dir=out)
        print(f"Capture: Exported bundle: {bundle_path}")
    finally:
        store.close()


@app.command("evidence")
def provenance_evidence(
    label: str = typer.Option(..., "--label", help="Evidence label for this marker."),
    note: Optional[str] = typer.Option(None, "--note", help="Descriptive note about this evidence."),
    packet_id: Optional[list[int]] = typer.Option(None, "--packet-id", help="Specific packet IDs to tag (repeatable)."),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None, "--session", help="Session reference (ID, name, latest, active). Defaults to active.",
    ),
    device_ip: Optional[str] = typer.Option(None, "--device-ip", help="Filter packets by device IP."),
    opcode: Optional[str] = typer.Option(None, "--opcode", help="Filter packets by opcode (hex, e.g. 0x1100)."),
    direction: Optional[str] = typer.Option(None, "--direction", help="Filter packets by direction (request/response)."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="active",
        )

        resolved_packet_ids = []

        if packet_id:
            for pid in packet_id:
                pkt = store.get_packet(pid)
                if not pkt:
                    print(f"Capture: Packet #{pid} not found.", file=sys.stderr)
                    raise typer.Exit(1)
                resolved_packet_ids.append(pid)

        query_kwargs = {}
        if device_ip:
            query_kwargs["device_ip"] = device_ip
        if opcode:
            query_kwargs["opcode"] = int(opcode, 16) if opcode.startswith("0x") else int(opcode)
        if direction:
            query_kwargs["direction"] = direction

        if query_kwargs:
            query_kwargs["session_id"] = resolved_session_id
            matched = store.query_packets(**query_kwargs)
            for pkt in matched:
                if pkt["id"] not in resolved_packet_ids:
                    resolved_packet_ids.append(pkt["id"])

        if not resolved_packet_ids:
            print("Capture: No packets matched the given filters.", file=sys.stderr)
            raise typer.Exit(1)

        normalized_label = _normalize_marker_label(label)
        marker_id = store.add_marker(
            session_id=resolved_session_id,
            marker_type="evidence",
            label=normalized_label,
            note=note or f"Tagged {len(resolved_packet_ids)} packets as evidence",
            data={
                "packet_ids": resolved_packet_ids,
                "filters": {
                    k: (f"0x{v:04X}" if k == "opcode" else v)
                    for k, v in query_kwargs.items()
                    if k != "session_id"
                },
            },
        )

        print(f"Capture: Evidence marker #{marker_id} added to session #{resolved_session_id}")
        print(f"Capture: Label: {normalized_label}")
        print(f"Capture: Packets tagged: {len(resolved_packet_ids)}")
        for pid in resolved_packet_ids[:20]:
            pkt = store.get_packet(pid)
            if pkt:
                payload = pkt.get("payload", b"")
                opcode_hex = ""
                if len(payload) >= 8:
                    opcode_hex = f" opcode=0x{int.from_bytes(payload[6:8], 'big'):04X}"
                pkt_direction = pkt.get("direction", "?")
                print(f"  #{pid} {pkt_direction}{opcode_hex} {len(payload)}B")
        if len(resolved_packet_ids) > 20:
            print(f"  ... and {len(resolved_packet_ids) - 20} more")
    finally:
        store.close()


@app.command("analysis")
def provenance_analysis(
    label: str = typer.Option(..., "--label", help="Analysis label."),
    note: str = typer.Option(..., "--note", help="What was found in the packet(s)."),
    packet_id: Optional[list[int]] = typer.Option(None, "--packet-id", help="Packet ID(s) analyzed (repeatable)."),
    field: Optional[list[str]] = typer.Option(
        None,
        "--field",
        help="Field extracted: name:offset:length:type:value (repeatable). Example: current_latency:108:4:uint32_be:150000",
    ),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None, "--session", help="Session reference (ID, name, latest, active). Defaults to active.",
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="active",
        )

        resolved_packet_ids = []
        if packet_id:
            for pid in packet_id:
                pkt = store.get_packet(pid)
                if not pkt:
                    print(f"Capture: Packet #{pid} not found.", file=sys.stderr)
                    raise typer.Exit(1)
                resolved_packet_ids.append(pid)

        fields_parsed = [_parse_field_spec(f) for f in field] if field else []

        normalized_label = _normalize_marker_label(label)
        marker_id = store.add_marker(
            session_id=resolved_session_id,
            marker_type="analysis",
            label=normalized_label,
            note=note,
            data={
                "packet_ids": resolved_packet_ids,
                "fields": fields_parsed,
            },
        )

        print(f"Capture: Analysis marker #{marker_id} added to session #{resolved_session_id}")
        print(f"Capture: Label: {normalized_label}")
        if resolved_packet_ids:
            print(f"Capture: Packets referenced: {', '.join(f'#{p}' for p in resolved_packet_ids)}")
        if fields_parsed:
            for field_entry in fields_parsed:
                value_str = f" value={field_entry['value']}" if "value" in field_entry else ""
                print(
                    f"  {field_entry['name']}: offset={field_entry['offset']} "
                    f"len={field_entry['length']} type={field_entry['dtype']}"
                    f"{value_str}"
                )
    finally:
        store.close()


@app.command("analyze")
def provenance_analyze(
    bundle: str = typer.Argument(..., help="Path to provenance bundle (.tar.gz or directory)."),
    raw: bool = typer.Option(False, "--raw", help="Show raw hexdump for each packet."),
):
    from netaudio_lib.dante.fact_store import _load_bundle as lib_load_bundle, list_facts, _verify_field

    bundle_path = Path(bundle)
    if not bundle_path.exists():
        bundle_path = Path("tests/fixtures/provenance") / bundle
    if not bundle_path.exists():
        print(f"Bundle not found: {bundle}", file=sys.stderr)
        raise typer.Exit(1)

    manifest, files = lib_load_bundle(bundle_path)
    if not manifest:
        print(f"Empty or invalid bundle: {bundle_path}", file=sys.stderr)
        raise typer.Exit(1)

    facts_path = _resolve_facts_path()
    all_facts = list_facts(facts_path) if facts_path.exists() else []

    arc_facts_by_opcode = {}
    conmon_facts_by_type = {}
    for fact in all_facts:
        if fact["category"] == "arc_opcode":
            try:
                arc_facts_by_opcode[int(fact["key"], 16)] = fact
            except ValueError:
                pass
        elif fact["category"] == "conmon_message":
            try:
                conmon_facts_by_type[int(fact["key"], 16)] = fact
            except ValueError:
                pass

    samples = manifest.get("samples", [])
    scope = manifest.get("scope", {})
    session_name = manifest.get("session_name", bundle_path.stem)

    print(f"Bundle: {bundle_path.name}")
    if scope.get("device_ip"):
        print(f"Device: {scope.get('device_ip', '?')}  {scope.get('device_name', '')}")
    print(f"Session: {session_name}")
    print(f"Packets: {len(samples)}")
    print()

    responses = [s for s in samples if s.get("direction") == "response"]

    device_profile = {}

    for sample in responses:
        filename = sample.get("file", "")
        payload = files.get(filename)
        if payload is None:
            continue

        protocol_id = sample.get("protocol_id", 0)
        opcode_val = sample.get("opcode", 0)
        opcode_hex = f"0x{opcode_val:04X}" if opcode_val else "?"

        fact = None
        if protocol_id in (0x27FF, 0x2809, 0x1200):
            fact = arc_facts_by_opcode.get(opcode_val)
        elif protocol_id == 0xFFFF and len(payload) >= 28:
            message_type = struct.unpack(">H", payload[26:28])[0]
            fact = conmon_facts_by_type.get(message_type)

        label = _label_packet(payload)
        src = f"{sample.get('src_ip', '?')}:{sample.get('src_port', '?')}"
        dst = f"{sample.get('dst_ip', '?')}:{sample.get('dst_port', '?')}"

        print(f"  {opcode_hex}  {label or '?':30s}  {src} -> {dst}  {len(payload)}B")

        if fact and fact.get("fields"):
            for field_def in fact["fields"]:
                result = _extract_field(payload, field_def)
                if result:
                    print(f"    {result['name']:25s} = {result['display']}")
                    if result.get("profile_key"):
                        device_profile[result["profile_key"]] = result["display"]

        if raw:
            for line in _compact_hexdump(payload, max_lines=4):
                print(line)

        print()

    if device_profile:
        print("Device Profile:")
        for profile_key, value in device_profile.items():
            print(f"  {profile_key:25s} = {value}")


@app.command("send")
def provenance_send(
    device_ip: str = typer.Option(..., "--device-ip", help="Target device IP address."),
    port: int = typer.Option(4440, "--port", help="Target UDP port."),
    payload_hex: Optional[str] = typer.Option(None, "--payload-hex", help="Raw payload as hex string."),
    packet_id: Optional[int] = typer.Option(None, "--packet-id", help="Replay an existing packet's payload (to a new target)."),
    label: str = typer.Option(..., "--label", help="Label for this send (used in evidence marker)."),
    note: Optional[str] = typer.Option(None, "--note", help="Descriptive note."),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None, "--session", help="Session reference (ID, name, latest, active). Defaults to active.",
    ),
    timeout: float = typer.Option(2.0, "--timeout", help="Response timeout in seconds."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    if payload_hex is None and packet_id is None:
        print("Error: Provide --payload-hex or --packet-id.", file=sys.stderr)
        raise typer.Exit(1)

    _require_positive_session_id(session_id, "--session-id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="active",
        )

        if payload_hex is not None:
            payload = bytes.fromhex(payload_hex.replace(" ", "").replace(":", ""))
        else:
            source_packet = store.get_packet(packet_id)
            if not source_packet:
                print(f"Error: Packet #{packet_id} not found.", file=sys.stderr)
                raise typer.Exit(1)
            payload = source_packet["payload"]
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)

        _do_send(
            payload=payload,
            device_ip=device_ip,
            port=port,
            label=label,
            note=note,
            session_id=resolved_session_id,
            store=store,
            timeout=timeout,
            dump=dump,
        )
    finally:
        store.close()


def _do_send(
    payload: bytes,
    device_ip: str,
    port: int,
    label: str,
    note: str | None,
    session_id: int,
    store: PacketStore,
    timeout: float,
    dump: bool,
):
    normalized_label = _normalize_marker_label(label)
    source_host = socket.gethostname()

    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    probe.connect((device_ip, port))
    local_ip = probe.getsockname()[0]
    probe.close()

    send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    send_socket.settimeout(timeout)

    send_timestamp = time.time_ns()
    send_socket.sendto(payload, (device_ip, port))
    local_port = send_socket.getsockname()[1]

    sent_id = store.store_packet(
        payload=payload,
        source_type="provenance_send",
        src_ip=local_ip,
        src_port=local_port,
        dst_ip=device_ip,
        dst_port=port,
        device_ip=device_ip,
        direction="request",
        timestamp_ns=send_timestamp,
        session_id=session_id,
        source_host=source_host,
    )

    info = _label_packet(payload)
    print(f"Sent: #{sent_id}  {local_ip}:{local_port} -> {device_ip}:{port}  {len(payload)}B  {info or ''}")

    if dump:
        print(_hexdump(payload))

    tagged_packet_ids = []
    if sent_id:
        tagged_packet_ids.append(sent_id)

    try:
        reply_data, reply_addr = send_socket.recvfrom(4096)
        reply_timestamp = time.time_ns()
        reply_ip, reply_port = reply_addr

        reply_id = store.store_packet(
            payload=reply_data,
            source_type="provenance_send",
            src_ip=reply_ip,
            src_port=reply_port,
            dst_ip=local_ip,
            dst_port=local_port,
            device_ip=reply_ip,
            direction="response",
            timestamp_ns=reply_timestamp,
            session_id=session_id,
            source_host=source_host,
        )

        reply_info = _label_packet(reply_data)
        print(f"Recv: #{reply_id}  {reply_ip}:{reply_port} -> {local_ip}:{local_port}  {len(reply_data)}B  {reply_info or ''}")

        if dump:
            print(_hexdump(reply_data))

        if reply_id:
            tagged_packet_ids.append(reply_id)
    except socket.timeout:
        print("  (no unicast reply)")

    send_socket.close()

    if tagged_packet_ids:
        marker_id = store.add_marker(
            session_id=session_id,
            marker_type="evidence",
            label=normalized_label,
            note=note or f"provenance send: {info or label}",
            data={
                "packet_ids": tagged_packet_ids,
                "device_ip": device_ip,
                "port": port,
                "payload_size": len(payload),
            },
        )
        print(f"\nEvidence marker #{marker_id}: {normalized_label} ({len(tagged_packet_ids)} packets)")


@app.command("replay")
def provenance_replay(
    bundle: str = typer.Argument(..., help="Path to provenance bundle (.tar.gz or directory)."),
    device_ip: Optional[str] = typer.Option(None, "--device-ip", help="Override target device IP."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be sent without sending."),
    timeout: float = typer.Option(2.0, "--timeout", help="Response timeout per packet in seconds."),
    session_name: Optional[str] = typer.Option(None, "--session-name", help="Override replay session name."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio_lib.dante.fact_store import _load_bundle as lib_load_bundle

    bundle_path = Path(bundle)
    if not bundle_path.exists():
        bundle_path = Path("tests/fixtures/provenance") / bundle
    if not bundle_path.exists():
        print(f"Bundle not found: {bundle}", file=sys.stderr)
        raise typer.Exit(1)

    manifest, files = lib_load_bundle(bundle_path)
    if not manifest:
        print(f"Empty or invalid bundle: {bundle_path}", file=sys.stderr)
        raise typer.Exit(1)

    samples = manifest.get("samples", [])
    requests = [s for s in samples if s.get("direction") == "request" and not s.get("evidence", False)]
    responses_by_opcode_seq = {}
    request_idx = 0
    for sample in samples:
        if sample.get("evidence", False):
            continue
        if sample.get("direction") == "request":
            request_idx += 1
        elif sample.get("direction") == "response":
            responses_by_opcode_seq[request_idx] = sample

    if not requests:
        print("No request packets found in bundle.", file=sys.stderr)
        raise typer.Exit(1)

    original_name = manifest.get("session", {}).get("name", bundle_path.stem)
    replay_name = session_name or f"replay_{original_name}"

    original_device_ip = manifest.get("device", {}).get("ip")
    for sample in samples:
        if sample.get("device_ip"):
            original_device_ip = sample["device_ip"]
            break

    target_ip = device_ip or original_device_ip
    if not target_ip:
        print("Cannot determine target device IP. Use --device-ip.", file=sys.stderr)
        raise typer.Exit(1)

    print(f"Bundle: {bundle_path.name}")
    print(f"  Original session: {original_name}")
    print(f"  Target device: {target_ip}")
    print(f"  Requests to replay: {len(requests)}")
    print(f"  Original responses available: {len(responses_by_opcode_seq)}")
    print()

    request_response_pairs = []
    req_counter = 0
    for sample in samples:
        if sample.get("evidence", False):
            continue
        if sample.get("direction") == "request":
            req_counter += 1
            payload = files.get(sample["file"])
            if payload is None:
                continue
            original_response_sample = responses_by_opcode_seq.get(req_counter)
            original_response = None
            if original_response_sample:
                original_response = files.get(original_response_sample["file"])
            port = sample.get("dst_port") or 4440
            request_response_pairs.append({
                "sample": sample,
                "payload": payload,
                "port": port,
                "original_response": original_response,
                "original_response_sample": original_response_sample,
            })

    if dry_run:
        for idx, pair in enumerate(request_response_pairs, 1):
            sample = pair["sample"]
            opcode_hex = sample.get("opcode_hex", f"0x{sample.get('opcode', 0):04X}")
            print(f"  [{idx}/{len(request_response_pairs)}] {opcode_hex} -> {target_ip}:{pair['port']}  {len(pair['payload'])}B")
            for line in _compact_hexdump(pair["payload"], max_lines=4):
                print(line)
            if pair["original_response"]:
                print(f"    expected response: {len(pair['original_response'])}B")
        print(f"\nDry run: {len(request_response_pairs)} packets would be sent.")
        return

    asyncio.run(_run_replay(
        request_response_pairs=request_response_pairs,
        target_ip=target_ip,
        replay_name=replay_name,
        original_name=original_name,
        bundle_path=bundle_path,
        timeout=timeout,
        config=config,
        profile=profile,
        db_override=db,
    ))


async def _run_replay(
    request_response_pairs: list[dict],
    target_ip: str,
    replay_name: str,
    original_name: str,
    bundle_path: Path,
    timeout: float,
    config: str | None,
    profile: str | None,
    db_override: str | None,
):
    import struct
    from netaudio_lib.dante.protocol_verifier import ProtocolVerifier

    async with ProtocolVerifier(
        device_ip=target_ip,
        session_name=replay_name,
        config=config,
        profile=profile,
        db=db_override,
    ) as verifier:

        verifier.marker(
            "replay_started",
            marker_type="system",
            note=f"Replaying {len(request_response_pairs)} requests from {bundle_path.name}",
            data={"source_bundle": str(bundle_path), "original_session": original_name},
        )

        results = []
        total = len(request_response_pairs)

        for idx, pair in enumerate(request_response_pairs, 1):
            sample = pair["sample"]
            opcode = sample.get("opcode", 0)
            opcode_hex = sample.get("opcode_hex", f"0x{opcode:04X}")
            port = pair["port"]
            payload = pair["payload"]

            if len(payload) >= 6:
                original_txn = struct.unpack(">H", payload[4:6])[0]
                new_txn = (original_txn + 0x8000 + idx) & 0xFFFF
                payload = payload[:4] + struct.pack(">H", new_txn) + payload[6:]

            label = f"replay_{opcode_hex}_{idx}"
            print(f"  [{idx}/{total}] {opcode_hex} -> {target_ip}:{port}  {len(payload)}B  ", end="", flush=True)

            response = await verifier.send(payload, port=port, timeout=timeout, label=label)

            original_response = pair["original_response"]

            if response is None:
                print("TIMEOUT")
                results.append({"opcode_hex": opcode_hex, "status": "timeout", "idx": idx})
                verifier.observation(
                    f"replay_{opcode_hex}_{idx}_timeout",
                    note=f"No response for {opcode_hex}",
                )
                continue

            response_status = struct.unpack(">H", response[8:10])[0] if len(response) >= 10 else 0

            if original_response is None:
                print(f"{len(response)}B  status=0x{response_status:04X}  (no original to compare)")
                results.append({"opcode_hex": opcode_hex, "status": "ok_no_baseline", "idx": idx})
                continue

            size_match = len(response) == len(original_response)

            orig_comparable = original_response
            resp_comparable = response
            if len(orig_comparable) >= 6 and len(resp_comparable) >= 6:
                orig_comparable = orig_comparable[:4] + b"\x00\x00" + orig_comparable[6:]
                resp_comparable = resp_comparable[:4] + b"\x00\x00" + resp_comparable[6:]

            bytes_match = resp_comparable == orig_comparable

            if bytes_match:
                print(f"{len(response)}B  MATCH")
                results.append({"opcode_hex": opcode_hex, "status": "match", "idx": idx})
            elif size_match:
                diff_count = sum(1 for a, b in zip(resp_comparable, orig_comparable) if a != b)
                print(f"{len(response)}B  DIFF ({diff_count} bytes differ)")
                results.append({"opcode_hex": opcode_hex, "status": "diff", "idx": idx, "diff_bytes": diff_count})
                verifier.observation(
                    f"replay_{opcode_hex}_{idx}_diff",
                    note=f"{opcode_hex} response differs: {diff_count} bytes changed, same size ({len(response)}B)",
                    data={"diff_bytes": diff_count, "response_len": len(response)},
                )
            else:
                print(f"{len(response)}B  SIZE_DIFF (expected {len(original_response)}B)")
                results.append({
                    "opcode_hex": opcode_hex, "status": "size_diff", "idx": idx,
                    "got_len": len(response), "expected_len": len(original_response),
                })
                verifier.observation(
                    f"replay_{opcode_hex}_{idx}_size_diff",
                    note=f"{opcode_hex} response size differs: got {len(response)}B, expected {len(original_response)}B",
                )

        print()
        match_count = sum(1 for r in results if r["status"] == "match")
        diff_count = sum(1 for r in results if r["status"] in ("diff", "size_diff"))
        timeout_count = sum(1 for r in results if r["status"] == "timeout")
        no_baseline = sum(1 for r in results if r["status"] == "ok_no_baseline")

        summary_parts = []
        if match_count:
            summary_parts.append(f"{match_count} matched")
        if diff_count:
            summary_parts.append(f"{diff_count} differed")
        if timeout_count:
            summary_parts.append(f"{timeout_count} timed out")
        if no_baseline:
            summary_parts.append(f"{no_baseline} no baseline")

        summary = ", ".join(summary_parts)
        print(f"Replay complete: {total} packets — {summary}")

        verifier.marker(
            "replay_finished",
            marker_type="system",
            note=f"Replay complete: {summary}",
            data={"results": results},
        )
