from __future__ import annotations

import json
import logging
import sqlite3
import struct
from pathlib import Path

from netaudio.common.manifest import write_manifest
from netaudio.dante.const import (
    ARC_PROTOCOL_IDS,
    ARC_SUCCESS_RESULT_CODES,
    CAPTURE_PROTOCOL_IDS,
    OPCODE_RX_CHANNELS,
    PROTOCOL_CMC,
    PROTOCOL_LABELS,
    PROTOCOL_SETTINGS,
)
from netaudio.dante.debug_formatter import _external_labels
from netaudio.dante.dissection.header import parse_packet_header
from netaudio.dante.packet_store.payloads import decompress_payload

logger = logging.getLogger("netaudio")


def _extract_subscription_status_codes(payload: bytes) -> set[int]:
    from netaudio import core

    header = parse_packet_header(payload)
    if header is None or header["protocol_id"] not in ARC_PROTOCOL_IDS or header["opcode"] != OPCODE_RX_CHANNELS:
        return set()
    try:
        records = core.parse_page("rx", payload, 1)
    except core.NetaudioCoreError:
        return set()
    return {record["subscription_status_code"] for record in records}


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
    protocol_csv = ",".join(str(v) for v in CAPTURE_PROTOCOL_IDS if v != 0xFFFF)
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
    arc_protocol_csv = ",".join(str(v) for v in ARC_PROTOCOL_IDS)
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
        payload = decompress_payload(row["payload"])
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
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    protocol_csv = ",".join(str(p) for p in CAPTURE_PROTOCOL_IDS)
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
    database_rows = conn.execute(query, scope_params).fetchall()
    rows: list[dict[str, object]] = []
    for database_row in database_rows:
        sample = dict(database_row)
        sample["payload"] = decompress_payload(sample["payload"])
        rows.append(sample)

    arc_protocol_csv = ",".join(str(p) for p in ARC_PROTOCOL_IDS)
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
        payload = decompress_payload(row["payload"])
        codes = _extract_subscription_status_codes(payload)
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
                "payload": payload,
            }

    status_samples = [status_samples_by_code[code] for code in sorted(status_samples_by_code)]
    return rows, status_samples


def _write_seed_samples(
    rows: list[dict[str, object]],
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

    return write_manifest(output_dir, manifest)


def _scan_observed_from_fixtures(fixture_root: Path) -> tuple[set[tuple[int, int]], set[int], set[int]]:
    import tarfile

    observed_opcodes: set[tuple[int, int]] = set()
    observed_messages: set[int] = set()
    observed_subscription_statuses: set[int] = set()

    def _process_payload(payload: bytes) -> None:
        header = parse_packet_header(payload)
        if header is None:
            return

        protocol = header["protocol_id"]
        if protocol in (*ARC_PROTOCOL_IDS, PROTOCOL_CMC):
            observed_opcodes.add((protocol, header["opcode"]))
        if protocol == PROTOCOL_SETTINGS:
            observed_messages.add(header["opcode"])

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
        except (OSError, ValueError, tarfile.TarError) as exception:
            logger.warning(f"Failed to process archive {archive}: {exception}", exc_info=True)

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
    arc_variant_protocols = set(ARC_PROTOCOL_IDS)
    opcode_labels, _ = _external_labels()

    for (protocol, opcode), label in opcode_labels.items():
        if not label or label == f"0x{opcode:04X}":
            continue
        key = (protocol, opcode)
        if key in observed or key in overrides:
            continue
        if protocol in arc_variant_protocols:
            if any((fallback_protocol, opcode) in observed for fallback_protocol in arc_variant_protocols):
                continue
        failures.append(f"unproven opcode label: protocol=0x{protocol:04X} opcode=0x{opcode:04X} label={label!r}")

    return failures


def _check_message_labels(
    observed: set[int],
    overrides: set[int],
) -> list[str]:
    failures: list[str] = []
    _, message_labels = _external_labels()
    for message_type, label in message_labels.items():
        if not label or label == f"msg:0x{message_type:04X}":
            continue
        if message_type in observed or message_type in overrides:
            continue
        failures.append(f"unproven settings message label: msg=0x{message_type:04X} label={label!r}")
    return failures


def _verify_parse_header(data: bytes) -> dict | None:
    header = parse_packet_header(data)
    if header is None:
        return None
    protocol_id = header["protocol_id"]
    verified = {
        "opcode": header["opcode"],
        "protocol_id": protocol_id,
        "protocol_name": PROTOCOL_LABELS.get(protocol_id, f"0x{protocol_id:04X}"),
        "status": header["result_code"],
        "transaction_id": header["transaction_id"],
    }
    if protocol_id != PROTOCOL_SETTINGS:
        verified["opcode_name"] = header["opcode_name"]
    return verified


def _decode_packet_payload(data: bytes) -> dict:
    header = parse_packet_header(data)
    if header is None:
        return {"raw_hex": data.hex()}

    protocol_id = header["protocol_id"]
    result = {
        "actual_length": len(data),
        "declared_length": header["length"],
        "protocol": f"0x{protocol_id:04X}",
        "protocol_name": PROTOCOL_LABELS.get(protocol_id, f"0x{protocol_id:04X}"),
    }

    if protocol_id == PROTOCOL_SETTINGS:
        result["message_type"] = f"0x{header['opcode']:04X}"
        result["raw_hex"] = data.hex()
        return result

    result["transaction_id"] = f"0x{header['transaction_id']:04X}"
    result["opcode"] = f"0x{header['opcode']:04X}"
    result["opcode_name"] = header["opcode_name"]

    if header["result_code"] is not None:
        result["status"] = f"0x{header['result_code']:04X}"
        result["status_ok"] = header["result_code"] in ARC_SUCCESS_RESULT_CODES

    result["raw_hex"] = data.hex()

    if len(data) >= 12:
        result["payload_body_hex"] = data[10:].hex()

    words = []
    for offset in range(0, len(data), 4):
        chunk = data[offset : offset + 4]
        if len(chunk) == 4:
            words.append({"offset": offset, "hex": chunk.hex(), "u32": int.from_bytes(chunk, "big")})
    result["words"] = words

    return result


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
