from __future__ import annotations

import json
import logging
import sqlite3
import struct
from pathlib import Path

from netaudio.capture.packets import ARC_PROTOCOLS, TARGET_PROTOCOLS
from netaudio.dante.debug_formatter import (
    OPCODE_NAMES_BY_PROTOCOL,
    SETTINGS_MESSAGE_TYPE_NAMES,
)
from netaudio.dante.packet_store import _decompress_payload

logger = logging.getLogger("netaudio")
ARC_SUCCESS_RESULTS = frozenset({0x0001, 0x8112})


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
    if len(body) < 2:
        return set()

    record_size = 20
    record_count = body[1]
    if body[0] != record_count or record_count > 16:
        return set()

    records_end = 2 + record_count * record_size
    if len(body) < records_end:
        return set()

    statuses: set[int] = set()

    for record_index in range(record_count):
        record_start = 2 + record_index * record_size
        record = body[record_start : record_start + record_size]
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

        if channel_number == 0 or rx_channel_offset >= len(payload):
            return set()

        statuses.add(subscription_status_code)

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
        payload = _decompress_payload(row["payload"])
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
    database_rows = conn.execute(query, scope_params).fetchall()
    rows: list[dict[str, object]] = []
    for database_row in database_rows:
        sample = dict(database_row)
        sample["payload"] = _decompress_payload(sample["payload"])
        rows.append(sample)

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
        payload = _decompress_payload(row["payload"])
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

        if protocol in (*ARC_PROTOCOLS, 0x1200) and len(payload) >= 8:
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
        except Exception as exception:
            logger.debug(f"Failed to process archive {archive}: {exception}")

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
    arc_variant_protocols = set(ARC_PROTOCOLS)

    for protocol, mapping in OPCODE_NAMES_BY_PROTOCOL.items():
        for opcode, label in mapping.items():
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
    for message_type, label in SETTINGS_MESSAGE_TYPE_NAMES.items():
        if not label or label == f"msg:0x{message_type:04X}":
            continue
        if message_type in observed or message_type in overrides:
            continue
        failures.append(f"unproven settings message label: msg=0x{message_type:04X} label={label!r}")
    return failures


KNOWN_PROTOCOL_NAMES = {
    0x2729: "ARC",
    0x27FF: "ARC",
    0x2801: "ARC",
    0x2809: "ARC",
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
    0x1102: "PROPERTY_DIRECTORY",
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
        result["status_ok"] = status in ARC_SUCCESS_RESULTS

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
