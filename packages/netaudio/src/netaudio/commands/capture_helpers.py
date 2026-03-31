from __future__ import annotations

import datetime
import hashlib
import json
import re
import struct
import sys
from pathlib import Path
from typing import Optional

import typer

from netaudio.dante.packet_store import PacketStore


PORT_LABELS = {
    8751: "metering",
    8702: "info",
}

PACKET_ENDPOINT_WIDTH = 28

TARGET_PROTOCOLS = (0x27FF, 0x2809, 0x1200, 0xFFFF)
ARC_PROTOCOLS = (0x27FF, 0x2809)

STANDARD_MARKER_TYPES = ("action", "observation", "state_change", "system", "hypothesis", "evidence", "analysis")
MARKER_TYPE_ALIASES = {
    "action": "action",
    "observation": "observation",
    "observe": "observation",
    "state": "state_change",
    "state_change": "state_change",
    "state-change": "state_change",
    "system": "system",
    "hypothesis": "hypothesis",
    "inference": "hypothesis",
    "evidence": "evidence",
    "analysis": "analysis",
    "analyze": "analysis",
    "note": "observation",
    "start": "action",
    "end": "state_change",
    "capture": "system",
    "session": "system",
}
MARKER_LABEL_SANITIZE = re.compile(r"[^a-z0-9]+")


_FACT_LABEL_CACHE: dict[str, str] | None = None


def _load_fact_labels() -> dict[str, str]:
    global _FACT_LABEL_CACHE
    if _FACT_LABEL_CACHE is not None:
        return _FACT_LABEL_CACHE

    _FACT_LABEL_CACHE = {}
    try:
        from netaudio.dante.fact_store import DEFAULT_FACTS_PATH, list_facts
        if DEFAULT_FACTS_PATH.exists():
            for fact in list_facts(DEFAULT_FACTS_PATH):
                category = fact["category"]
                key = fact["key"]
                name = fact["name"]
                if category == "arc_opcode":
                    _FACT_LABEL_CACHE[f"arc:{key}"] = name
                elif category == "conmon_message":
                    _FACT_LABEL_CACHE[f"conmon:{key}"] = name
                elif category == "multicast_announcement":
                    _FACT_LABEL_CACHE[f"multicast:{key}"] = name
    except Exception:
        pass

    return _FACT_LABEL_CACHE


def _label_packet(payload: bytes):
    if len(payload) < 8:
        return ""

    from netaudio.dante.debug_formatter import (
        PROTOCOL_NAMES,
        get_opcode_name,
        get_settings_message_type_name,
    )

    protocol_id = struct.unpack(">H", payload[0:2])[0]

    if protocol_id in PROTOCOL_NAMES and protocol_id != 0xFFFF:
        opcode = struct.unpack(">H", payload[6:8])[0]
        fact_labels = _load_fact_labels()
        if protocol_id in ARC_PROTOCOLS:
            fact_name = fact_labels.get(f"arc:0x{opcode:04X}")
            if fact_name:
                return fact_name

        return get_opcode_name(protocol_id, opcode)

    if protocol_id == 0xFFFF and len(payload) >= 28:
        message_type = struct.unpack(">H", payload[26:28])[0]
        fact_labels = _load_fact_labels()
        fact_name = fact_labels.get(f"conmon:0x{message_type:04X}") or fact_labels.get(f"multicast:0x{message_type:04X}")
        if fact_name:
            return fact_name

        return get_settings_message_type_name(message_type)

    return f"proto:0x{protocol_id:04X}"


def _hexdump(data: bytes, indent: str = "         "):
    lines = []
    for offset in range(0, len(data), 16):
        chunk = data[offset : offset + 16]
        left = " ".join(f"{b:02x}" for b in chunk[:8])
        right = " ".join(f"{b:02x}" for b in chunk[8:])
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"{indent}{offset:04x}  {left:<23s}  {right:<23s}  |{ascii_part}|")
    return "\n".join(lines)


def _hexdump_line(data: bytes, offset: int, length: int = 16) -> str:
    chunk = data[offset : offset + length]
    hex_part = " ".join(f"{b:02x}" for b in chunk)
    ascii_part = "".join(chr(b) if 0x20 <= b < 0x7F else "." for b in chunk)
    return f"  {offset:04x}  {hex_part:<48s}  |{ascii_part}|"


def _compact_hexdump(data: bytes, max_lines: int = 8) -> list[str]:
    lines = []
    total_lines = (len(data) + 15) // 16

    if total_lines <= max_lines:
        for offset in range(0, len(data), 16):
            lines.append(_hexdump_line(data, offset))
        return lines

    head_lines = max_lines // 2
    tail_lines = max_lines - head_lines - 1

    for i in range(head_lines):
        lines.append(_hexdump_line(data, i * 16))

    skipped = total_lines - head_lines - tail_lines
    lines.append(f"  ...  ({skipped} lines, {skipped * 16} bytes skipped)")

    tail_start = (total_lines - tail_lines) * 16
    for offset in range(tail_start, len(data), 16):
        lines.append(_hexdump_line(data, offset))

    return lines


def _normalize_marker_type(marker_type: str | None, *, strict: bool = True) -> str:
    token = (marker_type or "").strip().lower().replace(" ", "_")
    normalized = MARKER_TYPE_ALIASES.get(token)
    if normalized:
        return normalized
    if strict:
        allowed = ", ".join(STANDARD_MARKER_TYPES)
        raise typer.Exit(f"Invalid --type {marker_type!r}. Use one of: {allowed}.")
    return "observation"


def _normalize_marker_label(label: str) -> str:
    token = (label or "").strip().lower()
    token = MARKER_LABEL_SANITIZE.sub("_", token).strip("_")
    if not token:
        raise typer.Exit("Marker label is empty after normalization.")
    return token


def _default_provenance_output_dir() -> Path:
    cwd = Path.cwd()
    if (cwd / "pyproject.toml").exists() and (cwd / "tests" / "fixtures").exists():
        return cwd / "tests" / "fixtures" / "provenance"
    return Path.home() / ".local" / "share" / "netaudio" / "provenance" / "fixtures"


def _default_fixture_root() -> Path:
    cwd = Path.cwd()
    candidate = cwd / "tests" / "fixtures"
    if candidate.exists():
        return candidate
    return _default_provenance_output_dir().parent


def _default_label_overrides_path() -> Path:
    cwd = Path.cwd()
    default = cwd / "tests" / "fixtures" / "label_provenance_overrides.json"
    if default.exists():
        return default
    return Path.home() / ".local" / "share" / "netaudio" / "provenance" / "label_provenance_overrides.json"


def _parse_u16_token(token: str) -> int:
    value = (token or "").strip()
    if value.lower().startswith("0x"):
        return int(value, 16) & 0xFFFF
    return int(value, 10) & 0xFFFF


def _parse_set_opcode(token: str) -> tuple[tuple[int, int], str]:
    left, label = token.split("=", 1)
    protocol_token, opcode_token = left.split(":", 1)
    protocol_id = _parse_u16_token(protocol_token)
    opcode = _parse_u16_token(opcode_token)
    clean_label = label.strip()
    if not clean_label:
        raise ValueError("empty label")
    return (protocol_id, opcode), clean_label


def _parse_set_message(token: str) -> tuple[int, str]:
    left, label = token.split("=", 1)
    message_type = _parse_u16_token(left)
    clean_label = label.strip()
    if not clean_label:
        raise ValueError("empty label")
    return message_type, clean_label


def _parse_set_status(token: str) -> tuple[int, dict[str, object]]:
    left, value = token.split("=", 1)
    status_code = _parse_u16_token(left)
    text = value.strip()
    if not text:
        raise ValueError("empty status value")

    if ":" in text:
        state, label = text.split(":", 1)
        state = state.strip() or "unknown"
        label = label.strip()
    else:
        state = "unknown"
        label = text

    if not label:
        raise ValueError("empty status label")

    return status_code, {
        "state": state,
        "label": label,
        "detail": None,
        "labels": [label],
    }


def _valid_label(label: str) -> bool:
    if not label:
        return False
    if any(ch in label for ch in ("\r", "\n", "\t")):
        return False
    return True


def _parse_field_spec(spec: str) -> dict:
    parts = spec.split(":")
    if len(parts) < 4:
        print(
            f"Invalid --field format: {spec!r}. "
            "Expected name:offset:length:type[:value]",
            file=sys.stderr,
        )
        raise typer.Exit(1)
    result = {
        "name": parts[0],
        "offset": int(parts[1]),
        "length": int(parts[2]),
        "dtype": parts[3],
    }
    if len(parts) >= 5:
        result["value"] = parts[4]
    return result


def _format_endpoint(ip: str | None, port: int | None) -> str:
    host = ip or "?"
    if port in (None, 0):
        return host
    return f"{host}:{port}"


def _print_packet_table_header():
    print(
        f"  {'ID':<6s}  {'Time':12s}  "
        f"{'Source':>{PACKET_ENDPOINT_WIDTH}s} {'Dir':3s} {'Destination':<{PACKET_ENDPOINT_WIDTH}s}  "
        f"{'Type':>10s}  {'Size':>6s}  {'Info'}"
    )
    from netaudio.common.app_config import settings as app_settings
    separator = "-" if app_settings.no_color else "─"
    print("  " + separator * (76 + PACKET_ENDPOINT_WIDTH * 2))


def _print_packet_line(
    packet_id,
    timestamp_ns,
    source_ip,
    source_port,
    destination_ip,
    destination_port,
    direction,
    payload,
    dump=False,
    source_endpoint: str | None = None,
    destination_endpoint: str | None = None,
    dissect_mode=False,
):
    timestamp = datetime.datetime.fromtimestamp(timestamp_ns / 1e9)
    timestamp_str = timestamp.strftime("%H:%M:%S.%f")[:-3]

    size = len(payload)
    info_str = _label_packet(payload)
    if not info_str:
        info_str = PORT_LABELS.get(destination_port) or PORT_LABELS.get(source_port, "")

    arrow = "->" if direction == "request" else "<-" if direction == "response" else "**"
    direction_label = direction or "multicast"
    source = source_endpoint or _format_endpoint(source_ip, source_port)
    destination = destination_endpoint or _format_endpoint(destination_ip, destination_port)

    print(
        f"  {packet_id:<6d}  {timestamp_str:12s}  "
        f"{source:>{PACKET_ENDPOINT_WIDTH}s} {arrow} {destination:<{PACKET_ENDPOINT_WIDTH}s}  "
        f"{direction_label:>10s}  {size:5d}B  {info_str}"
    )

    if dump:
        if dissect_mode:
            from netaudio.dante.packet_dissector import dissect_and_render
            print(dissect_and_render(payload))
        else:
            print(_hexdump(payload))


def _packet_fingerprint(
    payload: bytes,
    src_ip: str | None,
    src_port: int | None,
    dst_ip: str | None,
    dst_port: int | None,
    direction: str | None,
) -> str:
    digest = hashlib.blake2b(digest_size=16)
    digest.update((src_ip or "").encode("utf-8", "ignore"))
    digest.update(b"|")
    digest.update(str(src_port if src_port is not None else "").encode("ascii", "ignore"))
    digest.update(b"|")
    digest.update((dst_ip or "").encode("utf-8", "ignore"))
    digest.update(b"|")
    digest.update(str(dst_port if dst_port is not None else "").encode("ascii", "ignore"))
    digest.update(b"|")
    digest.update((direction or "").encode("utf-8", "ignore"))
    digest.update(b"|")
    digest.update(payload)
    return digest.hexdigest()


def _as_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _coalesce(*values):
    for value in values:
        if value is not None:
            return value
    return None


def _parse_config_int(value, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise typer.Exit(f"Invalid integer for {field_name} in capture config: {value!r}")
    try:
        return int(value)
    except Exception:
        raise typer.Exit(f"Invalid integer for {field_name} in capture config: {value!r}")


def _parse_config_bool(value, field_name: str) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"1", "true", "yes", "on"}:
            return True
        if token in {"0", "false", "no", "off"}:
            return False
    raise typer.Exit(f"Invalid boolean for {field_name} in capture config: {value!r}")


def _default_capture_config_path() -> Path:
    from netaudio.common.config_loader import default_config_path

    return default_config_path()


def _load_capture_profile(config: str | None, profile: str | None) -> tuple[dict, Path]:
    from netaudio.common.config_loader import load_capture_profile

    try:
        return load_capture_profile(config, profile)
    except ValueError as exception:
        raise typer.Exit(str(exception))


def _resolve_db_from_config(db: str | None, profile_cfg: dict) -> str:
    from netaudio.common.config_loader import resolve_db_from_config

    return resolve_db_from_config(db, profile_cfg)


def _resolve_redis_from_config(
    profile_cfg: dict,
    redis_host: Optional[str],
    redis_port: Optional[int],
    redis_db: Optional[int],
    redis_password: Optional[str],
    redis_socket: Optional[str],
) -> tuple[Optional[str], Optional[int], Optional[int], Optional[str], Optional[str]]:
    redis_cfg = _as_dict(profile_cfg.get("redis"))

    resolved_host = _coalesce(redis_host, redis_cfg.get("host"))
    resolved_port = _parse_config_int(_coalesce(redis_port, redis_cfg.get("port")), "redis.port")
    resolved_db = _parse_config_int(_coalesce(redis_db, redis_cfg.get("db")), "redis.db")
    resolved_password = _coalesce(redis_password, redis_cfg.get("password"))
    resolved_socket = _coalesce(
        redis_socket,
        redis_cfg.get("socket"),
        redis_cfg.get("socket_path"),
        redis_cfg.get("unix_socket"),
    )
    return (
        str(resolved_host) if resolved_host is not None else None,
        resolved_port,
        resolved_db,
        str(resolved_password) if resolved_password is not None else None,
        str(resolved_socket) if resolved_socket is not None else None,
    )


def _parse_int_option(value: str | None, option_name: str) -> int | None:
    if value is None:
        return None

    try:
        if value.startswith("0x") or value.startswith("0X"):
            return int(value, 16)

        return int(value)
    except ValueError:
        print(f"Capture: {option_name} must be an integer or hex value (e.g. 0x2010), got: {value}", file=sys.stderr)
        raise typer.Exit(1)


def _parse_time_filter(value: str | None, store: PacketStore, session_id: int) -> int | None:
    if value is None:
        return None

    import datetime

    session = store.get_session(session_id)
    if not session:
        return None

    session_start_ns = int(session["started_ns"])
    session_date = datetime.datetime.fromtimestamp(session_start_ns / 1e9)

    parts = value.split(":")
    if len(parts) < 2:
        print(f"Capture: time filter must be HH:MM:SS or HH:MM:SS.fff, got: {value}", file=sys.stderr)
        raise typer.Exit(1)

    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = 0
        microseconds = 0

        if len(parts) >= 3:
            sec_parts = parts[2].split(".")
            seconds = int(sec_parts[0])

            if len(sec_parts) > 1:
                frac = sec_parts[1].ljust(6, "0")[:6]
                microseconds = int(frac)

        target = session_date.replace(
            hour=hours, minute=minutes, second=seconds, microsecond=microseconds
        )
        return int(target.timestamp() * 1e9)
    except (ValueError, IndexError):
        print(f"Capture: invalid time format: {value}", file=sys.stderr)
        raise typer.Exit(1)


def _require_positive_session_id(value: int | None, option_name: str) -> None:
    if value is None:
        return
    if int(value) <= 0:
        raise typer.Exit(f"{option_name} must be > 0.")


def _resolve_session_reference(
    store: PacketStore,
    *,
    session_id: int | None,
    session: str | None,
    default_selector: str | None = None,
) -> tuple[int, dict]:
    if session_id is not None and session:
        raise typer.Exit("Use either --id or --session, not both.")

    resolved_session: dict | None = None
    if session_id is not None:
        _require_positive_session_id(session_id, "--id")
        resolved_session = store.get_session(int(session_id))
        if not resolved_session:
            raise typer.Exit(f"Capture: Session #{session_id} not found.")
        return int(resolved_session["id"]), resolved_session

    selector = (session or default_selector or "").strip()
    if not selector:
        raise typer.Exit("Session reference is required. Use --id or --session.")

    lower = selector.lower()
    if lower in {"latest", "last"}:
        resolved_session = store.get_latest_session(active_only=False)
    elif lower in {"active", "current"}:
        resolved_session = store.get_latest_session(active_only=True)
    elif selector.isdigit():
        candidate_id = int(selector)
        _require_positive_session_id(candidate_id, "--session")
        resolved_session = store.get_session(candidate_id)
    else:
        resolved_session = store.find_session_by_name(selector, active_only=False)

    if not resolved_session:
        raise typer.Exit(f"Capture: session {selector!r} not found.")
    return int(resolved_session["id"]), resolved_session


def _resolve_marker_window(
    store: PacketStore,
    *,
    session_id: int,
    from_label: str | None,
    to_label: str | None,
) -> tuple[int | None, int | None]:
    start_ns = None
    end_ns = None
    if from_label:
        start_ns = store.get_marker_timestamp(session_id, _normalize_marker_label(from_label), latest=False)
        if start_ns is None:
            raise typer.Exit(f"Capture: marker label {from_label!r} not found in session #{session_id}.")
    if to_label:
        end_ns = store.get_marker_timestamp(session_id, _normalize_marker_label(to_label), latest=True)
        if end_ns is None:
            raise typer.Exit(f"Capture: marker label {to_label!r} not found in session #{session_id}.")
    if start_ns is not None and end_ns is not None and start_ns > end_ns:
        raise typer.Exit(
            f"Capture: invalid marker window ({from_label!r} occurs after {to_label!r}) in session #{session_id}."
        )
    return start_ns, end_ns


def _parse_optional_int(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _resolve_facts_path() -> Path:
    from netaudio.dante.fact_store import DEFAULT_FACTS_PATH
    return DEFAULT_FACTS_PATH
