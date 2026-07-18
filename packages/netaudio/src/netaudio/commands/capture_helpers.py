from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

from netaudio.capture.markers import (
    MARKER_LABEL_SANITIZE,
    MARKER_TYPE_ALIASES,
    STANDARD_MARKER_TYPES,
    normalize_marker_label,
    normalize_marker_type,
)
from netaudio.capture.packets import (
    ARC_PROTOCOLS,
    PACKET_ENDPOINT_WIDTH,
    PORT_LABELS,
    TARGET_PROTOCOLS,
    _compact_hexdump,
    _format_endpoint,
    _hexdump,
    _hexdump_line,
    _label_packet,
    _load_fact_labels,
    _packet_fingerprint,
    _print_packet_table_header,
)
from netaudio.dante.packet_store import PacketStore


def _normalize_marker_type(marker_type: str | None, *, strict: bool = True) -> str:
    try:
        return normalize_marker_type(marker_type, strict=strict)
    except ValueError as exception:
        raise typer.Exit(str(exception))


def _normalize_marker_label(label: str) -> str:
    try:
        return normalize_marker_label(label)
    except ValueError as exception:
        raise typer.Exit(str(exception))


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
            f"Invalid --field format: {spec!r}. Expected name:offset:length:type[:value]",
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

        target = session_date.replace(hour=hours, minute=minutes, second=seconds, microsecond=microseconds)
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
