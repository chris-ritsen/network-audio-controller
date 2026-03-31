from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import os
import re
import signal
import socket
import struct
import sys
import time
import traceback
from collections import deque
from pathlib import Path
from threading import Event, Thread
from typing import Optional

import typer

from netaudio.common.app_config import settings as app_settings
from netaudio.common.app_config import get_available_interfaces
from netaudio.dante.const import (
    DEVICE_INFO_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
)
from netaudio.dante.packet_store import DEFAULT_DB_PATH, PacketStore
from netaudio.dante.tshark_capture import TsharkCapture

try:
    import tomllib
except ImportError:
    tomllib = None

if tomllib is None:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None

try:
    from redis import Redis
    from redis.exceptions import ConnectionError as RedisConnectionError
except ImportError:
    Redis = None
    RedisConnectionError = None


from netaudio.icons import icon
from netaudio._common import ansi

_LAST_REDIS_ERROR: str | None = None


def _hrule(width: int) -> str:
    return "-" * width if app_settings.no_color else "─" * width


def _emdash() -> str:
    return "--" if app_settings.no_color else "\u2014"


def _rarrow() -> str:
    return "->" if app_settings.no_color else "\u2192"


_redis_client_cache: dict[str, "Redis"] = {}


def _resolve_host_ipv4(hostname: str) -> str:
    import socket as _socket

    try:
        results = _socket.getaddrinfo(hostname, None, _socket.AF_INET, _socket.SOCK_STREAM)
        if results:
            return results[0][4][0]
    except _socket.gaierror:
        pass
    return hostname


def _get_redis_client(
    host: str | None = None,
    port: int | None = None,
    db: int | None = None,
    password: str | None = None,
    socket_path: str | None = None,
):
    global _LAST_REDIS_ERROR
    if Redis is None:
        _LAST_REDIS_ERROR = "python package 'redis' is not installed in this environment"
        return None
    try:
        resolved_socket = socket_path or os.environ.get("REDIS_SOCKET")
        resolved_host = host or os.environ.get("REDIS_HOST") or "localhost"
        resolved_port = port if port is not None else int(os.environ.get("REDIS_PORT") or 6379)
        resolved_db = db if db is not None else int(os.environ.get("REDIS_DB") or 0)
        resolved_password = password or os.environ.get("REDIS_PASSWORD")

        cache_key = f"{resolved_socket or resolved_host}:{resolved_port}:{resolved_db}"
        if cache_key in _redis_client_cache:
            cached = _redis_client_cache[cache_key]
            try:
                cached.ping()
                return cached
            except Exception:
                del _redis_client_cache[cache_key]

        if resolved_socket:
            client = Redis(
                unix_socket_path=resolved_socket, db=resolved_db, password=resolved_password,
                decode_responses=True, socket_timeout=5, socket_connect_timeout=5,
            )
        else:
            resolved_ip = _resolve_host_ipv4(resolved_host)
            client = Redis(
                host=resolved_ip,
                port=resolved_port,
                db=resolved_db,
                password=resolved_password,
                decode_responses=True,
                socket_timeout=5, socket_connect_timeout=5,
            )
        client.ping()
        _LAST_REDIS_ERROR = None
        _redis_client_cache[cache_key] = client
        return client
    except Exception as exception:
        _LAST_REDIS_ERROR = f"{type(exception).__name__}: {exception}"
        return None


def _resolve_devices_from_redis(redis_client):
    if not redis_client:
        return {}

    mapping = {}
    try:
        keys = redis_client.keys("netaudio:daemon:device:*")
        for key in keys:
            data = redis_client.hgetall(key)
            if data and data.get("ipv4"):
                name = data.get("name") or key.rsplit(":", 1)[-1]
                mapping[name] = data["ipv4"]
    except Exception:
        pass
    return mapping


def _hexdump(data: bytes, indent: str = "         "):
    lines = []
    for offset in range(0, len(data), 16):
        chunk = data[offset : offset + 16]
        left = " ".join(f"{b:02x}" for b in chunk[:8])
        right = " ".join(f"{b:02x}" for b in chunk[8:])
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"{indent}{offset:04x}  {left:<23s}  {right:<23s}  |{ascii_part}|")
    return "\n".join(lines)


_FACT_LABEL_CACHE: dict[str, str] | None = None

ARC_PROTOCOLS = (0x27FF, 0x2809)


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
        opcode_str = f"0x{opcode:04X}"
        fact_labels = _load_fact_labels()
        if protocol_id in ARC_PROTOCOLS:
            fact_name = fact_labels.get(f"arc:{opcode_str}")
            if fact_name:
                return f"{opcode_str} {fact_name}"
        name = get_opcode_name(protocol_id, opcode)
        if name and name != opcode_str:
            return f"{opcode_str} {name}"
        return opcode_str

    if protocol_id == 0xFFFF and len(payload) >= 28:
        message_type = struct.unpack(">H", payload[26:28])[0]
        msg_str = f"0x{message_type:04X}"
        fact_labels = _load_fact_labels()
        fact_name = fact_labels.get(f"conmon:{msg_str}") or fact_labels.get(f"multicast:{msg_str}")
        if fact_name:
            return f"{msg_str} {fact_name}"
        name = get_settings_message_type_name(message_type)
        if name and name != msg_str:
            return f"{msg_str} {name}"
        return msg_str

    return f"proto:0x{protocol_id:04X}"


PORT_LABELS = {
    8751: "metering",
    8702: "info",
}

PACKET_ENDPOINT_WIDTH = 28

STANDARD_MARKER_TYPES = (
    "action", "observation", "state_change", "system", "hypothesis",
    "evidence", "analysis", "bug", "bug:fix", "code_change",
    "code_change:fix", "code_change:feat", "code_change:refactor",
)
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
    "bug": "bug",
    "bug:fix": "bug:fix",
    "bugfix": "bug:fix",
    "code_change": "code_change",
    "code-change": "code_change",
    "code_change:fix": "code_change:fix",
    "code_change:feat": "code_change:feat",
    "code_change:refactor": "code_change:refactor",
}
MARKER_LABEL_SANITIZE = re.compile(r"[^a-z0-9]+")


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


def _default_session_name() -> str:
    return datetime.datetime.now().strftime("session_%Y%m%d_%H%M%S")


def _parse_optional_int(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


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
    print("  " + _hrule(76 + PACKET_ENDPOINT_WIDTH * 2))


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


def _default_interface() -> tuple[str, str]:
    if app_settings.interface:
        return app_settings.interface, "config"

    if sys.platform == "darwin":
        interface, service_name = _default_interface_macos()
        if interface:
            return interface, service_name

    interfaces = get_available_interfaces()
    for name, ip, _ in interfaces:
        if ip != "127.0.0.1":
            return name, "first available"

    if interfaces:
        return interfaces[0][0], "first available"

    return "any", "fallback"


def _default_interface_macos() -> tuple[str | None, str | None]:
    import subprocess

    try:
        result = subprocess.run(
            ["networksetup", "-listnetworkserviceorder"],
            capture_output=True, text=True, timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None, None

    if result.returncode != 0:
        return None, None

    available_ips = {
        name: ip
        for name, ip, _ in get_available_interfaces()
        if ip != "127.0.0.1"
    }

    import re
    for match in re.finditer(
        r"^\(\d+\)\s+(.+)\n\(Hardware Port: .+, Device: (\S+)\)",
        result.stdout,
        re.MULTILINE,
    ):
        service_name = match.group(1)
        device = match.group(2)
        if device in available_ips:
            return device, service_name

    return None, None


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


class CaptureDaemon:
    def __init__(
        self,
        db_path: str,
        interface: str = "en0",
        use_tshark: bool = True,
        use_multicast: bool = True,
        device_filter: list = None,
        opcode_filter: list = None,
        export_dir: str = None,
        live: bool = True,
        dump: bool = False,
        dissect: bool = False,
        metering: bool = False,
        tcp: bool = False,
        session_id: int | None = None,
        session_name: str | None = None,
        redis_host: str | None = None,
        redis_port: int | None = None,
        redis_db: int | None = None,
        redis_password: str | None = None,
        redis_socket: str | None = None,
        relay_stream: str | None = None,
    ):
        self.stop_event = Event()
        self.store = PacketStore(db_path=db_path)
        self.interface = interface
        self.dump = dump
        self.dissect = dissect
        self.metering = metering
        self.tcp = tcp
        self.use_tshark = use_tshark
        self.use_multicast = use_multicast
        self.device_filter = device_filter or []
        self._explicit_device_filter = bool(device_filter)
        self.opcode_filter = opcode_filter or []
        self.export_dir = export_dir
        self.live = live
        self._threads = []
        self._packet_count = 0
        self._name_to_ip = {}
        self._ip_to_name = {}
        self._multicast_started = False
        self._tshark_failure_reported = False
        self.session_id = session_id
        self.session_name = session_name
        self._auto_session = False
        self._source_host = socket.gethostname()
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password
        self.redis_socket = redis_socket
        self.relay_stream = relay_stream
        self._relay_redis = None

    def _label_endpoint(self, ip, port):
        name = self._ip_to_name.get(ip)
        if name:
            return f"{name}:{port}"
        return f"{ip}:{port}"

    def _publish_packet_to_relay(self, packet_id: int, fields: dict):
        if not self.relay_stream or self._relay_redis is None:
            return

        payload = fields.get("payload", b"")
        if isinstance(payload, bytes):
            payload_hex = payload.hex()
            payload_len = len(payload)
        else:
            payload_hex = ""
            payload_len = 0

        event = {
            "event": "packet",
            "packet_id": str(packet_id),
            "source_host": self._source_host,
            "timestamp_ns": str(fields.get("timestamp_ns") or time.time_ns()),
            "src_ip": str(fields.get("src_ip") or ""),
            "src_port": str(fields.get("src_port") or ""),
            "dst_ip": str(fields.get("dst_ip") or ""),
            "dst_port": str(fields.get("dst_port") or ""),
            "direction": str(fields.get("direction") or ""),
            "device_ip": str(fields.get("device_ip") or ""),
            "source_type": str(fields.get("source_type") or ""),
            "session_id": str(self.session_id or ""),
            "payload_len": str(payload_len),
            "payload_hex": payload_hex,
        }

        try:
            self._relay_redis.xadd(self.relay_stream, event, maxlen=200000, approximate=True)
        except Exception as exception:
            print(f"Capture: Redis relay publish failed: {exception}", file=sys.stderr)

    def _publish_marker_to_relay(
        self,
        session_id: int,
        marker_type: str,
        label: str,
        summary: str | None = None,
        note: str | None = None,
        data: dict | None = None,
        timestamp_ns: int | None = None,
    ):
        if not self.relay_stream or self._relay_redis is None:
            return

        if timestamp_ns is None:
            timestamp_ns = time.time_ns()

        event = {
            "event": "marker",
            "source_host": self._source_host,
            "timestamp_ns": str(timestamp_ns),
            "session_id": str(session_id),
            "marker_type": str(marker_type),
            "label": str(label),
            "summary": str(summary or ""),
            "note": str(note or ""),
            "data_json": json.dumps(data, sort_keys=True) if data else "",
        }

        try:
            self._relay_redis.xadd(self.relay_stream, event, maxlen=200000, approximate=True)
        except Exception as exception:
            print(f"Capture: Redis relay marker publish failed: {exception}", file=sys.stderr)

    def _print_packet(self, packet_id, fields):
        self._packet_count += 1
        payload = fields.get("payload", b"")
        _print_packet_line(
            packet_id=packet_id,
            timestamp_ns=fields.get("timestamp_ns") or time.time_ns(),
            source_ip=fields.get("src_ip"),
            source_port=fields.get("src_port"),
            destination_ip=fields.get("dst_ip"),
            destination_port=fields.get("dst_port"),
            direction=fields.get("direction"),
            payload=payload,
            dump=self.dump,
            source_endpoint=self._label_endpoint(fields.get("src_ip"), fields.get("src_port")),
            destination_endpoint=self._label_endpoint(fields.get("dst_ip"), fields.get("dst_port")),
            dissect_mode=self.dissect,
        )

    def _multicast_worker(self, group: str, port: int):
        source_ip = app_settings.interface_ip or ""
        multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        try:
            multicast_socket.bind((group, port))
            group_bytes = socket.inet_aton(group)
            if source_ip:
                membership_request = struct.pack("4s4s", group_bytes, socket.inet_aton(source_ip))
            else:
                membership_request = struct.pack("4sL", group_bytes, socket.INADDR_ANY)
            multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership_request)
            multicast_socket.settimeout(1.0)
            print(f"Capture: Listening on multicast {group}:{port}", file=sys.stderr)

            while not self.stop_event.is_set():
                try:
                    data, addr = multicast_socket.recvfrom(2048)
                    timestamp_ns = time.time_ns()
                    source_host, source_port = addr

                    device_name = self._ip_to_name.get(source_host)

                    packet_id = self.store.store_packet(
                        payload=data,
                        source_type="multicast",
                        src_ip=source_host,
                        src_port=source_port,
                        dst_ip=group,
                        dst_port=port,
                        device_name=device_name,
                        device_ip=source_host,
                        multicast_group=group,
                        multicast_port=port,
                        session_id=self.session_id,
                        timestamp_ns=timestamp_ns,
                        interface=self.interface,
                    )

                    if packet_id and self.live:
                        self._print_packet(
                            packet_id,
                            {
                                "src_ip": source_host,
                                "src_port": source_port,
                                "dst_ip": group,
                                "dst_port": port,
                                "direction": None,
                                "payload": data,
                                "timestamp_ns": timestamp_ns,
                                "device_ip": source_host,
                                "source_type": "multicast",
                            },
                        )
                    if packet_id:
                        self._publish_packet_to_relay(
                            packet_id,
                            {
                                "src_ip": source_host,
                                "src_port": source_port,
                                "dst_ip": group,
                                "dst_port": port,
                                "direction": None,
                                "payload": data,
                                "timestamp_ns": timestamp_ns,
                                "device_ip": source_host,
                                "source_type": "multicast",
                            },
                        )

                except socket.timeout:
                    continue
                except (socket.error, OSError) as exception:
                    if self.stop_event.is_set():
                        break
                    print(f"Capture: Socket error on {group}:{port}: {exception}", file=sys.stderr)
                    time.sleep(1)
                except Exception as exception:
                    print(f"Capture: Error on {group}:{port}: {exception}", file=sys.stderr)
                    traceback.print_exc()
                    time.sleep(1)
        except OSError as exception:
            print(f"Capture: Failed to bind multicast {group}:{port}: {exception}", file=sys.stderr)
        finally:
            multicast_socket.close()

    async def _run_tshark(self):
        tshark_filter_ips = self.device_filter if self._explicit_device_filter else None
        capture = TsharkCapture(
            packet_store=self.store,
            interface=self.interface,
            device_ips=tshark_filter_ips,
            known_device_ips=set(self.device_filter) if self.device_filter else None,
            include_metering=self.metering,
            include_tcp=self.tcp,
            session_id=self.session_id,
        )

        async def on_packet(packet_id, fields):
            fields["source_type"] = "tshark"
            self._publish_packet_to_relay(packet_id, fields)
            if self.live:
                self._print_packet(packet_id, fields)

        await capture.start(on_packet=on_packet)

    def _start_multicast_workers(self):
        if self._multicast_started or not self.use_multicast:
            return

        multicast_configs = [
            (MULTICAST_GROUP_CONTROL_MONITORING, DEVICE_INFO_PORT),
        ]
        for group, port in multicast_configs:
            thread = Thread(
                target=self._multicast_worker,
                args=(group, port),
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

        self._multicast_started = True

    def _report_tshark_failure(self, tshark_task):
        if self._tshark_failure_reported:
            return

        self._tshark_failure_reported = True
        exception_message = None
        if tshark_task and tshark_task.done():
            exception = tshark_task.exception()
            if exception:
                exception_message = str(exception)
                print(f"Capture: tshark failed: {exception_message}", file=sys.stderr)

        if exception_message:
            lower = exception_message.lower()
            if "dumpcap" in lower and "permission denied" in lower:
                print(
                    "Capture: tshark capture permissions are missing (dumpcap).\n"
                    "  Quick test: sudo uv run netaudio capture live\n"
                    "  Permanent fix: configure dumpcap capabilities/group permissions.",
                    file=sys.stderr,
                )
            elif "no such device" in lower or "unknown interface" in lower:
                print(
                    "Capture: tshark interface is invalid. Use --interface with a valid NIC.",
                    file=sys.stderr,
                )

        print(
            "Capture: tshark exited, falling back to multicast socket only.",
            file=sys.stderr,
        )

    def _print_stats(self):
        stats = self.store.get_stats()
        print(f"\n{'=' * 60}")
        print(f"{icon('capture')}Capture Statistics")
        print(f"{'=' * 60}")
        print(f"  Total packets:    {stats['total']}")
        print(f"  Correlated:       {stats['correlated']}")
        print(f"  Uncorrelated:     {stats['uncorrelated']}")

        if stats["by_source"]:
            print(f"\n  By source:")
            for source, count in stats["by_source"].items():
                print(f"    {source:25s} {count}")

        if stats["by_opcode"]:
            print(f"\n  By opcode/direction:")
            for entry in stats["by_opcode"][:20]:
                name = entry["opcode_name"] or "unknown"
                direction = entry["direction"] or "multicast"
                print(f"    {name:35s} {direction:10s} {entry['count']}")

    def _export_fixtures(self):
        if not self.export_dir:
            return

        pairs = self.store.get_correlated_pairs()
        if not pairs:
            print("No correlated pairs to export.")
            return

        exported = 0
        for request, response in pairs:
            result = self.store.export_correlated_pair(request["id"], self.export_dir)
            if result:
                exported += 1
                print(f"  Exported: {result[0]}")
                print(f"           {result[1]}")

        print(f"\nExported {exported} correlated pair(s) to {self.export_dir}")

    def _resolve_device_filter(self):
        redis_client = _get_redis_client(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            password=self.redis_password,
            socket_path=self.redis_socket,
        )
        name_to_ip = _resolve_devices_from_redis(redis_client)

        if self.device_filter:
            resolved = []
            for entry in self.device_filter:
                if entry in name_to_ip:
                    resolved.append(name_to_ip[entry])
                    print(f"Capture: Resolved {entry} -> {name_to_ip[entry]}")
                else:
                    resolved.append(entry)
            self.device_filter = resolved
        elif name_to_ip:
            self.device_filter = list(name_to_ip.values())
            print(f"Capture: Auto-discovered {len(name_to_ip)} device(s) from daemon")
            for name, ip in sorted(name_to_ip.items()):
                print(f"  {name} ({ip})")
        else:
            print("Capture: No devices found in Redis -- capturing all traffic")

        self._name_to_ip = name_to_ip
        self._ip_to_name = {v: k for k, v in name_to_ip.items()}

    async def run(self):
        print(f"{icon('capture')}Capture: Database at {self.store._db_path}")

        if self.relay_stream:
            self._relay_redis = _get_redis_client(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                socket_path=self.redis_socket,
            )
            if self._relay_redis is None:
                print("Capture: Redis relay requested but Redis is unavailable.", file=sys.stderr)
            else:
                print(f"Capture: Relaying packets to Redis stream {self.relay_stream}")

        if self.session_name and self.session_id is None:
            self.session_id = self.store.start_session(
                name=self.session_name,
                source_host=self._source_host,
                metadata={
                    "interface": self.interface,
                    "relay_stream": self.relay_stream,
                },
            )
            self._auto_session = True
            print(f"Capture: Started session #{self.session_id} ({self.session_name})")

        if self.session_id is not None:
            marker_ts = time.time_ns()
            self.store.add_marker(
                session_id=self.session_id,
                marker_type="system",
                label="capture_started",
                source_host=self._source_host,
                data={
                    "interface": self.interface,
                    "relay_stream": self.relay_stream,
                },
                timestamp_ns=marker_ts,
            )
            self._publish_marker_to_relay(
                session_id=self.session_id,
                marker_type="system",
                label="capture_started",
                data={
                    "interface": self.interface,
                    "relay_stream": self.relay_stream,
                },
                timestamp_ns=marker_ts,
            )

        self._resolve_device_filter()

        if self.device_filter and self._explicit_device_filter:
            print(f"Capture: Filtering to IPs: {', '.join(sorted(self.device_filter))}")
        elif self.device_filter:
            print(f"Capture: Known device IPs: {', '.join(sorted(self.device_filter))}")

        if self.live:
            print("\nPackets")
            _print_packet_table_header()

        tshark_task = None
        tshark_running = False
        if self.use_tshark:
            if TsharkCapture.is_available():
                print("Capture: Starting tshark...", flush=True)
                tshark_task = asyncio.create_task(self._run_tshark())
                await asyncio.sleep(0.2)
                if tshark_task.done():
                    self._report_tshark_failure(tshark_task)
                    if not self.use_multicast:
                        exception = tshark_task.exception()
                        if exception:
                            raise RuntimeError(f"tshark failed and multicast is disabled: {exception}")
                        raise RuntimeError("tshark exited and multicast is disabled")
                else:
                    tshark_running = True
            else:
                print(
                    "Capture: tshark not found, falling back to multicast socket only.\n"
                    "  Install with: brew install --cask wireshark",
                    file=sys.stderr,
                )

        if self.use_multicast and not tshark_running:
            self._start_multicast_workers()

        try:
            while not self.stop_event.is_set():
                if (
                    self.use_multicast
                    and not self._multicast_started
                    and tshark_task is not None
                    and tshark_task.done()
                ):
                    self._report_tshark_failure(tshark_task)
                    self._start_multicast_workers()

                if not self.use_multicast and tshark_task is not None and tshark_task.done():
                    exception = tshark_task.exception()
                    if exception:
                        raise RuntimeError(f"tshark failed and multicast is disabled: {exception}")
                    raise RuntimeError("tshark exited and multicast is disabled")

                await asyncio.sleep(0.1)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            self.stop_event.set()

            if tshark_task and not tshark_task.done():
                tshark_task.cancel()
                try:
                    await tshark_task
                except asyncio.CancelledError:
                    pass

            for thread in self._threads:
                thread.join(timeout=3.0)

            self._print_stats()

            if self.export_dir:
                self._export_fixtures()

            if self.session_id is not None:
                marker_ts = time.time_ns()
                self.store.add_marker(
                    session_id=self.session_id,
                    marker_type="system",
                    label="capture_stopped",
                    source_host=self._source_host,
                    data={
                        "packets_total": self.store.get_stats().get("total", 0),
                    },
                    timestamp_ns=marker_ts,
                )
                self._publish_marker_to_relay(
                    session_id=self.session_id,
                    marker_type="system",
                    label="capture_stopped",
                    data={
                        "packets_total": self.store.get_stats().get("total", 0),
                    },
                    timestamp_ns=marker_ts,
                )

            if self._auto_session and self.session_id is not None:
                self.store.end_session(self.session_id)
                print(f"{icon('session')}Capture: Ended session #{self.session_id}")

            self.store.close()
            print(f"\n{icon('capture')}Capture stopped.")


async def _replay_packet(
    packet_id: int, store: PacketStore, interface: str, tshark_duration: float, dump: bool = False
):
    packet = store.get_packet(packet_id)
    if not packet:
        print(f"Error: Packet #{packet_id} not found in database.", file=sys.stderr)
        raise typer.Exit(1)

    payload = packet["payload"]
    if isinstance(payload, str):
        payload = bytes.fromhex(payload)

    destination_ip = packet["dst_ip"]
    destination_port = packet["dst_port"]

    if not destination_ip or not destination_port:
        print(f"Error: Packet #{packet_id} has no destination address.", file=sys.stderr)
        raise typer.Exit(1)

    if destination_ip.startswith("224."):
        print(
            f"Error: Packet #{packet_id} targets multicast address {destination_ip}, nothing to replay.",
            file=sys.stderr,
        )
        raise typer.Exit(1)

    print(f"{icon('packet')}Replaying packet #{packet_id}")
    print(f"  Target:  {destination_ip}:{destination_port}")
    print(f"  Size:    {len(payload)} bytes")
    info = _label_packet(payload)
    if info:
        print(f"  Label:   {info}")
    print()

    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    probe.connect((destination_ip, destination_port))
    local_ip = probe.getsockname()[0]
    probe.close()

    tshark_task = None
    received_packets = []

    if TsharkCapture.is_available():
        capture = TsharkCapture(
            packet_store=store,
            interface=interface,
            device_ips=[destination_ip],
        )

        async def on_tshark_packet(captured_packet_id, fields):
            if fields.get("direction") == "request" and fields.get("src_ip") == local_ip:
                return
            received_packets.append((captured_packet_id, fields))

        tshark_task = asyncio.create_task(capture.start(on_packet=on_tshark_packet))
        await asyncio.sleep(0.3)
    else:
        print(
            "Warning: tshark not found -- multicast responses won't be captured.\n"
            "  Install with: brew install --cask wireshark",
            file=sys.stderr,
        )

    send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    send_socket.settimeout(0.5)

    send_timestamp = time.time_ns()
    send_socket.sendto(payload, (destination_ip, destination_port))

    local_port = send_socket.getsockname()[1]

    sent_id = store.store_packet(
        payload=payload,
        source_type="replay_request",
        src_ip=local_ip,
        src_port=local_port,
        dst_ip=destination_ip,
        dst_port=destination_port,
        device_ip=destination_ip,
        direction="request",
        timestamp_ns=send_timestamp,
    )

    print("Packets")
    _print_packet_table_header()

    if sent_id:
        _print_packet_line(
            sent_id,
            send_timestamp,
            local_ip,
            local_port,
            destination_ip,
            destination_port,
            "request",
            payload,
            dump=dump,
        )

    try:
        reply_data, reply_addr = send_socket.recvfrom(4096)
        reply_timestamp = time.time_ns()
        reply_ip, reply_port = reply_addr

        reply_id = store.store_packet(
            payload=reply_data,
            source_type="replay_response",
            src_ip=reply_ip,
            src_port=reply_port,
            dst_ip=local_ip,
            dst_port=local_port,
            device_ip=reply_ip,
            direction="response",
            timestamp_ns=reply_timestamp,
        )

        if reply_id:
            _print_packet_line(
                reply_id,
                reply_timestamp,
                reply_ip,
                reply_port,
                local_ip,
                local_port,
                "response",
                reply_data,
                dump=dump,
            )
    except socket.timeout:
        print("  (no unicast reply within 500ms)")

    send_socket.close()

    if tshark_task:
        await asyncio.sleep(tshark_duration)
        tshark_task.cancel()
        try:
            await tshark_task
        except asyncio.CancelledError:
            pass

        for captured_packet_id, fields in received_packets:
            _print_packet_line(
                captured_packet_id,
                fields.get("timestamp_ns", time.time_ns()),
                fields.get("src_ip", "?"),
                fields.get("src_port", 0),
                fields.get("dst_ip", "?"),
                fields.get("dst_port", 0),
                fields.get("direction"),
                fields.get("payload", b""),
                dump=dump,
            )

    total = 1 + (1 if "reply_id" in dir() else 0) + len(received_packets)
    print(f"\n  {total} packet(s) total")


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
    direction_icon = icon("tx") if direction == "request" else icon("rx") if direction == "response" else icon("packet")
    source = source_endpoint or _format_endpoint(source_ip, source_port)
    destination = destination_endpoint or _format_endpoint(destination_ip, destination_port)

    print(
        f"  {direction_icon}{packet_id:<6d}  {timestamp_str:12s}  "
        f"{source:>{PACKET_ENDPOINT_WIDTH}s} {arrow} {destination:<{PACKET_ENDPOINT_WIDTH}s}  "
        f"{direction_label:>10s}  {size:5d}B  {info_str}"
    )

    if dissect_mode:
        from netaudio.dante.packet_dissector import dissect_and_render
        print(dissect_and_render(payload))
    elif dump:
        print(_hexdump(payload))


def _resolve_redis_for_capture(
    redis_host: Optional[str],
    redis_port: Optional[int],
    redis_db: Optional[int],
    redis_password: Optional[str],
    redis_socket: Optional[str],
):
    client = _get_redis_client(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
        socket_path=redis_socket,
    )
    if client is None:
        detail = f" ({_LAST_REDIS_ERROR})" if _LAST_REDIS_ERROR else ""
        raise typer.Exit(f"Redis is not available with the provided settings{detail}.")
    return client


app = typer.Typer(help="Capture and replay Dante traffic.", no_args_is_help=True)
session_app = typer.Typer(help="Manage capture sessions.", no_args_is_help=True)
app.add_typer(session_app, name="session")
packet_app = typer.Typer(help="Inspect individual captured packets.", no_args_is_help=True)
app.add_typer(packet_app, name="packet")


@app.command()
def live(
    interface: Optional[str] = typer.Option(None, "-i", "--interface", help="Network interface for capture."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    tshark: bool = typer.Option(True, "--tshark/--no-tshark", help="Enable tshark capture."),
    multicast: bool = typer.Option(True, "--multicast/--no-multicast", help="Enable multicast listener."),
    device: Optional[list[str]] = typer.Option(None, "--device", help="Filter to specific device name(s) or IP(s)."),
    opcode: Optional[list[str]] = typer.Option(None, "--opcode", help="Filter to specific opcode(s)."),
    export_dir: Optional[str] = typer.Option(
        None, "--export-dir", help="Directory to export fixture files on shutdown."
    ),
    show: bool = typer.Option(True, "--live/--no-live", help="Show live packet feed."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    metering: bool = typer.Option(False, "--metering", help="Include metering traffic (port 8751)."),
    tcp: bool = typer.Option(False, "--tcp", help="Include TCP traffic to/from devices."),
    session_id: Optional[int] = typer.Option(
        None, "--session-id", help="Attach packets to an existing capture session ID."
    ),
    session_name: Optional[str] = typer.Option(
        None, "--session-name", help="Create a new capture session with this name."
    ),
    redis_host: Optional[str] = typer.Option(None, "--redis-host", help="Redis host for device discovery/relay."),
    redis_port: Optional[int] = typer.Option(None, "--redis-port", help="Redis port for device discovery/relay."),
    redis_db: Optional[int] = typer.Option(None, "--redis-db", help="Redis DB index for device discovery/relay."),
    redis_password: Optional[str] = typer.Option(
        None, "--redis-password", help="Redis password for device discovery/relay."
    ),
    redis_socket: Optional[str] = typer.Option(
        None, "--redis-socket", help="Redis UNIX socket path for device discovery/relay."
    ),
    relay_stream: Optional[str] = typer.Option(
        None, "--relay-stream", help="Redis stream key to publish capture events."
    ),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.cli import state as cli_state

    _require_positive_session_id(session_id, "--session-id")

    profile_cfg, _ = _load_capture_profile(config, profile)
    capture_cfg = _as_dict(profile_cfg.get("capture"))
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    (
        resolved_redis_host,
        resolved_redis_port,
        resolved_redis_db,
        resolved_redis_password,
        resolved_redis_socket,
    ) = _resolve_redis_from_config(
        profile_cfg=profile_cfg,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_socket=redis_socket,
    )
    if interface:
        resolved_interface = interface
        interface_source = "--interface flag"
    elif app_settings.interface:
        resolved_interface = app_settings.interface
        interface_source = "NETAUDIO_INTERFACE" if os.environ.get("NETAUDIO_INTERFACE") else "--interface flag"
    elif capture_cfg.get("interface"):
        resolved_interface = capture_cfg["interface"]
        interface_source = "capture config"
    else:
        resolved_interface, interface_source = _default_interface()

    available = get_available_interfaces()
    interface_ip = None
    for iface_name, iface_ip, _ in available:
        if iface_name == resolved_interface:
            interface_ip = iface_ip
            break

    if not interface_ip and resolved_interface != "any":
        available_names = sorted({name for name, _, _ in available})
        print(
            f"Error: Interface '{resolved_interface}' not found or has no IP address.\n"
            f"  Available: {', '.join(available_names)}",
            file=sys.stderr,
        )
        raise typer.Exit(1)

    print(f"{icon('capture')}Capture: Interface {resolved_interface} ({interface_ip}) {_emdash()} {interface_source}")
    resolved_relay_stream = _coalesce(relay_stream, capture_cfg.get("ingress_stream"))

    daemon = CaptureDaemon(
        db_path=resolved_db,
        interface=resolved_interface,
        use_tshark=tshark,
        use_multicast=multicast,
        device_filter=device or [],
        opcode_filter=opcode or [],
        export_dir=export_dir,
        live=show,
        dump=dump,
        dissect=cli_state.dissect,
        metering=metering,
        tcp=tcp,
        session_id=session_id,
        session_name=session_name,
        redis_host=resolved_redis_host,
        redis_port=resolved_redis_port,
        redis_db=resolved_redis_db,
        redis_password=resolved_redis_password,
        redis_socket=resolved_redis_socket,
        relay_stream=str(resolved_relay_stream) if resolved_relay_stream is not None else None,
    )

    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)

    def on_signal(sig, frame):
        print(f"\nCapture: Signal {sig} received, shutting down...")
        daemon.stop_event.set()
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    try:
        asyncio.run(daemon.run())
    except Exception as exception:
        print(f"Capture: Fatal error: {exception}", file=sys.stderr)
        traceback.print_exc()
    finally:
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)


@app.command()
def replay(
    id: int = typer.Option(..., "--id", help="Packet ID to replay."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    interface: Optional[str] = typer.Option(None, "-i", "--interface", help="Network interface for tshark capture."),
    duration: float = typer.Option(2.0, "--duration", help="Seconds to listen for multicast responses."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    resolved_interface = interface or _default_interface()

    try:
        asyncio.run(_replay_packet(id, store, resolved_interface, duration, dump=dump))
    finally:
        store.close()


@session_app.command("start")
def session_start(
    name: Optional[str] = typer.Option(None, "--name", help="Session name (defaults to timestamped name)."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    description: Optional[str] = typer.Option(None, "--description", help="Session description."),
    source_host: Optional[str] = typer.Option(None, "--source-host", help="Host that owns this session."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_name = name or _default_session_name()
        resolved_host = source_host or socket.gethostname()
        session_id = store.start_session(
            name=resolved_name,
            source_host=resolved_host,
            description=description,
        )
        store.add_marker(
            session_id=session_id,
            marker_type="system",
            label=_normalize_marker_label("session_started"),
            source_host=resolved_host,
        )
        print(f"{icon('session')}Capture: Started session #{session_id} ({resolved_name})")
    finally:
        store.close()


@session_app.command("stop")
def session_stop(
    id: Optional[int] = typer.Option(None, "--id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to active.",
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    description: Optional[str] = typer.Option(None, "--description", help="Optional stop summary."),
    source_host: Optional[str] = typer.Option(None, "--source-host", help="Host recording stop marker."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=id,
            session=session,
            default_selector="active",
        )
        resolved_host = source_host or socket.gethostname()
        store.add_marker(
            session_id=resolved_session_id,
            marker_type="system",
            label=_normalize_marker_label("session_stopped"),
            note=description,
            source_host=resolved_host,
        )
        ok = store.end_session(resolved_session_id, description=description)
        if not ok:
            print(f"Capture: Session #{resolved_session_id} not found.", file=sys.stderr)
            raise typer.Exit(1)
        print(f"{icon('session')}Capture: Ended session #{resolved_session_id}")

        from netaudio.dante.protocol_verifier import export_session_bundle

        session_row = store.get_session(resolved_session_id)
        session_name = session_row["name"] if session_row else f"session_{resolved_session_id}"
        bundle_path = export_session_bundle(store, resolved_session_id)
        print(f"{icon('packet')}Capture: Exported bundle: {bundle_path}")
    finally:
        store.close()


@session_app.command("end", hidden=True)
def session_end(
    id: Optional[int] = typer.Option(None, "--id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to active.",
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    description: Optional[str] = typer.Option(None, "--description", help="Optional stop summary."),
    source_host: Optional[str] = typer.Option(None, "--source-host", help="Host recording stop marker."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Alias for stop."""
    session_stop(
        id=id,
        session=session,
        db=db,
        description=description,
        source_host=source_host,
        config=config,
        profile=profile,
    )


@session_app.command("rename")
def session_rename(
    name: str = typer.Argument(..., help="New session name."),
    id: Optional[int] = typer.Option(None, "--id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to active.",
    ),
    description: Optional[str] = typer.Option(None, "--description", help="Update description."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Rename a capture session."""
    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=id,
            session=session,
            default_selector="active",
        )
        ok = store.update_session(
            resolved_session_id,
            name=name,
            description=description,
        )
        if not ok:
            print(f"Capture: Session #{resolved_session_id} not found.", file=sys.stderr)
            raise typer.Exit(1)
        print(f"{icon('name')}Capture: Session #{resolved_session_id} renamed to '{name}'")
    finally:
        store.close()


def _print_session_evidence(store: PacketStore, sessions: list, has_evidence: bool, no_evidence: bool):
    from netaudio.dante.packet_dissector import dissect_and_render

    for session in sessions:
        session_id = int(session["id"])
        evidence_count = store.get_session_evidence_count(session_id)
        session_name = session.get("name") or f"session_{session_id}"

        if has_evidence and evidence_count == 0:
            continue
        if no_evidence and evidence_count > 0:
            continue
        if evidence_count == 0:
            continue

        markers = store.get_markers(session_id, marker_types=["evidence"])

        print(f"\n{icon('session')}Session #{session_id} ({session_name}) {_emdash()} {evidence_count} evidence packet(s)")
        print(_hrule(80))

        for marker in markers:
            marker_data = marker.get("data")
            if not marker_data:
                continue

            marker_label = marker.get("label") or ""
            marker_summary = marker.get("summary") or ""
            display_label = marker_summary or marker_label
            if display_label:
                print(f"\n  {icon('marker')}{display_label}")

            packet_ids = marker_data.get("packet_ids") or []
            if not packet_ids and marker_data.get("packet_id"):
                packet_ids = [marker_data["packet_id"]]

            for packet_id in packet_ids:
                packet = store.get_packet(packet_id)
                if not packet:
                    continue

                payload = packet.get("payload", b"")
                direction = packet.get("direction") or "multicast"
                source_ip = packet.get("src_ip") or "?"
                source_port = packet.get("src_port") or "?"
                destination_ip = packet.get("dst_ip") or "?"
                destination_port = packet.get("dst_port") or "?"

                opcode_hex = ""
                if len(payload) >= 8:
                    opcode_hex = f"0x{int.from_bytes(payload[6:8], 'big'):04X} "

                direction_icon = icon("tx") if direction == "request" else icon("rx") if direction == "response" else icon("packet")
                arrow = "->" if direction == "request" else "<-" if direction == "response" else "**"

                print(
                    f"\n    {direction_icon}#{packet_id} {direction:8s} {opcode_hex}"
                    f"{source_ip}:{source_port} {arrow} {destination_ip}:{destination_port} "
                    f"{len(payload)}B"
                )
                print(dissect_and_render(payload, indent="    "))


@session_app.command("list")
def session_list(
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    limit: int = typer.Option(25, "--limit", help="Number of sessions to show."),
    category: Optional[str] = typer.Option(None, "--category", help="Filter by category (experiment, diagnostic, etc)."),
    has_evidence: bool = typer.Option(False, "--has-evidence", "--evidence", help="Only show sessions with evidence-tagged packets."),
    no_evidence: bool = typer.Option(False, "--no-evidence", help="Only show sessions with no evidence-tagged packets."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio._common import output_table

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        sessions = store.list_sessions(limit=limit, category=category)
        if not sessions:
            print("Capture: No sessions.")
            return
        headers = ["ID", "Started", "Ended", "Packets", "Evidence", "Category", "Name"]
        rows = []
        json_data = []
        for session in sessions:
            session_id = int(session["id"])
            packets = store.get_session_packet_count(session_id)
            evidence = store.get_session_evidence_count(session_id)
            started = session.get("started_iso") or ""
            ended = session.get("ended_iso") or ""
            name = session.get("name") or ""
            session_category = session.get("category") or ""

            if has_evidence and evidence == 0:
                continue

            if no_evidence and evidence > 0:
                continue

            rows.append([str(session_id), started, ended, str(packets), str(evidence), session_category, name])
            json_data.append({
                "id": session_id,
                "name": name,
                "category": session_category,
                "started": started,
                "ended": ended,
                "packets": packets,
                "evidence": evidence,
                "source_host": session.get("source_host") or "",
                "description": session.get("description") or "",
            })
        output_table(headers, rows, json_data=json_data, title="Sessions")

        from netaudio.cli import state as cli_state
        if cli_state.dissect:
            _print_session_evidence(store, sessions, has_evidence, no_evidence)
    finally:
        store.close()


def _print_timeline_header(show_window_packets: bool = False):
    pass


def _format_marker_time(iso_timestamp: str | None) -> str:
    if not iso_timestamp:
        return ""
    try:
        parsed = datetime.datetime.fromisoformat(iso_timestamp)
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return iso_timestamp[:8] if iso_timestamp else ""


def _print_wrapped(text: str, indent: str = "  "):
    import shutil
    import textwrap

    terminal_width = shutil.get_terminal_size((120, 24)).columns
    wrapped = textwrap.fill(
        text,
        width=terminal_width,
        initial_indent=indent,
        subsequent_indent=indent,
    )
    print(wrapped)


def _print_marker_row(
    marker: dict,
    next_ts: int | None,
    session_id: int,
    store: PacketStore,
    use_dissect: bool = False,
    show_notes: bool = True,
    show_packets: bool = True,
    show_window_packets: bool = False,
    brief: bool = False,
):
    marker_id = int(marker["id"])
    marker_time = int(marker["timestamp_ns"])
    window_packets = None
    if show_window_packets:
        window_packets = store.get_session_packet_count(
            session_id, start_ns=marker_time, end_ns=next_ts
        )

    summary_text = marker.get("summary") or ""
    label_text = marker.get("label") or ""
    note_text = marker.get("note") or ""
    marker_type_str = _normalize_marker_type(str(marker.get("marker_type") or "observation"), strict=False)

    _MARKER_TYPE_ICONS = {
        "hypothesis": "info",
        "observation": "info",
        "evidence": "marker",
        "code_change": "config",
        "action": "success",
        "system": "server",
    }
    marker_type_icon = icon(_MARKER_TYPE_ICONS.get(marker_type_str, "marker"))
    time_str = _format_marker_time(marker.get("timestamp_iso"))

    if brief:
        display_text = summary_text if summary_text else label_text
        print(f"  {time_str}  {marker_type_icon}{marker_type_str:12s}  {display_text}")
        return

    print()
    header = f"{ansi('1', f'{marker_type_icon}{marker_type_str}')}  {label_text}"
    if window_packets is not None:
        header += f"  ({window_packets} packets)"

    marker_data = marker.get("data")
    evidence_packet_ids = []
    if marker_data and marker.get("marker_type") == "evidence":
        evidence_packet_ids = marker_data.get("packet_ids") or []
        if not evidence_packet_ids and marker_data.get("packet_id"):
            evidence_packet_ids = [marker_data["packet_id"]]

    if evidence_packet_ids:
        evidence_sizes = []
        for pid in evidence_packet_ids[:20]:
            pkt = store.get_packet(pid)
            if pkt:
                payload = pkt.get("payload", b"")
                evidence_sizes.append(f"#{pid} {len(payload)}B")
        if evidence_sizes:
            header += f"  {ansi('90', f'[{chr(44).join(evidence_sizes)}]')}"

    print(f"  {time_str}  {header}  {ansi('90', f'#{marker_id}')}")

    if summary_text and show_notes:
        _print_wrapped(summary_text)

    if show_notes and note_text:
        _print_wrapped(ansi("90", note_text))
    elif not show_notes and summary_text:
        _print_wrapped(summary_text)

    if not show_packets:
        return

    if marker_data and marker.get("marker_type") == "evidence":
        packet_ids = evidence_packet_ids
        filters = marker_data.get("filters", {})

        if filters:
            filter_parts = [f"{k}={v}" for k, v in filters.items()]
            print(f"    filters: {', '.join(filter_parts)}")

        evidence_indent = "      "
        for pid in packet_ids[:20]:
            pkt = store.get_packet(pid)
            if not pkt:
                continue

            payload = pkt.get("payload", b"")
            opcode_hex = ""

            if len(payload) >= 8:
                opcode_hex = f"0x{int.from_bytes(payload[6:8], 'big'):04X} "

            pkt_dir = pkt.get("direction") or "multicast"
            pkt_dir_icon = icon("tx") if pkt_dir == "request" else icon("rx") if pkt_dir == "response" else icon("packet")
            src = f"{pkt.get('src_ip', '?')}:{pkt.get('src_port', '?')}"
            dst = f"{pkt.get('dst_ip', '?')}:{pkt.get('dst_port', '?')}"
            print(f"{evidence_indent}{pkt_dir_icon}#{pid} {pkt_dir:8s} {opcode_hex}{src} -> {dst} {len(payload)}B")

            if use_dissect:
                from netaudio.dante.packet_dissector import dissect_and_render
                print(dissect_and_render(payload, indent=evidence_indent + "  "))
            else:
                print(_hexdump(payload, indent=evidence_indent + "  "))

        if len(packet_ids) > 20:
            print(f"{'':8s}  {'':26s}  {'':12s}  ... and {len(packet_ids) - 20} more")


@session_app.command("show")
def session_show(
    id: Optional[int] = typer.Option(None, "--id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to latest.",
    ),
    follow_mode: bool = typer.Option(False, "--follow", "-f", help="Tail the session timeline, polling for new markers."),
    poll_interval: float = typer.Option(1.0, "--poll", help="Poll interval in seconds for --follow mode."),
    marker_type: Optional[list[str]] = typer.Option(None, "--type", help="Filter by marker type (repeatable)."),
    after: Optional[str] = typer.Option(None, "--after", help="Show markers after this time (HH:MM:SS or ISO timestamp)."),
    before: Optional[str] = typer.Option(None, "--before", help="Show markers before this time (HH:MM:SS or ISO timestamp)."),
    grep: Optional[str] = typer.Option(None, "--grep", help="Filter markers matching string in label, summary, or note."),
    brief: bool = typer.Option(False, "--brief", help="One-line per marker: summary or label only."),
    no_notes: bool = typer.Option(False, "--no-notes", help="Hide full notes (show summary if available)."),
    packets: bool = typer.Option(False, "--packets", help="Show evidence packet dumps and per-marker packet counts."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Maximum number of markers to show."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio._common import output_single
    from netaudio.cli import OutputFormat, state as cli_state

    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, session_row = _resolve_session_reference(
            store,
            session_id=id,
            session=session,
            default_selector="active" if follow_mode else "latest",
        )
        after_ns = _parse_time_filter(after, store, resolved_session_id)
        before_ns = _parse_time_filter(before, store, resolved_session_id)

        normalized_types = None
        if marker_type:
            normalized_types = [_normalize_marker_type(t, strict=False) for t in marker_type]

        markers = store.get_markers(
            resolved_session_id,
            marker_types=normalized_types,
            after_ns=after_ns,
            before_ns=before_ns,
            grep=grep,
            limit=limit,
        )
        total_packets = store.get_session_packet_count(resolved_session_id)

        show_notes = not brief and not no_notes
        show_packets = not brief and packets

        if cli_state.output_format in (OutputFormat.json, OutputFormat.yaml):
            marker_list = []
            for m in markers:
                entry = {
                    "id": int(m["id"]),
                    "timestamp": m.get("timestamp_iso") or "",
                    "type": m.get("marker_type") or "",
                    "label": m.get("label") or "",
                    "summary": m.get("summary") or "",
                }
                if show_notes:
                    entry["note"] = m.get("note") or ""
                entry["data"] = m.get("data")
                marker_list.append(entry)
            output_single({
                "id": resolved_session_id,
                "name": session_row.get("name") or "",
                "category": session_row.get("category") or "",
                "source_host": session_row.get("source_host") or "",
                "started": session_row.get("started_iso") or "",
                "ended": session_row.get("ended_iso") or "",
                "packets": total_packets,
                "description": session_row.get("description") or "",
                "markers": marker_list,
            })
            return

        print(f"Session #{resolved_session_id}")
        print(f"  Name:        {session_row.get('name') or ''}")
        print(f"  Category:    {session_row.get('category') or ''}")
        print(f"  Source Host: {session_row.get('source_host') or ''}")
        print(f"  Started:     {session_row.get('started_iso') or ''}")
        print(f"  Ended:       {session_row.get('ended_iso') or ''}")
        print(f"  Packets:     {total_packets}")
        print(f"  Description: {session_row.get('description') or ''}")
        print(f"  Markers:     {len(markers)}")

        if markers or follow_mode:
            print("\nTimeline:")
            _print_timeline_header(show_window_packets=packets)
            next_times = [m["timestamp_ns"] for m in markers[1:]] + [None]
            for m, next_ts in zip(markers, next_times):
                _print_marker_row(
                    m, next_ts, resolved_session_id, store,
                    use_dissect=cli_state.dissect,
                    show_notes=show_notes,
                    show_packets=show_packets,
                    show_window_packets=packets,
                    brief=brief,
                )

        if follow_mode:
            _follow_session_timeline(
                store, resolved_session_id,
                last_marker_id=int(markers[-1]["id"]) if markers else 0,
                poll_interval=poll_interval,
                use_dissect=cli_state.dissect,
                show_notes=show_notes,
                show_packets=show_packets,
                show_window_packets=packets,
                brief=brief,
            )
    finally:
        store.close()


def _follow_session_timeline(
    store: PacketStore,
    session_id: int,
    last_marker_id: int,
    poll_interval: float,
    use_dissect: bool,
    show_notes: bool = True,
    show_packets: bool = True,
    show_window_packets: bool = True,
    brief: bool = False,
):
    import time

    seen_id = last_marker_id

    try:
        while True:
            time.sleep(poll_interval)

            session_row = store.get_session(session_id)
            markers = store.get_markers(session_id)
            new_markers = [m for m in markers if int(m["id"]) > seen_id]

            if not new_markers:
                if session_row and session_row.get("ended_ns"):
                    total = store.get_session_packet_count(session_id)
                    print(f"\nSession ended. {total} packets total.")
                    break
                continue

            for i, marker in enumerate(new_markers):
                if i + 1 < len(new_markers):
                    next_ts = int(new_markers[i + 1]["timestamp_ns"])
                else:
                    next_ts = None

                _print_marker_row(
                    marker, next_ts, session_id, store,
                    use_dissect=use_dissect,
                    show_notes=show_notes,
                    show_packets=show_packets,
                    show_window_packets=show_window_packets,
                    brief=brief,
                )
                seen_id = int(marker["id"])

    except KeyboardInterrupt:
        total = store.get_session_packet_count(session_id)
        print(f"\nStopped following. {total} packets so far.")


@session_app.command("packets")
def session_packets(
    id: Optional[int] = typer.Option(None, "--id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to latest.",
    ),
    device_ip: Optional[str] = typer.Option(
        None, "--device-ip", help="Filter packets where src or dst IP matches this device."
    ),
    from_label: Optional[str] = typer.Option(None, "--from-label", help="Start at first marker with this label."),
    to_label: Optional[str] = typer.Option(None, "--to-label", help="End at last marker with this label."),
    opcode: Optional[str] = typer.Option(None, "--opcode", help="Filter by opcode (hex like 0x2010 or decimal)."),
    protocol: Optional[str] = typer.Option(None, "--protocol", help="Filter by protocol ID (hex like 0x2729 or decimal)."),
    direction: Optional[str] = typer.Option(None, "--direction", help="Filter by direction: request, response, or multicast."),
    after: Optional[str] = typer.Option(None, "--after", help="Show packets after this time (HH:MM:SS or HH:MM:SS.fff)."),
    before: Optional[str] = typer.Option(None, "--before", help="Show packets before this time (HH:MM:SS or HH:MM:SS.fff)."),
    limit: int = typer.Option(200, "--limit", min=1, max=5000, help="Max packets to show."),
    offset: int = typer.Option(0, "--offset", min=0, help="Packet offset within filtered result."),
    descending: bool = typer.Option(False, "--descending", help="Show newest packets first."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.cli import state as cli_state

    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    resolved_opcode = _parse_int_option(opcode, "--opcode")
    resolved_protocol = _parse_int_option(protocol, "--protocol")

    store = PacketStore(db_path=resolved_db)
    try:
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

        time_start_ns = _parse_time_filter(after, store, resolved_session_id)
        time_end_ns = _parse_time_filter(before, store, resolved_session_id)

        if time_start_ns is not None:
            start_ns = max(start_ns, time_start_ns) if start_ns is not None else time_start_ns

        if time_end_ns is not None:
            end_ns = min(end_ns, time_end_ns) if end_ns is not None else time_end_ns

        resolved_direction = direction
        if resolved_direction == "multicast":
            resolved_direction = "__null__"

        total = store.get_session_packet_count_filtered(
            session_id=resolved_session_id,
            device_ip=device_ip,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=resolved_opcode,
            protocol_id=resolved_protocol,
            direction=resolved_direction,
        )
        rows = store.get_session_packets(
            session_id=resolved_session_id,
            device_ip=device_ip,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=resolved_opcode,
            protocol_id=resolved_protocol,
            direction=resolved_direction,
            limit=limit,
            offset=offset,
            ascending=not descending,
        )

        print(
            f"Capture: Session #{resolved_session_id} packets={total} shown={len(rows)} (limit={limit} offset={offset})"
        )
        filters = []
        if device_ip:
            filters.append(f"device={device_ip}")
        if resolved_opcode is not None:
            filters.append(f"opcode=0x{resolved_opcode:04X}")
        if resolved_protocol is not None:
            filters.append(f"protocol=0x{resolved_protocol:04X}")
        if direction:
            filters.append(f"direction={direction}")
        if after:
            filters.append(f"after={after}")
        if before:
            filters.append(f"before={before}")
        if filters:
            print(f"Capture: Filters: {', '.join(filters)}")
        if from_label or to_label:
            print(
                "Capture: Marker window from="
                f"{_normalize_marker_label(from_label) if from_label else '-'} "
                f"to={_normalize_marker_label(to_label) if to_label else '-'}"
            )
        _print_packet_table_header()

        for row in rows:
            payload = row.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            _print_packet_line(
                packet_id=int(row["id"]),
                timestamp_ns=int(row["timestamp_ns"]),
                source_ip=row.get("src_ip"),
                source_port=row.get("src_port"),
                destination_ip=row.get("dst_ip"),
                destination_port=row.get("dst_port"),
                direction=row.get("direction"),
                payload=payload,
                dump=dump,
                dissect_mode=cli_state.dissect,
            )
    finally:
        store.close()


@app.command("marker")
def marker(
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to active.",
    ),
    label: str = typer.Option(..., "--label", help="Marker label (normalized to lowercase snake_case)."),
    marker_type: str = typer.Option(
        "observation",
        "--type",
        help="Marker type: action|observation|state_change|system|hypothesis",
    ),
    summary: Optional[str] = typer.Option(None, "--summary", help="One-line summary (shown in brief mode)."),
    note: Optional[str] = typer.Option(None, "--note", help="Optional marker note (full detail)."),
    data: Optional[str] = typer.Option(None, "--data", help="Optional JSON object payload."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    source_host: Optional[str] = typer.Option(None, "--source-host", help="Host that generated this marker."),
    relay_stream: Optional[str] = typer.Option(
        None, "--relay-stream", help="Redis stream key to publish marker events."
    ),
    relay_only: bool = typer.Option(False, "--relay-only", help="Only publish to Redis stream (skip local DB write)."),
    redis_host: Optional[str] = typer.Option(None, "--redis-host", help="Redis host."),
    redis_port: Optional[int] = typer.Option(None, "--redis-port", help="Redis port."),
    redis_db: Optional[int] = typer.Option(None, "--redis-db", help="Redis DB."),
    redis_password: Optional[str] = typer.Option(None, "--redis-password", help="Redis password."),
    redis_socket: Optional[str] = typer.Option(None, "--redis-socket", help="Redis UNIX socket path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    payload = None
    if data:
        try:
            parsed = json.loads(data)
            if isinstance(parsed, dict):
                payload = parsed
            else:
                payload = {"value": parsed}
        except Exception as exception:
            print(f"Capture: invalid --data JSON: {exception}", file=sys.stderr)
            raise typer.Exit(1)

    profile_cfg, _ = _load_capture_profile(config, profile)
    capture_cfg = _as_dict(profile_cfg.get("capture"))
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    (
        resolved_redis_host,
        resolved_redis_port,
        resolved_redis_db,
        resolved_redis_password,
        resolved_redis_socket,
    ) = _resolve_redis_from_config(
        profile_cfg=profile_cfg,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_socket=redis_socket,
    )
    normalized_type = _normalize_marker_type(marker_type, strict=True)
    normalized_label = _normalize_marker_label(label)
    resolved_session_id: int
    if relay_only and session_id is not None and not session:
        resolved_session_id = int(session_id)
    else:
        store = PacketStore(db_path=resolved_db)
        try:
            resolved_session_id, _ = _resolve_session_reference(
                store,
                session_id=session_id,
                session=session,
                default_selector="active",
            )
        finally:
            store.close()

    resolved_host = source_host or socket.gethostname()
    marker_ts = time.time_ns()
    resolved_relay_stream = _coalesce(relay_stream, capture_cfg.get("ingress_stream"))
    if relay_only and not resolved_relay_stream:
        print("Capture: --relay-only requires --relay-stream (or capture.ingress_stream in config).", file=sys.stderr)
        raise typer.Exit(1)

    if not relay_only:
        store = PacketStore(db_path=resolved_db)
        try:
            marker_id = store.add_marker(
                session_id=resolved_session_id,
                marker_type=normalized_type,
                label=normalized_label,
                summary=summary,
                note=note,
                source_host=resolved_host,
                data=payload,
                timestamp_ns=marker_ts,
            )
            print(f"Capture: Added marker #{marker_id} to session #{resolved_session_id}")
        finally:
            store.close()

    if resolved_relay_stream:
        client = _resolve_redis_for_capture(
            redis_host=resolved_redis_host,
            redis_port=resolved_redis_port,
            redis_db=resolved_redis_db,
            redis_password=resolved_redis_password,
            redis_socket=resolved_redis_socket,
        )
        event = {
            "event": "marker",
            "source_host": resolved_host,
            "timestamp_ns": str(marker_ts),
            "session_id": str(resolved_session_id),
            "marker_type": normalized_type,
            "label": normalized_label,
            "summary": str(summary or ""),
            "note": str(note or ""),
            "data_json": json.dumps(payload, sort_keys=True) if payload else "",
            "already_stored": "0" if relay_only else "1",
        }
        try:
            client.xadd(str(resolved_relay_stream), event, maxlen=200000, approximate=True)
            print(f"Capture: Published marker to stream {resolved_relay_stream}")
        except Exception as exception:
            print(f"Capture: failed to publish marker: {exception}", file=sys.stderr)
            raise typer.Exit(1)


@app.command("collect")
def collect(
    stream: Optional[str] = typer.Option(None, "--stream", help="Redis stream key to consume."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    start_id: str = typer.Option("0-0", "--start-id", help="Redis stream ID cursor."),
    block_ms: int = typer.Option(5000, "--block-ms", help="Redis XREAD block timeout in milliseconds."),
    count: int = typer.Option(100, "--count", help="Max stream entries per read."),
    once: bool = typer.Option(False, "--once", help="Read once and exit."),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Assign imported packets to this session."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session routing: active (default), none, event, or fixed session reference (id/name/latest).",
    ),
    publish_stream: Optional[str] = typer.Option(
        None, "--publish-stream", help="Redis stream key for deduped unified events."
    ),
    publish_maxlen: int = typer.Option(
        200000, "--publish-maxlen", help="Approximate max length for published unified stream."
    ),
    dedupe: Optional[bool] = typer.Option(
        None, "--dedupe/--no-dedupe", help="Enable packet deduplication across source hosts."
    ),
    dedupe_window_ms: Optional[int] = typer.Option(
        None, "--dedupe-window-ms", help="Deduplication time window in milliseconds."
    ),
    live: bool = typer.Option(False, "--live", help="Print packet lines as packets are imported."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII in live mode."),
    redis_host: Optional[str] = typer.Option(None, "--redis-host", help="Redis host."),
    redis_port: Optional[int] = typer.Option(None, "--redis-port", help="Redis port."),
    redis_db: Optional[int] = typer.Option(None, "--redis-db", help="Redis DB."),
    redis_password: Optional[str] = typer.Option(None, "--redis-password", help="Redis password."),
    redis_socket: Optional[str] = typer.Option(None, "--redis-socket", help="Redis UNIX socket path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    if session_id is not None and session is not None:
        raise typer.Exit("Use either --session-id or --session, not both.")
    profile_cfg, _ = _load_capture_profile(config, profile)
    capture_cfg = _as_dict(profile_cfg.get("capture"))
    resolved_stream = _coalesce(stream, capture_cfg.get("ingress_stream"))
    if not resolved_stream:
        raise typer.Exit("--stream is required (or set capture.ingress_stream in config).")
    resolved_publish_stream = _coalesce(publish_stream, capture_cfg.get("unified_stream"))
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    (
        resolved_redis_host,
        resolved_redis_port,
        resolved_redis_db,
        resolved_redis_password,
        resolved_redis_socket,
    ) = _resolve_redis_from_config(
        profile_cfg=profile_cfg,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_socket=redis_socket,
    )
    resolved_dedupe = dedupe
    if resolved_dedupe is None:
        resolved_dedupe = _parse_config_bool(capture_cfg.get("dedupe"), "capture.dedupe")
    if resolved_dedupe is None:
        resolved_dedupe = True
    resolved_dedupe_window_ms = dedupe_window_ms
    if resolved_dedupe_window_ms is None:
        resolved_dedupe_window_ms = _parse_config_int(capture_cfg.get("dedupe_window_ms"), "capture.dedupe_window_ms")
    if resolved_dedupe_window_ms is None:
        resolved_dedupe_window_ms = 15

    if resolved_publish_stream and resolved_publish_stream == resolved_stream:
        raise typer.Exit("--publish-stream must be different from --stream to avoid event loops.")

    client = _resolve_redis_for_capture(
        redis_host=resolved_redis_host,
        redis_port=resolved_redis_port,
        redis_db=resolved_redis_db,
        redis_password=resolved_redis_password,
        redis_socket=resolved_redis_socket,
    )
    store = PacketStore(db_path=resolved_db)
    session_selector = str(_coalesce(session, capture_cfg.get("session"), "active")).strip()
    if not session_selector:
        session_selector = "active"

    fixed_session_id: int | None = None
    use_active_session = False
    use_event_session = False
    if session_id is not None:
        fixed_session_id = int(session_id)
    else:
        selector = session_selector.lower()
        if selector in {"active", "current"}:
            use_active_session = True
        elif selector in {"none", "off", "disabled"}:
            pass
        elif selector in {"event", "source"}:
            use_event_session = True
        else:
            fixed_session_id, _ = _resolve_session_reference(
                store,
                session_id=None,
                session=session_selector,
                default_selector="latest",
            )

    def _resolve_target_session_id(inferred_session_id: int | None) -> int | None:
        if fixed_session_id is not None:
            return fixed_session_id
        if use_active_session:
            active_session = store.get_latest_session(active_only=True)
            if active_session:
                return int(active_session["id"])
            return None
        if use_event_session:
            return inferred_session_id
        return None

    cursor = start_id
    total_packets = 0
    total_markers = 0
    deduped_packets = 0
    published_packets = 0
    published_markers = 0
    dedupe_window_ns = max(resolved_dedupe_window_ms, 0) * 1_000_000
    dedupe_horizon_ns = max(dedupe_window_ns * 200, 2_000_000_000) if dedupe_window_ns > 0 else 0
    recent_fingerprints: dict[str, tuple[int, str | None]] = {}
    recent_order: deque[tuple[int, str, str | None]] = deque()
    collector_host = socket.gethostname()
    print(f"Capture: Collecting from Redis stream {resolved_stream} into {resolved_db}")
    if fixed_session_id is not None:
        print(f"Capture: Session routing fixed to #{fixed_session_id}")
    elif use_active_session:
        print("Capture: Session routing mode active (assign to current active session)")
    elif use_event_session:
        print("Capture: Session routing mode event (use source event session_id)")
    else:
        print("Capture: Session routing mode none (do not assign sessions)")
    if resolved_publish_stream:
        print(f"Capture: Publishing unified events to Redis stream {resolved_publish_stream}")
    if live:
        _print_packet_table_header()
    try:
        while True:
            response = client.xread({str(resolved_stream): cursor}, count=count, block=block_ms)
            if not response:
                if once:
                    break
                continue
            for _, entries in response:
                for entry_id, fields in entries:
                    cursor = entry_id
                    event_type = (fields.get("event") or "packet").strip().lower()

                    timestamp_ns = None
                    try:
                        ts = fields.get("timestamp_ns")
                        if ts:
                            timestamp_ns = int(ts)
                    except Exception:
                        timestamp_ns = None

                    inferred_session_id = None
                    try:
                        raw_session = fields.get("session_id")
                        if raw_session not in (None, ""):
                            inferred_session_id = int(raw_session)
                    except Exception:
                        inferred_session_id = None

                    target_session_id = _resolve_target_session_id(inferred_session_id)

                    if event_type == "marker":
                        if target_session_id is None:
                            continue

                        if fields.get("already_stored") == "1":
                            continue

                        normalized_marker_type = _normalize_marker_type(
                            str(fields.get("marker_type") or "observation"),
                            strict=False,
                        )
                        normalized_label = _normalize_marker_label(str(fields.get("label") or "marker"))
                        marker_data = None
                        data_json = fields.get("data_json")
                        if data_json:
                            try:
                                parsed = json.loads(data_json)
                                if isinstance(parsed, dict):
                                    marker_data = parsed
                                else:
                                    marker_data = {"value": parsed}
                            except Exception:
                                marker_data = {"raw": data_json}

                        marker_id = store.add_marker(
                            session_id=target_session_id,
                            marker_type=normalized_marker_type,
                            label=normalized_label,
                            summary=fields.get("summary") or None,
                            note=fields.get("note") or None,
                            source_host=fields.get("source_host") or None,
                            data=marker_data,
                            timestamp_ns=timestamp_ns,
                        )
                        total_markers += 1
                        print(f"  imported marker #{marker_id} from stream id {entry_id}")
                        if resolved_publish_stream:
                            event = {
                                "event": "marker",
                                "collector_emitted": "1",
                                "collector_host": collector_host,
                                "source_stream_id": entry_id,
                                "source_host": fields.get("source_host") or "",
                                "timestamp_ns": str(timestamp_ns or time.time_ns()),
                                "session_id": str(target_session_id),
                                "marker_type": normalized_marker_type,
                                "label": normalized_label,
                                "note": str(fields.get("note") or ""),
                                "data_json": fields.get("data_json") or "",
                            }
                            try:
                                client.xadd(
                                    str(resolved_publish_stream), event, maxlen=publish_maxlen, approximate=True
                                )
                                published_markers += 1
                            except Exception as exception:
                                print(f"Capture: unified marker publish failed: {exception}", file=sys.stderr)
                        continue

                    payload_hex = fields.get("payload_hex") or ""
                    if not payload_hex:
                        continue
                    try:
                        payload = bytes.fromhex(payload_hex)
                    except Exception:
                        continue

                    src_port = _parse_optional_int(fields.get("src_port"))
                    dst_port = _parse_optional_int(fields.get("dst_port"))
                    src_ip = fields.get("src_ip") or None
                    dst_ip = fields.get("dst_ip") or None
                    direction = fields.get("direction") or None
                    source_host = fields.get("source_host") or None
                    effective_timestamp_ns = timestamp_ns or time.time_ns()

                    if resolved_dedupe and dedupe_window_ns > 0:
                        prune_before = effective_timestamp_ns - dedupe_horizon_ns
                        while recent_order and recent_order[0][0] < prune_before:
                            stale_ts, stale_key, stale_host = recent_order.popleft()
                            if recent_fingerprints.get(stale_key) == (stale_ts, stale_host):
                                del recent_fingerprints[stale_key]

                        fingerprint = _packet_fingerprint(
                            payload=payload,
                            src_ip=src_ip,
                            src_port=src_port,
                            dst_ip=dst_ip,
                            dst_port=dst_port,
                            direction=direction,
                        )
                        last_seen = recent_fingerprints.get(fingerprint)
                        if last_seen is not None:
                            last_seen_ns, last_seen_host = last_seen
                            host_changed = source_host != last_seen_host
                            if host_changed and abs(effective_timestamp_ns - last_seen_ns) <= dedupe_window_ns:
                                deduped_packets += 1
                                continue
                        recent_fingerprints[fingerprint] = (effective_timestamp_ns, source_host)
                        recent_order.append((effective_timestamp_ns, fingerprint, source_host))

                    packet_id = store.store_packet(
                        payload=payload,
                        source_type=fields.get("source_type") or "redis_capture",
                        src_ip=src_ip,
                        src_port=src_port,
                        dst_ip=dst_ip,
                        dst_port=dst_port,
                        device_ip=fields.get("device_ip") or None,
                        direction=direction,
                        session_id=target_session_id,
                        timestamp_ns=effective_timestamp_ns,
                        source_host=source_host,
                    )
                    if packet_id:
                        total_packets += 1
                        if live:
                            _print_packet_line(
                                packet_id=packet_id,
                                timestamp_ns=effective_timestamp_ns,
                                source_ip=src_ip or "?",
                                source_port=src_port or 0,
                                destination_ip=dst_ip or "?",
                                destination_port=dst_port or 0,
                                direction=direction,
                                payload=payload,
                                dump=dump,
                            )
                        else:
                            print(f"  imported packet #{packet_id} from stream id {entry_id}")
                        if resolved_publish_stream:
                            event = {
                                "event": "packet",
                                "collector_emitted": "1",
                                "collector_host": collector_host,
                                "source_stream_id": entry_id,
                                "source_host": fields.get("source_host") or "",
                                "packet_id": str(fields.get("packet_id") or ""),
                                "unified_packet_id": str(packet_id),
                                "timestamp_ns": str(effective_timestamp_ns),
                                "src_ip": str(src_ip or ""),
                                "src_port": str(src_port or ""),
                                "dst_ip": str(dst_ip or ""),
                                "dst_port": str(dst_port or ""),
                                "direction": str(direction or ""),
                                "device_ip": str(fields.get("device_ip") or ""),
                                "source_type": str(fields.get("source_type") or "redis_capture"),
                                "session_id": str(target_session_id or ""),
                                "payload_len": str(len(payload)),
                                "payload_hex": payload_hex,
                            }
                            try:
                                client.xadd(
                                    str(resolved_publish_stream), event, maxlen=publish_maxlen, approximate=True
                                )
                                published_packets += 1
                            except Exception as exception:
                                print(f"Capture: unified packet publish failed: {exception}", file=sys.stderr)
            if once:
                break
    finally:
        store.close()
    print(f"Capture: Imported {total_packets} packet(s), {total_markers} marker(s), deduped={deduped_packets}")
    if resolved_publish_stream:
        print(
            "Capture: Published unified "
            f"{published_packets} packet(s), {published_markers} marker(s) "
            f"to {resolved_publish_stream}"
        )


@app.command("follow")
def follow(
    stream: Optional[str] = typer.Option(None, "--stream", help="Redis stream key to consume."),
    start_id: str = typer.Option("$", "--start-id", help="Redis stream ID cursor."),
    block_ms: int = typer.Option(5000, "--block-ms", help="Redis XREAD block timeout in milliseconds."),
    count: int = typer.Option(100, "--count", help="Max stream entries per read."),
    once: bool = typer.Option(False, "--once", help="Read once and exit."),
    markers: bool = typer.Option(True, "--markers/--no-markers", help="Show marker events."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    redis_host: Optional[str] = typer.Option(None, "--redis-host", help="Redis host."),
    redis_port: Optional[int] = typer.Option(None, "--redis-port", help="Redis port."),
    redis_db: Optional[int] = typer.Option(None, "--redis-db", help="Redis DB."),
    redis_password: Optional[str] = typer.Option(None, "--redis-password", help="Redis password."),
    redis_socket: Optional[str] = typer.Option(None, "--redis-socket", help="Redis UNIX socket path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    profile_cfg, _ = _load_capture_profile(config, profile)
    capture_cfg = _as_dict(profile_cfg.get("capture"))
    resolved_stream = _coalesce(stream, capture_cfg.get("unified_stream"), capture_cfg.get("ingress_stream"))
    if not resolved_stream:
        raise typer.Exit("--stream is required (or set capture.unified_stream in config).")
    (
        resolved_redis_host,
        resolved_redis_port,
        resolved_redis_db,
        resolved_redis_password,
        resolved_redis_socket,
    ) = _resolve_redis_from_config(
        profile_cfg=profile_cfg,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_socket=redis_socket,
    )
    client = _resolve_redis_for_capture(
        redis_host=resolved_redis_host,
        redis_port=resolved_redis_port,
        redis_db=resolved_redis_db,
        redis_password=resolved_redis_password,
        redis_socket=resolved_redis_socket,
    )
    cursor = start_id
    total_packets = 0
    total_markers = 0
    print(f"Capture: Following Redis stream {resolved_stream} from {start_id}")
    _print_packet_table_header()

    while True:
        response = client.xread({str(resolved_stream): cursor}, count=count, block=block_ms)
        if not response:
            if once:
                break
            continue

        for _, entries in response:
            for entry_id, fields in entries:
                cursor = entry_id
                event_type = (fields.get("event") or "packet").strip().lower()

                timestamp_ns = _parse_optional_int(fields.get("timestamp_ns")) or time.time_ns()

                if event_type == "marker":
                    if not markers:
                        continue
                    total_markers += 1
                    marker_time = datetime.datetime.fromtimestamp(timestamp_ns / 1e9).strftime("%H:%M:%S.%f")[:-3]
                    marker_type = _normalize_marker_type(str(fields.get("marker_type") or "observation"), strict=False)
                    label = _normalize_marker_label(str(fields.get("label") or "marker"))
                    source_host = fields.get("source_host") or "?"
                    session_token = fields.get("session_id") or "-"
                    note = fields.get("note") or ""
                    print(
                        f"  marker  {marker_time}  session={session_token}  type={marker_type}  "
                        f"label={label}  host={source_host}"
                    )
                    if note:
                        print(f"          note: {note}")
                    continue

                payload_hex = fields.get("payload_hex") or ""
                if not payload_hex:
                    continue
                try:
                    payload = bytes.fromhex(payload_hex)
                except Exception:
                    continue

                src_ip = fields.get("src_ip") or "?"
                dst_ip = fields.get("dst_ip") or "?"
                src_port = _parse_optional_int(fields.get("src_port")) or 0
                dst_port = _parse_optional_int(fields.get("dst_port")) or 0
                direction = fields.get("direction") or None
                packet_id = _parse_optional_int(fields.get("unified_packet_id"))
                if packet_id is None:
                    packet_id = _parse_optional_int(fields.get("packet_id"))
                if packet_id is None:
                    packet_id = total_packets + 1

                _print_packet_line(
                    packet_id=packet_id,
                    timestamp_ns=timestamp_ns,
                    source_ip=src_ip,
                    source_port=src_port,
                    destination_ip=dst_ip,
                    destination_port=dst_port,
                    direction=direction,
                    payload=payload,
                    dump=dump,
                )
                total_packets += 1

        if once:
            break

    print(f"Capture: Followed {total_packets} packet(s), {total_markers} marker(s).")


@packet_app.command("list")
def packet_list(
    session: Optional[str] = typer.Option(
        None, "--session",
        help="Session reference (ID, name, latest, active). Omit to search all packets.",
    ),
    device_ip: Optional[str] = typer.Option(
        None, "--device-ip", help="Filter by device IP (src or dst)."
    ),
    source_ip: Optional[str] = typer.Option(
        None, "--src", help="Filter by source IP."
    ),
    destination_ip: Optional[str] = typer.Option(
        None, "--dst", help="Filter by destination IP."
    ),
    port: Optional[int] = typer.Option(
        None, "--port", help="Filter by port (src or dst)."
    ),
    device_name: Optional[str] = typer.Option(
        None, "--device", help="Filter by device name."
    ),
    opcode: Optional[str] = typer.Option(
        None, "--opcode", help="Filter by opcode (hex like 0x2000 or decimal)."
    ),
    protocol: Optional[str] = typer.Option(
        None, "--protocol", help="Filter by protocol ID (hex like 0x27FF or decimal)."
    ),
    direction: Optional[str] = typer.Option(
        None, "--direction", help="Filter by direction: request, response, or multicast."
    ),
    after: Optional[str] = typer.Option(
        None, "--after", help="Show packets after this time (HH:MM:SS or ISO timestamp)."
    ),
    before: Optional[str] = typer.Option(
        None, "--before", help="Show packets before this time (HH:MM:SS or ISO timestamp)."
    ),
    grep: Optional[str] = typer.Option(
        None, "--grep", help="Filter packets containing this string in their payload."
    ),
    tail: Optional[int] = typer.Option(
        None, "--tail", help="Show the N most recent packets (shorthand for --descending --limit N)."
    ),
    limit: int = typer.Option(200, "--limit", min=1, max=10000, help="Max packets to show."),
    offset: int = typer.Option(0, "--offset", min=0, help="Skip first N results."),
    descending: bool = typer.Option(False, "--descending", help="Show newest packets first."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Search and filter captured packets."""
    from netaudio.cli import state as cli_state

    if tail is not None:
        descending = True
        limit = tail

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    resolved_opcode = _parse_int_option(opcode, "--opcode")
    resolved_protocol = _parse_int_option(protocol, "--protocol")

    resolved_direction = direction
    if resolved_direction == "multicast":
        resolved_direction = "__null__"

    store = PacketStore(db_path=resolved_db)
    try:
        session_id = None
        session_row = None
        if session is not None:
            session_id, session_row = _resolve_session_reference(
                store, session_id=None, session=session, default_selector="latest",
            )

        start_ns = None
        end_ns = None
        if session_row:
            start_ns = session_row.get("started_ns")
            ended_ns = session_row.get("ended_ns")
            if ended_ns:
                end_ns = ended_ns
        if after and session_id is not None:
            start_ns = _parse_time_filter(after, store, session_id)
        if before and session_id is not None:
            end_ns = _parse_time_filter(before, store, session_id)

        total = store.search_packets_count(
            session_id=None,
            device_ip=device_ip,
            device_name=device_name,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=resolved_opcode,
            protocol_id=resolved_protocol,
            direction=resolved_direction,
            payload_contains=grep,
            src_ip=source_ip,
            dst_ip=destination_ip,
            port=port,
        )
        rows = store.search_packets(
            session_id=None,
            device_ip=device_ip,
            device_name=device_name,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=resolved_opcode,
            protocol_id=resolved_protocol,
            direction=resolved_direction,
            payload_contains=grep,
            src_ip=source_ip,
            dst_ip=destination_ip,
            port=port,
            limit=limit,
            offset=offset,
            ascending=not descending,
        )

        scope = f"session #{session_id}" if session_id else "all packets"
        print(f"Capture: {scope} {_emdash()} {total} matched, showing {len(rows)} (limit={limit} offset={offset})")

        filters = []
        if device_ip:
            filters.append(f"device_ip={device_ip}")
        if source_ip:
            filters.append(f"src={source_ip}")
        if destination_ip:
            filters.append(f"dst={destination_ip}")
        if port is not None:
            filters.append(f"port={port}")
        if device_name:
            filters.append(f"device={device_name}")
        if resolved_opcode is not None:
            filters.append(f"opcode=0x{resolved_opcode:04X}")
        if resolved_protocol is not None:
            filters.append(f"protocol=0x{resolved_protocol:04X}")
        if direction:
            filters.append(f"direction={direction}")
        if grep:
            filters.append(f"grep={grep}")
        if after:
            filters.append(f"after={after}")
        if before:
            filters.append(f"before={before}")
        if filters:
            print(f"Capture: Filters: {', '.join(filters)}")

        _print_packet_table_header()

        for row in rows:
            payload = row.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            _print_packet_line(
                packet_id=int(row["id"]),
                timestamp_ns=int(row["timestamp_ns"]),
                source_ip=row.get("src_ip"),
                source_port=row.get("src_port"),
                destination_ip=row.get("dst_ip"),
                destination_port=row.get("dst_port"),
                direction=row.get("direction"),
                payload=payload,
                dump=dump,
                dissect_mode=cli_state.dissect,
            )
    finally:
        store.close()


@packet_app.command("show")
def packet_show(
    packet_id: list[int] = typer.Argument(..., help="Packet ID(s) to display."),
    raw: bool = typer.Option(False, "--raw", help="Plain hex dump instead of annotated dissection."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    store = PacketStore(db_path=resolved_db)
    try:
        for pid in packet_id:
            pkt = store.get_packet(pid)
            if not pkt:
                print(f"Packet #{pid}: not found", file=sys.stderr)
                continue

            payload = pkt.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)

            direction = pkt.get("direction") or "multicast"
            src_ip = pkt.get("src_ip") or "?"
            src_port = pkt.get("src_port") or "?"
            dst_ip = pkt.get("dst_ip") or "?"
            dst_port = pkt.get("dst_port") or "?"
            device_ip = pkt.get("device_ip") or "?"
            session_id_val = pkt.get("session_id") or "?"
            source_type = pkt.get("source_type") or "?"
            timestamp_ns = int(pkt.get("timestamp_ns") or 0)

            timestamp = datetime.datetime.fromtimestamp(timestamp_ns / 1e9)
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            info_str = _label_packet(payload)

            packet_interface = pkt.get("interface") or ""

            print(f"Packet #{pid}")
            print(f"  Time:      {timestamp_str}")
            print(f"  Source:    {src_ip}:{src_port}")
            print(f"  Dest:      {dst_ip}:{dst_port}")
            print(f"  Direction: {direction}")
            print(f"  Device:    {device_ip}")
            if packet_interface:
                print(f"  Interface: {packet_interface}")
            print(f"  Session:   {session_id_val}")
            print(f"  Type:      {source_type}")
            print(f"  Size:      {len(payload)}B")
            if info_str:
                print(f"  Label:     {info_str}")

            if raw:
                if len(payload) >= 2:
                    protocol_id = struct.unpack(">H", payload[0:2])[0]
                    print(f"  Protocol:  0x{protocol_id:04X}")

                if len(payload) >= 8:
                    opcode = struct.unpack(">H", payload[6:8])[0]
                    print(f"  Opcode:    0x{opcode:04X}")

                if len(payload) >= 10:
                    status = struct.unpack(">H", payload[8:10])[0]
                    print(f"  Status:    0x{status:04X}")

                print(f"  Payload:")
                print(_hexdump(payload, indent="    "))
            else:
                from netaudio.dante.packet_dissector import dissect_and_render
                print(dissect_and_render(payload, indent="  "))

            print()
    finally:
        store.close()


@packet_app.command("diff")
def packet_diff(
    packet_ids: list[int] = typer.Argument(..., help="Two or more packet IDs to compare."),
    full: bool = typer.Option(False, "--full", help="Show full hex dump with diffs highlighted, not just changed bytes."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Byte-level diff of two or more packets."""
    if len(packet_ids) < 2:
        print("Need at least 2 packet IDs to diff.", file=sys.stderr)
        raise typer.Exit(1)

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    store = PacketStore(db_path=resolved_db)
    try:
        packets = []
        for pid in packet_ids:
            pkt = store.get_packet(pid)
            if not pkt:
                print(f"Packet #{pid}: not found", file=sys.stderr)
                raise typer.Exit(1)
            payload = pkt.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            packets.append((pid, pkt, payload))

        for pid, pkt, payload in packets:
            timestamp_ns = int(pkt.get("timestamp_ns") or 0)
            timestamp = datetime.datetime.fromtimestamp(timestamp_ns / 1e9)
            timestamp_str = timestamp.strftime("%H:%M:%S.%f")[:-3]
            src = _format_endpoint(pkt.get("src_ip"), pkt.get("src_port"))
            dst = _format_endpoint(pkt.get("dst_ip"), pkt.get("dst_port"))
            direction = pkt.get("direction") or "multicast"
            label = _label_packet(payload)
            print(f"  #{pid}  {timestamp_str}  {src} -> {dst}  {direction}  {len(payload)}B  {label}")

        max_length = max(len(payload) for _, _, payload in packets)
        reference_payload = packets[0][2]

        differing_offsets = set()
        for _, _, payload in packets[1:]:
            for offset in range(max_length):
                reference_byte = reference_payload[offset] if offset < len(reference_payload) else None
                compare_byte = payload[offset] if offset < len(payload) else None
                if reference_byte != compare_byte:
                    differing_offsets.add(offset)

        if not differing_offsets:
            print("\n  Payloads are identical.")
            return

        print(f"\n  {len(differing_offsets)} bytes differ (of {max_length} total)")

        if full:
            _print_diff_full(packets, differing_offsets, max_length)
        else:
            _print_diff_compact(packets, differing_offsets, max_length)

    finally:
        store.close()


def _print_diff_compact(packets, differing_offsets, max_length):
    header_parts = ["  offset"]
    for pid, _, _ in packets:
        header_parts.append(f"  #{pid:<8d}")
    header_parts.append("  ascii")
    print("".join(header_parts))
    print("  " + _hrule(8 + 12 * len(packets) + 8))

    for offset in sorted(differing_offsets):
        parts = [f"  {offset:04x}   "]
        ascii_parts = []
        for _, _, payload in packets:
            if offset < len(payload):
                byte_val = payload[offset]
                parts.append(f"  0x{byte_val:02x}      ")
                ascii_parts.append(chr(byte_val) if 32 <= byte_val < 127 else ".")
            else:
                parts.append("  --        ")
                ascii_parts.append(" ")
        parts.append("  " + " ".join(ascii_parts))
        print("".join(parts))


def _print_diff_full(packets, differing_offsets, max_length):
    reference_payload = packets[0][2]
    reference_pid = packets[0][0]

    for compare_pid, _, compare_payload in packets[1:]:
        print(f"\n  #{reference_pid} vs #{compare_pid}")
        print("  " + _hrule(80))

        for row_offset in range(0, max_length, 16):
            ref_chunk = reference_payload[row_offset:row_offset + 16] if row_offset < len(reference_payload) else b""
            cmp_chunk = compare_payload[row_offset:row_offset + 16] if row_offset < len(compare_payload) else b""

            row_has_diff = any(
                offset in differing_offsets
                for offset in range(row_offset, min(row_offset + 16, max_length))
            )
            if not row_has_diff:
                continue

            ref_hex_parts = []
            cmp_hex_parts = []
            for byte_index in range(16):
                absolute_offset = row_offset + byte_index
                ref_byte = ref_chunk[byte_index] if byte_index < len(ref_chunk) else None
                cmp_byte = cmp_chunk[byte_index] if byte_index < len(cmp_chunk) else None
                is_diff = absolute_offset in differing_offsets

                if ref_byte is not None:
                    ref_str = f"{ref_byte:02x}"
                else:
                    ref_str = "--"
                if cmp_byte is not None:
                    cmp_str = f"{cmp_byte:02x}"
                else:
                    cmp_str = "--"

                if is_diff:
                    ref_hex_parts.append(ansi("91", ref_str))
                    cmp_hex_parts.append(ansi("92", cmp_str))
                else:
                    ref_hex_parts.append(ref_str)
                    cmp_hex_parts.append(cmp_str)

            ref_ascii = "".join(
                chr(b) if 32 <= b < 127 else "." for b in ref_chunk
            ).ljust(16)
            cmp_ascii = "".join(
                chr(b) if 32 <= b < 127 else "." for b in cmp_chunk
            ).ljust(16)

            ref_hex = " ".join(ref_hex_parts[:8]) + "  " + " ".join(ref_hex_parts[8:])
            cmp_hex = " ".join(cmp_hex_parts[:8]) + "  " + " ".join(cmp_hex_parts[8:])

            print(f"  {row_offset:04x}  {ref_hex}  |{ref_ascii}|")
            print(f"  {row_offset:04x}  {cmp_hex}  |{cmp_ascii}|")
            print()


ARC_VOLATILE_OFFSETS = {4, 5}
CONMON_VOLATILE_OFFSETS = {4, 5}


def _classify_protocol(payload: bytes) -> str:
    if len(payload) < 2:
        return "unknown"
    protocol_id = struct.unpack(">H", payload[0:2])[0]
    if protocol_id in (0x27FF, 0x2809, 0x2729):
        return "arc"
    if protocol_id == 0xFFFF:
        return "conmon"
    return "unknown"


def _get_volatile_offsets(protocol_type: str) -> set[int]:
    if protocol_type == "arc":
        return ARC_VOLATILE_OFFSETS
    if protocol_type == "conmon":
        return CONMON_VOLATILE_OFFSETS
    return set()


def _detect_jitter_offsets(payloads: list[bytes]) -> set[int]:
    if len(payloads) < 2:
        return set()

    jitter_offsets = set()
    max_length = max(len(payload) for payload in payloads)
    for offset in range(max_length):
        values = set()
        for payload in payloads:
            if offset < len(payload):
                values.add(payload[offset])
            else:
                values.add(None)
        if len(values) > 1:
            jitter_offsets.add(offset)
    return jitter_offsets


def _opcode_key(payload: bytes) -> str | None:
    if len(payload) < 8:
        return None
    protocol_id = struct.unpack(">H", payload[0:2])[0]
    if protocol_id in (0x27FF, 0x2809, 0x2729):
        opcode = struct.unpack(">H", payload[6:8])[0]
        return f"arc:0x{opcode:04X}"
    if protocol_id == 0xFFFF and len(payload) >= 28:
        message_type = struct.unpack(">H", payload[26:28])[0]
        return f"conmon:0x{message_type:04X}"
    return f"proto:0x{protocol_id:04X}"


def _parse_time_to_ns(value: str) -> int | None:
    parts = value.split(":")
    if len(parts) < 2:
        return None

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

        today = datetime.date.today()
        target = datetime.datetime(
            today.year, today.month, today.day,
            hours, minutes, seconds, microseconds,
        )
        return int(target.timestamp() * 1e9)
    except (ValueError, IndexError):
        return None


def _state_diff_print_opcode_diff(
    opcode_label: str,
    before_payloads: list[tuple[int, bytes]],
    after_payloads: list[tuple[int, bytes]],
    volatile_offsets: set[int],
    jitter_offsets: set[int],
    full: bool,
):
    ignored_offsets = volatile_offsets | jitter_offsets

    before_representative_id, before_representative = before_payloads[-1]
    after_representative_id, after_representative = after_payloads[-1]

    max_length = max(len(before_representative), len(after_representative))
    stable_diff_offsets = set()

    for offset in range(max_length):
        if offset in ignored_offsets:
            continue
        before_byte = before_representative[offset] if offset < len(before_representative) else None
        after_byte = after_representative[offset] if offset < len(after_representative) else None
        if before_byte != after_byte:
            stable_diff_offsets.add(offset)

    if not stable_diff_offsets:
        return False

    fact_labels = _load_fact_labels()
    human_name = fact_labels.get(opcode_label, opcode_label)

    print(f"\n  {ansi('1', opcode_label)}  {human_name}")
    print(f"  before: #{before_representative_id} ({len(before_representative)}B)    after: #{after_representative_id} ({len(after_representative)}B)")
    if jitter_offsets:
        print(f"  {ansi('90', f'({len(jitter_offsets)} jitter bytes excluded, {len(volatile_offsets)} volatile header bytes excluded)')}")
    elif volatile_offsets:
        print(f"  {ansi('90', f'({len(volatile_offsets)} volatile header bytes excluded)')}")
    print(f"  {len(stable_diff_offsets)} stable bytes differ")

    if full:
        for row_offset in range(0, max_length, 16):
            before_chunk = before_representative[row_offset:row_offset + 16] if row_offset < len(before_representative) else b""
            after_chunk = after_representative[row_offset:row_offset + 16] if row_offset < len(after_representative) else b""

            row_has_diff = any(
                offset in stable_diff_offsets
                for offset in range(row_offset, min(row_offset + 16, max_length))
            )
            if not row_has_diff:
                continue

            before_hex_parts = []
            after_hex_parts = []
            for byte_index in range(16):
                absolute_offset = row_offset + byte_index
                before_byte = before_chunk[byte_index] if byte_index < len(before_chunk) else None
                after_byte = after_chunk[byte_index] if byte_index < len(after_chunk) else None
                is_diff = absolute_offset in stable_diff_offsets
                is_jitter = absolute_offset in ignored_offsets

                before_str = f"{before_byte:02x}" if before_byte is not None else "--"
                after_str = f"{after_byte:02x}" if after_byte is not None else "--"

                if is_jitter:
                    before_hex_parts.append(ansi("90", before_str))
                    after_hex_parts.append(ansi("90", after_str))
                elif is_diff:
                    before_hex_parts.append(ansi("91", before_str))
                    after_hex_parts.append(ansi("92", after_str))
                else:
                    before_hex_parts.append(before_str)
                    after_hex_parts.append(after_str)

            before_hex = " ".join(before_hex_parts[:8]) + "  " + " ".join(before_hex_parts[8:])
            after_hex = " ".join(after_hex_parts[:8]) + "  " + " ".join(after_hex_parts[8:])

            before_ascii = "".join(
                chr(byte) if 32 <= byte < 127 else "." for byte in before_chunk
            ).ljust(16)
            after_ascii = "".join(
                chr(byte) if 32 <= byte < 127 else "." for byte in after_chunk
            ).ljust(16)

            print(f"  {row_offset:04x}  {before_hex}  |{before_ascii}|")
            print(f"  {row_offset:04x}  {after_hex}  |{after_ascii}|")
            print()
    else:
        header_parts = ["  offset", "  before   ", "  after    ", "  ascii"]
        print("".join(header_parts))
        print("  " + _hrule(50))
        for offset in sorted(stable_diff_offsets):
            before_byte = before_representative[offset] if offset < len(before_representative) else None
            after_byte = after_representative[offset] if offset < len(after_representative) else None
            before_str = f"0x{before_byte:02x}" if before_byte is not None else "--  "
            after_str = f"0x{after_byte:02x}" if after_byte is not None else "--  "
            before_char = chr(before_byte) if before_byte is not None and 32 <= before_byte < 127 else "."
            after_char = chr(after_byte) if after_byte is not None and 32 <= after_byte < 127 else "."
            print(f"  {offset:04x}    {before_str}       {after_str}       {before_char} {_rarrow()} {after_char}")

    return True


@packet_app.command("state-diff")
def packet_state_diff(
    device_ip: str = typer.Option(..., "--device-ip", help="Device IP address."),
    before_time: str = typer.Option(..., "--before", help="Time before state change (HH:MM:SS). Packets before this time."),
    after_time: str = typer.Option(..., "--after", help="Time after state change (HH:MM:SS). Packets after this time."),
    ignore_volatile: bool = typer.Option(True, "--ignore-volatile/--no-ignore-volatile", help="Exclude known volatile header bytes (transaction_id, sequence)."),
    ignore_jitter: bool = typer.Option(True, "--ignore-jitter/--no-ignore-jitter", help="Exclude bytes that vary within the same time window (counters/timestamps)."),
    direction: Optional[str] = typer.Option("response", "--direction", help="Packet direction filter (default: response)."),
    opcode: Optional[str] = typer.Option(None, "--opcode", help="Filter to specific opcode (hex like 0x2000)."),
    full: bool = typer.Option(False, "--full", help="Show full hex dump with diffs highlighted."),
    session: Optional[str] = typer.Option(None, "--session", help="Session reference (ID, name, latest, active)."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Diff device state between two time windows, grouped by opcode.

    Finds all response packets from a device in both time windows, groups them
    by opcode, and shows only the stable byte differences — filtering out known
    volatile bytes (transaction IDs, sequence counters) and bytes that jitter
    between consecutive same-state packets.
    """
    before_ns = _parse_time_to_ns(before_time)
    after_ns = _parse_time_to_ns(after_time)

    if before_ns is None:
        print(f"Invalid --before time format: {before_time} (use HH:MM:SS)", file=sys.stderr)
        raise typer.Exit(1)
    if after_ns is None:
        print(f"Invalid --after time format: {after_time} (use HH:MM:SS)", file=sys.stderr)
        raise typer.Exit(1)
    if before_ns >= after_ns:
        print("--before must be earlier than --after", file=sys.stderr)
        raise typer.Exit(1)

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    resolved_opcode = _parse_int_option(opcode, "--opcode")

    resolved_direction = direction
    if resolved_direction == "multicast":
        resolved_direction = "__null__"

    store = PacketStore(db_path=resolved_db)
    try:
        session_id = None
        if session is not None:
            session_id, _ = _resolve_session_reference(
                store, session_id=None, session=session, default_selector="latest",
            )

        before_rows = store.search_packets(
            session_id=session_id,
            device_ip=device_ip,
            end_ns=before_ns,
            opcode=resolved_opcode,
            direction=resolved_direction,
            limit=10000,
            ascending=True,
        )
        after_rows = store.search_packets(
            session_id=session_id,
            device_ip=device_ip,
            start_ns=after_ns,
            opcode=resolved_opcode,
            direction=resolved_direction,
            limit=10000,
            ascending=True,
        )

        before_by_opcode: dict[str, list[tuple[int, bytes]]] = {}
        after_by_opcode: dict[str, list[tuple[int, bytes]]] = {}

        for row in before_rows:
            payload = row.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            key = _opcode_key(payload)
            if key is None:
                continue
            before_by_opcode.setdefault(key, []).append((int(row["id"]), payload))

        for row in after_rows:
            payload = row.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            key = _opcode_key(payload)
            if key is None:
                continue
            after_by_opcode.setdefault(key, []).append((int(row["id"]), payload))

        common_opcodes = sorted(set(before_by_opcode.keys()) & set(after_by_opcode.keys()))

        before_ts = datetime.datetime.fromtimestamp(before_ns / 1e9).strftime("%H:%M:%S")
        after_ts = datetime.datetime.fromtimestamp(after_ns / 1e9).strftime("%H:%M:%S")

        print(f"State diff for {device_ip}")
        print(f"  before window: ≤ {before_ts} ({sum(len(v) for v in before_by_opcode.values())} packets, {len(before_by_opcode)} opcodes)")
        print(f"  after  window: ≥ {after_ts} ({sum(len(v) for v in after_by_opcode.values())} packets, {len(after_by_opcode)} opcodes)")
        print(f"  common opcodes: {len(common_opcodes)}")

        before_only = sorted(set(before_by_opcode.keys()) - set(after_by_opcode.keys()))
        after_only = sorted(set(after_by_opcode.keys()) - set(before_by_opcode.keys()))
        if before_only:
            print(f"  before-only opcodes: {', '.join(before_only)}")
        if after_only:
            print(f"  after-only opcodes: {', '.join(after_only)}")

        diff_count = 0
        identical_count = 0

        for opcode_label in common_opcodes:
            before_payloads = before_by_opcode[opcode_label]
            after_payloads = after_by_opcode[opcode_label]

            sample_payload = before_payloads[0][1]
            protocol_type = _classify_protocol(sample_payload)
            volatile_offsets = _get_volatile_offsets(protocol_type) if ignore_volatile else set()

            before_only_payloads = [payload for _, payload in before_payloads]
            after_only_payloads = [payload for _, payload in after_payloads]

            before_jitter = _detect_jitter_offsets(before_only_payloads) if ignore_jitter and len(before_only_payloads) > 1 else set()
            after_jitter = _detect_jitter_offsets(after_only_payloads) if ignore_jitter and len(after_only_payloads) > 1 else set()
            jitter_offsets = before_jitter | after_jitter

            had_diff = _state_diff_print_opcode_diff(
                opcode_label, before_payloads, after_payloads,
                volatile_offsets, jitter_offsets, full,
            )
            if had_diff:
                diff_count += 1
            else:
                identical_count += 1

        print(f"\n  {diff_count} opcodes with stable differences, {identical_count} identical")

    finally:
        store.close()


@app.command("clear")
def clear(
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Delete the capture database."""
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    db_path = Path(resolved_db)

    for suffix in ("", "-shm", "-wal"):
        target = Path(str(db_path) + suffix)
        if target.exists():
            target.unlink()

    print(f"Deleted {db_path}", file=sys.stderr)
