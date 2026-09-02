from __future__ import annotations

import hashlib
import logging

from netaudio.dante.const import (
    ARC_PROTOCOL_IDS,
    DANTE_CONTROLLER_METERING_PORT,
    DEVICE_INFO_PORT,
    PROTOCOL_NAMES,
    PROTOCOL_SETTINGS,
)
from netaudio.dante.dissection.header import parse_packet_header

PORT_LABELS = {
    DANTE_CONTROLLER_METERING_PORT: "metering",
    DEVICE_INFO_PORT: "info",
}

PACKET_ENDPOINT_WIDTH = 28


_FACT_LABEL_CACHE: dict[str, str] | None = None

logger = logging.getLogger("netaudio")


def _load_fact_labels() -> dict[str, str]:
    global _FACT_LABEL_CACHE
    if _FACT_LABEL_CACHE is not None:
        return _FACT_LABEL_CACHE

    _FACT_LABEL_CACHE = {}
    try:
        from netaudio.dante.fact_store import DEFAULT_FACTS_PATH, fact_status, list_facts

        if DEFAULT_FACTS_PATH.exists():
            for fact in list_facts(DEFAULT_FACTS_PATH):
                if fact_status(fact) == "disproved":
                    continue
                category = fact["category"]
                key = fact["key"]
                name = fact["name"]
                if category == "arc_opcode":
                    fact_protocols = fact.get("protocol_id")
                    if fact_protocols is None:
                        _FACT_LABEL_CACHE[f"arc:{key}"] = name
                    else:
                        if not isinstance(fact_protocols, list):
                            fact_protocols = [fact_protocols]
                        for fact_protocol in fact_protocols:
                            _FACT_LABEL_CACHE[f"arc:0x{fact_protocol:04X}:{key}"] = name
                elif category == "conmon_message":
                    _FACT_LABEL_CACHE[f"conmon:{key}"] = name
                elif category == "multicast_announcement":
                    _FACT_LABEL_CACHE[f"multicast:{key}"] = name
    except (OSError, ValueError, KeyError, TypeError) as exception:
        logger.warning(f"Failed to load capture fact labels: {exception}")

    return _FACT_LABEL_CACHE


def _label_packet(payload: bytes, *, include_code: bool = False):
    header = parse_packet_header(payload)
    if header is None:
        return ""

    protocol_id = header["protocol_id"]
    opcode = header["opcode"]
    code = f"0x{opcode:04X}"
    fact_labels = _load_fact_labels()

    if protocol_id == PROTOCOL_SETTINGS:
        fact_name = fact_labels.get(f"conmon:{code}") or fact_labels.get(f"multicast:{code}")
    elif protocol_id in PROTOCOL_NAMES:
        fact_name = None
        if protocol_id in ARC_PROTOCOL_IDS:
            fact_name = fact_labels.get(f"arc:0x{protocol_id:04X}:{code}") or fact_labels.get(f"arc:{code}")
    else:
        return f"proto:0x{protocol_id:04X}"

    if fact_name:
        return f"{code} {fact_name}" if include_code else fact_name
    name = header["opcode_name"]
    if not include_code:
        return name
    if name and name != code:
        return f"{code} {name}"
    return code


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
