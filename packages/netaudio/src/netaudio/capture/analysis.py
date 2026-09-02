from __future__ import annotations

import datetime

from netaudio.dante.const import ARC_PROTOCOL_IDS, PROTOCOL_SETTINGS
from netaudio.dante.packet_header import parse_packet_header

ARC_VOLATILE_OFFSETS = {4, 5}
CONMON_VOLATILE_OFFSETS = {4, 5}


def _classify_protocol(payload: bytes) -> str:
    header = parse_packet_header(payload)
    if header is None:
        return "unknown"
    if header["protocol_id"] in ARC_PROTOCOL_IDS:
        return "arc"
    if header["protocol_id"] == PROTOCOL_SETTINGS:
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
    header = parse_packet_header(payload)
    if header is None:
        return None
    protocol_id = header["protocol_id"]
    if protocol_id in ARC_PROTOCOL_IDS:
        return f"arc:0x{header['opcode']:04X}"
    if protocol_id == PROTOCOL_SETTINGS:
        return f"conmon:0x{header['opcode']:04X}"
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
            today.year,
            today.month,
            today.day,
            hours,
            minutes,
            seconds,
            microseconds,
        )
        return int(target.timestamp() * 1e9)
    except (ValueError, IndexError):
        return None
