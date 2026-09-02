from __future__ import annotations

import struct

from netaudio.dante.const import CONMON_MESSAGE_NAMES
from netaudio.dante.dissection.models import DECIMAL_FIELD_NAMES, NANOSECOND_FIELD_NAMES


def _format_ns(value: int) -> str:
    if value == 0:
        return "0 ns"

    if value >= 1_000_000:
        ms = value / 1_000_000
        if ms == int(ms):
            return f"{int(ms)} ms"
        return f"{ms:.2f} ms"

    if value >= 1_000:
        us = value / 1_000
        if us == int(us):
            return f"{int(us)} us"
        return f"{us:.1f} us"

    return f"{value} ns"


def _format_hz(value: int) -> str:
    if value == 0:
        return "0 Hz"

    if value >= 1_000:
        khz = value / 1_000
        if khz == int(khz):
            return f"{int(khz)} kHz"
        return f"{khz:.1f} kHz"

    return f"{value} Hz"


def _format_detail(name: str, raw: bytes, int_val, dtype: str) -> str:
    if "mac" in name and len(raw) >= 6:
        mac_bytes = raw[:6]
        return ":".join(f"{b:02x}" for b in mac_bytes)

    if name == "version" and dtype == "uint16_be" and isinstance(int_val, int):
        major = (int_val >> 8) & 0xFF
        minor = int_val & 0xFF
        return f"v{major}.{minor}"

    if name == "message_type" and isinstance(int_val, int):
        label = CONMON_MESSAGE_NAMES.get(int_val)
        if label:
            return label

    if name == "link_speed_mbps" and isinstance(int_val, int):
        return f"{int_val} Mbps"

    return ""


def _extract_value(payload: bytes, offset: int, length: int, dtype: str, name: str = ""):
    raw = payload[offset : offset + length]

    if dtype == "uint8" and length == 1:
        val = raw[0]
        if name in DECIMAL_FIELD_NAMES:
            return val, str(val)
        return val, f"0x{val:02X}"
    elif dtype == "uint16_be" and length == 2:
        val = struct.unpack(">H", raw)[0]
        if name in DECIMAL_FIELD_NAMES:
            return val, str(val)
        return val, f"0x{val:04X}"
    elif dtype == "uint32_be" and length == 4:
        val = struct.unpack(">I", raw)[0]
        if name in DECIMAL_FIELD_NAMES:
            return val, str(val)
        return val, f"0x{val:08X}"
    elif dtype == "int32_be" and length == 4:
        val = struct.unpack(">i", raw)[0]
        return val, str(val)
    elif dtype == "ascii":
        null_pos = raw.find(b"\x00")
        if null_pos >= 0:
            val = raw[:null_pos].decode("ascii", errors="replace")
        else:
            val = raw.decode("ascii", errors="replace")
        return val, f'"{val}"'
    elif dtype == "ipv4" and length == 4:
        val = f"{raw[0]}.{raw[1]}.{raw[2]}.{raw[3]}"
        return val, val
    elif dtype == "hex":
        val = raw.hex()
        return val, val

    return raw.hex(), raw.hex()


def _humanize_value(name: str, int_val, display: str, dtype: str) -> str:
    if not isinstance(int_val, int):
        return display

    if name in NANOSECOND_FIELD_NAMES and dtype in ("uint32_be", "int32_be"):
        return f"{int_val:,} ns ({_format_ns(int_val)})"

    if ("sample_rate" in name or name == "current_rate") and dtype == "uint32_be" and int_val > 8000:
        return f"{int_val:,} ({_format_hz(int_val)})"

    return display
