from __future__ import annotations

import struct

from netaudio.dante.dissection.models import DECIMAL_FIELD_NAMES, NANOSECOND_FIELD_NAMES

HEXADECIMAL_DIGITS = {"uint16_be": 4, "uint32_be": 8, "uint8": 2}


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

    if name == "link_speed_mbps" and isinstance(int_val, int):
        return f"{int_val} Mbps"

    return ""


def decode_field(raw: bytes, dtype: str):
    if dtype == "ascii":
        return raw.split(b"\x00", 1)[0].decode("ascii", errors="replace")
    if dtype == "int32_be" and len(raw) == 4:
        return struct.unpack(">i", raw)[0]
    if dtype == "ipv4" and len(raw) == 4:
        return ".".join(str(byte) for byte in raw)
    if dtype == "uint16_be" and len(raw) == 2:
        return struct.unpack(">H", raw)[0]
    if dtype == "uint32_be" and len(raw) == 4:
        return struct.unpack(">I", raw)[0]
    if dtype == "uint8" and len(raw) == 1:
        return raw[0]
    return raw.hex()


def hexadecimal_display(value, dtype: str) -> str | None:
    digits = HEXADECIMAL_DIGITS.get(dtype)
    if digits is None or not isinstance(value, int):
        return None
    return f"0x{value:0{digits}X}"


def _extract_value(payload: bytes, offset: int, length: int, dtype: str, name: str = ""):
    value = decode_field(payload[offset : offset + length], dtype)
    if dtype == "ascii":
        return value, f'"{value}"'
    if name not in DECIMAL_FIELD_NAMES:
        display = hexadecimal_display(value, dtype)
        if display is not None:
            return value, display
    return value, str(value)


def _humanize_value(name: str, int_val, display: str, dtype: str) -> str:
    if not isinstance(int_val, int):
        return display

    if name in NANOSECOND_FIELD_NAMES and dtype in ("uint32_be", "int32_be"):
        return f"{int_val:,} ns ({_format_ns(int_val)})"

    if ("sample_rate" in name or name == "current_rate") and dtype == "uint32_be" and int_val > 8000:
        return f"{int_val:,} ({_format_hz(int_val)})"

    return display
