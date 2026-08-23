from __future__ import annotations

CLOCK_SOURCE_MAXIMUM = 0xFFFF
CLOCK_SUBDOMAIN_SIZE = 16


def clock_subdomain_bytes(value) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
    elif isinstance(value, str):
        raw = value.encode("latin-1")
    elif isinstance(value, (list, tuple)):
        if any(not isinstance(item, int) or item < 0 or item > 255 for item in value):
            return None
        raw = bytes(value)
    else:
        return None
    if len(raw) > CLOCK_SUBDOMAIN_SIZE:
        return None
    return raw.ljust(CLOCK_SUBDOMAIN_SIZE, b"\x00")


def format_clock_source_code(clock_source_code) -> str:
    if clock_source_code is None:
        return "unknown"
    if isinstance(clock_source_code, bool) or not isinstance(clock_source_code, int):
        return "unknown"
    return f"{clock_source_code} (0x{clock_source_code:04X})"


def format_clock_subdomain(value) -> str:
    raw = clock_subdomain_bytes(value)
    if raw is None:
        return "unknown"
    terminator = raw.find(b"\x00")
    text_bytes = raw if terminator < 0 else raw[:terminator]
    if not text_bytes:
        return "unset"
    try:
        text = text_bytes.decode("ascii")
    except UnicodeDecodeError:
        return text_bytes.hex()
    if text.isprintable():
        return text
    return text_bytes.hex()


def parse_clock_source_selection(text: str) -> int:
    if not isinstance(text, str) or not text.strip():
        raise ValueError("clock source must be an integer from 0 through 65535")
    stripped = text.strip()
    if stripped.lower().startswith("0x"):
        try:
            clock_source = int(stripped, 16)
        except ValueError as exception:
            raise ValueError("clock source must be an integer from 0 through 65535") from exception
    else:
        if not stripped.isdigit():
            raise ValueError("clock source must be an integer from 0 through 65535")
        clock_source = int(stripped)
    if clock_source > CLOCK_SOURCE_MAXIMUM:
        raise ValueError("clock source must be an integer from 0 through 65535")
    return clock_source


def parse_clock_subdomain_selection(text: str) -> bytes:
    if not isinstance(text, str):
        raise ValueError("clock subdomain must be an ASCII string, hex:<bytes>, or unset")
    stripped = text.strip()
    if stripped.lower() in {"", "unset", "default"}:
        return bytes(CLOCK_SUBDOMAIN_SIZE)
    if stripped.lower().startswith("hex:"):
        hexadecimal = stripped[4:].replace(" ", "")
        try:
            raw = bytes.fromhex(hexadecimal)
        except ValueError as exception:
            raise ValueError("clock subdomain hex must be an even-length hexadecimal string") from exception
        if len(raw) > CLOCK_SUBDOMAIN_SIZE:
            raise ValueError("clock subdomain is longer than 16 bytes")
        return raw.ljust(CLOCK_SUBDOMAIN_SIZE, b"\x00")
    try:
        raw = stripped.encode("ascii")
    except UnicodeEncodeError as exception:
        raise ValueError("clock subdomain must be ASCII or hex:<bytes>") from exception
    if len(raw) > CLOCK_SUBDOMAIN_SIZE:
        raise ValueError("clock subdomain is longer than 16 bytes")
    return raw.ljust(CLOCK_SUBDOMAIN_SIZE, b"\x00")
