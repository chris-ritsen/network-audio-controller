from __future__ import annotations

from netaudio import core


def clock_subdomain_bytes(value) -> bytes | None:
    if value is None:
        return None

    try:
        return core.normalize_clock_subdomain(value)
    except core.NetaudioCoreError as error:
        if error.category != "json_input":
            raise

        return None


def format_clock_source_code(clock_source_code) -> str:
    return core.clock_sources({"current": clock_source_code, "supported": []})["current"] or "unknown"


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
        return "unknown"

    if text.isprintable():
        return text

    return "unknown"


def parse_clock_source_selection(text: str) -> int:
    if not isinstance(text, str) or not text.strip():
        raise ValueError("clock source must be an integer")

    stripped = text.strip()

    if stripped.lower().startswith("0x"):
        try:
            clock_source = int(stripped, 16)
        except ValueError as exception:
            raise ValueError("clock source must be an integer") from exception
    else:
        if not stripped.isdigit():
            raise ValueError("clock source must be an integer")

        clock_source = int(stripped)

    try:
        selected = core.clock_sources({"current": clock_source, "supported": []})["current"]
    except core.NetaudioCoreError as error:
        if error.category != "json_input":
            raise

        raise ValueError("clock source is unknown or unsupported") from error

    if selected is None:
        raise ValueError("clock source is unknown or unsupported")

    return clock_source


def parse_clock_subdomain_selection(text: str) -> bytes:
    if not isinstance(text, str):
        raise ValueError("clock subdomain must be an ASCII string, hex:<bytes>, or unset")

    stripped = text.strip()

    if stripped.lower() in {"", "unset", "default"}:
        raw = b""
    elif stripped.lower().startswith("hex:"):
        hexadecimal = stripped[4:].replace(" ", "")

        try:
            raw = bytes.fromhex(hexadecimal)
        except ValueError as exception:
            raise ValueError("clock subdomain hex must be an even-length hexadecimal string") from exception
    else:
        try:
            raw = stripped.encode("ascii")
        except UnicodeEncodeError as exception:
            raise ValueError("clock subdomain must be ASCII or hex:<bytes>") from exception

    try:
        return core.normalize_clock_subdomain(raw)
    except core.NetaudioCoreError as error:
        if error.category != "json_input":
            raise

        raise ValueError(error.detail or str(error)) from error
