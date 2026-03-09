from __future__ import annotations

import struct
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Span:
    offset: int
    length: int
    name: str
    raw: bytes
    value: str
    detail: str = ""
    fact_ref: str = ""
    section: str = ""
    dtype: str = ""


@dataclass
class DissectedPacket:
    payload: bytes
    spans: list[Span] = field(default_factory=list)
    sections: list[tuple[str, str]] = field(default_factory=list)
    header_summary: str = ""
    fact_refs: list[str] = field(default_factory=list)


PROTOCOL_ID_NAMES = {
    0x1200: "CMC",
    0xFFFF: "Conmon/Settings",
}

ARC_STATUS_NAMES = {
    0x0000: "request",
    0x0001: "success",
    0x0022: "error",
    0x8112: "success (paginated)",
}

NANOSECOND_FIELD_NAMES = {
    "default_latency",
    "current_latency",
    "dup_latency",
    "max_latency",
    "min_latency",
    "target_latency",
    "latency",
    "target_sample_rate",
    "current_rate",
}

DECIMAL_FIELD_NAMES = {
    "packet_length",
    "rx_channel",
    "max_per_page",
    "named_count",
    "channel_number",
    "tx_count",
    "rx_count",
}


DISSECT_FIELD_ICONS = {
    "protocol_id": "\U000f0003",       # nf-md-access_point
    "packet_length": "\U000f03d3",     # nf-md-package
    "sequence": "\U000f03a0",          # nf-md-numeric
    "transaction_id": "\U000f03a0",    # nf-md-numeric
    "opcode": "\U000f0169",            # nf-md-code_braces
    "status": "\U000f02fc",            # nf-md-information
    "source_mac": "\U000f0237",        # nf-md-fingerprint
    "magic": "\uebcf",                 # nf-cod-wand
    "version": "\uf02b",               # nf-fa-tag
    "message_type": "\U000f0315",      # nf-md-label
    "channel_number": "\U000f062e",    # nf-md-tune
    "channel_count": "\U000f062e",     # nf-md-tune
    "tx_count": "\uf093",              # nf-fa-upload
    "rx_count": "\uf019",              # nf-fa-download
    "sample_rate": "\U000f1479",       # nf-md-cosine_wave
    "sample_rate_ptr": "\U000f1479",   # nf-md-cosine_wave
    "tx_channel_ptr": "\uf093",        # nf-fa-upload
    "tx_device_ptr": "\U000f04c3",     # nf-md-speaker
    "rx_channel_ptr": "\uf019",        # nf-fa-download
    "name_ptr": "\U000f0455",          # nf-md-rename_box
    "metadata_ptr": "\U000f0328",      # nf-md-layers
    "flags": "\U000f0328",             # nf-md-layers
    "subscription_status": "\uf0c1",   # nf-fa-link
    "default_latency": "\U000f04c5",   # nf-md-speedometer
    "current_latency": "\U000f04c5",   # nf-md-speedometer
    "max_latency": "\U000f04c5",       # nf-md-speedometer
    "min_latency": "\U000f04c5",       # nf-md-speedometer
    "target_latency": "\U000f04c5",    # nf-md-speedometer
    "latency": "\U000f04c5",           # nf-md-speedometer
    "max_per_page": "\U000f03a0",      # nf-md-numeric
    "named_count": "\U000f03a0",       # nf-md-numeric
    "string": "\U000f0455",            # nf-md-rename_box
    "padding": "\U000f0328",           # nf-md-layers
    "unknown_6": "\U000f02fc",         # nf-md-information
}

DISSECT_SECTION_ICONS = {
    "protocol_structure:arc_header": "\U000f0003",     # nf-md-access_point
    "protocol_structure:conmon_header": "\U000f0003",   # nf-md-access_point
    "protocol_structure:cmc_header": "\U000f0003",      # nf-md-access_point
}


_RESET = "\033[0m"
_BOLD = "\033[1m"

_COLOR_HEADER = "\033[1;97m"
_COLOR_SECTION = "\033[1;36m"
_COLOR_SEPARATOR = "\033[36m"
_COLOR_OFFSET = "\033[37m"
_COLOR_FIELD_NAME = "\033[1;37m"
_COLOR_ANNOTATION = "\033[1;32m"
_COLOR_FACTS = "\033[35m"
_COLOR_UNCHARTED = "\033[33m"
_COLOR_UNCHARTED_HEX = "\033[37m"
_COLOR_ASCII = "\033[90m"
_COLOR_DIRECTION_REQUEST = "\033[1;33m"
_COLOR_DIRECTION_RESPONSE = "\033[1;32m"
_COLOR_DIRECTION_SEND = "\033[1;35m"
_COLOR_DIRECTION_MULTICAST = "\033[1;35m"

_FIELD_PALETTE = [
    "\033[38;5;75m",
    "\033[38;5;114m",
    "\033[38;5;179m",
    "\033[38;5;168m",
    "\033[38;5;73m",
    "\033[38;5;215m",
    "\033[38;5;141m",
    "\033[38;5;107m",
    "\033[38;5;174m",
    "\033[38;5;110m",
]

DIRECTION_COLORS = {
    "request": _COLOR_DIRECTION_REQUEST,
    "response": _COLOR_DIRECTION_RESPONSE,
    "send": _COLOR_DIRECTION_SEND,
    "multicast": _COLOR_DIRECTION_MULTICAST,
}





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

    return ""


CONMON_MESSAGE_NAMES = {
    0x0060: "dante_model_response",
    0x0080: "sample_rate_announcement",
    0x0081: "set_sample_rate",
    0x0090: "reboot",
    0x0092: "reboot_ack",
    0x00C0: "make_model_response",
    0x01FE: "metering_data",
    0x0326: "set_output_gain",
    0x0344: "set_input_gain",
    0x03D7: "set_encoding",
    0x0BC8: "identify",
    0x22DC: "set_aes67",
    0x40FE: "metering_data_extended",
}


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

    if "sample_rate" in name and dtype in ("uint32_be",) and int_val > 8000:
        return f"{int_val:,} ({_format_hz(int_val)})"

    return display


def _load_facts_for_packet(
    payload: bytes,
    facts_path: Path | None = None,
) -> list[dict]:
    if facts_path is None:
        from netaudio_lib.dante.fact_store import DEFAULT_FACTS_PATH

        facts_path = DEFAULT_FACTS_PATH

    if not facts_path.exists():
        return []

    from netaudio_lib.dante.fact_store import list_facts

    all_facts = list_facts(facts_path)

    if len(payload) < 2:
        return []

    protocol_id = struct.unpack(">H", payload[0:2])[0]
    matched = []

    for fact in all_facts:
        fact_protocol = fact.get("protocol_id")
        if fact_protocol is None:
            continue
        if isinstance(fact_protocol, list):
            if protocol_id not in fact_protocol:
                continue
        elif fact_protocol != protocol_id:
            continue

        match_offset = fact.get("match_offset")
        if match_offset is None:
            matched.append(fact)
            continue

        match_size = fact.get("match_size", 2)
        if match_offset + match_size > len(payload):
            continue

        actual_value = int.from_bytes(payload[match_offset:match_offset + match_size], "big")
        fact_key = fact["key"]
        try:
            expected_value = int(fact_key, 0)
        except (ValueError, TypeError):
            continue

        if actual_value == expected_value:
            matched.append(fact)

    return matched


def _find_fact(facts: list[dict], category: str, key: str) -> dict | None:
    for fact in facts:
        if fact["category"] == category and fact["key"] == key:
            return fact
    return None


def _build_span(
    payload: bytes,
    field_def: dict,
    fact_ref: str,
    section_name: str,
    all_facts: list[dict],
) -> Span:
    offset = field_def.get("offset", 0)
    length = field_def.get("length", 0)
    name = field_def.get("name", "?")
    dtype = field_def.get("dtype", "")
    expected = field_def.get("value")

    if offset + length > len(payload):
        return Span(
            offset=offset,
            length=length,
            name=name,
            raw=b"",
            value=f"<out of bounds ({offset}+{length} > {len(payload)})>",
            fact_ref=fact_ref,
            section=section_name,
            dtype=dtype,
        )

    raw = payload[offset : offset + length]
    int_val, display = _extract_value(payload, offset, length, dtype, name)

    detail = ""
    if name == "protocol_id" and isinstance(int_val, int):
        label = PROTOCOL_ID_NAMES.get(int_val)
        if label:
            detail = label
    elif name == "status" and isinstance(int_val, int):
        label = ARC_STATUS_NAMES.get(int_val)
        if label:
            detail = label
    elif name == "opcode" and isinstance(int_val, int):
        opcode_fact = _find_opcode_fact(all_facts, int_val)
        if opcode_fact:
            detail = opcode_fact.get("name", "")

    humanized = _humanize_value(name, int_val, display, dtype)

    if not detail:
        detail = _format_detail(name, raw, int_val, dtype)

    value_str = humanized

    return Span(
        offset=offset,
        length=length,
        name=name,
        raw=raw,
        value=value_str,
        detail=detail,
        fact_ref=fact_ref,
        section=section_name,
        dtype=dtype,
    )


RX_SUBSCRIPTION_STATUS_NAMES = {
    0x0000: "unsubscribed",
    0x0001: "idle",
    0x0002: "in_progress",
    0x0003: "subscribed",
    0x0004: "error",
    0x0005: "rejected",
}


def _get_null_terminated_string(payload: bytes, abs_offset: int) -> str:
    if abs_offset < 0 or abs_offset >= len(payload):
        return ""
    end = payload.find(b"\x00", abs_offset)
    if end < 0:
        end = len(payload)
    return payload[abs_offset:end].decode("ascii", errors="replace")


def _dissect_rx_channels_body(payload: bytes, result: DissectedPacket, covered: set[int]) -> None:
    if len(payload) < 32:
        return

    record_size = 20
    body_start = 12

    max_per_page = payload[10]
    channel_count = payload[11]

    header_section_ref = "arc_opcode:0x3000_header"
    header_section_label = "RX Channel Body Header"
    result.sections.append((header_section_ref, header_section_label))

    result.spans.append(Span(
        offset=10, length=1, name="max_per_page",
        raw=payload[10:11], value=str(max_per_page), detail="",
        fact_ref=header_section_ref, section=header_section_label, dtype="uint8",
    ))
    covered.add(10)

    result.spans.append(Span(
        offset=11, length=1, name="channel_count",
        raw=payload[11:12], value=str(channel_count), detail="",
        fact_ref=header_section_ref, section=header_section_label, dtype="uint8",
    ))
    covered.add(11)

    section_ref = "arc_opcode:0x3000_body"
    section_label = "RX Channel Records"
    result.sections.append((section_ref, section_label))

    record_index = 0
    offset = body_start
    max_records = channel_count if channel_count > 0 else 64
    metadata_pointer = None

    while offset + record_size <= len(payload) and record_index < max_records:
        channel_number = struct.unpack(">H", payload[offset:offset + 2])[0]
        if channel_number == 0:
            break

        flags = struct.unpack(">H", payload[offset + 2:offset + 4])[0]
        sample_rate_pointer = struct.unpack(">H", payload[offset + 4:offset + 6])[0]
        if metadata_pointer is None and sample_rate_pointer > 0:
            metadata_pointer = sample_rate_pointer
        tx_channel_pointer = struct.unpack(">H", payload[offset + 6:offset + 8])[0]
        tx_device_pointer = struct.unpack(">H", payload[offset + 8:offset + 10])[0]
        rx_channel_pointer = struct.unpack(">H", payload[offset + 10:offset + 12])[0]
        status = struct.unpack(">H", payload[offset + 12:offset + 14])[0]
        subscription_status = struct.unpack(">H", payload[offset + 14:offset + 16])[0]

        rx_channel_name = ""
        if rx_channel_pointer > 0:
            rx_channel_name = _get_null_terminated_string(payload, rx_channel_pointer)

        tx_channel_name = ""
        if tx_channel_pointer > 0:
            tx_channel_name = _get_null_terminated_string(payload, tx_channel_pointer)

        tx_device_name = ""
        if tx_device_pointer > 0:
            tx_device_name = _get_null_terminated_string(payload, tx_device_pointer)

        sample_rate = None
        if sample_rate_pointer > 0 and sample_rate_pointer + 4 <= len(payload):
            raw_rate = struct.unpack(">I", payload[sample_rate_pointer:sample_rate_pointer + 4])[0]
            if 8000 <= raw_rate <= 384000:
                sample_rate = raw_rate

        sub_detail = RX_SUBSCRIPTION_STATUS_NAMES.get(subscription_status, "")

        channel_label = rx_channel_name or str(channel_number)
        subscription_info = ""
        if tx_channel_name and tx_device_name:
            subscription_info = f"{tx_channel_name}@{tx_device_name}"
        elif tx_channel_name:
            subscription_info = tx_channel_name

        channel_detail = channel_label
        if subscription_info:
            channel_detail += f" <- {subscription_info}"
        if sample_rate:
            channel_detail += f" ({_format_hz(sample_rate)})"

        result.spans.append(Span(
            offset=offset, length=2, name="channel_number",
            raw=payload[offset:offset + 2],
            value=str(channel_number), detail=channel_detail,
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset, offset + 2):
            covered.add(byte_offset)

        result.spans.append(Span(
            offset=offset + 2, length=2, name="flags",
            raw=payload[offset + 2:offset + 4],
            value=f"0x{flags:04X}", detail="",
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset + 2, offset + 4):
            covered.add(byte_offset)

        result.spans.append(Span(
            offset=offset + 4, length=2, name="sample_rate_ptr",
            raw=payload[offset + 4:offset + 6],
            value=f"0x{sample_rate_pointer:04X}",
            detail=_format_hz(sample_rate) if sample_rate else "",
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset + 4, offset + 6):
            covered.add(byte_offset)

        result.spans.append(Span(
            offset=offset + 6, length=2, name="tx_channel_ptr",
            raw=payload[offset + 6:offset + 8],
            value=f"0x{tx_channel_pointer:04X}",
            detail=tx_channel_name if tx_channel_name and tx_channel_name != "." else "",
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset + 6, offset + 8):
            covered.add(byte_offset)

        result.spans.append(Span(
            offset=offset + 8, length=2, name="tx_device_ptr",
            raw=payload[offset + 8:offset + 10],
            value=f"0x{tx_device_pointer:04X}",
            detail=tx_device_name if tx_device_name and tx_device_name != "." else "",
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset + 8, offset + 10):
            covered.add(byte_offset)

        result.spans.append(Span(
            offset=offset + 10, length=2, name="rx_channel_ptr",
            raw=payload[offset + 10:offset + 12],
            value=f"0x{rx_channel_pointer:04X}",
            detail=rx_channel_name,
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset + 10, offset + 12):
            covered.add(byte_offset)

        result.spans.append(Span(
            offset=offset + 12, length=2, name="status",
            raw=payload[offset + 12:offset + 14],
            value=f"0x{status:04X}", detail="",
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset + 12, offset + 14):
            covered.add(byte_offset)

        result.spans.append(Span(
            offset=offset + 14, length=2, name="subscription_status",
            raw=payload[offset + 14:offset + 16],
            value=f"0x{subscription_status:04X}", detail=sub_detail,
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset + 14, offset + 16):
            covered.add(byte_offset)

        if offset + 16 < offset + record_size:
            result.spans.append(Span(
                offset=offset + 16, length=4, name="padding",
                raw=payload[offset + 16:offset + 20],
                value=payload[offset + 16:offset + 20].hex(), detail="",
                fact_ref=section_ref, section=section_label, dtype="hex",
            ))
            for byte_offset in range(offset + 16, offset + 20):
                covered.add(byte_offset)

        offset += record_size
        record_index += 1

    if metadata_pointer is not None and metadata_pointer + 16 <= len(payload):
        _dissect_rx_metadata_block(payload, metadata_pointer, result, covered)
        string_area_start = metadata_pointer + 16
    else:
        string_area_start = offset

    _dissect_string_area(payload, string_area_start, "arc_opcode:0x3000_strings", "RX Channel Strings", result, covered)


def _dissect_rx_metadata_block(
    payload: bytes,
    start_offset: int,
    result: DissectedPacket,
    covered: set[int],
) -> None:
    section_ref = "arc_opcode:0x3000_metadata"
    section_label = "RX Channel Metadata"
    result.sections.append((section_ref, section_label))

    sample_rate = struct.unpack(">I", payload[start_offset:start_offset + 4])[0]
    result.spans.append(Span(
        offset=start_offset, length=4, name="sample_rate",
        raw=payload[start_offset:start_offset + 4],
        value=str(sample_rate),
        detail=_format_hz(sample_rate) if 8000 <= sample_rate <= 384000 else "",
        fact_ref=section_ref, section=section_label, dtype="uint32_be",
    ))
    for byte_offset in range(start_offset, start_offset + 4):
        covered.add(byte_offset)

    field_names = ["unknown_0x04", "unknown_0x06", "unknown_0x08", "unknown_0x0A", "unknown_0x0C", "unknown_0x0E"]
    for index, field_name in enumerate(field_names):
        field_offset = start_offset + 4 + index * 2
        field_value = struct.unpack(">H", payload[field_offset:field_offset + 2])[0]
        result.spans.append(Span(
            offset=field_offset, length=2, name=field_name,
            raw=payload[field_offset:field_offset + 2],
            value=str(field_value), detail="",
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(field_offset, field_offset + 2):
            covered.add(byte_offset)


def _dissect_tx_channels_body(payload: bytes, result: DissectedPacket, covered: set[int]) -> None:
    if len(payload) < 20:
        return

    record_size = 8
    body_start = 12

    max_per_page = payload[10]
    channel_count = payload[11]

    header_section_ref = "arc_opcode:0x2000_header"
    header_section_label = "TX Channel Body Header"
    result.sections.append((header_section_ref, header_section_label))

    result.spans.append(Span(
        offset=10, length=1, name="max_per_page",
        raw=payload[10:11], value=str(max_per_page), detail="",
        fact_ref=header_section_ref, section=header_section_label, dtype="uint8",
    ))
    covered.add(10)

    result.spans.append(Span(
        offset=11, length=1, name="channel_count",
        raw=payload[11:12], value=str(channel_count), detail="",
        fact_ref=header_section_ref, section=header_section_label, dtype="uint8",
    ))
    covered.add(11)

    section_ref = "arc_opcode:0x2000_body"
    section_label = "TX Channel Records"
    result.sections.append((section_ref, section_label))

    record_index = 0
    offset = body_start
    max_records = channel_count if channel_count > 0 else 128
    metadata_pointer = None

    while offset + record_size <= len(payload) and record_index < max_records:
        channel_number = struct.unpack(">H", payload[offset:offset + 2])[0]
        if channel_number == 0:
            break

        unknown_field = struct.unpack(">H", payload[offset + 2:offset + 4])[0]
        metadata_ptr = struct.unpack(">H", payload[offset + 4:offset + 6])[0]
        name_pointer = struct.unpack(">H", payload[offset + 6:offset + 8])[0]

        if metadata_pointer is None and metadata_ptr > 0:
            metadata_pointer = metadata_ptr

        channel_name = ""
        if name_pointer > 0:
            channel_name = _get_null_terminated_string(payload, name_pointer)

        sample_rate = None
        if metadata_ptr > 0 and metadata_ptr + 4 <= len(payload):
            raw_rate = struct.unpack(">I", payload[metadata_ptr:metadata_ptr + 4])[0]
            if 8000 <= raw_rate <= 384000:
                sample_rate = raw_rate

        channel_detail = channel_name or str(channel_number)
        if sample_rate:
            channel_detail += f" ({_format_hz(sample_rate)})"

        result.spans.append(Span(
            offset=offset, length=2, name="channel_number",
            raw=payload[offset:offset + 2],
            value=str(channel_number), detail=channel_detail,
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset, offset + 2):
            covered.add(byte_offset)

        result.spans.append(Span(
            offset=offset + 2, length=2, name="unknown_0x02",
            raw=payload[offset + 2:offset + 4],
            value=f"0x{unknown_field:04X}", detail="",
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset + 2, offset + 4):
            covered.add(byte_offset)

        result.spans.append(Span(
            offset=offset + 4, length=2, name="metadata_ptr",
            raw=payload[offset + 4:offset + 6],
            value=f"0x{metadata_ptr:04X}",
            detail=_format_hz(sample_rate) if sample_rate else "",
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset + 4, offset + 6):
            covered.add(byte_offset)

        result.spans.append(Span(
            offset=offset + 6, length=2, name="name_ptr",
            raw=payload[offset + 6:offset + 8],
            value=f"0x{name_pointer:04X}",
            detail=channel_name,
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(offset + 6, offset + 8):
            covered.add(byte_offset)

        offset += record_size
        record_index += 1

    if metadata_pointer is not None and metadata_pointer + 16 <= len(payload):
        _dissect_tx_metadata_block(payload, metadata_pointer, result, covered)
        string_area_start = metadata_pointer + 16
    else:
        string_area_start = offset

    _dissect_string_area(payload, string_area_start, "arc_opcode:0x2000_strings", "TX Channel Strings", result, covered)


def _dissect_tx_metadata_block(
    payload: bytes,
    start_offset: int,
    result: DissectedPacket,
    covered: set[int],
) -> None:
    section_ref = "arc_opcode:0x2000_metadata"
    section_label = "TX Channel Metadata"
    result.sections.append((section_ref, section_label))

    sample_rate = struct.unpack(">I", payload[start_offset:start_offset + 4])[0]
    result.spans.append(Span(
        offset=start_offset, length=4, name="sample_rate",
        raw=payload[start_offset:start_offset + 4],
        value=str(sample_rate),
        detail=_format_hz(sample_rate) if 8000 <= sample_rate <= 384000 else "",
        fact_ref=section_ref, section=section_label, dtype="uint32_be",
    ))
    for byte_offset in range(start_offset, start_offset + 4):
        covered.add(byte_offset)

    field_names = ["unknown_0x04", "unknown_0x06", "unknown_0x08", "unknown_0x0A", "unknown_0x0C", "unknown_0x0E"]
    for index, field_name in enumerate(field_names):
        field_offset = start_offset + 4 + index * 2
        field_value = struct.unpack(">H", payload[field_offset:field_offset + 2])[0]
        result.spans.append(Span(
            offset=field_offset, length=2, name=field_name,
            raw=payload[field_offset:field_offset + 2],
            value=str(field_value), detail="",
            fact_ref=section_ref, section=section_label, dtype="uint16_be",
        ))
        for byte_offset in range(field_offset, field_offset + 2):
            covered.add(byte_offset)


def _dissect_string_area(
    payload: bytes,
    start_offset: int,
    section_ref: str,
    section_label: str,
    result: DissectedPacket,
    covered: set[int],
) -> None:
    if start_offset >= len(payload):
        return

    string_data = payload[start_offset:]
    strings_found = []
    pos = 0

    while pos < len(string_data):
        null_pos = string_data.find(b"\x00", pos)
        if null_pos < 0:
            break
        string_val = string_data[pos:null_pos].decode("ascii", errors="replace")
        if string_val:
            strings_found.append((start_offset + pos, string_val))
        pos = null_pos + 1

    if strings_found:
        result.sections.append((section_ref, section_label))
        for string_offset, string_val in strings_found:
            string_length = len(string_val) + 1
            result.spans.append(Span(
                offset=string_offset, length=string_length, name="string",
                raw=payload[string_offset:string_offset + string_length],
                value=f'"{string_val}"', detail="",
                fact_ref=section_ref, section=section_label, dtype="ascii",
            ))
            for byte_offset in range(string_offset, string_offset + string_length):
                covered.add(byte_offset)


def dissect(
    payload: bytes,
    facts: list[dict] | None = None,
    facts_path: Path | None = None,
) -> DissectedPacket:
    if facts is None:
        facts = _load_facts_for_packet(payload, facts_path)

    result = DissectedPacket(payload=payload)
    span_by_offset: dict[int, Span] = {}
    covered = set()

    for fact in facts:
        section_name = fact.get("name", "")
        fact_ref = f"{fact['category']}:{fact['key']}"
        section_label = section_name

        if fact_ref not in result.fact_refs:
            result.fact_refs.append(fact_ref)

        fields = fact.get("fields", [])
        if not fields:
            result.sections.append((fact_ref, section_label))
            continue

        result.sections.append((fact_ref, section_label))

        for field_def in sorted(fields, key=lambda f: f.get("offset", 0)):
            span = _build_span(payload, field_def, fact_ref, section_name, facts)
            span_by_offset[span.offset] = span

            for byte_offset in range(span.offset, span.offset + span.length):
                covered.add(byte_offset)

    result.spans = list(span_by_offset.values())

    if len(payload) >= 8:
        protocol_id = struct.unpack(">H", payload[0:2])[0]
        if protocol_id in (0x2729, 0x2809, 0x27FF):
            opcode = struct.unpack(">H", payload[6:8])[0]
            status = struct.unpack(">H", payload[8:10])[0]
            if status != 0x0000:
                if opcode == 0x3000:
                    _dissect_rx_channels_body(payload, result, covered)
                elif opcode == 0x2000:
                    _dissect_tx_channels_body(payload, result, covered)

    _add_unknown_regions(result, covered)

    if len(payload) >= 4:
        protocol_id = struct.unpack(">H", payload[0:2])[0]
        proto_name = PROTOCOL_ID_NAMES.get(protocol_id, f"0x{protocol_id:04X}")
        pkt_len = struct.unpack(">H", payload[2:4])[0]
        if pkt_len != len(payload):
            result.header_summary = f"protocol={proto_name}  {len(payload)}B  (header says {pkt_len}, LENGTH MISMATCH)"
        else:
            result.header_summary = f"protocol={proto_name}  {pkt_len}B"

    return result


def _find_opcode_fact(facts: list[dict], opcode: int) -> dict | None:
    key = f"0x{opcode:04X}"
    for fact in facts:
        if fact["category"] in ("arc_opcode", "cmc_opcode") and fact["key"] == key:
            return fact
    return None


def _add_unknown_regions(result: DissectedPacket, covered: set[int]):
    payload = result.payload
    total = len(payload)

    if not covered or total == 0:
        return

    uncovered_ranges = []
    start = None

    for i in range(total):
        if i not in covered:
            if start is None:
                start = i
        else:
            if start is not None:
                uncovered_ranges.append((start, i))
                start = None

    if start is not None:
        uncovered_ranges.append((start, total))

    for range_start, range_end in uncovered_ranges:
        length = range_end - range_start
        raw = payload[range_start:range_end]
        result.spans.append(
            Span(
                offset=range_start,
                length=length,
                name="",
                raw=raw,
                value="",
                section="unknown",
                dtype="raw",
            )
        )






_VALUE_WIDTH = 20


def _format_value_with_detail(span: Span, field_color: str = "") -> str:
    if field_color:
        if span.detail:
            return (
                f"{field_color}{span.value:<{_VALUE_WIDTH}s}{_RESET}"
                f" {_COLOR_ANNOTATION}{span.detail}{_RESET}"
            )
        return f"{field_color}{span.value}{_RESET}"
    else:
        if span.detail:
            return f"{span.value:<{_VALUE_WIDTH}s} {span.detail}"
        return span.value


def _render_span_line(span: Span, indent: str, field_color: str = "", icons: bool = False) -> list[str]:
    lines = []
    hex_str = " ".join(f"{b:02x}" for b in span.raw)
    offset_str = f"{span.offset:04x}"

    value_display = _format_value_with_detail(span, field_color)

    field_icon = ""
    if icons:
        glyph = DISSECT_FIELD_ICONS.get(span.name, "")
        if glyph:
            field_icon = f"{glyph} "

    name_display = f"{field_icon}{span.name}"

    skip_hexdump = span.dtype == "ascii"

    if field_color:
        if span.length <= 8:
            lines.append(
                f"{indent}  {_COLOR_OFFSET}{offset_str}{_RESET}  "
                f"{field_color}{hex_str:<24s}{_RESET} "
                f"{_COLOR_FIELD_NAME}{name_display:<24s}{_RESET} = "
                f"{value_display}"
            )
        else:
            size_label = f"({span.length}B)"
            lines.append(
                f"{indent}  {_COLOR_OFFSET}{offset_str}{_RESET}  "
                f"{field_color}{size_label:<24s}{_RESET} "
                f"{_COLOR_FIELD_NAME}{name_display:<24s}{_RESET} = "
                f"{value_display}"
            )
            if not skip_hexdump:
                for chunk_offset in range(0, span.length, 16):
                    chunk = span.raw[chunk_offset : chunk_offset + 16]
                    left = " ".join(f"{b:02x}" for b in chunk[:8])
                    right = " ".join(f"{b:02x}" for b in chunk[8:])
                    ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
                    abs_offset = span.offset + chunk_offset
                    lines.append(
                        f"{indent}        {_COLOR_OFFSET}{abs_offset:04x}{_RESET}  "
                        f"{field_color}{left:<23s}  {right:<23s}{_RESET}  "
                        f"{_COLOR_ASCII}|{ascii_part}|{_RESET}"
                    )
    else:
        if span.length <= 8:
            lines.append(f"{indent}  {offset_str}  {hex_str:<24s} {name_display:<24s} = {value_display}")
        else:
            lines.append(f"{indent}  {offset_str}  ({span.length}B){' ':18s} {name_display:<24s} = {value_display}")
            if not skip_hexdump:
                for chunk_offset in range(0, span.length, 16):
                    chunk = span.raw[chunk_offset : chunk_offset + 16]
                    left = " ".join(f"{b:02x}" for b in chunk[:8])
                    right = " ".join(f"{b:02x}" for b in chunk[8:])
                    ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
                    abs_offset = span.offset + chunk_offset
                    lines.append(f"{indent}        {abs_offset:04x}  {left:<23s}  {right:<23s}  |{ascii_part}|")

    return lines


def _render_hexdump_region(raw: bytes, start_offset: int, indent: str, color: bool = False) -> list[str]:
    lines = []
    for chunk_offset in range(0, len(raw), 16):
        chunk = raw[chunk_offset : chunk_offset + 16]
        left = " ".join(f"{b:02x}" for b in chunk[:8])
        right = " ".join(f"{b:02x}" for b in chunk[8:])
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        abs_offset = start_offset + chunk_offset
        if color:
            lines.append(
                f"{indent}  {_COLOR_OFFSET}{abs_offset:04x}{_RESET}  "
                f"{left:<23s}  {right:<23s}  "
                f"|{ascii_part}|{_RESET}"
            )
        else:
            lines.append(f"{indent}  {abs_offset:04x}  {left:<23s}  {right:<23s}  |{ascii_part}|")
    return lines


def _colored_hex_half(chunk: bytes, start_abs_offset: int, byte_color_map: dict[int, str], default_color: str) -> str:
    if not chunk:
        return ""
    parts = []
    current_color = None
    current_hex = []
    for i, byte_val in enumerate(chunk):
        color = byte_color_map.get(start_abs_offset + i, default_color)
        if color != current_color:
            if current_hex:
                parts.append(f"{current_color}{' '.join(current_hex)}{_RESET}")
            current_color = color
            current_hex = [f"{byte_val:02x}"]
        else:
            current_hex.append(f"{byte_val:02x}")
    if current_hex:
        parts.append(f"{current_color}{' '.join(current_hex)}{_RESET}")
    return " ".join(parts)


def _colored_ascii(chunk: bytes, start_abs_offset: int, byte_color_map: dict[int, str], default_color: str) -> str:
    if not chunk:
        return ""
    parts = []
    current_color = None
    current_chars = []
    for i, byte_val in enumerate(chunk):
        color = byte_color_map.get(start_abs_offset + i, default_color)
        char = chr(byte_val) if 32 <= byte_val < 127 else "."
        if color != current_color:
            if current_chars:
                parts.append(f"{current_color}{''.join(current_chars)}{_RESET}")
            current_color = color
            current_chars = [char]
        else:
            current_chars.append(char)
    if current_chars:
        parts.append(f"{current_color}{''.join(current_chars)}{_RESET}")
    return "".join(parts)


def _render_full_colored_hexdump(payload: bytes, byte_color_map: dict[int, str], indent: str) -> list[str]:
    lines = []
    default_color = _COLOR_UNCHARTED_HEX
    for line_offset in range(0, len(payload), 16):
        chunk = payload[line_offset : line_offset + 16]
        left_chunk = chunk[:8]
        right_chunk = chunk[8:]

        left_hex = _colored_hex_half(left_chunk, line_offset, byte_color_map, default_color)
        right_hex = _colored_hex_half(right_chunk, line_offset + 8, byte_color_map, default_color)
        ascii_str = _colored_ascii(chunk, line_offset, byte_color_map, default_color)

        left_visible_len = len(left_chunk) * 3 - (1 if left_chunk else 0)
        right_visible_len = len(right_chunk) * 3 - (1 if right_chunk else 0)
        left_pad = " " * (23 - left_visible_len)
        right_pad = " " * (23 - right_visible_len)

        lines.append(
            f"{indent}  {_COLOR_OFFSET}{line_offset:04x}{_RESET}  "
            f"{left_hex}{left_pad}  "
            f"{right_hex}{right_pad}  "
            f"{_COLOR_OFFSET}|{_RESET}{ascii_str}{_COLOR_OFFSET}|{_RESET}"
        )
    return lines


def render_dissection(
    dissected: DissectedPacket,
    indent: str = "",
    show_unknown_hexdump: bool = True,
    color: bool = False,
    icons: bool = False,
) -> str:
    lines = []
    field_index = 0
    byte_color_map: dict[int, str] = {}

    header_icon = "\U000f0219 " if icons else ""
    dump_icon = "\U000f048d " if icons else ""

    if dissected.header_summary:
        if color:
            lines.append(f"{indent}{_COLOR_HEADER}{header_icon}{dissected.header_summary}{_RESET}")
        else:
            lines.append(f"{indent}{header_icon}{dissected.header_summary}")
        lines.append("")

    known_spans = sorted(
        [s for s in dissected.spans if s.section != "unknown"],
        key=lambda s: s.offset,
    )
    unknown_spans = sorted(
        [s for s in dissected.spans if s.section == "unknown"],
        key=lambda s: s.offset,
    )

    section_labels = {}
    for ref, label in dissected.sections:
        section_labels[ref] = label

    section_span_groups = []
    if known_spans:
        current_ref = known_spans[0].fact_ref
        current_group = []

        for span in known_spans:
            if span.fact_ref != current_ref:
                if current_group:
                    section_span_groups.append((current_ref, current_group))
                current_ref = span.fact_ref
                current_group = [span]
            else:
                current_group.append(span)

        if current_group:
            section_span_groups.append((current_ref, current_group))

    emitted_sections = set()

    for fact_ref, spans in section_span_groups:
        label = section_labels.get(fact_ref, fact_ref)
        section_icon = ""
        if icons:
            glyph = DISSECT_SECTION_ICONS.get(fact_ref, "")
            if glyph:
                section_icon = f"{glyph} "
        if fact_ref not in emitted_sections:
            if color:
                lines.append(f"{indent}{_COLOR_SECTION}{section_icon}{label}{_RESET}")
                lines.append(f"{indent}{_COLOR_SEPARATOR}{'─' * 72}{_RESET}")
            else:
                lines.append(f"{indent}{section_icon}{label}")
                lines.append(f"{indent}{'─' * 72}")
            emitted_sections.add(fact_ref)

        for span in spans:
            if color:
                field_color = _FIELD_PALETTE[field_index % len(_FIELD_PALETTE)]
                field_index += 1
                for byte_offset in range(span.offset, span.offset + span.length):
                    byte_color_map[byte_offset] = field_color
            else:
                field_color = ""
            lines.extend(_render_span_line(span, indent, field_color=field_color, icons=icons))

    if show_unknown_hexdump:
        if color:
            lines.append("")
            lines.append(f"{indent}{_COLOR_SECTION}{dump_icon}Packet Dump ({len(dissected.payload)}B){_RESET}")
            lines.append(f"{indent}{_COLOR_SEPARATOR}{'─' * 72}{_RESET}")
            lines.extend(_render_full_colored_hexdump(dissected.payload, byte_color_map, indent))
        elif unknown_spans:
            total_unknown = sum(s.length for s in unknown_spans)
            lines.append("")
            lines.append(f"{indent}{dump_icon}Uncharted ({total_unknown}B)")
            lines.append(f"{indent}{'─' * 72}")
            for span in unknown_spans:
                lines.extend(_render_hexdump_region(span.raw, span.offset, indent))

    return "\n".join(lines)


def dissect_and_render(
    payload: bytes,
    facts: list[dict] | None = None,
    facts_path: Path | None = None,
    indent: str = "",
    show_unknown_hexdump: bool = True,
    color: bool | None = None,
    icons: bool | None = None,
) -> str:
    if color is None:
        from netaudio_lib.common.app_config import settings as app_settings
        color = not app_settings.no_color
    if icons is None:
        icons = _resolve_icons_setting()
    dissected = dissect(payload, facts=facts, facts_path=facts_path)
    return render_dissection(dissected, indent=indent, show_unknown_hexdump=show_unknown_hexdump, color=color, icons=icons)


def _resolve_icons_setting() -> bool:
    try:
        from netaudio.cli import state
        return state.icons
    except Exception:
        return False


_FACTS_CACHE: list[dict] | None = None


def _cached_facts(facts_path: Path | None = None) -> list[dict]:
    global _FACTS_CACHE
    if _FACTS_CACHE is not None:
        return _FACTS_CACHE

    if facts_path is None:
        from netaudio_lib.dante.fact_store import DEFAULT_FACTS_PATH

        facts_path = DEFAULT_FACTS_PATH

    if not facts_path.exists():
        _FACTS_CACHE = []
        return _FACTS_CACHE

    from netaudio_lib.dante.fact_store import list_facts

    _FACTS_CACHE = list_facts(facts_path)
    return _FACTS_CACHE


def format_dissect_label(direction: str, address: str, command_name: str = "", color: bool = False) -> str:
    if color:
        direction_color = DIRECTION_COLORS.get(direction, _BOLD)
        label = f"{direction_color}{direction}{_RESET} {address}"
        if command_name:
            label += f" {_COLOR_OFFSET}({command_name}){_RESET}"
    else:
        label = f"{direction} {address}"
        if command_name:
            label += f" ({command_name})"
    return label


def hexdump_or_dissect(
    payload: bytes,
    indent: str = "         ",
    dissect_mode: bool = False,
    color: bool | None = None,
) -> str:
    if color is None:
        from netaudio_lib.common.app_config import settings as app_settings
        color = not app_settings.no_color
    if not dissect_mode:
        return _plain_hexdump(payload, indent, color=color)
    return dissect_and_render(payload, indent=indent, color=color)


def _plain_hexdump(data: bytes, indent: str = "         ", color: bool = False) -> str:
    lines = []
    for offset in range(0, len(data), 16):
        chunk = data[offset : offset + 16]
        left = " ".join(f"{b:02x}" for b in chunk[:8])
        right = " ".join(f"{b:02x}" for b in chunk[8:])
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        if color:
            lines.append(
                f"{indent}{_COLOR_OFFSET}{offset:04x}{_RESET}  "
                f"{left:<23s}  {right:<23s}  "
                f"{_COLOR_ASCII}|{ascii_part}|{_RESET}"
            )
        else:
            lines.append(f"{indent}{offset:04x}  {left:<23s}  {right:<23s}  |{ascii_part}|")
    return "\n".join(lines)
