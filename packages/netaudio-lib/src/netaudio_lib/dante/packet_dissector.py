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
    0x2729: "LX-DANTE (128ch)",
    0x2809: "AVIO adapters",
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


DECIMAL_FIELD_NAMES = {
    "packet_length",
    "rx_channel",
    "max_per_page",
    "named_count",
    "channel_number",
    "tx_count",
    "rx_count",
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
        val = raw.rstrip(b"\x00").decode("ascii", errors="replace")
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

    if protocol_id == 0xFFFF:
        matched.append(_find_fact(all_facts, "protocol_structure", "conmon_header"))

        if len(payload) >= 28:
            message_type = struct.unpack(">H", payload[26:28])[0]
            key = f"0x{message_type:04X}"
            matched.append(_find_fact(all_facts, "conmon_message", key))

    elif protocol_id in (0x2729, 0x2809, 0x27FF):
        matched.append(_find_fact(all_facts, "protocol_structure", "arc_header"))

        if len(payload) >= 8:
            opcode = struct.unpack(">H", payload[6:8])[0]
            key = f"0x{opcode:04X}"
            matched.append(_find_fact(all_facts, "arc_opcode", key))

    elif protocol_id == 0x1200:
        matched.append(_find_fact(all_facts, "protocol_structure", "arc_header"))

        if len(payload) >= 8:
            opcode = struct.unpack(">H", payload[6:8])[0]
            key = f"0x{opcode:04X}"
            matched.append(_find_fact(all_facts, "cmc_opcode", key))

    return [f for f in matched if f is not None]


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

    if expected is not None:
        expected_str = str(expected)
        match_marker = ""
        if expected_str.startswith("0x"):
            try:
                if isinstance(int_val, int) and int_val == int(expected_str, 16):
                    match_marker = " [match]"
            except ValueError:
                pass

        if detail:
            value_str = f"{humanized} ({detail}){match_marker}"
        else:
            value_str = f"{humanized}{match_marker}"
    elif detail:
        value_str = f"{humanized} ({detail})"
    else:
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
        confidence = fact.get("confidence", "unknown")
        section_label = f"{section_name} [{confidence}]"

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

    _add_unknown_regions(result, covered)

    if len(payload) >= 8:
        protocol_id = struct.unpack(">H", payload[0:2])[0]
        proto_name = PROTOCOL_ID_NAMES.get(protocol_id, f"0x{protocol_id:04X}")
        pkt_len = struct.unpack(">H", payload[2:4])[0]
        result.header_summary = f"protocol={proto_name}  length={pkt_len}  ({len(payload)}B actual)"

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


def _render_span_line(span: Span, indent: str) -> list[str]:
    lines = []
    hex_str = " ".join(f"{b:02x}" for b in span.raw)
    offset_str = f"{span.offset:04x}"

    if span.length <= 8:
        lines.append(f"{indent}  {offset_str}  {hex_str:<24s} {span.name:<24s} = {span.value}")
    else:
        lines.append(f"{indent}  {offset_str}  ({span.length}B){' ':18s} {span.name:<24s} = {span.value}")
        for chunk_offset in range(0, span.length, 16):
            chunk = span.raw[chunk_offset : chunk_offset + 16]
            left = " ".join(f"{b:02x}" for b in chunk[:8])
            right = " ".join(f"{b:02x}" for b in chunk[8:])
            ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
            abs_offset = span.offset + chunk_offset
            lines.append(f"{indent}        {abs_offset:04x}  {left:<23s}  {right:<23s}  |{ascii_part}|")

    return lines


def _render_hexdump_region(raw: bytes, start_offset: int, indent: str) -> list[str]:
    lines = []
    for chunk_offset in range(0, len(raw), 16):
        chunk = raw[chunk_offset : chunk_offset + 16]
        left = " ".join(f"{b:02x}" for b in chunk[:8])
        right = " ".join(f"{b:02x}" for b in chunk[8:])
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        abs_offset = start_offset + chunk_offset
        lines.append(f"{indent}  {abs_offset:04x}  {left:<23s}  {right:<23s}  |{ascii_part}|")
    return lines


def render_dissection(
    dissected: DissectedPacket,
    indent: str = "",
    show_unknown_hexdump: bool = True,
) -> str:
    lines = []

    if dissected.header_summary:
        lines.append(f"{indent}{dissected.header_summary}")
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
        if fact_ref not in emitted_sections:
            lines.append(f"{indent}{label}")
            lines.append(f"{indent}{'─' * 72}")
            emitted_sections.add(fact_ref)

        for span in spans:
            lines.extend(_render_span_line(span, indent))

    if show_unknown_hexdump and unknown_spans:
        total_unknown = sum(s.length for s in unknown_spans)
        lines.append("")
        lines.append(f"{indent}Uncharted ({total_unknown}B)")
        lines.append(f"{indent}{'─' * 72}")

        for span in unknown_spans:
            lines.extend(_render_hexdump_region(span.raw, span.offset, indent))

    if dissected.fact_refs:
        lines.append("")
        lines.append(f"{indent}Facts applied: {', '.join(dissected.fact_refs)}")

    return "\n".join(lines)


def dissect_and_render(
    payload: bytes,
    facts: list[dict] | None = None,
    facts_path: Path | None = None,
    indent: str = "",
    show_unknown_hexdump: bool = True,
) -> str:
    dissected = dissect(payload, facts=facts, facts_path=facts_path)
    return render_dissection(dissected, indent=indent, show_unknown_hexdump=show_unknown_hexdump)


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


def hexdump_or_dissect(
    payload: bytes,
    indent: str = "         ",
    dissect_mode: bool = False,
) -> str:
    if not dissect_mode:
        return _plain_hexdump(payload, indent)

    return dissect_and_render(payload, indent=indent)


def _plain_hexdump(data: bytes, indent: str = "         ") -> str:
    lines = []
    for offset in range(0, len(data), 16):
        chunk = data[offset : offset + 16]
        left = " ".join(f"{b:02x}" for b in chunk[:8])
        right = " ".join(f"{b:02x}" for b in chunk[8:])
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"{indent}{offset:04x}  {left:<23s}  {right:<23s}  |{ascii_part}|")
    return "\n".join(lines)
