from __future__ import annotations

import struct
from pathlib import Path

from netaudio.dante.packet_channel_dissection import _dissect_rx_channels_body, _dissect_tx_channels_body
from netaudio.dante.packet_dissection_models import (
    ARC_PROTOCOL_IDENTIFIERS,
    ARC_SUCCESS_STATUSES,
    CONMON_MESSAGE_NAMES,
    PROTOCOL_ID_NAMES,
    DissectedPacket,
    Span,
)
from netaudio.dante.packet_dissection_rendering import (
    dissect_and_render,
    format_dissect_label,
    hexdump_or_dissect,
    render_dissection,
)
from netaudio.dante.packet_dissection_values import _extract_value, _format_detail, _humanize_value
from netaudio.dante.packet_settings_dissection import (
    _dissect_device_settings_query,
    _dissect_device_settings_response,
    _dissect_encoding_control,
    _dissect_encoding_status,
    _dissect_property_directory,
    _dissect_routing_capacity_status,
)


def _load_facts_for_packet(
    payload: bytes,
    facts_path: Path | None = None,
) -> list[dict]:
    if facts_path is None:
        from netaudio.dante.fact_store import DEFAULT_FACTS_PATH

        facts_path = DEFAULT_FACTS_PATH

    if not facts_path.exists():
        return []

    from netaudio.dante.fact_store import fact_status, list_facts

    all_facts = [fact for fact in list_facts(facts_path) if fact_status(fact) != "disproved"]

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

        actual_value = int.from_bytes(payload[match_offset : match_offset + match_size], "big")
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


def dissect(
    payload: bytes,
    facts: list[dict] | None = None,
    facts_path: Path | None = None,
    direction: str | None = None,
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
            field_direction = field_def.get("direction")
            if field_direction is not None and field_direction != direction:
                continue
            span = _build_span(payload, field_def, fact_ref, section_name, facts)
            span_by_offset[span.offset] = span

            for byte_offset in range(span.offset, span.offset + span.length):
                covered.add(byte_offset)

    result.spans = list(span_by_offset.values())

    if len(payload) >= 8:
        protocol_id = struct.unpack(">H", payload[0:2])[0]
        if protocol_id in ARC_PROTOCOL_IDENTIFIERS and len(payload) >= 10:
            opcode = struct.unpack(">H", payload[6:8])[0]
            status = struct.unpack(">H", payload[8:10])[0]
            if opcode == 0x1100:
                if status == 0x0000:
                    _dissect_device_settings_query(payload, result, covered)
                elif status in ARC_SUCCESS_STATUSES:
                    _dissect_device_settings_response(payload, result, covered)
            elif opcode == 0x1102 and status in ARC_SUCCESS_STATUSES:
                _dissect_property_directory(payload, result, covered)
            elif status != 0x0000:
                if opcode == 0x3000:
                    _dissect_rx_channels_body(payload, result, covered)
                elif opcode == 0x2000:
                    _dissect_tx_channels_body(payload, result, covered)

        if protocol_id == 0xFFFF and len(payload) >= 28:
            message_type = struct.unpack(">H", payload[26:28])[0]
            if message_type == 0x0082:
                _dissect_encoding_status(payload, result, covered)
            elif message_type == 0x0083:
                _dissect_encoding_control(payload, result, covered)
            elif message_type == 0x0100:
                _dissect_routing_capacity_status(payload, result, covered)

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
