from __future__ import annotations

import logging
from pathlib import Path

from netaudio.dante.const import (
    PROTOCOL_LABELS,
    RESULT_CODE_REQUEST,
)
from netaudio.dante.dissection.header import parse_packet_header
from netaudio.dante.dissection.models import DissectedPacket, Span
from netaudio.dante.dissection.values import _extract_value, _format_detail, _humanize_value

logger = logging.getLogger("netaudio")


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

    protocol_id = int.from_bytes(payload[0:2], "big")
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


def _build_span(
    payload: bytes,
    field_def: dict,
    fact_ref: str,
    section_name: str,
    all_facts: list[dict],
    header: dict | None,
) -> Span:
    offset = field_def.get("offset", 0)
    length = field_def.get("length", 0)
    name = field_def.get("name", "?")
    dtype = field_def.get("dtype", "")

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
    if (
        name == "message_type"
        and header is not None
        and header["protocol_name"] == "PROTOCOL_SETTINGS"
        and int_val == header["opcode"]
    ):
        detail = (header.get("response_decoder") or {}).get("kind") or section_name
    elif name == "protocol_id" and isinstance(int_val, int):
        label = PROTOCOL_LABELS.get(int_val)
        if label:
            detail = label
    elif name == "status" and header is not None and int_val == header.get("result_code"):
        detail = header.get("result_label") or ""
    elif name == "opcode" and isinstance(int_val, int):
        opcode_fact = _find_opcode_fact(all_facts, int_val)
        if opcode_fact:
            detail = opcode_fact.get("name", "")

    humanized = _humanize_value(name, int_val, display, dtype)

    if not detail:
        detail = _format_detail(name, raw, int_val, dtype)

    return Span(
        offset=offset,
        length=length,
        name=name,
        raw=raw,
        value=humanized,
        detail=detail,
        fact_ref=fact_ref,
        section=section_name,
        dtype=dtype,
    )


def _core_fields(payload: bytes, header: dict, request: bytes | None) -> tuple[str | None, dict | None]:
    from netaudio import core

    decoder = header.get("response_decoder")

    if decoder is None:
        return None, None

    kind, paged = decoder["kind"], decoder["paged"]

    try:
        if paged:
            if request is not None:
                request_header = parse_packet_header(request)
                if (
                    request_header is None
                    or request_header["result_code"] != RESULT_CODE_REQUEST
                    or any(request_header[key] != header[key] for key in ("protocol_id", "opcode", "transaction_id"))
                ):
                    return kind, None
            starting_channel = core.parse_response("channel_page_start", request if request is not None else payload)
            parsed = [] if starting_channel is None else core.parse_page(kind, payload, starting_channel)
            parsed = {"records": parsed, "starting_channel": starting_channel}
        else:
            parsed = core.parse_response(kind, payload)
    except core.NetaudioCoreError as exception:
        logger.debug(f"Core {kind} parser rejected packet: {exception}")
        return kind, None
    if not isinstance(parsed, dict):
        parsed = {"records": parsed}
    return kind, parsed


def dissect(
    payload: bytes,
    facts: list[dict] | None = None,
    facts_path: Path | None = None,
    direction: str | None = None,
    request: bytes | None = None,
) -> DissectedPacket:
    if facts is None:
        facts = _load_facts_for_packet(payload, facts_path)

    result = DissectedPacket(payload=payload)
    header = parse_packet_header(payload)
    span_by_offset: dict[int, Span] = {}
    covered = set()

    for fact in facts:
        section_name = fact.get("name", "")
        fact_ref = f"{fact['category']}:{fact['key']}"

        if fact_ref not in result.fact_refs:
            result.fact_refs.append(fact_ref)

        result.sections.append((fact_ref, section_name))
        for field_def in sorted(fact.get("fields", []), key=lambda f: f.get("offset", 0)):
            field_direction = field_def.get("direction")
            if field_direction is not None and field_direction != direction:
                continue
            span = _build_span(payload, field_def, fact_ref, section_name, facts, header)
            span_by_offset[span.offset] = span

            for byte_offset in range(span.offset, span.offset + span.length):
                covered.add(byte_offset)

    result.spans = list(span_by_offset.values())

    if header is not None:
        result.core_kind, result.core_fields = _core_fields(payload, header, request)
        protocol_label = PROTOCOL_LABELS.get(header["protocol_id"], f"0x{header['protocol_id']:04X}")
        if header["length"] != len(payload):
            result.header_summary = (
                f"protocol={protocol_label}  {len(payload)}B  (header says {header['length']}, LENGTH MISMATCH)"
            )
        else:
            result.header_summary = f"protocol={protocol_label}  {header['length']}B"

    _add_unknown_regions(result, covered)

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
