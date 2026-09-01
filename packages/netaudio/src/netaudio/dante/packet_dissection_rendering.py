from __future__ import annotations

from pathlib import Path

from netaudio.dante.packet_dissection_models import DissectedPacket, Span


DISSECT_FIELD_ICONS = {
    "protocol_id": "\U000f0003",
    "packet_length": "\U000f03d3",
    "sequence": "\U000f03a0",
    "transaction_id": "\U000f03a0",
    "opcode": "\U000f0169",
    "status": "\U000f02fc",
    "source_eui64": "\U000f0237",
    "magic": "\uebcf",
    "version": "\uf02b",
    "message_type": "\U000f0315",
    "channel_number": "\U000f062e",
    "channel_count": "\U000f062e",
    "tx_count": "\uf093",
    "rx_count": "\uf019",
    "sample_rate": "\U000f1479",
    "sample_rate_ptr": "\U000f1479",
    "tx_channel_ptr": "\uf093",
    "tx_device_ptr": "\U000f04c3",
    "rx_channel_ptr": "\uf019",
    "name_ptr": "\U000f0455",
    "metadata_ptr": "\U000f0328",
    "flags": "\U000f0328",
    "subscription_status": "\uf0c1",
    "default_latency": "\U000f04c5",
    "configured_latency": "\U000f04c5",
    "active_latency": "\U000f04c5",
    "current_latency": "\U000f04c5",
    "max_latency": "\U000f04c5",
    "min_latency": "\U000f04c5",
    "target_latency": "\U000f04c5",
    "latency": "\U000f04c5",
    "supported_encoding_count": "\U000f0169",
    "current_encoding": "\U000f0169",
    "target_encoding": "\U000f0169",
    "max_per_page": "\U000f03a0",
    "named_count": "\U000f03a0",
    "string": "\U000f0455",
    "padding": "\U000f0328",
    "unknown_6": "\U000f02fc",
}


DISSECT_SECTION_ICONS = {
    "protocol_structure:arc_header": "\U000f0003",
    "protocol_structure:conmon_header": "\U000f0003",
    "protocol_structure:cmc_header": "\U000f0003",
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


_VALUE_WIDTH = 20


def _format_value_with_detail(span: Span, field_color: str = "") -> str:
    if field_color:
        if span.detail:
            return f"{field_color}{span.value:<{_VALUE_WIDTH}s}{_RESET} {_COLOR_ANNOTATION}{span.detail}{_RESET}"
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
                f"{indent}  {_COLOR_OFFSET}{abs_offset:04x}{_RESET}  {left:<23s}  {right:<23s}  |{ascii_part}|{_RESET}"
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
    direction: str | None = None,
) -> str:
    if color is None:
        from netaudio.common.app_config import settings as app_settings

        color = not app_settings.no_color
    if icons is None:
        icons = _resolve_icons_setting()
    from netaudio.dante.packet_dissector import dissect

    dissected = dissect(payload, facts=facts, facts_path=facts_path, direction=direction)
    return render_dissection(
        dissected, indent=indent, show_unknown_hexdump=show_unknown_hexdump, color=color, icons=icons
    )


def _resolve_icons_setting() -> bool:
    try:
        from netaudio.cli import state

        return state.icons
    except Exception:
        return False


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
        from netaudio.common.app_config import settings as app_settings

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
