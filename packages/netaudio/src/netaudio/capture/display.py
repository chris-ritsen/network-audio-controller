from __future__ import annotations

import datetime

from netaudio.capture.markers import normalize_marker_type
from netaudio.capture.packets import _hexdump, _load_fact_labels
from netaudio.cli_support.execution import ansi
from netaudio.common.app_config import settings as app_settings
from netaudio.dante.packet_store import PacketStore
from netaudio.icons import icon


def _hrule(width: int) -> str:
    return "-" * width if app_settings.no_color else "─" * width


def _emdash() -> str:
    return "--" if app_settings.no_color else "—"


def _rarrow() -> str:
    return "->" if app_settings.no_color else "→"


def _print_timeline_header(show_window_packets: bool = False):
    pass


def _format_marker_time(iso_timestamp: str | None) -> str:
    if not iso_timestamp:
        return ""
    try:
        parsed = datetime.datetime.fromisoformat(iso_timestamp)
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return iso_timestamp[:8] if iso_timestamp else ""


def _print_wrapped(text: str, indent: str = "  "):
    import shutil
    import textwrap

    terminal_width = shutil.get_terminal_size((120, 24)).columns
    wrapped = textwrap.fill(
        text,
        width=terminal_width,
        initial_indent=indent,
        subsequent_indent=indent,
    )
    print(wrapped)


def _print_marker_row(
    marker: dict,
    next_timestamp: int | None,
    session_id: int,
    store: PacketStore,
    use_dissect: bool = False,
    show_notes: bool = True,
    show_packets: bool = True,
    show_window_packets: bool = False,
    brief: bool = False,
):
    marker_id = int(marker["id"])
    marker_time = int(marker["timestamp_ns"])
    window_packets = None
    if show_window_packets:
        window_packets = store.get_session_packet_count(session_id, start_ns=marker_time, end_ns=next_timestamp)

    summary_text = marker.get("summary") or ""
    label_text = marker.get("label") or ""
    note_text = marker.get("note") or ""
    marker_type = normalize_marker_type(str(marker.get("marker_type") or "observation"), strict=False)

    _MARKER_TYPE_ICONS = {
        "hypothesis": "info",
        "observation": "info",
        "evidence": "marker",
        "code_change": "config",
        "action": "success",
        "system": "server",
    }
    marker_type_icon = icon(_MARKER_TYPE_ICONS.get(marker_type, "marker"))
    time_text = _format_marker_time(marker.get("timestamp_iso"))

    if brief:
        display_text = summary_text if summary_text else label_text
        print(f"  {time_text}  {marker_type_icon}{marker_type:12s}  {display_text}")
        return

    print()
    header = f"{ansi('1', f'{marker_type_icon}{marker_type}')}  {label_text}"
    if window_packets is not None:
        header += f"  ({window_packets} packets)"

    marker_data = marker.get("data")
    evidence_packet_ids = []
    if marker_data and marker.get("marker_type") == "evidence":
        evidence_packet_ids = marker_data.get("packet_ids") or []
        if not evidence_packet_ids and marker_data.get("packet_id"):
            evidence_packet_ids = [marker_data["packet_id"]]

    header += _evidence_size_summary(store, evidence_packet_ids)

    print(f"  {time_text}  {header}  {ansi('90', f'#{marker_id}')}")

    if summary_text and show_notes:
        _print_wrapped(summary_text)

    if show_notes and note_text:
        _print_wrapped(ansi("90", note_text))
    elif not show_notes and summary_text:
        _print_wrapped(summary_text)

    if not show_packets:
        return

    if marker_data and marker.get("marker_type") == "evidence":
        _print_evidence_packets(store, evidence_packet_ids, marker_data.get("filters", {}), use_dissect)


def _evidence_size_summary(store: PacketStore, evidence_packet_ids: list[int]) -> str:
    evidence_sizes = []
    for packet_id in evidence_packet_ids[:20]:
        packet = store.get_packet(packet_id)
        if packet:
            payload = packet.get("payload", b"")
            evidence_sizes.append(f"#{packet_id} {len(payload)}B")
    if not evidence_sizes:
        return ""
    return f"  {ansi('90', f'[{chr(44).join(evidence_sizes)}]')}"


def _print_evidence_packets(store: PacketStore, packet_ids: list[int], filters: dict, use_dissect: bool) -> None:
    if filters:
        filter_parts = [f"{key}={value}" for key, value in filters.items()]
        print(f"    filters: {', '.join(filter_parts)}")

    evidence_indent = "      "
    for packet_id in packet_ids[:20]:
        packet = store.get_packet(packet_id)
        if not packet:
            continue

        payload = packet.get("payload", b"")
        opcode_hex = ""

        if len(payload) >= 8:
            opcode_hex = f"0x{int.from_bytes(payload[6:8], 'big'):04X} "

        direction = packet.get("direction") or "multicast"
        direction_icon = (
            icon("tx") if direction == "request" else icon("rx") if direction == "response" else icon("packet")
        )
        source = f"{packet.get('src_ip', '?')}:{packet.get('src_port', '?')}"
        destination = f"{packet.get('dst_ip', '?')}:{packet.get('dst_port', '?')}"
        print(
            f"{evidence_indent}{direction_icon}#{packet_id} {direction:8s} {opcode_hex}{source} -> {destination} {len(payload)}B"
        )

        if use_dissect:
            from netaudio.dante.dissection.rendering import dissect_and_render

            print(dissect_and_render(payload, indent=evidence_indent + "  "))
        else:
            print(_hexdump(payload, indent=evidence_indent + "  "))

    if len(packet_ids) > 20:
        print(f"{'':8s}  {'':26s}  {'':12s}  ... and {len(packet_ids) - 20} more")


def _follow_session_timeline(
    store: PacketStore,
    session_id: int,
    last_marker_id: int,
    poll_interval: float,
    use_dissect: bool,
    show_notes: bool = True,
    show_packets: bool = True,
    show_window_packets: bool = True,
    brief: bool = False,
):
    import time

    seen_id = last_marker_id

    try:
        while True:
            time.sleep(poll_interval)

            session_row = store.get_session(session_id)
            markers = store.get_markers(session_id)
            new_markers = [marker for marker in markers if int(marker["id"]) > seen_id]

            if not new_markers:
                if session_row and session_row.get("ended_ns"):
                    total = store.get_session_packet_count(session_id)
                    print(f"\nSession ended. {total} packets total.")
                    break
                continue

            for marker_index, marker in enumerate(new_markers):
                if marker_index + 1 < len(new_markers):
                    next_timestamp = int(new_markers[marker_index + 1]["timestamp_ns"])
                else:
                    next_timestamp = None

                _print_marker_row(
                    marker,
                    next_timestamp,
                    session_id,
                    store,
                    use_dissect=use_dissect,
                    show_notes=show_notes,
                    show_packets=show_packets,
                    show_window_packets=show_window_packets,
                    brief=brief,
                )
                seen_id = int(marker["id"])

    except KeyboardInterrupt:
        total = store.get_session_packet_count(session_id)
        print(f"\nStopped following. {total} packets so far.")


def _print_session_evidence(store: PacketStore, sessions: list, has_evidence: bool, no_evidence: bool):
    from netaudio.dante.dissection.rendering import dissect_and_render

    for session in sessions:
        session_id = int(session["id"])
        evidence_count = store.get_session_evidence_count(session_id)
        session_name = session.get("name") or f"session_{session_id}"

        if has_evidence and evidence_count == 0:
            continue
        if no_evidence and evidence_count > 0:
            continue
        if evidence_count == 0:
            continue

        markers = store.get_markers(session_id, marker_types=["evidence"])

        print(
            f"\n{icon('session')}Session #{session_id} ({session_name}) {_emdash()} {evidence_count} evidence packet(s)"
        )
        print(_hrule(80))

        for marker in markers:
            marker_data = marker.get("data")
            if not marker_data:
                continue

            marker_label = marker.get("label") or ""
            marker_summary = marker.get("summary") or ""
            display_label = marker_summary or marker_label
            if display_label:
                print(f"\n  {icon('marker')}{display_label}")

            packet_ids = marker_data.get("packet_ids") or []
            if not packet_ids and marker_data.get("packet_id"):
                packet_ids = [marker_data["packet_id"]]

            for packet_id in packet_ids:
                packet = store.get_packet(packet_id)
                if not packet:
                    continue

                payload = packet.get("payload", b"")
                direction = packet.get("direction") or "multicast"
                source_ip = packet.get("src_ip") or "?"
                source_port = packet.get("src_port") or "?"
                destination_ip = packet.get("dst_ip") or "?"
                destination_port = packet.get("dst_port") or "?"

                opcode_hex = ""
                if len(payload) >= 8:
                    opcode_hex = f"0x{int.from_bytes(payload[6:8], 'big'):04X} "

                direction_icon = (
                    icon("tx") if direction == "request" else icon("rx") if direction == "response" else icon("packet")
                )
                arrow = "->" if direction == "request" else "<-" if direction == "response" else "**"

                print(
                    f"\n    {direction_icon}#{packet_id} {direction:8s} {opcode_hex}"
                    f"{source_ip}:{source_port} {arrow} {destination_ip}:{destination_port} "
                    f"{len(payload)}B"
                )
                print(dissect_and_render(payload, indent="    "))


def _print_diff_compact(packets, differing_offsets, max_length):
    header_parts = ["  offset"]
    for packet_id, _, _ in packets:
        header_parts.append(f"  #{packet_id:<8d}")
    header_parts.append("  ascii")
    print("".join(header_parts))
    print("  " + _hrule(8 + 12 * len(packets) + 8))

    for offset in sorted(differing_offsets):
        parts = [f"  {offset:04x}   "]
        ascii_parts = []
        for _, _, payload in packets:
            if offset < len(payload):
                byte_value = payload[offset]
                parts.append(f"  0x{byte_value:02x}      ")
                ascii_parts.append(chr(byte_value) if 32 <= byte_value < 127 else ".")
            else:
                parts.append("  --        ")
                ascii_parts.append(" ")
        parts.append("  " + " ".join(ascii_parts))
        print("".join(parts))


def _print_diff_full(packets, differing_offsets, max_length):
    reference_payload = packets[0][2]
    reference_packet_id = packets[0][0]

    for compare_packet_id, _, compare_payload in packets[1:]:
        print(f"\n  #{reference_packet_id} vs #{compare_packet_id}")
        print("  " + _hrule(80))

        for row_offset in range(0, max_length, 16):
            reference_chunk = (
                reference_payload[row_offset : row_offset + 16] if row_offset < len(reference_payload) else b""
            )
            compare_chunk = compare_payload[row_offset : row_offset + 16] if row_offset < len(compare_payload) else b""

            row_has_difference = any(
                offset in differing_offsets for offset in range(row_offset, min(row_offset + 16, max_length))
            )
            if not row_has_difference:
                continue

            reference_hex_parts = []
            compare_hex_parts = []
            for byte_index in range(16):
                absolute_offset = row_offset + byte_index
                reference_byte = reference_chunk[byte_index] if byte_index < len(reference_chunk) else None
                compare_byte = compare_chunk[byte_index] if byte_index < len(compare_chunk) else None
                is_different = absolute_offset in differing_offsets

                if reference_byte is not None:
                    reference_text = f"{reference_byte:02x}"
                else:
                    reference_text = "--"
                if compare_byte is not None:
                    compare_text = f"{compare_byte:02x}"
                else:
                    compare_text = "--"

                if is_different:
                    reference_hex_parts.append(ansi("91", reference_text))
                    compare_hex_parts.append(ansi("92", compare_text))
                else:
                    reference_hex_parts.append(reference_text)
                    compare_hex_parts.append(compare_text)

            reference_ascii = "".join(chr(byte) if 32 <= byte < 127 else "." for byte in reference_chunk).ljust(16)
            compare_ascii = "".join(chr(byte) if 32 <= byte < 127 else "." for byte in compare_chunk).ljust(16)

            reference_hex = " ".join(reference_hex_parts[:8]) + "  " + " ".join(reference_hex_parts[8:])
            compare_hex = " ".join(compare_hex_parts[:8]) + "  " + " ".join(compare_hex_parts[8:])

            print(f"  {row_offset:04x}  {reference_hex}  |{reference_ascii}|")
            print(f"  {row_offset:04x}  {compare_hex}  |{compare_ascii}|")
            print()


def _state_diff_print_opcode_diff(
    opcode_label: str,
    before_payloads: list[tuple[int, bytes]],
    after_payloads: list[tuple[int, bytes]],
    volatile_offsets: set[int],
    jitter_offsets: set[int],
    full: bool,
):
    ignored_offsets = volatile_offsets | jitter_offsets

    before_representative_id, before_representative = before_payloads[-1]
    after_representative_id, after_representative = after_payloads[-1]

    max_length = max(len(before_representative), len(after_representative))
    stable_difference_offsets = set()

    for offset in range(max_length):
        if offset in ignored_offsets:
            continue
        before_byte = before_representative[offset] if offset < len(before_representative) else None
        after_byte = after_representative[offset] if offset < len(after_representative) else None
        if before_byte != after_byte:
            stable_difference_offsets.add(offset)

    if not stable_difference_offsets:
        return False

    fact_labels = _load_fact_labels()
    human_name = fact_labels.get(opcode_label, opcode_label)

    print(f"\n  {ansi('1', opcode_label)}  {human_name}")
    print(
        f"  before: #{before_representative_id} ({len(before_representative)}B)    after: #{after_representative_id} ({len(after_representative)}B)"
    )
    if jitter_offsets:
        print(
            f"  {ansi('90', f'({len(jitter_offsets)} jitter bytes excluded, {len(volatile_offsets)} volatile header bytes excluded)')}"
        )
    elif volatile_offsets:
        print(f"  {ansi('90', f'({len(volatile_offsets)} volatile header bytes excluded)')}")
    print(f"  {len(stable_difference_offsets)} stable bytes differ")

    if full:
        for row_offset in range(0, max_length, 16):
            before_chunk = (
                before_representative[row_offset : row_offset + 16] if row_offset < len(before_representative) else b""
            )
            after_chunk = (
                after_representative[row_offset : row_offset + 16] if row_offset < len(after_representative) else b""
            )

            row_has_difference = any(
                offset in stable_difference_offsets for offset in range(row_offset, min(row_offset + 16, max_length))
            )
            if not row_has_difference:
                continue

            before_hex_parts = []
            after_hex_parts = []
            for byte_index in range(16):
                absolute_offset = row_offset + byte_index
                before_byte = before_chunk[byte_index] if byte_index < len(before_chunk) else None
                after_byte = after_chunk[byte_index] if byte_index < len(after_chunk) else None
                is_different = absolute_offset in stable_difference_offsets
                is_jitter = absolute_offset in ignored_offsets

                before_text = f"{before_byte:02x}" if before_byte is not None else "--"
                after_text = f"{after_byte:02x}" if after_byte is not None else "--"

                if is_jitter:
                    before_hex_parts.append(ansi("90", before_text))
                    after_hex_parts.append(ansi("90", after_text))
                elif is_different:
                    before_hex_parts.append(ansi("91", before_text))
                    after_hex_parts.append(ansi("92", after_text))
                else:
                    before_hex_parts.append(before_text)
                    after_hex_parts.append(after_text)

            before_hex = " ".join(before_hex_parts[:8]) + "  " + " ".join(before_hex_parts[8:])
            after_hex = " ".join(after_hex_parts[:8]) + "  " + " ".join(after_hex_parts[8:])

            before_ascii = "".join(chr(byte) if 32 <= byte < 127 else "." for byte in before_chunk).ljust(16)
            after_ascii = "".join(chr(byte) if 32 <= byte < 127 else "." for byte in after_chunk).ljust(16)

            print(f"  {row_offset:04x}  {before_hex}  |{before_ascii}|")
            print(f"  {row_offset:04x}  {after_hex}  |{after_ascii}|")
            print()
    else:
        header_parts = ["  offset", "  before   ", "  after    ", "  ascii"]
        print("".join(header_parts))
        print("  " + _hrule(50))
        for offset in sorted(stable_difference_offsets):
            before_byte = before_representative[offset] if offset < len(before_representative) else None
            after_byte = after_representative[offset] if offset < len(after_representative) else None
            before_text = f"0x{before_byte:02x}" if before_byte is not None else "--  "
            after_text = f"0x{after_byte:02x}" if after_byte is not None else "--  "
            before_character = chr(before_byte) if before_byte is not None and 32 <= before_byte < 127 else "."
            after_character = chr(after_byte) if after_byte is not None and 32 <= after_byte < 127 else "."
            print(
                f"  {offset:04x}    {before_text}       {after_text}       {before_character} {_rarrow()} {after_character}"
            )

    return True
