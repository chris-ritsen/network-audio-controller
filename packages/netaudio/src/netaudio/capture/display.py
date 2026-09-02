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
    next_ts: int | None,
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
        window_packets = store.get_session_packet_count(session_id, start_ns=marker_time, end_ns=next_ts)

    summary_text = marker.get("summary") or ""
    label_text = marker.get("label") or ""
    note_text = marker.get("note") or ""
    marker_type_str = normalize_marker_type(str(marker.get("marker_type") or "observation"), strict=False)

    _MARKER_TYPE_ICONS = {
        "hypothesis": "info",
        "observation": "info",
        "evidence": "marker",
        "code_change": "config",
        "action": "success",
        "system": "server",
    }
    marker_type_icon = icon(_MARKER_TYPE_ICONS.get(marker_type_str, "marker"))
    time_str = _format_marker_time(marker.get("timestamp_iso"))

    if brief:
        display_text = summary_text if summary_text else label_text
        print(f"  {time_str}  {marker_type_icon}{marker_type_str:12s}  {display_text}")
        return

    print()
    header = f"{ansi('1', f'{marker_type_icon}{marker_type_str}')}  {label_text}"
    if window_packets is not None:
        header += f"  ({window_packets} packets)"

    marker_data = marker.get("data")
    evidence_packet_ids = []
    if marker_data and marker.get("marker_type") == "evidence":
        evidence_packet_ids = marker_data.get("packet_ids") or []
        if not evidence_packet_ids and marker_data.get("packet_id"):
            evidence_packet_ids = [marker_data["packet_id"]]

    header += _evidence_size_summary(store, evidence_packet_ids)

    print(f"  {time_str}  {header}  {ansi('90', f'#{marker_id}')}")

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
    for pid in evidence_packet_ids[:20]:
        pkt = store.get_packet(pid)
        if pkt:
            payload = pkt.get("payload", b"")
            evidence_sizes.append(f"#{pid} {len(payload)}B")
    if not evidence_sizes:
        return ""
    return f"  {ansi('90', f'[{chr(44).join(evidence_sizes)}]')}"


def _print_evidence_packets(store: PacketStore, packet_ids: list[int], filters: dict, use_dissect: bool) -> None:
    if filters:
        filter_parts = [f"{k}={v}" for k, v in filters.items()]
        print(f"    filters: {', '.join(filter_parts)}")

    evidence_indent = "      "
    for pid in packet_ids[:20]:
        pkt = store.get_packet(pid)
        if not pkt:
            continue

        payload = pkt.get("payload", b"")
        opcode_hex = ""

        if len(payload) >= 8:
            opcode_hex = f"0x{int.from_bytes(payload[6:8], 'big'):04X} "

        pkt_dir = pkt.get("direction") or "multicast"
        pkt_dir_icon = icon("tx") if pkt_dir == "request" else icon("rx") if pkt_dir == "response" else icon("packet")
        src = f"{pkt.get('src_ip', '?')}:{pkt.get('src_port', '?')}"
        dst = f"{pkt.get('dst_ip', '?')}:{pkt.get('dst_port', '?')}"
        print(f"{evidence_indent}{pkt_dir_icon}#{pid} {pkt_dir:8s} {opcode_hex}{src} -> {dst} {len(payload)}B")

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
            new_markers = [m for m in markers if int(m["id"]) > seen_id]

            if not new_markers:
                if session_row and session_row.get("ended_ns"):
                    total = store.get_session_packet_count(session_id)
                    print(f"\nSession ended. {total} packets total.")
                    break
                continue

            for i, marker in enumerate(new_markers):
                if i + 1 < len(new_markers):
                    next_ts = int(new_markers[i + 1]["timestamp_ns"])
                else:
                    next_ts = None

                _print_marker_row(
                    marker,
                    next_ts,
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
    for pid, _, _ in packets:
        header_parts.append(f"  #{pid:<8d}")
    header_parts.append("  ascii")
    print("".join(header_parts))
    print("  " + _hrule(8 + 12 * len(packets) + 8))

    for offset in sorted(differing_offsets):
        parts = [f"  {offset:04x}   "]
        ascii_parts = []
        for _, _, payload in packets:
            if offset < len(payload):
                byte_val = payload[offset]
                parts.append(f"  0x{byte_val:02x}      ")
                ascii_parts.append(chr(byte_val) if 32 <= byte_val < 127 else ".")
            else:
                parts.append("  --        ")
                ascii_parts.append(" ")
        parts.append("  " + " ".join(ascii_parts))
        print("".join(parts))


def _print_diff_full(packets, differing_offsets, max_length):
    reference_payload = packets[0][2]
    reference_pid = packets[0][0]

    for compare_pid, _, compare_payload in packets[1:]:
        print(f"\n  #{reference_pid} vs #{compare_pid}")
        print("  " + _hrule(80))

        for row_offset in range(0, max_length, 16):
            ref_chunk = reference_payload[row_offset : row_offset + 16] if row_offset < len(reference_payload) else b""
            cmp_chunk = compare_payload[row_offset : row_offset + 16] if row_offset < len(compare_payload) else b""

            row_has_diff = any(
                offset in differing_offsets for offset in range(row_offset, min(row_offset + 16, max_length))
            )
            if not row_has_diff:
                continue

            ref_hex_parts = []
            cmp_hex_parts = []
            for byte_index in range(16):
                absolute_offset = row_offset + byte_index
                ref_byte = ref_chunk[byte_index] if byte_index < len(ref_chunk) else None
                cmp_byte = cmp_chunk[byte_index] if byte_index < len(cmp_chunk) else None
                is_diff = absolute_offset in differing_offsets

                if ref_byte is not None:
                    ref_str = f"{ref_byte:02x}"
                else:
                    ref_str = "--"
                if cmp_byte is not None:
                    cmp_str = f"{cmp_byte:02x}"
                else:
                    cmp_str = "--"

                if is_diff:
                    ref_hex_parts.append(ansi("91", ref_str))
                    cmp_hex_parts.append(ansi("92", cmp_str))
                else:
                    ref_hex_parts.append(ref_str)
                    cmp_hex_parts.append(cmp_str)

            ref_ascii = "".join(chr(b) if 32 <= b < 127 else "." for b in ref_chunk).ljust(16)
            cmp_ascii = "".join(chr(b) if 32 <= b < 127 else "." for b in cmp_chunk).ljust(16)

            ref_hex = " ".join(ref_hex_parts[:8]) + "  " + " ".join(ref_hex_parts[8:])
            cmp_hex = " ".join(cmp_hex_parts[:8]) + "  " + " ".join(cmp_hex_parts[8:])

            print(f"  {row_offset:04x}  {ref_hex}  |{ref_ascii}|")
            print(f"  {row_offset:04x}  {cmp_hex}  |{cmp_ascii}|")
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
    stable_diff_offsets = set()

    for offset in range(max_length):
        if offset in ignored_offsets:
            continue
        before_byte = before_representative[offset] if offset < len(before_representative) else None
        after_byte = after_representative[offset] if offset < len(after_representative) else None
        if before_byte != after_byte:
            stable_diff_offsets.add(offset)

    if not stable_diff_offsets:
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
    print(f"  {len(stable_diff_offsets)} stable bytes differ")

    if full:
        for row_offset in range(0, max_length, 16):
            before_chunk = (
                before_representative[row_offset : row_offset + 16] if row_offset < len(before_representative) else b""
            )
            after_chunk = (
                after_representative[row_offset : row_offset + 16] if row_offset < len(after_representative) else b""
            )

            row_has_diff = any(
                offset in stable_diff_offsets for offset in range(row_offset, min(row_offset + 16, max_length))
            )
            if not row_has_diff:
                continue

            before_hex_parts = []
            after_hex_parts = []
            for byte_index in range(16):
                absolute_offset = row_offset + byte_index
                before_byte = before_chunk[byte_index] if byte_index < len(before_chunk) else None
                after_byte = after_chunk[byte_index] if byte_index < len(after_chunk) else None
                is_diff = absolute_offset in stable_diff_offsets
                is_jitter = absolute_offset in ignored_offsets

                before_str = f"{before_byte:02x}" if before_byte is not None else "--"
                after_str = f"{after_byte:02x}" if after_byte is not None else "--"

                if is_jitter:
                    before_hex_parts.append(ansi("90", before_str))
                    after_hex_parts.append(ansi("90", after_str))
                elif is_diff:
                    before_hex_parts.append(ansi("91", before_str))
                    after_hex_parts.append(ansi("92", after_str))
                else:
                    before_hex_parts.append(before_str)
                    after_hex_parts.append(after_str)

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
        for offset in sorted(stable_diff_offsets):
            before_byte = before_representative[offset] if offset < len(before_representative) else None
            after_byte = after_representative[offset] if offset < len(after_representative) else None
            before_str = f"0x{before_byte:02x}" if before_byte is not None else "--  "
            after_str = f"0x{after_byte:02x}" if after_byte is not None else "--  "
            before_char = chr(before_byte) if before_byte is not None and 32 <= before_byte < 127 else "."
            after_char = chr(after_byte) if after_byte is not None and 32 <= after_byte < 127 else "."
            print(f"  {offset:04x}    {before_str}       {after_str}       {before_char} {_rarrow()} {after_char}")

    return True
