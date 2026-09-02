from __future__ import annotations

import datetime
import struct
from pathlib import Path
from typing import Optional

import typer

from netaudio.capture.analysis import (
    _classify_protocol,
    _detect_jitter_offsets,
    _get_volatile_offsets,
    _opcode_key,
    _parse_time_to_ns,
)
from netaudio.capture.display import (
    _emdash,
    _print_diff_compact,
    _print_diff_full,
    _state_diff_print_opcode_diff,
)
from netaudio.capture.packets import _format_endpoint, _hexdump, _label_packet, _print_packet_table_header
from netaudio.commands.capture.options import (
    _load_capture_profile,
    _parse_int_option,
    _parse_time_filter,
    _resolve_db_from_config,
    _resolve_session_reference,
)
from netaudio.commands.capture.reporting import (
    emit_report,
    format_packet_timestamp,
    packet_row_data,
    structured_output_selected,
)
from netaudio.dante.packet_store import PacketQuery, PacketStore


def packet_list(
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, name, latest, active). Omit to search all packets.",
    ),
    device_ip: Optional[str] = typer.Option(None, "--device-ip", help="Filter by device IP (src or dst)."),
    source_ip: Optional[str] = typer.Option(None, "--src", help="Filter by source IP."),
    destination_ip: Optional[str] = typer.Option(None, "--dst", help="Filter by destination IP."),
    port: Optional[int] = typer.Option(None, "--port", help="Filter by port (src or dst)."),
    device_name: Optional[str] = typer.Option(None, "--device", help="Filter by device name."),
    opcode: Optional[str] = typer.Option(None, "--opcode", help="Filter by opcode (hex like 0x2000 or decimal)."),
    protocol: Optional[str] = typer.Option(
        None, "--protocol", help="Filter by protocol ID (hex like 0x27FF or decimal)."
    ),
    direction: Optional[str] = typer.Option(
        None, "--direction", help="Filter by direction: request, response, or multicast."
    ),
    after: Optional[str] = typer.Option(
        None, "--after", help="Show packets after this time (HH:MM:SS or ISO timestamp)."
    ),
    before: Optional[str] = typer.Option(
        None, "--before", help="Show packets before this time (HH:MM:SS or ISO timestamp)."
    ),
    grep: Optional[str] = typer.Option(None, "--grep", help="Filter packets containing this string in their payload."),
    tail: Optional[int] = typer.Option(
        None, "--tail", help="Show the N most recent packets (shorthand for --descending --limit N)."
    ),
    limit: int = typer.Option(200, "--limit", min=1, max=10000, help="Max packets to show."),
    offset: int = typer.Option(0, "--offset", min=0, help="Skip first N results."),
    descending: bool = typer.Option(False, "--descending", help="Show newest packets first."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Search and filter captured packets."""
    from netaudio.capture.daemon import PacketLine, _print_packet_line
    from netaudio.cli import state as cli_state

    if tail is not None:
        descending = True
        limit = tail

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    resolved_opcode = _parse_int_option(opcode, "--opcode")
    resolved_protocol = _parse_int_option(protocol, "--protocol")

    resolved_direction = direction
    if resolved_direction == "multicast":
        resolved_direction = "__null__"

    store = PacketStore(db_path=resolved_db)
    try:
        session_id = None
        session_row = None
        if session is not None:
            session_id, session_row = _resolve_session_reference(
                store,
                session_id=None,
                session=session,
                default_selector="latest",
            )

        start_ns, end_ns = _packet_search_window(store, session_id, session_row, after, before)
        query = PacketQuery(
            ascending=not descending,
            device_ip=device_ip,
            device_name=device_name,
            direction=resolved_direction,
            dst_ip=destination_ip,
            end_ns=end_ns,
            limit=limit,
            offset=offset,
            opcode=resolved_opcode,
            payload_contains=grep,
            port=port,
            protocol_id=resolved_protocol,
            session_id=session_id,
            src_ip=source_ip,
            start_ns=start_ns,
        )
        total = store.search_packets_count(query)
        rows = store.search_packets(query)
    finally:
        store.close()

    scope = f"session #{session_id}" if session_id else "all packets"
    filters = _describe_packet_filters(
        after=after,
        before=before,
        destination_ip=destination_ip,
        device_ip=device_ip,
        device_name=device_name,
        direction=direction,
        grep=grep,
        opcode=resolved_opcode,
        port=port,
        protocol=resolved_protocol,
        source_ip=source_ip,
    )
    lines = [f"Capture: {scope} {_emdash()} {total} matched, showing {len(rows)} (limit={limit} offset={offset})"]
    if filters:
        lines.append(f"Capture: Filters: {', '.join(filters)}")
    emit_report(
        lines,
        {
            "filters": filters,
            "limit": limit,
            "offset": offset,
            "packets": [packet_row_data(row) for row in rows],
            "scope": scope,
            "session_id": session_id,
            "shown": len(rows),
            "total": total,
        },
    )
    if structured_output_selected():
        return
    _print_packet_table_header()
    for row in rows:
        _print_packet_line(PacketLine.from_row(row), dump=dump, dissect_mode=cli_state.dissect)


def _packet_search_window(
    store: PacketStore, session_id: int | None, session_row: dict | None, after: str | None, before: str | None
) -> tuple[int | None, int | None]:
    start_ns = None
    end_ns = None
    if session_row:
        start_ns = session_row.get("started_ns")
        ended_ns = session_row.get("ended_ns")
        if ended_ns:
            end_ns = ended_ns
    if after and session_id is not None:
        start_ns = _parse_time_filter(after, store, session_id)
    if before and session_id is not None:
        end_ns = _parse_time_filter(before, store, session_id)
    return start_ns, end_ns


def _describe_packet_filters(
    *,
    after: str | None,
    before: str | None,
    destination_ip: str | None,
    device_ip: str | None,
    device_name: str | None,
    direction: str | None,
    grep: str | None,
    opcode: int | None,
    port: int | None,
    protocol: int | None,
    source_ip: str | None,
) -> list[str]:
    filters = []
    if device_ip:
        filters.append(f"device_ip={device_ip}")
    if source_ip:
        filters.append(f"src={source_ip}")
    if destination_ip:
        filters.append(f"dst={destination_ip}")
    if port is not None:
        filters.append(f"port={port}")
    if device_name:
        filters.append(f"device={device_name}")
    if opcode is not None:
        filters.append(f"opcode=0x{opcode:04X}")
    if protocol is not None:
        filters.append(f"protocol=0x{protocol:04X}")
    if direction:
        filters.append(f"direction={direction}")
    if grep:
        filters.append(f"grep={grep}")
    if after:
        filters.append(f"after={after}")
    if before:
        filters.append(f"before={before}")
    return filters


def _packet_detail_lines(packet_id: int, packet: dict, payload: bytes, raw: bool) -> list[str]:
    timestamp_ns = int(packet.get("timestamp_ns") or 0)
    lines = [
        f"Packet #{packet_id}",
        f"  Time:      {format_packet_timestamp(timestamp_ns, include_date=True)}",
        f"  Source:    {packet.get('src_ip') or '?'}:{packet.get('src_port') or '?'}",
        f"  Dest:      {packet.get('dst_ip') or '?'}:{packet.get('dst_port') or '?'}",
        f"  Direction: {packet.get('direction') or 'multicast'}",
        f"  Device:    {packet.get('device_ip') or '?'}",
    ]
    packet_interface = packet.get("interface") or ""
    if packet_interface:
        lines.append(f"  Interface: {packet_interface}")
    lines.append(f"  Session:   {packet.get('session_id') or '?'}")
    lines.append(f"  Type:      {packet.get('source_type') or '?'}")
    lines.append(f"  Size:      {len(payload)}B")
    info_str = _label_packet(payload, include_code=True)
    if info_str:
        lines.append(f"  Label:     {info_str}")
    if raw:
        if len(payload) >= 2:
            lines.append(f"  Protocol:  0x{struct.unpack('>H', payload[0:2])[0]:04X}")
        if len(payload) >= 8:
            lines.append(f"  Opcode:    0x{struct.unpack('>H', payload[6:8])[0]:04X}")
        if len(payload) >= 10:
            lines.append(f"  Status:    0x{struct.unpack('>H', payload[8:10])[0]:04X}")
        lines.append("  Payload:")
        lines.append(_hexdump(payload, indent="    "))
    else:
        from netaudio.dante.dissection.rendering import dissect_and_render

        lines.append(dissect_and_render(payload, indent="  "))
    lines.append("")
    return lines


def packet_show(
    packet_id: list[int] = typer.Argument(..., help="Packet ID(s) to display."),
    raw: bool = typer.Option(False, "--raw", help="Plain hex dump instead of annotated dissection."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    lines = []
    packets = []
    store = PacketStore(db_path=resolved_db)
    try:
        for pid in packet_id:
            pkt = store.get_packet(pid)
            if not pkt:
                typer.echo(f"Packet #{pid}: not found", err=True)
                continue
            payload = pkt.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            lines.extend(_packet_detail_lines(pid, pkt, payload, raw))
            packets.append(packet_row_data(pkt))
    finally:
        store.close()
    emit_report(lines, packets)


def packet_diff(
    packet_ids: list[int] = typer.Argument(..., help="Two or more packet IDs to compare."),
    full: bool = typer.Option(
        False, "--full", help="Show full hex dump with diffs highlighted, not just changed bytes."
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Byte-level diff of two or more packets."""
    if len(packet_ids) < 2:
        typer.echo("Need at least 2 packet IDs to diff.", err=True)
        raise typer.Exit(1)

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    store = PacketStore(db_path=resolved_db)
    try:
        packets = []
        for pid in packet_ids:
            pkt = store.get_packet(pid)
            if not pkt:
                typer.echo(f"Packet #{pid}: not found", err=True)
                raise typer.Exit(1)
            payload = pkt.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            packets.append((pid, pkt, payload))

        for pid, pkt, payload in packets:
            timestamp_ns = int(pkt.get("timestamp_ns") or 0)
            timestamp = datetime.datetime.fromtimestamp(timestamp_ns / 1e9)
            timestamp_str = timestamp.strftime("%H:%M:%S.%f")[:-3]
            src = _format_endpoint(pkt.get("src_ip"), pkt.get("src_port"))
            dst = _format_endpoint(pkt.get("dst_ip"), pkt.get("dst_port"))
            direction = pkt.get("direction") or "multicast"
            label = _label_packet(payload, include_code=True)
            print(f"  #{pid}  {timestamp_str}  {src} -> {dst}  {direction}  {len(payload)}B  {label}")

        max_length = max(len(payload) for _, _, payload in packets)
        reference_payload = packets[0][2]

        differing_offsets = set()
        for _, _, payload in packets[1:]:
            for offset in range(max_length):
                reference_byte = reference_payload[offset] if offset < len(reference_payload) else None
                compare_byte = payload[offset] if offset < len(payload) else None
                if reference_byte != compare_byte:
                    differing_offsets.add(offset)

        if not differing_offsets:
            print("\n  Payloads are identical.")
            return

        print(f"\n  {len(differing_offsets)} bytes differ (of {max_length} total)")

        if full:
            _print_diff_full(packets, differing_offsets, max_length)
        else:
            _print_diff_compact(packets, differing_offsets, max_length)

    finally:
        store.close()


def _state_diff_window(before_time: str, after_time: str) -> tuple[int, int]:
    before_ns = _parse_time_to_ns(before_time)
    after_ns = _parse_time_to_ns(after_time)
    if before_ns is None:
        typer.echo(f"Invalid --before time format: {before_time} (use HH:MM:SS)", err=True)
        raise typer.Exit(1)
    if after_ns is None:
        typer.echo(f"Invalid --after time format: {after_time} (use HH:MM:SS)", err=True)
        raise typer.Exit(1)
    if before_ns >= after_ns:
        typer.echo("--before must be earlier than --after", err=True)
        raise typer.Exit(1)
    return before_ns, after_ns


def packet_state_diff(
    device_ip: str = typer.Option(..., "--device-ip", help="Device IP address."),
    before_time: str = typer.Option(
        ..., "--before", help="Time before state change (HH:MM:SS). Packets before this time."
    ),
    after_time: str = typer.Option(..., "--after", help="Time after state change (HH:MM:SS). Packets after this time."),
    ignore_volatile: bool = typer.Option(
        True,
        "--ignore-volatile/--no-ignore-volatile",
        help="Exclude known volatile header bytes (transaction_id, sequence).",
    ),
    ignore_jitter: bool = typer.Option(
        True,
        "--ignore-jitter/--no-ignore-jitter",
        help="Exclude bytes that vary within the same time window (counters/timestamps).",
    ),
    direction: Optional[str] = typer.Option(
        "response", "--direction", help="Packet direction filter (default: response)."
    ),
    opcode: Optional[str] = typer.Option(None, "--opcode", help="Filter to specific opcode (hex like 0x2000)."),
    full: bool = typer.Option(False, "--full", help="Show full hex dump with diffs highlighted."),
    session: Optional[str] = typer.Option(None, "--session", help="Session reference (ID, name, latest, active)."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Diff device state between two time windows, grouped by opcode.

    Finds all response packets from a device in both time windows, groups them
    by opcode, and shows only the stable byte differences — filtering out known
    volatile bytes (transaction IDs, sequence counters) and bytes that jitter
    between consecutive same-state packets.
    """
    before_ns, after_ns = _state_diff_window(before_time, after_time)
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    resolved_opcode = _parse_int_option(opcode, "--opcode")

    resolved_direction = direction
    if resolved_direction == "multicast":
        resolved_direction = "__null__"

    store = PacketStore(db_path=resolved_db)
    try:
        session_id = None
        if session is not None:
            session_id, _ = _resolve_session_reference(
                store,
                session_id=None,
                session=session,
                default_selector="latest",
            )

        before_rows = store.search_packets(
            PacketQuery(
                device_ip=device_ip,
                direction=resolved_direction,
                end_ns=before_ns,
                opcode=resolved_opcode,
                session_id=session_id,
            )
        )
        after_rows = store.search_packets(
            PacketQuery(
                device_ip=device_ip,
                direction=resolved_direction,
                opcode=resolved_opcode,
                session_id=session_id,
                start_ns=after_ns,
            )
        )

        before_by_opcode: dict[str, list[tuple[int, bytes]]] = {}
        after_by_opcode: dict[str, list[tuple[int, bytes]]] = {}

        for row in before_rows:
            payload = row.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            key = _opcode_key(payload)
            if key is None:
                continue
            before_by_opcode.setdefault(key, []).append((int(row["id"]), payload))

        for row in after_rows:
            payload = row.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            key = _opcode_key(payload)
            if key is None:
                continue
            after_by_opcode.setdefault(key, []).append((int(row["id"]), payload))

        common_opcodes = sorted(set(before_by_opcode.keys()) & set(after_by_opcode.keys()))

        before_ts = datetime.datetime.fromtimestamp(before_ns / 1e9).strftime("%H:%M:%S")
        after_ts = datetime.datetime.fromtimestamp(after_ns / 1e9).strftime("%H:%M:%S")

        print(f"State diff for {device_ip}")
        print(
            f"  before window: ≤ {before_ts} ({sum(len(v) for v in before_by_opcode.values())} packets, {len(before_by_opcode)} opcodes)"
        )
        print(
            f"  after  window: ≥ {after_ts} ({sum(len(v) for v in after_by_opcode.values())} packets, {len(after_by_opcode)} opcodes)"
        )
        print(f"  common opcodes: {len(common_opcodes)}")

        before_only = sorted(set(before_by_opcode.keys()) - set(after_by_opcode.keys()))
        after_only = sorted(set(after_by_opcode.keys()) - set(before_by_opcode.keys()))
        if before_only:
            print(f"  before-only opcodes: {', '.join(before_only)}")
        if after_only:
            print(f"  after-only opcodes: {', '.join(after_only)}")

        diff_count = 0
        identical_count = 0

        for opcode_label in common_opcodes:
            before_payloads = before_by_opcode[opcode_label]
            after_payloads = after_by_opcode[opcode_label]

            sample_payload = before_payloads[0][1]
            protocol_type = _classify_protocol(sample_payload)
            volatile_offsets = _get_volatile_offsets(protocol_type) if ignore_volatile else set()

            before_only_payloads = [payload for _, payload in before_payloads]
            after_only_payloads = [payload for _, payload in after_payloads]

            before_jitter = (
                _detect_jitter_offsets(before_only_payloads)
                if ignore_jitter and len(before_only_payloads) > 1
                else set()
            )
            after_jitter = (
                _detect_jitter_offsets(after_only_payloads) if ignore_jitter and len(after_only_payloads) > 1 else set()
            )
            jitter_offsets = before_jitter | after_jitter

            had_diff = _state_diff_print_opcode_diff(
                opcode_label,
                before_payloads,
                after_payloads,
                volatile_offsets,
                jitter_offsets,
                full,
            )
            if had_diff:
                diff_count += 1
            else:
                identical_count += 1

        print(f"\n  {diff_count} opcodes with stable differences, {identical_count} identical")

    finally:
        store.close()


def clear(
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Delete the capture database."""
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    db_path = Path(resolved_db)

    for suffix in ("", "-shm", "-wal"):
        target = Path(str(db_path) + suffix)
        if target.exists():
            target.unlink()

    typer.echo(f"Deleted {db_path}", err=True)
