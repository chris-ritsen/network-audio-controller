from __future__ import annotations

import json
import socket
import time
from typing import Optional

import typer

from netaudio.capture.daemon import REDIS_ERRORS
from netaudio.capture.display import (
    _follow_session_timeline,
    _print_marker_row,
    _print_session_evidence,
    _print_timeline_header,
)
from netaudio.capture.packets import _print_packet_table_header
from netaudio.capture.sessions import _default_session_name
from netaudio.commands.capture.live import _resolve_redis_for_capture
from netaudio.commands.capture.options import (
    _as_dict,
    _coalesce,
    _load_capture_profile,
    _normalize_marker_label,
    _normalize_marker_type,
    _parse_int_option,
    _parse_time_filter,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_marker_window,
    _resolve_redis_from_config,
    _resolve_session_reference,
)
from netaudio.commands.capture.reporting import emit_report, packet_row_data, structured_output_selected
from netaudio.dante.packet_store import PacketQuery, PacketStore
from netaudio.icons import icon


def session_start(
    name: Optional[str] = typer.Option(None, "--name", help="Session name (defaults to timestamped name)."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    description: Optional[str] = typer.Option(None, "--description", help="Session description."),
    source_host: Optional[str] = typer.Option(None, "--source-host", help="Host that owns this session."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_name = name or _default_session_name()
        resolved_host = source_host or socket.gethostname()
        session_id = store.start_session(
            name=resolved_name,
            source_host=resolved_host,
            description=description,
        )
        store.add_marker(
            session_id=session_id,
            marker_type="system",
            label=_normalize_marker_label("session_started"),
            source_host=resolved_host,
        )
        emit_report(
            [f"{icon('session')}Capture: Started session #{session_id} ({resolved_name})"],
            {"id": session_id, "name": resolved_name, "source_host": resolved_host},
        )
    finally:
        store.close()


def session_stop(
    id: Optional[int] = typer.Option(None, "--id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to active.",
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    description: Optional[str] = typer.Option(None, "--description", help="Optional stop summary."),
    source_host: Optional[str] = typer.Option(None, "--source-host", help="Host recording stop marker."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=id,
            session=session,
            default_selector="active",
        )
        resolved_host = source_host or socket.gethostname()
        store.add_marker(
            session_id=resolved_session_id,
            marker_type="system",
            label=_normalize_marker_label("session_stopped"),
            note=description,
            source_host=resolved_host,
        )
        ok = store.end_session(resolved_session_id, description=description)
        if not ok:
            typer.echo(f"Capture: Session #{resolved_session_id} not found.", err=True)
            raise typer.Exit(1)

        from netaudio.dante.protocol_verifier import export_session_bundle

        bundle_path = export_session_bundle(store, resolved_session_id)
        evidence_count = store.get_session_evidence_count(resolved_session_id)
    finally:
        store.close()
    emit_report(
        [
            f"{icon('session')}Capture: Ended session #{resolved_session_id}",
            f"{icon('packet')}Capture: Exported bundle: {bundle_path}",
        ],
        {"bundle": str(bundle_path), "evidence_packets": evidence_count, "id": resolved_session_id},
    )
    if evidence_count == 0:
        typer.echo(
            "Capture: Warning: no packet evidence was tagged; this is a local draft, not a promotable provenance bundle.",
            err=True,
        )


def session_rename(
    name: str = typer.Argument(..., help="New session name."),
    id: Optional[int] = typer.Option(None, "--id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to active.",
    ),
    description: Optional[str] = typer.Option(None, "--description", help="Update description."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Rename a capture session."""
    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=id,
            session=session,
            default_selector="active",
        )
        ok = store.update_session(
            resolved_session_id,
            name=name,
            description=description,
        )
        if not ok:
            typer.echo(f"Capture: Session #{resolved_session_id} not found.", err=True)
            raise typer.Exit(1)
        emit_report(
            [f"{icon('name')}Capture: Session #{resolved_session_id} renamed to '{name}'"],
            {"description": description, "id": resolved_session_id, "name": name},
        )
    finally:
        store.close()


def _session_summary_lines(session_id: int, session_row: dict, total_packets: int, marker_count: int) -> list[str]:
    return [
        f"Session #{session_id}",
        f"  Name:        {session_row.get('name') or ''}",
        f"  Category:    {session_row.get('category') or ''}",
        f"  Source Host: {session_row.get('source_host') or ''}",
        f"  Started:     {session_row.get('started_iso') or ''}",
        f"  Ended:       {session_row.get('ended_iso') or ''}",
        f"  Packets:     {total_packets}",
        f"  Description: {session_row.get('description') or ''}",
        f"  Markers:     {marker_count}",
    ]


def _session_show_data(
    session_id: int, session_row: dict, total_packets: int, markers: list[dict], show_notes: bool
) -> dict:
    marker_list = []
    for marker_row in markers:
        entry = {
            "id": int(marker_row["id"]),
            "timestamp": marker_row.get("timestamp_iso") or "",
            "type": marker_row.get("marker_type") or "",
            "label": marker_row.get("label") or "",
            "summary": marker_row.get("summary") or "",
        }
        if show_notes:
            entry["note"] = marker_row.get("note") or ""
        entry["data"] = marker_row.get("data")
        marker_list.append(entry)
    return {
        "id": session_id,
        "name": session_row.get("name") or "",
        "category": session_row.get("category") or "",
        "source_host": session_row.get("source_host") or "",
        "started": session_row.get("started_iso") or "",
        "ended": session_row.get("ended_iso") or "",
        "packets": total_packets,
        "description": session_row.get("description") or "",
        "markers": marker_list,
    }


def _describe_session_packet_filters(
    *,
    after: str | None,
    before: str | None,
    device_ip: str | None,
    direction: str | None,
    opcode: int | None,
    protocol: int | None,
) -> list[str]:
    filters = []
    if device_ip:
        filters.append(f"device={device_ip}")
    if opcode is not None:
        filters.append(f"opcode=0x{opcode:04X}")
    if protocol is not None:
        filters.append(f"protocol=0x{protocol:04X}")
    if direction:
        filters.append(f"direction={direction}")
    if after:
        filters.append(f"after={after}")
    if before:
        filters.append(f"before={before}")
    return filters


def session_list(
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    limit: int = typer.Option(25, "--limit", help="Number of sessions to show."),
    category: Optional[str] = typer.Option(
        None, "--category", help="Filter by category (experiment, diagnostic, etc)."
    ),
    has_evidence: bool = typer.Option(
        False, "--has-evidence", "--evidence", help="Only show sessions with evidence-tagged packets."
    ),
    no_evidence: bool = typer.Option(
        False, "--no-evidence", help="Only show sessions with no evidence-tagged packets."
    ),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.cli_support.output import output_table

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        sessions = store.list_sessions(limit=limit, category=category)
        if not sessions:
            emit_report(["Capture: No sessions."], [])
            return
        headers = ["ID", "Started", "Ended", "Packets", "Evidence", "Category", "Name"]
        rows = []
        json_data = []
        for session in sessions:
            session_id = int(session["id"])
            packets = store.get_session_packet_count(session_id)
            evidence = store.get_session_evidence_count(session_id)
            started = session.get("started_iso") or ""
            ended = session.get("ended_iso") or ""
            name = session.get("name") or ""
            session_category = session.get("category") or ""

            if has_evidence and evidence == 0:
                continue

            if no_evidence and evidence > 0:
                continue

            rows.append([str(session_id), started, ended, str(packets), str(evidence), session_category, name])
            json_data.append(
                {
                    "id": session_id,
                    "name": name,
                    "category": session_category,
                    "started": started,
                    "ended": ended,
                    "packets": packets,
                    "evidence": evidence,
                    "source_host": session.get("source_host") or "",
                    "description": session.get("description") or "",
                }
            )
        output_table(headers, rows, json_data=json_data, title="Sessions")

        from netaudio.cli import state as cli_state

        if cli_state.dissect:
            _print_session_evidence(store, sessions, has_evidence, no_evidence)
    finally:
        store.close()


def session_show(
    id: Optional[int] = typer.Option(None, "--id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to latest.",
    ),
    follow_mode: bool = typer.Option(
        False, "--follow", "-f", help="Tail the session timeline, polling for new markers."
    ),
    poll_interval: float = typer.Option(1.0, "--poll", help="Poll interval in seconds for --follow mode."),
    marker_type: Optional[list[str]] = typer.Option(None, "--type", help="Filter by marker type (repeatable)."),
    after: Optional[str] = typer.Option(
        None, "--after", help="Show markers after this time (HH:MM:SS or ISO timestamp)."
    ),
    before: Optional[str] = typer.Option(
        None, "--before", help="Show markers before this time (HH:MM:SS or ISO timestamp)."
    ),
    grep: Optional[str] = typer.Option(
        None, "--grep", help="Filter markers matching string in label, summary, or note."
    ),
    brief: bool = typer.Option(False, "--brief", help="One-line per marker: summary or label only."),
    no_notes: bool = typer.Option(False, "--no-notes", help="Hide full notes (show summary if available)."),
    packets: bool = typer.Option(False, "--packets", help="Show evidence packet dumps and per-marker packet counts."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Maximum number of markers to show."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.cli import state as cli_state

    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, session_row = _resolve_session_reference(
            store,
            session_id=id,
            session=session,
            default_selector="active" if follow_mode else "latest",
        )
        after_ns = _parse_time_filter(after, store, resolved_session_id)
        before_ns = _parse_time_filter(before, store, resolved_session_id)

        normalized_types = None
        if marker_type:
            normalized_types = [_normalize_marker_type(t, strict=False) for t in marker_type]

        markers = store.get_markers(
            resolved_session_id,
            marker_types=normalized_types,
            after_ns=after_ns,
            before_ns=before_ns,
            grep=grep,
            limit=limit,
        )
        total_packets = store.get_session_packet_count(resolved_session_id)

        show_notes = not brief and not no_notes
        show_packets = not brief and packets

        emit_report(
            _session_summary_lines(resolved_session_id, session_row, total_packets, len(markers)),
            _session_show_data(resolved_session_id, session_row, total_packets, markers, show_notes),
        )
        if structured_output_selected():
            return

        if markers or follow_mode:
            print("\nTimeline:")
            _print_timeline_header(show_window_packets=packets)
            next_times = [m["timestamp_ns"] for m in markers[1:]] + [None]
            for m, next_ts in zip(markers, next_times):
                _print_marker_row(
                    m,
                    next_ts,
                    resolved_session_id,
                    store,
                    use_dissect=cli_state.dissect,
                    show_notes=show_notes,
                    show_packets=show_packets,
                    show_window_packets=packets,
                    brief=brief,
                )

        if follow_mode:
            _follow_session_timeline(
                store,
                resolved_session_id,
                last_marker_id=int(markers[-1]["id"]) if markers else 0,
                poll_interval=poll_interval,
                use_dissect=cli_state.dissect,
                show_notes=show_notes,
                show_packets=show_packets,
                show_window_packets=packets,
                brief=brief,
            )
    finally:
        store.close()


def session_packets(
    id: Optional[int] = typer.Option(None, "--id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to latest.",
    ),
    device_ip: Optional[str] = typer.Option(
        None, "--device-ip", help="Filter packets where src or dst IP matches this device."
    ),
    from_label: Optional[str] = typer.Option(None, "--from-label", help="Start at first marker with this label."),
    to_label: Optional[str] = typer.Option(None, "--to-label", help="End at last marker with this label."),
    opcode: Optional[str] = typer.Option(None, "--opcode", help="Filter by opcode (hex like 0x2010 or decimal)."),
    protocol: Optional[str] = typer.Option(
        None, "--protocol", help="Filter by protocol ID (hex like 0x2729 or decimal)."
    ),
    direction: Optional[str] = typer.Option(
        None, "--direction", help="Filter by direction: request, response, or multicast."
    ),
    after: Optional[str] = typer.Option(
        None, "--after", help="Show packets after this time (HH:MM:SS or HH:MM:SS.fff)."
    ),
    before: Optional[str] = typer.Option(
        None, "--before", help="Show packets before this time (HH:MM:SS or HH:MM:SS.fff)."
    ),
    limit: int = typer.Option(200, "--limit", min=1, max=5000, help="Max packets to show."),
    offset: int = typer.Option(0, "--offset", min=0, help="Packet offset within filtered result."),
    descending: bool = typer.Option(False, "--descending", help="Show newest packets first."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.capture.daemon import PacketLine, _print_packet_line
    from netaudio.cli import state as cli_state

    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    resolved_opcode = _parse_int_option(opcode, "--opcode")
    resolved_protocol = _parse_int_option(protocol, "--protocol")

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=id,
            session=session,
            default_selector="latest",
        )
        start_ns, end_ns = _resolve_marker_window(
            store,
            session_id=resolved_session_id,
            from_label=from_label,
            to_label=to_label,
        )

        time_start_ns = _parse_time_filter(after, store, resolved_session_id)
        time_end_ns = _parse_time_filter(before, store, resolved_session_id)

        if time_start_ns is not None:
            start_ns = max(start_ns, time_start_ns) if start_ns is not None else time_start_ns

        if time_end_ns is not None:
            end_ns = min(end_ns, time_end_ns) if end_ns is not None else time_end_ns

        resolved_direction = direction
        if resolved_direction == "multicast":
            resolved_direction = "__null__"

        total = store.get_session_packet_count_filtered(
            PacketQuery(
                session_id=resolved_session_id,
                device_ip=device_ip,
                start_ns=start_ns,
                end_ns=end_ns,
                opcode=resolved_opcode,
                protocol_id=resolved_protocol,
                direction=resolved_direction,
            )
        )
        rows = store.get_session_packets(
            PacketQuery(
                session_id=resolved_session_id,
                device_ip=device_ip,
                start_ns=start_ns,
                end_ns=end_ns,
                opcode=resolved_opcode,
                protocol_id=resolved_protocol,
                direction=resolved_direction,
                limit=limit,
                offset=offset,
                ascending=not descending,
            )
        )

    finally:
        store.close()

    filters = _describe_session_packet_filters(
        after=after,
        before=before,
        device_ip=device_ip,
        direction=direction,
        opcode=resolved_opcode,
        protocol=resolved_protocol,
    )
    lines = [
        f"Capture: Session #{resolved_session_id} packets={total} shown={len(rows)} (limit={limit} offset={offset})"
    ]
    if filters:
        lines.append(f"Capture: Filters: {', '.join(filters)}")
    marker_window = None
    if from_label or to_label:
        marker_window = {
            "from": _normalize_marker_label(from_label) if from_label else None,
            "to": _normalize_marker_label(to_label) if to_label else None,
        }
        lines.append(f"Capture: Marker window from={marker_window['from'] or '-'} to={marker_window['to'] or '-'}")
    emit_report(
        lines,
        {
            "filters": filters,
            "limit": limit,
            "marker_window": marker_window,
            "offset": offset,
            "packets": [packet_row_data(row) for row in rows],
            "session_id": resolved_session_id,
            "shown": len(rows),
            "total": total,
        },
    )
    if structured_output_selected():
        return
    _print_packet_table_header()
    for row in rows:
        _print_packet_line(PacketLine.from_row(row), dump=dump, dissect_mode=cli_state.dissect)


def marker(
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to active.",
    ),
    label: str = typer.Option(..., "--label", help="Marker label (normalized to lowercase snake_case)."),
    marker_type: str = typer.Option(
        "observation",
        "--type",
        help="Marker type: action|observation|state_change|system|hypothesis",
    ),
    summary: Optional[str] = typer.Option(None, "--summary", help="One-line summary (shown in brief mode)."),
    note: Optional[str] = typer.Option(None, "--note", help="Optional marker note (full detail)."),
    data: Optional[str] = typer.Option(None, "--data", help="Optional JSON object payload."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    source_host: Optional[str] = typer.Option(None, "--source-host", help="Host that generated this marker."),
    ingress_stream: Optional[str] = typer.Option(
        None, "--ingress-stream", help="Redis stream key to publish marker events."
    ),
    ingress_only: bool = typer.Option(
        False, "--ingress-only", help="Only publish to Redis stream (skip local DB write)."
    ),
    redis_host: Optional[str] = typer.Option(None, "--redis-host", help="Redis host."),
    redis_port: Optional[int] = typer.Option(None, "--redis-port", help="Redis port."),
    redis_db: Optional[int] = typer.Option(None, "--redis-db", help="Redis DB."),
    redis_password: Optional[str] = typer.Option(None, "--redis-password", help="Redis password."),
    redis_socket: Optional[str] = typer.Option(None, "--redis-socket", help="Redis UNIX socket path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    payload = None
    if data:
        try:
            parsed = json.loads(data)
            if isinstance(parsed, dict):
                payload = parsed
            else:
                payload = {"value": parsed}
        except ValueError as exception:
            typer.echo(f"Capture: invalid --data JSON: {exception}", err=True)
            raise typer.Exit(1)
    profile_cfg, _ = _load_capture_profile(config, profile)
    capture_cfg = _as_dict(profile_cfg.get("capture"))
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    (
        resolved_redis_host,
        resolved_redis_port,
        resolved_redis_db,
        resolved_redis_password,
        resolved_redis_socket,
    ) = _resolve_redis_from_config(
        profile_cfg=profile_cfg,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_socket=redis_socket,
    )
    normalized_type = _normalize_marker_type(marker_type, strict=True)
    normalized_label = _normalize_marker_label(label)
    resolved_session_id: int
    if ingress_only and session_id is not None and not session:
        resolved_session_id = int(session_id)
    else:
        store = PacketStore(db_path=resolved_db)
        try:
            resolved_session_id, _ = _resolve_session_reference(
                store,
                session_id=session_id,
                session=session,
                default_selector="active",
            )
        finally:
            store.close()

    resolved_host = source_host or socket.gethostname()
    marker_ts = time.time_ns()
    resolved_ingress_stream = _coalesce(ingress_stream, capture_cfg.get("ingress_stream"))
    if ingress_only and not resolved_ingress_stream:
        typer.echo("Capture: --ingress-only requires --ingress-stream (or capture.ingress_stream in config).", err=True)
        raise typer.Exit(1)
    lines = []
    marker_id = None
    if not ingress_only:
        store = PacketStore(db_path=resolved_db)
        try:
            marker_id = store.add_marker(
                session_id=resolved_session_id,
                marker_type=normalized_type,
                label=normalized_label,
                summary=summary,
                note=note,
                source_host=resolved_host,
                data=payload,
                timestamp_ns=marker_ts,
            )
            lines.append(f"Capture: Added marker #{marker_id} to session #{resolved_session_id}")
        finally:
            store.close()

    if resolved_ingress_stream:
        client = _resolve_redis_for_capture(
            redis_host=resolved_redis_host,
            redis_port=resolved_redis_port,
            redis_db=resolved_redis_db,
            redis_password=resolved_redis_password,
            redis_socket=resolved_redis_socket,
        )
        event = {
            "event": "marker",
            "source_host": resolved_host,
            "timestamp_ns": str(marker_ts),
            "session_id": str(resolved_session_id),
            "marker_type": normalized_type,
            "label": normalized_label,
            "summary": str(summary or ""),
            "note": str(note or ""),
            "data_json": json.dumps(payload, sort_keys=True) if payload else "",
            "already_stored": "0" if ingress_only else "1",
        }
        try:
            client.xadd(str(resolved_ingress_stream), event, maxlen=200000, approximate=True)
        except REDIS_ERRORS as exception:
            typer.echo(f"Capture: failed to publish marker: {exception}", err=True)
            raise typer.Exit(1)
        lines.append(f"Capture: Published marker to stream {resolved_ingress_stream}")
    emit_report(
        lines,
        {
            "data": payload,
            "id": marker_id,
            "label": normalized_label,
            "note": note,
            "published_stream": str(resolved_ingress_stream) if resolved_ingress_stream else None,
            "session_id": resolved_session_id,
            "source_host": resolved_host,
            "summary": summary,
            "timestamp_ns": marker_ts,
            "type": normalized_type,
        },
    )
