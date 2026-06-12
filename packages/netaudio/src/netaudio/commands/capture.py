from __future__ import annotations

import asyncio
import datetime
import json
import os
import signal
import socket
import struct
import sys
import time
import traceback
from collections import deque
from pathlib import Path
from typing import Optional

import typer

from netaudio.common.app_config import settings as app_settings
from netaudio.common.app_config import get_available_interfaces
from netaudio.dante.packet_store import PacketStore
from netaudio.dante.tshark_capture import TsharkCapture
from netaudio.capture import daemon as capture_daemon
from netaudio.capture.analysis import (
    _classify_protocol,
    _detect_jitter_offsets,
    _get_volatile_offsets,
    _opcode_key,
    _parse_time_to_ns,
)
from netaudio.capture.daemon import (
    CaptureDaemon,
    _get_redis_client,
    _print_packet_line,
)
from netaudio.capture.display import (
    _emdash,
    _follow_session_timeline,
    _print_diff_compact,
    _print_diff_full,
    _print_marker_row,
    _print_session_evidence,
    _print_timeline_header,
    _state_diff_print_opcode_diff,
)
from netaudio.capture.interfaces import _default_interface
from netaudio.capture.sessions import _default_session_name
from netaudio.commands.capture_helpers import (
    _as_dict,
    _coalesce,
    _format_endpoint,
    _hexdump,
    _label_packet,
    _load_capture_profile,
    _normalize_marker_label,
    _normalize_marker_type,
    _packet_fingerprint,
    _parse_config_bool,
    _parse_config_int,
    _parse_int_option,
    _parse_optional_int,
    _parse_time_filter,
    _print_packet_table_header,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_marker_window,
    _resolve_redis_from_config,
    _resolve_session_reference,
)

from netaudio.icons import icon


async def _replay_packet(
    packet_id: int, store: PacketStore, interface: str, tshark_duration: float, dump: bool = False
):
    packet = store.get_packet(packet_id)
    if not packet:
        print(f"Error: Packet #{packet_id} not found in database.", file=sys.stderr)
        raise typer.Exit(1)

    payload = packet["payload"]
    if isinstance(payload, str):
        payload = bytes.fromhex(payload)

    destination_ip = packet["dst_ip"]
    destination_port = packet["dst_port"]

    if not destination_ip or not destination_port:
        print(f"Error: Packet #{packet_id} has no destination address.", file=sys.stderr)
        raise typer.Exit(1)

    if destination_ip.startswith("224."):
        print(
            f"Error: Packet #{packet_id} targets multicast address {destination_ip}, nothing to replay.",
            file=sys.stderr,
        )
        raise typer.Exit(1)

    print(f"{icon('packet')}Replaying packet #{packet_id}")
    print(f"  Target:  {destination_ip}:{destination_port}")
    print(f"  Size:    {len(payload)} bytes")
    info = _label_packet(payload, include_code=True)
    if info:
        print(f"  Label:   {info}")
    print()

    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    probe.connect((destination_ip, destination_port))
    local_ip = probe.getsockname()[0]
    probe.close()

    tshark_task = None
    received_packets = []

    if TsharkCapture.is_available():
        capture = TsharkCapture(
            packet_store=store,
            interface=interface,
            device_ips=[destination_ip],
        )

        async def on_tshark_packet(captured_packet_id, fields):
            if fields.get("direction") == "request" and fields.get("src_ip") == local_ip:
                return
            received_packets.append((captured_packet_id, fields))

        tshark_task = asyncio.create_task(capture.start(on_packet=on_tshark_packet))
        await asyncio.sleep(0.3)
    else:
        print(
            "Warning: tshark not found -- multicast responses won't be captured.\n"
            "  Install with: brew install --cask wireshark",
            file=sys.stderr,
        )

    send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    send_socket.settimeout(0.5)

    send_timestamp = time.time_ns()
    send_socket.sendto(payload, (destination_ip, destination_port))

    local_port = send_socket.getsockname()[1]

    sent_id = store.store_packet(
        payload=payload,
        source_type="replay_request",
        src_ip=local_ip,
        src_port=local_port,
        dst_ip=destination_ip,
        dst_port=destination_port,
        device_ip=destination_ip,
        direction="request",
        timestamp_ns=send_timestamp,
    )

    print("Packets")
    _print_packet_table_header()

    if sent_id:
        _print_packet_line(
            sent_id,
            send_timestamp,
            local_ip,
            local_port,
            destination_ip,
            destination_port,
            "request",
            payload,
            dump=dump,
        )

    try:
        reply_data, reply_addr = send_socket.recvfrom(4096)
        reply_timestamp = time.time_ns()
        reply_ip, reply_port = reply_addr

        reply_id = store.store_packet(
            payload=reply_data,
            source_type="replay_response",
            src_ip=reply_ip,
            src_port=reply_port,
            dst_ip=local_ip,
            dst_port=local_port,
            device_ip=reply_ip,
            direction="response",
            timestamp_ns=reply_timestamp,
        )

        if reply_id:
            _print_packet_line(
                reply_id,
                reply_timestamp,
                reply_ip,
                reply_port,
                local_ip,
                local_port,
                "response",
                reply_data,
                dump=dump,
            )
    except socket.timeout:
        print("  (no unicast reply within 500ms)")

    send_socket.close()

    if tshark_task:
        await asyncio.sleep(tshark_duration)
        tshark_task.cancel()
        try:
            await tshark_task
        except asyncio.CancelledError:
            pass

        for captured_packet_id, fields in received_packets:
            _print_packet_line(
                captured_packet_id,
                fields.get("timestamp_ns", time.time_ns()),
                fields.get("src_ip", "?"),
                fields.get("src_port", 0),
                fields.get("dst_ip", "?"),
                fields.get("dst_port", 0),
                fields.get("direction"),
                fields.get("payload", b""),
                dump=dump,
            )

    total = 1 + (1 if "reply_id" in dir() else 0) + len(received_packets)
    print(f"\n  {total} packet(s) total")


def _resolve_redis_for_capture(
    redis_host: Optional[str],
    redis_port: Optional[int],
    redis_db: Optional[int],
    redis_password: Optional[str],
    redis_socket: Optional[str],
):
    client = _get_redis_client(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
        socket_path=redis_socket,
    )
    if client is None:
        detail = f" ({capture_daemon._LAST_REDIS_ERROR})" if capture_daemon._LAST_REDIS_ERROR else ""
        raise typer.Exit(f"Redis is not available with the provided settings{detail}.")
    return client


app = typer.Typer(help="Capture and replay Dante traffic.", no_args_is_help=True)
session_app = typer.Typer(help="Manage capture sessions.", no_args_is_help=True)
app.add_typer(session_app, name="session")
packet_app = typer.Typer(help="Inspect individual captured packets.", no_args_is_help=True)
app.add_typer(packet_app, name="packet")


@app.command()
def live(
    interface: Optional[str] = typer.Option(None, "-i", "--interface", help="Network interface for capture."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    tshark: bool = typer.Option(True, "--tshark/--no-tshark", help="Enable tshark capture."),
    multicast: bool = typer.Option(True, "--multicast/--no-multicast", help="Enable multicast listener."),
    device: Optional[list[str]] = typer.Option(None, "--device", help="Filter to specific device name(s) or IP(s)."),
    opcode: Optional[list[str]] = typer.Option(None, "--opcode", help="Filter to specific opcode(s)."),
    export_dir: Optional[str] = typer.Option(
        None, "--export-dir", help="Directory to export fixture files on shutdown."
    ),
    show: bool = typer.Option(True, "--live/--no-live", help="Show live packet feed."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    metering: bool = typer.Option(False, "--metering", help="Include metering traffic (port 8751)."),
    tcp: bool = typer.Option(False, "--tcp", help="Include TCP traffic to/from devices."),
    session_id: Optional[int] = typer.Option(
        None, "--session-id", help="Attach packets to an existing capture session ID."
    ),
    session_name: Optional[str] = typer.Option(
        None, "--session-name", help="Create a new capture session with this name."
    ),
    redis_host: Optional[str] = typer.Option(None, "--redis-host", help="Redis host for device discovery/relay."),
    redis_port: Optional[int] = typer.Option(None, "--redis-port", help="Redis port for device discovery/relay."),
    redis_db: Optional[int] = typer.Option(None, "--redis-db", help="Redis DB index for device discovery/relay."),
    redis_password: Optional[str] = typer.Option(
        None, "--redis-password", help="Redis password for device discovery/relay."
    ),
    redis_socket: Optional[str] = typer.Option(
        None, "--redis-socket", help="Redis UNIX socket path for device discovery/relay."
    ),
    relay_stream: Optional[str] = typer.Option(
        None, "--relay-stream", help="Redis stream key to publish capture events."
    ),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.cli import state as cli_state

    _require_positive_session_id(session_id, "--session-id")

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
    if interface:
        resolved_interface = interface
        interface_source = "--interface flag"
    elif app_settings.interface:
        resolved_interface = app_settings.interface
        interface_source = "NETAUDIO_INTERFACE" if os.environ.get("NETAUDIO_INTERFACE") else "--interface flag"
    elif capture_cfg.get("interface"):
        resolved_interface = capture_cfg["interface"]
        interface_source = "capture config"
    else:
        resolved_interface, interface_source = _default_interface()

    available = get_available_interfaces()
    interface_ip = None
    for iface_name, iface_ip, _ in available:
        if iface_name == resolved_interface:
            interface_ip = iface_ip
            break

    if not interface_ip and resolved_interface != "any":
        available_names = sorted({name for name, _, _ in available})
        print(
            f"Error: Interface '{resolved_interface}' not found or has no IP address.\n"
            f"  Available: {', '.join(available_names)}",
            file=sys.stderr,
        )
        raise typer.Exit(1)

    print(f"{icon('capture')}Capture: Interface {resolved_interface} ({interface_ip}) {_emdash()} {interface_source}")
    resolved_relay_stream = _coalesce(relay_stream, capture_cfg.get("ingress_stream"))

    daemon = CaptureDaemon(
        db_path=resolved_db,
        interface=resolved_interface,
        use_tshark=tshark,
        use_multicast=multicast,
        device_filter=device or [],
        opcode_filter=opcode or [],
        export_dir=export_dir,
        live=show,
        dump=dump,
        dissect=cli_state.dissect,
        metering=metering,
        tcp=tcp,
        session_id=session_id,
        session_name=session_name,
        redis_host=resolved_redis_host,
        redis_port=resolved_redis_port,
        redis_db=resolved_redis_db,
        redis_password=resolved_redis_password,
        redis_socket=resolved_redis_socket,
        relay_stream=str(resolved_relay_stream) if resolved_relay_stream is not None else None,
    )

    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)

    def on_signal(sig, frame):
        print(f"\nCapture: Signal {sig} received, shutting down...")
        daemon.stop_event.set()
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    try:
        asyncio.run(daemon.run())
    except Exception as exception:
        print(f"Capture: Fatal error: {exception}", file=sys.stderr)
        traceback.print_exc()
    finally:
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)


@app.command()
def replay(
    id: int = typer.Option(..., "--id", help="Packet ID to replay."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    interface: Optional[str] = typer.Option(None, "-i", "--interface", help="Network interface for tshark capture."),
    duration: float = typer.Option(2.0, "--duration", help="Seconds to listen for multicast responses."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    resolved_interface = interface or _default_interface()

    try:
        asyncio.run(_replay_packet(id, store, resolved_interface, duration, dump=dump))
    finally:
        store.close()


@session_app.command("start")
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
        print(f"{icon('session')}Capture: Started session #{session_id} ({resolved_name})")
    finally:
        store.close()


@session_app.command("stop")
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
            print(f"Capture: Session #{resolved_session_id} not found.", file=sys.stderr)
            raise typer.Exit(1)
        print(f"{icon('session')}Capture: Ended session #{resolved_session_id}")

        from netaudio.dante.protocol_verifier import export_session_bundle

        session_row = store.get_session(resolved_session_id)
        session_name = session_row["name"] if session_row else f"session_{resolved_session_id}"
        bundle_path = export_session_bundle(store, resolved_session_id)
        print(f"{icon('packet')}Capture: Exported bundle: {bundle_path}")
    finally:
        store.close()


@session_app.command("end", hidden=True)
def session_end(
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
    """Alias for stop."""
    session_stop(
        id=id,
        session=session,
        db=db,
        description=description,
        source_host=source_host,
        config=config,
        profile=profile,
    )


@session_app.command("rename")
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
            print(f"Capture: Session #{resolved_session_id} not found.", file=sys.stderr)
            raise typer.Exit(1)
        print(f"{icon('name')}Capture: Session #{resolved_session_id} renamed to '{name}'")
    finally:
        store.close()


@session_app.command("list")
def session_list(
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    limit: int = typer.Option(25, "--limit", help="Number of sessions to show."),
    category: Optional[str] = typer.Option(None, "--category", help="Filter by category (experiment, diagnostic, etc)."),
    has_evidence: bool = typer.Option(False, "--has-evidence", "--evidence", help="Only show sessions with evidence-tagged packets."),
    no_evidence: bool = typer.Option(False, "--no-evidence", help="Only show sessions with no evidence-tagged packets."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio._common import output_table

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        sessions = store.list_sessions(limit=limit, category=category)
        if not sessions:
            print("Capture: No sessions.")
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
            json_data.append({
                "id": session_id,
                "name": name,
                "category": session_category,
                "started": started,
                "ended": ended,
                "packets": packets,
                "evidence": evidence,
                "source_host": session.get("source_host") or "",
                "description": session.get("description") or "",
            })
        output_table(headers, rows, json_data=json_data, title="Sessions")

        from netaudio.cli import state as cli_state
        if cli_state.dissect:
            _print_session_evidence(store, sessions, has_evidence, no_evidence)
    finally:
        store.close()


@session_app.command("show")
def session_show(
    id: Optional[int] = typer.Option(None, "--id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, exact name, latest, or active). Defaults to latest.",
    ),
    follow_mode: bool = typer.Option(False, "--follow", "-f", help="Tail the session timeline, polling for new markers."),
    poll_interval: float = typer.Option(1.0, "--poll", help="Poll interval in seconds for --follow mode."),
    marker_type: Optional[list[str]] = typer.Option(None, "--type", help="Filter by marker type (repeatable)."),
    after: Optional[str] = typer.Option(None, "--after", help="Show markers after this time (HH:MM:SS or ISO timestamp)."),
    before: Optional[str] = typer.Option(None, "--before", help="Show markers before this time (HH:MM:SS or ISO timestamp)."),
    grep: Optional[str] = typer.Option(None, "--grep", help="Filter markers matching string in label, summary, or note."),
    brief: bool = typer.Option(False, "--brief", help="One-line per marker: summary or label only."),
    no_notes: bool = typer.Option(False, "--no-notes", help="Hide full notes (show summary if available)."),
    packets: bool = typer.Option(False, "--packets", help="Show evidence packet dumps and per-marker packet counts."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Maximum number of markers to show."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio._common import output_single
    from netaudio.cli import OutputFormat, state as cli_state

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

        if cli_state.output_format in (OutputFormat.json, OutputFormat.yaml):
            marker_list = []
            for m in markers:
                entry = {
                    "id": int(m["id"]),
                    "timestamp": m.get("timestamp_iso") or "",
                    "type": m.get("marker_type") or "",
                    "label": m.get("label") or "",
                    "summary": m.get("summary") or "",
                }
                if show_notes:
                    entry["note"] = m.get("note") or ""
                entry["data"] = m.get("data")
                marker_list.append(entry)
            output_single({
                "id": resolved_session_id,
                "name": session_row.get("name") or "",
                "category": session_row.get("category") or "",
                "source_host": session_row.get("source_host") or "",
                "started": session_row.get("started_iso") or "",
                "ended": session_row.get("ended_iso") or "",
                "packets": total_packets,
                "description": session_row.get("description") or "",
                "markers": marker_list,
            })
            return

        print(f"Session #{resolved_session_id}")
        print(f"  Name:        {session_row.get('name') or ''}")
        print(f"  Category:    {session_row.get('category') or ''}")
        print(f"  Source Host: {session_row.get('source_host') or ''}")
        print(f"  Started:     {session_row.get('started_iso') or ''}")
        print(f"  Ended:       {session_row.get('ended_iso') or ''}")
        print(f"  Packets:     {total_packets}")
        print(f"  Description: {session_row.get('description') or ''}")
        print(f"  Markers:     {len(markers)}")

        if markers or follow_mode:
            print("\nTimeline:")
            _print_timeline_header(show_window_packets=packets)
            next_times = [m["timestamp_ns"] for m in markers[1:]] + [None]
            for m, next_ts in zip(markers, next_times):
                _print_marker_row(
                    m, next_ts, resolved_session_id, store,
                    use_dissect=cli_state.dissect,
                    show_notes=show_notes,
                    show_packets=show_packets,
                    show_window_packets=packets,
                    brief=brief,
                )

        if follow_mode:
            _follow_session_timeline(
                store, resolved_session_id,
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


@session_app.command("packets")
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
    protocol: Optional[str] = typer.Option(None, "--protocol", help="Filter by protocol ID (hex like 0x2729 or decimal)."),
    direction: Optional[str] = typer.Option(None, "--direction", help="Filter by direction: request, response, or multicast."),
    after: Optional[str] = typer.Option(None, "--after", help="Show packets after this time (HH:MM:SS or HH:MM:SS.fff)."),
    before: Optional[str] = typer.Option(None, "--before", help="Show packets before this time (HH:MM:SS or HH:MM:SS.fff)."),
    limit: int = typer.Option(200, "--limit", min=1, max=5000, help="Max packets to show."),
    offset: int = typer.Option(0, "--offset", min=0, help="Packet offset within filtered result."),
    descending: bool = typer.Option(False, "--descending", help="Show newest packets first."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
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
            session_id=resolved_session_id,
            device_ip=device_ip,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=resolved_opcode,
            protocol_id=resolved_protocol,
            direction=resolved_direction,
        )
        rows = store.get_session_packets(
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

        print(
            f"Capture: Session #{resolved_session_id} packets={total} shown={len(rows)} (limit={limit} offset={offset})"
        )
        filters = []
        if device_ip:
            filters.append(f"device={device_ip}")
        if resolved_opcode is not None:
            filters.append(f"opcode=0x{resolved_opcode:04X}")
        if resolved_protocol is not None:
            filters.append(f"protocol=0x{resolved_protocol:04X}")
        if direction:
            filters.append(f"direction={direction}")
        if after:
            filters.append(f"after={after}")
        if before:
            filters.append(f"before={before}")
        if filters:
            print(f"Capture: Filters: {', '.join(filters)}")
        if from_label or to_label:
            print(
                "Capture: Marker window from="
                f"{_normalize_marker_label(from_label) if from_label else '-'} "
                f"to={_normalize_marker_label(to_label) if to_label else '-'}"
            )
        _print_packet_table_header()

        for row in rows:
            payload = row.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            _print_packet_line(
                packet_id=int(row["id"]),
                timestamp_ns=int(row["timestamp_ns"]),
                source_ip=row.get("src_ip"),
                source_port=row.get("src_port"),
                destination_ip=row.get("dst_ip"),
                destination_port=row.get("dst_port"),
                direction=row.get("direction"),
                payload=payload,
                dump=dump,
                dissect_mode=cli_state.dissect,
            )
    finally:
        store.close()


@app.command("marker")
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
    relay_stream: Optional[str] = typer.Option(
        None, "--relay-stream", help="Redis stream key to publish marker events."
    ),
    relay_only: bool = typer.Option(False, "--relay-only", help="Only publish to Redis stream (skip local DB write)."),
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
        except Exception as exception:
            print(f"Capture: invalid --data JSON: {exception}", file=sys.stderr)
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
    if relay_only and session_id is not None and not session:
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
    resolved_relay_stream = _coalesce(relay_stream, capture_cfg.get("ingress_stream"))
    if relay_only and not resolved_relay_stream:
        print("Capture: --relay-only requires --relay-stream (or capture.ingress_stream in config).", file=sys.stderr)
        raise typer.Exit(1)

    if not relay_only:
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
            print(f"Capture: Added marker #{marker_id} to session #{resolved_session_id}")
        finally:
            store.close()

    if resolved_relay_stream:
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
            "already_stored": "0" if relay_only else "1",
        }
        try:
            client.xadd(str(resolved_relay_stream), event, maxlen=200000, approximate=True)
            print(f"Capture: Published marker to stream {resolved_relay_stream}")
        except Exception as exception:
            print(f"Capture: failed to publish marker: {exception}", file=sys.stderr)
            raise typer.Exit(1)


@app.command("collect")
def collect(
    stream: Optional[str] = typer.Option(None, "--stream", help="Redis stream key to consume."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    start_id: str = typer.Option("0-0", "--start-id", help="Redis stream ID cursor."),
    block_ms: int = typer.Option(5000, "--block-ms", help="Redis XREAD block timeout in milliseconds."),
    count: int = typer.Option(100, "--count", help="Max stream entries per read."),
    once: bool = typer.Option(False, "--once", help="Read once and exit."),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Assign imported packets to this session."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session routing: active (default), none, event, or fixed session reference (id/name/latest).",
    ),
    publish_stream: Optional[str] = typer.Option(
        None, "--publish-stream", help="Redis stream key for deduped unified events."
    ),
    publish_maxlen: int = typer.Option(
        200000, "--publish-maxlen", help="Approximate max length for published unified stream."
    ),
    dedupe: Optional[bool] = typer.Option(
        None, "--dedupe/--no-dedupe", help="Enable packet deduplication across source hosts."
    ),
    dedupe_window_ms: Optional[int] = typer.Option(
        None, "--dedupe-window-ms", help="Deduplication time window in milliseconds."
    ),
    live: bool = typer.Option(False, "--live", help="Print packet lines as packets are imported."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII in live mode."),
    redis_host: Optional[str] = typer.Option(None, "--redis-host", help="Redis host."),
    redis_port: Optional[int] = typer.Option(None, "--redis-port", help="Redis port."),
    redis_db: Optional[int] = typer.Option(None, "--redis-db", help="Redis DB."),
    redis_password: Optional[str] = typer.Option(None, "--redis-password", help="Redis password."),
    redis_socket: Optional[str] = typer.Option(None, "--redis-socket", help="Redis UNIX socket path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    if session_id is not None and session is not None:
        raise typer.Exit("Use either --session-id or --session, not both.")
    profile_cfg, _ = _load_capture_profile(config, profile)
    capture_cfg = _as_dict(profile_cfg.get("capture"))
    resolved_stream = _coalesce(stream, capture_cfg.get("ingress_stream"))
    if not resolved_stream:
        raise typer.Exit("--stream is required (or set capture.ingress_stream in config).")
    resolved_publish_stream = _coalesce(publish_stream, capture_cfg.get("unified_stream"))
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
    resolved_dedupe = dedupe
    if resolved_dedupe is None:
        resolved_dedupe = _parse_config_bool(capture_cfg.get("dedupe"), "capture.dedupe")
    if resolved_dedupe is None:
        resolved_dedupe = True
    resolved_dedupe_window_ms = dedupe_window_ms
    if resolved_dedupe_window_ms is None:
        resolved_dedupe_window_ms = _parse_config_int(capture_cfg.get("dedupe_window_ms"), "capture.dedupe_window_ms")
    if resolved_dedupe_window_ms is None:
        resolved_dedupe_window_ms = 15

    if resolved_publish_stream and resolved_publish_stream == resolved_stream:
        raise typer.Exit("--publish-stream must be different from --stream to avoid event loops.")

    client = _resolve_redis_for_capture(
        redis_host=resolved_redis_host,
        redis_port=resolved_redis_port,
        redis_db=resolved_redis_db,
        redis_password=resolved_redis_password,
        redis_socket=resolved_redis_socket,
    )
    store = PacketStore(db_path=resolved_db)
    session_selector = str(_coalesce(session, capture_cfg.get("session"), "active")).strip()
    if not session_selector:
        session_selector = "active"

    fixed_session_id: int | None = None
    use_active_session = False
    use_event_session = False
    if session_id is not None:
        fixed_session_id = int(session_id)
    else:
        selector = session_selector.lower()
        if selector in {"active", "current"}:
            use_active_session = True
        elif selector in {"none", "off", "disabled"}:
            pass
        elif selector in {"event", "source"}:
            use_event_session = True
        else:
            fixed_session_id, _ = _resolve_session_reference(
                store,
                session_id=None,
                session=session_selector,
                default_selector="latest",
            )

    def _resolve_target_session_id(inferred_session_id: int | None) -> int | None:
        if fixed_session_id is not None:
            return fixed_session_id
        if use_active_session:
            active_session = store.get_latest_session(active_only=True)
            if active_session:
                return int(active_session["id"])
            return None
        if use_event_session:
            return inferred_session_id
        return None

    cursor = start_id
    total_packets = 0
    total_markers = 0
    deduped_packets = 0
    published_packets = 0
    published_markers = 0
    dedupe_window_ns = max(resolved_dedupe_window_ms, 0) * 1_000_000
    dedupe_horizon_ns = max(dedupe_window_ns * 200, 2_000_000_000) if dedupe_window_ns > 0 else 0
    recent_fingerprints: dict[str, tuple[int, str | None]] = {}
    recent_order: deque[tuple[int, str, str | None]] = deque()
    collector_host = socket.gethostname()
    print(f"Capture: Collecting from Redis stream {resolved_stream} into {resolved_db}")
    if fixed_session_id is not None:
        print(f"Capture: Session routing fixed to #{fixed_session_id}")
    elif use_active_session:
        print("Capture: Session routing mode active (assign to current active session)")
    elif use_event_session:
        print("Capture: Session routing mode event (use source event session_id)")
    else:
        print("Capture: Session routing mode none (do not assign sessions)")
    if resolved_publish_stream:
        print(f"Capture: Publishing unified events to Redis stream {resolved_publish_stream}")
    if live:
        _print_packet_table_header()
    try:
        while True:
            response = client.xread({str(resolved_stream): cursor}, count=count, block=block_ms)
            if not response:
                if once:
                    break
                continue
            for _, entries in response:
                for entry_id, fields in entries:
                    cursor = entry_id
                    event_type = (fields.get("event") or "packet").strip().lower()

                    timestamp_ns = None
                    try:
                        ts = fields.get("timestamp_ns")
                        if ts:
                            timestamp_ns = int(ts)
                    except Exception:
                        timestamp_ns = None

                    inferred_session_id = None
                    try:
                        raw_session = fields.get("session_id")
                        if raw_session not in (None, ""):
                            inferred_session_id = int(raw_session)
                    except Exception:
                        inferred_session_id = None

                    target_session_id = _resolve_target_session_id(inferred_session_id)

                    if event_type == "marker":
                        if target_session_id is None:
                            continue

                        if fields.get("already_stored") == "1":
                            continue

                        normalized_marker_type = _normalize_marker_type(
                            str(fields.get("marker_type") or "observation"),
                            strict=False,
                        )
                        normalized_label = _normalize_marker_label(str(fields.get("label") or "marker"))
                        marker_data = None
                        data_json = fields.get("data_json")
                        if data_json:
                            try:
                                parsed = json.loads(data_json)
                                if isinstance(parsed, dict):
                                    marker_data = parsed
                                else:
                                    marker_data = {"value": parsed}
                            except Exception:
                                marker_data = {"raw": data_json}

                        marker_id = store.add_marker(
                            session_id=target_session_id,
                            marker_type=normalized_marker_type,
                            label=normalized_label,
                            summary=fields.get("summary") or None,
                            note=fields.get("note") or None,
                            source_host=fields.get("source_host") or None,
                            data=marker_data,
                            timestamp_ns=timestamp_ns,
                        )
                        total_markers += 1
                        print(f"  imported marker #{marker_id} from stream id {entry_id}")
                        if resolved_publish_stream:
                            event = {
                                "event": "marker",
                                "collector_emitted": "1",
                                "collector_host": collector_host,
                                "source_stream_id": entry_id,
                                "source_host": fields.get("source_host") or "",
                                "timestamp_ns": str(timestamp_ns or time.time_ns()),
                                "session_id": str(target_session_id),
                                "marker_type": normalized_marker_type,
                                "label": normalized_label,
                                "note": str(fields.get("note") or ""),
                                "data_json": fields.get("data_json") or "",
                            }
                            try:
                                client.xadd(
                                    str(resolved_publish_stream), event, maxlen=publish_maxlen, approximate=True
                                )
                                published_markers += 1
                            except Exception as exception:
                                print(f"Capture: unified marker publish failed: {exception}", file=sys.stderr)
                        continue

                    payload_hex = fields.get("payload_hex") or ""
                    if not payload_hex:
                        continue
                    try:
                        payload = bytes.fromhex(payload_hex)
                    except Exception:
                        continue

                    src_port = _parse_optional_int(fields.get("src_port"))
                    dst_port = _parse_optional_int(fields.get("dst_port"))
                    src_ip = fields.get("src_ip") or None
                    dst_ip = fields.get("dst_ip") or None
                    direction = fields.get("direction") or None
                    source_host = fields.get("source_host") or None
                    effective_timestamp_ns = timestamp_ns or time.time_ns()

                    if resolved_dedupe and dedupe_window_ns > 0:
                        prune_before = effective_timestamp_ns - dedupe_horizon_ns
                        while recent_order and recent_order[0][0] < prune_before:
                            stale_ts, stale_key, stale_host = recent_order.popleft()
                            if recent_fingerprints.get(stale_key) == (stale_ts, stale_host):
                                del recent_fingerprints[stale_key]

                        fingerprint = _packet_fingerprint(
                            payload=payload,
                            src_ip=src_ip,
                            src_port=src_port,
                            dst_ip=dst_ip,
                            dst_port=dst_port,
                            direction=direction,
                        )
                        last_seen = recent_fingerprints.get(fingerprint)
                        if last_seen is not None:
                            last_seen_ns, last_seen_host = last_seen
                            host_changed = source_host != last_seen_host
                            if host_changed and abs(effective_timestamp_ns - last_seen_ns) <= dedupe_window_ns:
                                deduped_packets += 1
                                continue
                        recent_fingerprints[fingerprint] = (effective_timestamp_ns, source_host)
                        recent_order.append((effective_timestamp_ns, fingerprint, source_host))

                    packet_id = store.store_packet(
                        payload=payload,
                        source_type=fields.get("source_type") or "redis_capture",
                        src_ip=src_ip,
                        src_port=src_port,
                        dst_ip=dst_ip,
                        dst_port=dst_port,
                        device_ip=fields.get("device_ip") or None,
                        direction=direction,
                        session_id=target_session_id,
                        timestamp_ns=effective_timestamp_ns,
                        source_host=source_host,
                    )
                    if packet_id:
                        total_packets += 1
                        if live:
                            _print_packet_line(
                                packet_id=packet_id,
                                timestamp_ns=effective_timestamp_ns,
                                source_ip=src_ip or "?",
                                source_port=src_port or 0,
                                destination_ip=dst_ip or "?",
                                destination_port=dst_port or 0,
                                direction=direction,
                                payload=payload,
                                dump=dump,
                            )
                        else:
                            print(f"  imported packet #{packet_id} from stream id {entry_id}")
                        if resolved_publish_stream:
                            event = {
                                "event": "packet",
                                "collector_emitted": "1",
                                "collector_host": collector_host,
                                "source_stream_id": entry_id,
                                "source_host": fields.get("source_host") or "",
                                "packet_id": str(fields.get("packet_id") or ""),
                                "unified_packet_id": str(packet_id),
                                "timestamp_ns": str(effective_timestamp_ns),
                                "src_ip": str(src_ip or ""),
                                "src_port": str(src_port or ""),
                                "dst_ip": str(dst_ip or ""),
                                "dst_port": str(dst_port or ""),
                                "direction": str(direction or ""),
                                "device_ip": str(fields.get("device_ip") or ""),
                                "source_type": str(fields.get("source_type") or "redis_capture"),
                                "session_id": str(target_session_id or ""),
                                "payload_len": str(len(payload)),
                                "payload_hex": payload_hex,
                            }
                            try:
                                client.xadd(
                                    str(resolved_publish_stream), event, maxlen=publish_maxlen, approximate=True
                                )
                                published_packets += 1
                            except Exception as exception:
                                print(f"Capture: unified packet publish failed: {exception}", file=sys.stderr)
            if once:
                break
    finally:
        store.close()
    print(f"Capture: Imported {total_packets} packet(s), {total_markers} marker(s), deduped={deduped_packets}")
    if resolved_publish_stream:
        print(
            "Capture: Published unified "
            f"{published_packets} packet(s), {published_markers} marker(s) "
            f"to {resolved_publish_stream}"
        )


@app.command("follow")
def follow(
    stream: Optional[str] = typer.Option(None, "--stream", help="Redis stream key to consume."),
    start_id: str = typer.Option("$", "--start-id", help="Redis stream ID cursor."),
    block_ms: int = typer.Option(5000, "--block-ms", help="Redis XREAD block timeout in milliseconds."),
    count: int = typer.Option(100, "--count", help="Max stream entries per read."),
    once: bool = typer.Option(False, "--once", help="Read once and exit."),
    markers: bool = typer.Option(True, "--markers/--no-markers", help="Show marker events."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    redis_host: Optional[str] = typer.Option(None, "--redis-host", help="Redis host."),
    redis_port: Optional[int] = typer.Option(None, "--redis-port", help="Redis port."),
    redis_db: Optional[int] = typer.Option(None, "--redis-db", help="Redis DB."),
    redis_password: Optional[str] = typer.Option(None, "--redis-password", help="Redis password."),
    redis_socket: Optional[str] = typer.Option(None, "--redis-socket", help="Redis UNIX socket path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    profile_cfg, _ = _load_capture_profile(config, profile)
    capture_cfg = _as_dict(profile_cfg.get("capture"))
    resolved_stream = _coalesce(stream, capture_cfg.get("unified_stream"), capture_cfg.get("ingress_stream"))
    if not resolved_stream:
        raise typer.Exit("--stream is required (or set capture.unified_stream in config).")
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
    client = _resolve_redis_for_capture(
        redis_host=resolved_redis_host,
        redis_port=resolved_redis_port,
        redis_db=resolved_redis_db,
        redis_password=resolved_redis_password,
        redis_socket=resolved_redis_socket,
    )
    cursor = start_id
    total_packets = 0
    total_markers = 0
    print(f"Capture: Following Redis stream {resolved_stream} from {start_id}")
    _print_packet_table_header()

    while True:
        response = client.xread({str(resolved_stream): cursor}, count=count, block=block_ms)
        if not response:
            if once:
                break
            continue

        for _, entries in response:
            for entry_id, fields in entries:
                cursor = entry_id
                event_type = (fields.get("event") or "packet").strip().lower()

                timestamp_ns = _parse_optional_int(fields.get("timestamp_ns")) or time.time_ns()

                if event_type == "marker":
                    if not markers:
                        continue
                    total_markers += 1
                    marker_time = datetime.datetime.fromtimestamp(timestamp_ns / 1e9).strftime("%H:%M:%S.%f")[:-3]
                    marker_type = _normalize_marker_type(str(fields.get("marker_type") or "observation"), strict=False)
                    label = _normalize_marker_label(str(fields.get("label") or "marker"))
                    source_host = fields.get("source_host") or "?"
                    session_token = fields.get("session_id") or "-"
                    note = fields.get("note") or ""
                    print(
                        f"  marker  {marker_time}  session={session_token}  type={marker_type}  "
                        f"label={label}  host={source_host}"
                    )
                    if note:
                        print(f"          note: {note}")
                    continue

                payload_hex = fields.get("payload_hex") or ""
                if not payload_hex:
                    continue
                try:
                    payload = bytes.fromhex(payload_hex)
                except Exception:
                    continue

                src_ip = fields.get("src_ip") or "?"
                dst_ip = fields.get("dst_ip") or "?"
                src_port = _parse_optional_int(fields.get("src_port")) or 0
                dst_port = _parse_optional_int(fields.get("dst_port")) or 0
                direction = fields.get("direction") or None
                packet_id = _parse_optional_int(fields.get("unified_packet_id"))
                if packet_id is None:
                    packet_id = _parse_optional_int(fields.get("packet_id"))
                if packet_id is None:
                    packet_id = total_packets + 1

                _print_packet_line(
                    packet_id=packet_id,
                    timestamp_ns=timestamp_ns,
                    source_ip=src_ip,
                    source_port=src_port,
                    destination_ip=dst_ip,
                    destination_port=dst_port,
                    direction=direction,
                    payload=payload,
                    dump=dump,
                )
                total_packets += 1

        if once:
            break

    print(f"Capture: Followed {total_packets} packet(s), {total_markers} marker(s).")


@packet_app.command("list")
def packet_list(
    session: Optional[str] = typer.Option(
        None, "--session",
        help="Session reference (ID, name, latest, active). Omit to search all packets.",
    ),
    device_ip: Optional[str] = typer.Option(
        None, "--device-ip", help="Filter by device IP (src or dst)."
    ),
    source_ip: Optional[str] = typer.Option(
        None, "--src", help="Filter by source IP."
    ),
    destination_ip: Optional[str] = typer.Option(
        None, "--dst", help="Filter by destination IP."
    ),
    port: Optional[int] = typer.Option(
        None, "--port", help="Filter by port (src or dst)."
    ),
    device_name: Optional[str] = typer.Option(
        None, "--device", help="Filter by device name."
    ),
    opcode: Optional[str] = typer.Option(
        None, "--opcode", help="Filter by opcode (hex like 0x2000 or decimal)."
    ),
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
    grep: Optional[str] = typer.Option(
        None, "--grep", help="Filter packets containing this string in their payload."
    ),
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
                store, session_id=None, session=session, default_selector="latest",
            )

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

        total = store.search_packets_count(
            session_id=None,
            device_ip=device_ip,
            device_name=device_name,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=resolved_opcode,
            protocol_id=resolved_protocol,
            direction=resolved_direction,
            payload_contains=grep,
            src_ip=source_ip,
            dst_ip=destination_ip,
            port=port,
        )
        rows = store.search_packets(
            session_id=None,
            device_ip=device_ip,
            device_name=device_name,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=resolved_opcode,
            protocol_id=resolved_protocol,
            direction=resolved_direction,
            payload_contains=grep,
            src_ip=source_ip,
            dst_ip=destination_ip,
            port=port,
            limit=limit,
            offset=offset,
            ascending=not descending,
        )

        scope = f"session #{session_id}" if session_id else "all packets"
        print(f"Capture: {scope} {_emdash()} {total} matched, showing {len(rows)} (limit={limit} offset={offset})")

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
        if resolved_opcode is not None:
            filters.append(f"opcode=0x{resolved_opcode:04X}")
        if resolved_protocol is not None:
            filters.append(f"protocol=0x{resolved_protocol:04X}")
        if direction:
            filters.append(f"direction={direction}")
        if grep:
            filters.append(f"grep={grep}")
        if after:
            filters.append(f"after={after}")
        if before:
            filters.append(f"before={before}")
        if filters:
            print(f"Capture: Filters: {', '.join(filters)}")

        _print_packet_table_header()

        for row in rows:
            payload = row.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)
            _print_packet_line(
                packet_id=int(row["id"]),
                timestamp_ns=int(row["timestamp_ns"]),
                source_ip=row.get("src_ip"),
                source_port=row.get("src_port"),
                destination_ip=row.get("dst_ip"),
                destination_port=row.get("dst_port"),
                direction=row.get("direction"),
                payload=payload,
                dump=dump,
                dissect_mode=cli_state.dissect,
            )
    finally:
        store.close()


@packet_app.command("show")
def packet_show(
    packet_id: list[int] = typer.Argument(..., help="Packet ID(s) to display."),
    raw: bool = typer.Option(False, "--raw", help="Plain hex dump instead of annotated dissection."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    store = PacketStore(db_path=resolved_db)
    try:
        for pid in packet_id:
            pkt = store.get_packet(pid)
            if not pkt:
                print(f"Packet #{pid}: not found", file=sys.stderr)
                continue

            payload = pkt.get("payload") or b""
            if isinstance(payload, str):
                payload = bytes.fromhex(payload)

            direction = pkt.get("direction") or "multicast"
            src_ip = pkt.get("src_ip") or "?"
            src_port = pkt.get("src_port") or "?"
            dst_ip = pkt.get("dst_ip") or "?"
            dst_port = pkt.get("dst_port") or "?"
            device_ip = pkt.get("device_ip") or "?"
            session_id_val = pkt.get("session_id") or "?"
            source_type = pkt.get("source_type") or "?"
            timestamp_ns = int(pkt.get("timestamp_ns") or 0)

            timestamp = datetime.datetime.fromtimestamp(timestamp_ns / 1e9)
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            info_str = _label_packet(payload, include_code=True)

            packet_interface = pkt.get("interface") or ""

            print(f"Packet #{pid}")
            print(f"  Time:      {timestamp_str}")
            print(f"  Source:    {src_ip}:{src_port}")
            print(f"  Dest:      {dst_ip}:{dst_port}")
            print(f"  Direction: {direction}")
            print(f"  Device:    {device_ip}")
            if packet_interface:
                print(f"  Interface: {packet_interface}")
            print(f"  Session:   {session_id_val}")
            print(f"  Type:      {source_type}")
            print(f"  Size:      {len(payload)}B")
            if info_str:
                print(f"  Label:     {info_str}")

            if raw:
                if len(payload) >= 2:
                    protocol_id = struct.unpack(">H", payload[0:2])[0]
                    print(f"  Protocol:  0x{protocol_id:04X}")

                if len(payload) >= 8:
                    opcode = struct.unpack(">H", payload[6:8])[0]
                    print(f"  Opcode:    0x{opcode:04X}")

                if len(payload) >= 10:
                    status = struct.unpack(">H", payload[8:10])[0]
                    print(f"  Status:    0x{status:04X}")

                print(f"  Payload:")
                print(_hexdump(payload, indent="    "))
            else:
                from netaudio.dante.packet_dissector import dissect_and_render
                print(dissect_and_render(payload, indent="  "))

            print()
    finally:
        store.close()


@packet_app.command("diff")
def packet_diff(
    packet_ids: list[int] = typer.Argument(..., help="Two or more packet IDs to compare."),
    full: bool = typer.Option(False, "--full", help="Show full hex dump with diffs highlighted, not just changed bytes."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    """Byte-level diff of two or more packets."""
    if len(packet_ids) < 2:
        print("Need at least 2 packet IDs to diff.", file=sys.stderr)
        raise typer.Exit(1)

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    store = PacketStore(db_path=resolved_db)
    try:
        packets = []
        for pid in packet_ids:
            pkt = store.get_packet(pid)
            if not pkt:
                print(f"Packet #{pid}: not found", file=sys.stderr)
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


@packet_app.command("state-diff")
def packet_state_diff(
    device_ip: str = typer.Option(..., "--device-ip", help="Device IP address."),
    before_time: str = typer.Option(..., "--before", help="Time before state change (HH:MM:SS). Packets before this time."),
    after_time: str = typer.Option(..., "--after", help="Time after state change (HH:MM:SS). Packets after this time."),
    ignore_volatile: bool = typer.Option(True, "--ignore-volatile/--no-ignore-volatile", help="Exclude known volatile header bytes (transaction_id, sequence)."),
    ignore_jitter: bool = typer.Option(True, "--ignore-jitter/--no-ignore-jitter", help="Exclude bytes that vary within the same time window (counters/timestamps)."),
    direction: Optional[str] = typer.Option("response", "--direction", help="Packet direction filter (default: response)."),
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
    before_ns = _parse_time_to_ns(before_time)
    after_ns = _parse_time_to_ns(after_time)

    if before_ns is None:
        print(f"Invalid --before time format: {before_time} (use HH:MM:SS)", file=sys.stderr)
        raise typer.Exit(1)
    if after_ns is None:
        print(f"Invalid --after time format: {after_time} (use HH:MM:SS)", file=sys.stderr)
        raise typer.Exit(1)
    if before_ns >= after_ns:
        print("--before must be earlier than --after", file=sys.stderr)
        raise typer.Exit(1)

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
                store, session_id=None, session=session, default_selector="latest",
            )

        before_rows = store.search_packets(
            session_id=session_id,
            device_ip=device_ip,
            end_ns=before_ns,
            opcode=resolved_opcode,
            direction=resolved_direction,
            limit=10000,
            ascending=True,
        )
        after_rows = store.search_packets(
            session_id=session_id,
            device_ip=device_ip,
            start_ns=after_ns,
            opcode=resolved_opcode,
            direction=resolved_direction,
            limit=10000,
            ascending=True,
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
        print(f"  before window: ≤ {before_ts} ({sum(len(v) for v in before_by_opcode.values())} packets, {len(before_by_opcode)} opcodes)")
        print(f"  after  window: ≥ {after_ts} ({sum(len(v) for v in after_by_opcode.values())} packets, {len(after_by_opcode)} opcodes)")
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

            before_jitter = _detect_jitter_offsets(before_only_payloads) if ignore_jitter and len(before_only_payloads) > 1 else set()
            after_jitter = _detect_jitter_offsets(after_only_payloads) if ignore_jitter and len(after_only_payloads) > 1 else set()
            jitter_offsets = before_jitter | after_jitter

            had_diff = _state_diff_print_opcode_diff(
                opcode_label, before_payloads, after_payloads,
                volatile_offsets, jitter_offsets, full,
            )
            if had_diff:
                diff_count += 1
            else:
                identical_count += 1

        print(f"\n  {diff_count} opcodes with stable differences, {identical_count} identical")

    finally:
        store.close()


@app.command("clear")
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

    print(f"Deleted {db_path}", file=sys.stderr)
