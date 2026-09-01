from __future__ import annotations

import datetime
import json
import logging
import socket
import sys
import time
from collections import deque
from typing import Optional

import typer

from netaudio.capture.daemon import _print_packet_line
from netaudio.commands.capture_app import app

logger = logging.getLogger("netaudio")

from netaudio.commands.capture_helpers import (
    _as_dict,
    _coalesce,
    _load_capture_profile,
    _normalize_marker_label,
    _normalize_marker_type,
    _parse_config_bool,
    _parse_config_int,
    _parse_optional_int,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_redis_from_config,
    _resolve_session_reference,
)
from netaudio.capture.packets import _packet_fingerprint, _print_packet_table_header
from netaudio.commands.capture_live import _resolve_redis_for_capture
from netaudio.dante.packet_store import PacketStore


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
                    except ValueError as exception:
                        logger.warning(f"Skipping packet with invalid payload hex: {exception}")
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
                except ValueError as exception:
                    logger.warning(f"Skipping packet with invalid payload hex: {exception}")
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
