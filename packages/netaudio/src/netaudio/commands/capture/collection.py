from __future__ import annotations

import datetime
import json
import logging
import socket
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import typer

logger = logging.getLogger("netaudio")

from netaudio.capture.packets import _packet_fingerprint, _print_packet_table_header
from netaudio.commands.capture.live import _resolve_redis_for_capture
from netaudio.commands.capture.options import (
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
from netaudio.dante.packet_store import PacketRecord, PacketStore

DEFAULT_DEDUPE_WINDOW_MILLISECONDS = 15
FIXED_SESSION_ROUTING = "fixed"
ACTIVE_SESSION_ROUTING = "active"
EVENT_SESSION_ROUTING = "event"
NO_SESSION_ROUTING = "none"


@dataclass(frozen=True)
class CollectPlan:
    block_milliseconds: int
    count: int
    database_path: str
    dedupe: bool
    dedupe_window_milliseconds: int
    dump: bool
    live: bool
    once: bool
    publish_maxlen: int
    publish_stream: str | None
    redis_db: int | None
    redis_host: str | None
    redis_password: str | None
    redis_port: int | None
    redis_socket: str | None
    session_id: int | None
    session_selector: str
    start_id: str
    stream: str


@dataclass(frozen=True)
class SessionRouting:
    fixed_session_id: int | None
    mode: str

    def describe(self) -> str:
        if self.mode == FIXED_SESSION_ROUTING:
            return f"Capture: Session routing fixed to #{self.fixed_session_id}"
        if self.mode == ACTIVE_SESSION_ROUTING:
            return "Capture: Session routing mode active (assign to current active session)"
        if self.mode == EVENT_SESSION_ROUTING:
            return "Capture: Session routing mode event (use source event session_id)"
        return "Capture: Session routing mode none (do not assign sessions)"

    def resolve(self, store: PacketStore, inferred_session_id: int | None) -> int | None:
        if self.mode == FIXED_SESSION_ROUTING:
            return self.fixed_session_id
        if self.mode == ACTIVE_SESSION_ROUTING:
            active_session = store.get_latest_session(active_only=True)
            if active_session:
                return int(active_session["id"])
            return None
        if self.mode == EVENT_SESSION_ROUTING:
            return inferred_session_id
        return None


@dataclass(frozen=True)
class StreamEntry:
    entry_id: str
    event_type: str
    fields: dict
    inferred_session_id: int | None
    timestamp_ns: int | None

    @classmethod
    def parse(cls, entry_id: str, fields: dict) -> StreamEntry:
        return cls(
            entry_id=entry_id,
            event_type=(fields.get("event") or "packet").strip().lower(),
            fields=fields,
            inferred_session_id=_parse_stream_integer(fields.get("session_id")),
            timestamp_ns=_parse_stream_integer(fields.get("timestamp_ns")),
        )


@dataclass(frozen=True)
class PacketEntry:
    destination_ip: str | None
    destination_port: int | None
    direction: str | None
    payload: bytes
    payload_hex: str
    source_host: str | None
    source_ip: str | None
    source_port: int | None
    timestamp_ns: int


@dataclass
class CollectTotals:
    deduped_packets: int = 0
    published_markers: int = 0
    published_packets: int = 0
    total_markers: int = 0
    total_packets: int = 0


@dataclass
class PacketDeduplicator:
    window_ns: int
    horizon_ns: int = field(init=False)
    recent_fingerprints: dict[str, tuple[int, str | None]] = field(default_factory=dict)
    recent_order: deque[tuple[int, str, str | None]] = field(default_factory=deque)

    def __post_init__(self) -> None:
        self.horizon_ns = max(self.window_ns * 200, 2_000_000_000) if self.window_ns > 0 else 0

    @property
    def enabled(self) -> bool:
        return self.window_ns > 0

    def is_duplicate(self, packet: PacketEntry) -> bool:
        self._prune(packet.timestamp_ns - self.horizon_ns)
        fingerprint = _packet_fingerprint(
            payload=packet.payload,
            src_ip=packet.source_ip,
            src_port=packet.source_port,
            dst_ip=packet.destination_ip,
            dst_port=packet.destination_port,
            direction=packet.direction,
        )
        last_seen = self.recent_fingerprints.get(fingerprint)
        if last_seen is not None:
            last_seen_ns, last_seen_host = last_seen
            host_changed = packet.source_host != last_seen_host
            if host_changed and abs(packet.timestamp_ns - last_seen_ns) <= self.window_ns:
                return True
        self.recent_fingerprints[fingerprint] = (packet.timestamp_ns, packet.source_host)
        self.recent_order.append((packet.timestamp_ns, fingerprint, packet.source_host))
        return False

    def _prune(self, prune_before: int) -> None:
        while self.recent_order and self.recent_order[0][0] < prune_before:
            stale_timestamp, stale_key, stale_host = self.recent_order.popleft()
            if self.recent_fingerprints.get(stale_key) == (stale_timestamp, stale_host):
                del self.recent_fingerprints[stale_key]


@dataclass
class CollectContext:
    client: object
    collector_host: str
    deduplicator: PacketDeduplicator
    plan: CollectPlan
    routing: SessionRouting
    store: PacketStore
    totals: CollectTotals = field(default_factory=CollectTotals)


def _parse_stream_integer(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_marker_data(data_json: str | None) -> dict | None:
    if not data_json:
        return None
    try:
        parsed = json.loads(data_json)
    except ValueError:
        return {"raw": data_json}
    if isinstance(parsed, dict):
        return parsed
    return {"value": parsed}


def _resolve_dedupe_settings(dedupe: bool | None, dedupe_window_ms: int | None, capture_cfg: dict) -> tuple[bool, int]:
    resolved_dedupe = dedupe
    if resolved_dedupe is None:
        resolved_dedupe = _parse_config_bool(capture_cfg.get("dedupe"), "capture.dedupe")
    if resolved_dedupe is None:
        resolved_dedupe = True
    resolved_window = dedupe_window_ms
    if resolved_window is None:
        resolved_window = _parse_config_int(capture_cfg.get("dedupe_window_ms"), "capture.dedupe_window_ms")
    if resolved_window is None:
        resolved_window = DEFAULT_DEDUPE_WINDOW_MILLISECONDS
    return resolved_dedupe, resolved_window


def _plan_collect(
    *,
    block_ms: int,
    config: str | None,
    count: int,
    db: str | None,
    dedupe: bool | None,
    dedupe_window_ms: int | None,
    dump: bool,
    live: bool,
    once: bool,
    profile: str | None,
    publish_maxlen: int,
    publish_stream: str | None,
    redis_db: int | None,
    redis_host: str | None,
    redis_password: str | None,
    redis_port: int | None,
    redis_socket: str | None,
    session: str | None,
    session_id: int | None,
    start_id: str,
    stream: str | None,
) -> CollectPlan:
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
    resolved_redis = _resolve_redis_from_config(
        profile_cfg=profile_cfg,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_socket=redis_socket,
    )
    resolved_dedupe, resolved_dedupe_window = _resolve_dedupe_settings(dedupe, dedupe_window_ms, capture_cfg)
    if resolved_publish_stream and resolved_publish_stream == resolved_stream:
        raise typer.Exit("--publish-stream must be different from --stream to avoid event loops.")
    session_selector = str(_coalesce(session, capture_cfg.get("session"), "active")).strip() or "active"
    return CollectPlan(
        block_milliseconds=block_ms,
        count=count,
        database_path=resolved_db,
        dedupe=resolved_dedupe,
        dedupe_window_milliseconds=resolved_dedupe_window,
        dump=dump,
        live=live,
        once=once,
        publish_maxlen=publish_maxlen,
        publish_stream=str(resolved_publish_stream) if resolved_publish_stream else None,
        redis_db=resolved_redis[2],
        redis_host=resolved_redis[0],
        redis_password=resolved_redis[3],
        redis_port=resolved_redis[1],
        redis_socket=resolved_redis[4],
        session_id=session_id,
        session_selector=session_selector,
        start_id=start_id,
        stream=str(resolved_stream),
    )


def _resolve_session_routing(plan: CollectPlan, store: PacketStore) -> SessionRouting:
    if plan.session_id is not None:
        return SessionRouting(fixed_session_id=int(plan.session_id), mode=FIXED_SESSION_ROUTING)
    selector = plan.session_selector.lower()
    if selector in {"active", "current"}:
        return SessionRouting(fixed_session_id=None, mode=ACTIVE_SESSION_ROUTING)
    if selector in {"none", "off", "disabled"}:
        return SessionRouting(fixed_session_id=None, mode=NO_SESSION_ROUTING)
    if selector in {"event", "source"}:
        return SessionRouting(fixed_session_id=None, mode=EVENT_SESSION_ROUTING)
    fixed_session_id, _ = _resolve_session_reference(
        store,
        session_id=None,
        session=plan.session_selector,
        default_selector="latest",
    )
    return SessionRouting(fixed_session_id=fixed_session_id, mode=FIXED_SESSION_ROUTING)


def _report_collect_start(context: CollectContext) -> None:
    plan = context.plan
    print(f"Capture: Collecting from Redis stream {plan.stream} into {plan.database_path}")
    print(context.routing.describe())
    if plan.publish_stream:
        print(f"Capture: Publishing unified events to Redis stream {plan.publish_stream}")
    if plan.live:
        _print_packet_table_header()


def _report_collect_totals(context: CollectContext) -> None:
    totals = context.totals
    print(
        f"Capture: Imported {totals.total_packets} packet(s), {totals.total_markers} marker(s), "
        f"deduped={totals.deduped_packets}"
    )
    if context.plan.publish_stream:
        print(
            "Capture: Published unified "
            f"{totals.published_packets} packet(s), {totals.published_markers} marker(s) "
            f"to {context.plan.publish_stream}"
        )


def _publish_unified_event(context: CollectContext, event: dict, kind: str) -> bool:
    from netaudio.capture.daemon import REDIS_ERRORS

    try:
        context.client.xadd(context.plan.publish_stream, event, maxlen=context.plan.publish_maxlen, approximate=True)
    except REDIS_ERRORS as exception:
        typer.echo(f"Capture: unified {kind} publish failed: {exception}", err=True)
        return False
    return True


def _import_marker(context: CollectContext, entry: StreamEntry, target_session_id: int | None) -> None:
    fields = entry.fields
    if target_session_id is None or fields.get("already_stored") == "1":
        return
    normalized_marker_type = _normalize_marker_type(str(fields.get("marker_type") or "observation"), strict=False)
    normalized_label = _normalize_marker_label(str(fields.get("label") or "marker"))
    marker_id = context.store.add_marker(
        session_id=target_session_id,
        marker_type=normalized_marker_type,
        label=normalized_label,
        summary=fields.get("summary") or None,
        note=fields.get("note") or None,
        source_host=fields.get("source_host") or None,
        data=_parse_marker_data(fields.get("data_json")),
        timestamp_ns=entry.timestamp_ns,
    )
    context.totals.total_markers += 1
    print(f"  imported marker #{marker_id} from stream id {entry.entry_id}")
    if not context.plan.publish_stream:
        return
    event = {
        "event": "marker",
        "collector_emitted": "1",
        "collector_host": context.collector_host,
        "source_stream_id": entry.entry_id,
        "source_host": fields.get("source_host") or "",
        "timestamp_ns": str(entry.timestamp_ns or time.time_ns()),
        "session_id": str(target_session_id),
        "marker_type": normalized_marker_type,
        "label": normalized_label,
        "note": str(fields.get("note") or ""),
        "data_json": fields.get("data_json") or "",
    }
    if _publish_unified_event(context, event, "marker"):
        context.totals.published_markers += 1


def _parse_packet_entry(entry: StreamEntry) -> PacketEntry | None:
    payload_hex = entry.fields.get("payload_hex") or ""
    if not payload_hex:
        return None
    try:
        payload = bytes.fromhex(payload_hex)
    except ValueError as exception:
        logger.warning(f"Skipping packet with invalid payload hex: {exception}")
        return None
    return PacketEntry(
        destination_ip=entry.fields.get("dst_ip") or None,
        destination_port=_parse_optional_int(entry.fields.get("dst_port")),
        direction=entry.fields.get("direction") or None,
        payload=payload,
        payload_hex=payload_hex,
        source_host=entry.fields.get("source_host") or None,
        source_ip=entry.fields.get("src_ip") or None,
        source_port=_parse_optional_int(entry.fields.get("src_port")),
        timestamp_ns=entry.timestamp_ns or time.time_ns(),
    )


def _report_imported_packet(context: CollectContext, entry: StreamEntry, packet: PacketEntry, packet_id: int) -> None:
    from netaudio.capture.daemon import PacketLine, _print_packet_line

    if not context.plan.live:
        print(f"  imported packet #{packet_id} from stream id {entry.entry_id}")
        return
    _print_packet_line(
        PacketLine(
            destination_ip=packet.destination_ip or "?",
            destination_port=packet.destination_port or 0,
            direction=packet.direction,
            packet_id=packet_id,
            payload=packet.payload,
            source_ip=packet.source_ip or "?",
            source_port=packet.source_port or 0,
            timestamp_ns=packet.timestamp_ns,
        ),
        dump=context.plan.dump,
    )


def _publish_imported_packet(
    context: CollectContext, entry: StreamEntry, packet: PacketEntry, packet_id: int, target_session_id: int | None
) -> None:
    fields = entry.fields
    event = {
        "event": "packet",
        "collector_emitted": "1",
        "collector_host": context.collector_host,
        "source_stream_id": entry.entry_id,
        "source_host": fields.get("source_host") or "",
        "packet_id": str(fields.get("packet_id") or ""),
        "unified_packet_id": str(packet_id),
        "timestamp_ns": str(packet.timestamp_ns),
        "src_ip": str(packet.source_ip or ""),
        "src_port": str(packet.source_port or ""),
        "dst_ip": str(packet.destination_ip or ""),
        "dst_port": str(packet.destination_port or ""),
        "direction": str(packet.direction or ""),
        "device_ip": str(fields.get("device_ip") or ""),
        "source_type": str(fields.get("source_type") or "redis_capture"),
        "session_id": str(target_session_id or ""),
        "payload_len": str(len(packet.payload)),
        "payload_hex": packet.payload_hex,
    }
    if _publish_unified_event(context, event, "packet"):
        context.totals.published_packets += 1


def _import_packet(context: CollectContext, entry: StreamEntry, target_session_id: int | None) -> None:
    packet = _parse_packet_entry(entry)
    if packet is None:
        return
    if context.plan.dedupe and context.deduplicator.enabled and context.deduplicator.is_duplicate(packet):
        context.totals.deduped_packets += 1
        return
    packet_id = context.store.store_packet(
        PacketRecord(
            payload=packet.payload,
            source_type=entry.fields.get("source_type") or "redis_capture",
            src_ip=packet.source_ip,
            src_port=packet.source_port,
            dst_ip=packet.destination_ip,
            dst_port=packet.destination_port,
            device_ip=entry.fields.get("device_ip") or None,
            direction=packet.direction,
            session_id=target_session_id,
            timestamp_ns=packet.timestamp_ns,
            source_host=packet.source_host,
        )
    )
    if not packet_id:
        return
    context.totals.total_packets += 1
    _report_imported_packet(context, entry, packet, packet_id)
    if context.plan.publish_stream:
        _publish_imported_packet(context, entry, packet, packet_id, target_session_id)


def _import_stream_entry(context: CollectContext, entry_id: str, fields: dict) -> None:
    entry = StreamEntry.parse(entry_id, fields)
    target_session_id = context.routing.resolve(context.store, entry.inferred_session_id)
    if entry.event_type == "marker":
        _import_marker(context, entry, target_session_id)
        return
    _import_packet(context, entry, target_session_id)


def _collect_stream(context: CollectContext) -> None:
    plan = context.plan
    cursor = plan.start_id
    while True:
        response = context.client.xread({plan.stream: cursor}, count=plan.count, block=plan.block_milliseconds)
        if not response:
            if plan.once:
                break
            continue
        for _, entries in response:
            for entry_id, fields in entries:
                cursor = entry_id
                _import_stream_entry(context, entry_id, fields)
        if plan.once:
            break


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
    plan = _plan_collect(
        block_ms=block_ms,
        config=config,
        count=count,
        db=db,
        dedupe=dedupe,
        dedupe_window_ms=dedupe_window_ms,
        dump=dump,
        live=live,
        once=once,
        profile=profile,
        publish_maxlen=publish_maxlen,
        publish_stream=publish_stream,
        redis_db=redis_db,
        redis_host=redis_host,
        redis_password=redis_password,
        redis_port=redis_port,
        redis_socket=redis_socket,
        session=session,
        session_id=session_id,
        start_id=start_id,
        stream=stream,
    )
    client = _resolve_redis_for_capture(
        redis_host=plan.redis_host,
        redis_port=plan.redis_port,
        redis_db=plan.redis_db,
        redis_password=plan.redis_password,
        redis_socket=plan.redis_socket,
    )
    store = PacketStore(db_path=plan.database_path)
    context = CollectContext(
        client=client,
        collector_host=socket.gethostname(),
        deduplicator=PacketDeduplicator(window_ns=max(plan.dedupe_window_milliseconds, 0) * 1_000_000),
        plan=plan,
        routing=_resolve_session_routing(plan, store),
        store=store,
    )
    _report_collect_start(context)
    try:
        _collect_stream(context)
    finally:
        store.close()
    _report_collect_totals(context)


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
    from netaudio.capture.daemon import PacketLine, _print_packet_line

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
                    PacketLine(
                        destination_ip=dst_ip,
                        destination_port=dst_port,
                        direction=direction,
                        packet_id=packet_id,
                        payload=payload,
                        source_ip=src_ip,
                        source_port=src_port,
                        timestamp_ns=timestamp_ns,
                    ),
                    dump=dump,
                )
                total_packets += 1

        if once:
            break

    print(f"Capture: Followed {total_packets} packet(s), {total_markers} marker(s).")
