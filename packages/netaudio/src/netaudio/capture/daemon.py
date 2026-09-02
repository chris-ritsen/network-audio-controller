from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import socket
import struct
import sys
import time
import traceback
from dataclasses import dataclass
from threading import Event, Thread

from netaudio.common.app_config import settings as app_settings
from netaudio.dante.const import (
    DEVICE_INFO_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
)
from netaudio.dante.packet_store import PacketRecord, PacketStore
from netaudio.dante.tshark_capture import TsharkCapture

logger = logging.getLogger("netaudio")

try:
    from redis import Redis
    from redis.exceptions import RedisError
except ImportError:
    Redis = None

    class RedisError(Exception):
        pass


REDIS_ERRORS: tuple[type[BaseException], ...] = (OSError, RedisError)

from netaudio.capture.packets import (
    PACKET_ENDPOINT_WIDTH,
    PORT_LABELS,
    _format_endpoint,
    _hexdump,
    _label_packet,
    _print_packet_table_header,
)
from netaudio.icons import icon

_LAST_REDIS_ERROR: str | None = None


_redis_client_cache: dict[str, "Redis"] = {}


def _resolve_host_ipv4(hostname: str) -> str:
    import socket as _socket

    try:
        results = _socket.getaddrinfo(hostname, None, _socket.AF_INET, _socket.SOCK_STREAM)
        if results:
            return results[0][4][0]
    except _socket.gaierror as exception:
        logger.warning(f"Could not resolve {hostname}: {exception}")
    return hostname


def _get_redis_client(
    host: str | None = None,
    port: int | None = None,
    db: int | None = None,
    password: str | None = None,
    socket_path: str | None = None,
):
    global _LAST_REDIS_ERROR
    if Redis is None:
        _LAST_REDIS_ERROR = "python package 'redis' is not installed in this environment"
        return None
    try:
        resolved_socket = socket_path or os.environ.get("REDIS_SOCKET")
        resolved_host = host or os.environ.get("REDIS_HOST") or "localhost"
        resolved_port = port if port is not None else int(os.environ.get("REDIS_PORT") or 6379)
        resolved_db = db if db is not None else int(os.environ.get("REDIS_DB") or 0)
        resolved_password = password or os.environ.get("REDIS_PASSWORD")

        cache_key = f"{resolved_socket or resolved_host}:{resolved_port}:{resolved_db}"
        if cache_key in _redis_client_cache:
            cached = _redis_client_cache[cache_key]
            try:
                cached.ping()
                return cached
            except REDIS_ERRORS:
                del _redis_client_cache[cache_key]

        if resolved_socket:
            client = Redis(
                unix_socket_path=resolved_socket,
                db=resolved_db,
                password=resolved_password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
            )
        else:
            resolved_ip = _resolve_host_ipv4(resolved_host)
            client = Redis(
                host=resolved_ip,
                port=resolved_port,
                db=resolved_db,
                password=resolved_password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
            )
        client.ping()
        _LAST_REDIS_ERROR = None
        _redis_client_cache[cache_key] = client
        return client
    except (OSError, ValueError, RedisError) as exception:
        _LAST_REDIS_ERROR = f"{type(exception).__name__}: {exception}"
        return None


def _resolve_devices_from_redis(redis_client):
    if not redis_client:
        return {}

    mapping = {}
    try:
        keys = redis_client.keys("netaudio:daemon:device:*")
        for key in keys:
            data = redis_client.hgetall(key)
            if data and data.get("ipv4"):
                name = data.get("name") or key.rsplit(":", 1)[-1]
                mapping[name] = data["ipv4"]
    except (OSError, ValueError, RedisError):
        logger.exception("Failed to resolve daemon devices from Redis")
    return mapping


@dataclass(frozen=True)
class PacketLine:
    destination_ip: str | None
    destination_port: int | None
    direction: str | None
    packet_id: int
    payload: bytes
    source_ip: str | None
    source_port: int | None
    timestamp_ns: int
    destination_endpoint: str | None = None
    source_endpoint: str | None = None

    @classmethod
    def from_row(cls, row: dict) -> PacketLine:
        payload = row.get("payload") or b""
        if isinstance(payload, str):
            payload = bytes.fromhex(payload)
        return cls(
            destination_ip=row.get("dst_ip"),
            destination_port=row.get("dst_port"),
            direction=row.get("direction"),
            packet_id=int(row["id"]),
            payload=payload,
            source_ip=row.get("src_ip"),
            source_port=row.get("src_port"),
            timestamp_ns=int(row["timestamp_ns"]),
        )


def _print_packet_line(line: PacketLine, dump=False, dissect_mode=False):
    timestamp = datetime.datetime.fromtimestamp(line.timestamp_ns / 1e9)
    timestamp_str = timestamp.strftime("%H:%M:%S.%f")[:-3]

    size = len(line.payload)
    info_str = _label_packet(line.payload, include_code=True)
    if not info_str:
        info_str = PORT_LABELS.get(line.destination_port) or PORT_LABELS.get(line.source_port, "")

    direction = line.direction
    arrow = "->" if direction == "request" else "<-" if direction == "response" else "**"
    direction_label = direction or "multicast"
    direction_icon = icon("tx") if direction == "request" else icon("rx") if direction == "response" else icon("packet")
    source = line.source_endpoint or _format_endpoint(line.source_ip, line.source_port)
    destination = line.destination_endpoint or _format_endpoint(line.destination_ip, line.destination_port)

    print(
        f"  {direction_icon}{line.packet_id:<6d}  {timestamp_str:12s}  "
        f"{source:>{PACKET_ENDPOINT_WIDTH}s} {arrow} {destination:<{PACKET_ENDPOINT_WIDTH}s}  "
        f"{direction_label:>10s}  {size:5d}B  {info_str}"
    )

    if dissect_mode:
        from netaudio.dante.dissection.rendering import dissect_and_render

        print(dissect_and_render(line.payload))
    elif dump:
        print(_hexdump(line.payload))


@dataclass(frozen=True)
class CaptureDaemonOptions:
    db_path: str
    device_filter: list | None = None
    dissect: bool = False
    dump: bool = False
    export_dir: str | None = None
    ingress_stream: str | None = None
    interface: str = "en0"
    live: bool = True
    metering: bool = False
    opcode_filter: list | None = None
    redis_db: int | None = None
    redis_host: str | None = None
    redis_password: str | None = None
    redis_port: int | None = None
    redis_socket: str | None = None
    session_id: int | None = None
    session_name: str | None = None
    tcp: bool = False
    use_multicast: bool = True
    use_tshark: bool = True


class CaptureDaemon:
    def __init__(self, options: CaptureDaemonOptions):
        db_path = options.db_path
        interface = options.interface
        use_tshark = options.use_tshark
        use_multicast = options.use_multicast
        device_filter = options.device_filter
        opcode_filter = options.opcode_filter
        export_dir = options.export_dir
        live = options.live
        dump = options.dump
        dissect = options.dissect
        metering = options.metering
        tcp = options.tcp
        session_id = options.session_id
        session_name = options.session_name
        redis_host = options.redis_host
        redis_port = options.redis_port
        redis_db = options.redis_db
        redis_password = options.redis_password
        redis_socket = options.redis_socket
        ingress_stream = options.ingress_stream
        self.stop_event = Event()
        self.store = PacketStore(db_path=db_path)
        self.interface = interface
        self.dump = dump
        self.dissect = dissect
        self.metering = metering
        self.tcp = tcp
        self.use_tshark = use_tshark
        self.use_multicast = use_multicast
        self.device_filter = device_filter or []
        self._explicit_device_filter = bool(device_filter)
        self.opcode_filter = opcode_filter or []
        self.export_dir = export_dir
        self.live = live
        self._threads = []
        self._packet_count = 0
        self._name_to_ip = {}
        self._ip_to_name = {}
        self._multicast_started = False
        self._tshark_failure_reported = False
        self.session_id = session_id
        self.session_name = session_name
        self._auto_session = False
        self._source_host = socket.gethostname()
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password
        self.redis_socket = redis_socket
        self.ingress_stream = ingress_stream
        self._ingress_redis = None

    def _label_endpoint(self, ip, port):
        name = self._ip_to_name.get(ip)
        if name:
            return f"{name}:{port}"
        return f"{ip}:{port}"

    def _publish_packet_to_ingress_stream(self, packet_id: int, fields: dict):
        if not self.ingress_stream or self._ingress_redis is None:
            return

        payload = fields.get("payload", b"")
        if isinstance(payload, bytes):
            payload_hex = payload.hex()
            payload_len = len(payload)
        else:
            payload_hex = ""
            payload_len = 0

        event = {
            "event": "packet",
            "packet_id": str(packet_id),
            "source_host": self._source_host,
            "timestamp_ns": str(fields.get("timestamp_ns") or time.time_ns()),
            "src_ip": str(fields.get("src_ip") or ""),
            "src_port": str(fields.get("src_port") or ""),
            "dst_ip": str(fields.get("dst_ip") or ""),
            "dst_port": str(fields.get("dst_port") or ""),
            "direction": str(fields.get("direction") or ""),
            "device_ip": str(fields.get("device_ip") or ""),
            "source_type": str(fields.get("source_type") or ""),
            "session_id": str(self.session_id or ""),
            "payload_len": str(payload_len),
            "payload_hex": payload_hex,
        }

        try:
            self._ingress_redis.xadd(self.ingress_stream, event, maxlen=200000, approximate=True)
        except REDIS_ERRORS as exception:
            print(f"Capture: Redis ingress stream publish failed: {exception}", file=sys.stderr)

    def _publish_marker_to_ingress_stream(
        self,
        session_id: int,
        marker_type: str,
        label: str,
        summary: str | None = None,
        note: str | None = None,
        data: dict | None = None,
        timestamp_ns: int | None = None,
    ):
        if not self.ingress_stream or self._ingress_redis is None:
            return

        if timestamp_ns is None:
            timestamp_ns = time.time_ns()

        event = {
            "event": "marker",
            "source_host": self._source_host,
            "timestamp_ns": str(timestamp_ns),
            "session_id": str(session_id),
            "marker_type": str(marker_type),
            "label": str(label),
            "summary": str(summary or ""),
            "note": str(note or ""),
            "data_json": json.dumps(data, sort_keys=True) if data else "",
        }

        try:
            self._ingress_redis.xadd(self.ingress_stream, event, maxlen=200000, approximate=True)
        except REDIS_ERRORS as exception:
            print(f"Capture: Redis ingress stream marker publish failed: {exception}", file=sys.stderr)

    def _print_packet(self, packet_id, fields):
        self._packet_count += 1
        payload = fields.get("payload", b"")
        _print_packet_line(
            PacketLine(
                destination_endpoint=self._label_endpoint(fields.get("dst_ip"), fields.get("dst_port")),
                destination_ip=fields.get("dst_ip"),
                destination_port=fields.get("dst_port"),
                direction=fields.get("direction"),
                packet_id=packet_id,
                payload=payload,
                source_endpoint=self._label_endpoint(fields.get("src_ip"), fields.get("src_port")),
                source_ip=fields.get("src_ip"),
                source_port=fields.get("src_port"),
                timestamp_ns=fields.get("timestamp_ns") or time.time_ns(),
            ),
            dump=self.dump,
            dissect_mode=self.dissect,
        )

    def _multicast_worker(self, group: str, port: int):
        source_ip = app_settings.interface_ip or ""
        multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        try:
            multicast_socket.bind((group, port))
            group_bytes = socket.inet_aton(group)
            if source_ip:
                membership_request = struct.pack("4s4s", group_bytes, socket.inet_aton(source_ip))
            else:
                membership_request = struct.pack("4sL", group_bytes, socket.INADDR_ANY)
            multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership_request)
            multicast_socket.settimeout(1.0)
            print(f"Capture: Listening on multicast {group}:{port}", file=sys.stderr)

            while not self.stop_event.is_set():
                try:
                    data, addr = multicast_socket.recvfrom(2048)
                    timestamp_ns = time.time_ns()
                    source_host, source_port = addr

                    device_name = self._ip_to_name.get(source_host)

                    packet_id = self.store.store_packet(
                        PacketRecord(
                            payload=data,
                            source_type="multicast",
                            src_ip=source_host,
                            src_port=source_port,
                            dst_ip=group,
                            dst_port=port,
                            device_name=device_name,
                            device_ip=source_host,
                            multicast_group=group,
                            multicast_port=port,
                            session_id=self.session_id,
                            timestamp_ns=timestamp_ns,
                            interface=self.interface,
                        )
                    )

                    if packet_id and self.live:
                        self._print_packet(
                            packet_id,
                            {
                                "src_ip": source_host,
                                "src_port": source_port,
                                "dst_ip": group,
                                "dst_port": port,
                                "direction": None,
                                "payload": data,
                                "timestamp_ns": timestamp_ns,
                                "device_ip": source_host,
                                "source_type": "multicast",
                            },
                        )
                    if packet_id:
                        self._publish_packet_to_ingress_stream(
                            packet_id,
                            {
                                "src_ip": source_host,
                                "src_port": source_port,
                                "dst_ip": group,
                                "dst_port": port,
                                "direction": None,
                                "payload": data,
                                "timestamp_ns": timestamp_ns,
                                "device_ip": source_host,
                                "source_type": "multicast",
                            },
                        )

                except socket.timeout:
                    continue
                except (socket.error, OSError) as exception:
                    if self.stop_event.is_set():
                        break
                    print(f"Capture: Socket error on {group}:{port}: {exception}", file=sys.stderr)
                    time.sleep(1)
                except OSError as exception:
                    print(f"Capture: Error on {group}:{port}: {exception}", file=sys.stderr)
                    traceback.print_exc()
                    time.sleep(1)
        except OSError as exception:
            print(f"Capture: Failed to bind multicast {group}:{port}: {exception}", file=sys.stderr)
        finally:
            multicast_socket.close()

    async def _run_tshark(self):
        tshark_filter_ips = self.device_filter if self._explicit_device_filter else None
        capture = TsharkCapture(
            packet_store=self.store,
            interface=self.interface,
            device_ips=tshark_filter_ips,
            known_device_ips=set(self.device_filter) if self.device_filter else None,
            include_metering=self.metering,
            include_tcp=self.tcp,
            session_id=self.session_id,
        )

        async def on_packet(packet_id, fields):
            fields["source_type"] = "tshark"
            self._publish_packet_to_ingress_stream(packet_id, fields)
            if self.live:
                self._print_packet(packet_id, fields)

        await capture.start(on_packet=on_packet)

    def _start_multicast_workers(self):
        if self._multicast_started or not self.use_multicast:
            return

        multicast_configs = [
            (MULTICAST_GROUP_CONTROL_MONITORING, DEVICE_INFO_PORT),
        ]
        for group, port in multicast_configs:
            thread = Thread(
                target=self._multicast_worker,
                args=(group, port),
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

        self._multicast_started = True

    def _report_tshark_failure(self, tshark_task):
        if self._tshark_failure_reported:
            return

        self._tshark_failure_reported = True
        exception_message = None
        if tshark_task and tshark_task.done():
            exception = tshark_task.exception()
            if exception:
                exception_message = str(exception)
                print(f"Capture: tshark failed: {exception_message}", file=sys.stderr)

        if exception_message:
            lower = exception_message.lower()
            if "dumpcap" in lower and "permission denied" in lower:
                print(
                    "Capture: tshark capture permissions are missing (dumpcap).\n"
                    "  Quick test: sudo uv run netaudio capture live\n"
                    "  Permanent fix: configure dumpcap capabilities/group permissions.",
                    file=sys.stderr,
                )
            elif "no such device" in lower or "unknown interface" in lower:
                print(
                    "Capture: tshark interface is invalid. Use --interface with a valid NIC.",
                    file=sys.stderr,
                )

        print(
            "Capture: tshark exited, falling back to multicast socket only.",
            file=sys.stderr,
        )

    def _print_stats(self):
        stats = self.store.get_stats()
        print(f"\n{'=' * 60}")
        print(f"{icon('capture')}Capture Statistics")
        print(f"{'=' * 60}")
        print(f"  Total packets:    {stats['total']}")
        print(f"  Correlated:       {stats['correlated']}")
        print(f"  Uncorrelated:     {stats['uncorrelated']}")

        if stats["by_source"]:
            print("\n  By source:")
            for source, count in stats["by_source"].items():
                print(f"    {source:25s} {count}")

        if stats["by_opcode"]:
            print("\n  By opcode/direction:")
            for entry in stats["by_opcode"][:20]:
                name = entry["opcode_name"] or "unknown"
                direction = entry["direction"] or "multicast"
                print(f"    {name:35s} {direction:10s} {entry['count']}")

    def _export_fixtures(self):
        if not self.export_dir:
            return

        pairs = self.store.get_correlated_pairs()
        if not pairs:
            print("No correlated pairs to export.")
            return

        exported = 0
        for request, response in pairs:
            result = self.store.export_correlated_pair(request["id"], self.export_dir)
            if result:
                exported += 1
                print(f"  Exported: {result[0]}")
                print(f"           {result[1]}")

        print(f"\nExported {exported} correlated pair(s) to {self.export_dir}")

    def _resolve_device_filter(self):
        redis_client = _get_redis_client(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            password=self.redis_password,
            socket_path=self.redis_socket,
        )
        name_to_ip = _resolve_devices_from_redis(redis_client)

        if self.device_filter:
            resolved = []
            for entry in self.device_filter:
                if entry in name_to_ip:
                    resolved.append(name_to_ip[entry])
                    print(f"Capture: Resolved {entry} -> {name_to_ip[entry]}")
                else:
                    resolved.append(entry)
            self.device_filter = resolved
        elif name_to_ip:
            self.device_filter = list(name_to_ip.values())
            print(f"Capture: Auto-discovered {len(name_to_ip)} device(s) from daemon")
            for name, ip in sorted(name_to_ip.items()):
                print(f"  {name} ({ip})")
        else:
            print("Capture: No devices found in Redis -- capturing all traffic")

        self._name_to_ip = name_to_ip
        self._ip_to_name = {v: k for k, v in name_to_ip.items()}

    def _open_ingress_stream(self):
        if not self.ingress_stream:
            return
        self._ingress_redis = _get_redis_client(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            password=self.redis_password,
            socket_path=self.redis_socket,
        )
        if self._ingress_redis is None:
            print("Capture: Redis ingress stream requested but Redis is unavailable.", file=sys.stderr)
        else:
            print(f"Capture: Publishing packets to Redis stream {self.ingress_stream}")

    def _record_capture_marker(self, label, data):
        if self.session_id is None:
            return
        marker_ts = time.time_ns()
        self.store.add_marker(
            session_id=self.session_id,
            marker_type="system",
            label=label,
            source_host=self._source_host,
            data=data,
            timestamp_ns=marker_ts,
        )
        self._publish_marker_to_ingress_stream(
            session_id=self.session_id,
            marker_type="system",
            label=label,
            data=data,
            timestamp_ns=marker_ts,
        )

    def _start_capture_session(self):
        session_metadata = {"interface": self.interface, "ingress_stream": self.ingress_stream}
        if self.session_name and self.session_id is None:
            self.session_id = self.store.start_session(
                name=self.session_name,
                source_host=self._source_host,
                metadata=session_metadata,
            )
            self._auto_session = True
            print(f"Capture: Started session #{self.session_id} ({self.session_name})")
        self._record_capture_marker("capture_started", session_metadata)

    def _report_device_filter(self):
        if self.device_filter and self._explicit_device_filter:
            print(f"Capture: Filtering to IPs: {', '.join(sorted(self.device_filter))}")
        elif self.device_filter:
            print(f"Capture: Known device IPs: {', '.join(sorted(self.device_filter))}")

    def _raise_if_tshark_exited_without_multicast(self, tshark_task):
        exception = tshark_task.exception()
        if exception:
            raise RuntimeError(f"tshark failed and multicast is disabled: {exception}")
        raise RuntimeError("tshark exited and multicast is disabled")

    async def _start_tshark(self):
        if not self.use_tshark:
            return None, False
        if not TsharkCapture.is_available():
            print(
                "Capture: tshark not found, falling back to multicast socket only.\n"
                "  Install with: brew install --cask wireshark",
                file=sys.stderr,
            )
            return None, False
        print("Capture: Starting tshark...", flush=True)
        tshark_task = asyncio.create_task(self._run_tshark())
        await asyncio.sleep(0.2)
        if not tshark_task.done():
            return tshark_task, True
        self._report_tshark_failure(tshark_task)
        if not self.use_multicast:
            self._raise_if_tshark_exited_without_multicast(tshark_task)
        return tshark_task, False

    async def _supervise_capture(self, tshark_task):
        while not self.stop_event.is_set():
            tshark_finished = tshark_task is not None and tshark_task.done()
            if self.use_multicast and not self._multicast_started and tshark_finished:
                self._report_tshark_failure(tshark_task)
                self._start_multicast_workers()
            if not self.use_multicast and tshark_finished:
                self._raise_if_tshark_exited_without_multicast(tshark_task)
            await asyncio.sleep(0.1)

    async def _shutdown_capture(self, tshark_task):
        self.stop_event.set()
        if tshark_task and not tshark_task.done():
            tshark_task.cancel()
            try:
                await tshark_task
            except asyncio.CancelledError:
                pass
        for thread in self._threads:
            thread.join(timeout=3.0)
        self._print_stats()
        if self.export_dir:
            self._export_fixtures()
        self._record_capture_marker("capture_stopped", {"packets_total": self.store.get_stats().get("total", 0)})
        if self._auto_session and self.session_id is not None:
            self.store.end_session(self.session_id)
            print(f"{icon('session')}Capture: Ended session #{self.session_id}")
        self.store.close()
        print(f"\n{icon('capture')}Capture stopped.")

    async def run(self):
        print(f"{icon('capture')}Capture: Database at {self.store._db_path}")
        self._open_ingress_stream()
        self._start_capture_session()
        self._resolve_device_filter()
        self._report_device_filter()
        if self.live:
            print("\nPackets")
            _print_packet_table_header()
        tshark_task, tshark_running = await self._start_tshark()
        if self.use_multicast and not tshark_running:
            self._start_multicast_workers()
        try:
            await self._supervise_capture(tshark_task)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            await self._shutdown_capture(tshark_task)
