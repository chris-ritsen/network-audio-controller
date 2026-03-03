from __future__ import annotations

import asyncio
import datetime
import signal
import socket
import struct
import sys
import time
import traceback
from threading import Event, Thread
from typing import Optional

import typer

from netaudio_lib.common.app_config import settings as app_settings
from netaudio_lib.dante.const import (
    DEVICE_INFO_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
)
from netaudio_lib.dante.packet_store import DEFAULT_DB_PATH, PacketStore
from netaudio_lib.dante.tshark_capture import TsharkCapture

try:
    from redis import Redis
    from redis.exceptions import ConnectionError as RedisConnectionError
except ImportError:
    Redis = None
    RedisConnectionError = None


def _get_redis_client():
    if Redis is None:
        return None
    try:
        client = Redis(host="localhost", port=6379, db=0, decode_responses=True)
        client.ping()
        return client
    except Exception:
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
    except Exception:
        pass
    return mapping


def _hexdump(data: bytes, indent: str = "         "):
    lines = []
    for offset in range(0, len(data), 16):
        chunk = data[offset : offset + 16]
        left = " ".join(f"{b:02x}" for b in chunk[:8])
        right = " ".join(f"{b:02x}" for b in chunk[8:])
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"{indent}{offset:04x}  {left:<23s}  {right:<23s}  |{ascii_part}|")
    return "\n".join(lines)


def _label_packet(payload: bytes):
    if len(payload) < 8:
        return ""

    from netaudio_lib.dante.const import MESSAGE_TYPE_STRINGS
    from netaudio_lib.dante.debug_formatter import PROTOCOL_NAMES, get_opcode_name

    protocol_id = struct.unpack(">H", payload[0:2])[0]

    if protocol_id in PROTOCOL_NAMES and protocol_id != 0xFFFF:
        opcode = struct.unpack(">H", payload[6:8])[0]
        return get_opcode_name(protocol_id, opcode)

    if protocol_id == 0xFFFF and len(payload) >= 28:
        message_type = struct.unpack(">H", payload[26:28])[0]
        return MESSAGE_TYPE_STRINGS.get(message_type, f"msg:0x{message_type:04X}")

    return f"proto:0x{protocol_id:04X}"


PORT_LABELS = {
    8751: "metering",
    8702: "info",
}


class CaptureDaemon:
    def __init__(
        self,
        db_path: str,
        interface: str = "en0",
        use_tshark: bool = True,
        use_multicast: bool = True,
        device_filter: list = None,
        opcode_filter: list = None,
        export_dir: str = None,
        live: bool = True,
        dump: bool = False,
        metering: bool = False,
    ):
        self.stop_event = Event()
        self.store = PacketStore(db_path=db_path)
        self.interface = interface
        self.dump = dump
        self.metering = metering
        self.use_tshark = use_tshark
        self.use_multicast = use_multicast
        self.device_filter = device_filter or []
        self.opcode_filter = opcode_filter or []
        self.export_dir = export_dir
        self.live = live
        self._threads = []
        self._packet_count = 0
        self._name_to_ip = {}
        self._ip_to_name = {}

    def _label_endpoint(self, ip, port):
        name = self._ip_to_name.get(ip)
        if name:
            return f"{name}:{port}"
        return f"{ip}:{port}"

    def _print_packet(self, packet_id, fields):
        self._packet_count += 1
        source = self._label_endpoint(fields.get("src_ip", "?"), fields.get("src_port", "?"))
        destination = self._label_endpoint(fields.get("dst_ip", "?"), fields.get("dst_port", "?"))
        direction = fields.get("direction", "?")
        size = len(fields.get("payload", b""))

        timestamp_ns = fields.get("timestamp_ns") or time.time_ns()
        timestamp = datetime.datetime.fromtimestamp(timestamp_ns / 1e9)
        timestamp_str = timestamp.strftime("%H:%M:%S.%f")[:-3]

        payload = fields.get("payload", b"")
        info_str = _label_packet(payload)

        if not info_str:
            dst_port = fields.get("dst_port")
            src_port = fields.get("src_port")
            info_str = PORT_LABELS.get(dst_port) or PORT_LABELS.get(src_port, "")

        arrow = "->" if direction == "request" else "<-" if direction == "response" else "**"
        print(
            f"  {packet_id:<6d}  {timestamp_str}  {source:>21s} {arrow} {destination:<21s}  "
            f"{direction or 'multicast':>10s}  {size:4d}B  {info_str}"
        )

        if self.dump:
            print(_hexdump(payload))

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

                    if self.device_filter and source_host not in self.device_filter:
                        continue

                    device_name = self._ip_to_name.get(source_host)

                    packet_id = self.store.store_packet(
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
                        timestamp_ns=timestamp_ns,
                    )

                    if packet_id and self.live:
                        self._print_packet(packet_id, {
                            "src_ip": source_host,
                            "src_port": source_port,
                            "dst_ip": group,
                            "dst_port": port,
                            "direction": None,
                            "payload": data,
                            "timestamp_ns": timestamp_ns,
                        })

                except socket.timeout:
                    continue
                except (socket.error, OSError) as exception:
                    if self.stop_event.is_set():
                        break
                    print(f"Capture: Socket error on {group}:{port}: {exception}", file=sys.stderr)
                    time.sleep(1)
                except Exception as exception:
                    print(f"Capture: Error on {group}:{port}: {exception}", file=sys.stderr)
                    traceback.print_exc()
                    time.sleep(1)
        except OSError as exception:
            print(f"Capture: Failed to bind multicast {group}:{port}: {exception}", file=sys.stderr)
        finally:
            multicast_socket.close()

    async def _run_tshark(self):
        device_ips = self.device_filter or None
        capture = TsharkCapture(
            packet_store=self.store,
            interface=self.interface,
            device_ips=device_ips,
            include_metering=self.metering,
        )

        async def on_packet(packet_id, fields):
            if self.live:
                self._print_packet(packet_id, fields)

        await capture.start(on_packet=on_packet)

    def _print_stats(self):
        stats = self.store.get_stats()
        print(f"\n{'=' * 60}")
        print(f"Capture Statistics")
        print(f"{'=' * 60}")
        print(f"  Total packets:    {stats['total']}")
        print(f"  Correlated:       {stats['correlated']}")
        print(f"  Uncorrelated:     {stats['uncorrelated']}")

        if stats["by_source"]:
            print(f"\n  By source:")
            for source, count in stats["by_source"].items():
                print(f"    {source:25s} {count}")

        if stats["by_opcode"]:
            print(f"\n  By opcode/direction:")
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
        redis_client = _get_redis_client()
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

    async def run(self):
        print(f"Capture: Database at {self.store._db_path}")

        self._resolve_device_filter()

        if self.device_filter:
            print(f"Capture: Filtering to IPs: {', '.join(sorted(self.device_filter))}")

        if self.live:
            print(f"\n{'Packets':^70s}")
            print(f"{'─' * 70}")

        tshark_task = None
        tshark_running = False
        if self.use_tshark:
            if TsharkCapture.is_available():
                tshark_task = asyncio.create_task(self._run_tshark())
                tshark_running = True
            else:
                print(
                    "Capture: tshark not found, falling back to multicast socket only.\n"
                    "  Install with: brew install --cask wireshark",
                    file=sys.stderr,
                )

        if self.use_multicast and not tshark_running:
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

        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(0.5)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
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

            self.store.close()
            print("\nCapture stopped.")


async def _replay_packet(packet_id: int, store: PacketStore, interface: str, tshark_duration: float, dump: bool = False):
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
        print(f"Error: Packet #{packet_id} targets multicast address {destination_ip}, nothing to replay.", file=sys.stderr)
        raise typer.Exit(1)

    print(f"Replaying packet #{packet_id}")
    print(f"  Target:  {destination_ip}:{destination_port}")
    print(f"  Size:    {len(payload)} bytes")
    info = _label_packet(payload)
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

    print(f"{'Packets':^70s}")
    print(f"{'─' * 70}")

    if sent_id:
        _print_packet_line(
            sent_id, send_timestamp,
            local_ip, local_port,
            destination_ip, destination_port,
            "request", payload, dump=dump,
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
                reply_id, reply_timestamp,
                reply_ip, reply_port,
                local_ip, local_port,
                "response", reply_data, dump=dump,
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
                captured_packet_id, fields.get("timestamp_ns", time.time_ns()),
                fields.get("src_ip", "?"), fields.get("src_port", 0),
                fields.get("dst_ip", "?"), fields.get("dst_port", 0),
                fields.get("direction"), fields.get("payload", b""), dump=dump,
            )

    total = 1 + (1 if 'reply_id' in dir() else 0) + len(received_packets)
    print(f"\n  {total} packet(s) total")


def _print_packet_line(packet_id, timestamp_ns, source_ip, source_port, destination_ip, destination_port, direction, payload, dump=False):
    timestamp = datetime.datetime.fromtimestamp(timestamp_ns / 1e9)
    timestamp_str = timestamp.strftime("%H:%M:%S.%f")[:-3]

    size = len(payload)
    info_str = _label_packet(payload)
    if not info_str:
        info_str = PORT_LABELS.get(destination_port) or PORT_LABELS.get(source_port, "")

    arrow = "->" if direction == "request" else "<-" if direction == "response" else "**"
    direction_label = direction or "multicast"

    print(
        f"  {packet_id:<6d}  {timestamp_str}  {source_ip}:{source_port:>5d} {arrow} {destination_ip}:{destination_port:<5d}  "
        f"{direction_label:>10s}  {size:4d}B  {info_str}"
    )

    if dump:
        print(_hexdump(payload))


app = typer.Typer(help="Capture and replay Dante traffic.", no_args_is_help=True)


@app.command()
def live(
    interface: str = typer.Option("en0", "-i", "--interface", help="Network interface for capture."),
    db: str = typer.Option(DEFAULT_DB_PATH, "--db", help="SQLite database path."),
    tshark: bool = typer.Option(True, "--tshark/--no-tshark", help="Enable tshark capture."),
    multicast: bool = typer.Option(True, "--multicast/--no-multicast", help="Enable multicast listener."),
    device: Optional[list[str]] = typer.Option(None, "--device", help="Filter to specific device name(s) or IP(s)."),
    opcode: Optional[list[str]] = typer.Option(None, "--opcode", help="Filter to specific opcode(s)."),
    export_dir: Optional[str] = typer.Option(None, "--export-dir", help="Directory to export fixture files on shutdown."),
    show: bool = typer.Option(True, "--live/--no-live", help="Show live packet feed."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
    metering: bool = typer.Option(False, "--metering", help="Include metering traffic (port 8751)."),
):
    """Capture and correlate Dante protocol packets."""
    daemon = CaptureDaemon(
        db_path=db,
        interface=interface,
        use_tshark=tshark,
        use_multicast=multicast,
        device_filter=device or [],
        opcode_filter=opcode or [],
        export_dir=export_dir,
        live=show,
        dump=dump,
        metering=metering,
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
    db: str = typer.Option(DEFAULT_DB_PATH, "--db", help="SQLite database path."),
    interface: str = typer.Option("en0", "-i", "--interface", help="Network interface for tshark capture."),
    duration: float = typer.Option(2.0, "--duration", help="Seconds to listen for multicast responses."),
    dump: bool = typer.Option(False, "--dump", help="Dump packet payloads as hex + ASCII."),
):
    """Replay a captured packet and show all responses."""
    store = PacketStore(db_path=db)

    try:
        asyncio.run(_replay_packet(id, store, interface, duration, dump=dump))
    finally:
        store.close()


