from __future__ import annotations

import asyncio
import os
import signal
import socket
import sys
import time
import traceback
from typing import Optional

import typer

from netaudio.capture import daemon as capture_daemon
from netaudio.capture.daemon import CaptureDaemon, _get_redis_client, _print_packet_line
from netaudio.capture.display import _emdash
from netaudio.capture.interfaces import _default_interface
from netaudio.commands.capture_app import app
from netaudio.commands.capture_helpers import (
    _as_dict,
    _coalesce,
    _load_capture_profile,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_redis_from_config,
)
from netaudio.capture.packets import _label_packet, _print_packet_table_header
from netaudio.common.app_config import get_available_interfaces
from netaudio.common.app_config import settings as app_settings
from netaudio.dante.packet_store import PacketStore
from netaudio.dante.tshark_capture import TsharkCapture
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
    redis_host: Optional[str] = typer.Option(
        None, "--redis-host", help="Redis host for device discovery and capture ingress."
    ),
    redis_port: Optional[int] = typer.Option(
        None, "--redis-port", help="Redis port for device discovery and capture ingress."
    ),
    redis_db: Optional[int] = typer.Option(
        None, "--redis-db", help="Redis DB index for device discovery and capture ingress."
    ),
    redis_password: Optional[str] = typer.Option(
        None, "--redis-password", help="Redis password for device discovery and capture ingress."
    ),
    redis_socket: Optional[str] = typer.Option(
        None, "--redis-socket", help="Redis UNIX socket path for device discovery and capture ingress."
    ),
    ingress_stream: Optional[str] = typer.Option(
        None, "--ingress-stream", help="Redis stream key to publish capture events."
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
    resolved_ingress_stream = _coalesce(ingress_stream, capture_cfg.get("ingress_stream"))

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
        ingress_stream=str(resolved_ingress_stream) if resolved_ingress_stream is not None else None,
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
        raise typer.Exit(1)
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
