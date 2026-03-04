from __future__ import annotations

import asyncio
import logging
import time
from fnmatch import fnmatch
from typing import Optional

import typer

logger = logging.getLogger("netaudio")

from netaudio_lib.dante.const import BLUETOOTH_MODEL_IDS
from netaudio_lib.dante.device_commands import DanteDeviceCommands
from netaudio_lib.dante.device_operations import validate_dante_name
from netaudio_lib.dante.device_serializer import DanteDeviceSerializer

from netaudio._common import (
    _command_context,
    _discover,
    _get_arc_port,
    _populate_controls,
    _resolve_one,
    filter_devices,
    output_single,
    output_table,
    sort_devices,
)

app = typer.Typer(help="Manage Dante devices.", no_args_is_help=True)


def _format_mac(mac: str) -> str:
    if not mac:
        return ""
    raw = mac.replace(":", "").replace("-", "").upper()
    if len(raw) == 16 and raw[6:10] == "FFFE":
        raw = raw[:6] + raw[10:]
    elif len(raw) == 16 and raw.endswith("0000"):
        raw = raw[:12]
    return ":".join(raw[i:i+2] for i in range(0, len(raw), 2))


STANDARD_LATENCIES_MS = [0.15, 0.25, 0.5, 1.0, 2.0, 5.0]


def _format_latency_ms(v: float) -> str:
    if v == int(v):
        return str(int(v))
    return f"{v:g}"


def _format_supported_latencies(min_lat: float | None, max_lat: float | None) -> str:
    if min_lat is None:
        return ""
    steps = [v for v in STANDARD_LATENCIES_MS if v >= min_lat and (max_lat is None or v <= max_lat)]
    if not steps:
        return ""
    return ", ".join(_format_latency_ms(v) for v in steps) + "ms"


def _format_last_seen(last_seen: float | None) -> str:
    if last_seen is None:
        return ""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(last_seen, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _channel_matches(channel_key: int, channel_name: str, patterns: list[str]) -> bool:
    for pat in patterns:
        try:
            if int(pat) == channel_key:
                return True
        except ValueError:
            pass
        if fnmatch(channel_name.lower(), pat.lower()):
            return True
    return False


def _build_levels_with_names(levels: dict, device) -> dict:
    result = {
        "tx": {},
        "rx": {},
        "wall_time": levels.get("wall_time"),
        "source_ip": levels.get("source_ip"),
    }

    tx_names = {}
    if device.tx_channels:
        for ch in device.tx_channels.values():
            tx_names[ch.number] = ch.friendly_name or ch.name

    rx_names = {}
    if device.rx_channels:
        for ch in device.rx_channels.values():
            rx_names[ch.number] = ch.friendly_name or ch.name

    for channel_key, level in levels.get("tx", {}).items():
        result["tx"][channel_key] = {"name": tx_names.get(channel_key, ""), "level": level}

    for channel_key, level in levels.get("rx", {}).items():
        result["rx"][channel_key] = {"name": rx_names.get(channel_key, ""), "level": level}

    return result


def _format_wall_time(wall_time: float | None) -> str:
    if wall_time is None:
        return ""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(wall_time, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _collect_metering_rows(
    name: str,
    server_name: str,
    levels: dict,
    show_tx: bool,
    show_rx: bool,
    channel_patterns: list[str] | None,
    min_level: int | None = None,
    max_level: int | None = None,
) -> tuple[list[list[str]], dict]:
    rows = []
    json_data = {"tx": {}, "rx": {}}
    source_ip = levels.get("source_ip", "")
    wall_time = _format_wall_time(levels.get("wall_time"))

    for direction, key in [("TX", "tx"), ("RX", "rx")]:
        if key == "tx" and not show_tx:
            continue
        if key == "rx" and not show_rx:
            continue
        for channel_key, info in sorted(levels.get(key, {}).items(), key=lambda x: int(x[0])):
            channel_number = int(channel_key)
            channel_name = info.get("name", "")
            if channel_patterns and not _channel_matches(channel_number, channel_name, channel_patterns):
                continue
            level = info.get("level", 0)
            if min_level is not None and level < min_level:
                continue
            if max_level is not None and level > max_level:
                continue
            rows.append([name, server_name, source_ip or "", wall_time, direction, str(channel_number), channel_name, str(level)])
            json_data[key][channel_number] = {"name": channel_name, "level": level}

    return rows, json_data


@app.command("list")
def device_list():
    """List discovered Dante devices."""

    async def _run():
        from netaudio.cli import state

        devices = await _discover()
        await _populate_controls(devices)
        devices = filter_devices(devices)

        sorted_devices = list(sort_devices(devices))

        any_bluetooth = any(device.model_id in BLUETOOTH_MODEL_IDS for _, device in sorted_devices)

        compact_headers = ["Name", "IP Address", "MAC Address", "Model", "TX", "RX", "Last Seen", "Server Name"]
        verbose_extras = ["Manufacturer", "Product Version", "Board", "Firmware", "Software", "Sample Rate", "Encoding", "Bit Depth", "Latency", "Flows"]
        if any_bluetooth:
            verbose_extras.append("Bluetooth")
        verbose_headers = compact_headers + verbose_extras

        headers = verbose_headers if state.verbose else compact_headers
        rows = []
        json_data = {}

        for server_name, device in sorted_devices:
            last_seen = getattr(device, "last_seen", None)
            name_display = device.name or ""

            row = [
                name_display,
                str(device.ipv4) if device.ipv4 else "",
                _format_mac(device.mac_address),
                device.dante_model or device.model_id or "",
                str(len(device.tx_channels) if device.tx_channels else (device.tx_count or 0)),
                str(len(device.rx_channels) if device.rx_channels else (device.rx_count or 0)),
                _format_last_seen(last_seen),
                server_name,
            ]

            if state.verbose:
                row.append(device.manufacturer or "")
                row.append(device.product_version or "")
                row.append(device.board_name or device.dante_model_id or "")
                row.append(device.firmware_version or "")
                row.append(device.software_version or "")
                row.append(str(device.sample_rate or ""))

                encoding = getattr(device, "encoding", None)
                row.append(f"PCM{encoding}" if encoding is not None else "")

                bit_depth = getattr(device, "bit_depth", None)
                row.append(str(bit_depth) if bit_depth is not None else "")

                latency = getattr(device, "latency", None)
                row.append(f"{latency}ms" if latency is not None else "")

                tx_flows = getattr(device, "tx_flow_count", None)
                rx_flows = getattr(device, "rx_flow_count", None)
                if tx_flows is not None or rx_flows is not None:
                    row.append(f"{tx_flows or 0}/{rx_flows or 0}")
                else:
                    row.append("")

                if any_bluetooth:
                    row.append(device.bluetooth_device or "")

            rows.append(row)
            json_data[server_name] = DanteDeviceSerializer.to_json(device)

        output_table(headers, rows, json_data=json_data, devices=devices)

    asyncio.run(_run())


@app.command("show")
def device_show():
    """Show detailed device information."""

    async def _run():
        devices = await _discover()
        await _populate_controls(devices)
        filtered = filter_devices(devices)
        _, device = _resolve_one(filtered)
        data = DanteDeviceSerializer.to_json(device)
        output_single(data, device=device)

    asyncio.run(_run())


@app.command()
def identify():
    """Blink the identify LED on a device."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            if not filtered:
                typer.echo("Error: device not found.", err=True)
                raise typer.Exit(code=1)

            for server_name, device in filtered.items():
                packet, _, port = commands.command_identify()
                await send(packet, device.ipv4, port)
                typer.echo(f"Identified: {device.name}")

    asyncio.run(_run())


@app.command()
def reboot():
    """Reboot a device."""

    async def _run():
        devices = await _discover()
        await _populate_controls(devices)
        filtered = filter_devices(devices)
        _, device = _resolve_one(filtered)
        if not hasattr(device.commands, "command_reboot"):
            typer.echo("Error: reboot is not available in this build.", err=True)
            raise typer.Exit(code=1)
        await device.operations.reboot()
        typer.echo(f"Rebooting: {device.name}")

    asyncio.run(_run())


@app.command("factory-reset")
def factory_reset():
    """Factory reset a device (clears name, channels, routes, config)."""

    async def _run():
        devices = await _discover()
        await _populate_controls(devices)
        filtered = filter_devices(devices)
        _, device = _resolve_one(filtered)
        if not hasattr(device.commands, "command_factory_reset"):
            typer.echo("Error: factory-reset is not available in this build.", err=True)
            raise typer.Exit(code=1)
        await device.operations.factory_reset()
        typer.echo(f"Factory reset: {device.name}")

    asyncio.run(_run())


@app.command()
def name(
    new_name: Optional[str] = typer.Argument(None, help="New name (omit to get, empty string to reset)."),
):
    """Get or set device name."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            server_name, device = _resolve_one(filtered)

            if new_name is None:
                output_single(device.name)
                return

            arc_port = _get_arc_port(device)

            if new_name == "":
                packet, _ = commands.command_reset_name()
                await send(packet, device.ipv4, arc_port)
                typer.echo(f"Reset name for {server_name}")
            else:
                for sn, dev in devices.items():
                    if dev is device:
                        continue
                    if dev.name and dev.name.lower() == new_name.lower():
                        typer.echo(f"Error: name '{new_name}' already in use by {dev.name} ({sn})", err=True)
                        raise typer.Exit(code=1)

                error = validate_dante_name(new_name)
                if error:
                    typer.echo(f"Error: {error}", err=True)
                    raise typer.Exit(code=1)

                packet, _ = commands.command_set_name(new_name)
                await send(packet, device.ipv4, arc_port)
                typer.echo(f"Set name: {new_name}")

    asyncio.run(_run())


@app.command()
def clock():
    """Show PTP clock status (leader, grandmaster, sync)."""

    async def _run():
        devices = await _discover()
        await _populate_controls(devices)
        devices = filter_devices(devices)
        sorted_devices = list(sort_devices(devices))

        if not sorted_devices:
            typer.echo("No device found.", err=True)
            raise typer.Exit(code=1)

        results = {}
        for server_name, device in sorted_devices:
            results[server_name] = await device.get_clocking_status()

        leader_name = None
        leader_mac = None
        for server_name, device in sorted_devices:
            r = results[server_name]
            if r and r["clock_role"] == "leader":
                leader_name = device.name or server_name
                leader_mac = r["device_clock_mac"]
                break

        grandmaster_display = _format_mac(leader_mac) if leader_mac else ""
        if leader_name:
            grandmaster_display = f"{leader_name} ({grandmaster_display})"

        headers = ["Name", "Role", "Clock MAC", "Grandmaster", "Server Name"]
        rows = []
        json_data = {}

        for server_name, device in sorted_devices:
            r = results[server_name]
            if r is None:
                rows.append([device.name or "", "(timeout)", "", "", server_name])
                json_data[server_name] = {"error": "timeout"}
                continue

            json_entry = dict(r)
            json_entry["grandmaster_name"] = leader_name
            json_entry["grandmaster_mac"] = leader_mac

            rows.append([
                device.name or "",
                r["clock_role"],
                _format_mac(r["device_clock_mac"]),
                grandmaster_display,
                server_name,
            ])
            json_data[server_name] = json_entry

        output_table(headers, rows, json_data=json_data, devices=devices)

    asyncio.run(_run())


meter_app = typer.Typer(help="Device metering.", no_args_is_help=False, invoke_without_command=True)
app.add_typer(meter_app, name="meter")


@meter_app.callback(invoke_without_command=True)
def meter_callback(
    ctx: typer.Context,
    timeout: float = typer.Option(3.0, "--timeout", "-t", help="Seconds to wait for metering response."),
    tx: bool = typer.Option(False, "--tx", help="Show only TX channels."),
    rx: bool = typer.Option(False, "--rx", help="Show only RX channels."),
    channel: Optional[list[str]] = typer.Option(None, "--channel", "-c", help="Filter by channel number or name (fnmatch glob). Repeatable."),
    min_level: Optional[int] = typer.Option(None, "--min-level", help="Only show channels with level >= this value."),
    max_level: Optional[int] = typer.Option(None, "--max-level", help="Only show channels with level <= this value."),
):
    if ctx.invoked_subcommand is not None:
        return

    from netaudio_lib.daemon.client import get_devices_from_daemon, meter_snapshot_from_daemon

    show_tx = tx or not rx
    show_rx = rx or not tx

    async def _run():
        daemon_devices = await get_devices_from_daemon()

        if daemon_devices is not None:
            filtered = filter_devices(daemon_devices)
            if not filtered:
                typer.echo("No device found.", err=True)
                raise typer.Exit(code=1)

            ordered = list(filtered.keys())
            snapshots = await asyncio.gather(
                *(meter_snapshot_from_daemon(server_name) for server_name in ordered)
            )
            results = dict(zip(ordered, snapshots))

            all_rows = []
            all_json = {}

            for server_name, device in sorted(filtered.items(), key=lambda x: x[1].name or x[0]):
                levels = results[server_name]
                if levels is None:
                    logger.debug(f"No metering response from {device.name or server_name}")
                    continue
                rows, json_data = _collect_metering_rows(device.name or "", server_name, levels, show_tx, show_rx, channel, min_level, max_level)
                all_rows.extend(rows)
                all_json[server_name] = json_data

            if all_rows:
                headers = ["Name", "Server Name", "IP", "Timestamp", "Direction", "Channel", "Channel Name", "Level"]
                output_table(headers, all_rows, json_data=all_json)
            return

        from netaudio_lib.common.app_config import settings
        from netaudio_lib.daemon.metering import MeteringManager
        from netaudio_lib.dante.application import DanteApplication

        application = DanteApplication()
        await application.startup()

        try:
            devices = await application.discover_and_populate(timeout=settings.mdns_timeout)
            filtered = filter_devices(devices)
            if not filtered:
                typer.echo("No device found.", err=True)
                raise typer.Exit(code=1)

            metering = MeteringManager(application)
            await metering.start()

            try:
                ordered = list(filtered.keys())
                snapshots = await asyncio.gather(
                    *(metering.snapshot(server_name, timeout=timeout) for server_name in ordered)
                )
                results = dict(zip(ordered, snapshots))

                all_rows = []
                all_json = {}

                for server_name, target in sorted(filtered.items(), key=lambda x: x[1].name or x[0]):
                    levels = results[server_name]
                    if levels is None:
                        continue
                    levels = _build_levels_with_names(levels, target)
                    rows, json_data = _collect_metering_rows(target.name or "", server_name, levels, show_tx, show_rx, channel, min_level, max_level)
                    all_rows.extend(rows)
                    all_json[server_name] = json_data

                if all_rows:
                    headers = ["Name", "Server Name", "IP", "Timestamp", "Direction", "Channel", "Channel Name", "Level"]
                    output_table(headers, all_rows, json_data=all_json)
            finally:
                await metering.stop()
        finally:
            await application.shutdown()

    asyncio.run(_run())


@meter_app.command()
def start():
    """Start persistent metering (requires daemon)."""
    from netaudio_lib.daemon.client import meter_start_on_daemon

    async def _run():
        devices = await _discover()
        filtered = filter_devices(devices)
        if not filtered:
            typer.echo("No device found.", err=True)
            raise typer.Exit(code=1)

        for server_name in filtered:
            await meter_start_on_daemon(server_name, "cli")

    asyncio.run(_run())


@meter_app.command()
def stop():
    """Stop persistent metering (requires daemon)."""
    from netaudio_lib.daemon.client import meter_stop_on_daemon

    async def _run():
        devices = await _discover()
        filtered = filter_devices(devices)
        if not filtered:
            typer.echo("No device found.", err=True)
            raise typer.Exit(code=1)

        for server_name in filtered:
            await meter_stop_on_daemon(server_name, "cli")

    asyncio.run(_run())


@meter_app.command(name="measure-timeout")
def measure_timeout(
    gap: float = typer.Option(15.0, "--gap", "-g", help="Seconds of silence before declaring stream ended."),
    max_wait: float = typer.Option(120.0, "--max-wait", help="Maximum seconds to listen."),
):
    """Measure how long a device streams metering after a single start command."""
    import socket
    import struct

    from netaudio_lib.common.app_config import settings as app_settings
    from netaudio_lib.dante.const import MULTICAST_GROUP_CONTROL_MONITORING
    from netaudio_lib.dante.application import DanteApplication

    async def _run():
        application = DanteApplication()
        await application.startup()

        try:
            devices = await application.discover_and_populate(timeout=app_settings.mdns_timeout)
            filtered = filter_devices(devices)
            if not filtered:
                typer.echo("No device found.", err=True)
                raise typer.Exit(code=1)

            metering_port = app_settings.metering_port

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.bind(("", metering_port))

            mreq = struct.pack(
                "4s4s",
                socket.inet_aton(MULTICAST_GROUP_CONTROL_MONITORING),
                socket.inet_aton("0.0.0.0"),
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

            loop = asyncio.get_running_loop()
            timestamps_by_ip: dict[str, list[float]] = {}
            device_names: dict[str, str] = {}

            class TimingProtocol(asyncio.DatagramProtocol):
                def datagram_received(self, data, addr):
                    source_ip = addr[0]
                    now = time.monotonic()
                    if source_ip not in timestamps_by_ip:
                        timestamps_by_ip[source_ip] = []
                    timestamps_by_ip[source_ip].append(now)

            transport, _ = await loop.create_datagram_endpoint(
                TimingProtocol,
                sock=sock,
            )

            try:
                host_ip_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    host_ip_sock.connect(("224.0.0.231", 1))
                    host_ip = host_ip_sock.getsockname()[0]
                finally:
                    host_ip_sock.close()

                host_mac = application.cmc._host_mac

                for server_name, device in filtered.items():
                    device_ip = str(device.ipv4)
                    device_name = device.name or server_name
                    device_names[device_ip] = device_name
                    typer.echo(f"Sending single metering start to {device_name} ({device_ip})")
                    application.cmc.start_metering(
                        device_ip, device_name, host_ip, host_mac, metering_port,
                    )

                start_time = time.monotonic()
                last_any_packet = start_time

                while True:
                    await asyncio.sleep(0.5)
                    elapsed = time.monotonic() - start_time
                    now = time.monotonic()

                    all_timestamps = []
                    for timestamps in timestamps_by_ip.values():
                        all_timestamps.extend(timestamps)

                    if all_timestamps:
                        last_any_packet = max(all_timestamps)

                    silence = now - last_any_packet
                    total_packets = sum(len(timestamps) for timestamps in timestamps_by_ip.values())

                    if all_timestamps and silence >= gap:
                        typer.echo(f"\nNo packets for {silence:.1f}s — stream ended.")
                        break

                    if elapsed >= max_wait:
                        if all_timestamps:
                            typer.echo(f"\nMax wait reached ({max_wait}s) — still receiving packets.")
                        else:
                            typer.echo(f"\nMax wait reached ({max_wait}s) — no packets received.")
                        break

                    if int(elapsed) % 5 == 0 and elapsed > 0 and abs(elapsed - int(elapsed)) < 0.5:
                        typer.echo(f"  {elapsed:.0f}s elapsed, {total_packets} packets, last packet {silence:.1f}s ago")

                typer.echo("")
                for source_ip, timestamps in sorted(timestamps_by_ip.items()):
                    device_name = device_names.get(source_ip, source_ip)
                    count = len(timestamps)
                    if count == 0:
                        continue

                    duration = timestamps[-1] - timestamps[0]
                    first_offset = timestamps[0] - start_time

                    gaps = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps) - 1)]
                    average_gap = sum(gaps) / len(gaps) if gaps else 0
                    max_gap = max(gaps) if gaps else 0
                    min_gap = min(gaps) if gaps else 0

                    typer.echo(f"{device_name} ({source_ip}):")
                    typer.echo(f"  Packets:       {count}")
                    typer.echo(f"  First packet:  {first_offset:.2f}s after start")
                    typer.echo(f"  Duration:      {duration:.2f}s")
                    typer.echo(f"  Avg interval:  {average_gap*1000:.1f}ms")
                    typer.echo(f"  Min interval:  {min_gap*1000:.1f}ms")
                    typer.echo(f"  Max interval:  {max_gap*1000:.1f}ms")
                    typer.echo(f"  Rate:          {count/duration:.1f} packets/sec" if duration > 0 else "")
                    typer.echo("")

                for server_name, device in filtered.items():
                    device_ip = str(device.ipv4)
                    device_name = device.name or server_name
                    application.cmc.stop_metering(
                        device_ip, device_name, host_ip, host_mac, metering_port,
                    )

            finally:
                transport.close()
        finally:
            await application.shutdown()

    asyncio.run(_run())


@meter_app.command()
def status():
    """Show which devices have persistent metering active."""
    from netaudio_lib.daemon.client import meter_status_from_daemon

    async def _run():
        result = await meter_status_from_daemon()
        if result is None:
            typer.echo("Daemon is not running.", err=True)
            raise typer.Exit(code=1)

        if not result:
            typer.echo("No devices are being metered.")
            return

        headers = ["Name", "Server Name", "Online", "Receiving"]
        rows = []
        json_data = {}

        for server_name, info in sorted(result.items(), key=lambda x: x[1].get("name", "")):
            receiving = info.get("receiving", False)
            online = info.get("online", False)
            rows.append([
                info.get("name", ""),
                server_name,
                "yes" if online else "no",
                "yes" if receiving else "no",
            ])
            json_data[server_name] = info

        output_table(headers, rows, json_data=json_data)

    asyncio.run(_run())
