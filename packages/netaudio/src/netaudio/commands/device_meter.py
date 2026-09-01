from __future__ import annotations

import asyncio
import time
from typing import Optional

import typer

from netaudio._common_cli import HELP_CONTEXT_SETTINGS

from netaudio._common import _discover
from netaudio._common_output import output_table
from netaudio._common_selection import filter_devices
from netaudio.commands.device_display import _channel_matches
from netaudio.icons import icon


meter_app = typer.Typer(
    help="Device metering.", no_args_is_help=False, invoke_without_command=True, context_settings=HELP_CONTEXT_SETTINGS
)


def _render_meter_bar(level: int, bar_width: int = 32, no_color: bool = False) -> str:
    from netaudio._common import ansi

    if level >= 254:
        return ansi("90", "░" * bar_width + "  --")

    amplitude = 254 - level
    filled = round(amplitude / 254 * bar_width)
    filled = max(0, min(bar_width, filled))
    empty = bar_width - filled

    if amplitude > 220:
        color_code = "31"
    elif amplitude > 180:
        color_code = "33"
    else:
        color_code = "32"

    return f"{ansi(color_code, '█' * filled)}{ansi('90', '░' * empty)} {level:>3}"


def _render_meter_display(
    device_levels: list[tuple[str, str, dict]],
    show_tx: bool,
    show_rx: bool,
    channel_patterns: list[str] | None,
    no_color: bool,
) -> str:
    from netaudio._common import ansi

    lines = []
    bar_width = 32

    max_name_width = 2
    for _, _, levels in device_levels:
        for key in ("tx", "rx"):
            if key == "tx" and not show_tx:
                continue
            if key == "rx" and not show_rx:
                continue
            for channel_key, info in levels.get(key, {}).items():
                channel_number = int(channel_key)
                channel_name = info.get("name", "")
                if channel_patterns and not _channel_matches(channel_number, channel_name, channel_patterns):
                    continue
                display_name = channel_name or f"Ch {channel_number}"
                max_name_width = max(max_name_width, len(display_name))

    for device_name, source_ip, levels in device_levels:
        source = levels.get("metering_source") or "unknown"
        lines.append(f"{ansi('1', device_name)} {ansi('90', f'({source_ip}) [{source}]')}")

        for direction, key, color_code in [("TX", "tx", "36"), ("RX", "rx", "35")]:
            if key == "tx" and not show_tx:
                continue
            if key == "rx" and not show_rx:
                continue

            channels = levels.get(key, {})
            if not channels:
                continue

            sorted_channels = sorted(channels.items(), key=lambda x: int(x[0]))

            for channel_key, info in sorted_channels:
                channel_number = int(channel_key)
                channel_name = info.get("name", "")

                if channel_patterns and not _channel_matches(channel_number, channel_name, channel_patterns):
                    continue

                level = info.get("level", 254)
                bar = _render_meter_bar(level, bar_width, no_color)
                display_name = channel_name or f"Ch {channel_number}"

                lines.append(
                    f"  {ansi(color_code, direction)} {channel_number:>3} {ansi('90', f'{display_name:<{max_name_width}}')} {bar}"
                )

    return "\n".join(lines)


@meter_app.callback(invoke_without_command=True)
def meter_callback(
    ctx: typer.Context,
    timeout: float = typer.Option(3.0, "--timeout", "-t", help="Seconds to wait for fresh metering data."),
    tx: bool = typer.Option(False, "--tx", help="Show only TX channels."),
    rx: bool = typer.Option(False, "--rx", help="Show only RX channels."),
    channel: Optional[list[str]] = typer.Option(
        None, "--channel", "-c", help="Filter by channel number or name (fnmatch glob). Repeatable."
    ),
    snapshot: bool = typer.Option(False, "--snapshot", help="Take a single snapshot instead of live display."),
    detailed: bool = typer.Option(
        False,
        "--detailed",
        help=(
            "Request detailed metering for every selected device. In the interactive TUI, AVIO monitoring stays "
            "passive while known detailed-only devices such as lx-dante and Ferrofish A32 are started automatically."
        ),
    ),
):
    if ctx.invoked_subcommand is not None:
        return

    from netaudio.cli import OutputFormat, state as cli_state

    show_tx = tx or not rx
    show_rx = rx or not tx
    use_json = cli_state.output_format == OutputFormat.json
    no_color = cli_state.no_color

    async def _run():
        from netaudio.daemon.client import (
            get_devices_from_daemon,
            meter_cache_from_daemon,
            meter_start_on_daemon,
            meter_stop_on_daemon,
        )
        from netaudio.commands.meter_tui import format_meter_sample, run_meter_tui, stop_metering_attempts

        devices = await get_devices_from_daemon()
        if not devices:
            typer.echo("Daemon not running. Start the daemon first: netaudio daemon start", err=True)
            raise typer.Exit(code=1)

        filtered = filter_devices(devices)
        if not filtered:
            typer.echo("No device found.", err=True)
            raise typer.Exit(code=1)

        ordered = list(filtered.keys())

        async def _read_fresh_cache() -> dict[str, dict]:
            import os
            import uuid

            client_id = f"meter_snapshot:{os.getpid()}:{uuid.uuid4().hex[:8]}"
            attempted = list(ordered) if detailed else []
            try:
                if attempted:
                    await asyncio.gather(
                        *(meter_start_on_daemon(server_name, client_id) for server_name in attempted),
                        return_exceptions=True,
                    )

                deadline = asyncio.get_running_loop().time() + max(0.0, timeout)
                selected: dict[str, dict] = {}
                while True:
                    cache = await meter_cache_from_daemon()
                    if cache is not None:
                        selected = {
                            server_name: sample
                            for server_name in ordered
                            if isinstance((sample := cache.get(server_name)), dict)
                            and (not detailed or sample.get("metering_source") == "detailed")
                        }
                    if selected and (not detailed or len(selected) == len(ordered)):
                        return selected
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        return selected
                    await asyncio.sleep(min(0.1, remaining))
            finally:
                if attempted:
                    failed_stops = await stop_metering_attempts(
                        list(reversed(attempted)),
                        client_id,
                        meter_stop_on_daemon,
                    )
                    if failed_stops:
                        typer.echo(
                            "Warning: detailed metering cleanup was not acknowledged for " + ", ".join(failed_stops),
                            err=True,
                        )

        def _filter_json_levels(levels: dict) -> dict:
            filtered_levels = {key: value for key, value in levels.items() if key not in ("tx", "rx")}
            filtered_levels["tx"] = {}
            filtered_levels["rx"] = {}
            for direction, enabled in (("tx", show_tx), ("rx", show_rx)):
                if not enabled:
                    continue
                for channel_number, info in levels.get(direction, {}).items():
                    channel_name = info.get("name", "")
                    if channel and not _channel_matches(int(channel_number), channel_name, channel):
                        continue
                    filtered_levels[direction][channel_number] = info
            return filtered_levels

        if snapshot or use_json:
            device_levels = []
            all_json = {}
            cache = await _read_fresh_cache()
            for server_name in sorted(
                ordered,
                key=lambda candidate_server_name: filtered[candidate_server_name].name or candidate_server_name,
            ):
                target = filtered[server_name]
                levels = format_meter_sample(target, cache.get(server_name))
                if not levels:
                    continue
                source_ip = levels.get("source_ip", "")
                device_levels.append((target.name or server_name, source_ip, levels))
                all_json[server_name] = _filter_json_levels(levels)

            if not device_levels:
                typer.echo("No fresh metering data received.", err=True)
                raise typer.Exit(code=1)
            if use_json:
                import json as json_module

                typer.echo(json_module.dumps(all_json, indent=2))
            else:
                typer.echo(_render_meter_display(device_levels, show_tx, show_rx, channel, no_color))
            return

        import sys

        if not sys.stdin.isatty() or not sys.stdout.isatty():
            typer.echo("Interactive meter requires a TTY; use --snapshot or --json.", err=True)
            raise typer.Exit(code=1)

        await run_meter_tui(
            filtered,
            show_tx=show_tx,
            show_rx=show_rx,
            channel_patterns=channel,
            detailed=detailed,
            no_color=no_color,
        )

    asyncio.run(_run())


@meter_app.command()
def start():
    """Start persistent metering (requires daemon)."""
    from netaudio.daemon.client import meter_start_on_daemon

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
    from netaudio.daemon.client import meter_stop_on_daemon

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

    from netaudio.common.app_config import settings as app_settings
    from netaudio.dante.const import MULTICAST_GROUP_CONTROL_MONITORING
    from netaudio.dante.application import DanteApplication

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

                host_mac = application.cmc.host_media_access_control_address

                for server_name, device in filtered.items():
                    device_ip = str(device.ipv4)
                    device_name = device.name or server_name
                    device_names[device_ip] = device_name
                    typer.echo(f"Sending single metering start to {device_name} ({device_ip})")
                    application.cmc.start_metering(
                        device_ip,
                        device_name,
                        host_ip,
                        host_mac,
                        metering_port,
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

                    gaps = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
                    average_gap = sum(gaps) / len(gaps) if gaps else 0
                    max_gap = max(gaps) if gaps else 0
                    min_gap = min(gaps) if gaps else 0

                    typer.echo(f"{device_name} ({source_ip}):")
                    typer.echo(f"  Packets:       {count}")
                    typer.echo(f"  First packet:  {first_offset:.2f}s after start")
                    typer.echo(f"  Duration:      {duration:.2f}s")
                    typer.echo(f"  Avg interval:  {average_gap * 1000:.1f}ms")
                    typer.echo(f"  Min interval:  {min_gap * 1000:.1f}ms")
                    typer.echo(f"  Max interval:  {max_gap * 1000:.1f}ms")
                    typer.echo(f"  Rate:          {count / duration:.1f} packets/sec" if duration > 0 else "")
                    typer.echo("")

                for server_name, device in filtered.items():
                    device_ip = str(device.ipv4)
                    device_name = device.name or server_name
                    application.cmc.stop_metering(
                        device_ip,
                        device_name,
                        host_ip,
                        host_mac,
                        metering_port,
                    )

            finally:
                transport.close()
        finally:
            await application.shutdown()

    asyncio.run(_run())


@meter_app.command()
def status():
    """Show which devices have persistent metering active."""
    from netaudio.daemon.client import meter_status_from_daemon

    async def _run():
        result = await meter_status_from_daemon()
        if result is None:
            typer.echo(f"{icon('offline')}Daemon is not running.", err=True)
            raise typer.Exit(code=1)

        if not result:
            typer.echo(f"{icon('meter')}No devices are being metered.")
            return

        headers = ["Name", "Server Name", "Online", "Receiving"]
        rows = []
        json_data = {}

        for server_name, info in sorted(result.items(), key=lambda x: x[1].get("name", "")):
            receiving = info.get("receiving", False)
            online = info.get("online", False)
            rows.append(
                [
                    info.get("name", ""),
                    server_name,
                    "yes" if online else "no",
                    "yes" if receiving else "no",
                ]
            )
            json_data[server_name] = info

        output_table(headers, rows, json_data=json_data)

    asyncio.run(_run())
