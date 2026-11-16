from __future__ import annotations

import asyncio
import os
import sys
import uuid
from typing import Optional

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import ansi, run_with_devices
from netaudio.cli_support.output import output_single, output_table, structured_output_selected
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.commands.device.display import _channel_matches
from netaudio.commands.meter import tui
from netaudio.commands.meter.models import METER_DISPLAY_FLOOR_DBFS, format_meter_sample
from netaudio.commands.meter.tui import MeterViewOptions
from netaudio.daemon import client as daemon_client
from netaudio.dante.metering import classify_signal_presence, metering_value_dbfs
from netaudio.icons import icon

meter_app = typer.Typer(
    help="Device metering.", no_args_is_help=False, invoke_without_command=True, context_settings=HELP_CONTEXT_SETTINGS
)


def _render_meter_bar(level: int, source: str | None, bar_width: int = 32) -> str:
    state = classify_signal_presence(level, source)
    dbfs = metering_value_dbfs(level, source)
    if state == "clipping":
        return ansi("31", "█" * bar_width + "  CLIP")

    if dbfs is None:
        label = "Muted" if state == "muted" else "Unknown"
        return ansi("90", "░" * bar_width + f"  {label}")

    fraction = (dbfs - METER_DISPLAY_FLOOR_DBFS) / -METER_DISPLAY_FLOOR_DBFS
    filled = round(max(0.0, min(1.0, fraction)) * bar_width)
    empty = bar_width - filled

    if dbfs >= -3:
        color_code = "31"
    elif dbfs >= -12:
        color_code = "33"
    else:
        color_code = "32"

    return f"{ansi(color_code, '█' * filled)}{ansi('90', '░' * empty)} {dbfs:.1f} dBFS"


def _direction_shown(key: str, options: MeterViewOptions) -> bool:
    return options.show_tx if key == "tx" else options.show_rx


def _render_meter_display(device_levels: list[tuple[str, str, dict]], options: MeterViewOptions) -> str:
    lines = []
    bar_width = 32

    max_name_width = 2
    for _, _, levels in device_levels:
        for key in ("tx", "rx"):
            if not _direction_shown(key, options):
                continue
            for channel_key, info in levels.get(key, {}).items():
                channel_number = int(channel_key)
                channel_name = info.get("name", "")
                if options.channel_patterns and not _channel_matches(
                    channel_number, channel_name, options.channel_patterns
                ):
                    continue
                display_name = channel_name or f"Ch {channel_number}"
                max_name_width = max(max_name_width, len(display_name))

    for device_name, source_ip, levels in device_levels:
        source = levels.get("metering_source") or "unknown"
        lines.append(f"{ansi('1', device_name)} {ansi('90', f'({source_ip}) [{source}]')}")

        for direction, key, color_code in [("TX", "tx", "36"), ("RX", "rx", "35")]:
            if not _direction_shown(key, options):
                continue

            channels = levels.get(key, {})
            if not channels:
                continue

            sorted_channels = sorted(channels.items(), key=lambda channel_entry: int(channel_entry[0]))

            for channel_key, info in sorted_channels:
                channel_number = int(channel_key)
                channel_name = info.get("name", "")

                if options.channel_patterns and not _channel_matches(
                    channel_number, channel_name, options.channel_patterns
                ):
                    continue

                level = info.get("level", 254)
                bar = _render_meter_bar(level, source, bar_width)
                display_name = channel_name or f"Ch {channel_number}"

                lines.append(
                    f"  {ansi(color_code, direction)} {channel_number:>3} {ansi('90', f'{display_name:<{max_name_width}}')} {bar}"
                )

    return "\n".join(lines)


async def _read_fresh_cache(ordered: list[str], timeout: float, detailed: bool) -> dict[str, dict]:
    client_id = f"meter_snapshot:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    attempted = list(ordered) if detailed else []
    try:
        if attempted:
            await asyncio.gather(
                *(daemon_client.meter_start_on_daemon(server_name, client_id) for server_name in attempted),
                return_exceptions=True,
            )

        deadline = asyncio.get_running_loop().time() + max(0.0, timeout)
        selected: dict[str, dict] = {}
        while True:
            cache = await daemon_client.meter_cache_from_daemon()
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
            failed_stops = await tui.stop_metering_attempts(
                list(reversed(attempted)),
                client_id,
                daemon_client.meter_stop_on_daemon,
            )
            if failed_stops:
                typer.echo(
                    "Warning: detailed metering cleanup was not acknowledged for " + ", ".join(failed_stops),
                    err=True,
                )


def _filter_json_levels(levels: dict, options: MeterViewOptions) -> dict:
    filtered_levels = {key: value for key, value in levels.items() if key not in ("tx", "rx")}
    filtered_levels["tx"] = {}
    filtered_levels["rx"] = {}
    for direction in ("tx", "rx"):
        if not _direction_shown(direction, options):
            continue
        for channel_number, info in levels.get(direction, {}).items():
            channel_name = info.get("name", "")
            if options.channel_patterns and not _channel_matches(
                int(channel_number), channel_name, options.channel_patterns
            ):
                continue
            filtered_levels[direction][channel_number] = info
    return filtered_levels


async def run_meter(devices, options: MeterViewOptions, timeout: float, snapshot: bool) -> None:
    from netaudio.cli import OutputFormat
    from netaudio.cli import state as cli_state

    structured_output = cli_state.output_format in (OutputFormat.json, OutputFormat.xml, OutputFormat.yaml)
    filtered = dict(select_device(filter_devices(devices), allow_many=True))
    ordered = list(filtered)

    if snapshot or structured_output:
        device_levels = []
        all_json = {}
        cache = await _read_fresh_cache(ordered, timeout, options.detailed)
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
            all_json[server_name] = _filter_json_levels(levels, options)

        if not device_levels:
            typer.echo("No fresh metering data received.", err=True)
            raise typer.Exit(code=1)
        if structured_output:
            output_single(all_json)
        else:
            typer.echo(_render_meter_display(device_levels, options))
        return

    if not sys.stdin.isatty() or not sys.stdout.isatty():
        typer.echo("Interactive meter requires a TTY; use --snapshot or -o json.", err=True)
        raise typer.Exit(code=1)

    await tui.run_meter_tui(filtered, options)


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
            "Request detailed metering immediately for every selected device. By default, the interactive TUI uses "
            "advertised per-channel signal presence when available and otherwise requests detailed metering only "
            "from devices that advertise it."
        ),
    ),
):
    if ctx.invoked_subcommand is not None:
        return

    from netaudio.cli import state as cli_state

    options = MeterViewOptions(
        channel_patterns=channel,
        detailed=detailed,
        no_color=cli_state.no_color,
        show_rx=rx or not tx,
        show_tx=tx or not rx,
    )
    run_with_devices(run_meter, options, timeout, snapshot)


async def run_meter_start(devices) -> None:
    for server_name, device in select_device(filter_devices(devices), allow_many=True):
        await daemon_client.meter_start_on_daemon(server_name, "cli")
        typer.echo(f"{icon('meter')}Metering started for {device.name or server_name}.")


@meter_app.command()
def start():
    """Start persistent metering."""
    run_with_devices(run_meter_start)


async def run_meter_stop(devices) -> None:
    for server_name, device in select_device(filter_devices(devices), allow_many=True):
        await daemon_client.meter_stop_on_daemon(server_name, "cli")
        typer.echo(f"{icon('meter')}Metering stopped for {device.name or server_name}.")


@meter_app.command()
def stop():
    """Stop persistent metering."""
    run_with_devices(run_meter_stop)


async def run_meter_status() -> None:
    result = await daemon_client.meter_status_from_daemon()
    if result is None:
        typer.echo(f"{icon('offline')}Daemon is not running.", err=True)
        raise typer.Exit(code=1)

    if not result:
        if structured_output_selected():
            output_single({})
        else:
            typer.echo(f"{icon('meter')}No devices are being metered.")
        return

    headers = ["Name", "Server Name", "Online", "Receiving"]
    rows = []
    json_data = {}

    for server_name, info in sorted(result.items(), key=lambda server_entry: server_entry[1].get("name", "")):
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


@meter_app.command()
def status():
    """Show which devices have persistent metering active."""
    asyncio.run(run_meter_status())
