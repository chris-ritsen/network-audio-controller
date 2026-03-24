from __future__ import annotations

import asyncio
import logging
import time
from fnmatch import fnmatch
from typing import Optional

import typer

logger = logging.getLogger("netaudio")

from netaudio_lib.dante.const import BLUETOOTH_MODEL_IDS, HEARTBEAT_LOCK_UNRELIABLE_MODEL_IDS
from netaudio_lib.dante.device_commands import DanteDeviceCommands
from netaudio_lib.dante.device_operations import _device_lock_operation, LOCK_OPERATION_LOCK, LOCK_OPERATION_UNLOCK, validate_dante_name, validate_pin
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
from netaudio.icons import icon, icon_only

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





def _get_lock_key() -> bytes:
    from netaudio_lib.common.app_config import settings as app_settings

    if app_settings.device_lock_key:
        return app_settings.device_lock_key

    from netaudio_lib.common.config_loader import config_search_paths, load_capture_profile
    profile_cfg, _ = load_capture_profile(None, None)
    lock_key_value = profile_cfg.get("device_lock_key")
    if lock_key_value:
        key = lock_key_value.encode("ascii")
        app_settings.device_lock_key = key
        return key

    from netaudio_lib.common.key_extract import extract_lock_key, find_dante_controller_binary
    binary_path = find_dante_controller_binary()
    if binary_path:
        typer.echo(f"Dante Controller found: {binary_path}", err=True)
        typer.echo("Lock/unlock requires a key from your Dante Controller installation.", err=True)
        typer.echo("Extract it? [Y/n] ", err=True, nl=False)
        answer = input().strip().lower()
        if answer in ("", "y", "yes"):
            key = extract_lock_key()
            if key:
                app_settings.device_lock_key = key
                return key
            typer.echo("Error: could not extract key from Dante Controller binary.", err=True)
            raise typer.Exit(code=1)

    typer.echo("Error: device lock requires a key.", err=True)
    typer.echo("", err=True)
    typer.echo("Options:", err=True)
    typer.echo("  1. Install Dante Controller — key is extracted automatically", err=True)
    typer.echo("  2. Set NETAUDIO_DEVICE_LOCK_KEY environment variable", err=True)
    typer.echo("  3. Add device_lock_key to config.toml:", err=True)
    typer.echo("", err=True)
    for search_path in config_search_paths():
        exists = search_path.exists()
        marker = "*" if exists else " "
        typer.echo(f"     {marker} {search_path}", err=True)
    raise typer.Exit(code=1)


@app.command("list")
def device_list(
    json_flag: bool = typer.Option(False, "-j", "--json", help="Shorthand for --output=json."),
):
    """List discovered Dante devices."""

    async def _run():
        from netaudio.cli import OutputFormat, state
        if json_flag:
            state.output_format = OutputFormat.json

        devices = await _discover()
        await _populate_controls(devices)
        _collect_lock_state(devices)
        devices = filter_devices(devices)

        sorted_devices = list(sort_devices(devices))

        any_bluetooth = any(device.model_id in BLUETOOTH_MODEL_IDS for _, device in sorted_devices)

        compact_headers = ["Name", "IP Address", "MAC Address", "Model", "Lock", "TX", "RX", "Last Seen", "Server Name"]
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

            if device.is_locked is True:
                lock_display = icon("lock") or "locked"
            elif device.is_locked is False:
                lock_display = ""
            else:
                lock_display = ""

            row = [
                name_display,
                str(device.ipv4) if device.ipv4 else "",
                _format_mac(device.mac_address),
                device.dante_model or device.model_id or "",
                lock_display,
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
                typer.echo(f"{icon('identify')}Identified: {device.name}")

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
        typer.echo(f"{icon('reboot')}Rebooting: {device.name}")

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
        typer.echo(f"{icon('factory_reset')}Factory reset: {device.name}")

    asyncio.run(_run())


from netaudio.commands.config import app as device_config_app
app.add_typer(device_config_app, name="config")

lock_app = typer.Typer(help="Device lock management.", no_args_is_help=True)
app.add_typer(lock_app, name="lock")



@lock_app.command("set")
def lock_set(
    pin: str = typer.Argument(..., help="4-digit numeric PIN to lock the device with."),
):
    """Lock a device with a PIN."""

    async def _run():
        lock_key = _get_lock_key()

        error = validate_pin(pin)
        if error:
            typer.echo(f"Error: {error}", err=True)
            raise typer.Exit(code=1)

        device_ip = await _resolve_lock_ip()

        result = await _device_lock_operation(device_ip, pin, lock_key, LOCK_OPERATION_LOCK)

        if result["already"]:
            typer.echo("already locked", err=True)
        elif not result["success"]:
            typer.echo(f"Error: lock failed (status 0x{result['status']:04x})", err=True)
            raise typer.Exit(code=1)

    asyncio.run(_run())


@lock_app.command("clear")
def lock_clear(
    pin: str = typer.Argument(..., help="4-digit numeric PIN to unlock the device."),
):
    """Unlock a device with its PIN."""

    async def _run():
        lock_key = _get_lock_key()

        error = validate_pin(pin)
        if error:
            typer.echo(f"Error: {error}", err=True)
            raise typer.Exit(code=1)

        device_ip = await _resolve_lock_ip()

        result = await _device_lock_operation(device_ip, pin, lock_key, LOCK_OPERATION_UNLOCK)

        if result["already"]:
            typer.echo("already unlocked", err=True)
        elif not result["success"]:
            typer.echo(f"Error: unlock failed (status 0x{result['status']:04x})", err=True)
            raise typer.Exit(code=1)

    asyncio.run(_run())


@lock_app.command("status")
def lock_status():
    """Show device lock status."""

    async def _run():
        from netaudio_lib.dante.services.heartbeat import _parse_lock_state
        import socket
        import struct
        import time

        from netaudio_lib.common.app_config import settings as app_settings
        from netaudio_lib.dante.const import DEVICE_HEARTBEAT_PORT, MULTICAST_GROUP_HEARTBEAT

        devices = await _discover()
        filtered = filter_devices(devices)

        if not filtered:
            typer.echo("Error: no devices found.", err=True)
            raise typer.Exit(code=1)

        device_ips = {}
        for server_name, device in filtered.items():
            if device.ipv4 and device.online:
                device_ips[str(device.ipv4)] = (server_name, device)

        multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        multicast_socket.bind(("", DEVICE_HEARTBEAT_PORT))
        membership = struct.pack(
            "4s4s",
            socket.inet_aton(MULTICAST_GROUP_HEARTBEAT),
            socket.inet_aton("0.0.0.0"),
        )
        multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
        multicast_socket.settimeout(0.5)

        deadline = time.monotonic() + app_settings.lock_state_timeout
        seen = set()
        try:
            while len(seen) < len(device_ips) and time.monotonic() < deadline:
                try:
                    data, addr = multicast_socket.recvfrom(4096)
                except TimeoutError:
                    break
                source_ip = addr[0]
                if source_ip in device_ips and source_ip not in seen:
                    seen.add(source_ip)
                    server_name, device = device_ips[source_ip]
                    if getattr(device, "model_id", None) not in HEARTBEAT_LOCK_UNRELIABLE_MODEL_IDS:
                        lock_state = _parse_lock_state(data)
                        device.is_locked = lock_state if lock_state is not None else False
        finally:
            multicast_socket.close()

        from netaudio.cli import state as cli_state

        headers = ["Name", "IP Address", "Lock Status"]
        rows = []
        json_data = {}

        for server_name, device in sort_devices(filtered):
            if device.is_locked is True:
                status_display = f"{icon('lock')}locked"
            elif device.is_locked is False:
                status_display = f"{icon('unlock')}unlocked"
            else:
                status_display = "unknown"

            rows.append([
                device.name or "",
                str(device.ipv4) if device.ipv4 else "",
                status_display,
            ])
            json_data[server_name] = {
                "name": device.name,
                "ipv4": str(device.ipv4),
                "is_locked": device.is_locked,
            }

        output_table(headers, rows, json_data=json_data)

    asyncio.run(_run())


def _collect_lock_state(devices: dict) -> None:
    import socket
    import struct
    import time
    from netaudio_lib.dante.const import DEVICE_HEARTBEAT_PORT, MULTICAST_GROUP_HEARTBEAT
    from netaudio_lib.dante.services.heartbeat import _parse_lock_state

    device_ips = {}
    for server_name, device in devices.items():
        if device.ipv4 and device.online:
            device_ips[str(device.ipv4)] = device

    if not device_ips:
        return

    multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    multicast_socket.bind(("", DEVICE_HEARTBEAT_PORT))
    membership = struct.pack(
        "4s4s",
        socket.inet_aton(MULTICAST_GROUP_HEARTBEAT),
        socket.inet_aton("0.0.0.0"),
    )
    multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
    multicast_socket.settimeout(0.5)

    from netaudio_lib.common.app_config import settings as app_settings
    deadline = time.monotonic() + app_settings.lock_state_timeout
    seen = set()
    try:
        while len(seen) < len(device_ips) and time.monotonic() < deadline:
            try:
                data, addr = multicast_socket.recvfrom(4096)
            except TimeoutError:
                break
            source_ip = addr[0]
            if source_ip in device_ips and source_ip not in seen:
                seen.add(source_ip)
                device = device_ips[source_ip]
                if getattr(device, "model_id", None) in HEARTBEAT_LOCK_UNRELIABLE_MODEL_IDS:
                    continue
                lock_state = _parse_lock_state(data)
                if lock_state is not None:
                    device.is_locked = lock_state
                else:
                    device.is_locked = False
    finally:
        multicast_socket.close()


async def _resolve_lock_ip() -> str:
    from netaudio.cli import state

    if state.hosts:
        return state.hosts[0]

    devices = await _discover()
    filtered = filter_devices(devices)
    _, device = _resolve_one(filtered)
    return str(device.ipv4)


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
                typer.echo(f"{icon('name')}Reset name for {server_name}")
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
                typer.echo(f"{icon('name')}Set name: {new_name}")

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


from netaudio.commands.flow import app as flow_app
app.add_typer(flow_app, name="flow")

meter_app = typer.Typer(help="Device metering.", no_args_is_help=False, invoke_without_command=True)
app.add_typer(meter_app, name="meter")


def _render_meter_bar(level: int, bar_width: int = 32, no_color: bool = False) -> str:
    if level >= 254:
        if no_color:
            return "░" * bar_width + "  --"
        return f"\033[90m{'░' * bar_width}  --\033[0m"

    amplitude = 254 - level
    filled = round(amplitude / 254 * bar_width)
    filled = max(0, min(bar_width, filled))
    empty = bar_width - filled

    if no_color:
        return "█" * filled + "░" * empty + f" {level:>3}"

    if amplitude > 220:
        bar_color = "\033[31m"
    elif amplitude > 180:
        bar_color = "\033[33m"
    else:
        bar_color = "\033[32m"

    return f"{bar_color}{'█' * filled}\033[90m{'░' * empty}\033[0m {level:>3}"


def _render_meter_display(
    device_levels: list[tuple[str, str, dict]],
    show_tx: bool,
    show_rx: bool,
    channel_patterns: list[str] | None,
    no_color: bool,
) -> str:
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
        if no_color:
            lines.append(f"{device_name} ({source_ip})")
        else:
            lines.append(f"\033[1m{device_name}\033[0m \033[90m({source_ip})\033[0m")

        for direction, key, label_color in [("TX", "tx", "\033[36m"), ("RX", "rx", "\033[35m")]:
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

                if no_color:
                    lines.append(f"  {direction} {channel_number:>3} {display_name:<{max_name_width}} {bar}")
                else:
                    lines.append(f"  {label_color}{direction}\033[0m {channel_number:>3} \033[90m{display_name:<{max_name_width}}\033[0m {bar}")

    return "\n".join(lines)


@meter_app.callback(invoke_without_command=True)
def meter_callback(
    ctx: typer.Context,
    timeout: float = typer.Option(3.0, "--timeout", "-t", help="Seconds to wait for initial metering response."),
    tx: bool = typer.Option(False, "--tx", help="Show only TX channels."),
    rx: bool = typer.Option(False, "--rx", help="Show only RX channels."),
    channel: Optional[list[str]] = typer.Option(None, "--channel", "-c", help="Filter by channel number or name (fnmatch glob). Repeatable."),
    snapshot: bool = typer.Option(False, "--snapshot", help="Take a single snapshot instead of live display."),
):
    if ctx.invoked_subcommand is not None:
        return

    from netaudio.cli import OutputFormat, state as cli_state

    show_tx = tx or not rx
    show_rx = rx or not tx
    use_json = cli_state.output_format == OutputFormat.json
    no_color = cli_state.no_color

    async def _run():
        from netaudio_lib.daemon.client import (
            get_devices_from_daemon,
            meter_snapshot_from_daemon,
            meter_start_on_daemon,
            meter_stop_on_daemon,
        )

        devices = await get_devices_from_daemon()
        if not devices:
            typer.echo("Daemon not running. Start the daemon first: netaudio server start", err=True)
            raise typer.Exit(code=1)

        filtered = filter_devices(devices)
        if not filtered:
            typer.echo("No device found.", err=True)
            raise typer.Exit(code=1)

        ordered = list(filtered.keys())
        client_id = "meter_cli"

        for server_name in ordered:
            await meter_start_on_daemon(server_name, client_id)

        try:
            first_levels = None
            for _ in range(int(timeout * 10)):
                for server_name in ordered:
                    result = await meter_snapshot_from_daemon(server_name)
                    if result and (result.get("tx") or result.get("rx")):
                        first_levels = result
                        break
                if first_levels:
                    break
                await asyncio.sleep(0.1)

            if not first_levels:
                typer.echo("No metering response received.", err=True)
                raise typer.Exit(code=1)

            if snapshot or use_json:
                device_levels = []
                all_json = {}
                for server_name in sorted(ordered, key=lambda sn: (filtered[sn].name or sn)):
                    target = filtered[server_name]
                    levels = await meter_snapshot_from_daemon(server_name)
                    if levels is None:
                        continue
                    source_ip = levels.get("source_ip", "")
                    device_levels.append((target.name or server_name, source_ip, levels))
                    all_json[server_name] = {"tx": levels.get("tx", {}), "rx": levels.get("rx", {})}

                if use_json:
                    import json as json_module
                    typer.echo(json_module.dumps(all_json, indent=2))
                else:
                    typer.echo(_render_meter_display(device_levels, show_tx, show_rx, channel, no_color))
                return

            import sys
            prev_line_count = 0

            try:
                while True:
                    device_levels = []
                    for server_name in sorted(ordered, key=lambda sn: (filtered[sn].name or sn)):
                        target = filtered[server_name]
                        levels = await meter_snapshot_from_daemon(server_name)
                        if levels is None:
                            continue
                        source_ip = levels.get("source_ip", "")
                        device_levels.append((target.name or server_name, source_ip, levels))

                    if device_levels:
                        output = _render_meter_display(device_levels, show_tx, show_rx, channel, no_color)
                        line_count = output.count("\n") + 1

                        if prev_line_count > 0:
                            sys.stdout.write(f"\033[{prev_line_count}A\033[J")

                        sys.stdout.write(output + "\n")
                        sys.stdout.flush()
                        prev_line_count = line_count

                    await asyncio.sleep(0.1)
            except (KeyboardInterrupt, asyncio.CancelledError):
                pass

        finally:
            for server_name in ordered:
                await meter_stop_on_daemon(server_name, client_id)

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
            rows.append([
                info.get("name", ""),
                server_name,
                "yes" if online else "no",
                "yes" if receiving else "no",
            ])
            json_data[server_name] = info

        output_table(headers, rows, json_data=json_data)

    asyncio.run(_run())
