from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio.dante.device_commands import DanteDeviceCommands

from netaudio._common import (
    _command_context,
    _discover,
    _get_arc_port,
    _populate_controls,
    filter_devices,
    find_channel,
    find_device,
    output_table,
    parse_qualified_name,
    sort_devices,
)
from netaudio._exit_codes import ExitCode
from netaudio.icons import icon, icon_only

app = typer.Typer(help="Manage audio subscriptions.", no_args_is_help=True)


@app.command("list")
def subscription_list():
    """List all active subscriptions."""

    async def _run():
        from netaudio.dante.const import SUBSCRIPTION_STATUS_INFO
        from netaudio.dante.device_serializer import DanteDeviceSerializer

        devices = await _discover()
        await _populate_controls(devices)
        devices = filter_devices(devices)

        all_subscriptions = []

        for server_name, device in sort_devices(devices):
            for subscription in device.subscriptions:
                all_subscriptions.append(subscription)

        if not all_subscriptions:
            typer.echo("No active subscriptions.")
            return

        from netaudio.cli import state

        _STATE_COLORS = {
            "connected": "\033[32m",
            "in_progress": "\033[33m",
            "resolved": "\033[33m",
            "idle": "\033[33m",
            "unresolved": "\033[31m",
            "error": "\033[31m",
            "none": "\033[90m",
        }

        _STATE_ICONS = {
            "connected": "connected",
            "in_progress": "info",
            "resolved": "info",
            "idle": "info",
            "unresolved": "error",
            "error": "error",
            "none": "offline",
        }

        def _status_label(code):
            info = SUBSCRIPTION_STATUS_INFO.get(code)
            if not info:
                return ""
            status_state, label, _ = info
            status_icon = icon(_STATE_ICONS.get(status_state, ""))
            if state.no_color:
                return f"{status_icon}{label}"
            color = _STATE_COLORS.get(status_state, "")
            if not color:
                return f"{status_icon}{label}"
            return f"{color}{status_icon}{label}\033[0m"

        headers = ["RX Channel", "RX Device", "TX Channel", "TX Device", "Status"]
        rows = []
        json_data = [DanteDeviceSerializer.subscription_to_json(s) for s in all_subscriptions]

        for subscription in all_subscriptions:
            rows.append([
                subscription.rx_channel_name or "",
                subscription.rx_device_name or "",
                subscription.tx_channel_name or "",
                subscription.tx_device_name or "",
                _status_label(subscription.status_code),
            ])

        output_table(headers, rows, json_data=json_data)

    asyncio.run(_run())


@app.command()
def add(
    tx: str = typer.Option(..., "--tx", help="TX source: channel@device (single) or device (bulk 1:1)."),
    rx: str = typer.Option(..., "--rx", help="RX destination: channel@device (single) or device (bulk 1:1)."),
    count: int = typer.Option(0, "--count", "-c", help="Number of channels (bulk only, 0 = all)."),
    offset_tx: int = typer.Option(0, "--offset-tx", help="Starting TX channel offset (bulk only, 0-based)."),
    offset_rx: int = typer.Option(0, "--offset-rx", help="Starting RX channel offset (bulk only, 0-based)."),
):
    """Add subscriptions. Single: --tx channel@device --rx channel@device. Bulk: --tx device --rx device."""

    commands = DanteDeviceCommands()

    async def _run():
        if not tx or not rx:
            typer.echo("Error: both --tx and --rx required.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        is_single = "@" in tx and "@" in rx

        if is_single:
            tx_channel_id, tx_device_id = parse_qualified_name(tx)
            rx_channel_id, rx_device_id = parse_qualified_name(rx)

            async with _command_context() as (devices, send):
                tx_device = find_device(devices, tx_device_id)
                if tx_device is None:
                    typer.echo(f"Error: TX device '{tx_device_id}' not found.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                rx_device = find_device(devices, rx_device_id)
                if rx_device is None:
                    typer.echo(f"Error: RX device '{rx_device_id}' not found.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                tx_channel = find_channel(tx_device, tx_channel_id, "tx")
                if tx_channel is None:
                    typer.echo(f"Error: TX channel '{tx_channel_id}' not found on {tx_device.name}.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                rx_channel = find_channel(rx_device, rx_channel_id, "rx")
                if rx_channel is None:
                    typer.echo(f"Error: RX channel '{rx_channel_id}' not found on {rx_device.name}.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                tx_channel_name = tx_channel.friendly_name or tx_channel.name
                packet, _ = commands.command_add_subscription(
                    rx_channel.number, tx_channel_name, tx_device.name
                )
                arc_port = _get_arc_port(rx_device)
                await send(packet, rx_device.ipv4, arc_port)
                typer.echo(f"{icon('add')}{rx_channel_id}@{rx_device.name} <- {tx_channel_id}@{tx_device.name}")
        else:
            if "@" in tx or "@" in rx:
                typer.echo("Error: both --tx and --rx must be channel@device or both must be device names.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            async with _command_context() as (devices, send):
                tx_device = find_device(devices, tx)
                if tx_device is None:
                    typer.echo(f"Error: TX device '{tx}' not found.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                rx_device = find_device(devices, rx)
                if rx_device is None:
                    typer.echo(f"Error: RX device '{rx}' not found.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                tx_sorted = sorted(tx_device.tx_channels.values(), key=lambda channel: channel.number)
                rx_sorted = sorted(rx_device.rx_channels.values(), key=lambda channel: channel.number)

                if not tx_sorted:
                    typer.echo(f"Error: no TX channels on {tx_device.name}.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                if not rx_sorted:
                    typer.echo(f"Error: no RX channels on {rx_device.name}.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                tx_sorted = tx_sorted[offset_tx:]
                rx_sorted = rx_sorted[offset_rx:]

                pairs = list(zip(tx_sorted, rx_sorted))
                if count > 0:
                    pairs = pairs[:count]

                if not pairs:
                    typer.echo("No channel pairs to subscribe.")
                    return

                arc_port = _get_arc_port(rx_device)

                batch_size = 16
                for batch_start in range(0, len(pairs), batch_size):
                    batch = pairs[batch_start:batch_start + batch_size]
                    subscriptions = []
                    for tx_channel, rx_channel in batch:
                        tx_channel_name = tx_channel.friendly_name or tx_channel.name
                        subscriptions.append((rx_channel.number, tx_channel_name, tx_device.name))

                    try:
                        packet, _ = commands.command_add_subscriptions(subscriptions)
                        await send(packet, rx_device.ipv4, arc_port)
                        for tx_channel, rx_channel in batch:
                            tx_channel_name = tx_channel.friendly_name or tx_channel.name
                            rx_channel_name = rx_channel.friendly_name or rx_channel.name
                            typer.echo(f"{icon('add')}{rx_channel_name}@{rx_device.name} <- {tx_channel_name}@{tx_device.name}")
                    except Exception as error:
                        for tx_channel, rx_channel in batch:
                            tx_channel_name = tx_channel.friendly_name or tx_channel.name
                            rx_channel_name = rx_channel.friendly_name or rx_channel.name
                            typer.echo(f"{icon('fail')}FAILED {rx_channel_name}@{rx_device.name} <- {tx_channel_name}@{tx_device.name}: {error}", err=True)

    asyncio.run(_run())


@app.command()
def remove(
    rx: str = typer.Option(..., "--rx", help="RX channel as channel@device."),
):
    """Remove a subscription from an RX channel."""

    commands = DanteDeviceCommands()

    async def _run():
        if not rx:
            typer.echo("Error: --rx required.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        rx_channel_id, rx_device_id = parse_qualified_name(rx)

        async with _command_context() as (devices, send):
            rx_device = find_device(devices, rx_device_id)
            if rx_device is None:
                typer.echo(f"Error: RX device '{rx_device_id}' not found.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            rx_channel = find_channel(rx_device, rx_channel_id, "rx")
            if rx_channel is None:
                typer.echo(f"Error: RX channel '{rx_channel_id}' not found on {rx_device.name}.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            packet, _ = commands.command_remove_subscription(rx_channel.number)
            arc_port = _get_arc_port(rx_device)
            await send(packet, rx_device.ipv4, arc_port)
            typer.echo(f"{icon('remove')}Removed: {rx_channel_id}@{rx_device.name}")

    asyncio.run(_run())
