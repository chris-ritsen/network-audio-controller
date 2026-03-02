from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio._common import (
    _discover,
    _populate_controls,
    _resolve_device,
    filter_devices,
    find_channel,
    output_single,
    output_table,
    set_device_filter,
    sort_devices,
)
from netaudio._exit_codes import ExitCode

app = typer.Typer(help="Manage device channels.", no_args_is_help=True)


@app.command("list")
def channel_list(
    device: Optional[str] = typer.Argument(None, help="Device name, IP, or server name."),
):
    """List channels on devices."""
    if device:
        set_device_filter(device)

    async def _run():
        devices = await _discover()
        await _populate_controls(devices)
        devices = filter_devices(devices)

        from netaudio.cli import OutputFormat, state

        if state.output_format in (OutputFormat.json, OutputFormat.xml, OutputFormat.yaml):
            data = {}
            for server_name, device in sort_devices(devices):
                data[server_name] = {
                    "name": device.name,
                    "tx_channels": {
                        channel.name: {"number": channel.number, "name": channel.name, "friendly_name": channel.friendly_name}
                        for channel in sorted(device.tx_channels.values(), key=lambda channel: channel.number)
                    },
                    "rx_channels": {
                        channel.name: {"number": channel.number, "name": channel.name, "friendly_name": channel.friendly_name}
                        for channel in sorted(device.rx_channels.values(), key=lambda channel: channel.number)
                    },
                }
            output_single(data)
            return

        for server_name, device in sort_devices(devices):
            device_label = device.name or server_name

            if device.tx_channels:
                headers = ["#", "Name", "Friendly Name"]
                rows = [
                    [str(channel.number), channel.name, channel.friendly_name or ""]
                    for channel in sorted(device.tx_channels.values(), key=lambda channel: channel.number)
                ]
                output_table(headers, rows, title=f"{device_label} TX Channels")

            if device.rx_channels:
                headers = ["#", "Name", "Friendly Name"]
                rows = [
                    [str(channel.number), channel.name, channel.friendly_name or ""]
                    for channel in sorted(device.rx_channels.values(), key=lambda channel: channel.number)
                ]
                output_table(headers, rows, title=f"{device_label} RX Channels")

    asyncio.run(_run())


@app.command()
def name(
    device: str = typer.Argument(help="Device name, IP, or server name."),
    channel: str = typer.Argument(help="Channel number or name."),
    new_name: Optional[str] = typer.Argument(None, help="New name (omit to get, empty string to reset)."),
    channel_type: str = typer.Option("tx", "--type", "-t", help="Channel type: tx or rx."),
):
    """Get or set a channel name."""
    set_device_filter(device)

    async def _run():
        resolved_device = await _resolve_device()
        found_channel = find_channel(resolved_device, channel, channel_type)

        if found_channel is None:
            typer.echo(f"Error: channel '{channel}' not found.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        if new_name is None:
            typer.echo(found_channel.friendly_name or found_channel.name)
            return

        if new_name == "":
            await resolved_device.operations.reset_channel_name(channel_type, found_channel.number)
            typer.echo(f"Reset channel name: {found_channel.name}")
        else:
            await resolved_device.operations.set_channel_name(channel_type, found_channel.number, new_name)
            typer.echo(f"Set channel name: {new_name}")

    asyncio.run(_run())


@app.command()
def gain(
    device: str = typer.Argument(help="Device name, IP, or server name."),
    channel: str = typer.Argument(help="Channel number or name."),
    level: Optional[float] = typer.Argument(None, help="Gain level (1-5)."),
    channel_type: str = typer.Option("rx", "--type", "-t", help="Channel type: tx or rx."),
):
    """Get or set channel gain level."""
    set_device_filter(device)

    async def _run():
        resolved_device = await _resolve_device()
        found_channel = find_channel(resolved_device, channel, channel_type)

        if found_channel is None:
            typer.echo(f"Error: channel '{channel}' not found.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        if level is None:
            typer.echo(found_channel.volume if found_channel.volume is not None else "N/A")
            return

        if not (1 <= level <= 5):
            typer.echo("Error: gain level must be between 1 and 5.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        await resolved_device.operations.set_gain_level(found_channel.number, level, channel_type)
        typer.echo(f"Set gain level: {level}")

    asyncio.run(_run())
