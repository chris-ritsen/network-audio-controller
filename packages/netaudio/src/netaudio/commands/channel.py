from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.services.notification import (
    NOTIFICATION_PROPERTY_CHANGE,
    NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_TX_CHANNEL_CHANGE,
    NOTIFICATION_TX_LABEL_CHANGE,
)

from netaudio._common import (
    _command_context,
    _discover,
    _get_arc_port,
    _populate_controls,
    _resolve_one,
    filter_devices,
    find_channel,
    output_single,
    output_table,
    readback_after_notification,
    send_and_wait_for_notification,
    sort_devices,
)
from netaudio._exit_codes import ExitCode
from netaudio.icons import icon

app = typer.Typer(help="Manage device channels.", no_args_is_help=True)


@app.command("list")
def channel_list():
    """List channels on devices."""

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
                        channel.name: {
                            "number": channel.number,
                            "name": channel.name,
                            "friendly_name": channel.friendly_name,
                        }
                        for channel in sorted(device.tx_channels.values(), key=lambda channel: channel.number)
                    },
                    "rx_channels": {
                        channel.name: {
                            "number": channel.number,
                            "name": channel.name,
                            "friendly_name": channel.friendly_name,
                        }
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
    channel: str = typer.Argument(help="Channel number or name."),
    new_name: Optional[str] = typer.Argument(None, help="New name (omit to get, empty string to reset)."),
    channel_type: str = typer.Option("tx", "--type", "-t", help="Channel type: tx or rx."),
):
    """Get or set a channel name."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            server_name, device = _resolve_one(filtered)

            if channel_type not in ("tx", "rx"):
                typer.echo("Error: channel type must be 'tx' or 'rx'.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            found_channel = find_channel(device, channel, channel_type)
            if found_channel is None:
                typer.echo(f"Error: channel '{channel}' not found.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            if new_name is None:
                typer.echo(found_channel.friendly_name or found_channel.name)
                return

            arc_port = _get_arc_port(device)

            if new_name == "":
                packet, _ = commands.command_reset_channel_name(channel_type, found_channel.number)
                try:
                    await send(packet, device.ipv4, arc_port)
                except Exception as exception:
                    typer.echo(f"Error: could not request channel name reset: {exception}", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)
                typer.echo(f"{icon('name')}Channel name reset requested for {found_channel.name}; not verified.")
            else:
                packet, _ = commands.command_set_channel_name(channel_type, found_channel.number, new_name)
                try:
                    notification_ids = (
                        (NOTIFICATION_RX_CHANNEL_CHANGE, NOTIFICATION_PROPERTY_CHANGE)
                        if channel_type == "rx"
                        else (
                            NOTIFICATION_TX_CHANNEL_CHANGE,
                            NOTIFICATION_TX_LABEL_CHANGE,
                            NOTIFICATION_PROPERTY_CHANGE,
                        )
                    )
                    await send_and_wait_for_notification(
                        send,
                        packet,
                        device.ipv4,
                        arc_port,
                        notification_ids,
                    )
                except Exception as exception:
                    typer.echo(f"Error: could not send channel name change: {exception}", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                async def _read_channel_name():
                    if channel_type == "rx":
                        await device.get_rx_channels()
                        refreshed = device.rx_channels.get(found_channel.number)
                        reported_name = refreshed.name if refreshed else None
                    else:
                        await device.get_tx_channels()
                        refreshed = device.tx_channels.get(found_channel.number)
                        reported_name = refreshed.friendly_name if refreshed else None
                    if not isinstance(reported_name, str):
                        raise RuntimeError("channel name readback was unavailable")
                    return reported_name

                result = await readback_after_notification(_read_channel_name, new_name)
                if result.matched:
                    typer.echo(f"{icon('name')}Set channel name: {new_name} (verified)")
                elif result.observed_available:
                    typer.echo(
                        "Error: channel name change sent, but the device reports "
                        f"{result.observed!r} instead of {new_name!r}.",
                        err=True,
                    )
                    raise typer.Exit(code=ExitCode.ERROR)
                else:
                    detail = f": {result.error}" if result.error is not None else ""
                    typer.echo(
                        f"Error: channel name change sent, but readback was unavailable{detail}; "
                        "the change was not verified.",
                        err=True,
                    )
                    raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_run())


@app.command()
def gain(
    channel: str = typer.Argument(help="Channel number or name."),
    level: Optional[int] = typer.Argument(None, help="Gain level (1-5)."),
    channel_type: str = typer.Option("rx", "--type", "-t", help="Channel type: tx or rx."),
):
    """Get or set channel gain level."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            server_name, device = _resolve_one(filtered)

            if channel_type not in ("tx", "rx"):
                typer.echo("Error: channel type must be 'tx' or 'rx'.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            found_channel = find_channel(device, channel, channel_type)
            if found_channel is None:
                typer.echo(f"Error: channel '{channel}' not found.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            if level is None:
                typer.echo(found_channel.volume if found_channel.volume is not None else "N/A")
                return

            if not (1 <= level <= 5):
                typer.echo("Error: gain level must be between 1 and 5.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            device_type = "input" if channel_type == "tx" else "output"
            packet, _, port = commands.command_set_gain_level(found_channel.number, level, device_type)
            try:
                await send(packet, device.ipv4, port)
            except Exception as exception:
                typer.echo(f"Error: could not request gain change: {exception}", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            typer.echo(f"{icon('gain')}Gain change requested: {level}; not verified.")

    asyncio.run(_run())
