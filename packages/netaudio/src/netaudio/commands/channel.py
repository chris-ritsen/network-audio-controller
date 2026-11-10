from __future__ import annotations

from typing import Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import run_with_devices
from netaudio.cli_support.output import output_sections, output_single, structured_output_selected
from netaudio.cli_support.selection import (
    CHANNEL_REFERENCE_FORMS,
    ChannelReference,
    filter_devices,
    parse_channel_reference,
    require_selected_devices,
    resolve_channel,
    select_device,
    sort_devices,
)
from netaudio.daemon.client import call_daemon
from netaudio.icons import icon

CHANNEL_ARGUMENT_HELP = f"Channel to address: {CHANNEL_REFERENCE_FORMS} (a bare name searches both directions)."

app = typer.Typer(
    help="Manage channels on the selected device.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


def _channel_json(channels: dict) -> dict:
    return {
        channel.name: {
            "number": channel.number,
            "name": channel.name,
            "friendly_name": channel.friendly_name,
            "factory_name": channel.factory_name,
        }
        for channel in sorted(channels.values(), key=lambda channel: channel.number)
    }


def _channel_section(channels: dict, title: str) -> tuple[str, list[str], list[list[str]]]:
    include_factory_name = any(channel.factory_name for channel in channels.values())
    headers = ["#", "Name", "Friendly Name"]
    if include_factory_name:
        headers.append("Factory Name")
    rows = []
    for channel in sorted(channels.values(), key=lambda channel: channel.number):
        row = [str(channel.number), channel.name, channel.friendly_name or ""]
        if include_factory_name:
            row.append(channel.factory_name or "")
        rows.append(row)
    return title, headers, rows


async def run_channel_list(devices) -> None:
    devices = require_selected_devices(filter_devices(devices))
    json_data = {
        server_name: {
            "name": device.name,
            "tx_channels": _channel_json(device.tx_channels),
            "rx_channels": _channel_json(device.rx_channels),
        }
        for server_name, device in sort_devices(devices)
    }
    sections = []
    for server_name, device in sort_devices(devices):
        device_label = device.name or server_name
        if device.tx_channels:
            sections.append(_channel_section(device.tx_channels, f"{device_label} TX Channels"))
        if device.rx_channels:
            sections.append(_channel_section(device.rx_channels, f"{device_label} RX Channels"))
    output_sections(sections, json_data)


@app.command("list")
def channel_list():
    """List channels on devices."""
    run_with_devices(run_channel_list)


async def run_channel_name(devices, reference: ChannelReference, new_name: str | None) -> None:
    [(server_name, device)] = select_device(filter_devices(devices))
    channel_type, channel = resolve_channel(device, reference)

    if new_name is None:
        if structured_output_selected():
            output_single(
                {
                    "friendly_name": channel.friendly_name,
                    "name": channel.name,
                    "number": channel.number,
                    "type": channel_type,
                }
            )
            return
        typer.echo(channel.friendly_name or channel.name)
        return

    await call_daemon(
        "POST",
        "/rename-channel",
        {"device": server_name, "channel_type": channel_type, "channel_number": channel.number, "name": new_name},
    )
    if new_name == "":
        typer.echo(f"{icon('name')}Channel name reset: {channel.name}")
    else:
        typer.echo(f"{icon('name')}Set channel name: {new_name}")


@app.command()
def name(
    channel: str = typer.Argument(..., help=CHANNEL_ARGUMENT_HELP),
    new_name: Optional[str] = typer.Argument(None, help="New name (omit to get, empty string to reset)."),
):
    """Get or set a channel name."""
    run_with_devices(run_channel_name, parse_channel_reference(channel), new_name)


async def run_channel_gain(devices, reference: ChannelReference, level: int | None) -> None:
    [(server_name, device)] = select_device(filter_devices(devices))
    device_type = device.record.get("gain_device_type")
    if device_type is None:
        typer.echo("unsupported")
        if level is not None:
            raise typer.Exit(code=ExitCode.ERROR)
        return

    channel_type = "tx" if device_type == "input" else "rx"
    if reference.direction not in (None, channel_type):
        typer.echo(f"Error: device reports {device_type} reference controls.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    _, channel = resolve_channel(device, ChannelReference(channel_type, reference.identifier))
    labels = {choice["value"]: choice["label"] for choice in device.record.get("gain_level_choices") or []}

    if level is None:
        levels = device.record.get("gain_levels") or []
        if channel.number > len(levels):
            typer.echo("unsupported")
            return
        current = levels[channel.number - 1]
        typer.echo(f"{labels.get(current, 'Unknown')} (level {current})")
        return

    await call_daemon(
        "POST",
        "/set-gain",
        {"device": server_name, "channel_number": channel.number, "device_type": device_type, "gain_level": level},
    )
    typer.echo(
        f"{icon('gain')}Set {device_type} reference level for channel {channel.number}: "
        f"{labels.get(level, f'level {level}')}"
    )


@app.command()
def gain(
    channel: str = typer.Argument(..., help=CHANNEL_ARGUMENT_HELP),
    level: Optional[int] = typer.Argument(None, help="Gain level (1-5)."),
):
    """Get or set channel gain level."""
    run_with_devices(run_channel_gain, parse_channel_reference(channel), level)
