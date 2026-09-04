from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import CapabilityProbeTimeout, readback_after_notification, run_command
from netaudio.cli_support.output import output_sections
from netaudio.cli_support.selection import (
    CHANNEL_REFERENCE_FORMS,
    ChannelReference,
    filter_devices,
    parse_channel_reference,
    resolve_channel,
    select_device,
    sort_devices,
)
from netaudio.commands.config.readback import MUTATION_ERRORS
from netaudio.dante.channel_frontend import ChannelFrontendError, channel_result_code
from netaudio.dante.const import RESULT_CODE_SUCCESS
from netaudio.dante.gain import SUPPORTED_GAIN_LEVELS, gain_channel_type, gain_level_label
from netaudio.dante.state import apply_device_status
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


async def run_channel_list(application, devices) -> None:
    devices = filter_devices(devices)
    await asyncio.gather(
        *[application.apply_modern_arc_status_pages(device) for device in devices.values()],
        return_exceptions=True,
    )

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
    run_command(run_channel_list)


async def _read_channel_name(device, channel_type: str, channel_number: int) -> str:
    if channel_type == "rx":
        await device.get_rx_channels()
        refreshed = device.rx_channels.get(channel_number)
        reported_name = refreshed.name if refreshed else None
    else:
        await device.get_tx_channels()
        refreshed = device.tx_channels.get(channel_number)
        reported_name = refreshed.friendly_name if refreshed else None
    if not isinstance(reported_name, str):
        raise RuntimeError("channel name readback was unavailable")
    return reported_name


async def run_channel_name(application, devices, reference: ChannelReference, new_name: str | None) -> None:
    [(_, device)] = select_device(filter_devices(devices))
    channel_type, found_channel = resolve_channel(device, reference)

    if new_name is None:
        typer.echo(found_channel.friendly_name or found_channel.name)
        return

    if new_name == "":
        try:
            await application.reset_channel_name(device, channel_type, found_channel.number)
        except MUTATION_ERRORS as exception:
            typer.echo(f"Error: could not request channel name reset: {exception}", err=True)
            raise typer.Exit(code=ExitCode.ERROR)
        typer.echo(f"{icon('name')}Channel name reset requested for {found_channel.name}; not verified.")
        return

    try:
        await application.resolve_channel_name_protocol_identifier(device, channel_type)
    except (*MUTATION_ERRORS, ChannelFrontendError) as exception:
        typer.echo(f"Error: could not determine {channel_type} channel frontend: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    try:
        response = await application.set_channel_name(device, channel_type, found_channel.number, new_name)
        result_code = channel_result_code(response, "channel name change")
        if result_code != RESULT_CODE_SUCCESS:
            raise ChannelFrontendError(f"channel name change failed with result 0x{result_code:04X}")
    except (*MUTATION_ERRORS, ChannelFrontendError) as exception:
        typer.echo(f"Error: could not send channel name change: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    result = await readback_after_notification(
        lambda: _read_channel_name(device, channel_type, found_channel.number),
        new_name,
    )
    if result.matched:
        typer.echo(f"{icon('name')}Set channel name: {new_name} (verified)")
        return
    if result.observed_available:
        typer.echo(
            f"Error: channel name change sent, but the device reports {result.observed!r} instead of {new_name!r}.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)
    detail = f": {result.error}" if result.error is not None else ""
    typer.echo(
        f"Error: channel name change sent, but readback was unavailable{detail}; the change was not verified.",
        err=True,
    )
    raise typer.Exit(code=ExitCode.ERROR)


@app.command()
def name(
    channel: str = typer.Argument(..., help=CHANNEL_ARGUMENT_HELP),
    new_name: Optional[str] = typer.Argument(None, help="New name (omit to get, empty string to reset)."),
):
    """Get or set a channel name."""
    reference = parse_channel_reference(channel)
    run_command(run_channel_name, reference, new_name)


def _select_gain_side(device, reference: ChannelReference, level: int | None):
    channel_type = reference.direction
    inferred_channel_type = gain_channel_type(device.gain_device_type or "")
    if device.gain_device_type is None or inferred_channel_type is None:
        if level is None:
            return None, None
        if channel_type is None:
            typer.echo(
                f"Error: this device does not report gain controls; use tx:{reference.identifier} or "
                f"rx:{reference.identifier} to choose the side.",
                err=True,
            )
            raise typer.Exit(code=ExitCode.ERROR)
        return channel_type, "input" if channel_type == "tx" else "output"
    if channel_type is not None and channel_type != inferred_channel_type:
        typer.echo(
            f"Error: device reports {device.gain_device_type} reference controls, not "
            f"{'input' if channel_type == 'tx' else 'output'} controls.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)
    return inferred_channel_type, device.gain_device_type


async def run_channel_gain(application, devices, reference: ChannelReference, level: int | None) -> None:
    [(_, device)] = select_device(filter_devices(devices))

    if device.gain_levels is None:
        try:
            device_type, channel_levels = await application.probe_gain_status(device)
        except CapabilityProbeTimeout:
            pass
        except MUTATION_ERRORS as exception:
            typer.echo(f"Error: could not read gain status: {exception}", err=True)
            raise typer.Exit(code=ExitCode.ERROR)
        else:
            apply_device_status(
                device,
                "gain",
                {
                    "gain_device_type": device_type,
                    "gain_levels": channel_levels,
                    "supported_gain_levels": list(SUPPORTED_GAIN_LEVELS),
                },
            )

    selected_channel_type, device_type = _select_gain_side(device, reference, level)
    if selected_channel_type is None:
        typer.echo("unsupported")
        return

    _, found_channel = resolve_channel(device, ChannelReference(selected_channel_type, reference.identifier))

    current_level = device.gain_level_for_channel(found_channel.number, selected_channel_type)
    if level is None:
        if current_level is None or device.gain_device_type is None:
            typer.echo("unsupported")
            return
        typer.echo(f"{gain_level_label(device.gain_device_type, current_level)} (level {current_level})")
        return

    if not (1 <= level <= 5):
        typer.echo("Error: gain level must be between 1 and 5.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    if device.supported_gain_levels is not None and level not in device.supported_gain_levels:
        typer.echo(
            f"Error: gain level {level} is not supported; device reports {device.supported_gain_levels}.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)

    try:
        status = await application.set_gain_level(device, found_channel.number, level, device_type)
    except MUTATION_ERRORS as exception:
        typer.echo(f"Error: could not set gain level: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    if status is None:
        typer.echo("Error: gain change sent, but device readback was unavailable.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    observed_device_type, channel_levels = status
    channel_index = found_channel.number - 1
    observed_level = channel_levels[channel_index] if 0 <= channel_index < len(channel_levels) else None
    if observed_device_type != device_type or observed_level != level:
        typer.echo(
            "Error: gain change was not applied; "
            f"device reports {observed_device_type} channel {found_channel.number} level {observed_level}.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)
    typer.echo(
        f"{icon('gain')}Set {device_type} reference level for channel {found_channel.number}: "
        f"{gain_level_label(device_type, level)} (verified)"
    )


@app.command()
def gain(
    channel: str = typer.Argument(..., help=CHANNEL_ARGUMENT_HELP),
    level: Optional[int] = typer.Argument(None, help="Gain level (1-5)."),
):
    """Get or set channel gain level."""
    reference = parse_channel_reference(channel)
    run_command(run_channel_gain, reference, level)
