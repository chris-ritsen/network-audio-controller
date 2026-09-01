from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.const import RESULT_CODE_SUCCESS
from netaudio.dante.gain import SUPPORTED_GAIN_LEVELS, gain_channel_type, gain_level_label
from netaudio.dante.channel_frontend import (
    ChannelFrontendError,
    channel_result_code,
    receiver_channel_name_protocol_identifier_from_probe,
    transmitter_channel_name_protocol_identifier_from_probe,
)
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
    readback_after_notification,
    send_and_wait_for_notification,
)
from netaudio._common_cli import HELP_CONTEXT_SETTINGS
from netaudio._common_output import output_single, output_table
from netaudio._common_selection import (
    CHANNEL_REFERENCE_FORMS,
    ChannelReference,
    filter_devices,
    parse_channel_reference,
    resolve_channel,
    sort_devices,
)
from netaudio._exit_codes import ExitCode
from netaudio.icons import icon

CHANNEL_ARGUMENT_HELP = f"Channel to address: {CHANNEL_REFERENCE_FORMS} (a bare name searches both directions)."

app = typer.Typer(
    help="Manage channels on the selected device.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


@app.command("list")
def channel_list():
    """List channels on devices."""

    async def _run():
        devices = await _discover()
        await _populate_controls(devices, strict=False)
        devices = filter_devices(devices)
        from netaudio._common import _apply_avio_status_pages

        await asyncio.gather(
            *[_apply_avio_status_pages(device) for device in devices.values()],
            return_exceptions=True,
        )

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
                            "factory_name": channel.factory_name,
                        }
                        for channel in sorted(device.tx_channels.values(), key=lambda channel: channel.number)
                    },
                    "rx_channels": {
                        channel.name: {
                            "number": channel.number,
                            "name": channel.name,
                            "friendly_name": channel.friendly_name,
                            "factory_name": channel.factory_name,
                        }
                        for channel in sorted(device.rx_channels.values(), key=lambda channel: channel.number)
                    },
                }
            output_single(data)
            return

        for server_name, device in sort_devices(devices):
            device_label = device.name or server_name

            if device.tx_channels:
                include_factory_name = any(channel.factory_name for channel in device.tx_channels.values())
                headers = ["#", "Name", "Friendly Name"]
                if include_factory_name:
                    headers.append("Factory Name")
                rows = []
                for channel in sorted(device.tx_channels.values(), key=lambda channel: channel.number):
                    row = [str(channel.number), channel.name, channel.friendly_name or ""]
                    if include_factory_name:
                        row.append(channel.factory_name or "")
                    rows.append(row)
                output_table(headers, rows, title=f"{device_label} TX Channels")

            if device.rx_channels:
                include_factory_name = any(channel.factory_name for channel in device.rx_channels.values())
                headers = ["#", "Name", "Friendly Name"]
                if include_factory_name:
                    headers.append("Factory Name")
                rows = []
                for channel in sorted(device.rx_channels.values(), key=lambda channel: channel.number):
                    row = [str(channel.number), channel.name, channel.friendly_name or ""]
                    if include_factory_name:
                        row.append(channel.factory_name or "")
                    rows.append(row)
                output_table(headers, rows, title=f"{device_label} RX Channels")

    asyncio.run(_run())


@app.command()
def name(
    channel: str = typer.Argument(..., help=CHANNEL_ARGUMENT_HELP),
    new_name: Optional[str] = typer.Argument(None, help="New name (omit to get, empty string to reset)."),
):
    """Get or set a channel name."""

    commands = DanteDeviceCommands()
    reference = parse_channel_reference(channel)

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            _, device = _resolve_one(filtered)
            channel_type, found_channel = resolve_channel(device, reference)

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
                attribute_name = (
                    "receiver_channel_name_protocol_identifier"
                    if channel_type == "rx"
                    else "transmitter_channel_name_protocol_identifier"
                )
                protocol_identifier = getattr(device, attribute_name, None)
                if protocol_identifier is None:
                    if channel_type == "rx":
                        probe_packet, _ = commands.command_query_receiver_channel_status_2809()
                        resolve_protocol_identifier = receiver_channel_name_protocol_identifier_from_probe
                    else:
                        probe_packet, _ = commands.command_query_transmitter_channel_status_2809()
                        resolve_protocol_identifier = transmitter_channel_name_protocol_identifier_from_probe
                    try:
                        probe_response = await send(probe_packet, device.ipv4, arc_port)
                        protocol_identifier = resolve_protocol_identifier(probe_response)
                    except Exception as exception:
                        typer.echo(f"Error: could not determine {channel_type} channel frontend: {exception}", err=True)
                        raise typer.Exit(code=ExitCode.ERROR)
                    setattr(device, attribute_name, protocol_identifier)
                packet, _ = commands.command_set_channel_name(
                    channel_type,
                    found_channel.number,
                    new_name,
                    protocol_id=protocol_identifier,
                )
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
                    response = await send_and_wait_for_notification(
                        send,
                        packet,
                        device.ipv4,
                        arc_port,
                        notification_ids,
                    )
                    result_code = channel_result_code(response, "channel name change")
                    if result_code != RESULT_CODE_SUCCESS:
                        raise ChannelFrontendError(f"channel name change failed with result 0x{result_code:04X}")
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
    channel: str = typer.Argument(..., help=CHANNEL_ARGUMENT_HELP),
    level: Optional[int] = typer.Argument(None, help="Gain level (1-5)."),
):
    """Get or set channel gain level."""

    reference = parse_channel_reference(channel)
    channel_type = reference.direction

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            _, device = _resolve_one(filtered)

            if device.gain_levels is None:
                try:
                    status = await send.probe_gain_status(device.ipv4)
                except Exception as exception:
                    typer.echo(f"Error: could not read gain status: {exception}", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)
                if status is not None:
                    device.gain_device_type, device.gain_levels = status
                    device.supported_gain_levels = list(SUPPORTED_GAIN_LEVELS)

            inferred_channel_type = gain_channel_type(device.gain_device_type or "")
            if device.gain_device_type is None or inferred_channel_type is None:
                if level is None:
                    typer.echo("unsupported")
                    return
                if channel_type is None:
                    typer.echo(
                        f"Error: this device does not report gain controls; use tx:{reference.identifier} or "
                        f"rx:{reference.identifier} to choose the side.",
                        err=True,
                    )
                    raise typer.Exit(code=ExitCode.ERROR)
                selected_channel_type = channel_type
                device_type = "input" if selected_channel_type == "tx" else "output"
            else:
                if channel_type is not None and channel_type != inferred_channel_type:
                    typer.echo(
                        f"Error: device reports {device.gain_device_type} reference controls, not "
                        f"{'input' if channel_type == 'tx' else 'output'} controls.",
                        err=True,
                    )
                    raise typer.Exit(code=ExitCode.ERROR)
                selected_channel_type = inferred_channel_type
                device_type = device.gain_device_type

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
                status = await send.set_gain_level(device.ipv4, found_channel.number, level, device_type)
            except Exception as exception:
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
            device.gain_device_type = observed_device_type
            device.gain_levels = channel_levels
            device.supported_gain_levels = list(SUPPORTED_GAIN_LEVELS)
            typer.echo(
                f"{icon('gain')}Set {device_type} reference level for channel {found_channel.number}: "
                f"{gain_level_label(device_type, level)} (verified)"
            )

    asyncio.run(_run())
