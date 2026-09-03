from __future__ import annotations

import logging
from enum import Enum
from typing import Optional

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import (
    CapabilityProbeTimeout,
    _enrich_lock_states,
    _load_device_for_show,
    _log_unreachable,
    readback_after_notification,
    run_command,
)
from netaudio.cli_support.output import output_table, output_value
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.commands.config.cli import app as device_config_app
from netaudio.commands.config.readback import MUTATION_ERRORS
from netaudio.commands.device.display import (
    _device_show_rows,
    _diagnostic_audio_capabilities_data,
    _diagnostic_audio_capability_rows,
)
from netaudio.commands.device.exports import export_capability, export_logs
from netaudio.commands.device.network_status import network_status
from netaudio.commands.status import status as status_command
from netaudio.dante.conmon_export import ConmonExportUnavailableError
from netaudio.dante.commands import validate_dante_name
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.diagnostic_logs import DeviceLogExportError
from netaudio.icons import icon

logger = logging.getLogger("netaudio")

app = typer.Typer(help="Manage Dante devices.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)


class ClearConfigurationMode(str, Enum):
    ALL = "all"
    PRESERVE_INTERNET_PROTOCOL_SETTINGS = "preserve-network"


app.command("list")(status_command)


async def run_show(application, devices) -> None:
    from netaudio.cli import OutputFormat, state

    include_channels = state.output_format in (OutputFormat.json, OutputFormat.yaml, OutputFormat.xml)
    server_name, device = await _load_device_for_show(application, include_channels=include_channels)
    for _, reason in (await _enrich_lock_states(application, {server_name: device}, only_unknown=True)).items():
        _log_unreachable(device, reason)
    data = DanteDeviceSerializer.to_json(device)
    output_table(
        ["Field", "Value"],
        _device_show_rows(device),
        json_data=data,
        title=device.name or server_name,
        devices={server_name: device},
    )


@app.command("show")
def device_show():
    """Show detailed device information."""
    run_command(run_show, discover_devices=False)


def _addressed_device(filtered):
    [(server_name, device)] = select_device(filtered)
    device_name = device.name or server_name
    if device.ipv4 is None:
        typer.echo(f"Error: {device_name} has no control address.", err=True)
        raise typer.Exit(code=1)
    return device_name, device


async def run_capabilities(application, devices, timeout: float) -> None:
    device_name, device = _addressed_device(filter_devices(devices))
    try:
        result = await application.export_device_logs(device.ipv4, timeout=timeout)
    except (CapabilityProbeTimeout, ConmonExportUnavailableError, DeviceLogExportError) as exception:
        typer.echo(f"Error: {exception}", err=True)
        raise typer.Exit(code=1) from None
    capabilities = result.audio_capabilities
    if capabilities is None:
        typer.echo(
            f"Error: {device_name} returned diagnostic logs without recognized audio capability records.",
            err=True,
        )
        raise typer.Exit(code=1)
    output_table(
        ["Capability", "Value"],
        _diagnostic_audio_capability_rows(capabilities),
        json_data=_diagnostic_audio_capabilities_data(capabilities),
        title=device_name,
    )


@app.command("capabilities")
def device_capabilities(
    timeout: float = typer.Option(15.0, "--timeout", min=0.1, help="Diagnostic response timeout in seconds."),
):
    """Inspect licensed and sample-rate-dependent audio capabilities."""
    run_command(run_capabilities, timeout)


async def run_identify(application, devices, all_devices: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)
    for server_name, device in targets:
        await application.identify(device)
        typer.echo(f"{icon('identify')}Identified: {device.name or server_name}")


@app.command()
def identify(
    all_devices: bool = typer.Option(
        False,
        "--all",
        help="Identify every matched device. Required when more than one device matches.",
    ),
):
    """Blink the identify LED on a device."""
    run_command(run_identify, all_devices)


async def run_reboot(application, devices, all_devices: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)
    for server_name, device in targets:
        await application.reboot(device)
        typer.echo(f"Reboot requested: {device.name or server_name}")


@app.command()
def reboot(
    all_devices: bool = typer.Option(
        False,
        "--all",
        help="Reboot every matched device. Required when more than one device matches.",
    ),
):
    """Reboot a device."""
    run_command(run_reboot, all_devices)


def _confirmed_device(filtered, confirm: str):
    [(server_name, device)] = select_device(filtered)
    device_name = device.name or server_name
    if confirm != device_name:
        typer.echo(f"Error: --confirm must exactly match {device_name!r}.", err=True)
        raise typer.Exit(code=1)
    return device_name, device


async def run_factory_reset(application, devices, confirm: str) -> None:
    device_name, device = _confirmed_device(filter_devices(devices), confirm)
    await application.factory_reset(device)
    typer.echo(f"Factory reset requested: {device_name}")


@app.command("factory-reset")
def factory_reset(
    confirm: str = typer.Option(
        ...,
        "--confirm",
        help="Exact device name required to authorize erasing all retained configuration.",
    ),
):
    """Erase all retained configuration and request a factory reset."""
    run_command(run_factory_reset, confirm)


async def run_clear_configuration(application, devices, mode: ClearConfigurationMode, confirm: str) -> None:
    device_name, device = _confirmed_device(filter_devices(devices), confirm)
    if device.ipv4 is None:
        typer.echo(f"Error: {device_name} has no control address.", err=True)
        raise typer.Exit(code=1)

    preserve_internet_protocol_settings = mode is ClearConfigurationMode.PRESERVE_INTERNET_PROTOCOL_SETTINGS
    status = await application.clear_configuration(
        str(device.ipv4),
        preserve_internet_protocol_settings,
    )
    typer.echo(
        f"Clear-configuration accepted: {device_name} (result {status['action_result_code']}, mode {mode.value})"
    )


@app.command("clear-configuration")
def clear_configuration(
    mode: ClearConfigurationMode = typer.Option(
        ...,
        "--mode",
        help="Configuration to clear: all, or preserve-network.",
    ),
    confirm: str = typer.Option(
        ...,
        "--confirm",
        help="Exact device name required to authorize clearing retained configuration.",
    ),
):
    """Clear retained configuration with verified device acknowledgement."""
    run_command(run_clear_configuration, mode, confirm)


app.add_typer(device_config_app, name="config")
app.command("export-capability")(export_capability)
app.command("export-logs")(export_logs)
app.command("network-status")(network_status)


async def run_name(application, devices, new_name: str | None) -> None:
    [(server_name, device)] = select_device(filter_devices(devices))

    if new_name is None:
        output_value("Device name", "device_name", device.name)
        return

    if new_name == "":
        try:
            await application.reset_device_name(device)
        except MUTATION_ERRORS as exception:
            typer.echo(f"Error: could not request name reset for {server_name}: {exception}", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"{icon('name')}Name reset requested for {server_name}; not verified.")
        return

    for candidate_server_name, candidate_device in devices.items():
        if candidate_device is device:
            continue
        if candidate_device.name and candidate_device.name.lower() == new_name.lower():
            typer.echo(
                f"Error: name '{new_name}' already in use by {candidate_device.name} ({candidate_server_name})",
                err=True,
            )
            raise typer.Exit(code=1)

    error = validate_dante_name(new_name)
    if error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1)

    try:
        await application.set_device_name(device, new_name)
    except MUTATION_ERRORS as exception:
        typer.echo(f"Error: could not send name change to {device.name or server_name}: {exception}", err=True)
        raise typer.Exit(code=1)

    async def _read_name():
        reported_name = await device.fetch_device_name()
        if not isinstance(reported_name, str):
            raise RuntimeError("device name readback was unavailable")
        return reported_name

    result = await readback_after_notification(_read_name, new_name)
    if result.matched:
        typer.echo(f"{icon('name')}Set name: {new_name} (verified)")
        return

    label = device.name or server_name
    if result.observed_available:
        typer.echo(
            f"Error: name change sent to {label}, but the device reports {result.observed!r} instead of {new_name!r}.",
            err=True,
        )
    else:
        detail = f": {result.error}" if result.error is not None else ""
        typer.echo(
            f"Error: name change sent to {label}, but readback was unavailable{detail}; the change was not verified.",
            err=True,
        )
    raise typer.Exit(code=1)


@app.command()
def name(
    new_name: Optional[str] = typer.Argument(None, help="New name (omit to get, empty string to reset)."),
):
    """Get or set device name."""
    run_command(run_name, new_name)
