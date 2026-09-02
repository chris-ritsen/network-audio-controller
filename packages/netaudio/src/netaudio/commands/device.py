from __future__ import annotations

import logging
from enum import Enum
from typing import Optional

import typer

from netaudio._common import (
    CapabilityProbeTimeout,
    _enrich_lock_states,
    _load_device_for_show,
    _log_unreachable,
    readback_after_notification,
    run_command,
)
from netaudio._common_cli import HELP_CONTEXT_SETTINGS
from netaudio._common_output import output_single, output_table
from netaudio._common_selection import filter_devices, select_device, sort_devices
from netaudio.commands.config_readback import MUTATION_ERRORS
from netaudio.commands.device_display import (
    _device_show_rows,
    _diagnostic_audio_capabilities_data,
    _diagnostic_audio_capability_rows,
)
from netaudio.commands.status import status as status_command
from netaudio.common.app_config import settings as app_settings
from netaudio.dante.conmon_export import ConmonExportUnavailableError
from netaudio.dante.device_operations import (
    core_lock_device,
    core_unlock_device,
    validate_dante_name,
    validate_pin,
)
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.diagnostic_logs import DeviceLogExportError
from netaudio.icons import icon

logger = logging.getLogger("netaudio")

app = typer.Typer(help="Manage Dante devices.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)


class ClearConfigurationMode(str, Enum):
    ALL = "all"
    PRESERVE_INTERNET_PROTOCOL_SETTINGS = "preserve-network"


async def _lock_via_daemon(device_name: str, pin: str, action: str) -> dict | None:
    from netaudio.daemon.client import _daemon_request

    status, data = await _daemon_request("POST", f"/{action}", body={"device": device_name, "pin": pin}, timeout=8.0)
    if status is None:
        return None
    return data


def _get_lock_key() -> bytes:
    if app_settings.device_lock_key:
        return app_settings.device_lock_key

    from netaudio.common.config_loader import config_search_paths, load_capture_profile

    profile_cfg, _ = load_capture_profile(None, None)
    lock_key_value = profile_cfg.get("device_lock_key")
    if lock_key_value:
        key = lock_key_value.encode("ascii")
        app_settings.device_lock_key = key
        return key

    from netaudio.common.key_extract import extract_lock_key, find_dante_controller_binary

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


def _report_lock_failure(action: str, result: dict) -> None:
    error = result.get("error")
    if error:
        typer.echo(f"Error: {action} failed: {error}", err=True)
    elif result.get("status") is not None:
        typer.echo(f"Error: {action} failed (status 0x{result['status']:04x})", err=True)
    else:
        typer.echo(f"Error: {action} failed: unknown", err=True)
    raise typer.Exit(code=1)


async def _standalone_lock_operation(application, device_ip: str, pin: str, lock_key: bytes, *, locking: bool) -> dict:
    operation = core_lock_device if locking else core_unlock_device
    result = await operation(device_ip, pin, lock_key)
    if result.get("success") is not True:
        return result

    try:
        observation = await application.probe_lock_status(device_ip, timeout=app_settings.lock_state_timeout)
    except (RuntimeError, OSError) as exception:
        logger.debug(f"Lock status unavailable for {device_ip}: {exception}")
        observation = None
    if observation is None:
        return {
            **result,
            "success": False,
            "is_locked": None,
            "error": "lock status readback was not reported",
        }
    if observation.is_locked is not locking:
        return {
            **result,
            "success": False,
            "is_locked": observation.is_locked,
            "lock_state_code": observation.lock_state_code,
            "error": "lock operation did not reach the requested state",
        }
    return {
        **result,
        "is_locked": observation.is_locked,
        "lock_state_code": observation.lock_state_code,
        "observation_source": "observed_after_0x1008",
    }


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


from netaudio.commands.device_exports import export_capability, export_logs
from netaudio.commands.device_network_status import network_status

app.command("export-logs")(export_logs)
app.command("export-capability")(export_capability)
app.command("network-status")(network_status)


from netaudio.commands.config import app as device_config_app

app.add_typer(device_config_app, name="config")

lock_app = typer.Typer(help="Device lock management.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)
app.add_typer(lock_app, name="lock", hidden=True)


async def run_lock_operation(application, devices, pin: str, *, locking: bool) -> None:
    action = "lock" if locking else "unlock"
    already_message = "already locked" if locking else "already unlocked"
    [(server_name, device)] = select_device(filter_devices(devices))
    device_name = device.name or server_name
    result = await _lock_via_daemon(device_name, pin, action)
    if result is not None:
        if not result.get("success"):
            _report_lock_failure(action, result)
        elif result.get("already"):
            typer.echo(already_message, err=True)
        return

    lock_key = _get_lock_key()

    error = validate_pin(pin)
    if error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1)

    if device.ipv4 is None:
        typer.echo(f"Error: {device_name} has no control address.", err=True)
        raise typer.Exit(code=1)

    result = await _standalone_lock_operation(application, str(device.ipv4), pin, lock_key, locking=locking)

    if not result["success"]:
        _report_lock_failure(action, result)
    elif result["already"]:
        typer.echo(already_message, err=True)


@lock_app.command("set", help="Lock the selected device with a 4-digit PIN.")
def lock_set(
    pin: str = typer.Argument(..., help="4-digit numeric PIN to lock the device with."),
):
    run_command(run_lock_operation, pin, locking=True)


@lock_app.command("clear", help="Unlock the selected device with its 4-digit PIN.")
def lock_clear(
    pin: str = typer.Argument(..., help="4-digit numeric PIN to unlock the device."),
):
    run_command(run_lock_operation, pin, locking=False)


async def run_lock_status(application, devices) -> None:
    filtered = filter_devices(devices)
    for server_name, reason in (await _enrich_lock_states(application, filtered)).items():
        _log_unreachable(filtered[server_name], reason)

    if not filtered:
        typer.echo("Error: no devices found.", err=True)
        raise typer.Exit(code=1)

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

        rows.append(
            [
                device.name or "",
                str(device.ipv4) if device.ipv4 else "",
                status_display,
            ]
        )
        json_data[server_name] = {
            "name": device.name,
            "ipv4": str(device.ipv4),
            "is_locked": device.is_locked,
        }

    output_table(headers, rows, json_data=json_data)


@lock_app.command("status")
def lock_status():
    """Show device lock status."""
    run_command(run_lock_status)


async def run_name(application, devices, new_name: str | None) -> None:
    [(server_name, device)] = select_device(filter_devices(devices))

    if new_name is None:
        output_single(device.name)
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


from netaudio.commands.flow import app as flow_app

app.add_typer(flow_app, name="flow", hidden=True)

from netaudio.commands.device_meter import (
    meter_app,
)

app.add_typer(meter_app, name="meter", hidden=True)
