from __future__ import annotations

import logging

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import _enrich_lock_states, _log_unreachable, run_command
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import filter_devices, select_device, sort_devices
from netaudio.common.app_config import settings as app_settings
from netaudio.dante.lock import core_lock_device, core_unlock_device, validate_pin
from netaudio.icons import icon

logger = logging.getLogger("netaudio")


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


lock_app = typer.Typer(help="Device lock management.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)


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
