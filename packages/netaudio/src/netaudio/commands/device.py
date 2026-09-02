from __future__ import annotations

import asyncio
import logging
from enum import Enum
from typing import Optional

import typer

logger = logging.getLogger("netaudio")

from netaudio.dante.conmon_export import ConmonExportUnavailableError
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_operations import (
    core_lock_device,
    core_unlock_device,
    validate_dante_name,
    validate_pin,
)
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.services.notification import (
    NOTIFICATION_ROUTING_DEVICE_CHANGE,
    NOTIFICATION_SETTINGS_CHANGE,
)

from netaudio._common import (
    _command_context,
    _discover,
    _get_arc_port,
    _load_device_for_show,
    _populate_controls,
    _probe_lock_status_once,
    _resolve_one,
    CapabilityProbeTimeout,
    readback_after_notification,
    send_and_wait_for_notification,
)
from netaudio._common_cli import HELP_CONTEXT_SETTINGS
from netaudio._common_output import output_single, output_table
from netaudio._common_selection import filter_devices, sort_devices
from netaudio.commands.status import status as status_command
from netaudio.icons import icon

app = typer.Typer(help="Manage Dante devices.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)


class ClearConfigurationMode(str, Enum):
    ALL = "all"
    PRESERVE_INTERNET_PROTOCOL_SETTINGS = "preserve-network"


from netaudio.commands.device_display import (
    _device_show_rows,
    _diagnostic_audio_capabilities_data,
    _diagnostic_audio_capability_rows,
)


async def _lock_via_daemon(pin: str, action: str) -> dict | None:
    from netaudio.cli import state
    from netaudio.daemon.client import _daemon_request

    device_name = None
    if state.names:
        device_name = state.names[0]
    elif state.hosts:
        device_name = state.hosts[0]

    if not device_name:
        devices = await _discover()
        filtered = filter_devices(devices)
        _, device = _resolve_one(filtered)
        device_name = device.name or device.server_name

    status, data = await _daemon_request("POST", f"/{action}", body={"device": device_name, "pin": pin}, timeout=8.0)
    if status is None:
        return None
    return data


def _get_lock_key() -> bytes:
    from netaudio.common.app_config import settings as app_settings

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


async def _standalone_lock_operation(device_ip: str, pin: str, lock_key: bytes, *, locking: bool) -> dict:
    operation = core_lock_device if locking else core_unlock_device
    result = await operation(device_ip, pin, lock_key)
    if result.get("success") is not True:
        return result

    observation = await _probe_lock_status_once(device_ip)
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


@app.command("show")
def device_show():
    """Show detailed device information."""

    async def _run():
        from netaudio.cli import OutputFormat, state

        include_channels = state.output_format in (OutputFormat.json, OutputFormat.yaml, OutputFormat.xml)
        server_name, device = await _load_device_for_show(include_channels=include_channels)
        from netaudio._common import _enrich_lock_states, _log_unreachable

        for _, reason in (await _enrich_lock_states({server_name: device}, only_unknown=True)).items():
            _log_unreachable(device, reason)
        data = DanteDeviceSerializer.to_json(device)
        output_table(
            ["Field", "Value"],
            _device_show_rows(device),
            json_data=data,
            title=device.name or server_name,
            devices={server_name: device},
        )

    asyncio.run(_run())


@app.command("capabilities")
def device_capabilities(
    timeout: float = typer.Option(15.0, "--timeout", min=0.1, help="Diagnostic response timeout in seconds."),
):
    """Inspect licensed and sample-rate-dependent audio capabilities."""

    async def _run():
        from netaudio.dante.diagnostic_logs import DeviceLogExportError

        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            if not filtered:
                typer.echo("Error: no devices matched.", err=True)
                raise typer.Exit(code=1)
            if len(filtered) > 1:
                typer.echo(
                    "Error: multiple devices matched. Narrow the selection to exactly one device.",
                    err=True,
                )
                raise typer.Exit(code=1)
            server_name, device = _resolve_one(filtered)
            device_name = device.name or server_name
            if device.ipv4 is None:
                typer.echo(f"Error: {device_name} has no control address.", err=True)
                raise typer.Exit(code=1)
            try:
                result = await send.export_device_logs(device.ipv4, timeout=timeout)
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

    asyncio.run(_run())


@app.command()
def identify(
    all_devices: bool = typer.Option(
        False,
        "--all",
        help="Identify every matched device. Required when more than one device matches.",
    ),
):
    """Blink the identify LED on a device."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            if not filtered:
                typer.echo("Error: device not found.", err=True)
                raise typer.Exit(code=1)

            if all_devices:
                targets = list(filtered.items())
            else:
                if len(filtered) > 1:
                    typer.echo(
                        "Error: multiple devices matched. Narrow the selection with a device "
                        "filter, or pass --all to identify every match.",
                        err=True,
                    )
                    raise typer.Exit(code=1)
                targets = [_resolve_one(filtered)]

            for server_name, device in targets:
                packet, _, port = commands.command_identify()
                await send(packet, device.ipv4, port, expect_response=False)
                typer.echo(f"{icon('identify')}Identified: {device.name}")

    asyncio.run(_run())


@app.command()
def reboot(
    all_devices: bool = typer.Option(
        False,
        "--all",
        help="Reboot every matched device. Required when more than one device matches.",
    ),
):
    """Reboot a device."""

    async def _run():
        filtered = filter_devices(await _discover())
        await _populate_controls(filtered)
        if not filtered:
            typer.echo("Error: no devices matched.", err=True)
            raise typer.Exit(code=1)

        if all_devices:
            targets = list(filtered.items())
        else:
            if len(filtered) > 1:
                typer.echo(
                    "Error: multiple devices matched. Narrow the selection with a device "
                    "filter, or pass --all to reboot every match.",
                    err=True,
                )
                raise typer.Exit(code=1)
            targets = [_resolve_one(filtered)]

        for server_name, device in targets:
            await device.operations.reboot()
            typer.echo(f"Reboot requested: {device.name or server_name}")

    asyncio.run(_run())


@app.command("factory-reset")
def factory_reset(
    confirm: str = typer.Option(
        ...,
        "--confirm",
        help="Exact device name required to authorize erasing all retained configuration.",
    ),
):
    """Erase all retained configuration and request a factory reset."""

    async def _run():
        filtered = filter_devices(await _discover())
        await _populate_controls(filtered)
        if not filtered:
            typer.echo("Error: no devices matched.", err=True)
            raise typer.Exit(code=1)
        if len(filtered) > 1:
            typer.echo(
                "Error: multiple devices matched. Narrow the selection to exactly one device.",
                err=True,
            )
            raise typer.Exit(code=1)

        server_name, device = _resolve_one(filtered)
        device_name = device.name or server_name
        if confirm != device_name:
            typer.echo(
                f"Error: --confirm must exactly match {device_name!r}.",
                err=True,
            )
            raise typer.Exit(code=1)

        await device.operations.factory_reset()
        typer.echo(f"Factory reset requested: {device_name}")

    asyncio.run(_run())


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

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            if not filtered:
                typer.echo("Error: no devices matched.", err=True)
                raise typer.Exit(code=1)
            if len(filtered) > 1:
                typer.echo(
                    "Error: multiple devices matched. Narrow the selection to exactly one device.",
                    err=True,
                )
                raise typer.Exit(code=1)

            server_name, device = _resolve_one(filtered)
            device_name = device.name or server_name
            if confirm != device_name:
                typer.echo(
                    f"Error: --confirm must exactly match {device_name!r}.",
                    err=True,
                )
                raise typer.Exit(code=1)
            if device.ipv4 is None:
                typer.echo(f"Error: {device_name} has no control address.", err=True)
                raise typer.Exit(code=1)

            preserve_internet_protocol_settings = mode is ClearConfigurationMode.PRESERVE_INTERNET_PROTOCOL_SETTINGS
            status = await send.clear_configuration(
                device.ipv4,
                preserve_internet_protocol_settings,
            )
            typer.echo(
                f"Clear-configuration accepted: {device_name} "
                f"(result {status['action_result_code']}, mode {mode.value})"
            )

    asyncio.run(_run())


from netaudio.commands.device_exports import export_capability, export_logs
from netaudio.commands.device_network_status import network_status

app.command("export-logs")(export_logs)
app.command("export-capability")(export_capability)
app.command("network-status")(network_status)


from netaudio.commands.config import app as device_config_app

app.add_typer(device_config_app, name="config")

lock_app = typer.Typer(help="Device lock management.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)
app.add_typer(lock_app, name="lock", hidden=True)


@lock_app.command("set", help="Lock the selected device with a 4-digit PIN.")
def lock_set(
    pin: str = typer.Argument(..., help="4-digit numeric PIN to lock the device with."),
):
    async def _run():
        result = await _lock_via_daemon(pin, "lock")
        if result is not None:
            if not result.get("success"):
                _report_lock_failure("lock", result)
            elif result.get("already"):
                typer.echo("already locked", err=True)
            return

        lock_key = _get_lock_key()

        error = validate_pin(pin)
        if error:
            typer.echo(f"Error: {error}", err=True)
            raise typer.Exit(code=1)

        device_ip = await _resolve_lock_ip()

        result = await _standalone_lock_operation(device_ip, pin, lock_key, locking=True)

        if not result["success"]:
            _report_lock_failure("lock", result)
        elif result["already"]:
            typer.echo("already locked", err=True)

    asyncio.run(_run())


@lock_app.command("clear", help="Unlock the selected device with its 4-digit PIN.")
def lock_clear(
    pin: str = typer.Argument(..., help="4-digit numeric PIN to unlock the device."),
):
    async def _run():
        result = await _lock_via_daemon(pin, "unlock")
        if result is not None:
            if not result.get("success"):
                _report_lock_failure("unlock", result)
            elif result.get("already"):
                typer.echo("already unlocked", err=True)
            return

        lock_key = _get_lock_key()

        error = validate_pin(pin)
        if error:
            typer.echo(f"Error: {error}", err=True)
            raise typer.Exit(code=1)

        device_ip = await _resolve_lock_ip()

        result = await _standalone_lock_operation(device_ip, pin, lock_key, locking=False)

        if not result["success"]:
            _report_lock_failure("unlock", result)
        elif result["already"]:
            typer.echo("already unlocked", err=True)

    asyncio.run(_run())


@lock_app.command("status")
def lock_status():
    """Show device lock status."""

    async def _run():
        filtered = filter_devices(await _discover())
        await _populate_controls(filtered)

        from netaudio._common import _enrich_lock_states, _log_unreachable

        for server_name, reason in (await _enrich_lock_states(filtered)).items():
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

    asyncio.run(_run())


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
                try:
                    await send(packet, device.ipv4, arc_port)
                except Exception as exception:
                    typer.echo(f"Error: could not request name reset for {server_name}: {exception}", err=True)
                    raise typer.Exit(code=1)
                typer.echo(f"{icon('name')}Name reset requested for {server_name}; not verified.")
            else:
                for candidate_server_name, candidate_device in devices.items():
                    if candidate_device is device:
                        continue
                    if candidate_device.name and candidate_device.name.lower() == new_name.lower():
                        typer.echo(
                            f"Error: name '{new_name}' already in use by "
                            f"{candidate_device.name} ({candidate_server_name})",
                            err=True,
                        )
                        raise typer.Exit(code=1)

                error = validate_dante_name(new_name)
                if error:
                    typer.echo(f"Error: {error}", err=True)
                    raise typer.Exit(code=1)

                packet, _ = commands.command_set_name(new_name)
                try:
                    await send_and_wait_for_notification(
                        send,
                        packet,
                        device.ipv4,
                        arc_port,
                        (NOTIFICATION_ROUTING_DEVICE_CHANGE, NOTIFICATION_SETTINGS_CHANGE),
                    )
                except Exception as exception:
                    typer.echo(
                        f"Error: could not send name change to {device.name or server_name}: {exception}", err=True
                    )
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
                        f"Error: name change sent to {label}, but the device reports "
                        f"{result.observed!r} instead of {new_name!r}.",
                        err=True,
                    )
                else:
                    detail = f": {result.error}" if result.error is not None else ""
                    typer.echo(
                        f"Error: name change sent to {label}, but readback was unavailable{detail}; "
                        "the change was not verified.",
                        err=True,
                    )
                raise typer.Exit(code=1)

    asyncio.run(_run())


from netaudio.commands.flow import app as flow_app

app.add_typer(flow_app, name="flow", hidden=True)

from netaudio.commands.device_meter import (
    meter_app,
)

app.add_typer(meter_app, name="meter", hidden=True)
