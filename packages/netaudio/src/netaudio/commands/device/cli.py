from __future__ import annotations

import logging
from enum import Enum
from typing import Optional

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import run_with_devices
from netaudio.cli_support.output import output_single, output_table, output_value, structured_output_selected
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.commands.config.cli import app as device_config_app
from netaudio.commands.device.display import _device_show_rows
from netaudio.commands.device.network_status import network_status
from netaudio.commands.status import status as status_command
from netaudio.daemon.client import call_daemon
from netaudio.icons import icon

logger = logging.getLogger("netaudio")

app = typer.Typer(help="Manage Dante devices.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)


class ClearConfigurationMode(str, Enum):
    ALL = "all"
    PRESERVE_INTERNET_PROTOCOL_SETTINGS = "preserve-network"


app.command("list")(status_command)


@app.command("diagnostics")
def diagnostics(
    device: str = typer.Argument(..., help="Device name or server name."),
    reset: bool = typer.Option(False, "--reset", help="Reset local display statistics."),
    export: bool = typer.Option(False, "--export", help="Export retained raw observations as JSON."),
):
    """Show retained receiver-path and clock measurements from the daemon."""
    import asyncio
    import json
    from netaudio.daemon.client import get_diagnostics_from_daemon
    from netaudio.commands.device.display import _connection_health_rows

    status, data = asyncio.run(get_diagnostics_from_daemon(device, reset=reset))
    if status != 200 or data is None:
        typer.echo((data or {}).get("error", "The daemon did not respond"), err=True)
        raise typer.Exit(1)

    if export:
        typer.echo(json.dumps(data, indent=2))
        return

    rows = _connection_health_rows(data.get("receiver"))
    for source, series in (data.get("clock") or {}).items():
        if source not in ("heartbeat", "conmon") or not series.get("current"):
            continue
        stats = series.get("statistics") or {}
        rows.append(
            [
                f"{source.title()} clock offset",
                f"{series['current']['value']} ppm; {'fresh' if series['fresh'] else 'stale'}",
            ]
        )
        rows.append(["Clock observations", str(stats.get("count", 0))])
        rows.append(
            ["Clock mean / deviation", f"{stats.get('mean')} / {stats.get('population_standard_deviation')} ppm"]
        )
    output_table(["Measurement", "Value"], rows, json_data=data, title=device)


from netaudio.commands.device.controls import app as controls_app

app.add_typer(controls_app, name="controls")


async def run_show(devices) -> None:
    [(server_name, device)] = select_device(filter_devices(devices))
    output_table(
        ["Field", "Value"],
        _device_show_rows(device),
        json_data=device.record,
        title=device.name or server_name,
    )


@app.command("show")
def device_show():
    """Show detailed device information."""
    run_with_devices(run_show)


async def run_identify(devices, all_devices: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)
    identified = []
    for server_name, device in targets:
        await call_daemon("POST", "/identify", {"device": server_name})
        identified.append(device.name or server_name)
        if not structured_output_selected():
            typer.echo(f"{icon('identify')}Identified: {device.name or server_name}")
    if structured_output_selected():
        output_single({"identified": identified})


@app.command()
def identify(
    all_devices: bool = typer.Option(
        False,
        "--all",
        help="Identify every matched device. Required when more than one device matches.",
    ),
):
    """Blink the identify LED on a device."""
    run_with_devices(run_identify, all_devices)


async def run_reboot(devices, all_devices: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)
    for server_name, device in targets:
        await call_daemon("POST", "/reboot", {"device": server_name})
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
    run_with_devices(run_reboot, all_devices)


def _confirmed_device(filtered, confirm: str):
    [(server_name, device)] = select_device(filtered)
    device_name = device.name or server_name
    if confirm != device_name:
        typer.echo(f"Error: --confirm must exactly match {device_name!r}.", err=True)
        raise typer.Exit(code=1)
    return server_name, device_name


async def run_factory_reset(devices, confirm: str) -> None:
    server_name, device_name = _confirmed_device(filter_devices(devices), confirm)
    await call_daemon("POST", "/factory-reset", {"device": server_name})
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
    run_with_devices(run_factory_reset, confirm)


async def run_clear_configuration(devices, mode: ClearConfigurationMode, confirm: str) -> None:
    server_name, device_name = _confirmed_device(filter_devices(devices), confirm)
    await call_daemon(
        "POST",
        "/clear-configuration",
        {
            "device": server_name,
            "mode": "all" if mode is ClearConfigurationMode.ALL else "keep_network",
        },
    )
    typer.echo(f"Configuration cleared: {device_name} (mode {mode.value})")


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
    run_with_devices(run_clear_configuration, mode, confirm)


app.add_typer(device_config_app, name="config")
app.command("network-status")(network_status)


async def run_name(devices, new_name: str | None) -> None:
    [(server_name, device)] = select_device(filter_devices(devices))

    if new_name is None:
        output_value("Device name", "device_name", device.name)
        return

    await call_daemon("POST", "/rename-device", {"device": server_name, "name": new_name})
    if new_name == "":
        typer.echo(f"{icon('name')}Name reset: {server_name}")
    else:
        typer.echo(f"{icon('name')}Set name: {new_name}")


@app.command()
def name(
    new_name: Optional[str] = typer.Argument(None, help="New name (omit to get, empty string to reset)."),
):
    """Get or set device name."""
    run_with_devices(run_name, new_name)
