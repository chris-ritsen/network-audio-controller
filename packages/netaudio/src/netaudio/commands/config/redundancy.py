from __future__ import annotations

from typing import Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.execution import run_command
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import filter_devices, select_device


def mode_label(mode):
    return {"switched": "Switched", "redundant": "Redundant", "split_redundant": "Split/Redundant"}.get(mode, "Unknown")


async def run_redundancy(application, devices, mode, all_devices):
    rows, data, failures = [], {}, 0
    for server_name, device in select_device(filter_devices(devices), allow_many=all_devices):
        try:
            status = (
                await application.probe_dante_redundancy(device)
                if mode is None
                else await application.set_dante_redundancy(device, mode)
            )
            data[server_name] = status
            rows.append(
                [
                    device.name or server_name,
                    mode_label(status["current"]),
                    mode_label(status["configured"]),
                    ", ".join(mode_label(value) for value in status["supported"]),
                    "yes" if status["reboot_required"] else "no",
                ]
            )
            if mode is not None:
                typer.echo(
                    f"Configured Dante redundancy on {device.name or server_name} (verified). No reboot sent.", err=True
                )
        except (ValueError, RuntimeError, OSError, TimeoutError) as exception:
            failures += 1
            typer.echo(f"Error: {device.name or server_name}: {exception}", err=True)
    output_table(["Device", "Active", "Configured", "Supported", "Reboot Required"], rows, json_data=data)
    if failures:
        raise typer.Exit(code=ExitCode.ERROR)


def redundancy(
    mode: Optional[str] = typer.Argument(None, help="switched, redundant, or split_redundant"),
    all_devices: bool = typer.Option(False, "--all", help="Read or apply to all devices."),
):
    """Read or set and verify Dante Redundancy. Does not reboot devices."""
    if mode is not None and mode not in {"switched", "redundant", "split_redundant"}:
        raise typer.BadParameter("Expected switched, redundant, or split_redundant")
    run_command(run_redundancy, mode, all_devices)
