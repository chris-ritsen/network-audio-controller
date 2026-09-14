from __future__ import annotations

from typing import Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.execution import run_command
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.dante.network_configuration import NetworkConfigurationUnverified


def mode_label(mode):
    return {"switched": "Switched", "redundant": "Redundant", "split_redundant": "Split/Redundant"}.get(mode, "Unknown")


async def run_redundancy(application, devices, mode, all_devices):
    rows, data, failures = [], {}, 0
    for server_name, device in select_device(filter_devices(devices), allow_many=all_devices):
        try:
            result = (
                await application.probe_dante_redundancy(device)
                if mode is None
                else await application.set_dante_redundancy(device, mode)
            )
            status = result if mode is None else result["effective_readback"]
            data[server_name] = result
            available_modes = [
                choice.get("mode")
                for choice in status.get("available_modes") or []
                if isinstance(choice, dict) and choice.get("mode") is not None
            ]
            rows.append(
                [
                    device.name or server_name,
                    {True: "supported", False: "unsupported", None: "unknown"}[status.get("advertised_support")],
                    mode_label(status.get("current_mode")),
                    mode_label(status.get("configured_mode")),
                    ", ".join(mode_label(value) for value in available_modes),
                    "yes"
                    if status["operation_availability"]["writable"]
                    else ", ".join(status["operation_availability"]["reasons"]),
                    "yes" if status["reboot_required"] else "no",
                ]
            )
            if mode is not None:
                typer.echo(
                    f"Configured Dante redundancy on {device.name or server_name}; fresh configured-state "
                    "readback matched. Persistence was not checked and no reboot was sent.",
                    err=True,
                )
        except (ValueError, RuntimeError, OSError, TimeoutError) as exception:
            if isinstance(exception, NetworkConfigurationUnverified) and exception.evidence is not None:
                data[server_name] = exception.evidence
            failures += 1
            typer.echo(f"Error: {device.name or server_name}: {exception}", err=True)
    output_table(
        ["Device", "Capability", "Active", "Configured", "Available Modes", "Writable", "Reboot Required"],
        rows,
        json_data=data,
    )
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
