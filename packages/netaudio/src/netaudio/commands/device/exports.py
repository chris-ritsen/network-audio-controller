from __future__ import annotations

from pathlib import Path

import typer

from netaudio.cli_support.execution import CapabilityProbeTimeout, run_command
from netaudio.cli_support.selection import filter_devices, select_device


def select_export_device(filtered_devices):
    [(server_name, device)] = select_device(filtered_devices)
    device_name = device.name or server_name
    if getattr(device, "requires_managed_control", False):
        typer.echo(f"Error: diagnostic export is not available through DDM for {device_name}.", err=True)
        raise typer.Exit(code=1)
    if device.ipv4 is None:
        typer.echo(f"Error: {device_name} has no control address.", err=True)
        raise typer.Exit(code=1)
    return device_name, device


async def run_export_logs(application, devices, output: Path, timeout: float) -> None:
    from netaudio.dante.diagnostic_logs import DeviceLogExportError, write_device_log_archive

    device_name, device = select_export_device(filter_devices(devices))
    try:
        result = await application.export_device_logs(device.ipv4, timeout=timeout)
        write_device_log_archive(output, result.archive_payload)
    except FileExistsError:
        typer.echo(f"Error: output already exists: {output}", err=True)
        raise typer.Exit(code=1) from None
    except (CapabilityProbeTimeout, DeviceLogExportError) as exception:
        typer.echo(f"Error: {exception}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(
        f"Exported {len(result.members)} archive entries from {device_name} to {output} "
        f"({len(result.archive_payload)} bytes, SHA-256 {result.archive_sha256})"
    )


def export_logs(
    output: Path = typer.Argument(..., help="New tar archive path."),
    timeout: float = typer.Option(15.0, "--timeout", min=0.1, help="Response timeout in seconds."),
):
    """Export the selected device's diagnostic logs."""
    run_command(run_export_logs, output, timeout)


async def run_export_capability(application, devices, output: Path, timeout: float) -> None:
    from netaudio.dante.capability_partition import write_capability_partition
    from netaudio.dante.conmon_export import ConmonExportError

    device_name, device = select_export_device(filter_devices(devices))
    try:
        result = await application.export_capability_partition(device.ipv4, timeout=timeout)
        write_capability_partition(output, result.capability_partition)
    except FileExistsError:
        typer.echo(f"Error: output already exists: {output}", err=True)
        raise typer.Exit(code=1) from None
    except (CapabilityProbeTimeout, ConmonExportError) as exception:
        typer.echo(f"Error: {exception}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(
        f"Exported CAP1 from {device_name} to {output} "
        f"({len(result.capability_partition)} bytes, "
        f"SHA-256 {result.capability_partition_sha256})"
    )


def export_capability(
    output: Path = typer.Argument(..., help="New raw CAP1 partition image path."),
    timeout: float = typer.Option(15.0, "--timeout", min=0.1, help="Response timeout in seconds."),
):
    """Export the selected device's CAP1 capability partition."""
    run_command(run_export_capability, output, timeout)
