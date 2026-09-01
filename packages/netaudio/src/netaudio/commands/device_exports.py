from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from netaudio._common import _command_context, _resolve_one, CapabilityProbeTimeout
from netaudio._common_selection import filter_devices


def _selected_device(filtered_devices):
    if not filtered_devices:
        typer.echo("Error: no devices matched.", err=True)
        raise typer.Exit(code=1)
    if len(filtered_devices) > 1:
        typer.echo(
            "Error: multiple devices matched. Narrow the selection to exactly one device.",
            err=True,
        )
        raise typer.Exit(code=1)
    server_name, device = _resolve_one(filtered_devices)
    device_name = device.name or server_name
    if device.ipv4 is None:
        typer.echo(f"Error: {device_name} has no control address.", err=True)
        raise typer.Exit(code=1)
    return device_name, device


def export_logs(
    output: Path = typer.Argument(..., help="New tar archive path."),
    timeout: float = typer.Option(15.0, "--timeout", min=0.1, help="Response timeout in seconds."),
):
    """Export the selected device's diagnostic logs."""

    async def _run():
        from netaudio.dante.diagnostic_logs import DeviceLogExportError, write_device_log_archive

        async with _command_context() as (devices, send):
            device_name, device = _selected_device(filter_devices(devices))
            try:
                result = await send.export_device_logs(device.ipv4, timeout=timeout)
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

    asyncio.run(_run())


def export_capability(
    output: Path = typer.Argument(..., help="New raw CAP1 partition image path."),
    timeout: float = typer.Option(15.0, "--timeout", min=0.1, help="Response timeout in seconds."),
):
    """Export the selected device's CAP1 capability partition."""

    async def _run():
        from netaudio.dante.capability_partition import write_capability_partition
        from netaudio.dante.conmon_export import ConmonExportError

        async with _command_context() as (devices, send):
            device_name, device = _selected_device(filter_devices(devices))
            try:
                result = await send.export_capability_partition(device.ipv4, timeout=timeout)
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

    asyncio.run(_run())
