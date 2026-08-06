from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import typer

from netaudio._common import filter_devices, output_table, sort_devices
from netaudio.icons import icon

STATUS_HEADERS = ["Name", "Manufacturer", "Model", "IP Address", "TX", "RX", "Clock", "Lock", "Last Seen"]


def _format_timestamp(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _lock_display(is_locked) -> str:
    if is_locked is True:
        return icon("lock") or "locked"
    return ""


def _dante_row_from_summary(summary: dict) -> list[str]:
    channels = summary.get("channels") or {}
    transmitters = channels.get("transmitters") or {}
    receivers = channels.get("receivers") or {}
    return [
        summary.get("name") or "",
        summary.get("manufacturer") or "",
        summary.get("dante_model") or summary.get("model_id") or "",
        summary.get("ipv4") or "",
        str(len(transmitters) or summary.get("tx_count") or 0),
        str(len(receivers) or summary.get("rx_count") or 0),
        summary.get("ptp_v1_role") or summary.get("clock_role") or "",
        _lock_display(summary.get("is_locked")),
        _format_timestamp(summary.get("last_seen")),
    ]


def _dante_row_from_device(device) -> list[str]:
    return [
        device.name or "",
        device.manufacturer or "",
        device.dante_model or device.model_id or "",
        str(device.ipv4) if device.ipv4 else "",
        str(len(device.tx_channels) if device.tx_channels else (device.tx_count or 0)),
        str(len(device.rx_channels) if device.rx_channels else (device.rx_count or 0)),
        device.ptp_v1_role or device.clock_role or "",
        _lock_display(device.is_locked),
        _format_timestamp(device.last_seen),
    ]


def _shure_row(summary: dict) -> list[str]:
    channels = summary.get("channels") or {}
    return [
        summary.get("name") or "",
        "Shure",
        summary.get("model") or summary.get("device_type") or "",
        summary.get("ip") or "",
        str(len(channels)) if channels else "",
        "",
        "",
        "",
        "",
    ]


async def _gather_status() -> tuple[list[list[str]], dict]:
    from netaudio.daemon.client import (
        daemon_is_accessible,
        get_device_summaries_from_daemon,
        get_shure_devices_from_daemon,
    )

    rows: list[list[str]] = []
    json_data: dict = {}

    dante_summaries = None
    shure_summaries = None
    if daemon_is_accessible():
        dante_summaries = await get_device_summaries_from_daemon()
        shure_summaries = await get_shure_devices_from_daemon()

    if dante_summaries is not None:
        json_data["dante"] = dante_summaries
        for summary in sorted(dante_summaries.values(), key=lambda entry: (entry.get("name") or "").lower()):
            rows.append(_dante_row_from_summary(summary))
    else:
        from netaudio._common import _discover, _populate_controls
        from netaudio.dante.device_serializer import DanteDeviceSerializer

        devices = await _discover()
        await _populate_controls(devices)
        devices = filter_devices(devices)
        json_data["dante"] = {
            server_name: DanteDeviceSerializer.to_json(device) for server_name, device in devices.items()
        }
        for _, device in sort_devices(devices):
            rows.append(_dante_row_from_device(device))

    if shure_summaries:
        json_data["shure"] = shure_summaries
        for summary in sorted(shure_summaries.values(), key=lambda entry: (entry.get("name") or "").lower()):
            rows.append(_shure_row(summary))

    return rows, json_data


def status(
    json_flag: bool = typer.Option(False, "-j", "--json", help="Shorthand for --output=json."),
):
    """Show all discovered network audio devices."""
    from netaudio.cli import OutputFormat, state

    if json_flag:
        state.output_format = OutputFormat.json

    rows, json_data = asyncio.run(_gather_status())

    if not rows and state.output_format not in (OutputFormat.json, OutputFormat.yaml, OutputFormat.xml):
        from netaudio.daemon.client import daemon_is_accessible

        typer.echo("No devices found.")
        if not daemon_is_accessible():
            typer.echo("The netaudio daemon is not running; discovery used a one-shot mDNS scan.")
            typer.echo(
                "Start it with 'netaudio daemon start', or install it as a boot service with 'netaudio daemon install'."
            )
        typer.echo("Run 'netaudio --help' to see all commands.")
        return

    output_table(STATUS_HEADERS, rows, json_data=json_data)
