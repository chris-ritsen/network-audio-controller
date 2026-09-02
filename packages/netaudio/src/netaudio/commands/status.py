from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import typer

from netaudio._common_output import output_single, output_table
from netaudio._common_selection import sort_devices
from netaudio.commands.device_display import (
    _device_active_latency,
    _device_encoding,
    device_list_headers,
    device_list_row,
    format_encoding,
    format_latency_milliseconds,
    format_lock_state,
    format_sample_rate_hertz,
)

DANTE_STATUS_HEADERS = [
    "Name",
    "Status",
    "Kind",
    "Manufacturer",
    "Model",
    "IP Address",
    "TX",
    "RX",
    "Sample Rate",
    "Encoding",
    "Latency",
    "Clock",
    "Lock",
    "Last Seen",
]

SHURE_DEVICE_TYPE_LABELS = {
    "ad4d": "receiver",
    "p10t": "transmitter",
}

SHURE_STATUS_HEADERS = ["Name", "Status", "Model", "IP Address", "Type", "Channels", "Last Seen"]


def _format_timestamp(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _status_label(online) -> str:
    if online is False:
        return "offline"
    return "online"


def _has_presence(online, last_seen) -> bool:
    if online is False and last_seen is None:
        return False
    return True


def _dante_row_from_device(device) -> list[str]:
    return [
        device.name or "",
        _status_label(device.online),
        device.kind,
        device.manufacturer or "",
        device.dante_model or device.model_id or "",
        str(device.ipv4) if device.ipv4 else "",
        str(len(device.tx_channels) if device.tx_channels else (device.tx_count or 0)),
        str(len(device.rx_channels) if device.rx_channels else (device.rx_count or 0)),
        format_sample_rate_hertz(device.sample_rate),
        format_encoding(_device_encoding(device)),
        format_latency_milliseconds(_device_active_latency(device)),
        device.clock_role or "",
        format_lock_state(device),
        _format_timestamp(device.last_seen),
    ]


def _shure_row(summary: dict) -> list[str]:
    channels = summary.get("channels") or {}
    device_type = summary.get("device_type") or ""
    return [
        summary.get("name") or "",
        _status_label(summary.get("online")),
        summary.get("model") or device_type,
        summary.get("ip") or "",
        SHURE_DEVICE_TYPE_LABELS.get(device_type, device_type),
        str(len(channels)) if channels else "",
        _format_timestamp(summary.get("last_seen")),
    ]


def _visible_shure_summaries(shure_summaries: dict) -> list[dict]:
    visible = []
    for summary in shure_summaries.values():
        if not _has_presence(summary.get("online"), summary.get("last_seen")):
            continue
        visible.append(summary)
    return sorted(visible, key=lambda entry: (entry.get("name") or "").lower())


async def _gather_status(verbose: bool = False) -> tuple[list[str], list[list[str]], list[list[str]], dict]:
    from netaudio.daemon.client import daemon_is_accessible, get_shure_devices_from_daemon

    dante_headers = device_list_headers(True) if verbose else list(DANTE_STATUS_HEADERS)
    dante_rows: list[list[str]] = []
    shure_rows: list[list[str]] = []
    json_data: dict = {}

    shure_summaries = None
    if daemon_is_accessible():
        shure_summaries = await get_shure_devices_from_daemon()

    from netaudio._common import _load_display_devices
    from netaudio.dante.device_serializer import DanteDeviceSerializer

    devices = await _load_display_devices()
    visible_dante = {
        server_name: device for server_name, device in devices.items() if _has_presence(device.online, device.last_seen)
    }
    json_data["dante"] = {
        server_name: DanteDeviceSerializer.to_json(device) for server_name, device in visible_dante.items()
    }
    for server_name, device in sort_devices(visible_dante):
        if verbose:
            dante_rows.append(device_list_row(server_name, device, verbose=True))
        else:
            dante_rows.append(_dante_row_from_device(device))

    if shure_summaries:
        visible_shure = _visible_shure_summaries(shure_summaries)
        json_data["shure"] = {
            summary.get("mac") or summary.get("name") or str(index): summary
            for index, summary in enumerate(visible_shure)
        }
        shure_rows = [_shure_row(summary) for summary in visible_shure]

    return dante_headers, dante_rows, shure_rows, json_data


def status(
    json_flag: bool = typer.Option(False, "-j", "--json", help="Shorthand for --output=json."),
):
    """Show all discovered network audio devices."""
    from netaudio.cli import OutputFormat, state

    if json_flag:
        state.output_format = OutputFormat.json

    dante_headers, dante_rows, shure_rows, json_data = asyncio.run(_gather_status(verbose=state.verbose))

    if state.output_format in (OutputFormat.json, OutputFormat.xml, OutputFormat.yaml):
        output_single(json_data)
        return

    if not dante_rows and not shure_rows:
        from netaudio.daemon.client import daemon_is_accessible

        typer.echo("No devices found.")
        if not daemon_is_accessible():
            typer.echo("The netaudio daemon is not running; discovery used a one-shot mDNS scan.")
            typer.echo(
                "Start it with 'netaudio daemon start', or install it as a boot service with 'netaudio daemon install'."
            )
        typer.echo("Run 'netaudio --help' to see all commands.")
        return

    if dante_rows:
        output_table(dante_headers, dante_rows, omit_empty_columns=state.verbose)
    if shure_rows:
        output_table(SHURE_STATUS_HEADERS, shure_rows, title="Shure")
