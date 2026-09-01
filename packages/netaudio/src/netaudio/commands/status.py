from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import typer

from netaudio._common_output import output_single, output_table
from netaudio._common_selection import sort_devices
from netaudio.icons import icon

DANTE_STATUS_HEADERS = [
    "Name",
    "Status",
    "Manufacturer",
    "Model",
    "IP Address",
    "TX",
    "RX",
    "Clock",
    "Lock",
    "Last Seen",
]

SHURE_STATUS_HEADERS = ["Name", "Status", "Model", "IP Address", "Type", "Channels", "Last Seen"]

SHURE_DEVICE_TYPE_LABELS = {
    "ad4d": "receiver",
    "p10t": "transmitter",
}


def _format_timestamp(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _lock_display(is_locked) -> str:
    if is_locked is True:
        return icon("lock") or "locked"
    if is_locked is False:
        return icon("unlock") or "unlocked"
    return ""


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
        device.manufacturer or "",
        device.dante_model or device.model_id or "",
        str(device.ipv4) if device.ipv4 else "",
        str(len(device.tx_channels) if device.tx_channels else (device.tx_count or 0)),
        str(len(device.rx_channels) if device.rx_channels else (device.rx_count or 0)),
        device.clock_role or "",
        _lock_display(device.is_locked),
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


async def _gather_status() -> tuple[list[list[str]], list[list[str]], dict]:
    from netaudio.daemon.client import daemon_is_accessible, get_shure_devices_from_daemon

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
    for _, device in sort_devices(visible_dante):
        dante_rows.append(_dante_row_from_device(device))

    if shure_summaries:
        visible_shure = _visible_shure_summaries(shure_summaries)
        json_data["shure"] = {
            summary.get("mac") or summary.get("name") or str(index): summary
            for index, summary in enumerate(visible_shure)
        }
        shure_rows = [_shure_row(summary) for summary in visible_shure]

    return dante_rows, shure_rows, json_data


def status(
    json_flag: bool = typer.Option(False, "-j", "--json", help="Shorthand for --output=json."),
):
    """Show all discovered network audio devices."""
    from netaudio.cli import OutputFormat, state

    if json_flag:
        state.output_format = OutputFormat.json

    dante_rows, shure_rows, json_data = asyncio.run(_gather_status())

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
        output_table(DANTE_STATUS_HEADERS, dante_rows, title="Dante")
    if shure_rows:
        output_table(SHURE_STATUS_HEADERS, shure_rows, title="Shure")
