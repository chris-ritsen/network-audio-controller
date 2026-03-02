from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio_lib.dante.device_serializer import DanteDeviceSerializer

from netaudio._common import (
    _discover,
    _populate_controls,
    _resolve_device,
    filter_devices,
    output_single,
    output_table,
    set_device_filter,
    sort_devices,
)

app = typer.Typer(help="Manage Dante devices.", no_args_is_help=True)


def _format_mac(mac: str) -> str:
    if not mac:
        return ""
    raw = mac.replace(":", "").replace("-", "").upper()
    if len(raw) == 16 and raw[6:10] == "FFFE":
        raw = raw[:6] + raw[10:]
    elif len(raw) == 16 and raw.endswith("0000"):
        raw = raw[:12]
    return ":".join(raw[i:i+2] for i in range(0, len(raw), 2))


@app.command("list")
def device_list():
    """List discovered Dante devices."""

    async def _run():
        devices = await _discover()
        await _populate_controls(devices)
        devices = filter_devices(devices)

        headers = ["Name", "IP Address", "MAC Address", "Model", "TX", "RX", "Server Name"]
        rows = []
        json_data = {}

        for server_name, device in sort_devices(devices):
            rows.append([
                device.name or "",
                str(device.ipv4) if device.ipv4 else "",
                _format_mac(device.mac_address),
                device.model_id or "",
                str(device.tx_count or 0),
                str(device.rx_count or 0),
                server_name,
            ])
            json_data[server_name] = DanteDeviceSerializer.device_summary_to_json(device)

        output_table(headers, rows, json_data=json_data, devices=devices)

    asyncio.run(_run())


@app.command("show")
def device_show(
    device: str = typer.Argument(help="Device name, IP, or server name."),
):
    """Show detailed device information."""
    set_device_filter(device)

    async def _run():
        resolved_device = await _resolve_device()
        data = DanteDeviceSerializer.to_json(resolved_device)
        output_single(data, device=resolved_device)

    asyncio.run(_run())


@app.command()
def identify(
    device: str = typer.Argument(help="Device name, IP, or server name."),
):
    """Blink the identify LED on a device."""
    set_device_filter(device)

    async def _run():
        resolved_device = await _resolve_device()
        await resolved_device.operations.identify()
        typer.echo(f"Identified: {resolved_device.name}")

    asyncio.run(_run())


@app.command()
def name(
    device: str = typer.Argument(help="Device name, IP, or server name."),
    new_name: Optional[str] = typer.Argument(None, help="New name (omit to get, empty string to reset)."),
):
    """Get or set device name."""
    set_device_filter(device)

    async def _run():
        resolved_device = await _resolve_device()

        if new_name is None:
            output_single(resolved_device.name)
            return

        if new_name == "":
            await resolved_device.operations.reset_name()
            typer.echo(f"Reset name for {resolved_device.server_name}")
        else:
            await resolved_device.operations.set_name(new_name)
            typer.echo(f"Set name: {new_name}")

    asyncio.run(_run())
