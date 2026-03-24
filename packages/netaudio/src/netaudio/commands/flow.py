from __future__ import annotations

import asyncio
import struct

import typer

from netaudio._common import (
    _get_arc_port,
    filter_devices,
    find_device,
    output_table,
)
from netaudio._exit_codes import ExitCode

app = typer.Typer(help="Manage TX multicast flows.", no_args_is_help=True)


async def _detect_flow_protocol(application, device, arc_port):
    if device.flow_protocol_id is not None:
        return device.flow_protocol_id

    flow_protocol_id = await application.arc.detect_flow_protocol(
        str(device.ipv4), arc_port
    )
    if flow_protocol_id is not None:
        device.flow_protocol_id = flow_protocol_id
    return flow_protocol_id


async def _get_device_and_app(device_name: str):
    from netaudio.dante.application import DanteApplication
    from netaudio.common.app_config import settings

    application = DanteApplication()
    await application.startup()
    devices = await application.discover_and_populate(timeout=settings.mdns_timeout)
    devices = devices or {}
    devices = filter_devices(devices)

    device = find_device(devices, device_name)
    if device is None:
        typer.echo(f"Error: device not found: {device_name}", err=True)
        await application.shutdown()
        raise typer.Exit(code=ExitCode.ERROR)

    arc_port = _get_arc_port(device)

    return application, device, arc_port


@app.command("list")
def flow_list(
    device_name: str = typer.Argument(..., help="Device name or IP."),
):
    """List TX multicast flows on a device."""

    async def _run():
        application, device, arc_port = await _get_device_and_app(device_name)
        try:
            device_ip = str(device.ipv4)
            flow_protocol_id = await _detect_flow_protocol(application, device, arc_port)
            if flow_protocol_id is None:
                typer.echo("Error: could not detect flow protocol for this device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            flows = await application.arc.query_tx_flows(device_ip, arc_port, flow_protocol_id)
            if flows is None:
                typer.echo("Error: failed to query flows.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            if not flows:
                typer.echo("No TX flows configured.")
                return

            headers = ["Slot", "Type", "Channels", "Sample Rate", "Encoding", "FPP"]
            rows = []
            for flow in flows:
                channel_list = ", ".join(str(channel_number) for channel_number in flow["channels"])
                rows.append([
                    str(flow["flow_number"]),
                    flow["flow_type"],
                    channel_list or str(flow["channel_count"]),
                    str(flow["sample_rate"]),
                    str(flow["encoding"]),
                    str(flow["fpp"]),
                ])
            output_table(headers, rows, json_data=flows)
        finally:
            await application.shutdown()

    asyncio.run(_run())


@app.command("create")
def flow_create(
    device_name: str = typer.Argument(..., help="Device name or IP."),
    slot: int = typer.Option(..., "--slot", help="Flow slot number (1-32, multicast typically 17-32)."),
    channels: str = typer.Option(..., "--channels", help="Comma-separated TX channel numbers."),
):
    """Create a TX multicast flow."""

    async def _run():
        channel_numbers = [int(channel_string.strip()) for channel_string in channels.split(",")]
        if not channel_numbers:
            typer.echo("Error: at least one channel required.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        application, device, arc_port = await _get_device_and_app(device_name)
        try:
            device_ip = str(device.ipv4)
            flow_protocol_id = await _detect_flow_protocol(application, device, arc_port)
            if flow_protocol_id is None:
                typer.echo("Error: could not detect flow protocol for this device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            response = await application.arc.create_tx_flow(
                device_ip, arc_port, flow_protocol_id, slot, channel_numbers,
            )
            if response and len(response) >= 10:
                result_code = struct.unpack(">H", response[8:10])[0]
                if result_code == 0x0001:
                    return
                else:
                    typer.echo(f"Error: create flow failed with result 0x{result_code:04X}", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)
            else:
                typer.echo("Error: no response from device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
        finally:
            await application.shutdown()

    asyncio.run(_run())


@app.command("delete")
def flow_delete(
    device_name: str = typer.Argument(..., help="Device name or IP."),
    slot: int = typer.Option(..., "--slot", help="Flow slot number to delete."),
):
    """Delete a TX multicast flow."""

    async def _run():
        application, device, arc_port = await _get_device_and_app(device_name)
        try:
            device_ip = str(device.ipv4)
            flow_protocol_id = await _detect_flow_protocol(application, device, arc_port)
            if flow_protocol_id is None:
                typer.echo("Error: could not detect flow protocol for this device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            response = await application.arc.delete_tx_flow(
                device_ip, arc_port, flow_protocol_id, slot,
            )
            if response and len(response) >= 10:
                result_code = struct.unpack(">H", response[8:10])[0]
                if result_code == 0x0001:
                    return
                else:
                    typer.echo(f"Error: delete flow failed with result 0x{result_code:04X}", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)
            else:
                typer.echo("Error: no response from device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
        finally:
            await application.shutdown()

    asyncio.run(_run())
