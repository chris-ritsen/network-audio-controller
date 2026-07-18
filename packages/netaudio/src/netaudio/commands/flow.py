from __future__ import annotations

import asyncio
from typing import NoReturn

import typer

from netaudio._common import (
    _get_arc_port,
    filter_devices,
    find_device,
    output_table,
)
from netaudio._exit_codes import ExitCode
from netaudio.dante import flows
from netaudio.dante.const import RESULT_CODE_SUCCESS

app = typer.Typer(help="Manage TX multicast flows.", no_args_is_help=True)


def _fail_validation(exception: flows.FlowValidationError) -> NoReturn:
    typer.echo(f"Error: {exception}", err=True)
    raise typer.Exit(code=ExitCode.ERROR)


def _parse_channel_numbers(value: str) -> list[int]:
    tokens = [token.strip() for token in value.split(",")]
    if not tokens or any(not token for token in tokens):
        _fail_validation(flows.FlowValidationError("channels must be a comma-separated list of integers"))
    try:
        channel_numbers = [int(token) for token in tokens]
    except ValueError:
        _fail_validation(flows.FlowValidationError("channels must be a comma-separated list of integers"))
    try:
        return flows.validate_flow_channels(channel_numbers)
    except flows.FlowValidationError as exception:
        _fail_validation(exception)


async def _detect_flow_protocol(application, device, arc_port):
    if device.flow_protocol_id is not None:
        return device.flow_protocol_id

    flow_protocol_id = await flows.detect_flow_protocol(str(device.ipv4), arc_port)
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

            device_flows = await flows.query_tx_flows(device_ip, arc_port, flow_protocol_id)
            if device_flows is None:
                typer.echo("Error: failed to query flows.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            if not device_flows:
                typer.echo("No TX flows configured.")
                return

            headers = ["Slot", "Type", "Channels", "Sample Rate", "Encoding", "FPP"]
            rows = []
            for flow in device_flows:
                channel_list = ", ".join(str(channel_number) for channel_number in flow["channels"])
                rows.append(
                    [
                        str(flow["flow_number"]),
                        flow["flow_type"],
                        channel_list or str(flow["channel_count"]),
                        str(flow["sample_rate"]),
                        str(flow["encoding"]),
                        str(flow["frames_per_packet"]),
                    ]
                )
            output_table(headers, rows, json_data=device_flows)
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
        try:
            flow_slot = flows.validate_flow_slot(slot)
        except flows.FlowValidationError as exception:
            _fail_validation(exception)
        channel_numbers = _parse_channel_numbers(channels)

        application, device, arc_port = await _get_device_and_app(device_name)
        try:
            device_ip = str(device.ipv4)
            flow_protocol_id = await _detect_flow_protocol(application, device, arc_port)
            if flow_protocol_id is None:
                typer.echo("Error: could not detect flow protocol for this device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            try:
                device_flows = await flows.query_tx_flows(
                    device_ip,
                    arc_port,
                    flow_protocol_id,
                )
            except Exception as exception:
                typer.echo(
                    f"Error: failed to query existing flows: {exception}; no change was sent.",
                    err=True,
                )
                raise typer.Exit(code=ExitCode.ERROR) from exception
            if device_flows is None:
                typer.echo("Error: failed to query existing flows; no change was sent.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            available_channels = {int(number) for number in (device.tx_channels or {}).keys()}
            try:
                flows.require_available_tx_channels(channel_numbers, available_channels)
                flows.require_available_flow_slot(device_flows, flow_slot)
            except flows.FlowValidationError as exception:
                _fail_validation(exception)

            try:
                result_code = await flows.create_tx_flow(
                    device_ip,
                    arc_port,
                    flow_protocol_id,
                    flow_slot,
                    channel_numbers,
                )
            except Exception as exception:
                typer.echo(f"Error: flow creation failed: {exception}", err=True)
                raise typer.Exit(code=ExitCode.ERROR) from exception
            if result_code is None:
                typer.echo("Error: no response from device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            if result_code != RESULT_CODE_SUCCESS:
                typer.echo(f"Error: create flow failed with result 0x{result_code:04X}", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            channel_label = ", ".join(str(number) for number in channel_numbers)
            typer.echo(
                f"Created multicast TX flow in slot {flow_slot} on "
                f"{device.name or device_name}: channels {channel_label} (device confirmed)."
            )
        finally:
            await application.shutdown()

    asyncio.run(_run())


@app.command("delete")
def flow_delete(
    device_name: str = typer.Argument(..., help="Device name or IP."),
    slot: int = typer.Option(..., "--slot", help="Flow slot number to delete."),
    confirmed: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Confirm deletion of the active multicast flow.",
    ),
):
    """Delete a TX multicast flow."""

    async def _run():
        try:
            flow_slot = flows.validate_flow_slot(slot)
        except flows.FlowValidationError as exception:
            _fail_validation(exception)
        if not confirmed:
            typer.echo(
                f"Error: refusing to delete flow slot {flow_slot} without --yes.",
                err=True,
            )
            raise typer.Exit(code=ExitCode.ERROR)

        application, device, arc_port = await _get_device_and_app(device_name)
        try:
            device_ip = str(device.ipv4)
            flow_protocol_id = await _detect_flow_protocol(application, device, arc_port)
            if flow_protocol_id is None:
                typer.echo("Error: could not detect flow protocol for this device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            try:
                device_flows = await flows.query_tx_flows(
                    device_ip,
                    arc_port,
                    flow_protocol_id,
                )
            except Exception as exception:
                typer.echo(
                    f"Error: failed to query existing flows: {exception}; no deletion was sent.",
                    err=True,
                )
                raise typer.Exit(code=ExitCode.ERROR) from exception
            if device_flows is None:
                typer.echo("Error: failed to query existing flows; no deletion was sent.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            try:
                flows.require_multicast_flow(device_flows, flow_slot)
            except flows.FlowValidationError as exception:
                _fail_validation(exception)

            try:
                result_code = await flows.delete_tx_flow(
                    device_ip,
                    arc_port,
                    flow_protocol_id,
                    flow_slot,
                )
            except Exception as exception:
                typer.echo(f"Error: flow deletion failed: {exception}", err=True)
                raise typer.Exit(code=ExitCode.ERROR) from exception
            if result_code is None:
                typer.echo("Error: no response from device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            if result_code != RESULT_CODE_SUCCESS:
                typer.echo(f"Error: delete flow failed with result 0x{result_code:04X}", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            typer.echo(
                f"Deleted multicast TX flow from slot {flow_slot} on {device.name or device_name} (device confirmed)."
            )
        finally:
            await application.shutdown()

    asyncio.run(_run())
