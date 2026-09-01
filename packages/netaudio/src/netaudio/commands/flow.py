from __future__ import annotations

import asyncio
from typing import NoReturn

import typer

from netaudio._common import _discover, _get_arc_port, _populate_controls, _resolve_one
from netaudio._common_cli import HELP_CONTEXT_SETTINGS
from netaudio._common_output import output_table
from netaudio._common_selection import filter_devices
from netaudio._exit_codes import ExitCode
from netaudio.dante import flows
from netaudio.dante.const import PROTOCOL_ARC_2809, RESULT_CODE_SUCCESS

app = typer.Typer(
    help="Inspect receiver flows and manage transmitter multicast flows on the selected device.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


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


async def _get_device_and_app():
    devices = await _discover()
    server_name, device = _resolve_one(filter_devices(devices))
    await _populate_controls({server_name: device}, strict=False)
    return None, device, _get_arc_port(device)


@app.command("list")
def flow_list():
    """List transmitter flow records reported by a device."""

    async def _run():
        application, device, arc_port = await _get_device_and_app()
        try:
            device_ip = str(device.ipv4)
            flow_protocol_id = await _detect_flow_protocol(application, device, arc_port)
            if flow_protocol_id is None:
                typer.echo("Error: could not detect flow protocol for this device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            flow_inventory = await flows.query_preferred_tx_flow_inventory(
                device_ip,
                arc_port,
                flow_protocol_id,
            )
            if flow_inventory is None:
                typer.echo("Error: failed to query flows.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            device_flows = flow_inventory["flows"]
            include_status_endpoint = any(
                flow.get("destination_internet_protocol_version_four_address") or flow.get("subscriber_device_name")
                for flow in device_flows
            )
            headers = ["Slot", "Type", "Channels", "Sample Rate", "Encoding"]
            if include_status_endpoint:
                headers.extend(["Destination", "Subscriber"])
            else:
                headers.append("FPP")

            if not device_flows:
                from netaudio.cli import OutputFormat, state

                if state.output_format in (OutputFormat.plain, OutputFormat.table, OutputFormat.pretty):
                    if flow_protocol_id == PROTOCOL_ARC_2809 or include_status_endpoint:
                        typer.echo(
                            f"No transmitter flow records reported (capacity {flow_inventory['max_flow_slots']})."
                        )
                    else:
                        typer.echo(f"No TX flows configured (0/{flow_inventory['max_flow_slots']} slots used).")
                    return

            rows = []
            for flow in device_flows:
                channel_numbers = flow.get("channels")
                channel_list = (
                    ", ".join(str(channel_number) for channel_number in channel_numbers)
                    if isinstance(channel_numbers, list)
                    else ""
                )
                flow_type = flow.get("flow_type")
                if flow_type is None:
                    flow_type_code = flow.get("flow_type_code")
                    flow_type = f"0x{flow_type_code:04X}" if isinstance(flow_type_code, int) else "unknown"
                row = [
                    str(flow["flow_number"]),
                    flow_type,
                    channel_list or str(flow.get("channel_count") or ""),
                    str(flow["sample_rate"]),
                    str(flow["encoding"]),
                ]
                if include_status_endpoint:
                    destination_address = flow.get("destination_internet_protocol_version_four_address")
                    destination_port = flow.get("destination_user_datagram_port")
                    if destination_address and destination_port:
                        destination = f"{destination_address}:{destination_port}"
                    else:
                        destination = destination_address or ""
                    subscriber_device = flow.get("subscriber_device_name") or ""
                    subscriber_flow = flow.get("subscriber_flow_name") or ""
                    if subscriber_device and subscriber_flow:
                        subscriber = f"{subscriber_device}/{subscriber_flow}"
                    else:
                        subscriber = subscriber_device
                    row.extend([destination, subscriber])
                else:
                    frames_per_packet = flow.get("frames_per_packet")
                    row.append(str(frames_per_packet) if frames_per_packet is not None else "unknown")
                rows.append(row)
            output_table(headers, rows, json_data=flow_inventory)
        finally:
            if application is not None:
                await application.shutdown()

    asyncio.run(_run())


@app.command("receiver-list")
def receiver_flow_list():
    """List receiver flows and their local channel mappings."""

    async def _run():
        application, device, arc_port = await _get_device_and_app()
        try:
            flow_inventory = await flows.query_preferred_receiver_flow_inventory(device)
            if flow_inventory is None:
                typer.echo("Error: failed to query receiver flows.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            receiver_flows = flow_inventory["flows"]
            headers = [
                "Slot",
                "Type",
                "Receiver Channels",
                "Status",
                "Destination",
                "Port",
                "Sample Rate",
                "Encoding",
                "Frames/Packet",
                "Latency (ns)",
            ]

            if not receiver_flows:
                from netaudio.cli import OutputFormat, state

                if state.output_format in (
                    OutputFormat.plain,
                    OutputFormat.table,
                    OutputFormat.pretty,
                ):
                    typer.echo(f"No receiver flows configured (0/{flow_inventory['maximum_flow_slots']} slots used).")
                    return

            rows = []
            for receiver_flow in receiver_flows:
                channel_lists = receiver_flow.get("receiver_channel_numbers_by_flow_channel")
                if channel_lists is not None:
                    receiver_channel_mapping = " / ".join(
                        "+".join(str(number) for number in receiver_channel_numbers) or "-"
                        for receiver_channel_numbers in channel_lists
                    )
                else:
                    descriptor = receiver_flow.get("receiver_mapping_descriptor_hexadecimal")
                    receiver_channel_mapping = f"raw {descriptor}" if descriptor else "unknown"
                subscription_status_code = receiver_flow.get("subscription_status_code")
                if subscription_status_code is not None:
                    status_display = f"0x{subscription_status_code:04X}"
                else:
                    offset_62_word = receiver_flow.get("status_code_at_record_offset_62")
                    status_display = f"raw 0x{offset_62_word:04X}" if offset_62_word is not None else "unknown"
                rows.append(
                    [
                        str(receiver_flow["flow_number"]),
                        receiver_flow["flow_type"] or "unknown",
                        receiver_channel_mapping,
                        status_display,
                        receiver_flow["destination_internet_protocol_version_four_address"],
                        (
                            str(receiver_flow["destination_user_datagram_port"])
                            if receiver_flow["destination_user_datagram_port"] is not None
                            else "unknown"
                        ),
                        str(
                            receiver_flow["sample_rate"] if receiver_flow.get("sample_rate") is not None else "unknown"
                        ),
                        str(receiver_flow["encoding"] if receiver_flow.get("encoding") is not None else "unknown"),
                        (
                            str(receiver_flow["frames_per_packet"])
                            if receiver_flow.get("frames_per_packet") is not None
                            else "unknown"
                        ),
                        (
                            str(receiver_flow["latency_nanoseconds"])
                            if receiver_flow.get("latency_nanoseconds") is not None
                            else "unknown"
                        ),
                    ]
                )
            output_table(headers, rows, json_data=flow_inventory)
        finally:
            if application is not None:
                await application.shutdown()

    asyncio.run(_run())


@app.command("receiver-port-ranges")
def receiver_port_ranges():
    """Show the receiver port ranges reported by a device."""

    async def _run():
        application, device, arc_port = await _get_device_and_app()
        try:
            port_ranges = await flows.query_receiver_port_ranges(
                str(device.ipv4),
                arc_port,
            )
            if port_ranges is None:
                typer.echo("Error: failed to query receiver port ranges.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            rows = [
                [
                    "First",
                    str(port_ranges["first_port_range_start"]),
                    str(port_ranges["first_port_range_end"]),
                ],
                [
                    "Second",
                    str(port_ranges["second_port_range_start"]),
                    str(port_ranges["second_port_range_end"]),
                ],
            ]
            output_table(["Range", "Start", "End"], rows, json_data=port_ranges)
        finally:
            if application is not None:
                await application.shutdown()

    asyncio.run(_run())


@app.command("transmit-channel-capabilities")
def transmit_channel_capabilities(
    starting_channel_identifier: int = typer.Option(
        1,
        "--starting-channel",
        min=1,
        max=65535,
        help="First transmitter channel identifier to query.",
    ),
    maximum_channel_count: int = typer.Option(
        0,
        "--maximum-count",
        min=0,
        max=65535,
        help="Maximum channels to return; zero requests all available channels.",
    ),
):
    """Show the transmitter channel capacity and raw capability flags reported by a device."""

    async def _run():
        application, device, arc_port = await _get_device_and_app()
        try:
            capabilities = await flows.query_transmit_channel_capabilities(
                str(device.ipv4),
                arc_port,
                starting_channel_identifier,
                maximum_channel_count,
            )
            if capabilities is None:
                typer.echo(
                    "Error: this device does not report transmitter channel capabilities.",
                    err=True,
                )
                raise typer.Exit(code=ExitCode.ERROR)
            rows = [
                ["Format identifier", str(capabilities["format_identifier"])],
                ["Starting channel", str(capabilities["starting_channel_identifier"])],
                ["Channel count", str(capabilities["channel_count"])],
                ["Capability flags", f"0x{capabilities['capability_flags']:04X}"],
            ]
            output_table(["Field", "Value"], rows, json_data=capabilities)
        finally:
            if application is not None:
                await application.shutdown()

    asyncio.run(_run())


@app.command("create")
def flow_create(
    slot: int = typer.Option(..., "--slot", help="Flow slot number, limited by the device-reported capacity."),
    channels: str = typer.Option(..., "--channels", help="Comma-separated TX channel numbers."),
):
    """Create a TX multicast flow."""

    async def _run():
        try:
            flow_slot = flows.validate_flow_slot(slot)
        except flows.FlowValidationError as exception:
            _fail_validation(exception)
        channel_numbers = _parse_channel_numbers(channels)

        application, device, arc_port = await _get_device_and_app()
        try:
            device_ip = str(device.ipv4)
            flow_protocol_id = await _detect_flow_protocol(application, device, arc_port)
            if flow_protocol_id is None:
                typer.echo("Error: could not detect flow protocol for this device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            try:
                flows.require_creatable_flow_protocol(flow_protocol_id)
            except flows.FlowValidationError as exception:
                _fail_validation(exception)

            try:
                flow_inventory = await flows.query_tx_flow_inventory(
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
            if flow_inventory is None:
                typer.echo("Error: failed to query existing flows; no change was sent.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            device_flows = flow_inventory["flows"]

            available_channels = {int(number) for number in (device.tx_channels or {}).keys()}
            try:
                flows.require_supported_flow_slot(flow_slot, flow_inventory["max_flow_slots"])
                flows.require_available_tx_channels(channel_numbers, available_channels)
                flows.require_available_flow_slot(device_flows, flow_slot)
            except flows.FlowValidationError as exception:
                _fail_validation(exception)

            try:
                async with device.topology_mutation_lock:
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
                f"{device.name or device.ipv4}: channels {channel_label} (device confirmed)."
            )
        finally:
            if application is not None:
                await application.shutdown()

    asyncio.run(_run())


@app.command("delete")
def flow_delete(
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

        application, device, arc_port = await _get_device_and_app()
        try:
            device_ip = str(device.ipv4)
            flow_protocol_id = await _detect_flow_protocol(application, device, arc_port)
            if flow_protocol_id is None:
                typer.echo("Error: could not detect flow protocol for this device.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            try:
                flows.require_deletable_flow_protocol(flow_protocol_id, flow_slot)
            except flows.FlowValidationError as exception:
                _fail_validation(exception)

            try:
                flow_inventory = await flows.query_tx_flow_inventory(
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
            if flow_inventory is None:
                typer.echo("Error: failed to query existing flows; no deletion was sent.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)
            device_flows = flow_inventory["flows"]
            try:
                flows.require_supported_flow_slot(flow_slot, flow_inventory["max_flow_slots"])
                flows.require_multicast_flow(device_flows, flow_slot)
            except flows.FlowValidationError as exception:
                _fail_validation(exception)

            try:
                async with device.topology_mutation_lock:
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
                f"Deleted multicast TX flow from slot {flow_slot} on {device.name or device.ipv4} (device confirmed)."
            )
        finally:
            if application is not None:
                await application.shutdown()

    asyncio.run(_run())
