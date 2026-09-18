from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import NoReturn

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import _get_arc_port, run_command
from netaudio.cli_support.output import output_single, output_table, structured_output_selected
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.commands.device.display import (
    format_encoding,
    format_latency_nanoseconds,
    format_sample_rate_hertz,
)
from netaudio.core.binding import NetaudioCoreError
from netaudio.dante import flows
from netaudio.dante.const import subscription_status_entry
from netaudio.dante.flow_lifecycle import (
    create_transmit_flow,
    delete_transmit_flow,
    inspect_transmit_flows,
    plan_create_transmit_flow,
)
from netaudio.dante.transmit_flow import TransmitFlowSpecification

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


def _parse_flow_slot_assignments(value: str) -> list[int]:
    tokens = [token.strip() for token in value.split(",")]
    if not tokens or any(not token for token in tokens):
        _fail_validation(flows.FlowValidationError("flow slots must be a comma-separated list of integers"))
    try:
        assignments = [int(token) for token in tokens]
    except ValueError:
        _fail_validation(flows.FlowValidationError("flow slots must be a comma-separated list of integers"))
    if any(value < 0 for value in assignments):
        _fail_validation(flows.FlowValidationError("flow slots must be zero or positive integers"))
    return assignments


def _managed_transport_option(device) -> dict:
    return {"device": device} if getattr(device, "requires_managed_control", False) else {}


async def _detect_flow_protocol(application, device, arc_port):
    if device.flow_protocol_id is not None:
        return device.flow_protocol_id

    flow_protocol_id = await flows.detect_flow_protocol(str(device.ipv4), arc_port, **_managed_transport_option(device))
    if flow_protocol_id is not None:
        device.flow_protocol_id = flow_protocol_id
    return flow_protocol_id


def _selected_device(devices):
    [(_, device)] = select_device(filter_devices(devices))
    return device, _get_arc_port(device)


def _read_specification(path: str) -> TransmitFlowSpecification:
    try:
        content = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
        value = json.loads(content)
        return TransmitFlowSpecification.from_dict(value)
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exception:
        _fail_validation(flows.FlowValidationError(f"invalid transmit-flow specification: {exception}"))


def _require_successful_result(result) -> None:
    payload = result.to_dict()
    if result.state.value in {"confirmed", "deleted"}:
        if structured_output_selected():
            output_single(payload)
        else:
            typer.echo(result.message)
            acknowledgement = result.request_acknowledgement
            if acknowledgement is None:
                acknowledgement_label = "not received"
            elif acknowledgement.get("accepted") is True:
                acknowledgement_label = f"accepted (result {acknowledgement.get('result_code')})"
            elif acknowledgement.get("parseable") is True:
                acknowledgement_label = f"rejected (result {acknowledgement.get('result_code')})"
            else:
                acknowledgement_label = "received but unparseable"
            typer.echo(f"Request acknowledgement: {acknowledgement_label}")
            typer.echo("Device confirmation: unavailable from this ARC transport")
            typer.echo(
                "Effective-state confirmation: "
                + ("confirmed" if result.effective_state_confirmation is True else "not confirmed")
            )
            typer.echo("Persistence confirmation: not performed")
        return
    typer.echo(f"Error: {result.message}", err=True)
    if result.state.value in {"partial", "pending", "inconsistent"}:
        typer.echo(json.dumps(payload, sort_keys=True), err=True)
    raise typer.Exit(code=ExitCode.ERROR)


async def run_flow_inspect(application, devices) -> None:
    device, _ = _selected_device(devices)
    try:
        inventory = await inspect_transmit_flows(device)
    except flows.FlowValidationError as exception:
        _fail_validation(exception)
    rows = []
    for specification in inventory["flows"]:
        identity = specification["identity"]
        destination = specification.get("primary_destination")
        rows.append(
            [
                str(identity.get("global_flow_id") or ""),
                specification["media_mode"],
                specification["flow_type"],
                specification.get("name") or "",
                ", ".join(
                    f"{entry['slot']}:{entry['transmitter_channel']}" for entry in specification["channel_slots"]
                ),
                format_sample_rate_hertz(specification.get("sample_rate_hz")),
                format_encoding(specification.get("encoding_bits")),
                (f"{destination['address']}:{destination['port']}" if destination is not None else "device allocated"),
            ]
        )
    output_table(
        ["Flow", "Media", "Type", "Name", "Slot:Channel", "Sample Rate", "Encoding", "Destination"],
        rows,
        json_data=inventory,
        empty_message="No transmitter flows are active.",
    )


@app.command("inspect")
def flow_inspect():
    """Read transmitter flows into the canonical, lossless flow schema."""
    run_command(run_flow_inspect)


async def run_flow_plan(application, devices, specification: TransmitFlowSpecification) -> None:
    device, _ = _selected_device(devices)
    output_single(plan_create_transmit_flow(device, specification).to_dict())


@app.command("plan")
def flow_plan(
    specification_file: str = typer.Argument(..., help="Canonical flow JSON file, or - for standard input."),
):
    """Validate and plan a canonical transmit-flow request without sending it."""
    run_command(run_flow_plan, _read_specification(specification_file))


async def run_flow_apply(application, devices, specification: TransmitFlowSpecification) -> None:
    device, _ = _selected_device(devices)
    operation = getattr(application, "create_transmit_flow", None)
    result = (
        await operation(device, specification)
        if operation is not None
        else await create_transmit_flow(device, specification)
    )
    _require_successful_result(result)


@app.command("apply")
def flow_apply(
    specification_file: str = typer.Argument(..., help="Canonical flow JSON file, or - for standard input."),
    confirmed: bool = typer.Option(False, "--yes", "-y", help="Confirm transmit-flow creation."),
):
    """Create an evidence-supported canonical transmit flow and verify readback."""
    if not confirmed:
        _fail_validation(flows.FlowValidationError("--yes is required"))
    run_command(run_flow_apply, _read_specification(specification_file))


async def run_receiver_flow_list(application, devices) -> None:
    device, arc_port = _selected_device(devices)
    flow_inventory = await flows.query_preferred_receiver_flow_inventory(device, require_complete=False)
    if flow_inventory is None:
        typer.echo("Error: failed to query receiver flows.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    receiver_flows = flow_inventory["flows"]
    complete = flow_inventory.get("page_disposition") == "complete"
    if not complete:
        typer.echo(
            f"Partial receiver-flow inventory: {len(receiver_flows)} flows returned; remaining flows unavailable.",
            err=True,
        )
    headers = [
        "Slot",
        "Type",
        "Receiver Channels",
        "Status",
        "Interface Destinations",
        "Transport",
        "External Identity",
        "SDP",
        "Sample Rate",
        "Encoding",
        "Frames/Packet",
        "Latency",
    ]

    empty_message = (
        f"No receiver flows configured (0/{flow_inventory['maximum_flow_slots']} slots used)."
        if complete
        else "Receiver-flow inventory unavailable."
    )

    rows = []
    for receiver_flow in receiver_flows:
        channel_lists = receiver_flow.get("receiver_channel_numbers_by_flow_channel")
        if channel_lists is not None:
            receiver_channel_mapping = " / ".join(
                "+".join(str(number) for number in receiver_channel_numbers) or "-"
                for receiver_channel_numbers in channel_lists
            )
        else:
            receiver_channel_mapping = "unknown"
        subscription_status_code = receiver_flow.get("subscription_status_code")
        if subscription_status_code is not None:
            status_display = str(subscription_status_entry(subscription_status_code)["label"])
        else:
            status_display = "unknown"
        flow_type = receiver_flow["flow_type"]
        if not flow_type or flow_type.startswith("0x"):
            flow_type = "unknown"
        endpoints = receiver_flow.get("interface_endpoints")
        if not isinstance(endpoints, list):
            port = receiver_flow.get("destination_user_datagram_port")
            endpoints = (
                [
                    {
                        "ipv4_address": receiver_flow.get("destination_internet_protocol_version_four_address"),
                        "udp_port": port,
                    }
                ]
                if port is not None
                else []
            )
        endpoint_display = (
            ", ".join(
                f"{endpoint.get('ipv4_address') or 'address unavailable'}:{endpoint.get('udp_port', 'port unavailable')}"
                for endpoint in endpoints
            )
            or "unknown"
        )
        external_identity = receiver_flow.get("external_identity")
        external_display = (
            f"{external_identity.get('source_ipv4') or 'source unavailable'}/"
            f"{external_identity.get('session_id', 'session unavailable')}"
            if isinstance(external_identity, dict)
            else "native"
        )
        correlation = receiver_flow.get("sdp_correlation")
        sdp_display = (
            "matched"
            if isinstance(correlation, dict) and correlation.get("matched") is True
            else "not matched"
            if isinstance(external_identity, dict)
            else "not applicable"
        )
        rows.append(
            [
                str(receiver_flow["flow_number"]),
                flow_type,
                receiver_channel_mapping,
                status_display,
                endpoint_display,
                str(receiver_flow.get("transport", "unknown")),
                external_display,
                sdp_display,
                (
                    format_sample_rate_hertz(receiver_flow["sample_rate"])
                    if receiver_flow.get("sample_rate") is not None
                    else "unknown"
                ),
                (
                    format_encoding(receiver_flow["encoding"])
                    if receiver_flow.get("encoding") is not None
                    else "unknown"
                ),
                (
                    str(receiver_flow["frames_per_packet"])
                    if receiver_flow.get("frames_per_packet") is not None
                    else "unknown"
                ),
                (
                    format_latency_nanoseconds(receiver_flow["latency_nanoseconds"])
                    if receiver_flow.get("latency_nanoseconds") is not None
                    else "unknown"
                ),
            ]
        )
    output_table(headers, rows, json_data=flow_inventory, empty_message=empty_message)


@app.command("receiver-list")
def receiver_flow_list():
    """List receiver flows and their local channel mappings."""
    run_command(run_receiver_flow_list)


async def run_receiver_port_ranges(application, devices) -> None:
    device, arc_port = _selected_device(devices)
    port_ranges = await flows.query_receiver_port_ranges(
        str(device.ipv4),
        arc_port,
        **_managed_transport_option(device),
    )
    if port_ranges is None:
        typer.echo("Error: failed to query receiver port ranges.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    rows = [
        [
            "First",
            str(port_ranges["first_port_range_start"]),
            str(port_ranges["first_port_range_end"]),
        ]
    ]
    if port_ranges.get("second_port_range_available", True):
        rows.append(
            [
                "Second",
                str(port_ranges["second_port_range_start"]),
                str(port_ranges["second_port_range_end"]),
            ]
        )
    output_table(["Range", "Start", "End"], rows, json_data=port_ranges)


@app.command("receiver-port-ranges")
def receiver_port_ranges():
    """Show the receiver port ranges reported by a device."""
    run_command(run_receiver_port_ranges)


async def run_external_flow_list(application, devices, listen_seconds: float) -> None:
    if listen_seconds:
        await asyncio.sleep(listen_seconds)
    discovered = application.external_flows.flows()
    rows = []
    for flow in discovered:
        primary = (
            f"{flow.primary_destination_address}:{flow.primary_destination_port}"
            if flow.primary_destination_address and flow.primary_destination_port
            else ""
        )
        secondary = (
            f"{flow.secondary_destination_address}:{flow.secondary_destination_port}"
            if flow.secondary_destination_address and flow.secondary_destination_port
            else ""
        )
        rows.append(
            [
                flow.source_ipv4,
                str(flow.session_id),
                flow.flow_name,
                flow.media_title or "",
                "yes" if flow.routable else "no",
                str(flow.channel_count or ""),
                format_sample_rate_hertz(flow.sample_rate) if flow.sample_rate else "",
                format_encoding(int(flow.encoding[1:]))
                if flow.encoding and flow.encoding.startswith("L")
                else flow.encoding or "",
                f"{flow.packet_time_microseconds} us" if flow.packet_time_microseconds is not None else "",
                primary,
                secondary,
                flow.announcement_interface,
                flow.refreshed_at,
                flow.expires_at,
                "; ".join(flow.routability_errors),
            ]
        )
    output_table(
        [
            "Source",
            "Session ID",
            "Flow",
            "Media Title",
            "Routable",
            "Channels",
            "Sample Rate",
            "Encoding",
            "Packet Time",
            "Primary",
            "Secondary",
            "Interface",
            "Refreshed",
            "Expires",
            "Routability Errors",
        ],
        rows,
        json_data=application.external_flows.to_dict(),
        empty_message="No advertised AES67 audio streams found.",
    )


@app.command("external-list")
def external_flow_list(
    listen_seconds: float = typer.Option(
        2.0,
        "--listen-seconds",
        min=0.0,
        max=60.0,
        help="Seconds to wait for stream announcements before showing the results.",
    ),
):
    """Discover advertised AES67 audio streams."""
    run_command(run_external_flow_list, listen_seconds, discover_devices=False)


async def run_external_flow_subscribe(
    application,
    devices,
    source_ipv4: str,
    session_id: int,
    receiver_channel_ids: list[int],
    flow_slot_assignments: list[int],
    receiver_supports_multiple_interfaces: bool,
    listen_seconds: float,
) -> None:
    device, _ = _selected_device(devices)
    if listen_seconds:
        await asyncio.sleep(listen_seconds)
    try:
        flow = application.external_flows.get(source_ipv4, session_id)
    except ValueError as exception:
        _fail_validation(flows.FlowValidationError(str(exception)))
    if flow is None:
        _fail_validation(flows.FlowValidationError("external flow was not discovered", status=404))
    try:
        result = await application.subscribe_external_rtp(
            device,
            flow,
            receiver_channel_ids,
            flow_slot_assignments,
            receiver_supports_multiple_interfaces=receiver_supports_multiple_interfaces,
        )
    except flows.FlowValidationError as exception:
        _fail_validation(exception)
    except (NetaudioCoreError, OSError, RuntimeError, TimeoutError, ValueError) as exception:
        typer.echo(f"Error: external subscription request failed: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR) from exception
    if not result["request_acknowledged"]:
        detail = f"result 0x{result['result_code']:04X}" if result["result_code"] is not None else "no device response"
        typer.echo(f"Error: external subscription was not acknowledged ({detail}).", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    def confirmation(value, false_label="not confirmed"):
        return "confirmed" if value is True else false_label if value is False else "not confirmed"

    output_table(
        [
            "Device",
            "External Flow",
            "Receiver Channels",
            "Flow Slots",
            "Request",
            "ARC effective state",
            "SDP correlation",
            "RTP reception",
            "Clock lock",
            "Persistence",
            "Decoded audio",
        ],
        [
            [
                device.name or device.server_name,
                f"{source_ipv4}/{session_id}",
                ", ".join(map(str, receiver_channel_ids)),
                ", ".join(map(str, flow_slot_assignments)),
                "acknowledged",
                confirmation(result.get("arc_effective_state_confirmed"), "contradicted"),
                confirmation(result.get("sdp_correlation_confirmed"), "not matched"),
                confirmation(result.get("rtp_packet_reception_confirmed")),
                confirmation(result.get("clock_lock_confirmed")),
                confirmation(result.get("persistence_confirmed")),
                confirmation(result.get("decoded_audio_confirmed")),
            ]
        ],
        json_data=result,
    )


@app.command("subscribe-external")
def external_flow_subscribe(
    source_ipv4: str = typer.Option(..., "--source", help="Source IPv4 from the discovered SAP identity."),
    session_id: int = typer.Option(..., "--session-id", min=0, help="Unsigned SDP session ID."),
    receiver_channels: str = typer.Option(..., "--receiver-channels", help="Comma-separated receiver channel IDs."),
    flow_slots: str = typer.Option(
        ...,
        "--flow-slots",
        help="Parallel one-based external-flow slots; zero leaves a receiver unassigned in this batch.",
    ),
    receiver_multiple_interfaces: bool = typer.Option(
        False,
        "--receiver-multiple-interfaces",
        help="Declare that the selected receiver supports multiple network interfaces.",
    ),
    listen_seconds: float = typer.Option(
        1.0,
        "--listen-seconds",
        min=0.0,
        max=60.0,
        help="Additional time to listen for the selected SAP announcement.",
    ),
    confirmed: bool = typer.Option(False, "--yes", "-y", help="Confirm the receiver subscription request."),
):
    """Subscribe receiver channels to a discovered external RTP/AES67 flow."""
    if not confirmed:
        _fail_validation(flows.FlowValidationError("--yes is required"))
    run_command(
        run_external_flow_subscribe,
        source_ipv4,
        session_id,
        _parse_channel_numbers(receiver_channels),
        _parse_flow_slot_assignments(flow_slots),
        receiver_multiple_interfaces,
        listen_seconds,
    )


async def run_transmit_channel_capabilities(
    application, devices, starting_channel_identifier: int, maximum_channel_count: int
) -> None:
    device, arc_port = _selected_device(devices)
    capabilities = await flows.query_transmit_channel_capabilities(
        str(device.ipv4),
        arc_port,
        starting_channel_identifier,
        maximum_channel_count,
        **_managed_transport_option(device),
    )
    if capabilities is None:
        typer.echo(
            "Error: this device does not report transmitter channel capabilities.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)
    rows = [
        [
            str(index),
            str(channel_range["first_transmit_channel"]),
            str(channel_range["last_transmit_channel"]),
            f"0x{channel_range['unknown_value']:04X}",
        ]
        for index, channel_range in enumerate(capabilities["ranges"], start=1)
    ]
    output_table(
        ["Range", "First TX channel", "Last TX channel", "Unknown value"],
        rows,
        json_data=capabilities,
        empty_message="The device reports no transmitter channel capability ranges.",
    )


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
    """Show the transmitter channel capability ranges reported by a device."""
    run_command(run_transmit_channel_capabilities, starting_channel_identifier, maximum_channel_count)


async def run_flow_delete(application, devices, flow_slot: int) -> None:
    device, arc_port = _selected_device(devices)
    flow_protocol_id = await _detect_flow_protocol(application, device, arc_port)
    if flow_protocol_id is None:
        typer.echo("Error: could not detect flow protocol for this device.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    try:
        operation = getattr(application, "delete_transmit_flow", None)
        result = (
            await operation(device, flow_slot)
            if operation is not None
            else await delete_transmit_flow(device, flow_slot)
        )
    except (OSError, RuntimeError, TimeoutError, ValueError, NetaudioCoreError) as exception:
        typer.echo(f"Error: flow deletion failed: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR) from exception
    _require_successful_result(result)


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
    run_command(run_flow_delete, flow_slot)
