from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import NoReturn

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import run_with_devices
from netaudio.cli_support.output import local_time, output_single, output_table, structured_output_selected
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.commands.device.display import (
    format_encoding,
    format_latency_nanoseconds,
    format_sample_rate_hertz,
)
from netaudio.daemon.client import DaemonRefusal, ask_daemon, call_daemon

app = typer.Typer(
    help="Inspect receiver flows and manage transmitter multicast flows on the selected device.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


def _fail(message: str) -> NoReturn:
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=ExitCode.ERROR)


def _numbers(value: str, label: str, minimum: int) -> list[int]:
    try:
        numbers = [int(token) for token in value.split(",")]
    except ValueError:
        _fail(f"{label} must be a comma-separated list of integers")
    if any(number < minimum for number in numbers):
        _fail(f"{label} must be {minimum} or greater")
    return numbers


def _selected(devices):
    [(server_name, device)] = select_device(filter_devices(devices))
    return server_name, device


def _read_specification(path: str) -> dict:
    try:
        content = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
        specification = json.loads(content)
    except (OSError, UnicodeError, json.JSONDecodeError) as exception:
        _fail(f"invalid transmit-flow specification: {exception}")
    if not isinstance(specification, dict):
        _fail("invalid transmit-flow specification: expected a JSON object")
    return specification


def _destination(destination: dict | None) -> str:
    return f"{destination['address']}:{destination['port']}" if destination else ""


async def _author(path: str, body: dict) -> None:
    status, result = await ask_daemon("POST", path, body)
    if status == 200:
        output_single(result if structured_output_selected() else result["message"])
        return
    if structured_output_selected():
        output_single(result)
    raise DaemonRefusal(status, result)


def _advertised(advertised: bool | None) -> str:
    if advertised is None:
        return ""
    return "yes" if advertised else "no"


def _unparsed_row(record: dict) -> list[str]:
    flow_type_code = record.get("flow_type_code")
    channels = record.get("transmitter_channel_ids_by_slot") or record.get("channels") or []
    address = record.get("destination_internet_protocol_version_four_address")
    port = record.get("destination_user_datagram_port")
    return [
        str(record.get("global_flow_id") or record.get("flow_number") or ""),
        record.get("media_mode") or "",
        f"0x{flow_type_code:04X}" if isinstance(flow_type_code, int) else "unknown",
        _advertised(record.get("advertised")),
        record.get("flow_name") or "",
        ", ".join(f"{slot}:{channel}" for slot, channel in enumerate(channels, start=1) if channel),
        format_sample_rate_hertz(record.get("sample_rate")),
        format_encoding(record.get("encoding")),
        f"{address}:{port}" if address and port else "",
    ]


async def run_flow_inspect(devices) -> None:
    server_name, _ = _selected(devices)
    inventory = await call_daemon("GET", f"/transmit-flows/{server_name}")
    rows = [
        [
            str(flow["identity"].get("global_flow_id") or ""),
            flow["media_mode"],
            flow["flow_type"],
            _advertised(flow.get("advertised")),
            flow.get("name") or "",
            ", ".join(f"{entry['slot']}:{entry['transmitter_channel']}" for entry in flow["channel_slots"]),
            format_sample_rate_hertz(flow.get("sample_rate_hz")),
            format_encoding(flow.get("encoding_bits")),
            _destination(flow.get("primary_destination")) or "device allocated",
        ]
        for flow in inventory["flows"]
    ]
    rows.extend(_unparsed_row(record["raw_record"]) for record in inventory["unparsed_records"])
    output_table(
        ["Flow", "Media", "Type", "Advertised", "Name", "Slot:Channel", "Sample Rate", "Encoding", "Destination"],
        rows,
        json_data=inventory,
        empty_message="No transmitter flows are active.",
    )


@app.command("inspect")
def flow_inspect():
    """List the transmitter flows on a device."""
    run_with_devices(run_flow_inspect)


async def run_flow_plan(devices, specification: dict) -> None:
    server_name, _ = _selected(devices)
    plan = await call_daemon("POST", "/transmit-flows/plan", {"device": server_name, "specification": specification})
    output_single(plan if structured_output_selected() else json.dumps(plan, indent=2, sort_keys=True))


@app.command("plan")
def flow_plan(
    specification_file: str = typer.Argument(..., help="Flow JSON file, or - for standard input."),
):
    """Plan a transmit flow without sending it."""
    run_with_devices(run_flow_plan, _read_specification(specification_file))


async def run_flow_apply(devices, specification: dict) -> None:
    server_name, _ = _selected(devices)
    await _author(
        "/transmit-flows/create",
        {"confirmed": True, "device": server_name, "specification": specification},
    )


@app.command("apply")
def flow_apply(
    specification_file: str = typer.Argument(..., help="Flow JSON file, or - for standard input."),
    confirmed: bool = typer.Option(False, "--yes", "-y", help="Confirm transmit-flow creation."),
):
    """Create a transmit flow and read it back."""
    if not confirmed:
        _fail("--yes is required")
    run_with_devices(run_flow_apply, _read_specification(specification_file))


async def run_flow_delete(devices, flow_slot: int) -> None:
    server_name, _ = _selected(devices)
    await _author("/transmit-flows/delete", {"confirmed": True, "device": server_name, "flow_id": flow_slot})


@app.command("delete")
def flow_delete(
    slot: int = typer.Option(..., "--slot", min=1, max=65534, help="Flow slot number to delete."),
    confirmed: bool = typer.Option(False, "--yes", "-y", help="Confirm deletion of the multicast flow."),
):
    """Delete a transmitter multicast flow."""
    if not confirmed:
        _fail("--yes is required")
    run_with_devices(run_flow_delete, slot)


async def run_receiver_flow_list(devices) -> None:
    _, device = _selected(devices)
    record = device.record
    receiver_flows = record.get("receiver_flows")
    if receiver_flows is None:
        _fail(record.get("receiver_flow_read_error") or "receiver flows have not been read from this device")
    rows = []
    for flow in receiver_flows:
        channel_lists = flow.get("receiver_channel_numbers_by_flow_channel") or []
        channel_numbers = {number for channel_numbers in channel_lists for number in channel_numbers}
        routes = [
            route for route in record.get("subscriptions") or [] if route.get("rx_channel_number") in channel_numbers
        ]
        external_identity = flow.get("external_identity")
        rows.append(
            [
                str(flow["flow_number"]),
                flow.get("flow_type") or "",
                " / ".join("+".join(map(str, channel_numbers)) or "-" for channel_numbers in channel_lists),
                ", ".join(sorted({route["tx_device"] for route in routes if route.get("tx_device")})),
                ", ".join(sorted({route["status"]["label"] for route in routes if route.get("status")})),
                ", ".join(
                    f"{endpoint.get('ipv4_address')}:{endpoint.get('udp_port')}"
                    for endpoint in flow.get("interface_endpoints") or []
                ),
                (
                    f"{external_identity.get('source_ipv4')}/{external_identity.get('session_id')}"
                    if external_identity
                    else ""
                ),
                format_sample_rate_hertz(flow.get("sample_rate")),
                format_encoding(flow.get("encoding")),
                format_latency_nanoseconds(flow.get("latency_nanoseconds")),
            ]
        )
    maximum = record.get("maximum_receive_flows")
    output_table(
        [
            "Slot",
            "Type",
            "Receiver Channels",
            "From",
            "Status",
            "Destinations",
            "External Flow",
            "Sample Rate",
            "Encoding",
            "Latency",
        ],
        rows,
        json_data=receiver_flows,
        omit_empty_columns=True,
        empty_message=(
            f"No receiver flows configured (0 of at most {maximum})."
            if maximum is not None
            else "No receiver flows configured."
        ),
    )


@app.command("receiver-list")
def receiver_flow_list():
    """List receiver flows and their local channel mappings."""
    run_with_devices(run_receiver_flow_list)


async def run_external_flow_list() -> None:
    flows = await call_daemon("GET", "/external-flows")
    rows = [
        [
            flow["source_ipv4"],
            str(flow["session_id"]),
            flow.get("flow_name") or "",
            flow.get("media_title") or "",
            "yes" if flow.get("routable") else "no",
            str(flow.get("channel_count") or ""),
            format_sample_rate_hertz(flow.get("sample_rate")) if flow.get("sample_rate") else "",
            flow.get("encoding") or "",
            f"{flow['packet_time_microseconds']} us" if flow.get("packet_time_microseconds") is not None else "",
            _destination(flow.get("primary_destination")),
            _destination(flow.get("secondary_destination")),
            flow.get("announcement_interface") or "",
            local_time(flow.get("refreshed_at")),
            local_time(flow.get("expires_at")),
            "; ".join(flow.get("routability_errors") or []),
        ]
        for flow in flows.values()
    ]
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
        json_data=flows,
        omit_empty_columns=True,
        empty_message="No advertised AES67 audio streams found.",
    )


@app.command("external-list")
def external_flow_list():
    """List advertised AES67 audio streams."""
    asyncio.run(run_external_flow_list())


async def run_external_flow_subscribe(
    devices,
    source_ipv4: str,
    session_id: int,
    receiver_channel_ids: list[int],
    flow_slot_assignments: list[int],
) -> None:
    server_name, device = _selected(devices)
    result = await call_daemon(
        "POST",
        "/external-flows/subscribe",
        {
            "flow_slot_assignments": flow_slot_assignments,
            "receiver_channel_ids": receiver_channel_ids,
            "rx_device": server_name,
            "session_id": session_id,
            "source_ipv4": source_ipv4,
        },
    )

    def confirmation(key: str, refuted: str = "not confirmed") -> str:
        return {True: "confirmed", False: refuted}.get(result.get(key), "not confirmed")

    output_table(
        [
            "Device",
            "External Flow",
            "Receiver Channels",
            "Flow Slots",
            "Receiver State",
            "SDP",
            "RTP Reception",
            "Clock Lock",
            "Persistence",
            "Decoded Audio",
        ],
        [
            [
                device.name or server_name,
                f"{source_ipv4}/{session_id}",
                ", ".join(map(str, receiver_channel_ids)),
                ", ".join(map(str, flow_slot_assignments)),
                confirmation("arc_effective_state_confirmed", "contradicted"),
                confirmation("sdp_correlation_confirmed", "not matched"),
                confirmation("rtp_packet_reception_confirmed"),
                confirmation("clock_lock_confirmed"),
                confirmation("persistence_confirmed"),
                confirmation("decoded_audio_confirmed"),
            ]
        ],
        json_data=result,
    )


@app.command("subscribe-external")
def external_flow_subscribe(
    source_ipv4: str = typer.Option(..., "--source", help="Source IPv4 of the advertised stream."),
    session_id: int = typer.Option(..., "--session-id", min=0, help="Session ID of the advertised stream."),
    receiver_channels: str = typer.Option(..., "--receiver-channels", help="Comma-separated receiver channel IDs."),
    flow_slots: str = typer.Option(
        ...,
        "--flow-slots",
        help="One stream slot per receiver channel, starting at 1; 0 leaves that receiver channel unassigned.",
    ),
    confirmed: bool = typer.Option(False, "--yes", "-y", help="Confirm the receiver subscription request."),
):
    """Subscribe receiver channels to an advertised AES67 stream."""
    if not confirmed:
        _fail("--yes is required")
    run_with_devices(
        run_external_flow_subscribe,
        source_ipv4,
        session_id,
        _numbers(receiver_channels, "receiver channels", 1),
        _numbers(flow_slots, "flow slots", 0),
    )
