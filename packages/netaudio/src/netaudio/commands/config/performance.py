from __future__ import annotations

import asyncio

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.commands.config.readback import MUTATION_ERRORS


async def _run_operation(application, devices, operation_name: str, arguments: tuple, all_devices: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)
    operation = getattr(application, operation_name)

    async def apply(server_name, device):
        try:
            result = await operation(device, *arguments)
            return server_name, device, result, None
        except MUTATION_ERRORS as exception:
            return server_name, device, None, exception

    outcomes = await asyncio.gather(*(apply(server_name, device) for server_name, device in targets))
    rows = []
    structured = {}
    failures = 0
    for server_name, device, result, exception in outcomes:
        label = device.name or server_name
        if exception is not None:
            failures += 1
            rows.append([label, "Failed", str(exception)])
            structured[server_name] = {"success": False, "error": str(exception)}
            continue
        payload = result.to_dict()
        structured[server_name] = payload
        confirmation = payload.get("effective_state_confirmation")
        persistence_ack = payload.get("persistence_request_acknowledgement")
        if confirmation is True:
            result_label = "Verified"
        elif persistence_ack and persistence_ack.get("accepted") is True:
            result_label = "Acknowledged"
        else:
            result_label = payload["state"].replace("_", " ").title()
        rows.append([label, result_label, payload["message"]])
        if payload["state"] in {"rejected", "contradicted", "unverified"}:
            failures += 1
    output_table(["Device", "Result", "Detail"], rows, json_data=structured)
    if failures:
        raise typer.Exit(code=ExitCode.ERROR)


async def run_receive_flow_performance(application, devices, latency: int, frames: int, all_devices: bool):
    await _run_operation(
        application,
        devices,
        "set_receive_flow_performance",
        (latency, frames),
        all_devices,
    )


async def run_transmit_flow_performance(application, devices, latency: int, frames: int, all_devices: bool):
    await _run_operation(
        application,
        devices,
        "set_transmit_flow_performance",
        (latency, frames),
        all_devices,
    )


async def run_unicast_performance(application, devices, latency: int, frames: int, all_devices: bool):
    await _run_operation(
        application,
        devices,
        "set_unicast_performance",
        (latency, frames),
        all_devices,
    )


async def run_receive_flow_default_slots(application, devices, slots: int, all_devices: bool):
    await _run_operation(
        application,
        devices,
        "set_receive_flow_default_slots",
        (slots,),
        all_devices,
    )


async def run_store_current_configuration(application, devices, all_devices: bool):
    await _run_operation(application, devices, "store_current_configuration", (), all_devices)
