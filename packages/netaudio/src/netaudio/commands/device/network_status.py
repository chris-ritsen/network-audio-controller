from __future__ import annotations

import asyncio
import time

import typer

from netaudio.cli_support.context import _get_state
from netaudio.cli_support.execution import CapabilityProbeTimeout, run_command
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import filter_devices, sort_devices
from netaudio.commands.config.redundancy import mode_label
from netaudio.commands.device.display import format_link_speed_megabits_per_second
from netaudio.ddm.controller import DAPISessionError
from netaudio.dante.network_configuration import (
    advertised_redundancy_support,
    probe_switch_configuration_if_reported,
    redundancy_snapshot,
)

NETWORK_STATUS_HEADERS = [
    "Name",
    "IP Address",
    "Interface",
    "Transmit",
    "Receive",
    "Tx Errors",
    "Rx Errors",
    "Speed",
    "Fresh",
    "Switch Mode",
    "Available Switch Modes",
]

NETWORK_STATUS_DISSECT_HEADERS = [
    "Selected",
    "Group Pointer",
    "Record",
    "Discriminator",
    "Size",
    "Pointer",
    "Extension",
    "Record Pointers",
    "Transport",
    "Received At",
    "Packet Source",
    "Switch Mode Codes",
    "Raw Record",
]


def _format_bit_rate(bits_per_second: int | None) -> str:
    if bits_per_second is None:
        return ""
    if bits_per_second >= 1_000_000:
        return f"{bits_per_second / 1_000_000:g} Mbps"
    if bits_per_second >= 1_000:
        return f"{bits_per_second / 1_000:g} kbps"
    return f"{bits_per_second} bps"


def _switch_mode_summary(switch_configuration: dict | None, dissect: bool) -> tuple[str, str, str]:
    if switch_configuration is None:
        return "", "", ""
    choices = {choice["code"]: choice["label"] for choice in switch_configuration["choices"]}
    mode_codes = switch_configuration["mode_codes_at_record_offsets_20_and_22"]
    selected_labels = [choices.get(code, f"unknown 0x{code:04X}") for code in mode_codes]
    switch_mode = selected_labels[0] if len(set(mode_codes)) == 1 else " / ".join(selected_labels)
    switch_mode_codes = " ".join(f"0x{code:04X}" for code in mode_codes)
    if dissect:
        available_switch_modes = ", ".join(f"0x{code:04X} {label}" for code, label in choices.items())
    else:
        available_switch_modes = ", ".join(choices.values())
    return switch_mode, switch_mode_codes, available_switch_modes


def _dissect_cells(observation, group, record_index: int, record, switch_mode_codes: str) -> list[str]:
    return [
        "yes"
        if group.selected_stats is not None and record.record_pointer == group.selected_stats.record_pointer
        else "no",
        f"0x{group.group_pointer:04X}",
        str(record_index),
        f"0x{record.discriminator_status_word:08X}",
        str(record.record_size_bytes),
        f"0x{record.record_pointer:04X}",
        record.extension_hexadecimal,
        " ".join(f"0x{pointer:04X}" for pointer in group.record_pointers),
        observation.transport_source,
        observation.received_at,
        observation.packet_source,
        switch_mode_codes,
        record.raw_record_hexadecimal,
    ]


def network_status_rows(
    device_name: str,
    address: str,
    interface_statistics,
    switch_configuration: dict | None,
    dissect: bool,
    switch_configuration_applicable: bool = True,
    redundancy: dict | None = None,
) -> list[list[str]]:
    switch_mode, switch_mode_codes, available_switch_modes = _switch_mode_summary(switch_configuration, dissect)
    if switch_configuration is None and address:
        if redundancy and redundancy.get("current_mode"):
            switch_mode = mode_label(redundancy["current_mode"])
            available_switch_modes = ", ".join(
                mode_label(choice["mode"])
                for choice in redundancy.get("available_modes") or []
                if isinstance(choice, dict) and choice.get("mode") is not None
            )
        else:
            switch_mode = "not reported" if switch_configuration_applicable else "N/A"
    if interface_statistics is None:
        response_label = "no response" if address else ""
        row = [device_name, address, "", response_label, "", "", "", "", "", switch_mode, available_switch_modes]
        if dissect:
            row.extend(["", "", "", "", "", "", "", "", "", "", "", switch_mode_codes, ""])
        return [row]
    rows = []
    observation = interface_statistics.at(time.monotonic())
    for group_index, group in enumerate(observation.interface_groups):
        displayed_records = group.raw_records if dissect else (group.selected_stats,)
        if not displayed_records:
            displayed_records = (None,)
        for record_index, record in enumerate(displayed_records):
            first_record = group_index == 0 and record_index == 0
            row = [
                device_name,
                address,
                str(group.group_index + 1),
                _format_bit_rate(record.transmit_bits_per_second) if record is not None else "not selected",
                _format_bit_rate(record.receive_bits_per_second) if record is not None else "",
                str(record.transmit_errors_since_local_reset)
                if record and record.transmit_errors_since_local_reset is not None
                else "",
                str(record.receive_errors_since_local_reset)
                if record and record.receive_errors_since_local_reset is not None
                else "",
                format_link_speed_megabits_per_second(record.speed_megabits_per_second) if record is not None else "",
                "yes" if observation.fresh else "no",
                switch_mode if first_record else "",
                available_switch_modes if first_record else "",
            ]
            if dissect:
                if record is None:
                    row.extend(
                        [
                            "",
                            f"0x{group.group_pointer:04X}",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            observation.transport_source,
                            observation.received_at,
                            observation.packet_source,
                            switch_mode_codes if first_record else "",
                            "",
                        ]
                    )
                else:
                    row.extend(
                        _dissect_cells(
                            observation,
                            group,
                            record_index,
                            record,
                            switch_mode_codes if first_record else "",
                        )
                    )
            rows.append(row)
    return rows


def _should_probe_switch_configuration(device) -> bool:
    return advertised_redundancy_support(device) is not False


async def run_network_status(application, devices, timeout: float) -> None:
    dissect = _get_state().dissect
    filtered = filter_devices(devices)
    if not filtered:
        typer.echo("Error: no devices matched.", err=True)
        raise typer.Exit(code=1)

    async def probe(server_name, device):
        if not device.requires_managed_control and device.ipv4 is None:
            return server_name, device, None, None, True

        async def capture(operation):
            try:
                return await operation(device, timeout=timeout)
            except (CapabilityProbeTimeout, DAPISessionError, RuntimeError, OSError):
                return None

        async def capture_switch_configuration():
            try:
                return await probe_switch_configuration_if_reported(application, device, timeout)
            except (CapabilityProbeTimeout, DAPISessionError, RuntimeError, OSError):
                return None

        switch_configuration_applicable = _should_probe_switch_configuration(device)
        if switch_configuration_applicable:
            interface_statistics, switch_configuration = await asyncio.gather(
                capture(application.probe_interface_statistics),
                capture_switch_configuration(),
            )
        else:
            interface_statistics = await capture(application.probe_interface_statistics)
            switch_configuration = None
        return server_name, device, interface_statistics, switch_configuration, switch_configuration_applicable

    results = await asyncio.gather(*(probe(server_name, device) for server_name, device in sort_devices(filtered)))
    headers = NETWORK_STATUS_HEADERS + NETWORK_STATUS_DISSECT_HEADERS if dissect else NETWORK_STATUS_HEADERS
    rows = []
    json_data = {}
    for server_name, device, interface_statistics, switch_configuration, switch_configuration_applicable in results:
        device_name = device.name or server_name
        address = str(device.ipv4) if device.ipv4 is not None else None
        redundancy = redundancy_snapshot(device)
        json_data[server_name] = {
            "available": interface_statistics is not None or switch_configuration is not None,
            "dante_model": device.dante_model,
            "ipv4": address,
            "kind": device.kind,
            "interface_statistics": interface_statistics.to_dict() if interface_statistics is not None else None,
            "interface_statistics_available": interface_statistics is not None,
            "manufacturer": device.manufacturer,
            "name": device.name,
            "server_name": server_name,
            "switch_configuration": switch_configuration,
            "switch_configuration_applicable": switch_configuration_applicable,
            "switch_configuration_available": switch_configuration is not None,
            "redundancy": redundancy,
        }
        rows.extend(
            network_status_rows(
                device_name,
                address or "",
                interface_statistics,
                switch_configuration,
                dissect,
                switch_configuration_applicable,
                redundancy,
            )
        )

    output_table(headers, rows, json_data=json_data)


def network_status(
    timeout: float = typer.Option(
        2.0,
        "--timeout",
        min=0.1,
        help="Per-probe network-status response timeout in seconds.",
    ),
):
    """Probe interface statistics and switch-configuration status."""
    run_command(run_network_status, timeout)
