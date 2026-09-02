from __future__ import annotations

import asyncio

import typer

from netaudio.cli_support.context import _get_state
from netaudio.cli_support.execution import CapabilityProbeTimeout, run_command
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import filter_devices, sort_devices
from netaudio.commands.device.display import format_link_speed_megabits_per_second

NETWORK_STATUS_HEADERS = [
    "Name",
    "IP Address",
    "Port",
    "Link",
    "Speed",
    "Switch Mode",
    "Available Switch Modes",
]

NETWORK_STATUS_DISSECT_HEADERS = [
    "Record",
    "Label",
    "Status Word",
    "Size",
    "Pointer",
    "Prefix Words",
    "Trailing Bytes",
    "Switch Mode Codes",
    "Raw Record",
]


def _format_port_label(label: str | None) -> str:
    if not label:
        return ""
    return label.replace("_", " ")


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


def _dissect_cells(record, switch_mode_codes: str) -> list[str]:
    return [
        str(record.record_index),
        record.label or "",
        f"0x{record.raw_link_status_word:08X}",
        str(record.record_size_bytes),
        f"0x{record.record_pointer:04X}",
        " ".join(f"0x{word:08X}" for word in record.unmapped_prefix_words),
        record.unmapped_trailing_hexadecimal,
        switch_mode_codes,
        record.raw_record_hexadecimal,
    ]


def network_status_rows(
    device_name: str,
    address: str,
    link_status,
    switch_configuration: dict | None,
    dissect: bool,
) -> list[list[str]]:
    switch_mode, switch_mode_codes, available_switch_modes = _switch_mode_summary(switch_configuration, dissect)
    if switch_configuration is None and address:
        switch_mode = "no response"
    if link_status is None:
        link_label = "no response" if address else ""
        row = [device_name, address, "", link_label, "", switch_mode, available_switch_modes]
        if dissect:
            row.extend(["", "", "", "", "", "", "", switch_mode_codes, ""])
        return [row]
    rows = []
    for record_index, record in enumerate(link_status.records):
        first_record = record_index == 0
        row = [
            device_name,
            address,
            _format_port_label(record.label),
            "up" if record.link_up else "down",
            format_link_speed_megabits_per_second(record.link_speed_megabits_per_second),
            switch_mode if first_record else "",
            available_switch_modes if first_record else "",
        ]
        if dissect:
            row.extend(_dissect_cells(record, switch_mode_codes if first_record else ""))
        rows.append(row)
    return rows


def _without_port_column(headers: list[str], rows: list[list[str]]) -> tuple[list[str], list[list[str]]]:
    port_index = headers.index("Port")
    if any(row[port_index] for row in rows):
        return headers, rows
    return (
        [header for index, header in enumerate(headers) if index != port_index],
        [[cell for index, cell in enumerate(row) if index != port_index] for row in rows],
    )


async def run_network_status(application, devices, timeout: float) -> None:
    dissect = _get_state().dissect
    filtered = filter_devices(devices)
    if not filtered:
        typer.echo("Error: no devices matched.", err=True)
        raise typer.Exit(code=1)

    async def probe(server_name, device):
        if device.ipv4 is None:
            return server_name, device, None, None

        async def capture(operation):
            try:
                return await operation(str(device.ipv4), timeout=timeout)
            except CapabilityProbeTimeout:
                return None

        link_status, switch_configuration = await asyncio.gather(
            capture(application.probe_link_status),
            capture(application.probe_switch_configuration),
        )
        return server_name, device, link_status, switch_configuration

    results = await asyncio.gather(*(probe(server_name, device) for server_name, device in sort_devices(filtered)))
    headers = NETWORK_STATUS_HEADERS + NETWORK_STATUS_DISSECT_HEADERS if dissect else NETWORK_STATUS_HEADERS
    rows = []
    json_data = {}
    for server_name, device, link_status, switch_configuration in results:
        device_name = device.name or server_name
        address = str(device.ipv4) if device.ipv4 is not None else None
        json_data[server_name] = {
            "available": link_status is not None or switch_configuration is not None,
            "dante_model": device.dante_model,
            "ipv4": address,
            "kind": device.kind,
            "link_status": link_status.to_dict() if link_status is not None else None,
            "link_status_available": link_status is not None,
            "manufacturer": device.manufacturer,
            "name": device.name,
            "server_name": server_name,
            "switch_configuration": switch_configuration,
            "switch_configuration_available": switch_configuration is not None,
        }
        rows.extend(network_status_rows(device_name, address or "", link_status, switch_configuration, dissect))

    headers, rows = _without_port_column(headers, rows)
    output_table(headers, rows, json_data=json_data)


def network_status(
    timeout: float = typer.Option(
        2.0,
        "--timeout",
        min=0.1,
        help="Per-probe network-status response timeout in seconds.",
    ),
):
    """Probe Controller-compatible link and switch-configuration status."""
    run_command(run_network_status, timeout)
