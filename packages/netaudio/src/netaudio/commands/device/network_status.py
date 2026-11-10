from __future__ import annotations

from netaudio.cli_support.execution import run_with_devices
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.commands.device.display import format_link_speed_megabits_per_second

HEADERS = [
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
MODE_LABELS = {"switched": "Switched", "redundant": "Redundant", "split_redundant": "Split/Redundant"}


def _bit_rate(bits_per_second: int | None) -> str:
    if bits_per_second is None:
        return ""
    if bits_per_second >= 1_000_000:
        return f"{bits_per_second / 1_000_000:g} Mbps"
    if bits_per_second >= 1_000:
        return f"{bits_per_second / 1_000:g} kbps"
    return f"{bits_per_second} bps"


def _switch_modes(redundancy: dict | None) -> tuple[str, str]:
    if not redundancy or not redundancy.get("current_mode"):
        return "not reported", ""
    current = MODE_LABELS.get(redundancy["current_mode"], "Unknown")
    configured = MODE_LABELS.get(redundancy.get("configured_mode"), "Unknown")
    available = ", ".join(
        MODE_LABELS.get(choice.get("mode"), "Unknown") for choice in redundancy.get("available_modes") or []
    )
    return (current if current == configured else f"{current} / {configured}"), available


def _rows(label: str, record: dict) -> list[list[str]]:
    address = record.get("ipv4") or ""
    switch_mode, available = _switch_modes(record.get("network_redundancy"))
    traffic = record.get("network_interface_traffic")
    if not traffic:
        return [[label, address, "", "not reported", "", "", "", "", "", switch_mode, available]]
    speed = format_link_speed_megabits_per_second(record.get("link_speed_mbps"))
    return [
        [
            label,
            address,
            str(entry["entry_index"] + 1),
            _bit_rate(entry.get("transmit_rate_bits_per_second")),
            _bit_rate(entry.get("receive_rate_bits_per_second")),
            str(entry.get("transmit_error_count", "")),
            str(entry.get("receive_error_count", "")),
            speed if index == 0 else "",
            "yes" if traffic.get("fresh") else "no",
            switch_mode if index == 0 else "",
            available if index == 0 else "",
        ]
        for index, entry in enumerate(traffic.get("interfaces") or [])
    ]


async def run_network_status(devices) -> None:
    targets = select_device(filter_devices(devices), allow_many=True)
    rows = []
    for server_name, device in targets:
        rows.extend(_rows(device.name or server_name, device.record))
    output_table(
        HEADERS,
        rows,
        json_data={
            server_name: {
                "name": device.name,
                "ipv4": device.record.get("ipv4"),
                "interface_statistics": device.record.get("network_interface_traffic"),
                "link_speed_mbps": device.record.get("link_speed_mbps"),
                "redundancy": device.record.get("network_redundancy"),
            }
            for server_name, device in targets
        },
    )


def network_status():
    """Show interface traffic, errors and switch mode."""
    run_with_devices(run_network_status)
