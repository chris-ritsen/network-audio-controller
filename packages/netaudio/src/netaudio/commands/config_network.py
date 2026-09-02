from __future__ import annotations

from typing import Optional

import typer

from netaudio._common import run_command
from netaudio._common_output import output_table
from netaudio._common_selection import filter_devices, select_device, sort_devices
from netaudio._exit_codes import ExitCode
from netaudio.commands.config_readback import _send_requested_change

INTERFACE_HEADERS = ["Name", "Interface", "Mode", "IP Address", "Netmask", "Gateway", "DNS", "Pending"]


def _pending_label(pending_config) -> str:
    if not pending_config:
        return ""
    pending_mode = pending_config.get("mode", "")
    if pending_mode == "static":
        return f"static {pending_config.get('ip_address', '')}"
    return pending_mode


def _interface_rows(server_name, device) -> list[list[str]]:
    pending_label = _pending_label(device.interface_pending_config)
    if not device.interfaces:
        return [
            [
                device.name or server_name,
                "0",
                "",
                str(device.ipv4) if device.ipv4 else "",
                "",
                "",
                "",
                pending_label,
            ]
        ]
    return [
        [
            device.name or server_name,
            str(index),
            interface_state.get("mode", ""),
            interface_state.get("ip_address", ""),
            interface_state.get("netmask", ""),
            interface_state.get("gateway", ""),
            interface_state.get("dns_server", ""),
            pending_label if index == 0 else "",
        ]
        for index, interface_state in enumerate(device.interfaces)
    ]


async def run_interface(
    application,
    devices,
    mode: str | None,
    static_configuration: dict | None,
    all_devices: bool,
) -> None:
    filtered = filter_devices(devices)
    if mode is None:
        rows = []
        json_data = {}
        for server_name, device in sort_devices(filtered):
            rows.extend(_interface_rows(server_name, device))
            device_json = {"name": device.name, "interfaces": device.interfaces}
            if device.interface_pending_config:
                device_json["pending_config"] = device.interface_pending_config
            json_data[server_name] = device_json
        output_table(INTERFACE_HEADERS, rows, json_data=json_data)
        return

    targets = select_device(filtered, allow_many=all_devices)
    failures = await _send_requested_change(
        targets,
        lambda device: application.set_interface(device, mode, static_configuration),
        "interface change",
        lambda label: f"Interface change requested for {label}: {mode}; not verified.",
    )

    typer.echo("Reboot required for changes to take effect.", err=True)
    if failures:
        raise typer.Exit(code=ExitCode.ERROR)


def interface(
    mode: Optional[str] = typer.Argument(None, help="dhcp or static"),
    ip_address: Optional[str] = typer.Option(None, "--ip", help="IP address (static only)."),
    netmask: Optional[str] = typer.Option(None, "--netmask", help="Subnet mask (static only)."),
    dns_server: Optional[str] = typer.Option(None, "--dns", help="DNS server (static only)."),
    gateway: Optional[str] = typer.Option(None, "--gateway", help="Gateway (static only)."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set interface configuration."""
    if mode is not None and mode not in ("dhcp", "static"):
        typer.echo("Error: mode must be 'dhcp' or 'static'.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    static_configuration = None
    if mode == "static":
        if not all([ip_address, netmask, dns_server, gateway]):
            typer.echo("Error: --ip, --netmask, --dns, and --gateway are required for static mode.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)
        static_configuration = {
            "dns_server": dns_server,
            "gateway": gateway,
            "ip_address": ip_address,
            "netmask": netmask,
        }

    run_command(run_interface, mode, static_configuration, all_devices)
