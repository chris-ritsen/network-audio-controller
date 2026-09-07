from __future__ import annotations

from typing import Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.execution import run_command
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import filter_devices, select_device, sort_devices
from netaudio.dante.network_configuration import (
    interface_configuration,
    network_snapshot,
    validate_interface_configuration,
)

INTERFACE_HEADERS = ["Name", "Interface", "State", "Mode", "IP Address", "Netmask", "Gateway", "DNS", "Reboot Required"]


def _interface_rows(server_name, device) -> list[list[str]]:
    rows = []
    for entry in device.interfaces or []:
        for state, configuration in (("Active", entry), ("Configured", entry.get("configured"))):
            if configuration is None:
                continue
            rows.append(
                [
                    device.name or server_name,
                    (entry.get("interface") or "unknown").capitalize(),
                    state,
                    {"dynamic": "DHCP", "static": "Static"}.get(configuration.get("mode"), "Unknown"),
                    *[configuration.get(key) or "" for key in ("ip_address", "netmask", "gateway", "dns_server")],
                    "yes" if entry.get("reboot_required") else "no",
                ]
            )
    return rows


async def run_interface(application, devices, mode, static_configuration, all_devices, interface="primary") -> None:
    filtered = filter_devices(devices)
    targets = sort_devices(filtered) if mode is None else select_device(filtered, allow_many=all_devices)
    rows, json_data, failures = [], {}, 0
    for server_name, device in targets:
        try:
            if mode is None:
                device.interfaces = await application.probe_interface_status(device)
            else:
                device.interfaces = await application.set_interface(
                    device, mode, static_configuration, interface=interface
                )
                expected = validate_interface_configuration(mode, static_configuration)
                configured = interface_configuration(device.interfaces, interface).get("configured") or {}
                if not all(configured.get(key) == value for key, value in expected.items()):
                    raise RuntimeError("Interface change could not be verified; no reboot sent")
                typer.echo(
                    f"Configured {interface} interface on {device.name or server_name} (verified). No reboot sent.",
                    err=True,
                )
            rows.extend(_interface_rows(server_name, device))
            json_data[server_name] = {"name": device.name, **network_snapshot(device)}
        except (ValueError, RuntimeError, OSError, TimeoutError) as exception:
            failures += 1
            typer.echo(f"Error: {device.name or server_name}: {exception}", err=True)
    output_table(INTERFACE_HEADERS, rows, json_data=json_data)
    if failures:
        raise typer.Exit(code=ExitCode.ERROR)


def interface(
    mode: Optional[str] = typer.Argument(None, help="dhcp or static"),
    ip_address: Optional[str] = typer.Option(None, "--ip", help="IP address (static only)."),
    netmask: Optional[str] = typer.Option(None, "--netmask", help="Subnet mask (static only)."),
    dns_server: Optional[str] = typer.Option(None, "--dns", help="DNS server (static only)."),
    gateway: Optional[str] = typer.Option(None, "--gateway", help="Gateway (static only)."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
    interface: str = typer.Option("primary", "--interface", help="Target primary or secondary interface."),
):
    """Read active/configured network settings or set and verify an interface."""
    configuration = {"ip_address": ip_address, "netmask": netmask, "dns_server": dns_server, "gateway": gateway}
    try:
        if interface not in {"primary", "secondary"}:
            raise ValueError("interface must be primary or secondary")
        if mode is not None:
            validate_interface_configuration(mode, configuration)
    except ValueError as exception:
        typer.echo(f"Error: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR) from None
    run_command(run_interface, mode, configuration if mode == "static" else None, all_devices, interface)
