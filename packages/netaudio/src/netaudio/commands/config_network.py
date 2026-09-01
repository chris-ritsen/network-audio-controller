from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio._common import _command_context
from netaudio._common_output import output_table
from netaudio._common_selection import filter_devices, sort_devices
from netaudio._exit_codes import ExitCode
from netaudio.commands.config_readback import _resolve_targets, _send_requested_change
from netaudio.dante.device_commands import DanteDeviceCommands


def interface(
    mode: Optional[str] = typer.Argument(None, help="dhcp or static"),
    ip_address: Optional[str] = typer.Option(None, "--ip", help="IP address (static only)."),
    netmask: Optional[str] = typer.Option(None, "--netmask", help="Subnet mask (static only)."),
    dns_server: Optional[str] = typer.Option(None, "--dns", help="DNS server (static only)."),
    gateway: Optional[str] = typer.Option(None, "--gateway", help="Gateway (static only)."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set interface configuration."""

    commands = DanteDeviceCommands()

    async def _run():
        if mode is None:
            from netaudio.daemon.client import get_devices_from_daemon

            devices = await get_devices_from_daemon()

            if devices is not None:
                devices = filter_devices(devices)
            else:
                from netaudio.common.app_config import settings
                from netaudio.dante.application import DanteApplication

                application = DanteApplication()
                await application.startup()
                try:
                    devices = await application.discover_and_populate(timeout=settings.mdns_timeout)
                    devices = filter_devices(devices or {})
                finally:
                    await application.shutdown()

            headers = ["Name", "Interface", "Mode", "IP Address", "Netmask", "Gateway", "DNS", "Pending"]
            rows = []
            json_data = {}

            for server_name, device in sort_devices(devices):
                interfaces = device.interfaces
                pending_config = device.interface_pending_config
                pending_label = ""
                if pending_config:
                    pending_mode = pending_config.get("mode", "")
                    if pending_mode == "static":
                        pending_label = f"static {pending_config.get('ip_address', '')}"
                    else:
                        pending_label = pending_mode

                if not interfaces:
                    rows.append(
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
                    )
                    continue

                for index, interface_state in enumerate(interfaces):
                    rows.append(
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
                    )

                device_json = {
                    "name": device.name,
                    "interfaces": interfaces,
                }
                if pending_config:
                    device_json["pending_config"] = pending_config
                json_data[server_name] = device_json

            output_table(headers, rows, json_data=json_data)
            return

        if mode not in ("dhcp", "static"):
            typer.echo("Error: mode must be 'dhcp' or 'static'.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        if mode == "static" and not all([ip_address, netmask, dns_server, gateway]):
            typer.echo("Error: --ip, --netmask, --dns, and --gateway are required for static mode.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if mode == "dhcp":
                packet, _, port = commands.command_set_interface_dhcp()
            else:
                packet, _, port = commands.command_set_interface_static(ip_address, netmask, dns_server, gateway)
            failures = await _send_requested_change(
                targets,
                lambda device: send(
                    packet,
                    str(device.ipv4),
                    port,
                    expect_response=False,
                ),
                "interface change",
                lambda label: f"Interface change requested for {label}: {mode}; not verified.",
            )

            typer.echo("Reboot required for changes to take effect.", err=True)
            if failures:
                raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_run())
