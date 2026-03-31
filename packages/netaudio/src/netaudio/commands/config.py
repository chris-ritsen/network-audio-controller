from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from typing import Optional

import typer

from netaudio.dante.device_commands import DanteDeviceCommands

from netaudio._common import (
    _command_context,
    _discover,
    _get_arc_port,
    _populate_controls,
    filter_devices,
    output_single,
    output_table,
    sort_devices,
)
from netaudio._exit_codes import ExitCode

app = typer.Typer(help="Get or set device configuration.", no_args_is_help=True)

top_app = typer.Typer(help="Manage netaudio configuration.", no_args_is_help=True)

VALID_SAMPLE_RATES = [44100, 48000, 88200, 96000, 176400, 192000]
VALID_ENCODINGS = [16, 24, 32]

MOVED_COMMANDS = ["sample-rate", "encoding", "latency", "aes67"]


def _moved_command(name: str):
    def handler(ctx: typer.Context):
        typer.echo(f"This command has moved. Use: netaudio device config {name}", err=True)
        raise typer.Exit(code=1)
    return handler


for _name in MOVED_COMMANDS:
    top_app.command(_name, hidden=True, context_settings={"allow_extra_args": True, "allow_interspersed_args": False})(_moved_command(_name))


@top_app.command("edit")
def config_edit():
    """Open config.toml in $EDITOR."""
    from netaudio.common.config_loader import default_config_path

    config_path = default_config_path()

    if not config_path.exists():
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("")

    editor = os.environ.get("EDITOR") or os.environ.get("VISUAL")
    if not editor:
        if sys.platform == "darwin":
            editor = "open -t"
        elif sys.platform == "win32":
            editor = "notepad"
        else:
            editor = "vi"

    try:
        subprocess.run([*editor.split(), str(config_path)], check=True)
    except FileNotFoundError:
        typer.echo(f"Error: editor not found: {editor}", err=True)
        raise typer.Exit(code=1)
    except subprocess.CalledProcessError as exception:
        typer.echo(f"Error: editor exited with code {exception.returncode}", err=True)
        raise typer.Exit(code=1)


@top_app.command("path")
def config_path():
    """Show the config file path."""
    from netaudio.common.config_loader import default_config_path

    typer.echo(str(default_config_path()))


def _resolve_targets(filtered, all_devices):
    if all_devices:
        if not filtered:
            typer.echo("Error: no devices found.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)
        return list(sort_devices(filtered))

    if len(filtered) == 0:
        typer.echo("Error: device not found.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    if len(filtered) > 1:
        names = ", ".join(device.name or server_name for server_name, device in filtered.items())
        typer.echo(f"Error: multiple devices matched: {names}", err=True)
        typer.echo("Use -n to select a device or --all for all devices.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    return [next(iter(filtered.items()))]


@app.command("sample-rate")
def sample_rate(
    rate: Optional[int] = typer.Argument(None, help=f"Sample rate: {VALID_SAMPLE_RATES}"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the sample rate."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if rate is None:
                if all_devices:
                    output_table(
                        ["Name", "Sample Rate"],
                        [[device.name or server_name, device.sample_rate or ""] for server_name, device in targets],
                    )
                else:
                    output_single(targets[0][1].sample_rate)
                return

            if rate not in VALID_SAMPLE_RATES:
                typer.echo(f"Error: invalid sample rate. Must be one of: {VALID_SAMPLE_RATES}", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            packet, _, port = commands.command_set_sample_rate(rate)
            for server_name, device in targets:
                await send(packet, device.ipv4, port)

    asyncio.run(_run())


@app.command()
def encoding(
    bits: Optional[int] = typer.Argument(None, help=f"Encoding bit depth: {VALID_ENCODINGS}"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the encoding bit depth."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if bits is None:
                if all_devices:
                    output_table(
                        ["Name", "Encoding"],
                        [[device.name or server_name, device.encoding if device.encoding is not None else "N/A"] for server_name, device in targets],
                    )
                else:
                    output_single(targets[0][1].encoding if targets[0][1].encoding is not None else "N/A")
                return

            if bits not in VALID_ENCODINGS:
                typer.echo(f"Error: invalid encoding. Must be one of: {VALID_ENCODINGS}", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            packet, _, port = commands.command_set_encoding(bits)
            for server_name, device in targets:
                await send(packet, device.ipv4, port)

    asyncio.run(_run())


@app.command()
def latency(
    value: Optional[float] = typer.Argument(None, help="Latency in milliseconds."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the device latency."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if value is None:
                if all_devices:
                    output_table(
                        ["Name", "Latency"],
                        [[device.name or server_name, device.latency or ""] for server_name, device in targets],
                    )
                else:
                    output_single(targets[0][1].latency)
                return

            packet, service_type = commands.command_set_latency(value)
            for server_name, device in targets:
                arc_port = _get_arc_port(device)
                await send(packet, device.ipv4, arc_port)

    asyncio.run(_run())


def _aes67_state_label(value):
    if value is None:
        return "N/A"
    return "on" if value else "off"


def _aes67_reboot_required(device):
    if device.aes67_current is not None and device.aes67_configured is not None:
        return device.aes67_current != device.aes67_configured
    return False


@app.command()
def aes67(
    enabled: Optional[str] = typer.Argument(None, help="on or off"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set AES67 mode."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if enabled is None:
                if all_devices:
                    headers = ["Name", "Current", "Configured", "Reboot Required"]
                    rows = []
                    for server_name, device in targets:
                        rows.append([
                            device.name or server_name,
                            _aes67_state_label(device.aes67_current),
                            _aes67_state_label(device.aes67_configured),
                            "yes" if _aes67_reboot_required(device) else "no",
                        ])
                    output_table(headers, rows)
                else:
                    device = targets[0][1]
                    current_label = _aes67_state_label(device.aes67_current)
                    configured_label = _aes67_state_label(device.aes67_configured)
                    reboot = _aes67_reboot_required(device)
                    if device.aes67_current is None and device.aes67_configured is not None:
                        output_single(configured_label)
                    elif device.aes67_current is not None and device.aes67_current == device.aes67_configured:
                        output_single(current_label)
                    elif device.aes67_current is None and device.aes67_configured is None:
                        output_single("N/A")
                    else:
                        typer.echo(f"current: {current_label}", err=False)
                        typer.echo(f"configured: {configured_label}", err=False)
                        if reboot:
                            typer.echo("reboot required", err=True)
                return

            if enabled.lower() not in ("on", "off"):
                typer.echo("Error: expected 'on' or 'off'.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            is_enabled = enabled.lower() == "on"
            packet, _, port = commands.command_enable_aes67(is_enabled)
            for server_name, device in targets:
                await send(packet, device.ipv4, port)

    asyncio.run(_run())


@app.command("preferred-leader")
def preferred_leader(
    enabled: Optional[str] = typer.Argument(None, help="on or off"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set preferred leader mode."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if enabled is None:
                if all_devices:
                    def _pref_display(device):
                        if device.preferred_leader is None:
                            return "N/A"
                        return "on" if device.preferred_leader else "off"
                    output_table(
                        ["Name", "Preferred Leader"],
                        [[device.name or server_name, _pref_display(device)] for server_name, device in targets],
                    )
                else:
                    device = targets[0][1]
                    if device.preferred_leader is None:
                        output_single("N/A")
                    else:
                        output_single("on" if device.preferred_leader else "off")
                return

            if enabled.lower() not in ("on", "off"):
                typer.echo("Error: expected 'on' or 'off'.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            is_preferred = enabled.lower() == "on"
            packet, _, port = commands.command_set_preferred_leader(is_preferred)
            for server_name, device in targets:
                for attempt in range(3):
                    await send(packet, device.ipv4, port)
                    if attempt < 2:
                        await asyncio.sleep(0.5)

    asyncio.run(_run())


@app.command("interface")
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
                from netaudio.dante.application import DanteApplication
                from netaudio.common.app_config import settings

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
                    rows.append([device.name or server_name, "0", "", str(device.ipv4) if device.ipv4 else "", "", "", "", pending_label])
                    continue

                for index, iface in enumerate(interfaces):
                    rows.append([
                        device.name or server_name,
                        str(index),
                        iface.get("mode", ""),
                        iface.get("ip_address", ""),
                        iface.get("netmask", ""),
                        iface.get("gateway", ""),
                        iface.get("dns_server", ""),
                        pending_label if index == 0 else "",
                    ])

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

        if mode == "static":
            if not all([ip_address, netmask, dns_server, gateway]):
                typer.echo("Error: --ip, --netmask, --dns, and --gateway are required for static mode.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            for server_name, device in targets:
                device_ip = str(device.ipv4)
                if mode == "dhcp":
                    packet, _, port = commands.command_set_interface_dhcp()
                else:
                    packet, _, port = commands.command_set_interface_static(ip_address, netmask, dns_server, gateway)
                await send(packet, device_ip, port)
                typer.echo(f"Set {device.name or server_name} to {mode}", err=True)

            typer.echo("Reboot required for changes to take effect.", err=True)

    asyncio.run(_run())
