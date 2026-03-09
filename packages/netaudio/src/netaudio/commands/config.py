from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from typing import Optional

import typer

from netaudio_lib.dante.device_commands import DanteDeviceCommands

from netaudio._common import (
    _command_context,
    _get_arc_port,
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
    from netaudio_lib.common.config_loader import default_config_path

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
    from netaudio_lib.common.config_loader import default_config_path

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
                    def _aes67_display(device):
                        if device.aes67_enabled is None:
                            return "N/A"
                        return "on" if device.aes67_enabled else "off"
                    output_table(
                        ["Name", "AES67"],
                        [[device.name or server_name, _aes67_display(device)] for server_name, device in targets],
                    )
                else:
                    device = targets[0][1]
                    if device.aes67_enabled is None:
                        output_single("N/A")
                    else:
                        output_single("on" if device.aes67_enabled else "off")
                return

            if enabled.lower() not in ("on", "off"):
                typer.echo("Error: expected 'on' or 'off'.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            is_enabled = enabled.lower() == "on"
            packet, _, port = commands.command_enable_aes67(is_enabled)
            for server_name, device in targets:
                await send(packet, device.ipv4, port)

    asyncio.run(_run())
