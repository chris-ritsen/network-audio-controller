from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio_lib.dante.device_commands import DanteDeviceCommands

from netaudio._common import (
    _command_context,
    _get_arc_port,
    _resolve_one,
    filter_devices,
    output_single,
)
from netaudio._exit_codes import ExitCode

app = typer.Typer(help="Get or set device configuration.", no_args_is_help=True)

VALID_SAMPLE_RATES = [44100, 48000, 88200, 96000, 176400, 192000]
VALID_ENCODINGS = [16, 24, 32]


@app.command("sample-rate")
def sample_rate(
    rate: Optional[int] = typer.Argument(None, help=f"Sample rate: {VALID_SAMPLE_RATES}"),
):
    """Get or set the sample rate."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            server_name, device = _resolve_one(filtered)

            if rate is None:
                output_single(device.sample_rate)
                return

            if rate not in VALID_SAMPLE_RATES:
                typer.echo(f"Error: invalid sample rate. Must be one of: {VALID_SAMPLE_RATES}", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            packet, _, port = commands.command_set_sample_rate(rate)
            await send(packet, device.ipv4, port)
            typer.echo(f"Set sample rate: {rate}")

    asyncio.run(_run())


@app.command()
def encoding(
    bits: Optional[int] = typer.Argument(None, help=f"Encoding bit depth: {VALID_ENCODINGS}"),
):
    """Get or set the encoding bit depth."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            server_name, device = _resolve_one(filtered)

            if bits is None:
                output_single(device.encoding if device.encoding is not None else "N/A")
                return

            if bits not in VALID_ENCODINGS:
                typer.echo(f"Error: invalid encoding. Must be one of: {VALID_ENCODINGS}", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            packet, _, port = commands.command_set_encoding(bits)
            await send(packet, device.ipv4, port)
            typer.echo(f"Set encoding: {bits}")

    asyncio.run(_run())


@app.command()
def latency(
    value: Optional[float] = typer.Argument(None, help="Latency in milliseconds."),
):
    """Get or set the device latency."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            server_name, device = _resolve_one(filtered)

            if value is None:
                output_single(device.latency)
                return

            packet, service_type = commands.command_set_latency(value)
            arc_port = _get_arc_port(device)
            await send(packet, device.ipv4, arc_port)
            typer.echo(f"Set latency: {value}")

    asyncio.run(_run())


@app.command()
def aes67(
    enabled: Optional[str] = typer.Argument(None, help="on or off"),
):
    """Get or set AES67 mode."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            server_name, device = _resolve_one(filtered)

            if enabled is None:
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
            await send(packet, device.ipv4, port)
            typer.echo(f"Set AES67: {'on' if is_enabled else 'off'}")

    asyncio.run(_run())
