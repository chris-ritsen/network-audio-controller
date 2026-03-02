from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio._common import _resolve_device, output_single, set_device_filter
from netaudio._exit_codes import ExitCode

app = typer.Typer(help="Get or set device configuration.", no_args_is_help=True)

VALID_SAMPLE_RATES = [44100, 48000, 88200, 96000, 176400, 192000]
VALID_ENCODINGS = [16, 24, 32]


@app.command("sample-rate")
def sample_rate(
    device: str = typer.Argument(help="Device name, IP, or server name."),
    rate: Optional[int] = typer.Argument(None, help=f"Sample rate: {VALID_SAMPLE_RATES}"),
):
    """Get or set the sample rate."""
    set_device_filter(device)

    async def _run():
        resolved_device = await _resolve_device()

        if rate is None:
            output_single(resolved_device.sample_rate)
            return

        if rate not in VALID_SAMPLE_RATES:
            typer.echo(f"Error: invalid sample rate. Must be one of: {VALID_SAMPLE_RATES}", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        await resolved_device.operations.set_sample_rate(rate)
        typer.echo(f"Set sample rate: {rate}")

    asyncio.run(_run())


@app.command()
def encoding(
    device: str = typer.Argument(help="Device name, IP, or server name."),
    bits: Optional[int] = typer.Argument(None, help=f"Encoding bit depth: {VALID_ENCODINGS}"),
):
    """Get or set the encoding bit depth."""
    set_device_filter(device)

    async def _run():
        resolved_device = await _resolve_device()

        if bits is None:
            settings = await resolved_device.operations.get_device_settings()
            typer.echo(settings.get("encoding", "N/A") if settings else "N/A")
            return

        if bits not in VALID_ENCODINGS:
            typer.echo(f"Error: invalid encoding. Must be one of: {VALID_ENCODINGS}", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        await resolved_device.operations.set_encoding(str(bits))
        typer.echo(f"Set encoding: {bits}")

    asyncio.run(_run())


@app.command()
def latency(
    device: str = typer.Argument(help="Device name, IP, or server name."),
    value: Optional[int] = typer.Argument(None, help="Latency in microseconds."),
):
    """Get or set the device latency."""
    set_device_filter(device)

    async def _run():
        resolved_device = await _resolve_device()

        if value is None:
            output_single(resolved_device.latency)
            return

        await resolved_device.operations.set_latency(value)
        typer.echo(f"Set latency: {value}")

    asyncio.run(_run())


@app.command()
def aes67(
    device: str = typer.Argument(help="Device name, IP, or server name."),
    enabled: Optional[str] = typer.Argument(None, help="on or off"),
):
    """Get or set AES67 mode."""
    set_device_filter(device)

    async def _run():
        resolved_device = await _resolve_device()

        if enabled is None:
            if resolved_device.aes67_enabled is None:
                output_single("N/A")
            else:
                output_single("on" if resolved_device.aes67_enabled else "off")
            return

        if enabled.lower() not in ("on", "off"):
            typer.echo("Error: expected 'on' or 'off'.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        is_enabled = enabled.lower() == "on"
        await resolved_device.operations.enable_aes67(is_enabled)
        typer.echo(f"Set AES67: {'on' if is_enabled else 'off'}")

    asyncio.run(_run())
