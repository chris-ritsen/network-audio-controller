from __future__ import annotations

import asyncio
import math
from typing import Optional

import typer

from netaudio._common import _get_arc_port, filter_devices, output_single, output_table
from netaudio._exit_codes import ExitCode
from netaudio.commands.config_readback import _resolve_targets, _send_verified_change
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.latency import latency_state_from_settings
from netaudio.dante.services.notification import NOTIFICATION_LATENCY_CHANGE, NOTIFICATION_SETTINGS_CHANGE


async def _read_latency_settings(device):
    reader = getattr(device.operations, "get_latency_settings", None)
    if reader is None:
        reader = device.operations.get_device_settings
    return await reader()


async def _read_latency_setting_value(device, key):
    settings = await _read_latency_settings(device)
    if not isinstance(settings, dict) or settings.get(key) is None:
        raise RuntimeError(f"{key} readback was unavailable")
    return settings[key]


async def _read_latency_target(server_name, device):
    try:
        settings = await _read_latency_settings(device)
        values = latency_state_from_settings(settings)
        if not values or not any(key.endswith("latency_ns") and value is not None for key, value in values.items()):
            raise RuntimeError("latency readback was unavailable")
        return server_name, device, values, None
    except Exception as exception:
        return server_name, device, None, exception


async def _read_latency_targets(targets):
    readings = await asyncio.gather(*(_read_latency_target(server_name, device) for server_name, device in targets))
    failures = [reading for reading in readings if reading[3] is not None]
    if failures:
        for server_name, device, _, exception in failures:
            typer.echo(
                f"Error: could not read latency from {device.name or server_name}: {exception}",
                err=True,
            )
        raise typer.Exit(code=ExitCode.ERROR)
    return readings


def _format_latency_milliseconds(value) -> str:
    return "unknown" if value is None else f"{value:g}"


def _format_latency_range(values: dict) -> str:
    minimum = values.get("min_latency_ms")
    maximum = values.get("max_latency_ms")
    if minimum is None or maximum is None:
        return "unknown"
    return f"{minimum:g}-{maximum:g}"


def _format_latency_choices(values: dict) -> str:
    choices = values.get("latency_options_ms")
    if choices is None:
        return "unknown"
    if not choices:
        return "none"
    return ", ".join(f"{choice:g}" for choice in choices)


def _render_all_latency_readings(readings) -> None:
    headers = ["Name", "Active (ms)", "Configured (ms)", "Default (ms)", "Reported range (ms)", "Choices (ms)"]
    rows = [
        [
            device.name or server_name,
            _format_latency_milliseconds(values.get("active_latency_ms")),
            _format_latency_milliseconds(values.get("configured_latency_ms")),
            _format_latency_milliseconds(values.get("default_latency_ms")),
            _format_latency_range(values),
            _format_latency_choices(values),
        ]
        for server_name, device, values, _ in readings
    ]
    output_table(
        headers,
        rows,
        json_data={
            server_name: {"name": device.name or server_name, **values} for server_name, device, values, _ in readings
        },
    )


def _render_one_latency_reading(values: dict) -> None:
    from netaudio.cli import OutputFormat, state

    if state.output_format in (OutputFormat.json, OutputFormat.xml, OutputFormat.yaml):
        output_single(values)
    else:
        labels = (
            ("active_latency_ms", "Active latency"),
            ("configured_latency_ms", "Configured latency"),
            ("default_latency_ms", "Default latency"),
        )
        lines = [f"{label}: {_format_latency_milliseconds(values[key])} ms" for key, label in labels if key in values]
        if "min_latency_ms" in values or "max_latency_ms" in values:
            lines.append(f"Reported latency range: {_format_latency_range(values)} ms")
        if "latency_options_ms" in values:
            lines.append(f"Latency options: {_format_latency_choices(values)} ms")
        output_single("\n".join(lines))


def register_latency_command(app: typer.Typer, command_context_factory):
    @app.command()
    def latency(
        value: Optional[float] = typer.Argument(None, help="Latency in milliseconds."),
        all_devices: bool = typer.Option(False, "--all", help="Read or apply to all devices."),
    ):
        """Get the complete device latency state or set and verify latency."""

        commands = DanteDeviceCommands()

        async def _run():
            async with command_context_factory() as (devices, send):
                targets = _resolve_targets(filter_devices(devices), all_devices)
                if value is None:
                    readings = await _read_latency_targets(targets)
                    if all_devices:
                        _render_all_latency_readings(readings)
                    else:
                        _render_one_latency_reading(readings[0][2])
                    return

                if not math.isfinite(value) or value < 0:
                    typer.echo("Error: latency must be a finite, nonnegative number.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                packet, _ = commands.command_set_latency(value)
                expected_nanoseconds = int(round(value * 1_000_000))
                failures = await _send_verified_change(
                    targets,
                    send,
                    packet,
                    _get_arc_port,
                    expected_nanoseconds,
                    lambda device: _read_latency_setting_value(device, "active_latency_ns"),
                    "latency change",
                    lambda label: f"Set latency for {label}: {value:g} ms (verified)",
                    (NOTIFICATION_LATENCY_CHANGE, NOTIFICATION_SETTINGS_CHANGE),
                )
                if failures:
                    raise typer.Exit(code=ExitCode.ERROR)

        asyncio.run(_run())

    return latency
