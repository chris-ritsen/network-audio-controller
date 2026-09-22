from __future__ import annotations

import asyncio
import math

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.output import output_single, output_table
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.dante.readback import MUTATION_ERRORS
from netaudio.commands.device.display import _format_latency_milliseconds as format_latency_milliseconds
from netaudio.dante.latency import latency_state_from_settings


async def _read_latency_target(application, server_name, device):
    try:
        settings = await application.get_latency_settings(device)
        values = latency_state_from_settings(settings)
        if not values or not any(key.endswith("latency_ns") and value is not None for key, value in values.items()):
            raise RuntimeError("latency readback was unavailable")
        return server_name, device, values, None
    except MUTATION_ERRORS as exception:
        return server_name, device, None, exception


async def _read_latency_targets(application, targets):
    readings = await asyncio.gather(
        *(_read_latency_target(application, server_name, device) for server_name, device in targets)
    )
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
    return "unknown" if value is None else format_latency_milliseconds(value)


def _format_latency_range(values: dict) -> str:
    minimum = values.get("min_latency_ms")
    maximum = values.get("max_latency_ms")
    if minimum is None or maximum is None:
        return "unknown"
    return f"{format_latency_milliseconds(minimum)}-{format_latency_milliseconds(maximum)}"


def _format_latency_choices(values: dict) -> str:
    choices = values.get("latency_options_ms")
    if choices is None:
        return "unknown"
    if not choices:
        return "none"
    return ", ".join(format_latency_milliseconds(choice) for choice in choices)


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

    if state.output_format in (OutputFormat.json, OutputFormat.xml, OutputFormat.csv, OutputFormat.yaml):
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


async def run_latency(application, devices, value: float | None, all_devices: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)
    if value is None:
        readings = await _read_latency_targets(application, targets)
        if all_devices:
            _render_all_latency_readings(readings)
        else:
            values = readings[0][2]
            assert values is not None
            _render_one_latency_reading(values)
        return

    if not math.isfinite(value) or value < 0:
        typer.echo("Error: latency must be a finite, nonnegative number.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    async def set_target(server_name, device):
        label = device.name or server_name

        try:
            return label, await application.set_latency(device, value), None
        except MUTATION_ERRORS as exception:
            return label, None, exception

    results = await asyncio.gather(*(set_target(server_name, device) for server_name, device in targets))
    failures = 0

    for label, result, exception in results:
        if exception is not None:
            typer.echo(f"Error: could not set latency for {label}: {exception}", err=True)
            failures += 1
            continue

        if result["effective_state_confirmed"]:
            typer.echo(f"Set configured latency for {label}: {result['configured_latency_ms']:g} ms (verified)")
            continue

        failures += 1
        if result["state"] == "rejected":
            detail = "device rejected the request"
        elif result["configured_latency_ms"] is None:
            detail = "configured latency readback was unavailable"
        else:
            detail = f"device reports {result['configured_latency_ms']:g} ms instead of {value:g} ms"

        typer.echo(f"Error: latency change for {label}: {detail}", err=True)

    if failures:
        raise typer.Exit(code=ExitCode.ERROR)
