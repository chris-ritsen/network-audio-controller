from __future__ import annotations

from typing import Any

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.selection import filter_devices
from netaudio.presets.loading import (
    MatchedPresetDevice,
    PresetLoadReport,
    PresetValidationError,
    build_preset_plan,
    apply_preset_plan,
)


def _refuse(lines: list[str]) -> None:
    for line in lines:
        typer.echo(line, err=True)
    raise typer.Exit(code=ExitCode.ERROR)


def _match_preset_devices(devices: dict, preset_devices: dict[str, dict[str, Any]]) -> list[MatchedPresetDevice]:
    from netaudio.cli import state as cli_state

    devices = filter_devices(devices)
    if not devices:
        _refuse(["Error: no devices matched the global filters."])
    devices_by_name: dict[str, list] = {}
    for server_name, device in devices.items():
        if device.name:
            devices_by_name.setdefault(device.name, []).append((server_name, device))
    matched = []
    unmatched_preset_names = []
    for device_name, config in preset_devices.items():
        candidates = devices_by_name.get(device_name, [])
        if not candidates:
            unmatched_preset_names.append(device_name)
            continue
        if len(candidates) > 1:
            servers = ", ".join(server_name for server_name, _ in candidates)
            _refuse([f"Error: preset device name {device_name!r} is ambiguous: {servers}"])
        server_name, device = candidates[0]
        matched.append(
            MatchedPresetDevice(config=config, device=device, device_name=device_name, server_name=server_name)
        )
    filters_active = bool(cli_state.names or cli_state.hosts or cli_state.server_names or cli_state.macs)
    if unmatched_preset_names and not filters_active:
        _refuse(
            [
                "Error: preset load was refused before sending any changes because these preset devices were not found:",
                *(f"  - {device_name}" for device_name in unmatched_preset_names),
                "Use global device filters to intentionally load only a selected subset.",
            ]
        )
    if not matched:
        _refuse(["Error: no selected devices have matching entries in this preset."])
    return matched


def _report_preset_load(report: PresetLoadReport) -> None:
    typer.echo("\nPreset load summary:", err=True)
    for device_name, result in report.results:
        typer.echo(f"  {device_name}: {result}", err=True)
    if report.needs_reboot:
        typer.echo(f"\nReboot required: {', '.join(dict.fromkeys(report.needs_reboot))}", err=True)
    if report.failures:
        raise typer.Exit(code=ExitCode.ERROR)


async def run_preset_load(application, devices, preset_devices: dict, confirm_destructive: bool) -> None:
    matched_devices = _match_preset_devices(devices, preset_devices)
    try:
        plan = await build_preset_plan(matched_devices)
    except PresetValidationError as exception:
        _refuse(exception.lines)
    report = await apply_preset_plan(application, plan, confirm_destructive=confirm_destructive)
    _report_preset_load(report)
