from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import NoReturn, Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import run_with_devices
from netaudio.cli_support.output import output_single, output_table, structured_output_selected
from netaudio.cli_support.selection import filter_devices, selection_is_explicit
from netaudio.daemon.client import DaemonRefusal, call_daemon

app = typer.Typer(
    help="Save and load device presets (DC-compatible XML).",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


PRESET_REFERENCE_HELP = "Preset name in the preset directory, or an explicit .xml path."
SECTIONS = ["audio", "device_controls", "network", "routing"]


def _refuse(*lines: str) -> NoReturn:
    for line in lines:
        typer.echo(line, err=True)
    raise typer.Exit(code=ExitCode.ERROR)


def preset_directory() -> Path:
    from netaudio.common.config_loader import default_config_path, get_config_value

    configured, _ = get_config_value("preset_directory")
    if configured:
        return Path(str(configured)).expanduser()
    return default_config_path().parent / "presets"


def resolve_preset_path(reference: str, *, for_write: bool) -> Path:
    path = Path(reference).expanduser()
    if path.is_absolute() or len(path.parts) > 1 or path.suffix.lower() == ".xml":
        return path
    if not for_write and path.exists():
        return path
    return preset_directory() / f"{reference}.xml"


def _read_preset(reference: str) -> tuple[Path, str]:
    path = resolve_preset_path(reference, for_write=False)
    try:
        return path, path.read_text(encoding="utf-8")
    except FileNotFoundError:
        _refuse(f"Error: preset not found: {path}")
    except (OSError, UnicodeError) as exception:
        _refuse(f"Error: could not read preset {path}: {exception}")


def _write_preset(path: Path, content: str, *, force: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        if force:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


async def _targets(devices, xml: str) -> dict:
    selected = filter_devices(devices)
    if not selected:
        _refuse("Error: no devices matched the global filters.")
    preview = await call_daemon("POST", "/presets/preview", {"devices": list(selected), "xml": xml})
    excluded, targets = [], {}
    for entry in preview["devices"]:
        candidates = entry["targets"]
        if len(candidates) > 1:
            names = ", ".join(candidate["id"] for candidate in candidates)
            _refuse(f"Error: preset device {entry['name']!r} matches more than one device: {names}")
        if candidates:
            targets[entry["name"]] = candidates[0]["id"]
        else:
            excluded.append(entry["name"])
    if excluded and not selection_is_explicit():
        _refuse(
            "Error: these preset devices were not found, so nothing was sent:",
            *(f"  - {name}" for name in excluded),
            "Select devices with the global filters to load only part of the preset.",
        )
    if not targets:
        _refuse("Error: no selected devices have matching entries in this preset.")
    return {"excluded": excluded, "targets": targets, "xml": xml}


def _value(value) -> str:
    return "unavailable" if value is None else value if isinstance(value, str) else json.dumps(value, sort_keys=True)


def _show_plan(plan: dict) -> None:
    typer.echo(f"Preset: {plan['name']} ({len(plan['devices'])} devices)")
    for device in plan["devices"]:
        typer.echo(f"{device['device_name']} [{device['server_name']}]:")
        unchanged = 0
        for action in device["actions"]:
            if action["state"] == "unchanged":
                unchanged += 1
                continue
            label = action["kind"].replace("_", " ")
            typer.echo(
                f"  {label}: {action['state']}; {_value(action['current'])} -> {_value(action['requested'])}"
                f" ({action['reason']})"
            )
        if unchanged:
            typer.echo(f"  {unchanged} settings already match")
        if not device["actions"]:
            typer.echo("  no applicable settings")


async def run_preset_save(devices, output_path: Path, preset_name: str | None, force: bool) -> None:
    selected = filter_devices(devices)
    if not selected:
        _refuse("Error: no devices found.")
    captured = await call_daemon(
        "POST",
        "/presets/save",
        {"devices": list(selected), "name": preset_name or output_path.stem, "sections": SECTIONS},
    )
    try:
        _write_preset(output_path, captured["xml"], force=force)
    except FileExistsError:
        _refuse(f"Error: refusing to overwrite existing file: {output_path}; use --force to replace it.")
    except OSError as exception:
        _refuse(f"Error: could not save preset to {output_path}: {exception}")
    for omission in captured["omitted"]:
        typer.echo(f"Not saved for {omission['device']}: {omission['setting']} ({omission['reason']})", err=True)
    typer.echo(f"Saved {captured['device_count']} devices to {output_path}")


@app.command("save", help="Save the selected devices' configuration as a preset.")
def preset_save(
    output: str = typer.Argument(..., help=PRESET_REFERENCE_HELP),
    preset_name: Optional[str] = typer.Option(None, "--name", "-n", help="Preset name (defaults to filename)."),
    force: bool = typer.Option(False, "--force", help="Replace an existing preset file."),
):
    output_path = resolve_preset_path(output, for_write=True)
    if output_path.exists() and not force:
        _refuse(f"Error: refusing to overwrite existing file: {output_path}; use --force to replace it.")
    run_with_devices(run_preset_save, output_path, preset_name, force)


async def run_preset_show(devices, xml: str) -> None:
    plan = await call_daemon("POST", "/presets/plan", await _targets(devices, xml), timeout=120.0)
    if structured_output_selected():
        output_single(plan)
    else:
        _show_plan(plan)


async def run_preset_load(devices, xml: str, store_current_configuration: bool) -> None:
    request = await _targets(devices, xml)
    request.update(
        confirmed=True,
        digest=hashlib.sha256(xml.encode()).hexdigest(),
        store_current_configuration=store_current_configuration,
    )
    try:
        result = await call_daemon("POST", "/presets/load", request, timeout=300.0)
    except DaemonRefusal as refusal:
        if not isinstance(refusal.data, dict) or "report" not in refusal.data:
            raise
        result = refusal.data
    if structured_output_selected():
        output_single(result)
    else:
        report = result["report"]
        unchanged: dict[str, int] = {}
        for operation in report["operations"]:
            if operation["state"] == "unchanged":
                unchanged[operation["device_name"]] = unchanged.get(operation["device_name"], 0) + 1
                continue
            label = operation["kind"].replace("_", " ")
            typer.echo(f"{operation['device_name']}: {label}: {operation['state']}; {operation['message']}")
        for device_name, count in unchanged.items():
            typer.echo(f"{device_name}: {count} settings already matched")
        if report["needs_reboot"]:
            typer.echo(f"Reboot required: {', '.join(dict.fromkeys(report['needs_reboot']))}")
    if not result.get("complete"):
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("load", help="Apply a saved preset to the devices it names.")
def preset_load(
    input_file: str = typer.Argument(..., help=PRESET_REFERENCE_HELP),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be applied without making changes."),
    store_current_configuration: bool = typer.Option(
        False,
        "--store-current-configuration",
        help="Ask each changed device to store its configuration once every setting has been read back.",
    ),
):
    _, xml = _read_preset(input_file)
    if dry_run:
        run_with_devices(run_preset_show, xml)
    else:
        run_with_devices(run_preset_load, xml, store_current_configuration)


@app.command("show", help="Show what a saved preset would apply, without changing anything.")
def preset_show(
    input_file: str = typer.Argument(..., help=PRESET_REFERENCE_HELP),
):
    _, xml = _read_preset(input_file)
    run_with_devices(run_preset_show, xml)


async def run_preset_list() -> None:
    directory = preset_directory()
    rows = []
    listing = {}
    for path in sorted(directory.glob("*.xml")) if directory.is_dir() else []:
        try:
            preview = await call_daemon(
                "POST", "/presets/preview", {"devices": [], "xml": path.read_text(encoding="utf-8")}
            )
        except (DaemonRefusal, OSError, UnicodeError) as exception:
            typer.echo(f"Warning: skipping {path}: {exception}", err=True)
            continue
        names = sorted(device["name"] for device in preview["devices"])
        saved_at = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        rows.append([path.stem, preview["name"], str(len(names)), saved_at, str(path)])
        listing[path.stem] = {
            "device_count": len(names),
            "devices": names,
            "name": preview["name"],
            "path": str(path),
            "saved": saved_at,
        }
    output_table(
        ["Preset", "Name", "Devices", "Saved", "Path"],
        rows,
        json_data=listing,
        empty_message=f"No presets in {directory}.",
    )


@app.command("list", help="List presets saved in the preset directory.")
def preset_list():
    asyncio.run(run_preset_list())
