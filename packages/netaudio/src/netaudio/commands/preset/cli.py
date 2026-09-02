from __future__ import annotations

import os
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import run_command
from netaudio.commands.preset.display import show_preset_dry_run
from netaudio.commands.preset.loading import run_preset_load
from netaudio.commands.preset.parsing import parse_preset

app = typer.Typer(
    help="Save and load device presets (DC-compatible XML).",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)

PRESET_REFERENCE_HELP = "Preset name in the preset directory, or an explicit .xml path."


def preset_directory() -> Path:
    from netaudio.common.config_loader import default_config_path, get_config_value

    configured, _ = get_config_value("preset_directory")
    if configured:
        return Path(str(configured)).expanduser()
    return default_config_path().parent / "presets"


def _is_explicit_preset_path(reference: str) -> bool:
    path = Path(reference).expanduser()
    return path.is_absolute() or len(path.parts) > 1 or path.suffix.lower() == ".xml"


def resolve_preset_path(reference: str, *, for_write: bool) -> Path:
    path = Path(reference).expanduser()
    if _is_explicit_preset_path(reference):
        return path
    if not for_write and path.exists():
        return path
    return preset_directory() / f"{reference}.xml"


def _write_preset_atomic(path: Path, content: str, *, force: bool) -> None:
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
            temporary.unlink()
    finally:
        temporary.unlink(missing_ok=True)


async def run_preset_save(application, devices, output_path: Path, preset_name: str | None, force: bool) -> None:
    from netaudio.cli_support.output import format_devices_xml
    from netaudio.cli_support.selection import filter_devices

    devices = filter_devices(devices)

    if not devices:
        typer.echo("Error: no devices found.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    name = preset_name or output_path.stem
    xml_content = format_devices_xml(devices, preset_name=name)

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        _write_preset_atomic(output_path, xml_content, force=force)
    except FileExistsError as exception:
        typer.echo(
            f"Error: refusing to overwrite existing file: {output_path}; use --force to replace it.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR) from exception
    except OSError as exception:
        typer.echo(f"Error: could not save preset to {output_path}: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR) from exception
    typer.echo(f"Saved {len(devices)} devices to {output_path}", err=True)


@app.command("save", help="Save the selected devices' configuration as a preset.")
def preset_save(
    output: str = typer.Argument(..., help=PRESET_REFERENCE_HELP),
    preset_name: Optional[str] = typer.Option(None, "--name", "-n", help="Preset name (defaults to filename)."),
    force: bool = typer.Option(False, "--force", help="Replace an existing preset file."),
):
    output_path = resolve_preset_path(output, for_write=True)
    if output_path.exists() and not force:
        typer.echo(
            f"Error: refusing to overwrite existing file: {output_path}; use --force to replace it.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)

    run_command(run_preset_save, output_path, preset_name, force)


@app.command("load", help="Apply a saved preset to the devices it names.")
def preset_load(
    input_file: str = typer.Argument(..., help=PRESET_REFERENCE_HELP),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be applied without making changes."),
    confirm_destructive: bool = typer.Option(
        False,
        "--confirm-destructive",
        help="Confirm permanent transmitter-flow membership loss caused by sample-rate restoration.",
    ),
):
    preset_path = resolve_preset_path(input_file, for_write=False)
    if not preset_path.exists():
        typer.echo(f"Error: preset not found: {preset_path}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    try:
        preset_name, preset_devices = parse_preset(preset_path)
    except (ET.ParseError, OSError, ValueError) as exception:
        typer.echo(f"Error: invalid preset {preset_path}: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR) from exception

    typer.echo(f"Preset: {preset_name} ({len(preset_devices)} devices)", err=True)

    if dry_run:
        show_preset_dry_run(preset_devices)
        return

    run_command(run_preset_load, preset_devices, confirm_destructive)


@app.command("show", help="Show what a saved preset would apply, without changing anything.")
def preset_show(
    input_file: str = typer.Argument(..., help=PRESET_REFERENCE_HELP),
):
    preset_load(input_file=input_file, dry_run=True)


@app.command("list", help="List presets saved in the preset directory.")
def preset_list():
    from netaudio.cli import OutputFormat, state
    from netaudio.cli_support.output import output_table

    directory = preset_directory()
    rows = []
    json_data = {}
    for preset_path in sorted(directory.glob("*.xml")) if directory.is_dir() else []:
        try:
            preset_name, preset_devices = parse_preset(preset_path)
        except (ET.ParseError, OSError, ValueError) as exception:
            typer.echo(f"Warning: skipping {preset_path}: {exception}", err=True)
            continue
        saved_at = datetime.fromtimestamp(preset_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        rows.append([preset_path.stem, preset_name, str(len(preset_devices)), saved_at, str(preset_path)])
        json_data[preset_path.stem] = {
            "device_count": len(preset_devices),
            "devices": sorted(preset_devices),
            "name": preset_name,
            "path": str(preset_path),
            "saved": saved_at,
        }

    if not rows and state.output_format in (OutputFormat.plain, OutputFormat.pretty, OutputFormat.table):
        typer.echo(f"No presets in {directory}.")
        return

    output_table(["Preset", "Name", "Devices", "Saved", "Path"], rows, json_data=json_data)
