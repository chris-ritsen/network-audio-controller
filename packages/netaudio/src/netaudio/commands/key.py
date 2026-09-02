from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Optional

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS

app = typer.Typer(help="Lock key management.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)


def _write_qr_code(lock_key: str, output: Optional[Path], open_qr_code: bool) -> Path:
    from netaudio.common.lock_key_qr import open_path, write_lock_key_qr

    try:
        qr_code_path = write_lock_key_qr(lock_key, output)
    except (OSError, ValueError) as error:
        typer.echo(f"Error: could not write QR code: {error}", err=True)
        raise typer.Exit(code=1)

    if open_qr_code:
        try:
            open_path(qr_code_path)
        except (OSError, subprocess.CalledProcessError) as error:
            typer.echo(f"Error: QR code was created but could not be opened: {error}", err=True)
            raise typer.Exit(code=1)
    return qr_code_path


@app.command("get")
def key_get():
    """Show the current device lock key."""
    from netaudio.common.config_loader import get_config_value

    value, config_path = get_config_value("device_lock_key")
    if value:
        typer.echo(value)
    else:
        typer.echo(f"No device_lock_key in {config_path}", err=True)
        raise typer.Exit(code=1)


@app.command("set")
def key_set(
    value: str = typer.Argument(..., help="Lock key value (32-char hex string)."),
):
    """Set the device lock key in config."""
    from netaudio.common.lock_key_qr import normalize_lock_key

    try:
        normalized_value = normalize_lock_key(value)
    except ValueError:
        typer.echo("Error: key must be a 32-character hex string.", err=True)
        raise typer.Exit(code=1)

    from netaudio.common.config_loader import set_config_value

    set_config_value("device_lock_key", normalized_value)


@app.command("qr", help="Create a QR code that imports the configured lock key into the netaudio iOS app.")
def key_qr(
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output PNG or SVG path."),
    open_qr_code: bool = typer.Option(False, "--open", help="Open the generated QR code."),
):
    from netaudio.common.config_loader import get_config_value

    value, config_path = get_config_value("device_lock_key")
    if not value:
        typer.echo(f"No device_lock_key in {config_path}", err=True)
        raise typer.Exit(code=1)
    qr_code_path = _write_qr_code(value, output, open_qr_code)
    typer.echo(qr_code_path)


@app.command("clear")
def key_clear():
    """Remove the device lock key from config."""
    from netaudio.common.config_loader import set_config_value

    set_config_value("device_lock_key", None)


@app.command("extract")
def key_extract(
    path: Optional[Path] = typer.Option(
        None,
        "--path",
        help="Path to Dante Controller binary (libDanteController.dylib or .dll).",
        exists=True,
        readable=True,
    ),
    save: bool = typer.Option(
        False,
        "--save",
        help="Save extracted key to config.toml.",
    ),
    qr_code: bool = typer.Option(
        False,
        "--qr",
        help="Create an iOS lock-key import QR code.",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output PNG or SVG path; implies --qr.",
    ),
    open_qr_code: bool = typer.Option(
        False,
        "--open",
        help="Open the generated QR code; implies --qr.",
    ),
):
    """Extract the device lock key from a Dante Controller installation."""
    from netaudio.common.key_extract import extract_key_from_binary, find_dante_controller_binary

    if path is None:
        path = find_dante_controller_binary()
        if path is None:
            typer.echo("Error: Dante Controller not found. Use --path to specify the binary.", err=True)
            raise typer.Exit(code=1)

    typer.echo(f"Binary: {path}", err=True)

    key = extract_key_from_binary(path)
    if key is None:
        typer.echo("Error: could not extract key from binary.", err=True)
        raise typer.Exit(code=1)

    key_string = key.decode("ascii")
    typer.echo(key_string)

    if save:
        from netaudio.common.config_loader import set_config_value

        config_path = set_config_value("device_lock_key", key_string)
        typer.echo(f"Saved to {config_path}", err=True)

    if qr_code or output is not None or open_qr_code:
        qr_code_path = _write_qr_code(key_string, output, open_qr_code)
        typer.echo(f"QR code: {qr_code_path}", err=True)
