from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(help="Lock key management.", no_args_is_help=True)


@app.command("get")
def key_get():
    """Show the current device lock key."""
    from netaudio_lib.common.config_loader import get_config_value

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
    if not re.fullmatch(r"[0-9a-fA-F]{32}", value):
        typer.echo("Error: key must be a 32-character hex string.", err=True)
        raise typer.Exit(code=1)

    from netaudio_lib.common.config_loader import set_config_value

    set_config_value("device_lock_key", value.lower())


@app.command("clear")
def key_clear():
    """Remove the device lock key from config."""
    from netaudio_lib.common.config_loader import set_config_value

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
):
    """Extract the device lock key from a Dante Controller installation."""
    from netaudio_lib.common.key_extract import extract_key_from_binary, find_dante_controller_binary

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
        from netaudio_lib.common.config_loader import set_config_value

        config_path = set_config_value("device_lock_key", key_string)
        typer.echo(f"Saved to {config_path}", err=True)
