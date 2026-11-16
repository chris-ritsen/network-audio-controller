from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import NoReturn

import typer


def executable() -> Path:
    name = "netaudio-endpoint.exe" if sys.platform == "win32" else "netaudio-endpoint"
    path = Path(__file__).resolve().parents[1] / "bin" / name

    if not path.is_file():
        typer.echo("Error: the native media worker is missing; rebuild or reinstall NetAudio.", err=True)
        raise typer.Exit(code=1)

    return path


def execute(arguments: list[str]) -> NoReturn:
    path = str(executable())
    os.execv(path, [path, *arguments])
