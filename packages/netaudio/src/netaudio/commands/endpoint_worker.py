from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import NoReturn

import typer


def execute(arguments: list[str]) -> NoReturn:
    name = "netaudio-endpoint.exe" if sys.platform == "win32" else "netaudio-endpoint"
    executable = Path(__file__).resolve().parents[1] / "bin" / name

    if not executable.is_file():
        typer.echo("Error: the native media worker is missing; rebuild or reinstall NetAudio.", err=True)
        raise typer.Exit(code=1)

    os.execv(str(executable), [str(executable), *arguments])
