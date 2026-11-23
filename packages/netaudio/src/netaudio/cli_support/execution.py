from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support import coverage
from netaudio.common.app_config import settings
from netaudio.daemon.client import DaemonRefusal, call_daemon
from netaudio.device_record import Device


def ansi(code: str, text: str) -> str:
    if settings.no_color:
        return str(text)
    return f"\033[{code}m{text}\033[0m"


async def daemon_devices() -> dict[str, Device]:
    records = await call_daemon("GET", "/devices", timeout=5.0)
    devices = {server_name: Device(record) for server_name, record in records.items()}
    coverage.warn(devices)
    return devices


def run_with_devices(run: Callable[..., Awaitable[Any]], *arguments, **options) -> Any:
    async def _run():
        return await run(await daemon_devices(), *arguments, **options)

    return asyncio.run(_run())


async def change_devices(targets, path: str, change: dict, describe: Callable[[str], str]) -> None:
    failed = False
    for server_name, device in targets:
        label = device.name or server_name
        try:
            await call_daemon("POST", path, {"device": server_name, **change})
        except DaemonRefusal as refusal:
            failed = True
            typer.echo(f"Error: {label}: {refusal}", err=True)
        else:
            typer.echo(describe(label))
    if failed:
        raise typer.Exit(code=ExitCode.ERROR)
