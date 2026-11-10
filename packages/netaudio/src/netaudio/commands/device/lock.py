from __future__ import annotations

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import run_with_devices
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.daemon.client import call_daemon
from netaudio.icons import icon

lock_app = typer.Typer(help="Device lock management.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)


async def run_lock_operation(devices, pin: str, *, locking: bool) -> None:
    [(server_name, _)] = select_device(filter_devices(devices))
    result = await call_daemon("POST", "/lock" if locking else "/unlock", {"device": server_name, "pin": pin})
    if result.get("already"):
        typer.echo("already locked" if locking else "already unlocked", err=True)


@lock_app.command("set", help="Lock the selected device with a 4-digit PIN.")
def lock_set(
    pin: str = typer.Argument(..., help="4-digit numeric PIN to lock the device with."),
):
    run_with_devices(run_lock_operation, pin, locking=True)


@lock_app.command("clear", help="Unlock the selected device with its 4-digit PIN.")
def lock_clear(
    pin: str = typer.Argument(..., help="4-digit numeric PIN to unlock the device."),
):
    run_with_devices(run_lock_operation, pin, locking=False)


async def run_lock_status(devices) -> None:
    targets = select_device(filter_devices(devices), allow_many=True)
    labels = {True: f"{icon('lock')}locked", False: f"{icon('unlock')}unlocked", None: "unknown"}
    output_table(
        ["Name", "IP Address", "Lock Status"],
        [
            [device.name or "", str(device.ipv4) if device.ipv4 else "", labels[device.is_locked]]
            for _, device in targets
        ],
        json_data={
            server_name: {
                "name": device.name,
                "ipv4": str(device.ipv4) if device.ipv4 else None,
                "is_locked": device.is_locked,
            }
            for server_name, device in targets
        },
    )


@lock_app.command("status")
def lock_status():
    """Show device lock status."""
    run_with_devices(run_lock_status)
