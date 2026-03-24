from __future__ import annotations

import asyncio
import socket
import time
from typing import Optional

import typer

from netaudio_lib.common.socket_path import daemon_is_accessible, open_daemon_connection
from netaudio_lib.daemon.protocol import CMD_SHUTDOWN
from netaudio_lib.daemon.server import run_daemon

from netaudio.icons import icon

app = typer.Typer(help="Manage the netaudio daemon.", no_args_is_help=True)


def _port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _send_shutdown():
    async def _run():
        try:
            reader, writer = await open_daemon_connection()
            writer.write(CMD_SHUTDOWN)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

    asyncio.run(_run())


def _wait_for_shutdown(relay_port, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not daemon_is_accessible() and not _port_in_use(relay_port):
            return True
        time.sleep(0.25)
    return False


@app.command()
def start(
    relay_port: Optional[int] = typer.Option(None, "--relay-port", help="Relay server port.", envvar="NETAUDIO_RELAY_PORT"),
):
    """Start the netaudio daemon."""
    from netaudio_lib.common.app_config import settings as app_settings
    from netaudio.cli import state

    effective_port = relay_port or app_settings.relay_port
    app_settings.relay_port = effective_port

    asyncio.run(run_daemon(dissect=state.dissect, capture=state.capture, relay_port=effective_port))


@app.command()
def stop():
    """Stop the netaudio daemon."""
    if not daemon_is_accessible():
        typer.echo(f"{icon('offline')}Daemon is not running.")
        return

    try:
        _send_shutdown()
    except Exception as exception:
        typer.echo(f"Error stopping daemon: {exception}", err=True)
        raise typer.Exit(code=1)


@app.command()
def restart(
    relay_port: Optional[int] = typer.Option(None, "--relay-port", help="Relay server port.", envvar="NETAUDIO_RELAY_PORT"),
):
    """Restart the netaudio daemon."""
    from netaudio_lib.common.app_config import settings as app_settings

    effective_port = relay_port or app_settings.relay_port

    if daemon_is_accessible():
        _send_shutdown()
        if not _wait_for_shutdown(effective_port):
            typer.echo("Timed out waiting for daemon to stop.", err=True)
            raise typer.Exit(code=1)

    from netaudio.cli import state

    app_settings.relay_port = effective_port
    asyncio.run(run_daemon(dissect=state.dissect, capture=state.capture, relay_port=effective_port))


@app.command()
def status():
    """Check if the daemon is running."""
    if not daemon_is_accessible():
        typer.echo(f"{icon('offline')}Daemon is not running.")
        raise typer.Exit(code=1)

    async def _run():
        try:
            reader, writer = await open_daemon_connection()
            writer.write(b"\x02")
            await writer.drain()

            import struct

            length_data = await asyncio.wait_for(reader.readexactly(4), timeout=2.0)
            length = struct.unpack(">I", length_data)[0]
            data = await asyncio.wait_for(reader.readexactly(length), timeout=2.0)

            import json

            devices = json.loads(data.decode("utf-8"))
            typer.echo(f"{icon('online')}Daemon is running. {len(devices)} device(s) cached.")

            writer.close()
            await writer.wait_closed()
        except Exception:
            typer.echo("Daemon socket exists but is not responding.")
            raise typer.Exit(code=1)

    asyncio.run(_run())
