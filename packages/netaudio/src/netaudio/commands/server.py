from __future__ import annotations

import asyncio
import logging

import typer

from netaudio_lib.common.socket_path import get_socket_path
from netaudio_lib.daemon.server import NetaudioDaemon, run_daemon

app = typer.Typer(help="Manage the netaudio daemon.", no_args_is_help=True)


@app.command()
def start(
    log_level: str = typer.Option("info", "--log-level", help="Logging level."),
):
    """Start the netaudio daemon."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    asyncio.run(run_daemon())


@app.command()
def status():
    """Check if the daemon is running."""
    socket_path = get_socket_path()

    if not socket_path.exists():
        typer.echo("Daemon is not running.")
        raise typer.Exit(code=1)

    async def _run():
        try:
            reader, writer = await asyncio.open_unix_connection(str(socket_path))
            writer.write(b'\x02')
            await writer.drain()

            import struct
            length_data = await asyncio.wait_for(reader.readexactly(4), timeout=2.0)
            length = struct.unpack(">I", length_data)[0]
            data = await asyncio.wait_for(reader.readexactly(length), timeout=2.0)

            import json
            devices = json.loads(data.decode("utf-8"))
            typer.echo(f"Daemon is running. {len(devices)} device(s) cached.")

            writer.close()
            await writer.wait_closed()
        except Exception:
            typer.echo("Daemon socket exists but is not responding.")
            raise typer.Exit(code=1)

    asyncio.run(_run())


@app.command()
def stop():
    """Stop the netaudio daemon."""
    socket_path = get_socket_path()

    if not socket_path.exists():
        typer.echo("Daemon is not running.")
        return

    import signal
    import os

    try:
        typer.echo("Sending stop signal to daemon...")

        async def _run():
            try:
                reader, writer = await asyncio.open_unix_connection(str(socket_path))
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

        asyncio.run(_run())
        typer.echo("Stop signal sent. The daemon will shut down shortly.")
    except Exception as e:
        typer.echo(f"Error stopping daemon: {e}", err=True)
        raise typer.Exit(code=1)
