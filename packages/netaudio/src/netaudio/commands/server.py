from __future__ import annotations

import asyncio
import typer

from netaudio_lib.common.socket_path import daemon_is_accessible, open_daemon_connection
from netaudio_lib.daemon.protocol import CMD_SHUTDOWN
from netaudio_lib.daemon.server import run_daemon

from netaudio.icons import icon

app = typer.Typer(help="Manage the netaudio daemon.", no_args_is_help=True)


@app.command()
def start():
    """Start the netaudio daemon."""
    from netaudio.cli import state
    asyncio.run(run_daemon(dissect=state.dissect, capture=state.capture))


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


@app.command()
def stop():
    """Stop the netaudio daemon."""
    if not daemon_is_accessible():
        typer.echo(f"{icon('offline')}Daemon is not running.")
        return

    try:

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
    except Exception as e:
        typer.echo(f"Error stopping daemon: {e}", err=True)
        raise typer.Exit(code=1)
