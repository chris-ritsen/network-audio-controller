from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.output import output_single, structured_output_selected
from netaudio.daemon.client import refresh_discovery_on_daemon
from netaudio.dante.discovery import discovery_destination

app = typer.Typer(
    help="Request network service discovery.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)


@app.command()
def refresh(
    address: Optional[str] = typer.Option(
        None, "--address", help="Send directly to one IPv4 address instead of multicast."
    ),
):
    """Request discovery once through the running daemon; replies update its inventory."""
    try:
        address = discovery_destination(address)
    except ValueError as exception:
        raise typer.BadParameter(str(exception), param_hint="--address") from exception
    status, result = asyncio.run(refresh_discovery_on_daemon(address))
    if status != 200 or not isinstance(result, dict) or result.get("success") is not True:
        message = result.get("error") if isinstance(result, dict) else None
        typer.echo(f"Error: {message or 'could not request discovery from the daemon'}", err=True)
        raise typer.Exit(code=1)
    if structured_output_selected():
        output_single(result)
        return
    service_types = ", ".join(result.get("service_types") or [])
    typer.echo(f"Discovery requested through the daemon to {result.get('destination')} for {service_types}.")
