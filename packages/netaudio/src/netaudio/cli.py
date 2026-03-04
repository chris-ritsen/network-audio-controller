from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import typer

from netaudio_lib.common.app_config import settings

from netaudio import __version__


def _version_callback(value: bool):
    if value:
        typer.echo(f"netaudio {__version__}")
        raise typer.Exit()


SORT_FIELDS = {"mac", "name", "ip", "model", "server-name"}


class OutputFormat(str, Enum):
    plain = "plain"
    table = "table"
    pretty = "pretty"
    json = "json"
    xml = "xml"
    csv = "csv"
    yaml = "yaml"


@dataclass
class State:
    names: list[str] = field(default_factory=list)
    hosts: list[str] = field(default_factory=list)
    server_names: list[str] = field(default_factory=list)
    macs: list[str] = field(default_factory=list)
    output_format: OutputFormat = OutputFormat.plain
    sort_field: str = "mac"
    sort_reverse: bool = False
    no_color: bool = False
    timeout: float = 5.0
    verbose: bool = False


state = State()


def _parse_sort(value: str) -> tuple[str, bool]:
    parts = value.rsplit(":", 1)
    sort_field = parts[0]
    if sort_field not in SORT_FIELDS:
        raise typer.BadParameter(f"Unknown sort field: {sort_field}. Must be one of: {', '.join(sorted(SORT_FIELDS))}")
    reverse = False
    if len(parts) == 2:
        if parts[1] == "desc":
            reverse = True
        elif parts[1] != "asc":
            raise typer.BadParameter(f"Sort direction must be 'asc' or 'desc', got: {parts[1]}")
    return sort_field, reverse


app = typer.Typer(
    name="netaudio",
    help="CLI for controlling Audinate Dante network audio devices.",
    context_settings={"help_option_names": ["--help"]},
    invoke_without_command=True,
)


@app.callback()
def _global_options(
    ctx: typer.Context,
    name: Optional[list[str]] = typer.Option(None, "-n", "--name", help="Filter by device name (glob).", envvar="NETAUDIO_NAME"),
    host: Optional[list[str]] = typer.Option(None, "-h", "--host", help="Filter by device IP.", envvar="NETAUDIO_HOST"),
    server_name: Optional[list[str]] = typer.Option(None, "-s", "--server-name", help="Filter by mDNS server name (glob).", envvar="NETAUDIO_SERVER_NAME"),
    mac: Optional[list[str]] = typer.Option(None, "-m", "--mac", help="Filter by MAC address (any format).", envvar="NETAUDIO_MAC"),
    output_format: OutputFormat = typer.Option(OutputFormat.plain, "-o", "--output", help="Output format.", envvar="NETAUDIO_OUTPUT"),
    json_flag: bool = typer.Option(False, "-j", "--json", help="Shorthand for --output=json."),
    sort: str = typer.Option("mac", "--sort", help="Sort field[:asc|desc]. Fields: mac, name, ip, model, server-name.", envvar="NETAUDIO_SORT"),
    no_color: bool = typer.Option(False, "--no-color", help="Disable colored output.", envvar="NETAUDIO_NO_COLOR"),
    timeout: float = typer.Option(5.0, "--timeout", help="mDNS discovery timeout in seconds.", envvar="NETAUDIO_TIMEOUT"),
    interface: Optional[str] = typer.Option(None, "--interface", help="Network interface to use.", envvar="NETAUDIO_INTERFACE"),
    log_level: str = typer.Option("WARNING", "--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR).", envvar="NETAUDIO_LOG_LEVEL"),
    debug: bool = typer.Option(False, "--debug", help="Shorthand for --log-level DEBUG.", envvar="NETAUDIO_DEBUG"),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Show all device fields.", envvar="NETAUDIO_VERBOSE"),
    version: Optional[bool] = typer.Option(None, "-V", "--version", help="Show version and exit.", callback=_version_callback, is_eager=True),
):
    state.names = name or []
    state.hosts = host or []
    state.server_names = server_name or []
    state.macs = mac or []
    state.output_format = OutputFormat.json if json_flag else output_format
    state.sort_field, state.sort_reverse = _parse_sort(sort)
    state.no_color = no_color
    state.timeout = timeout
    state.verbose = verbose

    settings.mdns_timeout = timeout
    settings.no_color = no_color

    if interface:
        settings.interface = interface

    effective_level = "DEBUG" if debug else log_level.upper()
    numeric_level = getattr(logging, effective_level, None)
    if numeric_level is None:
        raise typer.BadParameter(f"Invalid log level: {log_level}")
    logging.basicConfig(level=numeric_level, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    if debug:
        settings.debug = True

    if ctx.invoked_subcommand is None:
        from netaudio.commands.device import device_list
        device_list()


from netaudio.commands import capture, channel, config, device, server, subscription

app.add_typer(device.app, name="device")
app.add_typer(channel.app, name="channel")
app.add_typer(subscription.app, name="subscription")
app.add_typer(subscription.app, name="sub", hidden=True)
app.add_typer(config.app, name="config")
app.add_typer(server.app, name="server")
app.add_typer(capture.app, name="capture")


def main():
    app()
