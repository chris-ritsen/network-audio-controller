from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import typer
from click.core import ParameterSource

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.common.app_config import settings

logger = logging.getLogger("netaudio")


def _version_callback(value: bool):
    if value:
        from netaudio import __version__

        typer.echo(f"netaudio {__version__}")
        raise typer.Exit()


SORT_FIELDS = {"mac", "name", "ip", "model", "server-name"}


class ColoredLogFormatter(logging.Formatter):
    RESET = "\033[0m"
    LEVEL_COLORS = {
        logging.DEBUG: "\033[90m",
        logging.INFO: "\033[1;36m",
        logging.WARNING: "\033[1;33m",
        logging.ERROR: "\033[1;31m",
        logging.CRITICAL: "\033[1;41m",
    }
    MESSAGE_COLORS = {
        logging.DEBUG: "\033[37m",
        logging.INFO: "",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[1;31m",
    }

    def format(self, record):
        level_color = self.LEVEL_COLORS.get(record.levelno, self.RESET)
        message_color = self.MESSAGE_COLORS.get(record.levelno, "")
        timestamp = self.formatTime(record, self.default_time_format)
        level = record.levelname
        message = record.getMessage()
        colored_message = f"{message_color}{message}{self.RESET}" if message_color else message
        return f"\033[90m{timestamp}\033[0m {level_color}{level:<8}{self.RESET} {colored_message}"


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
    timeout_explicit: bool = False
    verbose: bool = False
    capture: bool = False
    dissect: bool = False
    icons: bool = False
    ddm_context: str | None = None


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


def _load_icons_from_config() -> bool:
    from netaudio.common.config_loader import load_config_document

    try:
        ui_section = load_config_document().get("ui", {})
    except ValueError as exception:
        logger.warning(f"Failed to read config for icons setting: {exception}")
        return False
    if isinstance(ui_section, dict):
        return bool(ui_section.get("icons", False))
    return False


app = typer.Typer(
    name="netaudio",
    help="CLI for managing network audio devices.",
    context_settings=HELP_CONTEXT_SETTINGS,
    invoke_without_command=True,
    pretty_exceptions_enable=False,
)


@app.callback()
def _global_options(
    ctx: typer.Context,
    name: Optional[list[str]] = typer.Option(
        None, "-n", "--name", help="Filter by device name (glob).", envvar="NETAUDIO_NAME"
    ),
    host: Optional[list[str]] = typer.Option(None, "--host", help="Filter by device IP.", envvar="NETAUDIO_HOST"),
    server_name: Optional[list[str]] = typer.Option(
        None, "-s", "--server-name", help="Filter by mDNS server name (glob).", envvar="NETAUDIO_SERVER_NAME"
    ),
    mac: Optional[list[str]] = typer.Option(
        None, "-m", "--mac", help="Filter by MAC address (any format).", envvar="NETAUDIO_MAC"
    ),
    output_format: OutputFormat = typer.Option(
        OutputFormat.plain, "-o", "--output", help="Output format.", envvar="NETAUDIO_OUTPUT"
    ),
    json_flag: bool = typer.Option(False, "-j", "--json", help="Shorthand for --output=json."),
    sort: str = typer.Option(
        "mac",
        "--sort",
        help="Sort field[:asc|desc]. Fields: mac, name, ip, model, server-name.",
        envvar="NETAUDIO_SORT",
    ),
    no_color: bool = typer.Option(False, "--no-color", help="Disable colored output.", envvar="NETAUDIO_NO_COLOR"),
    timeout: float = typer.Option(
        5.0, "--timeout", help="mDNS discovery timeout in seconds.", envvar="NETAUDIO_TIMEOUT"
    ),
    lock_state_timeout: float = typer.Option(
        4.0,
        "--lock-state-timeout",
        help="Lock state collection timeout in seconds.",
        envvar="NETAUDIO_LOCK_STATE_TIMEOUT",
    ),
    interface: Optional[str] = typer.Option(
        None, "--interface", help="Network interface to use.", envvar="NETAUDIO_INTERFACE"
    ),
    ddm_context: Optional[str] = typer.Option(
        None,
        "--context",
        help="DDM server/domain context to use.",
        envvar="NETAUDIO_CONTEXT",
    ),
    log_level: str = typer.Option(
        "WARNING", "--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR).", envvar="NETAUDIO_LOG_LEVEL"
    ),
    debug: bool = typer.Option(False, "--debug", help="Shorthand for --log-level DEBUG.", envvar="NETAUDIO_DEBUG"),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Show all device fields.", envvar="NETAUDIO_VERBOSE"),
    dissect: bool = typer.Option(
        False, "--dissect", help="Annotated protocol dissection for packet displays.", envvar="NETAUDIO_DISSECT"
    ),
    capture: bool = typer.Option(
        False, "--capture", help="Record all packets to capture database.", envvar="NETAUDIO_CAPTURE"
    ),
    icons: bool = typer.Option(False, "--icons", help="Use Nerd Font icons in output.", envvar="NETAUDIO_ICONS"),
    version: Optional[bool] = typer.Option(
        None, "-V", "--version", help="Show version and exit.", callback=_version_callback, is_eager=True
    ),
):
    state.names = name or []
    state.hosts = host or []
    state.server_names = server_name or []
    state.macs = mac or []
    state.output_format = OutputFormat.json if json_flag else output_format
    state.sort_field, state.sort_reverse = _parse_sort(sort)
    state.no_color = no_color
    state.timeout = timeout
    state.timeout_explicit = ctx.get_parameter_source("timeout") is not ParameterSource.DEFAULT
    state.verbose = verbose
    state.dissect = dissect
    state.capture = capture
    state.ddm_context = ddm_context

    try:
        from netaudio.common.config_loader import default_config_path, load_config_document
        from netaudio.common.managed_api import resolve_ddm_configuration

        config_path = default_config_path()
        ddm_configuration = resolve_ddm_configuration(
            load_config_document(config_path),
            base_directory=config_path.parent,
        )
        if ddm_context is not None:
            ddm_configuration.context(ddm_context)
        else:
            state.ddm_context = ddm_configuration.default_context
    except ValueError as exception:
        raise typer.BadParameter(str(exception), param_hint="--context") from exception

    if not icons:
        icons = _load_icons_from_config()
    state.icons = icons

    settings.lock_state_timeout = lock_state_timeout
    settings.mdns_timeout = timeout
    settings.no_color = no_color

    if interface:
        settings.interface = interface

    effective_level = "DEBUG" if debug else log_level.upper()
    numeric_level = getattr(logging, effective_level, None)
    if numeric_level is None:
        raise typer.BadParameter(f"Invalid log level: {log_level}")

    if dissect and numeric_level > logging.INFO:
        numeric_level = logging.INFO

    if no_color:
        logging.basicConfig(level=numeric_level, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(ColoredLogFormatter())
        logging.basicConfig(level=numeric_level, handlers=[handler])
    if debug:
        settings.debug = True

    from netaudio import core

    try:
        core.require()
    except (core.NetaudioCoreError, core.NetaudioCoreLibraryMissing) as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1)

    if ctx.invoked_subcommand is None:
        from netaudio.commands.status import status as status_command

        status_command()


from netaudio.commands import channel, flow, key, report, server, shure, status, subscription, virtual
from netaudio.commands.capture.cli import app as capture_app
from netaudio.commands.config.cli import top_app as config_app
from netaudio.commands.ddm import app as ddm_app
from netaudio.commands.device.cli import app as device_app
from netaudio.commands.device.clock import clock as device_clock
from netaudio.commands.device.lock import lock_app
from netaudio.commands.fact.cli import app as fact_app
from netaudio.commands.firmware.cli import app as firmware_app
from netaudio.commands.meter.cli import meter_app
from netaudio.commands.preset.cli import app as preset_app
from netaudio.commands.provenance.cli import app as provenance_app

lab_app = typer.Typer(
    help="Protocol engineering: capture, dissection, provenance, firmware.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)
lab_app.add_typer(capture_app, name="capture")
lab_app.add_typer(fact_app, name="fact")
lab_app.add_typer(firmware_app, name="firmware")
lab_app.add_typer(provenance_app, name="provenance")
lock_app.add_typer(key.app, name="key")

app.add_typer(channel.app, name="channel")
app.command("clock")(device_clock)
app.add_typer(config_app, name="config")
app.add_typer(server.app, name="daemon")
app.add_typer(ddm_app, name="ddm")
app.add_typer(device_app, name="device")
app.add_typer(flow.app, name="flow")
app.add_typer(lab_app, name="lab")
app.add_typer(lock_app, name="lock")
app.add_typer(meter_app, name="meter")
app.add_typer(preset_app, name="preset")
app.add_typer(report.app, name="report")
app.add_typer(shure.app, name="shure")
app.command("status")(status.status)
app.add_typer(subscription.app, name="subscription")
app.add_typer(virtual.app, name="virtual")


def error_message(exception: BaseException) -> str:
    message = str(exception).strip()
    if message:
        return message
    return type(exception).__name__


def main():
    try:
        app()
    except Exception as exception:
        if settings.debug:
            raise
        typer.echo(f"Error: {error_message(exception)}", err=True)
        raise SystemExit(int(ExitCode.ERROR)) from None
