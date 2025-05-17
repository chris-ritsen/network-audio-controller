import asyncio
import os
import shlex
from typing import Optional

import typer
from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.history import FileHistory
from typing_extensions import Annotated

from netaudio import version as netaudio_version
from netaudio.common.app_config import (
    get_available_interfaces,
)
from netaudio.common.app_config import settings as app_settings

try:
    from signal import SIG_DFL, SIGPIPE, signal

    signal(SIGPIPE, SIG_DFL)
except ImportError:
    pass

app = typer.Typer(
    name="netaudio",
    help="Control Audinate Dante network audio devices.",
    add_completion=False,
    no_args_is_help=True,
)


def version_callback(value: bool):
    if value:
        print(f"Netaudio version {netaudio_version.version}")
        raise typer.Exit()


def interface_callback(value: str):
    if value == "list":
        interfaces = get_available_interfaces()

        for name, ip, prefix in interfaces:
            print(f"{name}: {ip}/{prefix}")

        raise typer.Exit()
    return value


@app.callback()
def global_options(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=version_callback,
            is_eager=True,
            help="Show application version and exit.",
        ),
    ] = False,
    interface: Annotated[
        Optional[str],
        typer.Option(
            "--interface",
            "-i",
            callback=interface_callback,
            help="Specify network interface to use for connections. Use 'list' to show available interfaces.",
            show_default=False,
        ),
    ] = None,
    mdns_timeout: Annotated[
        Optional[float],
        typer.Option(
            "--mdns-timeout",
            help=f"Set mDNS discovery timeout in seconds. Default: {app_settings.mdns_timeout}s.",
            show_default=False,
        ),
    ] = None,
    dump_payloads: Annotated[
        bool,
        typer.Option(
            "--dump-payloads",
            help="Enable dumping of raw Dante device command payloads to 'netaudio_device_payloads' directory.",
            show_default=True,
        ),
    ] = False,
    refresh: Annotated[
        bool,
        typer.Option(
            "--refresh",
            help="Force a full mDNS re-scan, bypassing all caches.",
            show_default=True,
        ),
    ] = False,
):
    """
    Netaudio: Network audio control utility.
    """
    if mdns_timeout is not None:
        app_settings.mdns_timeout = mdns_timeout

    if dump_payloads:
        app_settings.dump_payloads = True

    if interface is not None:
        app_settings.interface = interface

    if refresh:
        app_settings.refresh = True
    else:
        app_settings.refresh = False


from .commands.device import app as device_app

app.add_typer(device_app, name="device")

from .commands.channel import app as channel_app

app.add_typer(channel_app, name="channel")

from .commands.config import app as config_app

app.add_typer(config_app, name="config")

from .commands.subscription import app as subscription_app

app.add_typer(subscription_app, name="subscription")

from .commands.server import app as server_app

app.add_typer(server_app, name="server")


@app.command(name="repl", help="Start an interactive Netaudio shell.")
def start_repl():
    """
    Starts an interactive shell (REPL) for Netaudio commands.
    Type commands directly, e.g., 'device list --json'.
    Type 'exit' or 'quit' to leave the REPL.
    An empty prompt will show main help. 'help command' will show command help.
    """
    history_file = os.path.expanduser("~/.netaudio_repl_history")
    session = PromptSession(
        history=FileHistory(history_file),
        auto_suggest=AutoSuggestFromHistory(),
    )

    while True:
        try:
            command_string = session.prompt("netaudio> ")
            if not command_string.strip():
                args = ["--help"]
            else:
                raw_args = shlex.split(command_string)
                if raw_args[0].lower() == "help":
                    if len(raw_args) == 1:
                        args = ["--help"]
                    else:
                        args = raw_args[1:] + ["--help"]
                else:
                    args = raw_args

            if command_string.lower() in ["exit", "quit"]:
                break

            try:
                app(args, prog_name="netaudio", standalone_mode=False)
            except typer.Exit as e:
                if e.code != 0:
                    typer.echo(f"Command exited with code {e.code}.", err=True)
            except SystemExit as e:
                if e.code != 0:
                    typer.echo(
                        f"Command resulted in SystemExit with code {e.code}.", err=True
                    )
            except Exception as e:
                typer.echo(f"Error executing command: {e}", err=True)

        except KeyboardInterrupt:
            typer.echo("")
            continue
        except EOFError:
            break


def main():
    app()


if __name__ == "__main__":
    main()
