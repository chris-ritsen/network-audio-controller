import click
import typer
from typer.testing import CliRunner

from netaudio.cli import app

runner = CliRunner()


def _walk(command, path):
    yield path, command
    if isinstance(command, click.Group):
        for name in sorted(command.commands):
            yield from _walk(command.commands[name], f"{path} {name}")


def _command_tree():
    return list(_walk(typer.main.get_command(app), "netaudio"))


def test_dash_h_is_help_on_root_and_subcommands():
    for arguments in (["-h"], ["channel", "-h"], ["lab", "provenance", "-h"], ["device", "lock", "-h"]):
        result = runner.invoke(app, arguments)
        assert result.exit_code == 0, arguments
        assert "Usage:" in result.output, arguments


def test_every_argument_has_help():
    missing = [
        f"{path} {parameter.name}"
        for path, command in _command_tree()
        for parameter in command.params
        if isinstance(parameter, click.Argument) and not (getattr(parameter, "help", None) or "").strip()
    ]
    assert missing == []


def test_every_command_has_help():
    missing = [path for path, command in _command_tree() if not (command.help or "").strip()]
    assert missing == []


def test_every_option_has_help():
    missing = [
        f"{path} {'/'.join(parameter.opts)}"
        for path, command in _command_tree()
        for parameter in command.params
        if isinstance(parameter, click.Option) and not (parameter.help or "").strip()
    ]
    assert missing == []


def test_host_filter_has_no_short_form():
    root = typer.main.get_command(app)
    host_option = next(parameter for parameter in root.params if "--host" in parameter.opts)
    assert host_option.opts == ["--host"]
