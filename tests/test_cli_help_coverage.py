import click
import pytest
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


COMMAND_TREE = _command_tree()


@pytest.mark.parametrize(
    "arguments",
    [[] if path == "netaudio" else path.removeprefix("netaudio ").split() for path, _ in COMMAND_TREE],
    ids=lambda arguments: " ".join(arguments) or "netaudio",
)
def test_dash_h_displays_help_for_every_command(arguments):
    result = runner.invoke(app, [*arguments, "-h"])

    assert result.exit_code == 0, result.output
    assert "Usage:" in result.output


REQUIRED_PARAMETER_COMMANDS = [
    path.removeprefix("netaudio ").split()
    for path, command in COMMAND_TREE
    if not isinstance(command, click.Group) and any(parameter.required for parameter in command.params)
]


@pytest.mark.parametrize("arguments", REQUIRED_PARAMETER_COMMANDS, ids=lambda arguments: " ".join(arguments))
def test_every_bare_command_with_required_parameters_displays_help(arguments):
    result = runner.invoke(app, arguments)

    assert result.exit_code == 2
    assert "Usage:" in result.output
    assert "Missing" not in result.output


@pytest.mark.parametrize(
    "arguments",
    [
        ["config", "sample-rate"],
        ["config", "encoding"],
        ["config", "latency"],
        ["config", "aes67"],
        ["device", "lock"],
    ],
)
def test_removed_command_paths_are_not_registered(arguments):
    result = runner.invoke(app, arguments)

    assert result.exit_code == 2
    assert "No such command" in result.output


def test_every_argument_has_help():
    missing = [
        f"{path} {parameter.name}"
        for path, command in COMMAND_TREE
        for parameter in command.params
        if isinstance(parameter, click.Argument) and not (getattr(parameter, "help", None) or "").strip()
    ]
    assert missing == []


def test_every_command_has_help():
    missing = [path for path, command in COMMAND_TREE if not (command.help or "").strip()]
    assert missing == []


def test_every_option_has_help():
    missing = [
        f"{path} {'/'.join(parameter.opts)}"
        for path, command in COMMAND_TREE
        for parameter in command.params
        if isinstance(parameter, click.Option) and not (parameter.help or "").strip()
    ]
    assert missing == []


def test_host_filter_has_no_short_form():
    root = typer.main.get_command(app)
    host_option = next(parameter for parameter in root.params if "--host" in parameter.opts)
    assert host_option.opts == ["--host"]
