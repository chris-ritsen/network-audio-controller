import pytest
import typer
from typer.testing import CliRunner

from netaudio.cli import app
from netaudio.cli_support.context import subcommands

runner = CliRunner()


def _walk(command, path):
    yield path, command
    for name in sorted(subcommands(command)):
        yield from _walk(subcommands(command)[name], f"{path} {name}")


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
    if not subcommands(command) and any(parameter.required for parameter in command.params)
]


@pytest.mark.parametrize("arguments", REQUIRED_PARAMETER_COMMANDS, ids=lambda arguments: " ".join(arguments))
def test_every_bare_command_with_required_parameters_displays_help(arguments):
    result = runner.invoke(app, arguments)

    assert result.exit_code == 2
    assert "Usage:" in result.output
    assert "Missing" not in result.output
