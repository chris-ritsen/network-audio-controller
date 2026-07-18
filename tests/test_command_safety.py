from types import SimpleNamespace

import pytest
import typer
from typer.testing import CliRunner

from netaudio.commands import device as device_commands
from netaudio.commands.firmware import _prepare_rootfs_output


runner = CliRunner()


class RebootRecorder:
    def __init__(self):
        self.calls = 0

    async def reboot(self):
        self.calls += 1


def _fake_device(name):
    return SimpleNamespace(name=name, operations=RebootRecorder())


def _install_fake_discovery(monkeypatch, devices):
    async def discover():
        return devices

    async def populate(_devices):
        return None

    monkeypatch.setattr(device_commands, "_discover", discover)
    monkeypatch.setattr(device_commands, "_populate_controls", populate)


@pytest.fixture(autouse=True)
def reset_cli_filters():
    from netaudio.cli import state

    original = (list(state.names), list(state.hosts), list(state.server_names), list(state.macs))
    state.names = []
    state.hosts = []
    state.server_names = []
    state.macs = []
    try:
        yield
    finally:
        state.names, state.hosts, state.server_names, state.macs = original


def test_reboot_refuses_multiple_matches_without_all(monkeypatch):
    devices = {"one.local.": _fake_device("One"), "two.local.": _fake_device("Two")}
    _install_fake_discovery(monkeypatch, devices)

    result = runner.invoke(device_commands.app, ["reboot"])

    assert result.exit_code == 1
    assert "multiple devices matched" in result.output
    assert all(device.operations.calls == 0 for device in devices.values())


def test_reboot_allows_one_match_without_all(monkeypatch):
    device = _fake_device("One")
    _install_fake_discovery(monkeypatch, {"one.local.": device})

    result = runner.invoke(device_commands.app, ["reboot"])

    assert result.exit_code == 0
    assert device.operations.calls == 1
    assert "Reboot requested: One" in result.output


def test_reboot_all_is_explicit_and_reboots_every_match(monkeypatch):
    devices = {"one.local.": _fake_device("One"), "two.local.": _fake_device("Two")}
    _install_fake_discovery(monkeypatch, devices)

    result = runner.invoke(device_commands.app, ["reboot", "--all"])

    assert result.exit_code == 0
    assert all(device.operations.calls == 1 for device in devices.values())
    assert "Reboot requested: One" in result.output
    assert "Reboot requested: Two" in result.output


def test_rootfs_output_refuses_existing_path_without_force(tmp_path):
    output = tmp_path / "rootfs"
    output.mkdir()
    sentinel = output / "keep-me"
    sentinel.write_text("important")

    with pytest.raises(typer.Exit):
        _prepare_rootfs_output(output)

    assert sentinel.read_text() == "important"


def test_rootfs_output_force_replaces_existing_directory(tmp_path):
    output = tmp_path / "rootfs"
    output.mkdir()
    (output / "stale").write_text("old")

    _prepare_rootfs_output(output, force=True)

    assert output.is_dir()
    assert list(output.iterdir()) == []


def test_rootfs_output_force_refuses_current_directory(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    sentinel = tmp_path / "keep-me"
    sentinel.write_text("important")

    with pytest.raises(typer.Exit):
        _prepare_rootfs_output(tmp_path, force=True)

    assert sentinel.read_text() == "important"


def test_rootfs_output_force_refuses_parent_of_current_directory(monkeypatch, tmp_path):
    output = tmp_path / "project"
    working_directory = output / "nested"
    working_directory.mkdir(parents=True)
    sentinel = output / "keep-me"
    sentinel.write_text("important")
    monkeypatch.chdir(working_directory)

    with pytest.raises(typer.Exit):
        _prepare_rootfs_output(output, force=True)

    assert sentinel.read_text() == "important"
