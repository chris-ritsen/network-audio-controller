from contextlib import asynccontextmanager
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
        self.factory_reset_calls = 0

    async def reboot(self):
        self.calls += 1

    async def factory_reset(self):
        self.factory_reset_calls += 1


def _fake_device(name):
    return SimpleNamespace(name=name, ipv4="192.0.2.10", operations=RebootRecorder())


def _install_fake_discovery(monkeypatch, devices):
    async def discover():
        return devices

    async def populate(_devices):
        return None

    monkeypatch.setattr(device_commands, "_discover", discover)
    monkeypatch.setattr(device_commands, "_populate_controls", populate)


class ClearConfigurationSender:
    def __init__(self):
        self.calls = []

    async def clear_configuration(self, device_ip_address, preserve_internet_protocol_settings):
        self.calls.append((str(device_ip_address), preserve_internet_protocol_settings))
        return {
            "available_actions_mask": 3,
            "action_result_code": 2 if preserve_internet_protocol_settings else 1,
        }


def _install_fake_command_context(monkeypatch, devices):
    sender = ClearConfigurationSender()

    @asynccontextmanager
    async def command_context():
        yield devices, sender

    monkeypatch.setattr(device_commands, "_command_context", command_context)
    return sender


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


def test_factory_reset_requires_exact_device_name_confirmation(monkeypatch):
    device = _fake_device("One")
    _install_fake_discovery(monkeypatch, {"one.local.": device})

    result = runner.invoke(device_commands.app, ["factory-reset", "--confirm", "one"])

    assert result.exit_code == 1
    assert "must exactly match 'One'" in result.output
    assert device.operations.factory_reset_calls == 0


def test_factory_reset_refuses_multiple_matches(monkeypatch):
    devices = {"one.local.": _fake_device("One"), "two.local.": _fake_device("Two")}
    _install_fake_discovery(monkeypatch, devices)

    result = runner.invoke(device_commands.app, ["factory-reset", "--confirm", "One"])

    assert result.exit_code == 1
    assert "multiple devices matched" in result.output
    assert all(device.operations.factory_reset_calls == 0 for device in devices.values())


def test_factory_reset_sends_after_exact_confirmation(monkeypatch):
    device = _fake_device("One")
    _install_fake_discovery(monkeypatch, {"one.local.": device})

    result = runner.invoke(device_commands.app, ["factory-reset", "--confirm", "One"])

    assert result.exit_code == 0
    assert "Factory reset requested: One" in result.output
    assert device.operations.factory_reset_calls == 1


def test_clear_configuration_requires_exact_device_name_confirmation(monkeypatch):
    sender = _install_fake_command_context(monkeypatch, {"one.local.": _fake_device("One")})

    result = runner.invoke(
        device_commands.app,
        ["clear-configuration", "--mode", "all", "--confirm", "one"],
    )

    assert result.exit_code == 1
    assert "must exactly match 'One'" in result.output
    assert sender.calls == []


@pytest.mark.parametrize(
    ("mode", "preserve_internet_protocol_settings", "result_code"),
    [("all", False, 1), ("preserve-network", True, 2)],
)
def test_clear_configuration_sends_one_verified_mode(
    monkeypatch,
    mode,
    preserve_internet_protocol_settings,
    result_code,
):
    sender = _install_fake_command_context(monkeypatch, {"one.local.": _fake_device("One")})

    result = runner.invoke(
        device_commands.app,
        ["clear-configuration", "--mode", mode, "--confirm", "One"],
    )

    assert result.exit_code == 0
    assert f"result {result_code}, mode {mode}" in result.output
    assert sender.calls == [("192.0.2.10", preserve_internet_protocol_settings)]


def test_clear_configuration_refuses_multiple_matches(monkeypatch):
    sender = _install_fake_command_context(
        monkeypatch,
        {"one.local.": _fake_device("One"), "two.local.": _fake_device("Two")},
    )

    result = runner.invoke(
        device_commands.app,
        ["clear-configuration", "--mode", "all", "--confirm", "One"],
    )

    assert result.exit_code == 1
    assert "multiple devices matched" in result.output
    assert sender.calls == []


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
