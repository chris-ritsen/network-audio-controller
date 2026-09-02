import pytest
import typer
from netaudio.commands.device import cli as device_commands
from netaudio.commands.device.cli import ClearConfigurationMode
from netaudio.commands.firmware.cramfs import _prepare_rootfs_output

from tests.cli_test_support import FakeApplication, FakeDevice, invoke


def _fake_device(name):
    return FakeDevice(name)


def _application(devices):
    return FakeApplication(devices)


def _operations(application, name):
    return [sent.operation for sent in application.sent if sent.operation == name]


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


def test_reboot_refuses_multiple_matches_without_all():
    application = _application({"one.local.": _fake_device("One"), "two.local.": _fake_device("Two")})

    result = invoke(device_commands.run_reboot, application, application.devices, False)

    assert result.exit_code == 1
    assert "multiple devices matched" in result.output
    assert application.sent == []


def test_reboot_allows_one_match_without_all():
    device = _fake_device("One")
    application = _application({"one.local.": device})

    result = invoke(device_commands.run_reboot, application, application.devices, False)

    assert result.exit_code == 0
    assert _operations(application, "reboot") == ["reboot"]
    assert application.sent[0].device is device
    assert "Reboot requested: One" in result.output


def test_reboot_all_is_explicit_and_reboots_every_match():
    application = _application({"one.local.": _fake_device("One"), "two.local.": _fake_device("Two")})

    result = invoke(device_commands.run_reboot, application, application.devices, True)

    assert result.exit_code == 0
    assert _operations(application, "reboot") == ["reboot", "reboot"]
    assert "Reboot requested: One" in result.output
    assert "Reboot requested: Two" in result.output


def test_factory_reset_requires_exact_device_name_confirmation():
    application = _application({"one.local.": _fake_device("One")})

    result = invoke(device_commands.run_factory_reset, application, application.devices, "one")

    assert result.exit_code == 1
    assert "must exactly match 'One'" in result.output
    assert application.sent == []


def test_factory_reset_refuses_multiple_matches():
    application = _application({"one.local.": _fake_device("One"), "two.local.": _fake_device("Two")})

    result = invoke(device_commands.run_factory_reset, application, application.devices, "One")

    assert result.exit_code == 1
    assert "multiple devices matched" in result.output
    assert application.sent == []


def test_factory_reset_sends_after_exact_confirmation():
    application = _application({"one.local.": _fake_device("One")})

    result = invoke(device_commands.run_factory_reset, application, application.devices, "One")

    assert result.exit_code == 0
    assert "Factory reset requested: One" in result.output
    assert _operations(application, "factory_reset") == ["factory_reset"]


def test_clear_configuration_requires_exact_device_name_confirmation():
    application = _application({"one.local.": _fake_device("One")})

    result = invoke(
        device_commands.run_clear_configuration,
        application,
        application.devices,
        ClearConfigurationMode.ALL,
        "one",
    )

    assert result.exit_code == 1
    assert "must exactly match 'One'" in result.output
    assert application.sent == []


@pytest.mark.parametrize(
    ("mode", "preserve_internet_protocol_settings", "result_code"),
    [
        (ClearConfigurationMode.ALL, False, 1),
        (ClearConfigurationMode.PRESERVE_INTERNET_PROTOCOL_SETTINGS, True, 2),
    ],
)
def test_clear_configuration_sends_one_verified_mode(mode, preserve_internet_protocol_settings, result_code):
    device = _fake_device("One")
    application = _application({"one.local.": device})

    result = invoke(device_commands.run_clear_configuration, application, application.devices, mode, "One")

    assert result.exit_code == 0
    assert f"result {result_code}, mode {mode.value}" in result.output
    assert [(sent.operation, sent.device, sent.arguments) for sent in application.sent] == [
        ("clear_configuration", device, (preserve_internet_protocol_settings,))
    ]


def test_clear_configuration_refuses_multiple_matches():
    application = _application({"one.local.": _fake_device("One"), "two.local.": _fake_device("Two")})

    result = invoke(
        device_commands.run_clear_configuration,
        application,
        application.devices,
        ClearConfigurationMode.ALL,
        "One",
    )

    assert result.exit_code == 1
    assert "multiple devices matched" in result.output
    assert application.sent == []


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
