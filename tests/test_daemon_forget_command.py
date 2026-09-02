from unittest.mock import AsyncMock

from typer.testing import CliRunner

from netaudio.commands import server as server_commands

runner = CliRunner()


def test_forget_requires_a_target():
    result = runner.invoke(server_commands.app, ["forget"])

    assert result.exit_code == 1
    assert "Error: give a device name, --offline, or --emulated." in result.output


def test_forget_rejects_name_combined_with_selection_flags():
    result = runner.invoke(server_commands.app, ["forget", "ghost", "--offline"])

    assert result.exit_code == 1
    assert "not both" in result.output


def test_forget_reports_when_daemon_is_not_running(monkeypatch):
    monkeypatch.setattr(server_commands, "_port_in_use", lambda port: False)
    request = AsyncMock()
    monkeypatch.setattr(server_commands, "forget_devices_on_daemon", request)

    result = runner.invoke(server_commands.app, ["forget", "--emulated"])

    assert result.exit_code == 1
    assert "Daemon is not running." in result.output
    request.assert_not_awaited()


def test_forget_prints_each_forgotten_device(monkeypatch):
    monkeypatch.setattr(server_commands, "_port_in_use", lambda port: True)
    request = AsyncMock(
        return_value=(
            200,
            {
                "forgotten": [
                    {
                        "ipv4": "192.168.1.249",
                        "kind": "emulated",
                        "name": "netaudio-page-probe",
                        "online": False,
                        "server_name": "netaudio-page-probe.local.",
                    },
                    {
                        "ipv4": "192.168.1.107",
                        "kind": "emulated",
                        "name": "studio-media-b",
                        "online": False,
                        "server_name": "www.local.",
                    },
                ]
            },
        )
    )
    monkeypatch.setattr(server_commands, "forget_devices_on_daemon", request)

    result = runner.invoke(server_commands.app, ["forget", "--emulated", "--offline"])

    assert result.exit_code == 0
    assert result.output.splitlines() == [
        "Forgot netaudio-page-probe (netaudio-page-probe.local., 192.168.1.249, emulated, offline)",
        "Forgot studio-media-b (www.local., 192.168.1.107, emulated, offline)",
    ]
    request.assert_awaited_once_with(device_name=None, emulated=True, offline=True)


def test_forget_reports_empty_selection(monkeypatch):
    monkeypatch.setattr(server_commands, "_port_in_use", lambda port: True)
    monkeypatch.setattr(server_commands, "forget_devices_on_daemon", AsyncMock(return_value=(200, {"forgotten": []})))

    result = runner.invoke(server_commands.app, ["forget", "--offline"])

    assert result.exit_code == 0
    assert result.output.strip() == "No cached devices matched."


def test_forget_reports_unknown_device(monkeypatch):
    monkeypatch.setattr(server_commands, "_port_in_use", lambda port: True)
    request = AsyncMock(return_value=(404, {"error": "device not found"}))
    monkeypatch.setattr(server_commands, "forget_devices_on_daemon", request)

    result = runner.invoke(server_commands.app, ["forget", "ghost"])

    assert result.exit_code == 1
    assert "Error: ghost is not in the daemon cache." in result.output
    request.assert_awaited_once_with(device_name="ghost", emulated=False, offline=False)


def test_forget_explains_when_daemon_predates_the_route(monkeypatch):
    monkeypatch.setattr(server_commands, "_port_in_use", lambda port: True)
    monkeypatch.setattr(
        server_commands, "forget_devices_on_daemon", AsyncMock(return_value=(404, {"error": "not found"}))
    )

    result = runner.invoke(server_commands.app, ["forget", "--offline"])

    assert result.exit_code == 1
    assert "does not support forget" in result.output
