import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from netaudio.cli import app
from netaudio.commands.ddm import cli as ddm_cli

runner = CliRunner()


@pytest.fixture(autouse=True)
def _restore_cli_state():
    from netaudio.cli import state

    snapshot = dict(vars(state))
    yield
    vars(state).clear()
    vars(state).update(snapshot)


def _json(result):
    assert result.exit_code in (0, 1), result.output
    return json.loads(result.stdout)


def test_config_path_json():
    result = runner.invoke(app, ["-j", "config", "path"])
    assert result.exit_code == 0
    assert set(_json(result)) == {"path"}


def test_lock_key_get_json(monkeypatch):
    monkeypatch.setattr(
        "netaudio.common.config_loader.get_config_value",
        lambda key: ("0123456789abcdef0123456789abcdef", "config.toml"),
    )
    result = runner.invoke(app, ["-j", "lock", "key", "get"])
    assert _json(result) == {"device_lock_key": "0123456789abcdef0123456789abcdef"}


def test_daemon_tls_json_reports_null_when_unconfigured(monkeypatch):
    monkeypatch.setattr("netaudio.daemon.http.tls.daemon_tls_settings", lambda: None)
    result = runner.invoke(app, ["-j", "daemon", "tls"])
    assert result.exit_code == 0
    assert _json(result) is None


def test_virtual_status_json_when_not_running(monkeypatch):
    monkeypatch.setattr("netaudio.commands.virtual._read_process_record", lambda: None)
    result = runner.invoke(app, ["-j", "virtual", "status"])
    assert result.exit_code == 1
    assert _json(result) == {"running": False}


def test_meter_status_json_when_nothing_is_metered():
    with (
        patch("netaudio.daemon.client.meter_status_from_daemon", AsyncMock(return_value={})),
        patch.object(
            __import__("netaudio.dante.application", fromlist=["DanteApplication"]).DanteApplication,
            "startup",
            AsyncMock(),
        ),
        patch.object(
            __import__("netaudio.dante.application", fromlist=["DanteApplication"]).DanteApplication,
            "shutdown",
            AsyncMock(),
        ),
    ):
        result = runner.invoke(app, ["-j", "meter", "status"])
    assert result.exit_code == 0
    assert _json(result) == {}


def test_daemon_status_json_when_the_daemon_is_not_running(monkeypatch):
    monkeypatch.setattr("netaudio.commands.server._port_in_use", lambda port: False)
    monkeypatch.setattr("netaudio.commands.server.service_install.is_installed", lambda: False)
    result = runner.invoke(app, ["-j", "daemon", "status"])
    assert result.exit_code == 1
    payload = _json(result)
    assert payload["running"] is False
    assert payload["boot_service"]["installed"] is False


def test_ddm_status_rows_show_server_errors_when_the_managed_api_is_unreachable():
    result = {
        "state": "degraded",
        "last_error": None,
        "servers": {"lab": {"state": "degraded", "last_error": "Managed API transport failed: timed out"}},
    }
    rows = dict(ddm_cli._status_rows(result))
    assert rows["State"] == "degraded"
    assert rows["Last Error"] == "lab: Managed API transport failed: timed out"


def test_ddm_devices_reports_an_unreachable_managed_api_instead_of_no_devices(monkeypatch):
    monkeypatch.setattr(ddm_cli, "_active_context", lambda: None)
    monkeypatch.setattr(ddm_cli, "get_ddm_devices_from_daemon", AsyncMock(return_value={}))
    monkeypatch.setattr(
        ddm_cli,
        "get_ddm_status_from_daemon",
        AsyncMock(return_value={"state": "degraded", "servers": {"lab": {"last_error": "Host is down"}}}),
    )
    result = runner.invoke(app, ["ddm", "devices"])
    assert result.exit_code != 0
    assert "unreachable" in result.output
    assert "Host is down" in result.output
    assert "has not seen any devices" not in result.output


@pytest.mark.parametrize("state", ["dead", "different"])
def test_virtual_status_json_for_stale_records(monkeypatch, state):
    record = SimpleNamespace(pid=4242, name="virtual")
    monkeypatch.setattr("netaudio.commands.virtual._read_process_record", lambda: record)
    monkeypatch.setattr("netaudio.commands.virtual._ownership_state", lambda _record: state)
    monkeypatch.setattr("netaudio.commands.virtual._remove_process_record", lambda _record: None)
    result = runner.invoke(app, ["-j", "virtual", "status"])
    assert result.exit_code == 1
    assert _json(result) == {"running": False}
