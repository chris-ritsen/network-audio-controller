import asyncio
import os
import plistlib
import signal
import socket
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

from netaudio.commands import server as server_commands
from netaudio.daemon import server, service_install


command_runner = CliRunner()


class _FakeRedis:
    def __init__(self, *, ping_error=None, config_error=None):
        self.ping = AsyncMock(side_effect=ping_error)
        self.config_set = AsyncMock(side_effect=config_error)
        self.aclose = AsyncMock()


@pytest.mark.asyncio
async def test_failed_redis_connection_is_closed_and_disabled(monkeypatch):
    candidate = _FakeRedis(ping_error=ConnectionError("redis unavailable"))
    monkeypatch.setattr(server, "aioredis", SimpleNamespace(Redis=lambda **_kwargs: candidate))
    daemon = object.__new__(server.NetaudioDaemon)
    daemon._redis = None

    await daemon._connect_redis()

    candidate.ping.assert_awaited_once()
    candidate.aclose.assert_awaited_once()
    assert daemon._redis is None


@pytest.mark.asyncio
async def test_redis_config_rejection_keeps_usable_connection(monkeypatch):
    candidate = _FakeRedis(config_error=PermissionError("CONFIG is disabled"))
    monkeypatch.setattr(server, "aioredis", SimpleNamespace(Redis=lambda **_kwargs: candidate))
    daemon = object.__new__(server.NetaudioDaemon)
    daemon._redis = None

    await daemon._connect_redis()

    candidate.ping.assert_awaited_once()
    candidate.config_set.assert_awaited_once_with("notify-keyspace-events", "Kgh$")
    candidate.aclose.assert_not_awaited()
    assert daemon._redis is candidate


@pytest.mark.asyncio
async def test_run_daemon_removes_process_signal_handlers(monkeypatch):
    loop = asyncio.get_running_loop()
    callbacks = {}
    removed = []

    def add_signal_handler(sig, callback):
        callbacks[sig] = callback

    def remove_signal_handler(sig):
        removed.append(sig)
        return True

    monkeypatch.setattr(loop, "add_signal_handler", add_signal_handler)
    monkeypatch.setattr(loop, "remove_signal_handler", remove_signal_handler)
    monkeypatch.setattr(server.sys, "platform", "linux")

    fake = SimpleNamespace(request_shutdown=MagicMock(), stop=AsyncMock())

    async def start():
        callbacks[signal.SIGTERM]()

    fake.start = AsyncMock(side_effect=start)
    monkeypatch.setattr(server, "NetaudioDaemon", lambda **_kwargs: fake)

    await server.run_daemon()

    assert set(callbacks) == {signal.SIGTERM, signal.SIGINT}
    assert removed == [signal.SIGTERM, signal.SIGINT]
    fake.start.assert_awaited_once()
    fake.request_shutdown.assert_called_once()
    fake.stop.assert_awaited_once()


@pytest.mark.skipif(sys.platform != "linux", reason="systemd abstract notify sockets are Linux-specific")
def test_sd_notify_sends_exact_state_to_notify_socket(monkeypatch):
    name = f"netaudio-test-{os.getpid()}-{id(monkeypatch)}"
    receiver = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    receiver.settimeout(1)
    receiver.bind(f"\0{name}")
    monkeypatch.setenv("NOTIFY_SOCKET", f"@{name}")
    try:
        server._sd_notify("READY=1\nSTATUS=Discovering devices...")
        assert receiver.recv(1024) == b"READY=1\nSTATUS=Discovering devices..."
    finally:
        receiver.close()


def test_systemd_unit_quotes_exec_path_and_escapes_specifiers():
    unit = service_install.generate_systemd_unit('/opt/Network Audio/%i/netaudio "dev"')

    assert 'ExecStart="/opt/Network Audio/%%i/netaudio \\"dev\\"" daemon run' in unit
    assert "Type=notify" in unit
    assert f"{service_install.MANAGED_MARKER}=true" in unit


def test_launchd_plist_uses_argument_array_and_failure_restart(monkeypatch):
    monkeypatch.setattr(service_install, "spawn_log_path", lambda: service_install.Path("/tmp/netaudio daemon.log"))

    payload = plistlib.loads(service_install.generate_launchd_plist("/Applications/Net Audio/netaudio").encode())

    assert payload["ProgramArguments"] == ["/Applications/Net Audio/netaudio", "daemon", "run"]
    assert payload["EnvironmentVariables"]["PATH"].split(":")[:2] == [
        str(service_install.Path.home() / ".cargo" / "bin"),
        str(service_install.Path.home() / ".local" / "bin"),
    ]
    assert payload["KeepAlive"] == {"SuccessfulExit": False}
    assert payload["StandardOutPath"] == "/tmp/netaudio daemon.log"


def test_launchd_force_install_reloads_running_job(monkeypatch, tmp_path):
    service_path = tmp_path / "com.netaudio.daemon.plist"
    service_path.write_text("old")
    bootout = MagicMock(return_value=SimpleNamespace(returncode=0, stderr=""))
    bootstrap = MagicMock(return_value=SimpleNamespace(returncode=0, stderr=""))
    monkeypatch.setattr(service_install, "platform_name", lambda: "launchd")
    monkeypatch.setattr(service_install, "service_file_path", lambda: service_path)
    monkeypatch.setattr(service_install, "generate_service_file", lambda: "new")
    monkeypatch.setattr(service_install, "launchd_loaded", lambda: True)
    monkeypatch.setattr(service_install, "launchd_bootout", bootout)
    monkeypatch.setattr(service_install, "launchd_bootstrap", bootstrap)

    result = command_runner.invoke(server_commands.app, ["install", "--force"])

    assert result.exit_code == 0
    assert service_path.read_text() == "new"
    bootout.assert_called_once_with()
    bootstrap.assert_called_once_with()


def test_launchd_no_start_unloads_existing_job_without_reloading(monkeypatch, tmp_path):
    service_path = tmp_path / "com.netaudio.daemon.plist"
    service_path.write_text("old")
    bootout = MagicMock(return_value=SimpleNamespace(returncode=0, stderr=""))
    bootstrap = MagicMock(return_value=SimpleNamespace(returncode=0, stderr=""))
    monkeypatch.setattr(service_install, "platform_name", lambda: "launchd")
    monkeypatch.setattr(service_install, "service_file_path", lambda: service_path)
    monkeypatch.setattr(service_install, "generate_service_file", lambda: "new")
    monkeypatch.setattr(service_install, "launchd_loaded", lambda: True)
    monkeypatch.setattr(service_install, "launchd_bootout", bootout)
    monkeypatch.setattr(service_install, "launchd_bootstrap", bootstrap)

    result = command_runner.invoke(server_commands.app, ["install", "--force", "--no-start"])

    assert result.exit_code == 0
    assert "without loading it" in result.output
    bootout.assert_called_once_with()
    bootstrap.assert_not_called()
