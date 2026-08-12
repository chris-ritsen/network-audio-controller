import subprocess
import sys
import threading
from contextlib import nullcontext
from subprocess import CompletedProcess

import pytest
from typer.testing import CliRunner

from netaudio.commands import virtual


runner = CliRunner()


def _use_runtime(monkeypatch, tmp_path):
    monkeypatch.setattr(virtual, "RUNTIME_DIRECTORY", str(tmp_path))
    monkeypatch.setattr(virtual, "PIDFILE", str(tmp_path / "virtual.pid"))
    monkeypatch.setattr(virtual, "LOGFILE", str(tmp_path / "virtual.log"))
    monkeypatch.setattr(virtual, "START_LOCKFILE", str(tmp_path / "virtual-start.lock"))


def test_restart_passes_real_defaults_instead_of_typer_option_objects(monkeypatch):
    recorded_calls = []
    monkeypatch.setattr(virtual, "_stop_virtual", lambda **_keyword_arguments: False)
    monkeypatch.setattr(
        virtual,
        "_start_virtual",
        lambda **keyword_arguments: recorded_calls.append(keyword_arguments),
    )

    result = runner.invoke(virtual.app, ["restart"])

    assert result.exit_code == 0
    assert recorded_calls == [
        {
            "name": "netaudio-virtual",
            "model": "netaudio",
            "tx": 2,
            "rx": 2,
            "sample_rate": 48000,
            "interface": None,
            "foreground": False,
        }
    ]


def test_background_start_only_reports_success_after_readiness(monkeypatch, tmp_path, capsys):
    _use_runtime(monkeypatch, tmp_path)
    spawned_processes = []

    class SpawnedProcess:
        pid = 4242

        def poll(self):
            return None

    def spawn_process(command, **keyword_arguments):
        spawned_processes.append((command, keyword_arguments))
        return SpawnedProcess()

    monkeypatch.setattr(virtual, "_existing_managed_process", lambda: None)
    monkeypatch.setattr(virtual.subprocess, "Popen", spawn_process)
    monkeypatch.setattr(
        virtual,
        "_wait_for_startup",
        lambda process, readiness_listener, token: {
            "status": "ready",
            "pid": process.pid,
            "token": token,
            "mac": "02:00:00:00:00:01",
            "ports": [4440],
        },
    )

    virtual._start_background_claimed(
        name="Test",
        model="Model",
        tx=2,
        rx=2,
        sample_rate=48000,
        interface=None,
    )

    output = capsys.readouterr().out
    assert "Started virtual device 'Test' (PID 4242)" in output
    command = spawned_processes[0][0]
    token = command[command.index("--ownership-token") + 1]
    assert len(token) == 32
    assert "--ready-file" not in command
    readiness_port = int(command[command.index("--readiness-port") + 1])
    assert 0 < readiness_port <= 65535


def test_background_start_does_not_claim_success_when_child_fails(monkeypatch, tmp_path, capsys):
    _use_runtime(monkeypatch, tmp_path)
    terminated_processes = []

    class SpawnedProcess:
        pid = 4242

        def poll(self):
            return None

    monkeypatch.setattr(virtual, "_existing_managed_process", lambda: None)
    monkeypatch.setattr(
        virtual.subprocess,
        "Popen",
        lambda *_positional_arguments, **_keyword_arguments: SpawnedProcess(),
    )
    monkeypatch.setattr(
        virtual,
        "_wait_for_startup",
        lambda *_arguments: (_ for _ in ()).throw(virtual.VirtualLifecycleError("bind failed")),
    )
    monkeypatch.setattr(
        virtual,
        "_terminate_spawned_process",
        lambda process: terminated_processes.append(process.pid),
    )

    with pytest.raises(virtual.VirtualLifecycleError, match="bind failed"):
        virtual._start_background_claimed(
            name="Test",
            model="Model",
            tx=2,
            rx=2,
            sample_rate=48000,
            interface=None,
        )

    assert terminated_processes == [4242]
    assert "Started" not in capsys.readouterr().out


def test_startup_wait_receives_readiness_without_polling():
    token = "c" * 32

    class RunningProcess:
        pid = 4242

        def __init__(self):
            self.exit_event = threading.Event()

        def wait(self):
            self.exit_event.wait()
            return 0

    process = RunningProcess()
    with virtual._create_readiness_listener() as readiness_listener:
        readiness_port = int(readiness_listener.getsockname()[1])
        readiness_message = {
            "status": "ready",
            "token": token,
            "pid": process.pid,
            "mac": "02:00:00:00:00:01",
            "ports": [4440],
        }

        def send_readiness() -> None:
            virtual._send_readiness_message(readiness_port, readiness_message)

        sender = threading.Thread(target=send_readiness)
        sender.start()
        assert virtual._wait_for_startup(process, readiness_listener, token) == readiness_message
        sender.join()
        process.exit_event.set()


def test_startup_wait_reports_child_exit_without_timeout_delay():
    token = "e" * 32

    class FailedProcess:
        pid = 4242

        def wait(self):
            return 7

    with virtual._create_readiness_listener() as readiness_listener:
        with pytest.raises(virtual.VirtualLifecycleError, match="status 7"):
            virtual._wait_for_startup(FailedProcess(), readiness_listener, token)


def test_startup_claim_excludes_a_concurrent_start(monkeypatch, tmp_path):
    _use_runtime(monkeypatch, tmp_path)

    with virtual._startup_claim():
        with pytest.raises(virtual.VirtualLifecycleError, match="already in progress"):
            with virtual._startup_claim():
                pytest.fail("concurrent start acquired the claim")


def test_process_command_uses_untruncated_ps_output_for_token(monkeypatch):
    token = "d" * 32
    long_command = f"/a/{'very-long/' * 80}python -m netaudio virtual start --ownership-token {token}"
    recorded_calls = []

    def run(command, **_keyword_arguments):
        recorded_calls.append(command)
        return CompletedProcess(command, 0, stdout=long_command, stderr="")

    monkeypatch.setattr(virtual.os.path, "exists", lambda path: False)
    monkeypatch.setattr(virtual.subprocess, "run", run)
    monkeypatch.setattr(virtual, "_pid_exists", lambda pid: True)

    record = virtual.ProcessRecord(pid=1234, token=token)
    assert virtual._ownership_state(record) == "owned"
    if virtual.os.name != "nt":
        assert recorded_calls[0][:2] == ["ps", "-ww"]


def test_windows_pid_probe_never_uses_os_kill(monkeypatch):
    probed_process_ids = []
    monkeypatch.setattr(virtual.os, "name", "nt")
    monkeypatch.setattr(
        virtual,
        "_windows_pid_exists",
        lambda pid: probed_process_ids.append(pid) or True,
    )
    monkeypatch.setattr(
        virtual.os,
        "kill",
        lambda *_arguments: pytest.fail("os.kill(pid, 0) is destructive on Windows"),
    )

    assert virtual._pid_exists(1234) is True
    assert probed_process_ids == [1234]


def test_windows_stop_requests_ctrl_break_before_forceful_termination(monkeypatch):
    sent_signals = []
    monkeypatch.setattr(virtual.os, "name", "nt")
    monkeypatch.setattr(virtual.signal, "CTRL_BREAK_EVENT", 123, raising=False)
    monkeypatch.setattr(
        virtual.os,
        "kill",
        lambda pid, sent_signal: sent_signals.append((pid, sent_signal)),
    )

    virtual._request_process_stop(4321)

    assert sent_signals == [(4321, 123)]


def test_stop_never_signals_a_reused_pid(monkeypatch, tmp_path):
    _use_runtime(monkeypatch, tmp_path)
    record = virtual.ProcessRecord(pid=1234, token="a" * 32, name="old")
    virtual._write_process_record(record)
    monkeypatch.setattr(virtual, "_pid_exists", lambda pid: True)
    monkeypatch.setattr(virtual, "_process_command", lambda pid: "/usr/bin/unrelated-service")
    monkeypatch.setattr(
        virtual.os,
        "kill",
        lambda *_arguments: pytest.fail("a reused PID must not be signalled"),
    )

    with pytest.raises(virtual.VirtualLifecycleError, match="belongs to another process"):
        virtual._stop_virtual()

    assert not (tmp_path / "virtual.pid").exists()


def test_stop_waits_for_exit_before_reporting_success(monkeypatch, tmp_path, capsys):
    _use_runtime(monkeypatch, tmp_path)
    record = virtual.ProcessRecord(pid=1234, token="b" * 32, name="test")
    virtual._write_process_record(record)
    sent_signals = []
    monkeypatch.setattr(virtual, "_ownership_state", lambda current_record: "owned")
    monkeypatch.setattr(virtual, "_process_exit_waiter", lambda pid: nullcontext(lambda timeout: True))
    monkeypatch.setattr(
        virtual.os,
        "kill",
        lambda pid, sent_signal: sent_signals.append((pid, sent_signal)),
    )

    assert virtual._stop_virtual() is True

    assert sent_signals == [(1234, virtual.signal.SIGTERM)]
    assert not (tmp_path / "virtual.pid").exists()
    assert "Stopped virtual device (PID 1234)" in capsys.readouterr().out


def test_status_is_nonzero_when_process_ownership_cannot_be_verified(monkeypatch):
    record = virtual.ProcessRecord(pid=1234, token=None)
    monkeypatch.setattr(virtual, "_read_process_record", lambda: record)
    monkeypatch.setattr(virtual, "_ownership_state", lambda current_record: "unknown")

    result = runner.invoke(virtual.app, ["status"])

    assert result.exit_code == 1
    assert "cannot verify ownership" in result.output


def test_follow_logs_uses_platform_wait_command(monkeypatch):
    executed_commands = []

    def execute(command_path, command, environment):
        executed_commands.append((command_path, command, environment))
        raise OSError("execution stopped for test")

    monkeypatch.setattr(virtual.os, "execvpe", execute)

    with pytest.raises(virtual.VirtualLifecycleError, match="execution stopped for test"):
        virtual._follow_log_file(17)

    command_path, command, environment = executed_commands[0]
    if virtual.os.name == "nt":
        assert command_path == "powershell.exe"
        assert command[-1].endswith("-Tail 17")
        assert environment["NETAUDIO_LOG_FILE"] == virtual.LOGFILE
    else:
        assert command_path == "tail"
        assert command == ["tail", "-f", "-n", "17", virtual.LOGFILE]


def test_process_exit_waiter_detects_process_exit():
    process = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.buffer.read(1)"],
        stdin=subprocess.PIPE,
    )
    reaper = threading.Thread(target=process.wait)
    try:
        with virtual._process_exit_waiter(process.pid) as wait_for_exit:
            assert process.stdin is not None
            process.stdin.close()
            reaper.start()
            assert wait_for_exit(2.0) is True
        reaper.join(timeout=2.0)
        assert process.returncode == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2.0)


def test_process_exit_waiter_falls_back_without_pidfd(monkeypatch):
    fallback_calls = []
    monkeypatch.setattr(virtual.sys, "platform", "linux")
    monkeypatch.delattr(virtual.os, "pidfd_open", raising=False)
    monkeypatch.setattr(
        virtual,
        "_linux_process_exit_waiter",
        lambda _pid: pytest.fail("pidfd waiter used without os.pidfd_open"),
    )
    monkeypatch.setattr(
        virtual,
        "_fallback_process_exit_waiter",
        lambda pid: nullcontext(lambda timeout: fallback_calls.append((pid, timeout)) or True),
    )

    with virtual._process_exit_waiter(4242) as wait_for_exit:
        assert wait_for_exit(0.25) is True

    assert fallback_calls == [(4242, 0.25)]
