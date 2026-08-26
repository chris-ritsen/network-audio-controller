from __future__ import annotations

import os
import signal
from pathlib import Path
from typing import Any

import pytest

import tools.ddm_lab._lifecycle_evidence as evidence_module
from tools.ddm_lab.lifecycle import LabConfiguration, LabError, LabHarness


def _make_harness(tmp_path: Path) -> LabHarness:
    return LabHarness(
        LabConfiguration(
            state_root=tmp_path / "state",
            promotion_root=tmp_path / "promoted",
            require_tmpfs=False,
        )
    )


def _make_state(harness: LabHarness, run_id: str = "evidence-run") -> tuple[dict[str, Any], Path]:
    harness.initialize_state_root()
    session = harness._session_path(run_id)
    session.mkdir(mode=0o700)
    state: dict[str, Any] = {
        "run_id": run_id,
        "status": "running",
        "qmp_socket": str(session / "qmp.sock"),
        "qemu_pid": 4001,
        "qemu_start_ticks": 91,
        "tap": {"name": "nddm009test", "ifindex": 77},
        "identity": {"mac_address": "02:00:00:00:00:09"},
        "capture": None,
        "captures": [],
    }
    return state, session


def _install_snapshot_qmp(
    monkeypatch: pytest.MonkeyPatch,
    *,
    payload: bytes | None = None,
    snapshot_error: BaseException | None = None,
    resume_error: BaseException | None = None,
) -> list[tuple[str, dict[str, Any] | None]]:
    events: list[tuple[str, dict[str, Any] | None]] = []
    qemu = {"running": True}

    class FakeQmpClient:
        def __init__(self, path: Path, timeout: float = 5.0):
            del path, timeout

        def execute(self, command: str, arguments: dict[str, Any] | None = None) -> Any:
            events.append((command, arguments))
            if command == "stop":
                qemu["running"] = False
                return {}
            if command == "cont":
                if resume_error is not None:
                    raise resume_error
                qemu["running"] = True
                return {}
            if command == "query-status":
                return {"running": qemu["running"]}
            if command == "pmemsave":
                if snapshot_error is not None:
                    raise snapshot_error
                assert arguments is not None
                Path(arguments["filename"]).write_bytes(payload or b"")
                return {}
            raise AssertionError(f"unexpected QMP command: {command}")

    monkeypatch.setattr(evidence_module, "QmpClient", FakeQmpClient)
    return events


def test_snapshot_uses_partial_atomic_install_and_resumes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _make_harness(tmp_path)
    state, session = _make_state(harness)
    expected_size = 64
    monkeypatch.setattr(evidence_module, "DEFAULT_RAM_SIZE", expected_size)
    monkeypatch.setattr(
        harness,
        "_execute_qmp_for_identity",
        lambda identity, command: (state, {"running": True}),
    )
    events = _install_snapshot_qmp(monkeypatch, payload=b"R" * expected_size)
    partial = session / "snapshots" / ".baseline.ram.partial"
    final = session / "snapshots" / "baseline.ram"
    real_replace = os.replace

    def record_replace(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
        if Path(source) == partial:
            assert partial.exists()
            assert not final.exists()
            events.append(("atomic-install", {"destination": str(destination)}))
        real_replace(source, destination)

    monkeypatch.setattr(evidence_module.os, "replace", record_replace)

    record = harness.snapshot("009", "baseline")

    names = [name for name, _ in events]
    assert names.index("pmemsave") < names.index("atomic-install") < names.index("cont")
    pmemsave = next(arguments for name, arguments in events if name == "pmemsave")
    assert pmemsave is not None
    assert pmemsave["filename"] == str(partial)
    assert final.read_bytes() == b"R" * expected_size
    assert not partial.exists()
    assert record["path"] == str(final)
    assert state["status"] == "running"
    assert state["snapshots"] == [record]


def test_malformed_snapshot_is_removed_and_qemu_resumes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _make_harness(tmp_path)
    state, session = _make_state(harness)
    monkeypatch.setattr(evidence_module, "DEFAULT_RAM_SIZE", 64)
    monkeypatch.setattr(
        harness,
        "_execute_qmp_for_identity",
        lambda identity, command: (state, {"running": True}),
    )
    events = _install_snapshot_qmp(monkeypatch, payload=b"short")

    with pytest.raises(LabError, match="wrong size"):
        harness.snapshot("009", "malformed")

    assert not (session / "snapshots" / ".malformed.ram.partial").exists()
    assert not (session / "snapshots" / "malformed.ram").exists()
    assert "cont" in [name for name, _ in events]
    assert state["status"] == "running"
    assert state.get("snapshots", []) == []


def test_snapshot_and_resume_errors_are_both_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _make_harness(tmp_path)
    state, session = _make_state(harness)
    monkeypatch.setattr(evidence_module, "DEFAULT_RAM_SIZE", 64)
    monkeypatch.setattr(
        harness,
        "_execute_qmp_for_identity",
        lambda identity, command: (state, {"running": True}),
    )
    _install_snapshot_qmp(
        monkeypatch,
        snapshot_error=LabError("pmemsave failed"),
        resume_error=LabError("resume failed"),
    )

    with pytest.raises(LabError, match="pmemsave failed; QEMU restore also failed: resume failed"):
        harness.snapshot("009", "double-failure")

    assert not (session / "snapshots" / ".double-failure.ram.partial").exists()
    assert not (session / "snapshots" / "double-failure.ram").exists()


def test_session_aggregate_quota_refuses_more_than_512_mib(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _make_harness(tmp_path)
    session = tmp_path / "session"
    session.mkdir()
    limit = evidence_module.DEFAULT_SESSION_MIB * 1024 * 1024
    monkeypatch.setattr(harness, "_session_size", lambda candidate: limit)

    with pytest.raises(LabError, match="512 MiB aggregate limit"):
        harness._require_session_capacity(session, 1)


def test_snapshot_is_refused_while_capture_reservation_is_active(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _make_harness(tmp_path)
    state, _ = _make_state(harness)
    state["capture"] = {"maximum_mib": 256}
    monkeypatch.setattr(
        harness,
        "_execute_qmp_for_identity",
        lambda identity, command: (state, {"running": True}),
    )

    with pytest.raises(LabError, match="stop the active capture"):
        harness.snapshot("009", "while-capturing")


@pytest.mark.parametrize(
    ("process_status", "current_ifindex", "message"),
    [
        ("absent", 77, "exact active QEMU process"),
        ("active", 78, "recorded interface index"),
    ],
)
def test_public_capture_start_rejects_inactive_qemu_or_reused_tap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    process_status: str,
    current_ifindex: int,
    message: str,
) -> None:
    harness = _make_harness(tmp_path)
    state, _ = _make_state(harness)
    monkeypatch.setattr(harness, "state_for_identity", lambda identity: state)
    monkeypatch.setattr(evidence_module, "owned_process_status", lambda *arguments: process_status)
    monkeypatch.setattr(harness, "_interface_index", lambda interface: current_ifindex)
    starts: list[dict[str, Any]] = []
    monkeypatch.setattr(
        harness,
        "_start_capture_in_state",
        lambda capture_state, seconds, maximum_mib: starts.append(capture_state),
    )

    with pytest.raises(LabError, match=message):
        harness.capture_start("009")

    assert starts == []


class _FakeDumpcap:
    def __init__(self, command: list[str], *, pid: int = 5001):
        self.command = command
        self.pid = pid
        Path(command[command.index("-w") + 1]).write_bytes(b"P" * 24)


def _install_fake_dumpcap(
    harness: LabHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> list[_FakeDumpcap]:
    processes: list[_FakeDumpcap] = []

    def popen(command: list[str], **kwargs: Any) -> _FakeDumpcap:
        del kwargs
        process = _FakeDumpcap(command)
        processes.append(process)
        return process

    monkeypatch.setattr(evidence_module.subprocess, "Popen", popen)
    monkeypatch.setattr(evidence_module, "_process_start_ticks", lambda pid: 1234)
    monkeypatch.setattr(evidence_module, "process_matches", lambda *arguments: True)
    monkeypatch.setattr(harness, "_require_session_capacity", lambda session, additional: None)
    return processes


def test_dumpcap_command_is_filtered_to_the_guest_mac(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _make_harness(tmp_path)
    state, _ = _make_state(harness)
    state["identity"]["mac_address"] = "02:AA:BB:CC:DD:EE"
    processes = _install_fake_dumpcap(harness, monkeypatch)
    monkeypatch.setattr(harness, "_write_state", lambda value: None)

    record = harness._start_capture_in_state(state, seconds=15, maximum_mib=4)

    command = processes[0].command
    filter_index = command.index("-f")
    assert command[filter_index + 1] == ("ether host 02:aa:bb:cc:dd:ee or ether broadcast or ether multicast")
    assert command[command.index("-i") + 1] == state["tap"]["name"]
    assert record["capture_filter"] == ("ether host 02:aa:bb:cc:dd:ee or ether broadcast or ether multicast")


def test_capture_state_write_failure_terminates_dumpcap_and_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _make_harness(tmp_path)
    state, session = _make_state(harness)
    previous_records = [{"sequence": 1, "path": str(session / "capture-01.pcap")}]
    state["captures"] = list(previous_records)
    processes = _install_fake_dumpcap(harness, monkeypatch)
    terminated: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        harness,
        "_terminate_exact_process",
        lambda *arguments, **kwargs: terminated.append((*arguments, kwargs["first_signal"])),
    )

    def fail_write(value: dict[str, Any]) -> None:
        raise LabError("state write failed")

    monkeypatch.setattr(harness, "_write_state", fail_write)

    with pytest.raises(LabError, match="state write failed"):
        harness._start_capture_in_state(state, seconds=15, maximum_mib=4)

    assert terminated == [(processes[0].pid, 1234, state["tap"]["name"], signal.SIGTERM)]
    assert state["capture"] is None
    assert state["captures"] == previous_records
    assert not (session / "capture-02.pcap").exists()


def test_capture_stop_rejects_path_outside_owned_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _make_harness(tmp_path)
    state, _ = _make_state(harness)
    outside = tmp_path / "outside.pcap"
    outside.write_bytes(b"outside")
    state["capture"] = {
        "path": str(outside),
        "pid": 5001,
        "start_ticks": 1234,
    }
    monkeypatch.setattr(harness, "state_for_identity", lambda identity: state)
    monkeypatch.setattr(harness, "_terminate_exact_process", lambda *arguments, **kwargs: None)

    with pytest.raises(LabError, match="outside its owned run"):
        harness.capture_stop("009")

    assert outside.read_bytes() == b"outside"
    assert state["capture"]["path"] == str(outside)


def test_routine_promotion_rejects_full_ram_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _make_harness(tmp_path)
    state, session = _make_state(harness)
    snapshots = session / "snapshots"
    snapshots.mkdir()
    (snapshots / "full.ram").write_bytes(b"opaque full-memory snapshot")
    monkeypatch.setattr(harness, "state_for_identity", lambda identity: state)

    with pytest.raises(LabError, match="prohibited from routine promotion"):
        harness.promote(
            "009",
            claim="Synthetic offline evidence",
            evidence_class="observed",
            artifacts=["snapshots/full.ram"],
        )

    assert not harness.configuration.promotion_root.exists()
