from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest

import tools.ddm_lab._lifecycle_cleanup as cleanup_module
from tools.ddm_lab.lifecycle import (
    SCHEMA_VERSION,
    LabConfiguration,
    LabError,
    LabHarness,
    atomic_write_json,
    read_json,
)


IDENTITY = "009"
RUN_ID = "cleanup-run"


def _harness(tmp_path: Path) -> LabHarness:
    return LabHarness(
        LabConfiguration(
            state_root=tmp_path / "state",
            promotion_root=tmp_path / "promoted",
            require_tmpfs=False,
        )
    )


def _active_run(
    harness: LabHarness,
    *,
    captures: list[dict[str, Any]] | None = None,
) -> tuple[Path, dict[str, Any]]:
    harness.initialize_state_root()
    harness._create_lease(IDENTITY, RUN_ID)
    session = harness._session_path(RUN_ID)
    session.mkdir()
    harness._write_session_marker(session, RUN_ID)
    state = {
        "schema_version": SCHEMA_VERSION,
        "run_id": RUN_ID,
        "status": "running",
        "created_at": "2026-08-26T00:00:00Z",
        "identity": {
            "suffix": IDENTITY,
            "name": "ferrofish-a32-synthetic-009",
            "descriptor_sha256": "1" * 64,
            "mac_address": "02:00:00:00:00:09",
        },
        "validated_inputs": {
            "qemu": {"sha256": "2" * 64},
            "image": {"manifest": {"sha256": "3" * 64}},
        },
        "tap": {"name": "nddm009owned", "ifindex": 999999},
        "qmp_socket": str(session / "qmp.sock"),
        "capture": None,
        "captures": captures or [],
        "promotions": [],
    }
    atomic_write_json(session / "state.json", state)
    return session, state


def _mock_resource_cleanup(
    harness: LabHarness,
    monkeypatch: pytest.MonkeyPatch,
    events: list[str] | None = None,
) -> None:
    def record(name: str) -> None:
        if events is not None:
            events.append(name)

    monkeypatch.setattr(cleanup_module, "process_matches", lambda pid, start_ticks, argument: False)

    class ForbiddenQmpClient:
        def __init__(self, *args: Any, **kwargs: Any):
            raise AssertionError("QMP must not be opened by an offline cleanup test")

    monkeypatch.setattr(cleanup_module, "QmpClient", ForbiddenQmpClient)
    monkeypatch.setattr(harness, "_stop_capture_in_state", lambda state: record("capture"))
    monkeypatch.setattr(
        harness,
        "_terminate_exact_process",
        lambda pid, start_ticks, argument, first_signal: record("process"),
    )
    monkeypatch.setattr(
        harness,
        "_delete_tap",
        lambda tap, expected_index, tolerate_missing=False: record("tap"),
    )


def test_stop_stages_releases_discards_and_writes_completed_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness(tmp_path)
    session, _ = _active_run(harness)
    discard = harness.state_root / "discarding" / RUN_ID
    history_path = harness.state_root / "history" / f"{RUN_ID}.json"
    events: list[str] = []
    _mock_resource_cleanup(harness, monkeypatch, events)

    original_replace = cleanup_module.os.replace
    original_release = harness._release_lease
    original_rmtree = cleanup_module.shutil.rmtree

    def recording_replace(source: Path, destination: Path) -> None:
        if Path(source) == session and Path(destination) == discard:
            events.append("stage")
        original_replace(source, destination)

    def recording_release(identity: str, run_id: str) -> None:
        events.append("release")
        original_release(identity, run_id)

    def recording_rmtree(path: Path) -> None:
        if Path(path) == discard:
            events.append("discard")
        original_rmtree(path)

    monkeypatch.setattr(cleanup_module.os, "replace", recording_replace)
    monkeypatch.setattr(harness, "_release_lease", recording_release)
    monkeypatch.setattr(cleanup_module.shutil, "rmtree", recording_rmtree)

    summary = harness.stop(IDENTITY)

    assert events[:3] == ["capture", "process", "tap"]
    assert events.index("stage") < events.index("release") < events.index("discard")
    assert not session.exists()
    assert not discard.exists()
    assert not harness._lease_path(IDENTITY).exists()
    assert summary["outcome"] == "completed"
    assert summary["raw_session_retained"] is False
    history = read_json(history_path)
    assert history["outcome"] == "completed"
    assert history["raw_session_retained"] is False


def test_discard_failure_leaves_failed_history_then_prune_removes_discard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness(tmp_path)
    session, _ = _active_run(harness)
    discard = harness.state_root / "discarding" / RUN_ID
    history_path = harness.state_root / "history" / f"{RUN_ID}.json"
    _mock_resource_cleanup(harness, monkeypatch)
    original_rmtree = cleanup_module.shutil.rmtree
    discard_calls = 0

    def fail_first_discard(path: Path) -> None:
        nonlocal discard_calls
        if Path(path) == discard:
            discard_calls += 1
            if discard_calls == 1:
                raise OSError("injected discard failure")
        original_rmtree(path)

    monkeypatch.setattr(cleanup_module.shutil, "rmtree", fail_first_discard)

    with pytest.raises(OSError, match="injected discard failure"):
        harness.stop(IDENTITY)

    assert not session.exists()
    assert not harness._lease_path(IDENTITY).exists()
    assert discard.is_dir()
    failed = read_json(history_path)
    assert failed["outcome"] == "cleanup_failed"
    assert failed["raw_session_retained"] is True
    assert failed["discard_path"] == str(discard)

    result = harness.prune(apply=True)

    assert discard_calls == 2
    assert not discard.exists()
    assert result["removed"] == [{"path": str(discard), "reason": "incomplete discard"}]
    pruned = read_json(history_path)
    assert pruned["outcome"] == "pruned"
    assert pruned["raw_session_retained"] is False
    assert pruned["pruned_at"]


def test_discard_failure_reports_the_capture_that_is_still_retained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness(tmp_path)
    session, state = _active_run(harness)
    capture = session / "capture-01.pcap"
    capture.write_bytes(b"packet capture")
    state["captures"] = [{"sequence": 1, "path": str(capture), "size": capture.stat().st_size, "sha256": "4" * 64}]
    atomic_write_json(session / "state.json", state)
    discard = harness.state_root / "discarding" / RUN_ID
    _mock_resource_cleanup(harness, monkeypatch)

    def fail_discard(path: Path) -> None:
        raise OSError(f"cannot discard {path}")

    monkeypatch.setattr(cleanup_module.shutil, "rmtree", fail_discard)

    with pytest.raises(OSError, match="cannot discard"):
        harness.stop(IDENTITY)

    history = read_json(harness.state_root / "history" / f"{RUN_ID}.json")
    assert history["captures"][0]["retained"] is True
    assert (discard / "capture-01.pcap").is_file()


def test_lease_release_failure_rolls_staged_session_back_without_deleting_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness(tmp_path)
    session, _ = _active_run(harness)
    discard = harness.state_root / "discarding" / RUN_ID
    history_path = harness.state_root / "history" / f"{RUN_ID}.json"
    _mock_resource_cleanup(harness, monkeypatch)
    discard_attempts: list[Path] = []

    def fail_release(identity: str, run_id: str) -> None:
        raise OSError("injected lease release failure")

    def forbidden_rmtree(path: Path) -> None:
        discard_attempts.append(Path(path))
        raise AssertionError("a rolled-back session must not be deleted")

    monkeypatch.setattr(harness, "_release_lease", fail_release)
    monkeypatch.setattr(cleanup_module.shutil, "rmtree", forbidden_rmtree)

    with pytest.raises(OSError, match="injected lease release failure"):
        harness.stop(IDENTITY)

    assert discard_attempts == []
    assert session.is_dir()
    assert not discard.exists()
    assert harness._lease_path(IDENTITY).is_dir()
    assert read_json(session / "state.json")["status"] == "cleanup_failed"
    history = read_json(history_path)
    assert history["outcome"] == "cleanup_failed"
    assert history["raw_session_retained"] is True
    assert history["discard_path"] is None


def test_final_history_failure_after_discard_never_recreates_active_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness(tmp_path)
    session, _ = _active_run(harness)
    discard = harness.state_root / "discarding" / RUN_ID
    history_path = harness.state_root / "history" / f"{RUN_ID}.json"
    _mock_resource_cleanup(harness, monkeypatch)
    original_atomic_write = cleanup_module.atomic_write_json
    injected = False

    def fail_completed_history(path: Path, value: Any, mode: int = 0o600) -> None:
        nonlocal injected
        if Path(path) == history_path and value.get("outcome") == "completed" and not injected:
            injected = True
            raise OSError("injected final history failure")
        original_atomic_write(path, value, mode)

    monkeypatch.setattr(cleanup_module, "atomic_write_json", fail_completed_history)

    with pytest.raises(OSError, match="injected final history failure"):
        harness.stop(IDENTITY)

    assert injected
    assert not session.exists()
    assert not discard.exists()
    assert not harness._lease_path(IDENTITY).exists()
    failed = read_json(history_path)
    assert failed["outcome"] == "cleanup_failed"
    assert failed["raw_session_retained"] is False
    assert failed["discard_path"] is None


def test_retain_session_summary_marks_all_captures_retained(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _harness(tmp_path)
    session, state = _active_run(harness)
    capture = session / "capture-01.pcap"
    capture.write_bytes(b"pcap")
    captures = [{"sequence": 1, "path": str(capture), "size": 4, "sha256": "4" * 64}]
    state["captures"] = captures
    atomic_write_json(session / "state.json", state)
    _mock_resource_cleanup(harness, monkeypatch)

    summary = harness.stop(IDENTITY, retain_session=True)

    assert session.is_dir()
    assert not harness._lease_path(IDENTITY).exists()
    assert summary["raw_session_retained"] is True
    assert summary["captures"] == [{"sequence": 1, "size": 4, "sha256": "4" * 64, "retained": True}]
    history = read_json(harness.state_root / "history" / f"{RUN_ID}.json")
    assert history["raw_session_retained"] is True
    assert history["captures"][0]["retained"] is True
    assert read_json(session / "state.json")["retention"] == "explicit"

    prune = harness.prune(apply=True)

    assert session.is_dir()
    assert any("explicitly retained" in refusal["reason"] for refusal in prune["refused"])


def test_smoke_prune_never_touches_interactive_or_current_failed_run(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    harness.initialize_state_root()
    runs = {
        "interactive-completed": "completed",
        "smoke-old": "failed",
        "smoke-current": "failed",
    }
    sessions = {}
    for run_id, status in runs.items():
        session = harness._session_path(run_id)
        session.mkdir()
        harness._write_session_marker(session, run_id)
        atomic_write_json(
            session / "state.json",
            {
                "schema_version": SCHEMA_VERSION,
                "run_id": run_id,
                "status": status,
                "purpose": "smoke" if run_id.startswith("smoke-") else "interactive",
                "tap": {"name": "nddm009gone", "ifindex": 999999},
                "capture": None,
            },
        )
        sessions[run_id] = session

    result = harness.prune_smoke_failures("smoke-current", apply=True)

    assert not sessions["smoke-old"].exists()
    assert sessions["smoke-current"].exists()
    assert sessions["interactive-completed"].exists()
    assert result["removed"] == [{"path": str(sessions["smoke-old"]), "reason": "superseded smoke failure"}]


def test_smoke_prune_recovers_an_incomplete_staged_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _harness(tmp_path)
    harness.initialize_state_root()
    for run_id in ("smoke-old", "smoke-current"):
        session = harness._session_path(run_id)
        session.mkdir()
        harness._write_session_marker(session, run_id)
        atomic_write_json(
            session / "state.json",
            {
                "schema_version": SCHEMA_VERSION,
                "run_id": run_id,
                "status": "failed",
                "purpose": "smoke",
                "tap": {"name": "nddm009gone", "ifindex": 999999},
                "capture": None,
            },
        )
    discard = harness.state_root / "discarding" / "smoke-old"
    original_rmtree = cleanup_module.shutil.rmtree
    failed_once = False

    def fail_once(path: Path) -> None:
        nonlocal failed_once
        if Path(path) == discard and not failed_once:
            failed_once = True
            raise OSError("injected staged-prune failure")
        original_rmtree(path)

    monkeypatch.setattr(cleanup_module.shutil, "rmtree", fail_once)

    first = harness.prune_smoke_failures("smoke-current", apply=True)
    second = harness.prune_smoke_failures("smoke-current", apply=True)

    assert first["removed"] == []
    assert discard.exists() is False
    assert second["removed"] == [{"path": str(discard), "reason": "incomplete smoke discard"}]
    history = read_json(harness.state_root / "history" / "smoke-old.json")
    assert history["outcome"] == "pruned"
    assert history["raw_session_retained"] is False


def test_smoke_prune_retains_the_current_cleanup_failed_discard(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    harness.initialize_state_root()
    discard = harness.state_root / "discarding" / "smoke-current"
    discard.mkdir()
    atomic_write_json(
        harness.state_root / "history" / "smoke-current.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": "smoke-current",
            "purpose": "smoke",
            "outcome": "cleanup_failed",
            "raw_session_retained": True,
        },
    )

    result = harness.prune_smoke_failures("smoke-current", apply=True)

    assert discard.is_dir()
    assert result["removed"] == []
    assert str(discard) not in result["candidates"]


def test_history_pruning_preserves_unresolved_cleanup_records(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    harness.initialize_state_root()
    unresolved = harness.state_root / "history" / "unresolved-run.json"
    atomic_write_json(
        unresolved,
        {"schema_version": SCHEMA_VERSION, "run_id": "unresolved-run", "outcome": "cleanup_failed"},
    )
    for sequence in range(25):
        run_id = f"complete-{sequence:02d}"
        atomic_write_json(
            harness.state_root / "history" / f"{run_id}.json",
            {"schema_version": SCHEMA_VERSION, "run_id": run_id, "outcome": "completed"},
        )

    harness._prune_history(keep=20)

    assert unresolved.is_file()
    assert len(list((harness.state_root / "history").glob("complete-*.json"))) == 20


def test_same_harness_operation_lock_serializes_threads(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    first_entered = threading.Event()
    second_attempting = threading.Event()
    second_entered = threading.Event()
    release_first = threading.Event()
    failures: list[BaseException] = []

    def first_worker() -> None:
        try:
            with harness._operation_lock(exclusive=True):
                first_entered.set()
                if not release_first.wait(2):
                    raise AssertionError("first worker timed out")
        except BaseException as error:
            failures.append(error)

    def second_worker() -> None:
        try:
            if not first_entered.wait(2):
                raise AssertionError("second worker never observed first entry")
            second_attempting.set()
            with harness._operation_lock(exclusive=True):
                second_entered.set()
        except BaseException as error:
            failures.append(error)

    first = threading.Thread(target=first_worker)
    second = threading.Thread(target=second_worker)
    first.start()
    second.start()
    assert first_entered.wait(2)
    assert second_attempting.wait(2)
    assert not second_entered.wait(0.2)
    release_first.set()
    assert second_entered.wait(2)
    first.join(2)
    second.join(2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert failures == []
