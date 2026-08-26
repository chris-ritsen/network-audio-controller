from __future__ import annotations

import hashlib
import os
import socket
import subprocess
from pathlib import Path
from typing import Any

import pytest

import tools.ddm_lab.lifecycle as lifecycle_module
from tools.ddm_lab.lifecycle import (
    SCHEMA_VERSION,
    SESSION_MARKER,
    LabConfiguration,
    LabError,
    LabHarness,
    _process_start_ticks,
    atomic_write_json,
    build_qemu_command,
    read_json,
)


IDENTITY = "009"
RUN_ID = "ownership-run"
BOARD_INFORMATION_FLASH_OFFSET = 8
BOARD_INFORMATION_MAC_OFFSET = 10
CANONICAL_MAC = bytes.fromhex("020000000001")


def _harness(tmp_path: Path) -> LabHarness:
    return LabHarness(
        LabConfiguration(
            state_root=tmp_path / "state",
            promotion_root=tmp_path / "promoted",
            require_tmpfs=False,
        )
    )


def _validated_inputs(tmp_path: Path) -> dict[str, Any]:
    bootloader = tmp_path / "bootloader.bin"
    flash = tmp_path / "flash.bin"
    board_information = tmp_path / "board_information.bin"
    board_template = b"synthetic-" + CANONICAL_MAC + b"-board-template"
    bootloader.write_bytes(b"opaque bootloader")
    flash.write_bytes(b"flash---" + board_template + b"---opaque-flash-tail")
    board_information.write_bytes(board_template)
    flash_digest = hashlib.sha256(flash.read_bytes()).hexdigest()
    board_information_digest = hashlib.sha256(board_template).hexdigest()
    return {
        "qemu": {"path": "/offline/qemu-system-microblaze", "sha256": "1" * 64},
        "image": {
            "manifest": {"sha256": "2" * 64},
            "artifacts": {
                "bootloader": {"path": str(bootloader)},
                "flash": {"path": str(flash), "sha256": flash_digest},
                "board_information": {
                    "path": str(board_information),
                    "sha256": board_information_digest,
                },
            },
            "identity_template": {
                "board_information_flash_offset": BOARD_INFORMATION_FLASH_OFFSET,
                "board_information_size": len(board_template),
                "mac_offset": BOARD_INFORMATION_MAC_OFFSET,
                "base_identity": "001",
            },
        },
        "identity": {
            "suffix": IDENTITY,
            "name": "ferrofish-a32-synthetic-009",
            "descriptor_sha256": "3" * 64,
            "mac_address": "02:00:00:00:00:09",
        },
    }


def _write_bound_state(harness: LabHarness, run_id: str = RUN_ID) -> tuple[Path, dict[str, Any]]:
    harness.initialize_state_root()
    harness._create_lease(IDENTITY, run_id)
    session = harness._session_path(run_id)
    session.mkdir()
    harness._write_session_marker(session, run_id)
    state = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "status": "running",
        "created_at": "2026-08-26T00:00:00Z",
        "identity": {"suffix": IDENTITY},
        "tap": {"name": "nddm009owned", "ifindex": 999999},
        "capture": None,
        "qmp_socket": str(session / "qmp.sock"),
    }
    atomic_write_json(session / "state.json", state)
    return session, state


def _write_prunable_state(harness: LabHarness, run_id: str = "completed-run") -> tuple[Path, dict[str, Any]]:
    harness.initialize_state_root()
    session = harness._session_path(run_id)
    session.mkdir()
    harness._write_session_marker(session, run_id)
    state = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "status": "completed",
        "tap": {"name": "nddm009gone", "ifindex": 999999},
        "capture": None,
    }
    atomic_write_json(session / "state.json", state)
    return session, state


def test_create_lease_rolls_back_its_directory_when_owner_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness(tmp_path)
    harness.initialize_state_root()

    def fail_owner_write(path: Path, value: Any, mode: int = 0o600) -> None:
        assert path.name == "owner.json"
        raise OSError("injected owner write failure")

    monkeypatch.setattr(lifecycle_module, "atomic_write_json", fail_owner_write)

    with pytest.raises(OSError, match="injected owner write failure"):
        harness._create_lease(IDENTITY, RUN_ID)

    assert not harness._lease_path(IDENTITY).exists()


def test_create_lease_never_rolls_back_a_preexisting_directory(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    harness.initialize_state_root()
    lease = harness._lease_path(IDENTITY)
    lease.mkdir()
    sentinel = lease / "foreign-sentinel"
    sentinel.write_text("leave me", encoding="utf-8")

    with pytest.raises(LabError, match="already leased"):
        harness._create_lease(IDENTITY, RUN_ID)

    assert sentinel.read_text(encoding="utf-8") == "leave me"


def test_state_lookups_require_the_complete_local_binding(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    _, state = _write_bound_state(harness)

    assert harness.state_for_identity(IDENTITY)["run_id"] == state["run_id"]
    assert harness.state_for_run(RUN_ID)["identity"]["suffix"] == IDENTITY


def test_exact_failed_run_remains_readable_after_lease_release(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    session, state = _write_bound_state(harness)
    state["status"] = "failed"
    atomic_write_json(session / "state.json", state)
    harness._release_lease(IDENTITY, RUN_ID)

    assert harness.failed_state_for_run(RUN_ID)["run_id"] == RUN_ID
    with pytest.raises(LabError, match="no active lease"):
        harness.state_for_run(RUN_ID)


@pytest.mark.parametrize(
    ("record_name", "field", "value"),
    [
        ("owner", "schema_version", SCHEMA_VERSION + 1),
        ("owner", "identity", "008"),
        ("owner", "run_id", "different-run"),
        ("owner", "host", "different-host"),
        ("owner", "user_id", -1),
        ("marker", "ownership_token", "0" * 64),
        ("state", "schema_version", SCHEMA_VERSION + 1),
        ("state", "run_id", "different-run"),
        ("state_identity", "suffix", "008"),
    ],
)
def test_state_lookups_reject_corrupted_cross_file_bindings(
    tmp_path: Path,
    record_name: str,
    field: str,
    value: Any,
) -> None:
    harness = _harness(tmp_path)
    session, _ = _write_bound_state(harness)
    paths = {
        "owner": harness._lease_path(IDENTITY) / "owner.json",
        "marker": session / SESSION_MARKER,
        "state": session / "state.json",
        "state_identity": session / "state.json",
    }
    record = read_json(paths[record_name])
    if record_name == "state_identity":
        record["identity"][field] = value
    else:
        record[field] = value
    atomic_write_json(paths[record_name], record)

    with pytest.raises(LabError):
        harness.state_for_identity(IDENTITY)
    with pytest.raises(LabError):
        harness.state_for_run(RUN_ID)


def test_stop_verifies_binding_before_any_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _harness(tmp_path)
    session, _ = _write_bound_state(harness)
    marker = read_json(session / SESSION_MARKER)
    marker["ownership_token"] = "invalid"
    atomic_write_json(session / SESSION_MARKER, marker)
    cleanup_calls: list[str] = []
    monkeypatch.setattr(harness, "_stop_capture_in_state", lambda state: cleanup_calls.append("capture"))
    monkeypatch.setattr(
        harness,
        "_delete_tap",
        lambda tap, expected_index, tolerate_missing=False: cleanup_calls.append("tap"),
    )

    with pytest.raises(LabError, match="session ownership marker"):
        harness.stop(IDENTITY)

    assert cleanup_calls == []


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("qemu", "/offline/qemu,invalid", "QEMU executable path"),
        ("bootloader", "/tmp/boot\nloader", "bootloader path"),
        ("runtime_flash", "/tmp/runtime,flash", "runtime flash path"),
        ("qmp_socket", "/tmp/qmp,sock", "QMP socket path"),
    ],
)
def test_qemu_command_rejects_delimiter_and_control_paths(tmp_path: Path, field: str, value: str, message: str) -> None:
    validated = _validated_inputs(tmp_path)
    runtime_flash: Path | str = tmp_path / "runtime.bin"
    qmp_socket: Path | str = tmp_path / "qmp.sock"
    if field == "qemu":
        validated["qemu"]["path"] = value
    elif field == "bootloader":
        validated["image"]["artifacts"]["bootloader"]["path"] = value
    elif field == "runtime_flash":
        runtime_flash = value
    else:
        qmp_socket = value

    with pytest.raises(LabError, match=message):
        build_qemu_command(validated, Path(runtime_flash), Path(qmp_socket), "nddm009owned", RUN_ID)


def test_qemu_command_rejects_overlong_af_unix_path(tmp_path: Path) -> None:
    validated = _validated_inputs(tmp_path)
    overlong = Path("/") / ("q" * 108)

    with pytest.raises(LabError, match="exceeds 107 encoded bytes"):
        build_qemu_command(validated, tmp_path / "runtime.bin", overlong, "nddm009owned", RUN_ID)


def _prepare_offline_start(
    harness: LabHarness,
    validated: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(harness, "validate", lambda identity: validated)
    monkeypatch.setattr(harness, "_mac_on_bridge", lambda mac_address, tap=None: False)
    monkeypatch.setattr(harness, "_tap_name", lambda identity: "nddm009owned")


def test_start_builds_and_validates_command_before_tap_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness(tmp_path)
    validated = _validated_inputs(tmp_path)
    validated["image"]["artifacts"]["bootloader"]["path"] = "/tmp/bad,bootloader"
    _prepare_offline_start(harness, validated, monkeypatch)
    taps: list[str] = []
    monkeypatch.setattr(harness, "_create_tap", lambda tap: taps.append(tap))

    with pytest.raises(LabError, match="bootloader path"):
        harness.start(IDENTITY, run_id="invalid-command", wait_seconds=1)

    assert taps == []
    assert not harness._lease_path(IDENTITY).exists()
    assert not harness._session_path("invalid-command").exists()


@pytest.mark.parametrize(
    ("run_id", "purpose"),
    [("smoke-manual", "interactive"), ("ordinary-run", "smoke")],
)
def test_smoke_run_prefix_is_bound_to_smoke_purpose(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    run_id: str,
    purpose: str,
) -> None:
    harness = _harness(tmp_path)
    validated = _validated_inputs(tmp_path)
    _prepare_offline_start(harness, validated, monkeypatch)

    with pytest.raises(LabError, match="smoke- run identifier prefix is reserved"):
        harness.start(IDENTITY, run_id=run_id, purpose=purpose, wait_seconds=1)

    assert not harness._lease_path(IDENTITY).exists()


def test_start_sends_qemu_output_to_devnull(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _harness(tmp_path)
    validated = _validated_inputs(tmp_path)
    _prepare_offline_start(harness, validated, monkeypatch)
    monkeypatch.setattr(harness, "_session_path", lambda run_id: tmp_path / run_id)
    monkeypatch.setattr(harness, "_create_tap", lambda tap: 77)
    monkeypatch.setattr(harness, "_wait_qmp", lambda state, timeout: {"running": True})
    monkeypatch.setattr(harness, "_wait_lan", lambda state, timeout: None)
    monkeypatch.setattr(lifecycle_module, "_process_start_ticks", lambda pid: 88)
    popen_arguments: dict[str, Any] = {}

    class OfflineProcess:
        pid = 4242

    def fake_popen(command: list[str], **arguments: Any) -> OfflineProcess:
        popen_arguments.update(arguments)
        return OfflineProcess()

    monkeypatch.setattr(lifecycle_module.subprocess, "Popen", fake_popen)

    state = harness.start(IDENTITY, run_id="r", wait_seconds=1)

    assert state["status"] == "running"
    assert popen_arguments["stdin"] is subprocess.DEVNULL
    assert popen_arguments["stdout"] is subprocess.DEVNULL
    assert popen_arguments["stderr"] is subprocess.DEVNULL
    assert not (tmp_path / "r" / "qemu-stdout.log").exists()
    assert not (tmp_path / "r" / "qemu-stderr.log").exists()


def test_prune_refuses_exact_process_argument_mismatch(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    session, _ = _write_prunable_state(harness)
    state = read_json(session / "state.json")
    state.update(
        {
            "qemu_pid": os.getpid(),
            "qemu_start_ticks": _process_start_ticks(os.getpid()),
            "qmp_socket": "definitely-not-in-this-process-command",
        }
    )
    atomic_write_json(session / "state.json", state)

    result = harness.prune(apply=True)

    assert result["removed"] == []
    assert session.exists()
    assert any("argument mismatch" in refusal["reason"] for refusal in result["refused"])


def test_prune_detects_recorded_ifindex_under_renamed_interface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness(tmp_path)
    session, _ = _write_prunable_state(harness)
    monkeypatch.setattr(harness, "_interface_name_for_index", lambda interface_index: "renamed-tap")

    result = harness.prune(apply=True)

    assert result["removed"] == []
    assert session.exists()
    assert any("ifindex is still present as renamed-tap" in refusal["reason"] for refusal in result["refused"])


def test_prune_rechecks_live_resources_immediately_before_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness(tmp_path)
    session, _ = _write_prunable_state(harness)
    checks = 0

    def interface_for_index(interface_index: int) -> str | None:
        nonlocal checks
        checks += 1
        return None if checks == 1 else "late-renamed-tap"

    monkeypatch.setattr(harness, "_interface_name_for_index", interface_for_index)

    result = harness.prune(apply=True)

    assert checks == 2
    assert result["removed"] == []
    assert session.exists()
    assert any("late-renamed-tap" in refusal["reason"] for refusal in result["refused"])


def test_prune_rechecks_marker_and_state_before_deletion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _harness(tmp_path)
    session, _ = _write_prunable_state(harness)
    calls = 0
    original_lease_run_ids = harness._lease_run_ids

    def mutate_before_recheck() -> set[str]:
        nonlocal calls
        calls += 1
        if calls == 2:
            marker = read_json(session / SESSION_MARKER)
            marker["ownership_token"] = "invalidated-after-selection"
            atomic_write_json(session / SESSION_MARKER, marker)
        return original_lease_run_ids()

    monkeypatch.setattr(harness, "_lease_run_ids", mutate_before_recheck)

    result = harness.prune(apply=True)

    assert calls == 2
    assert result["removed"] == []
    assert session.exists()
    assert any("session ownership marker" in refusal["reason"] for refusal in result["refused"])
