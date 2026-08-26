from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

import tools.ddm_lab.lifecycle as lifecycle_module
from tools.ddm_lab.lifecycle import (
    LabConfiguration,
    LabError,
    LabHarness,
    _process_start_ticks,
    _safe_relative_path,
    atomic_write_json,
    build_qemu_command,
    materialize_runtime_identity,
    process_matches,
    sha256_file,
    validate_artifacts,
)


BOARD_INFORMATION_FLASH_OFFSET = 8
BOARD_INFORMATION_MAC_OFFSET = 10
CANONICAL_MAC = bytes.fromhex("020000000001")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_configuration(tmp_path: Path, identity: str = "009") -> LabConfiguration:
    qemu = tmp_path / "qemu-system-microblaze"
    qemu.write_text("#!/bin/sh\nprintf '%s\\n' 'dante-brooklyn test machine'\n", encoding="utf-8")
    qemu.chmod(0o700)

    image = tmp_path / "image"
    image.mkdir()
    board_information = b"synthetic-" + CANONICAL_MAC + b"-board-template"
    flash = b"flash---" + board_information + b"---opaque-flash-tail"
    artifacts = {
        "bootloader": b"opaque bootloader",
        "flash": flash,
        "board_information": board_information,
    }
    manifest: dict[str, object] = {"format_version": 4}
    for key, data in artifacts.items():
        filename = f"{key}.bin"
        (image / filename).write_bytes(data)
        manifest[key] = {"filename": filename, "size": len(data), "sha256": _sha256(data)}
    board_record = manifest["board_information"]
    assert isinstance(board_record, dict)
    board_record.update(
        {
            "identity_name": "ferrofish-a32-synthetic-001",
            "flash_offset": BOARD_INFORMATION_FLASH_OFFSET,
        }
    )
    atomic_write_json(image / "manifest.json", manifest)

    identities = tmp_path / "identities"
    identities.mkdir()
    atomic_write_json(
        identities / f"ferrofish-a32-synthetic-{identity}.json",
        {
            "format_version": 2,
            "identity_name": f"ferrofish-a32-synthetic-{identity}",
            "identity_kind": "synthetic",
            "media_access_control_address": f"02:00:00:00:00:{int(identity):02x}",
            "serial_number": 1,
            "hardware_revision_major": 0,
            "hardware_revision_minor": 0,
            "configuration_date": "20260722",
        },
    )
    return LabConfiguration(
        state_root=tmp_path / "state",
        promotion_root=tmp_path / "promoted",
        qemu=qemu,
        qemu_sha256=sha256_file(qemu),
        image_directory=image,
        image_manifest_sha256=sha256_file(image / "manifest.json"),
        identities_directory=identities,
        bridge="br0",
        require_tmpfs=False,
    )


def test_validate_artifacts_binds_every_opaque_input(tmp_path: Path) -> None:
    configuration = _make_configuration(tmp_path)

    result = validate_artifacts(configuration, "009")

    assert result["identity"]["mac_address"] == "02:00:00:00:00:09"
    assert result["qemu"]["sha256"] == sha256_file(configuration.qemu)
    assert set(result["image"]["artifacts"]) == {"bootloader", "flash", "board_information"}
    assert result["image"]["identity_template"] == {
        "board_information_flash_offset": BOARD_INFORMATION_FLASH_OFFSET,
        "board_information_size": len(b"synthetic-" + CANONICAL_MAC + b"-board-template"),
        "mac_offset": BOARD_INFORMATION_MAC_OFFSET,
        "base_identity": "001",
    }
    assert result["evidence_limits"]["protocol_semantics"] == "not_assessed"


def test_validate_artifacts_rejects_changed_flash_atomically(tmp_path: Path) -> None:
    configuration = _make_configuration(tmp_path)
    (configuration.image_directory / "flash.bin").write_bytes(b"changed")

    with pytest.raises(LabError, match="flash differs from its manifest"):
        validate_artifacts(configuration, "009")


def test_runtime_identity_materializes_requested_mac_only_in_private_flash(tmp_path: Path) -> None:
    configuration = _make_configuration(tmp_path, identity="009")
    validated = validate_artifacts(configuration, "009")
    source_flash = configuration.image_directory / "flash.bin"
    source_before = source_flash.read_bytes()
    runtime_flash = tmp_path / "flash.runtime.bin"
    runtime_flash.write_bytes(source_before)
    session = tmp_path / "session"
    session.mkdir()

    result = materialize_runtime_identity(validated, runtime_flash, session)

    materialized_board = (session / "brdinfo.runtime.bin").read_bytes()
    assert source_flash.read_bytes() == source_before
    assert materialized_board[BOARD_INFORMATION_MAC_OFFSET : BOARD_INFORMATION_MAC_OFFSET + 6] == bytes.fromhex(
        "020000000009"
    )
    assert result["base_identity"] == "001"
    assert result["identity"] == "009"
    assert result["changed_board_information_offsets"] == [BOARD_INFORMATION_MAC_OFFSET + 5]
    assert result["changed_flash_offsets"] == [BOARD_INFORMATION_FLASH_OFFSET + BOARD_INFORMATION_MAC_OFFSET + 5]


def test_qemu_command_uses_private_flash_unique_tap_and_no_capture_interface(tmp_path: Path) -> None:
    validated = validate_artifacts(_make_configuration(tmp_path), "009")
    command = build_qemu_command(
        validated,
        tmp_path / "private-flash.bin",
        tmp_path / "qmp.sock",
        "nddm009abc123",
        "run-009",
    )

    joined = " ".join(command)
    assert "if=mtd,format=raw,file=" in joined
    assert "ifname=nddm009abc123" in joined
    assert "server=on,wait=off" in joined
    assert "ringbuf,id=dante-serial,size=65536" in joined
    assert "guest_errors" not in joined
    assert "br0" not in joined
    assert "ddm-tap" not in joined
    assert "Authorization" not in joined


def test_process_identity_includes_start_time_and_required_argument() -> None:
    start_ticks = _process_start_ticks(os.getpid())
    assert start_ticks is not None
    assert process_matches(os.getpid(), start_ticks)
    assert not process_matches(os.getpid(), start_ticks + 1)
    assert not process_matches(os.getpid(), start_ticks, "definitely-not-in-this-command")


def test_exact_lease_owner_is_required_for_release(tmp_path: Path) -> None:
    harness = LabHarness(_make_configuration(tmp_path))
    harness.initialize_state_root()
    harness._create_lease("009", "owner-run")

    with pytest.raises(LabError, match="another run"):
        harness._release_lease("009", "different-run")

    harness._release_lease("009", "owner-run")
    assert not harness._lease_path("009").exists()


def _prepare_offline_start(harness: LabHarness, monkeypatch: pytest.MonkeyPatch) -> None:
    validated = validate_artifacts(harness.configuration, "009")
    monkeypatch.setattr(harness, "validate", lambda identity: validated)
    monkeypatch.setattr(harness, "_mac_on_bridge", lambda mac_address, tap=None: False)


def test_start_never_deletes_tap_when_initial_add_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = LabHarness(_make_configuration(tmp_path))
    _prepare_offline_start(harness, monkeypatch)
    monkeypatch.setattr(harness, "_tap_name", lambda identity: "nddm009foreign")
    deleted: list[tuple[str, int | None, bool]] = []

    def reject_add(command: list[str], *, timeout: float = 15.0, check: bool = True) -> None:
        raise LabError("TAP add failed")

    def record_delete(tap: str, expected_index: int | None, *, tolerate_missing: bool = False) -> None:
        deleted.append((tap, expected_index, tolerate_missing))

    monkeypatch.setattr(lifecycle_module, "_run", reject_add)
    monkeypatch.setattr(harness, "_delete_tap", record_delete)

    with pytest.raises(LabError, match="TAP add failed"):
        harness.start("009", run_id="foreign-collision", wait_seconds=1)

    assert deleted == []
    assert not harness._lease_path("009").exists()
    assert not harness._session_path("foreign-collision").exists()


def test_start_retains_lease_when_created_tap_cleanup_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = LabHarness(_make_configuration(tmp_path))
    _prepare_offline_start(harness, monkeypatch)
    monkeypatch.setattr(harness, "_tap_name", lambda identity: "nddm009owned")
    monkeypatch.setattr(harness, "_interface_index", lambda tap: 77)
    calls = 0

    def fail_configuration(command: list[str], *, timeout: float = 15.0, check: bool = True) -> None:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise LabError("TAP configuration failed")

    def fail_delete(tap: str, expected_index: int | None, *, tolerate_missing: bool = False) -> None:
        assert (tap, expected_index, tolerate_missing) == ("nddm009owned", 77, True)
        raise LabError("TAP deletion failed")

    monkeypatch.setattr(lifecycle_module, "_run", fail_configuration)
    monkeypatch.setattr(harness, "_delete_tap", fail_delete)

    with pytest.raises(LabError, match="cleanup failed: TAP cleanup: TAP deletion failed"):
        harness.start("009", run_id="tap-cleanup-failed", wait_seconds=1)

    assert harness._lease_path("009").exists()
    assert harness._session_path("tap-cleanup-failed").exists()


def _make_prunable_session(harness: LabHarness, run_id: str, status: str, *, modified_ns: int) -> Path:
    session = harness._session_path(run_id)
    session.mkdir()
    harness._write_session_marker(session, run_id)
    atomic_write_json(
        session / "state.json",
        {
            "schema_version": 1,
            "run_id": run_id,
            "status": status,
            "tap": {"name": "nddmtestdead", "ifindex": 999999},
            "capture": None,
        },
    )
    os.utime(session, ns=(modified_ns, modified_ns))
    return session


def test_prune_is_dry_run_scoped_and_keeps_newest_failure(tmp_path: Path) -> None:
    harness = LabHarness(_make_configuration(tmp_path))
    harness.initialize_state_root()
    completed = _make_prunable_session(harness, "completed-run", "completed", modified_ns=1)
    older_failure = _make_prunable_session(harness, "failed-old", "failed", modified_ns=2)
    newest_failure = _make_prunable_session(harness, "failed-new", "failed", modified_ns=3)
    foreign = harness.state_root / "sessions" / "foreign"
    foreign.mkdir()

    preview = harness.prune()

    candidate_paths = {record["path"] for record in preview["candidates"]}
    assert candidate_paths == {str(completed), str(older_failure)}
    assert preview["retained_latest_failure"] == str(newest_failure)
    assert preview["removed"] == []
    assert completed.exists() and older_failure.exists() and newest_failure.exists() and foreign.exists()

    applied = harness.prune(apply=True)
    assert {record["path"] for record in applied["removed"]} == candidate_paths
    assert not completed.exists() and not older_failure.exists()
    assert newest_failure.exists() and foreign.exists()


@pytest.mark.parametrize("value", ["../capture.pcap", "/tmp/capture.pcap", "a/../../b", "."])
def test_promotion_paths_cannot_escape_the_owned_session(value: str) -> None:
    with pytest.raises(LabError, match="unsafe relative"):
        _safe_relative_path(value)


def test_atomic_json_is_deterministic_and_private(tmp_path: Path) -> None:
    path = tmp_path / "record.json"
    atomic_write_json(path, {"b": 2, "a": 1})

    assert path.read_text(encoding="utf-8") == '{\n  "a": 1,\n  "b": 2\n}\n'
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text()) == {"a": 1, "b": 2}


def test_handwritten_source_and_tests_stay_below_nine_hundred_lines() -> None:
    repository = Path(__file__).resolve().parents[1]
    roots = [repository / "packages", repository / "tests", repository / "tools", repository / "website"]
    suffixes = {".c", ".css", ".h", ".html", ".js", ".jsx", ".md", ".py", ".rs", ".sh", ".ts", ".tsx"}
    excluded_directories = {"__pycache__", "node_modules", "target"}
    oversized = {}
    for root in roots:
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in suffixes:
                continue
            if excluded_directories.intersection(path.relative_to(repository).parts):
                continue
            content = path.read_bytes()
            line_count = content.count(b"\n") + int(bool(content) and not content.endswith(b"\n"))
            if line_count > 900:
                oversized[str(path.relative_to(repository))] = line_count

    assert oversized == {}
