"""Snapshot, capture, and evidence-promotion methods for the DDM lab harness."""

from __future__ import annotations

import os
import re
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

from ._lifecycle_support import (
    DEFAULT_CAPTURE_MIB,
    DEFAULT_CAPTURE_SECONDS,
    DEFAULT_RAM_BASE,
    DEFAULT_RAM_SIZE,
    SCHEMA_VERSION,
    SNAPSHOT_PATTERN,
    LabError,
    QmpClient,
    _locked,
    _process_start_ticks,
    _process_state,
    _safe_relative_path,
    _validate_token,
    atomic_write_json,
    owned_process_status,
    process_matches,
    sha256_file,
    utc_now,
)


DEFAULT_SESSION_MIB = 512
_MAC_ADDRESS = re.compile(r"(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}")


class EvidenceMixin:
    """Provide bounded transient evidence operations to LabHarness."""

    def _session_size(self, session: Path) -> int:
        total = 0
        for path in session.rglob("*"):
            if path.is_symlink():
                raise LabError(f"symlinks are not allowed in a lab session: {path}")
            if path.is_file():
                total += path.stat().st_size
        return total

    def _require_session_capacity(self, session: Path, additional_bytes: int) -> None:
        limit = DEFAULT_SESSION_MIB * 1024 * 1024
        if additional_bytes < 0 or self._session_size(session) + additional_bytes > limit:
            raise LabError(f"session would exceed the {DEFAULT_SESSION_MIB} MiB aggregate limit")
        if shutil.disk_usage(session).free < additional_bytes + 16 * 1024 * 1024:
            raise LabError("insufficient tmpfs space for the bounded lab artifact")

    def _owned_session_file(self, state: dict[str, Any], value: Any) -> Path:
        if not isinstance(value, str):
            raise LabError("session artifact path is invalid")
        session = self._session_path(state["run_id"]).resolve(strict=True)
        path = Path(value).resolve(strict=False)
        if path.parent != session:
            raise LabError("session artifact path is outside its owned run")
        return path

    @_locked(True)
    def snapshot(self, identity: str = "001", name: str = "snapshot") -> dict[str, Any]:
        _validate_token(name, SNAPSHOT_PATTERN, "snapshot name")
        state, qmp_status = self._execute_qmp_for_identity(identity, "query-status")
        if isinstance(state.get("capture"), dict):
            raise LabError("stop the active capture before taking a RAM snapshot")
        was_running = isinstance(qmp_status, dict) and qmp_status.get("running") is True
        session = self._session_path(state["run_id"])
        snapshots = session / "snapshots"
        snapshots.mkdir(mode=0o700, exist_ok=True)
        path = snapshots / f"{name}.ram"
        partial = snapshots / f".{name}.ram.partial"
        if path.exists() or partial.exists():
            raise LabError(f"snapshot already exists: {name}")
        self._require_session_capacity(session, DEFAULT_RAM_SIZE)
        paused_by_us = False
        snapshot_error: BaseException | None = None
        restore_error: BaseException | None = None
        record: dict[str, Any] | None = None
        record_committed = False
        try:
            if was_running:
                QmpClient(Path(state["qmp_socket"]), timeout=10).execute("stop")
                paused_by_us = True
                paused = QmpClient(Path(state["qmp_socket"]), timeout=10).execute("query-status")
                if not isinstance(paused, dict) or paused.get("running") is not False:
                    raise LabError("QEMU did not enter the paused state for the snapshot")
                state["status"] = "paused"
                self._write_state(state)
            QmpClient(Path(state["qmp_socket"]), timeout=60).execute(
                "pmemsave",
                {"val": DEFAULT_RAM_BASE, "size": DEFAULT_RAM_SIZE, "filename": str(partial)},
            )
            if partial.stat().st_size != DEFAULT_RAM_SIZE:
                raise LabError("RAM snapshot has the wrong size")
            digest = sha256_file(partial)
            os.replace(partial, path)
            record = {
                "name": name,
                "path": str(path),
                "size": path.stat().st_size,
                "sha256": digest,
                "created_at": utc_now(),
                "retention": "ephemeral unless explicitly promoted",
            }
            state.setdefault("snapshots", []).append(record)
            try:
                self._write_state(state)
            except BaseException:
                state["snapshots"].pop()
                raise
            record_committed = True
        except BaseException as error:
            snapshot_error = error
            partial.unlink(missing_ok=True)
            if not record_committed:
                path.unlink(missing_ok=True)
        finally:
            if paused_by_us:
                try:
                    QmpClient(Path(state["qmp_socket"]), timeout=10).execute("cont")
                    running = QmpClient(Path(state["qmp_socket"]), timeout=10).execute("query-status")
                    if not isinstance(running, dict) or running.get("running") is not True:
                        raise LabError("QEMU did not resume after the snapshot")
                    state["status"] = "running"
                    self._write_state(state)
                except BaseException as error:
                    restore_error = error
        if snapshot_error is not None:
            if restore_error is not None:
                raise LabError(f"{snapshot_error}; QEMU restore also failed: {restore_error}") from snapshot_error
            raise snapshot_error
        if restore_error is not None:
            raise restore_error
        assert record is not None
        return record

    def _start_capture_in_state(self, state: dict[str, Any], seconds: int, maximum_mib: int) -> dict[str, Any]:
        if state.get("capture") and process_matches(
            state["capture"].get("pid"), state["capture"].get("start_ticks"), state["tap"]["name"]
        ):
            raise LabError("capture is already running")
        if not 1 <= seconds <= 3600 or not 1 <= maximum_mib <= 256:
            raise LabError("capture bounds must be 1..3600 seconds and 1..256 MiB")
        session = self._session_path(state["run_id"])
        self._require_session_capacity(session, maximum_mib * 1024 * 1024)
        mac_address = state.get("identity", {}).get("mac_address")
        if not isinstance(mac_address, str) or not _MAC_ADDRESS.fullmatch(mac_address):
            raise LabError("capture identity has an invalid MAC address")
        sequence = len(state.get("captures", [])) + 1
        path = session / f"capture-{sequence:02d}.pcap"
        if path.exists():
            raise LabError(f"capture output already exists: {path.name}")
        capture_filter = f"ether host {mac_address.lower()} or ether broadcast or ether multicast"
        command = [
            "dumpcap",
            "-q",
            "-P",
            "-i",
            state["tap"]["name"],
            "-s",
            "0",
            "-f",
            capture_filter,
            "-a",
            f"duration:{seconds}",
            "-a",
            f"filesize:{maximum_mib * 1024}",
            "-w",
            str(path),
        ]
        process: subprocess.Popen[bytes] | None = None
        start_ticks: int | None = None
        previous_capture = state.get("capture")
        records = state.setdefault("captures", [])
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
            start_ticks = _process_start_ticks(process.pid)
            if start_ticks is None:
                process.terminate()
                process.wait(timeout=5)
                raise LabError("cannot identify capture process")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if path.exists() and path.stat().st_size >= 24:
                    break
                if not process_matches(process.pid, start_ticks, state["tap"]["name"]):
                    raise LabError("capture backend exited before becoming ready")
                time.sleep(0.05)
            else:
                raise LabError("capture backend did not become ready")
            record = {
                "sequence": sequence,
                "path": str(path),
                "pid": process.pid,
                "start_ticks": start_ticks,
                "started_at": utc_now(),
                "maximum_seconds": seconds,
                "maximum_mib": maximum_mib,
                "capture_filter": capture_filter,
                "scope": "guest traffic plus broadcasts/multicasts visible on its TAP",
            }
            state["capture"] = record
            records.append(record)
            try:
                self._write_state(state)
            except BaseException:
                records.pop()
                state["capture"] = previous_capture
                raise
            return record
        except BaseException as error:
            cleanup_error: BaseException | None = None
            if process is not None and start_ticks is not None:
                try:
                    self._terminate_exact_process(
                        process.pid,
                        start_ticks,
                        state["tap"]["name"],
                        first_signal=signal.SIGTERM,
                    )
                except BaseException as caught:
                    cleanup_error = caught
            if cleanup_error is None:
                path.unlink(missing_ok=True)
            if cleanup_error is not None:
                raise LabError(f"{error}; capture cleanup also failed: {cleanup_error}") from error
            raise

    @_locked(True)
    def capture_start(
        self, identity: str = "001", *, seconds: int = DEFAULT_CAPTURE_SECONDS, maximum_mib: int = DEFAULT_CAPTURE_MIB
    ) -> dict[str, Any]:
        state = self.state_for_identity(identity)
        if state.get("status") not in {"running", "paused"}:
            raise LabError(f"capture cannot start while the run is {state.get('status')!r}")
        if (
            owned_process_status(state.get("qemu_pid"), state.get("qemu_start_ticks"), state.get("qmp_socket"))
            != "active"
        ):
            raise LabError("capture requires the exact active QEMU process")
        tap = state.get("tap")
        if not isinstance(tap, dict) or not isinstance(tap.get("name"), str) or not isinstance(tap.get("ifindex"), int):
            raise LabError("capture TAP ownership is invalid")
        if self._interface_index(tap["name"]) != tap["ifindex"]:
            raise LabError("capture TAP no longer has its recorded interface index")
        return self._start_capture_in_state(state, seconds, maximum_mib)

    def _stop_capture_in_state(self, state: dict[str, Any]) -> dict[str, Any] | None:
        record = state.get("capture")
        if not isinstance(record, dict):
            return None
        pid = record.get("pid")
        start_ticks = record.get("start_ticks")
        self._terminate_exact_process(
            pid,
            start_ticks,
            state["tap"]["name"],
            first_signal=signal.SIGINT,
        )
        path = self._owned_session_file(state, record.get("path"))
        record["stopped_at"] = utc_now()
        record["size"] = path.stat().st_size if path.exists() else 0
        record["sha256"] = sha256_file(path) if path.exists() else None
        state["capture"] = None
        self._write_state(state)
        return record

    @staticmethod
    def _terminate_exact_process(
        pid: Any,
        start_ticks: Any,
        required_argument: str,
        *,
        first_signal: signal.Signals,
    ) -> None:
        if (
            isinstance(pid, bool)
            or not isinstance(pid, int)
            or isinstance(start_ticks, bool)
            or not isinstance(start_ticks, int)
            or _process_start_ticks(pid) != start_ticks
        ):
            return
        if not process_matches(pid, start_ticks, required_argument):
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if _process_start_ticks(pid) != start_ticks:
                    return
                if _process_state(pid) == "Z":
                    try:
                        os.waitpid(pid, os.WNOHANG)
                    except ChildProcessError:
                        pass
                    return
                if process_matches(pid, start_ticks, required_argument):
                    break
                time.sleep(0.05)
            else:
                raise LabError(f"owned process {pid} no longer has its recorded command argument")
        for requested_signal, wait_seconds in (
            (first_signal, 5.0),
            (signal.SIGTERM, 5.0),
            (signal.SIGKILL, 2.0),
        ):
            if not process_matches(pid, start_ticks, required_argument):
                return
            os.kill(pid, requested_signal)
            deadline = time.monotonic() + wait_seconds
            while time.monotonic() < deadline:
                try:
                    waited, _ = os.waitpid(pid, os.WNOHANG)
                except ChildProcessError:
                    waited = 0
                if waited == pid or not process_matches(pid, start_ticks, required_argument):
                    return
                time.sleep(0.05)
        if process_matches(pid, start_ticks, required_argument):
            raise LabError(f"owned process {pid} did not terminate")

    def _save_serial_tail(self, state: dict[str, Any]) -> dict[str, Any] | None:
        if not process_matches(state.get("qemu_pid"), state.get("qemu_start_ticks"), state.get("qmp_socket")):
            return None
        value = QmpClient(Path(state["qmp_socket"]), timeout=5).execute(
            "ringbuf-read", {"device": "dante-serial", "size": 65536}
        )
        if not isinstance(value, str):
            raise LabError("QEMU serial ring returned a non-text value")
        encoded = value.encode("utf-8", errors="replace")[-65536:]
        path = self._session_path(state["run_id"]) / "serial-tail.log"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        record = {"path": str(path), "size": len(encoded), "sha256": sha256_file(path)}
        state["serial_tail"] = record
        return record

    @_locked(True)
    def capture_stop(self, identity: str = "001") -> dict[str, Any] | None:
        return self._stop_capture_in_state(self.state_for_identity(identity))

    @_locked(True)
    def capture_discard(self, identity: str = "001") -> dict[str, Any]:
        """Discard stopped raw captures while retaining their compact hashes."""

        state = self.state_for_identity(identity)
        if isinstance(state.get("capture"), dict):
            raise LabError("stop the active capture before discarding capture files")
        removed: list[dict[str, Any]] = []
        for record in state.get("captures", []):
            if not isinstance(record, dict):
                raise LabError("capture history is invalid")
            path = self._owned_session_file(state, record.get("path"))
            if not path.exists() and record.get("discarded_at"):
                continue
            size = path.stat().st_size if path.exists() else record.get("size", 0)
            digest = sha256_file(path) if path.exists() else record.get("sha256")
            path.unlink(missing_ok=True)
            record["size"] = size
            record["sha256"] = digest
            record["discarded_at"] = utc_now()
            record["retained"] = False
            removed.append({"sequence": record.get("sequence"), "size": size, "sha256": digest})
        self._write_state(state)
        return {
            "identity": state["identity"]["suffix"],
            "removed_count": len(removed),
            "removed_bytes": sum(record["size"] for record in removed if isinstance(record["size"], int)),
            "captures": removed,
        }

    @_locked(True)
    def promote(
        self,
        identity: str,
        *,
        claim: str,
        evidence_class: str,
        artifacts: list[str],
    ) -> dict[str, Any]:
        if evidence_class not in {"documented", "observed", "causal", "inferred", "unknown"}:
            raise LabError("invalid evidence class")
        if not claim.strip() or len(claim.encode("utf-8")) > 4096:
            raise LabError("promotion requires a concise claim")
        if not artifacts:
            raise LabError("promotion requires at least one explicit artifact")
        state = self.state_for_identity(identity)
        session = self._session_path(state["run_id"]).resolve(strict=True)
        selected: list[tuple[Path, Path]] = []
        total = 0
        for name in artifacts:
            relative = _safe_relative_path(name)
            source = (session / relative).resolve(strict=True)
            if session not in source.parents or not source.is_file():
                raise LabError(f"artifact is outside this run: {name}")
            if (
                relative.name == "flash.runtime.bin"
                or relative.suffix == ".ram"
                or relative.name.startswith("qemu-")
                or relative.name == "serial-tail.log"
                or "credential" in relative.name.lower()
            ):
                raise LabError(f"artifact is prohibited from routine promotion: {name}")
            total += source.stat().st_size
            if total > 256 * 1024 * 1024:
                raise LabError("promotion exceeds the 256 MiB explicit-artifact cap")
            selected.append((relative, source))
        destination = self.configuration.promotion_root.absolute() / state["run_id"]
        if destination.exists():
            raise LabError(f"promotion already exists: {destination}")
        destination.mkdir(parents=True, mode=0o700)
        records = []
        try:
            for relative, source in selected:
                target = destination / relative.name
                if target.exists():
                    raise LabError(f"promotion artifact basenames collide: {relative.name}")
                shutil.copyfile(source, target)
                os.chmod(target, 0o600)
                records.append(
                    {
                        "source": str(relative),
                        "path": target.name,
                        "size": target.stat().st_size,
                        "sha256": sha256_file(target),
                    }
                )
            manifest = {
                "schema_version": SCHEMA_VERSION,
                "run_id": state["run_id"],
                "created_at": utc_now(),
                "claim": claim.strip(),
                "evidence_class": evidence_class,
                "artifacts": records,
                "inputs": {
                    "qemu_sha256": state["validated_inputs"]["qemu"]["sha256"],
                    "image_manifest_sha256": state["validated_inputs"]["image"]["manifest"]["sha256"],
                    "identity_descriptor_sha256": state["identity"]["descriptor_sha256"],
                },
                "boundary": "explicitly promoted minimal evidence; no protocol claim is implied by the harness",
            }
            atomic_write_json(destination / "manifest.json", manifest)
            manifest["manifest_sha256"] = sha256_file(destination / "manifest.json")
            state.setdefault("promotions", []).append({"path": str(destination), **manifest})
            self._write_state(state)
            return manifest
        except BaseException as error:
            try:
                shutil.rmtree(destination)
            except BaseException as cleanup_error:
                raise LabError(f"{error}; promotion cleanup also failed: {cleanup_error}") from error
            raise
