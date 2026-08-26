"""Transactional stop and bounded retention for the DDM lab harness."""

from __future__ import annotations

import os
import shutil
import signal
from pathlib import Path
from typing import Any

from ._lifecycle_support import (
    SCHEMA_VERSION,
    LabError,
    QmpClient,
    _locked,
    atomic_write_json,
    owned_process_status,
    process_matches,
    read_json,
    sha256_file,
    utc_now,
    validate_interface,
    validate_run_id,
)


class CleanupMixin:
    """Stop exact owned resources and prune only authenticated inactive runs."""

    def _final_summary(self, state: dict[str, Any]) -> dict[str, Any]:
        captures = []
        for record in state.get("captures", []):
            path = Path(record["path"])
            captures.append(
                {
                    "sequence": record["sequence"],
                    "size": record.get("size", path.stat().st_size if path.exists() else 0),
                    "sha256": record.get("sha256", sha256_file(path) if path.exists() else None),
                    "retained": False,
                }
            )
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": state["run_id"],
            "identity": state["identity"],
            "created_at": state["created_at"],
            "finished_at": utc_now(),
            "outcome": state.get("status"),
            "purpose": state.get("purpose", "interactive"),
            "qemu_sha256": state["validated_inputs"]["qemu"]["sha256"],
            "image_manifest_sha256": state["validated_inputs"]["image"]["manifest"]["sha256"],
            "captures": captures,
            "promotions": [record.get("path") for record in state.get("promotions", [])],
            "raw_session_retained": False,
            "protocol_work_performed": False,
        }

    def _prune_history(self, keep: int = 20) -> None:
        history = self.state_root / "history"
        entries = []
        for path in history.glob("*.json"):
            record = read_json(path)
            run_id = record.get("run_id")
            if record.get("outcome") in {"cleanup_pending", "cleanup_failed"}:
                continue
            if isinstance(run_id, str) and (self.state_root / "discarding" / run_id).exists():
                continue
            entries.append(path)
        entries.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        for path in entries[keep:]:
            path.unlink()

    @staticmethod
    def _with_capture_retention(
        summary: dict[str, Any],
        state: dict[str, Any],
        session: Path,
        discard: Path,
    ) -> dict[str, Any]:
        result = {**summary, "captures": [dict(record) for record in summary.get("captures", [])]}
        for compact, source in zip(result["captures"], state.get("captures", [])):
            source_path = Path(source["path"])
            candidates = [source_path]
            try:
                candidates.append(discard / source_path.relative_to(session))
            except ValueError:
                pass
            compact["retained"] = any(path.is_file() for path in candidates)
        return result

    def _lease_run_ids(self) -> set[str]:
        run_ids = set()
        for lease in (self.state_root / "leases").glob("identity-*.lease"):
            identity = lease.name[len("identity-") : -len(".lease")]
            run_ids.add(str(self._read_local_lease(identity)["run_id"]))
        return run_ids

    @staticmethod
    def _prune_process_status(
        pid: Any, start_ticks: Any, required_argument: Any, label: str, *, allow_unrecorded: bool
    ) -> str:
        if pid is None and start_ticks is None:
            if allow_unrecorded:
                return "absent"
            raise LabError(f"{label} process ownership record is incomplete")
        if (
            isinstance(pid, bool)
            or not isinstance(pid, int)
            or pid <= 1
            or isinstance(start_ticks, bool)
            or not isinstance(start_ticks, int)
            or not isinstance(required_argument, str)
        ):
            raise LabError(f"{label} process ownership record is unresolved")
        return owned_process_status(pid, start_ticks, required_argument)

    def _inspect_prunable_session(self, session: Path, lease_run_ids: set[str]) -> dict[str, Any]:
        sessions = self.state_root / "sessions"
        if session.is_symlink() or not session.is_dir() or session.parent != sessions:
            raise LabError("session path is not an owned session directory")
        run_id = validate_run_id(session.name)
        self._verify_session_marker(session, run_id)
        state_path = session / "state.json"
        if state_path.is_symlink():
            raise LabError("session state must not be a symlink")
        state = read_json(state_path)
        if state.get("schema_version") != SCHEMA_VERSION or state.get("run_id") != run_id:
            raise LabError("marker/state binding is invalid")
        if run_id in lease_run_ids:
            raise LabError("session is still leased")
        qemu_status = self._prune_process_status(
            state.get("qemu_pid"),
            state.get("qemu_start_ticks"),
            state.get("qmp_socket"),
            "QEMU",
            allow_unrecorded=True,
        )
        if qemu_status == "active":
            raise LabError("QEMU process is still active")
        if qemu_status == "mismatch":
            raise LabError("QEMU process argument mismatch is unresolved")
        tap = state.get("tap")
        if not isinstance(tap, dict):
            raise LabError("invalid TAP ownership record")
        tap_name = tap.get("name")
        tap_index = tap.get("ifindex")
        if not isinstance(tap_name, str) or isinstance(tap_index, bool) or not isinstance(tap_index, int):
            raise LabError("invalid TAP ownership record")
        validate_interface(tap_name)
        if tap_index <= 0:
            raise LabError("invalid TAP ownership record")
        capture = state.get("capture")
        if capture is not None and not isinstance(capture, dict):
            raise LabError("invalid capture process ownership record")
        if isinstance(capture, dict):
            capture_status = self._prune_process_status(
                capture.get("pid"), capture.get("start_ticks"), tap_name, "capture", allow_unrecorded=False
            )
            if capture_status == "active":
                raise LabError("capture process is still active")
            if capture_status == "mismatch":
                raise LabError("capture process argument mismatch is unresolved")
        if (Path("/sys/class/net") / tap_name).exists():
            raise LabError("recorded TAP name is still present")
        renamed = self._interface_name_for_index(tap_index)
        if renamed is not None:
            raise LabError(f"recorded TAP ifindex is still present as {renamed}")
        return state

    def _inspect_incomplete_discard(self, path: Path, lease_run_ids: set[str]) -> str:
        root = self.state_root / "discarding"
        if path.is_symlink() or not path.is_dir() or path.parent != root:
            raise LabError("discard path is not an owned directory")
        run_id = validate_run_id(path.name)
        if run_id in lease_run_ids:
            raise LabError("incomplete discard is still leased")
        history = read_json(self.state_root / "history" / f"{run_id}.json")
        if history.get("run_id") != run_id or history.get("outcome") not in {"cleanup_pending", "cleanup_failed"}:
            raise LabError("incomplete discard has no matching cleanup history")
        return run_id

    def _stage_prune(self, session: Path, reason: str) -> tuple[Path, Path, dict[str, Any]]:
        run_id = validate_run_id(session.name)
        discard = self.state_root / "discarding" / run_id
        if discard.exists():
            raise LabError(f"discard staging path already exists: {discard}")
        history_path = self.state_root / "history" / f"{run_id}.json"
        history = (
            read_json(history_path) if history_path.exists() else {"schema_version": SCHEMA_VERSION, "run_id": run_id}
        )
        state_path = session / "state.json"
        purpose = read_json(state_path).get("purpose", "interactive") if state_path.exists() else "interactive"
        pending = {
            **history,
            "outcome": "cleanup_pending",
            "purpose": purpose,
            "raw_session_retained": True,
            "prune_reason": reason,
        }
        atomic_write_json(history_path, pending)
        os.replace(session, discard)
        return discard, history_path, pending

    def _finish_staged_prune(
        self,
        discard: Path,
        history_path: Path,
        pending: dict[str, Any],
    ) -> None:
        try:
            shutil.rmtree(discard)
        except OSError as error:
            failed = {**pending, "outcome": "cleanup_failed", "cleanup_error": str(error)}
            try:
                atomic_write_json(history_path, failed)
            except OSError:
                pass
            raise LabError(f"staged prune failed: {error}") from error
        atomic_write_json(
            history_path,
            {**pending, "outcome": "pruned", "raw_session_retained": False, "pruned_at": utc_now()},
        )

    @_locked(True)
    def stop(self, identity: str = "001", *, retain_session: bool = False) -> dict[str, Any]:
        state = self.state_for_identity(identity)
        run_id = state["run_id"]
        session = self._session_path(run_id)
        discard = self.state_root / "discarding" / run_id
        history_path = self.state_root / "history" / f"{run_id}.json"
        summary: dict[str, Any] | None = None
        pending_summary: dict[str, Any] | None = None
        lease_released = False
        try:
            self._stop_capture_in_state(state)
            if process_matches(state.get("qemu_pid"), state.get("qemu_start_ticks"), state.get("qmp_socket")):
                try:
                    QmpClient(Path(state["qmp_socket"]), timeout=10).execute("quit")
                except LabError:
                    pass
            self._terminate_exact_process(
                state.get("qemu_pid"),
                state.get("qemu_start_ticks"),
                state["qmp_socket"],
                first_signal=signal.SIGTERM,
            )
            self._delete_tap(state["tap"]["name"], state["tap"]["ifindex"], tolerate_missing=True)
            state["status"] = "completed"
            state["finished_at"] = utc_now()
            state["retention"] = "explicit" if retain_session else "discard"
            self._write_state(state)
            summary = self._final_summary(state)
            if retain_session:
                summary["raw_session_retained"] = True
                summary = self._with_capture_retention(summary, state, session, discard)
            pending_summary = self._with_capture_retention(
                {**summary, "outcome": "cleanup_pending", "raw_session_retained": True},
                state,
                session,
                discard,
            )
            atomic_write_json(history_path, pending_summary)
            if not retain_session:
                self._verify_session_marker(session, run_id)
                if session.parent != self.state_root / "sessions" or discard.exists():
                    raise LabError("refusing to stage an invalid session discard")
                os.replace(session, discard)
            try:
                self._release_lease(identity, run_id)
                lease_released = True
            except BaseException:
                if discard.exists() and not session.exists():
                    os.replace(discard, session)
                raise
            if not retain_session:
                shutil.rmtree(discard)
            atomic_write_json(history_path, summary)
            try:
                self._prune_history()
            except BaseException as error:
                summary["history_prune_error"] = str(error)
                try:
                    atomic_write_json(history_path, summary)
                except BaseException:
                    pass
            return summary
        except BaseException as error:
            state["status"] = "cleanup_failed"
            if not lease_released and session.is_dir():
                try:
                    self._verify_session_marker(session, run_id)
                    self._write_state(state)
                except BaseException:
                    pass
            if summary is not None:
                retained = session.exists() or discard.exists()
                failed = self._with_capture_retention(
                    {
                        **(pending_summary or summary),
                        "outcome": "cleanup_failed",
                        "cleanup_error": str(error),
                        "raw_session_retained": retained,
                        "discard_path": str(discard) if discard.exists() else None,
                    },
                    state,
                    session,
                    discard,
                )
                try:
                    atomic_write_json(history_path, failed)
                except BaseException:
                    pass
            raise

    @_locked(True)
    def prune(self, *, apply: bool = False) -> dict[str, Any]:
        self.initialize_state_root()
        leases = self._lease_run_ids()
        failed: list[tuple[float, Path, dict[str, Any]]] = []
        removable: list[tuple[Path, str, dict[str, Any] | None]] = []
        refused: list[tuple[Path, str]] = []
        for session in sorted((self.state_root / "sessions").iterdir()):
            if not session.is_dir():
                continue
            try:
                state = self._inspect_prunable_session(session, leases)
            except LabError as error:
                refused.append((session, str(error)))
                continue
            status = state.get("status")
            if status == "failed":
                failed.append((session.stat().st_mtime, session, state))
            elif status == "completed":
                if state.get("retention") == "explicit":
                    refused.append((session, "session was explicitly retained"))
                else:
                    removable.append((session, status, state))
            else:
                refused.append((session, f"unresolved state: {status!r}"))
        failed.sort(reverse=True)
        for _, session, state in failed[1:]:
            removable.append((session, "superseded failure", state))
        for path in sorted((self.state_root / "discarding").iterdir()):
            try:
                self._inspect_incomplete_discard(path, leases)
            except LabError as error:
                refused.append((path, str(error)))
            else:
                removable.append((path, "incomplete discard", None))
        removed = []
        if apply:
            for path, reason, expected_state in removable:
                try:
                    if expected_state is None:
                        run_id = self._inspect_incomplete_discard(path, self._lease_run_ids())
                        history_path = self.state_root / "history" / f"{run_id}.json"
                        self._finish_staged_prune(path, history_path, read_json(history_path))
                    else:
                        current = self._inspect_prunable_session(path, self._lease_run_ids())
                        if current != expected_state:
                            raise LabError("session state changed during prune")
                        discard, history_path, pending = self._stage_prune(path, reason)
                        self._finish_staged_prune(discard, history_path, pending)
                except (LabError, OSError) as error:
                    refused.append((path, str(error)))
                    continue
                removed.append({"path": str(path), "reason": reason})
        return {
            "dry_run": not apply,
            "candidates": [{"path": str(path), "reason": reason} for path, reason, _ in removable],
            "removed": removed,
            "retained_latest_failure": str(failed[0][1]) if failed else None,
            "refused": [{"path": str(path), "reason": reason} for path, reason in refused],
        }

    @_locked(True)
    def prune_smoke_failures(self, current_run_id: str | None, *, apply: bool = False) -> dict[str, Any]:
        """Prune superseded failed smoke runs without touching interactive runs."""
        self.initialize_state_root()
        if current_run_id is not None:
            current_run_id = validate_run_id(current_run_id)
        leases = self._lease_run_ids()
        failures: list[tuple[float, Path, dict[str, Any]]] = []
        staged: list[Path] = []
        refused = []
        for session in sorted((self.state_root / "sessions").glob("smoke-*")):
            try:
                state = self._inspect_prunable_session(session, leases)
            except LabError as error:
                refused.append({"path": str(session), "reason": str(error)})
                continue
            if state.get("status") != "failed" or state.get("purpose") != "smoke":
                refused.append(
                    {
                        "path": str(session),
                        "reason": f"not a bound failed smoke: {state.get('status')!r}/{state.get('purpose')!r}",
                    }
                )
                continue
            failures.append((session.stat().st_mtime, session, state))
        for path in sorted((self.state_root / "discarding").iterdir()):
            try:
                run_id = self._inspect_incomplete_discard(path, leases)
                history = read_json(self.state_root / "history" / f"{run_id}.json")
                if history.get("purpose") != "smoke" or run_id == current_run_id:
                    continue
            except LabError as error:
                refused.append({"path": str(path), "reason": str(error)})
            else:
                staged.append(path)
        failures.sort(reverse=True)
        retained = next(
            (item for item in failures if item[1].name == current_run_id), failures[0] if failures else None
        )
        candidates = [item for item in failures if retained is None or item[1] != retained[1]]
        removed = []
        if apply:
            for _, session, expected in candidates:
                try:
                    current = self._inspect_prunable_session(session, self._lease_run_ids())
                    if current != expected:
                        raise LabError("smoke failure state changed during prune")
                    discard, history_path, pending = self._stage_prune(session, "superseded smoke failure")
                    self._finish_staged_prune(discard, history_path, pending)
                except (LabError, OSError) as error:
                    refused.append({"path": str(session), "reason": str(error)})
                    continue
                removed.append({"path": str(session), "reason": "superseded smoke failure"})
            for path in staged:
                try:
                    run_id = self._inspect_incomplete_discard(path, self._lease_run_ids())
                    history_path = self.state_root / "history" / f"{run_id}.json"
                    self._finish_staged_prune(path, history_path, read_json(history_path))
                except (LabError, OSError) as error:
                    refused.append({"path": str(path), "reason": str(error)})
                    continue
                removed.append({"path": str(path), "reason": "incomplete smoke discard"})
        return {
            "dry_run": not apply,
            "candidates": [str(session) for _, session, _ in candidates] + [str(path) for path in staged],
            "removed": removed,
            "retained_latest_failure": str(retained[1]) if retained else None,
            "refused": refused,
        }
