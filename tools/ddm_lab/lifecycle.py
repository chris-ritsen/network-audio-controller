"""Bounded lifecycle support for the clean-room DDM virtual-device lab.

This module deliberately knows how to launch an opaque, hash-bound emulator image,
but it contains no Dante wire-format knowledge. Every mutable run lives in tmpfs.
"""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import ipaddress
import json
import os
import secrets
import shutil
import socket
import subprocess
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ._lifecycle_cleanup import CleanupMixin
from ._lifecycle_evidence import EvidenceMixin
from ._lifecycle_support import (
    DEFAULT_BRIDGE,
    DEFAULT_CAPTURE_MIB,
    DEFAULT_CAPTURE_SECONDS,
    DEFAULT_IDENTITIES,
    DEFAULT_IDENTITY_SHA256,
    DEFAULT_IMAGE,
    DEFAULT_IMAGE_MANIFEST_SHA256,
    DEFAULT_LOADER_ADDRESS,
    DEFAULT_MAX_ACTIVE_GUESTS,
    DEFAULT_PROMOTION_ROOT,
    DEFAULT_QEMU,
    DEFAULT_QEMU_SHA256,
    DEFAULT_RAM_BASE,
    DEFAULT_RAM_SIZE,
    DEFAULT_STATE_ROOT,
    IDENTITY_PATTERN,
    INTERFACE_PATTERN,
    OWNERSHIP_KEY,
    RUN_ID_PATTERN,
    SCHEMA_VERSION,
    SESSION_MARKER,
    SNAPSHOT_PATTERN,
    STATE_MARKER,
    LabConfiguration,
    LabError,
    QmpClient,
    _admin_command,
    _artifact_record,
    _filesystem_type,
    _json_bytes,
    _locked,
    _process_start_ticks,
    _process_state,
    _run,
    _safe_relative_path,
    _validate_sha256,
    _validate_token,
    atomic_write_json,
    build_qemu_command,
    materialize_runtime_identity,
    process_matches,
    read_json,
    sha256_file,
    utc_now,
    validate_artifacts,
    validate_identity,
    validate_interface,
    validate_run_id,
)


class _TapCreationError(LabError):
    """A TAP was created, but its exact cleanup still needs resolution."""

    def __init__(self, tap: str, tap_index: int | None, cause: BaseException):
        self.tap = tap
        self.tap_index = tap_index
        super().__init__(f"created TAP {tap} could not be configured: {cause}")


class LabHarness(EvidenceMixin, CleanupMixin):
    def __init__(self, configuration: LabConfiguration = LabConfiguration()):
        self.configuration = configuration
        self._thread_lock = threading.RLock()
        self._thread_lock_state = threading.local()

    @property
    def state_root(self) -> Path:
        return self.configuration.state_root.absolute()

    @contextmanager
    def _operation_lock(self, *, exclusive: bool):
        with self._thread_lock:
            depth = getattr(self._thread_lock_state, "depth", 0)
            if depth:
                if exclusive and not getattr(self._thread_lock_state, "exclusive", False):
                    raise LabError("cannot upgrade a shared lab operation lock")
                self._thread_lock_state.depth = depth + 1
                try:
                    yield
                finally:
                    self._thread_lock_state.depth -= 1
                return
            self.initialize_state_root()
            path = self.state_root / ".operations.lock"
            descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
                self._thread_lock_state.depth = 1
                self._thread_lock_state.exclusive = exclusive
                try:
                    yield
                finally:
                    self._thread_lock_state.depth = 0
                    self._thread_lock_state.exclusive = False
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    @contextmanager
    def managed_api_action(self):
        """Serialize a public-API mutation with this harness's lifecycle actions."""
        with self._operation_lock(exclusive=True):
            yield

    @contextmanager
    def topology_action(self):
        """Serialize one multi-guest topology change with all lifecycle actions."""
        with self._operation_lock(exclusive=True):
            yield

    def initialize_state_root(self) -> None:
        root = self.state_root
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(root, 0o700)
        if self.configuration.require_tmpfs and _filesystem_type(root) != "tmpfs":
            raise LabError(f"state root is not tmpfs: {root}")
        marker = root / STATE_MARKER
        if marker.exists():
            value = read_json(marker)
            if value.get("schema_version") != SCHEMA_VERSION:
                raise LabError("lab state marker version is unsupported")
        else:
            atomic_write_json(marker, {"schema_version": SCHEMA_VERSION, "created_at": utc_now()})
        ownership_key = root / OWNERSHIP_KEY
        if not ownership_key.exists():
            try:
                descriptor = os.open(ownership_key, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass
            else:
                try:
                    os.write(descriptor, secrets.token_bytes(32))
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        if ownership_key.stat().st_mode & 0o777 != 0o600 or ownership_key.stat().st_size != 32:
            raise LabError("lab ownership key is missing, exposed, or invalid")
        for name in ("leases", "sessions", "history", "discarding"):
            (root / name).mkdir(exist_ok=True, mode=0o700)

    def _session_ownership_token(self, run_id: str) -> str:
        key = (self.state_root / OWNERSHIP_KEY).read_bytes()
        if len(key) != 32:
            raise LabError("lab ownership key is invalid")
        return hmac.new(key, validate_run_id(run_id).encode("ascii"), hashlib.sha256).hexdigest()

    def _write_session_marker(self, session: Path, run_id: str) -> None:
        atomic_write_json(
            session / SESSION_MARKER,
            {
                "schema_version": SCHEMA_VERSION,
                "run_id": validate_run_id(run_id),
                "ownership_token": self._session_ownership_token(run_id),
            },
        )

    def _verify_session_marker(self, session: Path, run_id: str) -> None:
        marker_path = session / SESSION_MARKER
        if marker_path.is_symlink():
            raise LabError("session ownership marker must not be a symlink")
        marker = read_json(marker_path)
        token = marker.get("ownership_token")
        if (
            marker.get("schema_version") != SCHEMA_VERSION
            or marker.get("run_id") != run_id
            or not isinstance(token, str)
            or not hmac.compare_digest(token, self._session_ownership_token(run_id))
        ):
            raise LabError("session ownership marker is invalid")

    @_locked(False)
    def validate(self, identity: str = "001", *, require_host: bool = True) -> dict[str, Any]:
        self.initialize_state_root()
        validated = validate_artifacts(self.configuration, identity)
        if require_host:
            bridge = validate_interface(self.configuration.bridge)
            result = _run(["ip", "-j", "link", "show", "dev", bridge], timeout=5)
            try:
                links = json.loads(result.stdout)
            except json.JSONDecodeError as error:
                raise LabError("ip link returned invalid JSON") from error
            if not isinstance(links, list) or len(links) != 1 or "UP" not in links[0].get("flags", []):
                raise LabError(f"bridge is not up: {bridge}")
            for command in ("sudo", "ip", "bridge", "dumpcap"):
                if shutil.which(command) is None:
                    raise LabError(f"required command is unavailable: {command}")
            if os.geteuid() != 0:
                _run(["sudo", "-n", "true"], timeout=5)
            validated["host"] = {
                "bridge": bridge,
                "state_root": str(self.state_root),
                "filesystem": _filesystem_type(self.state_root),
                "capture": "bounded dumpcap on the guest TAP only",
            }
        return validated

    def _lease_path(self, identity: str) -> Path:
        return self.state_root / "leases" / f"identity-{validate_identity(identity)}.lease"

    def _session_path(self, run_id: str) -> Path:
        return self.state_root / "sessions" / validate_run_id(run_id)

    def _read_local_lease(self, identity: str, expected_run_id: str | None = None) -> dict[str, Any]:
        identity = validate_identity(identity)
        lease = self._lease_path(identity)
        if lease.is_symlink() or not lease.is_dir():
            raise LabError(f"identity {identity} has no active lease")
        owner_path = lease / "owner.json"
        if owner_path.is_symlink():
            raise LabError("lease owner record must not be a symlink")
        owner = read_json(owner_path)
        run_id = owner.get("run_id")
        if not isinstance(run_id, str):
            raise LabError("lease run identifier is invalid")
        run_id = validate_run_id(run_id)
        if (
            owner.get("schema_version") != SCHEMA_VERSION
            or owner.get("identity") != identity
            or owner.get("host") != socket.gethostname()
            or isinstance(owner.get("user_id"), bool)
            or owner.get("user_id") != os.getuid()
        ):
            raise LabError("lease ownership binding is invalid")
        if expected_run_id is not None and run_id != validate_run_id(expected_run_id):
            raise LabError("lease is owned by another run")
        return owner

    def _create_lease(self, identity: str, run_id: str) -> Path:
        identity = validate_identity(identity)
        run_id = validate_run_id(run_id)
        lease = self._lease_path(identity)
        try:
            lease.mkdir(mode=0o700)
        except FileExistsError as error:
            owner = lease / "owner.json"
            detail = read_json(owner) if owner.exists() else {"state": "unknown"}
            raise LabError(f"identity {identity} is already leased: {detail}") from error
        owner = lease / "owner.json"
        try:
            atomic_write_json(
                owner,
                {
                    "schema_version": SCHEMA_VERSION,
                    "identity": identity,
                    "run_id": run_id,
                    "created_at": utc_now(),
                    "host": socket.gethostname(),
                    "user_id": os.getuid(),
                },
            )
        except BaseException as error:
            cleanup_errors = []
            try:
                owner.unlink(missing_ok=True)
            except OSError as cleanup_error:
                cleanup_errors.append(str(cleanup_error))
            try:
                lease.rmdir()
            except OSError as cleanup_error:
                cleanup_errors.append(str(cleanup_error))
            if cleanup_errors:
                raise LabError(
                    f"lease owner write failed and rollback was incomplete: {'; '.join(cleanup_errors)}"
                ) from error
            raise
        return lease

    def _release_lease(self, identity: str, run_id: str) -> None:
        lease = self._lease_path(identity)
        if not lease.exists():
            return
        self._read_local_lease(identity, run_id)
        (lease / "owner.json").unlink()
        lease.rmdir()

    def _make_run_id(self, identity: str) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"{timestamp}-i{identity}-{uuid.uuid4().hex[:6]}"

    def _tap_name(self, identity: str) -> str:
        return validate_interface(f"nddm{identity}{uuid.uuid4().hex[:6]}")

    def _interface_index(self, interface: str) -> int:
        try:
            return int((Path("/sys/class/net") / interface / "ifindex").read_text().strip())
        except (OSError, ValueError) as error:
            raise LabError(f"cannot read interface index for {interface}") from error

    def _interface_name_for_index(self, interface_index: int) -> str | None:
        if isinstance(interface_index, bool) or not isinstance(interface_index, int) or interface_index <= 0:
            raise LabError("interface index is invalid")
        for candidate in Path("/sys/class/net").iterdir():
            try:
                candidate_index = int((candidate / "ifindex").read_text().strip())
            except (OSError, ValueError):
                continue
            if candidate_index == interface_index:
                return candidate.name
        return None

    def _create_tap(self, tap: str) -> int:
        bridge = validate_interface(self.configuration.bridge)
        create_command = ["ip", "tuntap", "add", "dev", tap, "mode", "tap", "user", str(os.getuid())]
        configure_commands = [
            ["ip", "link", "set", "dev", tap, "mtu", "1500"],
            ["ip", "link", "set", "dev", tap, "master", bridge],
            ["ip", "link", "set", "dev", tap, "up"],
            ["bridge", "mdb", "replace", "dev", bridge, "port", tap, "grp", "224.0.1.129", "permanent"],
        ]
        _run(_admin_command(create_command), timeout=10)
        try:
            created_index = self._interface_index(tap)
        except BaseException as error:
            raise _TapCreationError(tap, None, error) from error
        try:
            for command in configure_commands:
                _run(_admin_command(command), timeout=10)
        except BaseException as error:
            raise _TapCreationError(tap, created_index, error) from error
        return created_index

    def _delete_tap(self, tap: str, expected_index: int | None, *, tolerate_missing: bool = False) -> None:
        validate_interface(tap)
        path = Path("/sys/class/net") / tap
        if not path.exists():
            if expected_index is not None:
                renamed = self._interface_name_for_index(expected_index)
                if renamed is not None:
                    raise LabError(f"owned TAP {tap} was renamed to {renamed}; refusing ambiguous cleanup")
            if tolerate_missing:
                return
            raise LabError(f"TAP is already absent: {tap}")
        actual_index = self._interface_index(tap)
        if expected_index is not None and actual_index != expected_index:
            raise LabError(f"refusing to delete reused interface {tap}")
        _run(_admin_command(["ip", "link", "delete", "dev", tap]), timeout=10)

    def _mac_on_bridge(self, mac_address: str, tap: str | None = None) -> bool:
        result = _run(["bridge", "-j", "fdb", "show", "br", self.configuration.bridge], timeout=5)
        try:
            entries = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise LabError("bridge FDB returned invalid JSON") from error
        return any(
            isinstance(entry, dict)
            and str(entry.get("mac", "")).lower() == mac_address.lower()
            and (tap is None or entry.get("ifname") == tap)
            for entry in entries
        )

    def _ipv4_addresses_for_mac(self, mac_address: str) -> list[str]:
        result = _run(
            ["ip", "-j", "-4", "neigh", "show", "dev", self.configuration.bridge],
            timeout=5,
        )
        try:
            entries = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise LabError("IPv4 neighbor table returned invalid JSON") from error
        addresses: set[str] = set()
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict) or str(entry.get("lladdr", "")).lower() != mac_address.lower():
                continue
            states = entry.get("state", [])
            if isinstance(states, list) and {str(value).upper() for value in states} & {"FAILED", "INCOMPLETE"}:
                continue
            address = entry.get("dst")
            try:
                parsed = ipaddress.ip_address(address)
            except (TypeError, ValueError):
                continue
            if parsed.version == 4:
                addresses.add(str(parsed))
        return sorted(addresses)

    def _wait_qmp(self, state: dict[str, Any], timeout: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        qmp = Path(state["qmp_socket"])
        while time.monotonic() < deadline:
            if not process_matches(state["qemu_pid"], state["qemu_start_ticks"], str(qmp)):
                raise LabError("QEMU exited before QMP became ready")
            if qmp.exists():
                try:
                    result = QmpClient(qmp, timeout=min(3.0, max(0.2, deadline - time.monotonic()))).execute(
                        "query-status"
                    )
                    if isinstance(result, dict):
                        return result
                except LabError:
                    pass
            time.sleep(0.1)
        raise LabError("QMP readiness deadline exceeded")

    def _wait_lan(self, state: dict[str, Any], timeout: float) -> list[str]:
        deadline = time.monotonic() + timeout
        mac_seen = False
        while time.monotonic() < deadline:
            if not process_matches(state["qemu_pid"], state["qemu_start_ticks"], state["qmp_socket"]):
                raise LabError("QEMU exited before LAN readiness")
            if self._mac_on_bridge(state["identity"]["mac_address"], state["tap"]["name"]):
                mac_seen = True
                addresses = self._ipv4_addresses_for_mac(state["identity"]["mac_address"])
                if addresses:
                    return addresses
            time.sleep(0.25)
        if mac_seen:
            raise LabError(
                "guest MAC was observed, but no IPv4 neighbor binding appeared before the readiness deadline"
            )
        raise LabError("guest MAC was not observed on its TAP before the readiness deadline")

    def _state_path(self, run_id: str) -> Path:
        return self._session_path(run_id) / "state.json"

    def _write_state(self, state: dict[str, Any]) -> None:
        state["updated_at"] = utc_now()
        atomic_write_json(self._state_path(state["run_id"]), state)

    def _read_session_state(self, run_id: str, expected_identity: str | None = None) -> tuple[str, dict[str, Any]]:
        run_id = validate_run_id(run_id)
        session = self._session_path(run_id)
        if session.is_symlink() or not session.is_dir() or session.parent != self.state_root / "sessions":
            raise LabError(f"run {run_id} has no owned session")
        self._verify_session_marker(session, run_id)
        state_path = session / "state.json"
        if state_path.is_symlink():
            raise LabError("session state must not be a symlink")
        state = read_json(state_path)
        identity_record = state.get("identity")
        identity = identity_record.get("suffix") if isinstance(identity_record, dict) else None
        if not isinstance(identity, str):
            raise LabError("state identity binding is invalid")
        identity = validate_identity(identity)
        if (
            state.get("schema_version") != SCHEMA_VERSION
            or state.get("run_id") != run_id
            or (expected_identity is not None and identity != validate_identity(expected_identity))
        ):
            raise LabError("session state binding is invalid")
        return identity, state

    @_locked(False)
    def state_for_identity(self, identity: str = "001") -> dict[str, Any]:
        identity = validate_identity(identity)
        owner = self._read_local_lease(identity)
        _, state = self._read_session_state(str(owner["run_id"]), identity)
        return state

    @_locked(False)
    def state_for_run(self, run_id: str) -> dict[str, Any]:
        run_id = validate_run_id(run_id)
        identity, state = self._read_session_state(run_id)
        self._read_local_lease(identity, run_id)
        return state

    @_locked(False)
    def failed_state_for_run(self, run_id: str) -> dict[str, Any]:
        """Read one exact retained failed run, without treating it as active."""
        run_id = validate_run_id(run_id)
        identity, state = self._read_session_state(run_id)
        if state.get("status") not in {"failed", "cleanup_failed"}:
            raise LabError(f"run {run_id} is not a retained failed run")
        lease = self._lease_path(identity)
        if lease.exists():
            self._read_local_lease(identity, run_id)
        return state

    @_locked(False)
    def active_identities(self) -> list[str]:
        """Return the exact synthetic identities currently leased by this harness."""
        identities: list[str] = []
        for lease in sorted((self.state_root / "leases").glob("identity-*.lease")):
            name = lease.name
            encoded = name[len("identity-") : -len(".lease")]
            identity = validate_identity(encoded)
            self._read_local_lease(identity)
            identities.append(identity)
        return identities

    @_locked(True)
    def start(
        self,
        identity: str = "001",
        *,
        run_id: str | None = None,
        purpose: str = "interactive",
        wait_seconds: float = 90.0,
        capture: bool = False,
        capture_seconds: int = DEFAULT_CAPTURE_SECONDS,
        capture_mib: int = DEFAULT_CAPTURE_MIB,
    ) -> dict[str, Any]:
        identity = validate_identity(identity)
        if wait_seconds <= 0 or wait_seconds > 600:
            raise LabError("wait_seconds must be between 0 and 600")
        if purpose not in {"interactive", "smoke", "topology"}:
            raise LabError(f"invalid run purpose: {purpose!r}")
        validated = self.validate(identity)
        active_identities = self.active_identities()
        if len(active_identities) >= self.configuration.max_active_guests:
            raise LabError(
                f"active virtual-device limit reached ({self.configuration.max_active_guests}); "
                "stop a guest or raise the explicit lab limit"
            )
        mac_address = validated["identity"]["mac_address"]
        if self._mac_on_bridge(mac_address):
            raise LabError(f"identity MAC already exists on {self.configuration.bridge}: {mac_address}")
        run_id = validate_run_id(run_id or self._make_run_id(identity))
        if run_id.startswith("smoke-") != (purpose == "smoke"):
            raise LabError("the smoke- run identifier prefix is reserved for smoke runs")
        if run_id.startswith("topology-") != (purpose == "topology"):
            raise LabError("the topology- run identifier prefix is reserved for topology runs")
        if (self.state_root / "history" / f"{run_id}.json").exists():
            raise LabError(f"run identifier already exists in compact history: {run_id}")
        if (self.configuration.promotion_root.absolute() / run_id).exists():
            raise LabError(f"run identifier already exists in promoted evidence: {run_id}")
        self._create_lease(identity, run_id)
        session = self._session_path(run_id)
        session_created = False
        tap: str | None = None
        tap_index: int | None = None
        state: dict[str, Any] | None = None
        process: subprocess.Popen[bytes] | None = None
        try:
            session.mkdir(mode=0o700)
            session_created = True
            self._write_session_marker(session, run_id)
            runtime_flash = session / "flash.runtime.bin"
            source_flash = Path(validated["image"]["artifacts"]["flash"]["path"])
            self._require_session_capacity(session, source_flash.stat().st_size)
            shutil.copyfile(source_flash, runtime_flash)
            os.chmod(runtime_flash, 0o600)
            runtime_digest = sha256_file(runtime_flash)
            if runtime_digest != validated["image"]["artifacts"]["flash"]["sha256"]:
                raise LabError("private runtime flash copy failed integrity validation")
            identity_materialization = materialize_runtime_identity(validated, runtime_flash, session)
            tap = self._tap_name(identity)
            qmp_socket = session / "qmp.sock"
            command = build_qemu_command(
                validated,
                runtime_flash,
                qmp_socket,
                tap,
                run_id,
            )
            tap_index = self._create_tap(tap)
            state = {
                "schema_version": SCHEMA_VERSION,
                "run_id": run_id,
                "status": "starting",
                "purpose": purpose,
                "created_at": utc_now(),
                "identity": validated["identity"],
                "validated_inputs": validated,
                "runtime_flash": {
                    "path": str(runtime_flash),
                    "template_sha256": runtime_digest,
                    "initial_sha256": identity_materialization["runtime_flash_sha256"],
                },
                "identity_materialization": identity_materialization,
                "tap": {"name": tap, "ifindex": tap_index, "bridge": self.configuration.bridge},
                "qmp_socket": str(qmp_socket),
                "qemu_command": command,
                "capture": None,
                "evidence_boundary": "harness validation only; no protocol semantics",
            }
            self._write_state(state)
            if capture:
                self._start_capture_in_state(state, capture_seconds, capture_mib)
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
            state["qemu_pid"] = process.pid
            start_ticks = _process_start_ticks(process.pid)
            if start_ticks is None:
                raise LabError("cannot identify the started QEMU process")
            state["qemu_start_ticks"] = start_ticks
            self._write_state(state)
            state["qmp_status"] = self._wait_qmp(state, min(wait_seconds, 30.0))
            state["lan_ipv4_addresses"] = self._wait_lan(state, wait_seconds)
            state["lan_ready_at"] = utc_now()
            state["status"] = "running"
            self._write_state(state)
            return state
        except BaseException as error:
            cleanup_errors: list[str] = []
            if isinstance(error, _TapCreationError):
                tap_index = error.tap_index
                if tap_index is None:
                    cleanup_errors.append("TAP cleanup: created TAP has no verified interface index; lease retained")
            if state is not None:
                state["status"] = "failed"
                state["failure"] = {"type": type(error).__name__, "message": str(error), "at": utc_now()}
                try:
                    self._save_serial_tail(state)
                except BaseException:
                    pass
                try:
                    self._stop_capture_in_state(state)
                except BaseException as cleanup_error:
                    cleanup_errors.append(f"capture cleanup: {cleanup_error}")
                try:
                    self._write_state(state)
                except BaseException as cleanup_error:
                    cleanup_errors.append(f"state finalization: {cleanup_error}")
            if process is not None and process.poll() is None:
                try:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                except BaseException as cleanup_error:
                    cleanup_errors.append(f"QEMU cleanup: {cleanup_error}")
            if tap is not None and tap_index is not None:
                try:
                    self._delete_tap(tap, tap_index, tolerate_missing=True)
                except BaseException as cleanup_error:
                    cleanup_errors.append(f"TAP cleanup: {cleanup_error}")
            if cleanup_errors:
                if state is not None:
                    state["status"] = "cleanup_failed"
                    state["cleanup_errors"] = cleanup_errors
                    try:
                        self._write_state(state)
                    except BaseException:
                        pass
                raise LabError(f"{error}; cleanup failed: {'; '.join(cleanup_errors)}") from error
            if state is None and session_created and session.exists():
                self._verify_session_marker(session, run_id)
                shutil.rmtree(session)
            self._release_lease(identity, run_id)
            raise

    @_locked(False)
    def status(self, identity: str = "001") -> dict[str, Any]:
        state = self.state_for_identity(identity)
        running = process_matches(state.get("qemu_pid"), state.get("qemu_start_ticks"), state.get("qmp_socket"))
        result = dict(state)
        result["process_running"] = running
        result["tap_present"] = (Path("/sys/class/net") / state["tap"]["name"]).exists()
        result["lan_mac_seen"] = self._mac_on_bridge(state["identity"]["mac_address"], state["tap"]["name"])
        result["lan_ipv4_addresses"] = self._ipv4_addresses_for_mac(state["identity"]["mac_address"])
        capture = state.get("capture")
        result["capture_running"] = isinstance(capture, dict) and process_matches(
            capture.get("pid"),
            capture.get("start_ticks"),
            state["tap"]["name"],
        )
        if running:
            result["qmp_status"] = QmpClient(Path(state["qmp_socket"])).execute("query-status")
        return result

    def _execute_qmp_for_identity(
        self, identity: str, command: str, arguments: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], Any]:
        state = self.state_for_identity(identity)
        if not process_matches(state.get("qemu_pid"), state.get("qemu_start_ticks"), state.get("qmp_socket")):
            raise LabError("QEMU process identity no longer matches the lease")
        result = QmpClient(Path(state["qmp_socket"]), timeout=30).execute(command, arguments)
        return state, result

    @_locked(True)
    def pause(self, identity: str = "001") -> dict[str, Any]:
        state, _ = self._execute_qmp_for_identity(identity, "stop")
        state["status"] = "paused"
        self._write_state(state)
        return state

    @_locked(True)
    def resume(self, identity: str = "001") -> dict[str, Any]:
        state, _ = self._execute_qmp_for_identity(identity, "cont")
        state["status"] = "running"
        self._write_state(state)
        return state
