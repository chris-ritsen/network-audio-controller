"""Shared validation, process, and QMP primitives for the DDM lab lifecycle."""

from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import subprocess
import time
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1
STATE_MARKER = ".netaudio-ddm-lab-v1"
SESSION_MARKER = ".netaudio-ddm-session-v1"
OWNERSHIP_KEY = ".ownership-key"
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
INTERFACE_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,14}")
SNAPSHOT_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,47}")
IDENTITY_PATTERN = re.compile(r"[0-9]{3,5}")

DEFAULT_STATE_ROOT = Path("/dev/shm/netaudio-ddm-lab")
DEFAULT_PROMOTION_ROOT = Path("/home/chris/projects/netaudio/emulation/ddm-lab/promoted")
DEFAULT_QEMU = Path("/home/chris/projects/netaudio/emulation/qemu-src/build-netaudio/qemu-system-microblaze")
DEFAULT_QEMU_SHA256 = "0fe7555a74f89072c1f60a2588688afd08b7c689a258224eed1bf3964bb4af1c"
DEFAULT_IMAGE = Path(
    "/home/chris/projects/netaudio/firmware/emulator_images/"
    "ferrofish-a32-4.0.8.2-brooklyn2-standard-artifact-integrity-verified-synthetic-001"
)
DEFAULT_IMAGE_MANIFEST_SHA256 = "c227be9c3804edf47994d08d041986871d800c3309b1637341d66009cd057dae"
DEFAULT_IDENTITIES = Path("/home/chris/projects/netaudio/firmware/emulator_identities")
DEFAULT_IDENTITY_SHA256 = {
    "001": "f0446e9d350a9b8b50997ea70fa8419193404055daf7699beae4b6946c2e7310",
    "002": "3f5742004d057a2a2c92383e503f07ef9ff13de83d32a9afa9f137d1dbb639a5",
    "003": "980a3f5ae2d272429caa4513daa402fcb4c0f28e7cdf99b6dc865aa41ccace54",
    "004": "0f4812236840dfb608b165d44149aa65ed8ed506db7bcf6d67fb446d4f910a04",
    "005": "689ef9d6b2aa26fc9fd5c18e6e85b37e1a2062f4b85188891002c1b885331ca4",
    "006": "13752a58deff63be958a34f17269d5081263df590cb4bd18ae41fd8eeb4955aa",
    "007": "420338fbe880186979f43024663ebc0e8db9e4b51df2fb22a553d7ddeaa9d06b",
    "008": "af958749d34783f563daddba0995332a75db61e25ac6c6f3271721b6e9080cd7",
}
MAX_IDENTITY_NUMBER = 65535
BOARD_INFORMATION_MAC_OFFSET = 10
DEFAULT_BRIDGE = "br0"
DEFAULT_RAM_BASE = 0x28000000
DEFAULT_RAM_SIZE = 64 * 1024 * 1024
DEFAULT_LOADER_ADDRESS = 0x29FC0000
DEFAULT_CAPTURE_SECONDS = 300
DEFAULT_CAPTURE_MIB = 64
DEFAULT_MAX_ACTIVE_GUESTS = 64
MAX_AF_UNIX_PATH_BYTES = 107


class LabError(RuntimeError):
    """A fail-closed lab lifecycle error."""


def _locked(exclusive: bool):
    def decorate(method):
        @wraps(method)
        def wrapped(self, *args, **kwargs):
            with self._operation_lock(exclusive=exclusive):
                return method(self, *args, **kwargs)

        return wrapped

    return decorate


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def atomic_write_json(path: Path, value: Any, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(_json_bytes(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LabError(f"cannot read valid JSON from {path}: {error}") from error
    if not isinstance(value, dict):
        raise LabError(f"JSON object required in {path}")
    return value


def _validate_token(value: str, pattern: re.Pattern[str], label: str) -> str:
    if not pattern.fullmatch(value):
        raise LabError(f"invalid {label}: {value!r}")
    return value


def validate_identity(value: str) -> str:
    value = _validate_token(value, IDENTITY_PATTERN, "identity")
    number = int(value)
    if not 1 <= number <= MAX_IDENTITY_NUMBER or value != f"{number:03d}":
        raise LabError(f"identity must be canonical decimal 001 through {MAX_IDENTITY_NUMBER}")
    return value


def validate_run_id(value: str) -> str:
    return _validate_token(value, RUN_ID_PATTERN, "run identifier")


def validate_interface(value: str) -> str:
    return _validate_token(value, INTERFACE_PATTERN, "interface")


def _safe_relative_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise LabError(f"unsafe relative artifact path: {value!r}")
    return path


def _qemu_option_path(value: Any, label: str) -> str:
    try:
        path = os.fspath(value)
    except TypeError as error:
        raise LabError(f"{label} is not a filesystem path") from error
    if not isinstance(path, str) or not path:
        raise LabError(f"{label} must be a non-empty text path")
    if "," in path or any(unicodedata.category(character) == "Cc" for character in path):
        raise LabError(f"{label} must not contain commas or control characters")
    return path


def _qmp_socket_path(value: Any) -> str:
    path = _qemu_option_path(value, "QMP socket path")
    if len(os.fsencode(path)) > MAX_AF_UNIX_PATH_BYTES:
        raise LabError(f"QMP socket path exceeds {MAX_AF_UNIX_PATH_BYTES} encoded bytes")
    return path


def _process_start_ticks(pid: int) -> int | None:
    try:
        encoded = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    closing = encoded.rfind(")")
    if closing < 0:
        return None
    fields = encoded[closing + 2 :].split()
    try:
        return int(fields[19])
    except (IndexError, ValueError):
        return None


def _process_state(pid: int) -> str | None:
    try:
        encoded = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    closing = encoded.rfind(")")
    fields = encoded[closing + 2 :].split() if closing >= 0 else []
    return fields[0] if fields else None


def process_matches(pid: Any, start_ticks: Any, required_argument: str | None = None) -> bool:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 1:
        return False
    if isinstance(start_ticks, bool) or not isinstance(start_ticks, int):
        return False
    if _process_start_ticks(pid) != start_ticks:
        return False
    if required_argument is None:
        return True
    if not isinstance(required_argument, str):
        return False
    try:
        arguments = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return False
    expected = os.fsencode(required_argument)
    return any(expected in argument for argument in arguments)


def owned_process_status(pid: Any, start_ticks: Any, required_argument: Any) -> str:
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or isinstance(start_ticks, bool)
        or not isinstance(start_ticks, int)
        or _process_start_ticks(pid) != start_ticks
    ):
        return "absent"
    if _process_state(pid) == "Z":
        return "exited"
    return "active" if process_matches(pid, start_ticks, required_argument) else "mismatch"


@dataclass(frozen=True)
class LabConfiguration:
    state_root: Path = DEFAULT_STATE_ROOT
    promotion_root: Path = DEFAULT_PROMOTION_ROOT
    qemu: Path = DEFAULT_QEMU
    qemu_sha256: str = DEFAULT_QEMU_SHA256
    image_directory: Path = DEFAULT_IMAGE
    image_manifest_sha256: str | None = DEFAULT_IMAGE_MANIFEST_SHA256
    identities_directory: Path = DEFAULT_IDENTITIES
    bridge: str = DEFAULT_BRIDGE
    require_tmpfs: bool = True
    max_active_guests: int = DEFAULT_MAX_ACTIVE_GUESTS


def _validate_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise LabError(f"{label} does not contain a lowercase SHA-256")
    return value


def _artifact_record(manifest: dict[str, Any], key: str) -> tuple[str, int, str]:
    record = manifest.get(key)
    if not isinstance(record, dict):
        raise LabError(f"image manifest has no {key} object")
    filename = record.get("filename")
    size = record.get("size")
    digest = record.get("sha256")
    if not isinstance(filename, str) or Path(filename).name != filename:
        raise LabError(f"image manifest {key} filename is unsafe")
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise LabError(f"image manifest {key} size is invalid")
    return filename, size, _validate_sha256(digest, f"image manifest {key}")


def _run(command: list[str], *, timeout: float = 15.0, check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=check,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise LabError(f"command failed: {command[0]}: {error}") from error


def _admin_command(arguments: Iterable[str]) -> list[str]:
    command = list(arguments)
    return command if os.geteuid() == 0 else ["sudo", "-n", *command]


def _filesystem_type(path: Path) -> str:
    result = _run(["findmnt", "-n", "-o", "FSTYPE", "-T", str(path)], timeout=5)
    return result.stdout.strip()


def _identity_mac_address(identity: str) -> str:
    number = int(validate_identity(identity))
    octets = (0x02, 0x00, 0x00, 0x00, (number >> 8) & 0xFF, number & 0xFF)
    return ":".join(f"{value:02x}" for value in octets)


def _identity_descriptor(configuration: LabConfiguration, identity: str) -> dict[str, Any]:
    identity = validate_identity(identity)
    expected_name = f"ferrofish-a32-synthetic-{identity}"
    expected_mac = _identity_mac_address(identity)
    path = configuration.identities_directory / f"{expected_name}.json"
    if path.is_file():
        resolved = path.resolve(strict=True)
        record = read_json(resolved)
        digest = sha256_file(resolved)
        expected_digest = DEFAULT_IDENTITY_SHA256.get(identity)
        if expected_digest is not None and digest != expected_digest:
            raise LabError(f"identity descriptor hash mismatch: {digest}")
        descriptor_path: str | None = str(resolved)
        derived = False
    else:
        record = {
            "format_version": 2,
            "identity_name": expected_name,
            "identity_kind": "synthetic",
            "media_access_control_address": expected_mac,
            "serial_number": 1,
            "hardware_revision_major": 0,
            "hardware_revision_minor": 0,
            "configuration_date": "20260722",
        }
        digest = hashlib.sha256(_json_bytes(record)).hexdigest()
        descriptor_path = None
        derived = True
    if record.get("identity_name") != expected_name:
        raise LabError("identity descriptor name does not match the requested identity")
    if record.get("identity_kind") != "synthetic":
        raise LabError("identity descriptor is not synthetic")
    if record.get("media_access_control_address") != expected_mac:
        raise LabError("identity descriptor MAC does not match its numeric identity")
    return {
        "suffix": identity,
        "name": expected_name,
        "descriptor_path": descriptor_path,
        "descriptor_sha256": digest,
        "descriptor_derived": derived,
        "mac_address": expected_mac,
    }


def validate_artifacts(configuration: LabConfiguration, identity: str = "001") -> dict[str, Any]:
    identity = validate_identity(identity)
    if (
        isinstance(configuration.max_active_guests, bool)
        or not isinstance(configuration.max_active_guests, int)
        or not 1 <= configuration.max_active_guests <= MAX_IDENTITY_NUMBER
    ):
        raise LabError(f"max_active_guests must be between 1 and {MAX_IDENTITY_NUMBER}")
    qemu = configuration.qemu.resolve(strict=True)
    if not qemu.is_file() or not os.access(qemu, os.X_OK):
        raise LabError(f"QEMU is not executable: {qemu}")
    qemu_digest = sha256_file(qemu)
    if qemu_digest != _validate_sha256(configuration.qemu_sha256, "configured QEMU"):
        raise LabError(f"QEMU hash mismatch: {qemu_digest}")

    image = configuration.image_directory.resolve(strict=True)
    manifest_path = image / "manifest.json"
    manifest = read_json(manifest_path)
    manifest_digest = sha256_file(manifest_path)
    if configuration.image_manifest_sha256 is not None and manifest_digest != _validate_sha256(
        configuration.image_manifest_sha256, "configured image manifest"
    ):
        raise LabError(f"image manifest hash mismatch: {manifest_digest}")
    if manifest.get("format_version") != 4:
        raise LabError("unsupported image manifest format")

    artifacts: dict[str, dict[str, Any]] = {}
    for key in ("bootloader", "flash", "board_information"):
        filename, expected_size, expected_digest = _artifact_record(manifest, key)
        path = (image / filename).resolve(strict=True)
        if path.parent != image:
            raise LabError(f"image manifest {key} escapes its directory")
        actual_size = path.stat().st_size
        actual_digest = sha256_file(path)
        if actual_size != expected_size or actual_digest != expected_digest:
            raise LabError(f"{key} differs from its manifest")
        artifacts[key] = {
            "path": str(path),
            "size": actual_size,
            "sha256": actual_digest,
        }

    board_information = manifest.get("board_information", {})
    if board_information.get("identity_name") != "ferrofish-a32-synthetic-001":
        raise LabError("canonical image is not bound to the synthetic identity template")
    board_offset = board_information.get("flash_offset")
    board_size = board_information.get("size")
    if (
        isinstance(board_offset, bool)
        or not isinstance(board_offset, int)
        or board_offset < 0
        or isinstance(board_size, bool)
        or not isinstance(board_size, int)
        or board_size != artifacts["board_information"]["size"]
        or board_offset + board_size > artifacts["flash"]["size"]
    ):
        raise LabError("canonical board-information placement is invalid")
    identity_record = _identity_descriptor(configuration, identity)

    machine_help = _run([str(qemu), "-M", "help"], timeout=10).stdout
    if "dante-brooklyn" not in machine_help:
        raise LabError("QEMU does not advertise the dante-brooklyn machine")
    return {
        "schema_version": SCHEMA_VERSION,
        "qemu": {"path": str(qemu), "size": qemu.stat().st_size, "sha256": qemu_digest},
        "image": {
            "path": str(image),
            "manifest": {"path": str(manifest_path), "sha256": manifest_digest},
            "artifacts": artifacts,
            "identity_template": {
                "board_information_flash_offset": board_offset,
                "board_information_size": board_size,
                "mac_offset": BOARD_INFORMATION_MAC_OFFSET,
                "base_identity": "001",
            },
        },
        "identity": identity_record,
        "evidence_limits": {
            "base_artifact_integrity": "hash_bound",
            "runtime_identity": "deterministic synthetic derivative recorded per run",
            "protocol_semantics": "not_assessed",
            "licensing_behavior": "not_assessed",
        },
    }


def materialize_runtime_identity(
    validated: dict[str, Any],
    runtime_flash: Path,
    session: Path,
) -> dict[str, Any]:
    template_record = validated["image"]["identity_template"]
    template_path = Path(validated["image"]["artifacts"]["board_information"]["path"])
    template = template_path.read_bytes()
    expected_size = template_record["board_information_size"]
    if len(template) != expected_size:
        raise LabError("board-information template size changed after validation")
    mac_offset = template_record["mac_offset"]
    if not 0 <= mac_offset <= len(template) - 6:
        raise LabError("board-information MAC placement is outside the template")
    base_mac = bytes.fromhex(_identity_mac_address(template_record["base_identity"]).replace(":", ""))
    if template[mac_offset : mac_offset + 6] != base_mac:
        raise LabError("board-information template does not contain its declared synthetic base identity")

    target_mac = bytes.fromhex(validated["identity"]["mac_address"].replace(":", ""))
    materialized = bytearray(template)
    materialized[mac_offset : mac_offset + 6] = target_mac
    changed_offsets = [
        index for index, (original, replacement) in enumerate(zip(template, materialized)) if original != replacement
    ]
    expected_changed_offsets = [
        mac_offset + index
        for index, (original, replacement) in enumerate(zip(base_mac, target_mac))
        if original != replacement
    ]
    if changed_offsets != expected_changed_offsets:
        raise LabError("synthetic identity materialization changed data outside the declared MAC field")
    board_path = session / "brdinfo.runtime.bin"
    descriptor = os.open(board_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(materialized)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        board_path.unlink(missing_ok=True)
        raise

    flash_offset = template_record["board_information_flash_offset"]
    with runtime_flash.open("r+b") as stream:
        stream.seek(flash_offset)
        if stream.read(expected_size) != template:
            raise LabError("private runtime flash does not contain the hash-bound board-information template")
        stream.seek(flash_offset)
        stream.write(materialized)
        stream.flush()
        os.fsync(stream.fileno())
        stream.seek(flash_offset)
        if stream.read(expected_size) != materialized:
            raise LabError("private runtime flash identity materialization did not verify")
    record = {
        "method": "hash-bound synthetic board-information template with known MAC-field substitution",
        "base_identity": template_record["base_identity"],
        "identity": validated["identity"]["suffix"],
        "mac_address": validated["identity"]["mac_address"],
        "template_path": str(template_path),
        "template_sha256": validated["image"]["artifacts"]["board_information"]["sha256"],
        "board_information_path": str(board_path),
        "board_information_sha256": sha256_file(board_path),
        "flash_offset": flash_offset,
        "mac_offset": mac_offset,
        "changed_board_information_offsets": changed_offsets,
        "changed_flash_offsets": [flash_offset + offset for offset in changed_offsets],
        "runtime_flash_sha256": sha256_file(runtime_flash),
    }
    manifest_path = session / "identity-materialization.json"
    atomic_write_json(manifest_path, record)
    record["manifest"] = {
        "path": str(manifest_path),
        "size": manifest_path.stat().st_size,
        "sha256": sha256_file(manifest_path),
    }
    return record


def build_qemu_command(
    validated: dict[str, Any],
    runtime_flash: Path,
    qmp_socket: Path,
    tap: str,
    run_id: str,
) -> list[str]:
    validate_run_id(run_id)
    validate_interface(tap)
    qemu = _qemu_option_path(validated["qemu"]["path"], "QEMU executable path")
    bootloader = _qemu_option_path(validated["image"]["artifacts"]["bootloader"]["path"], "bootloader path")
    runtime_flash_path = _qemu_option_path(runtime_flash, "runtime flash path")
    qmp_socket_path = _qmp_socket_path(qmp_socket)
    return [
        qemu,
        "-name",
        f"netaudio-ddm-{run_id}",
        "-M",
        "dante-brooklyn",
        "-m",
        "64M",
        "-device",
        f"loader,file={bootloader},addr=0x{DEFAULT_LOADER_ADDRESS:x},cpu-num=0,force-raw=on",
        "-drive",
        f"if=mtd,format=raw,file={runtime_flash_path}",
        "-display",
        "none",
        "-chardev",
        "ringbuf,id=dante-serial,size=65536",
        "-serial",
        "chardev:dante-serial",
        "-monitor",
        "none",
        "-qmp",
        f"unix:{qmp_socket_path},server=on,wait=off",
        "-no-user-config",
        "-no-reboot",
        "-no-shutdown",
        "-global",
        "dante-akashi-emac.link-speed-megabits-per-second=1000",
        "-global",
        "dante-akashi-emac.switch-port=0",
        "-nic",
        f"tap,id=dante-device,ifname={tap},script=no,downscript=no,vhost=off",
    ]


class QmpClient:
    def __init__(self, path: Path, timeout: float = 5.0):
        self.path = path
        self.timeout = timeout

    def execute(self, command: str, arguments: dict[str, Any] | None = None) -> Any:
        deadline = time.monotonic() + self.timeout
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(self.timeout)
        try:
            connection.connect(str(self.path))
            stream = connection.makefile("rwb", buffering=0)
            greeting = self._read_message(stream, deadline)
            if "QMP" not in greeting:
                raise LabError("QMP greeting is invalid")
            self._send(stream, {"execute": "qmp_capabilities", "id": "capabilities"})
            self._wait_for_identifier(stream, "capabilities", deadline)
            request: dict[str, Any] = {"execute": command, "id": "command"}
            if arguments is not None:
                request["arguments"] = arguments
            self._send(stream, request)
            response = self._wait_for_identifier(stream, "command", deadline)
            if "error" in response:
                raise LabError(f"QMP {command} failed: {response['error']}")
            if "return" not in response:
                raise LabError(f"QMP {command} returned no result")
            return response["return"]
        except (OSError, socket.timeout) as error:
            raise LabError(f"QMP {command} failed: {error}") from error
        finally:
            connection.close()

    @staticmethod
    def _send(stream: Any, message: dict[str, Any]) -> None:
        stream.write(json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n")

    @staticmethod
    def _read_message(stream: Any, deadline: float) -> dict[str, Any]:
        if time.monotonic() >= deadline:
            raise LabError("QMP deadline exceeded")
        encoded = stream.readline(1024 * 1024 + 1)
        if not encoded or len(encoded) > 1024 * 1024:
            raise LabError("QMP response is absent or too large")
        try:
            value = json.loads(encoded)
        except json.JSONDecodeError as error:
            raise LabError(f"QMP response is invalid JSON: {error}") from error
        if not isinstance(value, dict):
            raise LabError("QMP response is not an object")
        return value

    def _wait_for_identifier(self, stream: Any, identifier: str, deadline: float) -> dict[str, Any]:
        while True:
            message = self._read_message(stream, deadline)
            if message.get("id") == identifier:
                return message
