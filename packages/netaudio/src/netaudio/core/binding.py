from __future__ import annotations

import ctypes
import ipaddress
import json
import logging
import os
import sys
import threading
from pathlib import Path

from . import _requests, _types

from ._abi import (
    ABI_VERSION,
    PORT_ARC as _PORT_ARC,
    PORT_CONTROL as _PORT_CONTROL,
    PORT_SETTINGS as _PORT_SETTINGS,
    STATUS_INVALID_SEQUENCE,
    STATUS_IO_ERROR as _STATUS_IO_ERROR,
    STATUS_OK,
)
from ._abi import STATUS_BUFFER_TOO_SMALL as _STATUS_BUFFER_TOO_SMALL
from ._abi import STATUS_TIMEOUT as STATUS_TIMEOUT
from ._abi import configure as _configure

logger = logging.getLogger("netaudio")

_REPO_ROOT = Path(__file__).resolve().parents[5]


def _library_names():
    if sys.platform == "darwin":
        return ("libnetaudio_core.dylib", "libnetaudio_core.so")
    if sys.platform == "win32":
        return ("netaudio_core.dll",)
    return ("libnetaudio_core.so",)


class NetaudioCoreError(RuntimeError):
    def __init__(self, status: int, context: str = ""):
        self.status = status
        self.context = context
        self.detail = last_error_message()
        library = require()
        self.category = library.netaudio_status_category(status).decode("ascii")
        message = library.netaudio_status_description(status).decode("utf-8")
        if self.detail and self.detail != message:
            message = f"{message} ({self.detail})"
        super().__init__(f"{context}: {message}" if context else message)


class NetaudioCoreJsonError(ValueError):
    def __init__(self, category: str, context: str):
        self.category = category
        self.context = context
        operation = "decoding" if category == "json_decoding" else "encoding"
        super().__init__(f"{context}: JSON {operation} failed")


def _decode_json_output(data: bytes, context: str):
    try:
        return json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exception:
        raise NetaudioCoreJsonError("json_decoding", context) from exception


def _encode_command_spec(spec: dict | str) -> bytes:
    try:
        return json.dumps(spec, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exception:
        raise NetaudioCoreJsonError("json_encoding", "command specification") from exception


class NetaudioCoreLibraryMissing(RuntimeError):
    pass


def _candidate_paths():
    override = os.environ.get("NETAUDIO_CORE_LIB")
    if override:
        yield Path(override)
        return

    for library_name in _library_names():
        yield Path(__file__).resolve().parent / library_name
        for profile in ("release", "debug"):
            yield _REPO_ROOT / "target" / profile / library_name


_library = None
_load_attempted = False
_load_failures: list[tuple[Path, str]] = []
_library_lock = threading.Lock()


def _abi_compatibility_failure(lib) -> str | None:
    try:
        version_function = lib.netaudio_abi_version
    except AttributeError:
        return "predates ABI versioning"
    version_function.argtypes = []
    version_function.restype = ctypes.c_uint32
    found = version_function()
    if found != ABI_VERSION:
        return f"ABI {found}, expected {ABI_VERSION}"
    return None


def _load():
    global _library, _load_attempted, _load_failures

    with _library_lock:
        if _load_attempted:
            return _library

        _load_failures = []

        for path in _candidate_paths():
            if not path:
                continue

            if not path.exists():
                _load_failures.append((path, "not found"))
                continue

            try:
                lib = ctypes.CDLL(str(path))
            except OSError as exception:
                _load_failures.append((path, str(exception)))
                continue

            compatibility_failure = _abi_compatibility_failure(lib)

            if compatibility_failure is not None:
                logger.warning(f"netaudio-core at {path} {compatibility_failure}, skipping")
                _load_failures.append((path, compatibility_failure))
                continue

            try:
                _library = _configure(lib)
            except AttributeError as exception:
                _load_failures.append((path, str(exception)))
                continue

            _load_attempted = True
            return _library

        _load_attempted = True
        return None


def available() -> bool:
    return _load() is not None


def require():
    library = _load()
    if library is not None:
        return library
    lines = [
        f"netaudio-core library not found (expected ABI {ABI_VERSION}).",
        "Build it with `make core`.",
        "Searched:",
    ]
    if _load_failures:
        lines.extend(f"  {path}: {reason}" for path, reason in _load_failures)
    else:
        lines.extend(f"  {path}" for path in _candidate_paths())
    raise NetaudioCoreLibraryMissing("\n".join(lines))


def _call_buffer(function, *leading_args, capacity=8192):
    require()
    out = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = function(*leading_args, out, capacity, ctypes.byref(length))
    if status == _STATUS_BUFFER_TOO_SMALL and length.value > capacity:
        out = (ctypes.c_uint8 * length.value)()
        capacity = length.value
        status = function(*leading_args, out, capacity, ctypes.byref(length))
    return status, ctypes.string_at(out, min(length.value, capacity))


def last_error_message() -> str:
    lib = _library
    if lib is None:
        return ""
    capacity = 1024
    out = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = lib.netaudio_last_error_message(out, capacity, ctypes.byref(length))
    if status == _STATUS_BUFFER_TOO_SMALL and length.value > capacity:
        capacity = length.value
        out = (ctypes.c_uint8 * capacity)()
        status = lib.netaudio_last_error_message(out, capacity, ctypes.byref(length))
    if status != STATUS_OK:
        return ""
    return bytes(out[: length.value]).decode("utf-8", errors="replace")


def next_message_id() -> int:
    return require().netaudio_next_message_id()


def _call_json(function, value, context):
    status, data = _call_buffer(function, _encode_command_spec(value))

    if status != STATUS_OK:
        raise NetaudioCoreError(status, context)

    return _decode_json_output(data, context)


def lock_key_search(path: str | None) -> dict:
    return _call_json(require().netaudio_lock_key_search, {"path": path}, "lock key search")


def operation_availability(facts: _requests.AvailabilityRequest) -> _types.Availability:
    return _call_json(require().netaudio_operation_availability, facts, "operation availability")


def clock_sources(facts: _requests.ClockSourceFacts) -> _types.ClockSources:
    return _call_json(require().netaudio_clock_sources, facts, "clock sources")


def metering_scale() -> _types.MeteringScales:
    status, data = _call_buffer(require().netaudio_metering_scale)

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "metering scale")

    return _decode_json_output(data, "metering scale")


def build_command(spec: dict) -> bytes:
    lib = require()
    status, data = _call_buffer(lib.netaudio_build_command, _encode_command_spec(spec))
    if status == STATUS_INVALID_SEQUENCE and "message_id" not in spec:
        spec = {**spec, "message_id": next_message_id()}
        status, data = _call_buffer(lib.netaudio_build_command, _encode_command_spec(spec))
    if status != STATUS_OK:
        raise NetaudioCoreError(status, f"build_command {spec.get('command')}")
    return data


def canonical_device_mac(value: str | None) -> str | None:
    return device_identity({"kind": "mac", "value": value if isinstance(value, str) else None})


def device_identity(request: _requests.DeviceIdentityRequest) -> str | None:
    return _call_json(require().netaudio_device_identity, request, "device identity")


def parse_response(kind: str, data: bytes):
    lib = require()
    in_buffer = (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else (ctypes.c_uint8 * 0)()
    status, out = _call_buffer(lib.netaudio_parse_response, kind.encode("utf-8"), in_buffer, len(data))
    if status != STATUS_OK:
        raise NetaudioCoreError(status, f"parse_response {kind}")
    return _decode_json_output(out, f"parse {kind}")


def parse_page(kind: str, data: bytes, starting_channel: int):
    lib = require()
    in_buffer = (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else (ctypes.c_uint8 * 0)()
    status, out = _call_buffer(lib.netaudio_parse_page, kind.encode("utf-8"), in_buffer, len(data), starting_channel)
    if status != STATUS_OK:
        raise NetaudioCoreError(status, f"parse_page {kind}")
    return _decode_json_output(out, f"parse {kind}")


def _as_buffer(data: bytes):
    return (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else (ctypes.c_uint8 * 0)()


class CoreClient:
    def __init__(
        self,
        device_ip: str,
        arc_port: int = _PORT_ARC,
        timeout_ms: int = 1000,
        attempts: int = 3,
        *,
        local_ip: str | None = None,
        control_port: int = _PORT_CONTROL,
        settings_port: int = _PORT_SETTINGS,
    ):
        self._native_lock = threading.RLock()
        self._handle = ctypes.c_void_p()
        self._lib = None
        if local_ip is not None:
            if not isinstance(local_ip, str):
                raise ValueError("local_ip must be an IPv4 address string")
            local_ip = str(ipaddress.IPv4Address(local_ip))
        library = require()
        self._lib = library
        self._device_ip = device_ip
        with self._native_lock:
            status = library.netaudio_client_new(
                device_ip.encode("ascii"),
                local_ip.encode("ascii") if local_ip is not None else None,
                arc_port,
                timeout_ms,
                attempts,
                ctypes.byref(self._handle),
            )
        if status != STATUS_OK:
            raise NetaudioCoreError(status, f"client_new {device_ip}")
        if (control_port, settings_port) != (_PORT_CONTROL, _PORT_SETTINGS):
            self.set_service_ports(control_port, settings_port)

    def close(self):
        with self._native_lock:
            if self._handle and self._lib is not None:
                self._lib.netaudio_client_free(self._handle)
                self._handle = ctypes.c_void_p()

    def __enter__(self):
        return self

    def __exit__(self, *_exception_information):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            logger.exception("Failed to close native core client")

    def _require_library(self):
        if self._lib is None:
            raise NetaudioCoreError(_STATUS_IO_ERROR, "netaudio-core library not loaded")
        return self._lib

    @property
    def device_ip(self) -> str:
        return self._device_ip

    def clear_wire_captures(self) -> None:
        library = self._require_library()

        with self._native_lock:
            status = library.netaudio_client_clear_wire_captures(self._handle)

            if status != STATUS_OK:
                raise NetaudioCoreError(status, "clear_wire_captures")

    def get_wire_captures(self) -> list[dict]:
        library = self._require_library()

        with self._native_lock:
            status, data = _call_buffer(library.netaudio_client_get_wire_captures_json, self._handle, capacity=262144)

            if status != STATUS_OK:
                raise NetaudioCoreError(status, "get_wire_captures")

        return _decode_json_output(data, "wire captures")

    def set_service_ports(self, control_port: int, settings_port: int) -> None:
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_set_service_ports(self._handle, control_port, settings_port)
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "set_service_ports")

    def request(
        self, packet: bytes, target_port: int, expect_response: bool = True, repeat: int = 1, interval_ms: int = 0
    ):
        out = (ctypes.c_uint8 * 65536)()
        length = ctypes.c_size_t(0)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_request(
                self._handle,
                _as_buffer(packet),
                len(packet),
                target_port,
                expect_response,
                repeat,
                interval_ms,
                out,
                65536,
                ctypes.byref(length),
            )
            data = bytes(out[: length.value])
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "client_request")
        response = data if expect_response else None
        return response


def clock_subdomain_presentation(value) -> _types.ClockSubdomainPresentation:
    if isinstance(value, (bytes, bytearray)):
        value = list(value)

    return _call_json(require().netaudio_clock_subdomain_presentation, value, "clock subdomain presentation")


def subscription_status(code: int, receiver_status_code: int | None = None) -> _types.SubscriptionStatus:
    values = (code,) if receiver_status_code is None else (code, receiver_status_code)
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 0xFFFF:
            raise ValueError("subscription and receiver status values must be unsigned 16-bit integers")
    status, data = _call_buffer(
        require().netaudio_subscription_status, code, receiver_status_code or 0, receiver_status_code is not None
    )
    if status != STATUS_OK:
        raise NetaudioCoreError(status, "subscription_status")
    return _decode_json_output(data, "Rust API response")
