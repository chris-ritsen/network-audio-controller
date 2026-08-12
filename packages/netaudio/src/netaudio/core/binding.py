from __future__ import annotations

import ctypes
import json
import logging
import os
import sys
import threading
from pathlib import Path

logger = logging.getLogger("netaudio")

_REPO_ROOT = Path(__file__).resolve().parents[5]

ABI_VERSION = 2

LOCK_NONCE_LENGTH = 24
LOCK_KEY_LENGTH = 32


def _library_names():
    if sys.platform == "darwin":
        return ("libnetaudio_core.dylib", "libnetaudio_core.so")
    if sys.platform == "win32":
        return ("netaudio_core.dll",)
    return ("libnetaudio_core.so",)


STATUS_OK = 0
STATUS_TIMEOUT = 9

_STATUS_NAMES = {
    1: "null pointer",
    2: "invalid utf-8",
    3: "name too long",
    4: "name cannot begin or end with a hyphen",
    5: "name must contain only A-Z, a-z, 0-9, and hyphens",
    6: "buffer too small",
    7: "invalid address",
    8: "io error",
    9: "device did not respond",
    10: "malformed response",
    11: "serialization error",
    12: "subscription count must be 1-16",
    13: "invalid command json",
    14: "invalid mac",
    15: "invalid ip",
    16: "invalid channel type",
    17: "invalid lock key",
    18: "pin must be exactly 4 digits",
    19: "crypto error",
    20: "page exceeds the protocol channel range",
    21: "subscription receiver channel must fit in one byte",
    22: "device type must be 'input' or 'output'",
    23: "command packet exceeds the protocol length limit",
    24: "channel number must be at least 1",
    25: "latency must be finite, nonnegative, and fit on the wire",
    26: "sample rate must be nonzero",
    27: "encoding value must be nonzero",
    28: "gain level must be an integer from 1 through 5",
    29: "flow slot must be from 1 through 32",
    30: "flow protocol must be 0x2729, 0x2801, or 0x2809",
}


class NetaudioCoreError(Exception):
    def __init__(self, status: int, context: str = ""):
        self.status = status
        message = _STATUS_NAMES.get(status, f"status {status}")
        super().__init__(f"{context}: {message}" if context else message)


def _candidate_paths():
    override = os.environ.get("NETAUDIO_CORE_LIB")
    if override:
        yield Path(override)
    for library_name in _library_names():
        yield Path(__file__).resolve().parent / library_name
        for profile in ("release", "debug"):
            yield _REPO_ROOT / "packages" / "netaudio-core" / "target" / profile / library_name


_library = None
_load_attempted = False


def _abi_compatible(lib, path):
    try:
        version_function = lib.netaudio_abi_version
    except AttributeError:
        logger.warning(f"netaudio-core at {path} predates ABI versioning, skipping")
        return False
    version_function.argtypes = []
    version_function.restype = ctypes.c_uint32
    found = version_function()
    if found != ABI_VERSION:
        logger.warning(f"netaudio-core at {path} has ABI {found}, expected {ABI_VERSION}, skipping")
        return False
    return True


def _load():
    global _library, _load_attempted
    if _load_attempted:
        return _library
    _load_attempted = True
    for path in _candidate_paths():
        if path and path.exists():
            lib = ctypes.CDLL(str(path))
            if not _abi_compatible(lib, path):
                continue
            _library = _configure(lib)
            return _library
    return None


def _configure(lib):
    u8p = ctypes.POINTER(ctypes.c_uint8)
    buffer_out = [
        u8p,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    lib.netaudio_build_command.argtypes = [ctypes.c_char_p, *buffer_out]
    lib.netaudio_build_command.restype = ctypes.c_int
    lib.netaudio_parse_response.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        *buffer_out,
    ]
    lib.netaudio_parse_response.restype = ctypes.c_int
    lib.netaudio_parse_page.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.c_uint16,
        *buffer_out,
    ]
    lib.netaudio_parse_page.restype = ctypes.c_int
    lib.netaudio_lock_token.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        *buffer_out,
    ]
    lib.netaudio_lock_token.restype = ctypes.c_int

    lib.netaudio_client_new.argtypes = [
        ctypes.c_char_p,
        ctypes.c_uint16,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.netaudio_client_new.restype = ctypes.c_int
    lib.netaudio_client_free.argtypes = [ctypes.c_void_p]
    lib.netaudio_client_free.restype = None
    lib.netaudio_client_set_host_mac.argtypes = [ctypes.c_void_p, u8p]
    lib.netaudio_client_set_host_mac.restype = ctypes.c_int
    lib.netaudio_client_request.argtypes = [
        ctypes.c_void_p,
        u8p,
        ctypes.c_size_t,
        ctypes.c_uint16,
        ctypes.c_bool,
        ctypes.c_uint32,
        ctypes.c_uint64,
        *buffer_out,
    ]
    lib.netaudio_client_request.restype = ctypes.c_int
    lib.netaudio_client_lock.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        u8p,
        ctypes.c_size_t,
        *buffer_out,
    ]
    lib.netaudio_client_lock.restype = ctypes.c_int
    lib.netaudio_client_unlock.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        u8p,
        ctypes.c_size_t,
        *buffer_out,
    ]
    lib.netaudio_client_unlock.restype = ctypes.c_int
    lib.netaudio_host_mac.argtypes = [u8p]
    lib.netaudio_host_mac.restype = ctypes.c_int
    lib.netaudio_client_execute.argtypes = [ctypes.c_void_p, ctypes.c_char_p, *buffer_out]
    lib.netaudio_client_execute.restype = ctypes.c_int
    lib.netaudio_client_get_channel_count.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint16),
        ctypes.POINTER(ctypes.c_uint16),
        ctypes.POINTER(ctypes.c_int32),
    ]
    lib.netaudio_client_get_channel_count.restype = ctypes.c_int
    lib.netaudio_client_get_aes67_configured.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int32)]
    lib.netaudio_client_get_aes67_configured.restype = ctypes.c_int
    lib.netaudio_client_get_rx_inventory_json.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint16,
        *buffer_out,
    ]
    lib.netaudio_client_get_rx_inventory_json.restype = ctypes.c_int
    for getter in (
        "netaudio_client_get_rx_channels_json",
        "netaudio_client_get_tx_channels_json",
        "netaudio_client_get_device_name_json",
        "netaudio_client_get_device_info_json",
        "netaudio_client_get_device_settings_json",
        "netaudio_client_get_property_directory_json",
    ):
        function = getattr(lib, getter)
        function.argtypes = [ctypes.c_void_p, *buffer_out]
        function.restype = ctypes.c_int
    return lib


def available() -> bool:
    return _load() is not None


def require():
    library = _load()
    if library is None:
        searched = "\n  ".join(str(path) for path in _candidate_paths())
        raise NetaudioCoreError(
            8,
            f"netaudio-core library not found or ABI-incompatible (expected ABI {ABI_VERSION}); searched:\n  {searched}",
        )
    return library


def _call_buffer(function, *leading_args, capacity=8192):
    lib = _load()
    if lib is None:
        raise NetaudioCoreError(8, "netaudio-core library not found")
    out = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = function(*leading_args, out, capacity, ctypes.byref(length))
    if status == 6 and length.value > capacity:
        out = (ctypes.c_uint8 * length.value)()
        capacity = length.value
        status = function(*leading_args, out, capacity, ctypes.byref(length))
    return status, bytes(out[: length.value])


def build_command(spec: dict) -> bytes:
    lib = _load()
    if lib is None:
        raise NetaudioCoreError(8, "netaudio-core library not found")
    payload = json.dumps(spec).encode("utf-8")
    status, data = _call_buffer(lib.netaudio_build_command, payload)
    if status != STATUS_OK:
        raise NetaudioCoreError(status, f"build_command {spec.get('command')}")
    return data


def parse_response(kind: str, data: bytes):
    lib = _load()
    if lib is None:
        raise NetaudioCoreError(8, "netaudio-core library not found")
    in_buffer = (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else (ctypes.c_uint8 * 0)()
    status, out = _call_buffer(lib.netaudio_parse_response, kind.encode("utf-8"), in_buffer, len(data))
    if status != STATUS_OK:
        raise NetaudioCoreError(status, f"parse_response {kind}")
    return json.loads(out)


def parse_page(kind: str, data: bytes, starting_channel: int):
    lib = _load()
    if lib is None:
        raise NetaudioCoreError(8, "netaudio-core library not found")
    in_buffer = (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else (ctypes.c_uint8 * 0)()
    status, out = _call_buffer(lib.netaudio_parse_page, kind.encode("utf-8"), in_buffer, len(data), starting_channel)
    if status != STATUS_OK:
        raise NetaudioCoreError(status, f"parse_page {kind}")
    return json.loads(out)


def lock_token(pin: str, nonce: bytes, key: bytes) -> bytes:
    if len(nonce) != LOCK_NONCE_LENGTH:
        raise ValueError(f"nonce must be exactly {LOCK_NONCE_LENGTH} bytes")
    if len(key) != LOCK_KEY_LENGTH:
        raise ValueError(f"key must be exactly {LOCK_KEY_LENGTH} bytes")
    lib = _load()
    if lib is None:
        raise NetaudioCoreError(8, "netaudio-core library not found")
    nonce_buffer = (ctypes.c_uint8 * len(nonce)).from_buffer_copy(nonce)
    key_buffer = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
    status, token = _call_buffer(
        lib.netaudio_lock_token,
        pin.encode("ascii"),
        nonce_buffer,
        len(nonce),
        key_buffer,
        len(key),
        capacity=256,
    )
    if status != STATUS_OK:
        raise NetaudioCoreError(status, "lock_token")
    return token


def host_mac() -> bytes | None:
    lib = _load()
    if lib is None:
        return None
    out = (ctypes.c_uint8 * 6)()
    if lib.netaudio_host_mac(out) == STATUS_OK:
        return bytes(out)
    return None


def _as_buffer(data: bytes):
    return (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else (ctypes.c_uint8 * 0)()


class CoreClient:
    def __init__(self, device_ip: str, arc_port: int = 4440, timeout_ms: int = 1000, attempts: int = 3):
        self._native_lock = threading.RLock()
        self._handle = ctypes.c_void_p()
        self._lib = None
        library = _load()
        if library is None:
            raise NetaudioCoreError(8, "netaudio-core library not found")
        self._lib = library
        self._device_ip = device_ip
        self._arc_port = arc_port
        self.observer = None
        with self._native_lock:
            status = library.netaudio_client_new(
                device_ip.encode("ascii"), arc_port, timeout_ms, attempts, ctypes.byref(self._handle)
            )
        if status != STATUS_OK:
            raise NetaudioCoreError(status, f"client_new {device_ip}")

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
            raise NetaudioCoreError(8, "netaudio-core library not loaded")
        return self._lib

    def set_host_mac(self, mac: bytes):
        if len(mac) != 6:
            raise ValueError("mac must be exactly 6 bytes")
        buffer = (ctypes.c_uint8 * 6).from_buffer_copy(mac)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_set_host_mac(self._handle, buffer)
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "set_host_mac")

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
        response = data if data else None
        if self.observer is not None:
            self.observer(packet, response, self._device_ip, target_port)
        return response

    def execute(self, spec: dict):
        out = (ctypes.c_uint8 * 65536)()
        length = ctypes.c_size_t(0)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_execute(
                self._handle, json.dumps(spec).encode("utf-8"), out, 65536, ctypes.byref(length)
            )
            data = bytes(out[: length.value])
        if status != STATUS_OK:
            raise NetaudioCoreError(status, f"execute {spec.get('command')}")
        return data if data else None

    def _json_getter(self, name):
        out = (ctypes.c_uint8 * 262144)()
        length = ctypes.c_size_t(0)
        library = self._require_library()
        with self._native_lock:
            status = getattr(library, name)(self._handle, out, 262144, ctypes.byref(length))
            data = bytes(out[: length.value])
        if status != STATUS_OK:
            raise NetaudioCoreError(status, name)
        return json.loads(data)

    def get_rx_channels(self):
        return self._json_getter("netaudio_client_get_rx_channels_json")

    def get_rx_inventory(self, rx_count: int):
        out = (ctypes.c_uint8 * 262144)()
        length = ctypes.c_size_t(0)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_get_rx_inventory_json(
                self._handle,
                rx_count,
                out,
                262144,
                ctypes.byref(length),
            )
            data = bytes(out[: length.value])
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "netaudio_client_get_rx_inventory_json")
        return json.loads(data)

    def get_tx_channels(self):
        return self._json_getter("netaudio_client_get_tx_channels_json")

    def get_device_name(self):
        return self._json_getter("netaudio_client_get_device_name_json")

    def get_device_info(self):
        return self._json_getter("netaudio_client_get_device_info_json")

    def get_device_settings(self):
        return self._json_getter("netaudio_client_get_device_settings_json")

    def get_property_directory(self):
        return self._json_getter("netaudio_client_get_property_directory_json")

    def get_channel_audio_metadata(self, tx_count: int, rx_count: int):
        candidates = (
            (tx_count, {"command": "transmitters", "page": 0}),
            (rx_count, {"command": "receivers", "page": 0}),
        )
        for channel_count, specification in candidates:
            if channel_count <= 0:
                continue
            try:
                packet = build_command(specification)
                response = self.request(packet, self._arc_port)
                if response is not None:
                    return parse_response("channel_audio_metadata", response)
            except NetaudioCoreError as exception:
                logger.debug(
                    f"Channel audio metadata query failed for {self._device_ip} "
                    f"using {specification['command']}: {exception}"
                )
        return None

    def get_channel_count(self):
        tx = ctypes.c_uint16(0)
        rx = ctypes.c_uint16(0)
        locked = ctypes.c_int32(-2)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_get_channel_count(
                self._handle, ctypes.byref(tx), ctypes.byref(rx), ctypes.byref(locked)
            )
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "get_channel_count")
        lock_state = None if locked.value < 0 else bool(locked.value)
        return tx.value, rx.value, lock_state

    def get_aes67_configured(self):
        state = ctypes.c_int32(-2)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_get_aes67_configured(self._handle, ctypes.byref(state))
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "get_aes67_configured")
        return None if state.value < 0 else bool(state.value)

    def lock(self, pin: str, key: bytes):
        return self._lock_op("netaudio_client_lock", pin, key)

    def unlock(self, pin: str, key: bytes):
        return self._lock_op("netaudio_client_unlock", pin, key)

    def _lock_op(self, name, pin, key):
        if len(key) != LOCK_KEY_LENGTH:
            raise ValueError(f"key must be exactly {LOCK_KEY_LENGTH} bytes")
        key_buffer = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
        out = (ctypes.c_uint8 * 4096)()
        length = ctypes.c_size_t(0)
        library = self._require_library()
        with self._native_lock:
            status = getattr(library, name)(
                self._handle,
                pin.encode("ascii"),
                key_buffer,
                len(key),
                out,
                4096,
                ctypes.byref(length),
            )
            data = bytes(out[: length.value])
        if status != STATUS_OK:
            raise NetaudioCoreError(status, name)
        return json.loads(data)
