from __future__ import annotations

import asyncio
import ctypes
import json
import logging
import threading
from collections.abc import Callable

from netaudio import core
from netaudio.dante.const import DEVICE_ARC_PORT

logger = logging.getLogger("netaudio")

DEFAULT_REQUEST_ATTEMPTS = 3
DEFAULT_REQUEST_TIMEOUT_MILLISECONDS = 1000

WireObserver = Callable[[bytes, str, int, str], None]


def _wire_capture_functions(library):
    clear_function = library.netaudio_client_clear_wire_captures
    read_function = library.netaudio_client_get_wire_captures_json
    if not getattr(read_function, "argtypes", None):
        clear_function.argtypes = [ctypes.c_void_p]
        clear_function.restype = ctypes.c_int
        read_function.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_size_t),
        ]
        read_function.restype = ctypes.c_int
    return clear_function, read_function


def clear_wire_captures(client) -> None:
    clear_function, _ = _wire_capture_functions(client._require_library())
    status = clear_function(client._handle)
    if status != core.STATUS_OK:
        raise core.NetaudioCoreError(status, "clear_wire_captures")


def take_wire_captures(client) -> list[dict]:
    _, read_function = _wire_capture_functions(client._require_library())
    capacity = 262144
    out = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = read_function(client._handle, out, capacity, ctypes.byref(length))
    if status == 6 and length.value > capacity:
        capacity = length.value
        out = (ctypes.c_uint8 * capacity)()
        status = read_function(client._handle, out, capacity, ctypes.byref(length))
    if status != core.STATUS_OK:
        raise core.NetaudioCoreError(status, "get_wire_captures_json")
    return json.loads(bytes(out[: length.value]))


class CoreTransport:
    def __init__(self, observer: WireObserver | None = None):
        self._clients: dict[tuple[str, int, int, int], core.CoreClient] = {}
        self._client_locks: dict[tuple[str, int, int, int], threading.Lock] = {}
        self._host_mac = core.host_mac()
        self._observer = observer

    @property
    def host_mac(self) -> bytes | None:
        return self._host_mac

    @property
    def observer(self) -> WireObserver | None:
        return self._observer

    @observer.setter
    def observer(self, value: WireObserver | None) -> None:
        self._observer = value

    def client(
        self,
        device_ip_address,
        arc_port: int = DEVICE_ARC_PORT,
        timeout_milliseconds: int = DEFAULT_REQUEST_TIMEOUT_MILLISECONDS,
        attempts: int = DEFAULT_REQUEST_ATTEMPTS,
    ) -> core.CoreClient:
        key = (str(device_ip_address), arc_port, timeout_milliseconds, attempts)
        client = self._clients.get(key)
        if client is None:
            client = core.CoreClient(
                key[0],
                arc_port=arc_port,
                timeout_ms=timeout_milliseconds,
                attempts=attempts,
            )
            if self._host_mac:
                client.set_host_mac(self._host_mac)
            self._clients[key] = client
            self._client_locks[key] = threading.Lock()
        return client

    async def call(
        self,
        device_ip_address,
        operation: Callable[[core.CoreClient], object],
        *,
        arc_port: int = DEVICE_ARC_PORT,
        timeout_milliseconds: int = DEFAULT_REQUEST_TIMEOUT_MILLISECONDS,
        attempts: int = DEFAULT_REQUEST_ATTEMPTS,
    ):
        key = (str(device_ip_address), arc_port, timeout_milliseconds, attempts)
        client = self.client(device_ip_address, arc_port, timeout_milliseconds, attempts)
        return await asyncio.to_thread(self._call_and_observe, client, self._client_locks[key], operation)

    async def execute(
        self,
        device_ip_address,
        specification: dict,
        *,
        arc_port: int = DEVICE_ARC_PORT,
        timeout_milliseconds: int = DEFAULT_REQUEST_TIMEOUT_MILLISECONDS,
        attempts: int = DEFAULT_REQUEST_ATTEMPTS,
    ) -> bytes | None:
        return await self.call(
            device_ip_address,
            lambda client: client.execute(specification),
            arc_port=arc_port,
            timeout_milliseconds=timeout_milliseconds,
            attempts=attempts,
        )

    def _call_and_observe(self, client, client_lock, operation):
        observer = self._observer
        if observer is None:
            with client_lock:
                return operation(client)
        with client_lock:
            clear_wire_captures(client)
            try:
                return operation(client)
            finally:
                for capture in take_wire_captures(client):
                    observer(
                        bytes.fromhex(capture["payload_hex"]),
                        client._device_ip,
                        capture["port"],
                        capture["direction"],
                    )

    def close(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        self._client_locks.clear()
        for client in clients:
            client.close()
