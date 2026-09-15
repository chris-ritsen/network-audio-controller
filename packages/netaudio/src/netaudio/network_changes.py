from __future__ import annotations

import asyncio
import ctypes
import logging
import socket
import sys
from collections.abc import Callable

logger = logging.getLogger("netaudio")

CHANGE_DEBOUNCE_SECONDS = 0.1
LINUX_NETLINK_ROUTE = 0
LINUX_RTMGRP_LINK = 0x1
LINUX_RTMGRP_IPV4_IFADDR = 0x10
LINUX_RTMGRP_IPV4_ROUTE = 0x40


class NetworkChangeError(RuntimeError):
    pass


class _SocketChangeSource:
    def __init__(self, change_socket: socket.socket) -> None:
        self._socket = change_socket
        self._loop: asyncio.AbstractEventLoop | None = None
        self._callback: Callable[[], None] | None = None

    def start(self, loop: asyncio.AbstractEventLoop, callback: Callable[[], None]) -> None:
        self._loop = loop
        self._callback = callback
        self._socket.setblocking(False)
        loop.add_reader(self._socket.fileno(), self._readable)

    def _readable(self) -> None:
        changed = False
        while True:
            try:
                payload = self._socket.recv(65535)
            except BlockingIOError:
                break
            except OSError as exception:
                logger.warning("Network change notification socket failed: %s", exception)
                changed = True
                break
            if not payload:
                break
            changed = True
        if changed and self._callback is not None:
            self._callback()

    def close(self) -> None:
        if self._loop is not None:
            self._loop.remove_reader(self._socket.fileno())
        self._loop = None
        self._callback = None
        self._socket.close()


def _linux_change_source() -> _SocketChangeSource:
    address_family = getattr(socket, "AF_NETLINK", None)
    if address_family is None:
        raise NetworkChangeError("Python does not expose Linux netlink sockets")
    change_socket = socket.socket(
        address_family,
        socket.SOCK_RAW,
        getattr(socket, "NETLINK_ROUTE", LINUX_NETLINK_ROUTE),
    )
    try:
        groups = LINUX_RTMGRP_LINK | LINUX_RTMGRP_IPV4_IFADDR | LINUX_RTMGRP_IPV4_ROUTE
        change_socket.bind((0, groups))
    except BaseException:
        change_socket.close()
        raise
    return _SocketChangeSource(change_socket)


def _macos_change_source() -> _SocketChangeSource:
    address_family = getattr(socket, "PF_ROUTE", getattr(socket, "AF_ROUTE", None))
    if address_family is None:
        raise NetworkChangeError("Python does not expose macOS routing sockets")
    change_socket = socket.socket(address_family, socket.SOCK_RAW, socket.AF_UNSPEC)
    return _SocketChangeSource(change_socket)


class _WindowsChangeSource:
    _REGISTRATIONS = (
        "NotifyIpInterfaceChange",
        "NotifyUnicastIpAddressChange",
        "NotifyRouteChange2",
    )

    def __init__(self) -> None:
        self._library = None
        self._callback = None
        self._handles: list[ctypes.c_void_p] = []
        self._closed = False

    def start(self, loop: asyncio.AbstractEventLoop, callback: Callable[[], None]) -> None:
        try:
            library = ctypes.WinDLL("Iphlpapi.dll")
            callback_type = ctypes.WINFUNCTYPE(None, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)
        except (AttributeError, OSError) as exception:
            raise NetworkChangeError("Windows IP Helper notifications are unavailable") from exception

        def changed(_context, _row, _notification_type) -> None:
            if self._closed:
                return
            try:
                loop.call_soon_threadsafe(callback)
            except RuntimeError:
                pass

        native_callback = callback_type(changed)
        cancel = library.CancelMibChangeNotify2
        cancel.argtypes = [ctypes.c_void_p]
        cancel.restype = ctypes.c_ulong
        self._library = library
        self._callback = native_callback
        try:
            for function_name in self._REGISTRATIONS:
                function = getattr(library, function_name)
                function.argtypes = [
                    ctypes.c_ushort,
                    callback_type,
                    ctypes.c_void_p,
                    ctypes.c_bool,
                    ctypes.POINTER(ctypes.c_void_p),
                ]
                function.restype = ctypes.c_ulong
                handle = ctypes.c_void_p()
                status = function(socket.AF_INET, native_callback, None, False, ctypes.byref(handle))
                if status != 0:
                    raise OSError(status, f"{function_name} failed")
                self._handles.append(handle)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        self._closed = True
        library = self._library
        self._library = None
        if library is not None:
            cancel = library.CancelMibChangeNotify2
            for handle in self._handles:
                if handle.value:
                    cancel(handle)
        self._handles.clear()
        self._callback = None


def _platform_change_source():
    if sys.platform.startswith("linux"):
        return _linux_change_source()
    if sys.platform == "darwin":
        return _macos_change_source()
    if sys.platform == "win32":
        return _WindowsChangeSource()
    raise NetworkChangeError(f"Network change notifications are unsupported on {sys.platform}")


class NetworkChangeMonitor:
    def __init__(
        self,
        *,
        source_factory: Callable[[], object] = _platform_change_source,
        debounce_seconds: float = CHANGE_DEBOUNCE_SECONDS,
    ) -> None:
        if debounce_seconds < 0:
            raise ValueError("Network change debounce interval cannot be negative")
        self._source_factory = source_factory
        self._debounce_seconds = debounce_seconds
        self._callbacks: dict[int, Callable[[], None]] = {}
        self._next_token = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._source = None
        self._pending: asyncio.TimerHandle | None = None

    def subscribe(self, callback: Callable[[], None]) -> Callable[[], None]:
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise NetworkChangeError("Network change monitor is already attached to another event loop")
        token = self._next_token
        self._next_token += 1
        self._callbacks[token] = callback
        if self._source is None:
            source = None
            try:
                source = self._source_factory()
                source.start(loop, self._queue_change)
            except BaseException:
                self._callbacks.pop(token, None)
                if source is not None:
                    try:
                        source.close()
                    except Exception:
                        logger.exception("Could not close a failed network change source")
                raise
            self._source = source
            self._loop = loop

        def unsubscribe() -> None:
            self._unsubscribe(token)

        return unsubscribe

    def _queue_change(self) -> None:
        if self._loop is None or not self._callbacks or self._pending is not None:
            return
        self._pending = self._loop.call_later(self._debounce_seconds, self._dispatch)

    def _dispatch(self) -> None:
        self._pending = None
        from netaudio.network_path import invalidate_platform_preferences

        invalidate_platform_preferences()
        for callback in tuple(self._callbacks.values()):
            try:
                callback()
            except Exception:
                logger.exception("Network change callback failed")

    def _unsubscribe(self, token: int) -> None:
        self._callbacks.pop(token, None)
        if self._callbacks or self._source is None:
            return
        if self._pending is not None:
            self._pending.cancel()
            self._pending = None
        self._source.close()
        self._source = None
        self._loop = None


network_change_monitor = NetworkChangeMonitor()


__all__ = ["NetworkChangeError", "NetworkChangeMonitor", "network_change_monitor"]
