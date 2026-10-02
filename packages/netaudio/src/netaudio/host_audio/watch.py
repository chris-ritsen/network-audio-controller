from __future__ import annotations

import asyncio
import ctypes
import ctypes.util
import logging
import os
import struct
import sys
from collections.abc import Callable
from pathlib import Path

logger = logging.getLogger("netaudio")

IN_MOVED_TO = 0x00000080
IN_CREATE = 0x00000100
IN_NONBLOCK = 0o4000
IN_CLOEXEC = 0o2000000
EVENT_HEADER = struct.Struct("iIII")


class DirectoryWatch:
    def __init__(self, on_created: Callable[[Path], None]):
        self.on_created = on_created
        self._library = None
        self._descriptor: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._watches: dict[int, Path] = {}

    @staticmethod
    def supported() -> bool:
        return sys.platform.startswith("linux")

    def start(self) -> bool:
        if self._descriptor is not None:
            return True
        if not self.supported():
            return False
        self._library = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
        descriptor = self._library.inotify_init1(IN_NONBLOCK | IN_CLOEXEC)
        if descriptor < 0:
            logger.warning("Cannot watch for audio servers starting: %s", os.strerror(ctypes.get_errno()))
            return False
        self._descriptor = descriptor
        self._loop = asyncio.get_running_loop()
        self._loop.add_reader(descriptor, self._read)
        return True

    def watch(self, directory: Path) -> bool:
        if self._descriptor is None or self._library is None:
            return False
        if directory in self._watches.values():
            return True
        if not directory.is_dir():
            return False
        watch = self._library.inotify_add_watch(self._descriptor, os.fsencode(directory), IN_CREATE | IN_MOVED_TO)
        if watch < 0:
            logger.warning("Cannot watch %s for audio servers: %s", directory, os.strerror(ctypes.get_errno()))
            return False
        self._watches[watch] = directory
        return True

    def _read(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        try:
            data = os.read(descriptor, 65536)
        except BlockingIOError:
            return
        offset = 0
        while offset + EVENT_HEADER.size <= len(data):
            watch, _, _, length = EVENT_HEADER.unpack_from(data, offset)
            start = offset + EVENT_HEADER.size
            name = data[start : start + length].rstrip(b"\0")
            offset = start + length
            directory = self._watches.get(watch)
            if directory is not None and name:
                self.on_created(directory / os.fsdecode(name))

    def stop(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        self._descriptor = None
        if self._loop is not None and not self._loop.is_closed():
            self._loop.remove_reader(descriptor)
        os.close(descriptor)
        self._watches.clear()
