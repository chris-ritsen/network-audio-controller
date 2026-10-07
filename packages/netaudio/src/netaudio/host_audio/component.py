from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from netaudio.asynchronous_primitives import DeferredAsyncioLock

logger = logging.getLogger("netaudio")

REFRESH_DELAY_SECONDS = 0.05
CHANGE_HISTORY = 200
RECONNECT_INTERVAL_SECONDS = 5.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class AudioComponent:
    name = "audio"

    def __init__(self, on_change: Callable[[str], None] | None = None):
        self.on_change = on_change
        self.available = False
        self.installed = True
        self.reason: str | None = "not started"
        self.connected_at: str | None = None
        self.changes: deque[dict] = deque(maxlen=CHANGE_HISTORY)
        self.generation = 0
        self.seen_running = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._running = False
        self._refresh_handle: asyncio.TimerHandle | None = None
        self._refresh_task: asyncio.Task | None = None
        self._refresh_again = False
        self._refresh_lock = DeferredAsyncioLock()
        self._connect_lock = DeferredAsyncioLock()
        self._last_connect_attempt = 0.0
        self._waiters: list[asyncio.Future] = []

    def _installation_problem(self) -> str | None:
        raise NotImplementedError

    async def _open(self) -> str | None:
        raise NotImplementedError

    async def _close(self) -> None:
        raise NotImplementedError

    async def _read(self) -> list[dict]:
        raise NotImplementedError

    def _clear(self) -> None:
        raise NotImplementedError

    async def start(self) -> None:
        problem = self._installation_problem()
        if problem is not None:
            self.installed = False
            self.reason = problem
            return
        self._loop = asyncio.get_running_loop()
        self._running = True
        await self.connect()

    async def stop(self) -> None:
        self._running = False
        if self._refresh_handle is not None:
            self._refresh_handle.cancel()
            self._refresh_handle = None
        task = self._refresh_task
        self._refresh_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self.available:
            self.available = False
            await self._close()
        self.reason = "stopped"
        self._wake_waiters()

    async def connect(self) -> bool:
        async with self._connect_lock:
            if self.available or not self._running:
                return self.available
            self._last_connect_attempt = time.monotonic()
            reason = await self._open()
            if reason is not None:
                self.reason = reason
                return False
            self.available = True
            self.seen_running = True
            self.reason = None
            self.connected_at = utc_now()
            self.record("server_connected")
        await self.refresh()
        return True

    async def ensure(self) -> None:
        if self.available or not self._running:
            return
        if time.monotonic() - self._last_connect_attempt >= RECONNECT_INTERVAL_SECONDS:
            await self.connect()

    def record(self, kind: str, **fields: Any) -> None:
        self.changes.append({"time": utc_now(), "kind": kind, **fields})

    def schedule_refresh(self) -> None:
        if not self.available or self._loop is None or self._refresh_handle is not None:
            return
        self._refresh_handle = self._loop.call_later(REFRESH_DELAY_SECONDS, self._begin_refresh)

    def schedule_refresh_threadsafe(self) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(self.schedule_refresh)

    def _begin_refresh(self) -> None:
        self._refresh_handle = None
        if self._refresh_task is not None and not self._refresh_task.done():
            self._refresh_again = True
            return
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name=f"host-audio-{self.name}-refresh")

    async def _refresh_loop(self) -> None:
        while True:
            self._refresh_again = False
            await self.refresh()
            if not self._refresh_again:
                return

    async def refresh(self) -> None:
        async with self._refresh_lock:
            if not self.available:
                return
            changes = await self._read()
            for change in changes:
                self.changes.append(change)
            self.generation += 1
            self._wake_waiters()
            if changes and self.on_change is not None:
                self.on_change(self.name)

    def lost(self, reason: str) -> None:
        if not self.available:
            return
        self.available = False
        self.reason = reason
        self.record("server_stopped", reason=reason)
        self._clear()
        self.generation += 1
        self._wake_waiters()
        if self.on_change is not None:
            self.on_change(self.name)

    async def wait_changed(self, generation: int, timeout: float) -> bool:
        if self.generation > generation:
            return True
        if self._loop is None or timeout <= 0:
            return False
        future = self._loop.create_future()
        self._waiters.append(future)
        try:
            await asyncio.wait_for(future, timeout)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            if future in self._waiters:
                self._waiters.remove(future)

    def _wake_waiters(self) -> None:
        waiters = self._waiters
        self._waiters = []
        for future in waiters:
            if not future.done():
                future.set_result(None)
