from __future__ import annotations

import asyncio


class DeferredAsyncioLock:
    def __init__(self) -> None:
        self._lock: asyncio.Lock | None = None
        self._event_loop: asyncio.AbstractEventLoop | None = None

    def _bound_lock(self) -> asyncio.Lock:
        event_loop = asyncio.get_running_loop()
        if self._lock is None:
            self._lock = asyncio.Lock()
            self._event_loop = event_loop
        elif self._event_loop is not event_loop:
            raise RuntimeError("lock cannot be shared across event loops")
        return self._lock

    async def acquire(self) -> bool:
        return await self._bound_lock().acquire()

    def release(self) -> None:
        if self._lock is None:
            raise RuntimeError("lock is not acquired")
        self._lock.release()

    def locked(self) -> bool:
        return self._lock.locked() if self._lock is not None else False

    async def __aenter__(self) -> DeferredAsyncioLock:
        await self.acquire()
        return self

    async def __aexit__(self, _exception_type, _exception, _traceback) -> None:
        self.release()


class DeferredAsyncioEvent:
    def __init__(self) -> None:
        self._event: asyncio.Event | None = None
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._set = False

    def _bound_event(self) -> asyncio.Event:
        event_loop = asyncio.get_running_loop()
        if self._event is None:
            self._event = asyncio.Event()
            self._event_loop = event_loop
            if self._set:
                self._event.set()
        elif self._event_loop is not event_loop:
            raise RuntimeError("event cannot be shared across event loops")
        return self._event

    def set(self) -> None:
        self._set = True
        if self._event is not None:
            self._event.set()

    def clear(self) -> None:
        self._set = False
        if self._event is not None:
            self._event.clear()

    def is_set(self) -> bool:
        return self._set

    async def wait(self) -> bool:
        return await self._bound_event().wait()
