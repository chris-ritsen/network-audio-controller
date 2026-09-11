from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Coroutine

logger = logging.getLogger("netaudio")


class EventType(Enum):
    DEVICE_DISCOVERED = auto()
    DEVICE_REMOVED = auto()
    DEVICE_STATUS_RECEIVED = auto()
    DEVICE_UPDATED = auto()
    METER_VALUES = auto()
    NOTIFICATION_RECEIVED = auto()
    SHURE_DEVICE_DISCOVERED = auto()
    SHURE_DEVICE_REMOVED = auto()
    SHURE_DEVICE_UPDATED = auto()
    SHURE_METER_VALUES = auto()


@dataclass
class DanteEvent:
    type: EventType
    device_name: str = ""
    server_name: str = ""
    data: dict = field(default_factory=dict)


EventCallback = Callable[[DanteEvent], Coroutine[Any, Any, None]]


class DanteEventDispatcher:
    def __init__(self):
        self._listeners: dict[EventType, list[EventCallback]] = {}
        self._queue: asyncio.Queue[DanteEvent | None] | None = None
        self._pending_events: deque[DanteEvent] = deque()
        self._coalesced_events: dict[tuple, DanteEvent] = {}
        self._dispatch_task: asyncio.Task | None = None
        self._running = False

    def on(self, event_type: EventType, callback: EventCallback) -> None:
        if event_type not in self._listeners:
            self._listeners[event_type] = []
        self._listeners[event_type].append(callback)

    def off(self, event_type: EventType, callback: EventCallback) -> None:
        listeners = self._listeners.get(event_type)
        if listeners is not None and callback in listeners:
            listeners.remove(callback)

    @staticmethod
    def _coalescing_key(event: DanteEvent) -> tuple | None:
        if event.type == EventType.NOTIFICATION_RECEIVED and event.server_name:
            return (event.type, event.server_name, event.data.get("notification_id"))
        if event.type == EventType.METER_VALUES and event.data.get("metering_source") == "detailed":
            return (event.type, event.server_name)
        return None

    def emit_nowait(self, event: DanteEvent) -> None:
        key = self._coalescing_key(event)
        if event.type in (EventType.DEVICE_DISCOVERED, EventType.DEVICE_REMOVED):
            self._coalesced_events.clear()
        elif event.type == EventType.METER_VALUES and key is None:
            self._coalesced_events.pop((EventType.METER_VALUES, event.server_name), None)
        if key is not None:
            existing = self._coalesced_events.get(key)
            if existing is not None:
                existing.device_name = event.device_name
                existing.data = event.data
                return
            self._coalesced_events[key] = event
        queue = self._queue
        if queue is None:
            self._pending_events.append(event)
            return
        queue.put_nowait(event)

    async def emit(self, event: DanteEvent) -> None:
        self.emit_nowait(event)

    async def start(self) -> None:
        if self._running:
            return
        queue: asyncio.Queue[DanteEvent | None] = asyncio.Queue()
        while self._pending_events:
            queue.put_nowait(self._pending_events.popleft())
        self._queue = queue
        self._running = True
        self._dispatch_task = asyncio.create_task(self._dispatch_loop(queue))

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        queue = self._queue
        self._queue = None
        dispatch_task = self._dispatch_task
        if queue is not None:
            queue.put_nowait(None)
        if dispatch_task is not None and dispatch_task is not asyncio.current_task():
            await dispatch_task
        if self._dispatch_task is dispatch_task:
            self._dispatch_task = None

    async def _dispatch_loop(self, queue: asyncio.Queue[DanteEvent | None]) -> None:
        while True:
            event = await queue.get()
            if event is None:
                return
            key = self._coalescing_key(event)
            if key is not None and self._coalesced_events.get(key) is event:
                self._coalesced_events.pop(key, None)
            callbacks = self._listeners.get(event.type, [])
            for callback in callbacks:
                try:
                    await callback(event)
                except Exception:
                    logger.exception(f"Error in event callback for {event.type.name}")
