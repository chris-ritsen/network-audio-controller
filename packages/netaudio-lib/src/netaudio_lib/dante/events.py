import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Coroutine

logger = logging.getLogger("netaudio")


class EventType(Enum):
    DEVICE_DISCOVERED = auto()
    DEVICE_REMOVED = auto()
    DEVICE_UPDATED = auto()
    CHANNEL_NAME_UPDATED = auto()
    SUBSCRIPTION_CHANGED = auto()
    SAMPLE_RATE_CHANGED = auto()
    LATENCY_CHANGED = auto()
    AES67_CHANGED = auto()
    METER_VALUES = auto()
    NOTIFICATION_RECEIVED = auto()


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
        self._queue: asyncio.Queue[DanteEvent] = asyncio.Queue()
        self._dispatch_task: asyncio.Task | None = None
        self._running = False

    def on(self, event_type: EventType, callback: EventCallback) -> None:
        if event_type not in self._listeners:
            self._listeners[event_type] = []
        self._listeners[event_type].append(callback)

    def off(self, event_type: EventType, callback: EventCallback) -> None:
        if event_type in self._listeners:
            try:
                self._listeners[event_type].remove(callback)
            except ValueError:
                pass

    def emit_nowait(self, event: DanteEvent) -> None:
        self._queue.put_nowait(event)

    async def emit(self, event: DanteEvent) -> None:
        await self._queue.put(event)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._dispatch_task = asyncio.create_task(self._dispatch_loop())

    async def stop(self) -> None:
        self._running = False
        if self._dispatch_task is not None:
            self._dispatch_task.cancel()
            try:
                await self._dispatch_task
            except asyncio.CancelledError:
                pass
            self._dispatch_task = None

    async def _dispatch_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            callbacks = self._listeners.get(event.type, [])
            for callback in callbacks:
                try:
                    await callback(event)
                except Exception:
                    logger.exception(
                        f"Error in event callback for {event.type.name}"
                    )
