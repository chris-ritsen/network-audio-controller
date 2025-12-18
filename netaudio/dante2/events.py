from enum import auto, Enum
# ~ import logging
from queue import Empty as QueueEmpty, SimpleQueue
from threading import Thread

# ~ if TYPE_CHECKING:

class DanteEventType(Enum):
    CHANNEL_NAME_UPDATED = auto()
    # ~ DEVICE_NAME_UPDATED = auto()
    SUBSCRIPTION_CHANGED = auto()
    TRANSMITTERS_CHANGED = auto()


class DanteEventDispatcher:

    def __init__(self, application):
        self._app = application
        self._listeners: list = []
        self._thread: Thread | None = None
        self._queue: SimpleQueue | None = None

    def notify(self, event_type: DanteEventType, context = None):
        self._queue.put((event_type, context))

    def register_listener(self, callback_listener):
        if callback_listener not in self._listeners:
            self._listeners.append(callback_listener)

    def run(self):
        while not self._shutdown_requested:
            try:
                event = self._queue.get(True, 0.1)
                for callback in self._listeners:
                    callback(*event)
            except QueueEmpty:
                pass

    def start(self):
        if not self._thread:
            self._shutdown_requested = False
            self._queue = SimpleQueue()
            self._thread = Thread(target=self.run)
            self._thread.start()

    def stop(self):
        if self._thread:
            self._shutdown_requested = True
            self._thread.join()
            self._thread = None

    def unregister_listener(self, callback_listener):
        if callback_listener in self._listeners:
            self._listeners.remove(callback_listener)
