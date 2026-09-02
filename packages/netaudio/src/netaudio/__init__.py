from netaudio.dante.application import DanteApplication
from netaudio.dante.channel import DanteChannel
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio.dante.subscription import DanteSubscription

__all__ = [
    "DanteApplication",
    "DanteBrowser",
    "DanteChannel",
    "DanteDevice",
    "DanteEvent",
    "DanteEventDispatcher",
    "DanteSubscription",
    "EventType",
    "__version__",
]


def __getattr__(name: str):
    if name == "__version__":
        from importlib.metadata import version

        return version("netaudio")
    if name == "DanteBrowser":
        from netaudio.dante.browser import DanteBrowser

        return DanteBrowser
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
