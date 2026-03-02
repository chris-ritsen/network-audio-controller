from importlib.metadata import version

__version__ = version("netaudio-lib")

from netaudio_lib.dante.application import DanteApplication
from netaudio_lib.dante.browser import DanteBrowser
from netaudio_lib.dante.channel import DanteChannel
from netaudio_lib.dante.device import DanteDevice
from netaudio_lib.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio_lib.dante.subscription import DanteSubscription

__all__ = [
    "DanteApplication",
    "DanteBrowser",
    "DanteChannel",
    "DanteDevice",
    "DanteEvent",
    "DanteEventDispatcher",
    "EventType",
    "DanteSubscription",
]
