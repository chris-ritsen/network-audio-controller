import sys

from .dante.browser import DanteBrowser
from .dante.channel import DanteChannel
from .dante.control import DanteControl
from .dante.device import DanteDevice
from .dante.multicast import DanteMulticast
from .dante.subscription import DanteSubscription

from .console.application import main

__author__ = "Chris Ritsen"
__maintainer__ = "Chris Ritsen <chris.ritsen@gmail.com>"
__version__ = "0.0.2"

if sys.version_info <= (3, 9):
    raise ImportError("Python version > 3.9 required.")
