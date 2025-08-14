import fire

from importlib.metadata import version

from os import name as os_name

# from netaudio.console.application import Command

if os_name == "nt":
    # Fix Windows issue, See: https://stackoverflow.com/q/58718659/
    try:
        from signal import signal, SIGPIPE, SIG_DFL

        signal(SIGPIPE, SIG_DFL)
    except ImportError:  # If SIGPIPE is not available (win32),
        pass  # we don't have to do anything to ignore it.

from netaudio.console.commands.device import DeviceCommands
from netaudio.console.commands.channel import ChannelCommands
from netaudio.console.commands.subscription import SubscriptionCommands
from netaudio.console.commands.config import device_configure
from netaudio.console.commands.server import ServerCommands

class Application(object):
    """ 
    A tool to control & configure DANTE network audio devices.
    """

    def __init__(self):
        self.device = DeviceCommands()
        self.channel = ChannelCommands()
        self.subscription = SubscriptionCommands()
        self.config = device_configure
        self.server = ServerCommands()

def main():
    fire.Fire(Application)


if __name__ == "__main__":
    main()
