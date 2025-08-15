from importlib.metadata import version

PACKAGE_NAME = __package__.split('.')[0]

class CLI(object):
    @property
    def __doc__(self):
        print(__package__)
        return f"""
Network Audio Controller

Configure Dante devices on the network

To get command arguments, run:
  netaudio <command> --help

Version: {version(PACKAGE_NAME)}
"""
    def __init__(self):
        from netaudio.commands.device import DeviceCommands
        from netaudio.commands.channel import ChannelCommands
        from netaudio.commands.subscription import SubscriptionCommands
        from netaudio.commands.server import ServerCommands

        self.device = DeviceCommands()
        self.channel = ChannelCommands()
        self.subscription = SubscriptionCommands()
        self.server = ServerCommands()

def run_cli():
    import fire

    fire.Fire(CLI)
