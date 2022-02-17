from cleo import Command
from cleo.helpers import option

from ._list import ChannelListCommand


class ChannelCommand(Command):
    name = "channel"
    description = "Control channels"
    commands = [ChannelListCommand()]

    def handle(self):
        return self.call("help", self._config.name)
