from cleo.commands.command import Command
from cleo.helpers import option

from ._list import ChannelListCommand


from typing import Any, List


class ChannelCommand(Command):
    name: str = "channel"
    description: str = "Control channels"
    commands: List[Any] = [ChannelListCommand()]

    def handle(self) -> int:
        return self.call("help", f"help {self.name}")
