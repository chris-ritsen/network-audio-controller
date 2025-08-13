from cleo.commands.command import Command
from cleo.helpers import option
from ._http import ServerHttpCommand
from ._mdns import ServerMdnsCommand


class ServerCommand(Command):
    name = "server"
    description = "Servers"
    commands = [ServerHttpCommand(), ServerMdnsCommand()]

    def handle(self):
        return self.call("help", self.name)
