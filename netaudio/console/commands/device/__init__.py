import asyncio

from cleo.commands.command import Command
from cleo.helpers import option

from netaudio import DanteBrowser

from ._list import DeviceListCommand


class DeviceCommand(Command):
    name = "device"
    description = "Control devices"
    commands = [DeviceListCommand()]

    def handle(self):
        return self.call("help", self.name)
