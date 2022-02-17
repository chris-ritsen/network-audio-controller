from cleo import Command
from cleo.helpers import option

from .list import DeviceListCommand


class DeviceCommand(Command):
    name = 'device'
    description = 'Control devices'
    commands = [DeviceListCommand()]


    def handle(self):
        return self.call('help', self._config.name)
