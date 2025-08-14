from cleo.commands.command import Command

from ._list import DeviceListCommand


from typing import Any, List


class DeviceCommand(Command):
    name: str = "device"
    description: str = "Control devices"
    commands: List[Any] = [DeviceListCommand()]

    def handle(self) -> int:
        return self.call("help", f"help {self.name}")
