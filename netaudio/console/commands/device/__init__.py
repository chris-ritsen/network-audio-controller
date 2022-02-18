import asyncio

from cleo import Command
from cleo.helpers import option

from netaudio import DanteBrowser

from ._list import DeviceListCommand


class DeviceCommand(Command):
    name = "device"
    description = "Control devices"
    commands = [DeviceListCommand()]

    options = [
        option("identify", None, "Identify the device by flashing an LED"),
        option("name", None, "Specify a device name", flag=False),
    ]

    async def identify(self):
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        devices = await dante_browser.get_devices()
        name = None

        if self.option("name"):
            name = self.option("name")

        else:
            for _, device in devices.items():
                await device.get_controls()

            device_names = sorted(
                list({k: v.name for k, v in devices.items()}.values())
            )
            name = self.choice("Select a device", device_names, None)

        if not name:
            return

        for _, device in devices.items():
            if self.option("name"):
                await device.get_controls()

            if device.name == name:
                await device.identify()

    def handle(self):
        if self.option("identify"):
            asyncio.run(self.identify())
        else:
            return self.call("help", self._config.name)
