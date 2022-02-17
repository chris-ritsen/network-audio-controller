import asyncio

from cleo import Command
from cleo.helpers import option

from netaudio import DanteBrowser

class DeviceCommand(Command):
    name = 'device'
    description = ''

    options = [
        option('identify', None, 'Identify the device by flashing an LED'),
        option('name', None, 'Specify a device name', flag=False)
    ]

    async def identify(self):
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        dante_devices = await dante_browser.get_devices()
        name = self.option('name')

        for _, device in dante_devices.items():
            await device.get_controls()

            if device.name == name:
                self.line(f'name: {name}')
                self.line(f'ip: {device.ipv4}')
                await device.identify()


    def handle(self):
        if self.option('identify'):
            asyncio.run(self.identify())
