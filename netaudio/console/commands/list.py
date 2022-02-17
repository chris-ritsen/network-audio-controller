import asyncio

from cleo import Command

from netaudio import DanteBrowser


class ListCommand(Command):
    name = 'list'
    arguments = []
    description = 'Test'

    async def list_devices(self):
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        dante_devices = await dante_browser.get_devices()

        for _, device in dante_devices.items():
            await device.get_controls()
            self.line(f'{device.name}')


    def handle(self):
        asyncio.run(self.list_devices())
