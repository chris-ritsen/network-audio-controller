import asyncio
import json

from json import JSONEncoder

from cleo import Command
from cleo.helpers import option

from netaudio.dante.browser import DanteBrowser


def _default(self, obj):
    return getattr(obj.__class__, 'to_json', _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class ConfigCommand(Command):
    name = 'config'
    description = 'Configure devices'

    options = [
        option('device-name', None, 'Specify a device name', flag=False),
        option('json', None, 'Output as JSON', flag=True)
    ]

    async def device_configure(self):
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        devices = await dante_browser.get_devices()
        name = None

        for _, device in devices.items():
            await device.get_controls()

        if self.option('device-name'):
            name = self.option('device-name')
        else:
            device_names = sorted(list({k: v.name for k, v in devices.items()}.values()))
            name = self.choice('Select a device', device_names, None)

        if not name:
            return

        devices = dict(filter(lambda d: d[1].name == name, devices.items()))

        if self.option('json'):
            json_object = json.dumps(devices, indent=2)
            self.line(f'{json_object}')
        else:
            for _, device in devices.items():
                self.line(f'{device}')


    def handle(self):
        asyncio.run(self.device_configure())
