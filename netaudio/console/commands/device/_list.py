import asyncio
import json

from json import JSONEncoder

from cleo import Command
from cleo.helpers import option

from netaudio.dante.browser import DanteBrowser


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class DeviceListCommand(Command):
    name = "list"
    description = "List devices"

    options = [option("json", None, "Output as JSON", flag=True)]

    async def device_list(self):
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        devices = await dante_browser.get_devices()

        for _, device in devices.items():
            await device.get_controls()

        if self.option("json"):
            json_object = json.dumps(devices, indent=2)
            self.line(f"{json_object}")
        else:
            for _, device in devices.items():
                self.line(f"{device}")

    def handle(self):
        asyncio.run(self.device_list())
