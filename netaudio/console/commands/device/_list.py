import asyncio
import ipaddress
import json
import socket

from json import JSONEncoder

from cleo import Command
from cleo.helpers import option

from netaudio.dante.browser import DanteBrowser
from netaudio.utils.timeout import Timeout


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


def get_host_by_name(host):
    ipv4 = None

    try:
        ipv4 = ipaddress.ip_address(Timeout(socket.gethostbyname, 0.1)(host))
    except socket.gaierror:
        pass
    except TimeoutError:
        pass

    return ipv4


class DeviceListCommand(Command):
    name = "list"
    description = "List devices"

    options = [
        option("json", None, "Output as JSON", flag=True),
        option("host", None, "Specify device by host", flag=False),
        option("name", None, "Specify device by name", flag=False),
    ]

    def filter_devices(self, devices):
        if self.option("name"):
            devices = dict(
                filter(lambda d: d[1].name == self.option("name"), devices.items())
            )
        elif self.option("host"):
            host = self.option("host")
            ipv4 = None

            try:
                ipv4 = ipaddress.ip_address(host)
            except ValueError:
                pass

            possible_names = set([host, host + ".local.", host + "."])

            if possible_names.intersection(set(devices.keys())):
                devices = dict(
                    filter(
                        lambda d: d[1].server_name in possible_names, devices.items()
                    )
                )
            else:
                try:
                    ipv4 = get_host_by_name(host)
                except TimeoutError:
                    pass

                devices = dict(filter(lambda d: d[1].ipv4 == ipv4, devices.items()))

        return devices

    async def device_list(self):
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        devices = await dante_browser.get_devices()

        if self.option("name"):
            for _, device in devices.items():
                await device.get_controls()

        devices = self.filter_devices(devices)

        if not self.option("name"):
            for _, device in devices.items():
                await device.get_controls()

        devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

        if self.option("json"):
            json_object = json.dumps(devices, indent=2)
            self.line(f"{json_object}")
        else:
            for _, device in devices.items():
                self.line(f"{device}")

    def handle(self):
        asyncio.run(self.device_list())
