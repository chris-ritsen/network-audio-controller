import asyncio
import ipaddress
import json
import socket

from json import JSONEncoder

from cleo import Command
from cleo.helpers import option

from netaudio.dante.browser import DanteBrowser


def _default(self, obj):
    return getattr(obj.__class__, 'to_json', _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class ChannelListCommand(Command):
    name = 'list'
    description = 'List channels'

    options = [
        option('json', None, 'Output as JSON', flag=True),
        option('device-host', None, 'Specify device by host', flag=False),
        option('device-name', None, 'Specify device by name', flag=False)
    ]


    async def channel_list(self):
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        devices = await dante_browser.get_devices()

        if self.option('device-name'):
            devices = dict(filter(lambda d: d[1].ipv4 == self.option('device-name'), devices.items()))
        elif self.option('device-host'):
            ipv4 = None

            try:
                ipv4 = ipaddress.ip_address(self.option('device-host'))
            except ValueError:
                pass

            try:
                ipv4 = ipaddress.ip_address(socket.gethostbyname(self.option('device-host')))
            except socket.gaierror as e:
                print(e)

            if ipv4:
                devices = dict(filter(lambda d: d[1].ipv4 == ipv4, devices.items()))
            else:
                devices = None

        for _, device in devices.items():
            await device.get_controls()

        if self.option('json'):
            channels = {}

            for _, device in devices.items():
                channels[device.name] = {
                    'receivers': device.rx_channels,
                    'transmitters': device.tx_channels
                }

            json_object = json.dumps(channels, indent=2)
            self.line(f'{json_object}')
        else:
            for index, (_, device) in enumerate(devices.items()):
                self.line(f'<info>{device.name}</info>')
                if device.tx_channels:
                    self.line('<info>tx channels</info>')

                for _, channel in device.tx_channels.items():
                    self.line(f'{channel}')

                if device.rx_channels:
                    if device.tx_channels:
                        self.line('')

                    self.line('<info>rx channels</info>')

                for _, channel in device.rx_channels.items():
                    self.line(f'{channel}')

                if index < len(devices) - 1:
                    self.line('')


    def handle(self):
        asyncio.run(self.channel_list())
