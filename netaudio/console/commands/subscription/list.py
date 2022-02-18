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


class SubscriptionListCommand(Command):
    name = 'list'
    description = 'List subscriptions'

    options = [
        option('json', None, 'Output as JSON', flag=True)
    ]

    #  options = [
    #      option('rx-channel-name', None, 'Filter by Rx channel name', flag=False),
    #      option('rx-channel-number', None, 'Filter by Rx channel number', flag=False),
    #      option('rx-device-host', None, 'Filter by Rx device host', flag=False),
    #      option('rx-device-name', None, 'Filter by Rx device name', flag=False),
    #      option('tx-channel-name', None, 'Filter by Tx channel name', flag=False),
    #      option('tx-channel-number', None, 'Filter by Tx channel number', flag=False),
    #      option('tx-device-host', None, 'Filter by Tx device host', flag=False),
    #      option('tx-device-name', None, 'Filter by Tx device name', flag=False),
    #  ]

    async def subscription_add(self):
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        devices = await dante_browser.get_devices()

        subscriptions = []

        for _, device in devices.items():
            await device.get_controls()

        #  rx_channel = None
        #  rx_device = None
        #  tx_channel = None
        #  tx_device = None

        #  if self.option('tx-device-name'):
        #      tx_device = next(filter(lambda d: d[1].name == self.option('tx-device-name'), devices.items()))[1]
        #  elif self.option('tx-device-host'):
        #      tx_device = next(filter(lambda d: d[1].ipv4 == self.option('tx-device-host'), devices.items()))[1]

        #  if self.option('tx-channel-name'):
        #      tx_channel = next(filter(lambda c: c[1].name == self.option('tx-channel-name'), tx_device.tx_channels.items()))[1]
        #  elif self.option('tx-channel-number'):
        #      tx_channel = next(filter(lambda c: c[1].number == self.option('tx-channel-number'), tx_device.tx_channels.items()))[1]

        #  if self.option('rx-device-name'):
        #      rx_device = next(filter(lambda d: d[1].name == self.option('rx-device-name'), devices.items()))[1]
        #  elif self.option('rx-device-host'):
        #      rx_device = next(filter(lambda d: d[1].ipv4 == self.option('rx-device-host'), devices.items()))[1]

        #  if self.option('rx-channel-name'):
        #      rx_channel = next(filter(lambda c: c[1].name == self.option('rx-channel-name'), rx_device.rx_channels.items()))[1]
        #  elif self.option('rx-channel-number'):
        #      rx_channel = next(filter(lambda c: c[1].number == self.option('rx-channel-number'), rx_device.rx_channels.items()))[1]

        for _, device in devices.items():
            for subscription in device.subscriptions:
                subscriptions.append(subscription)

        if self.option('json'):
            json_object = json.dumps(subscriptions, indent=2)
            self.line(f'{json_object}')
        else:
            for subscription in subscriptions:
                self.line(f'{subscription}')


    def handle(self):
        asyncio.run(self.subscription_add())
