from netaudio.dante.browser import DanteBrowser
from netaudio.utils.json_encoder import dump_json_formatted
from netaudio.utils.cli import FireTyped
from typing import List

@FireTyped
async def subscription_list(
        json: bool = False,
        interfaces: List[str] = None
) -> None:
    """
    List all subscriptions.
    """
    subscriptions = []

    dante_browser = DanteBrowser(mdns_timeout=1.5)
    devices = await dante_browser.get_devices(interfaces=interfaces)
    devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

    for _, device in devices.items():
        await device.begin()

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

    if json:
        json_object = dump_json_formatted(subscriptions)
        print(f"{json_object}")
    else:
        for subscription in subscriptions:
            print(f"{subscription}")
