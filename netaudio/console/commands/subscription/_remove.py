import asyncio

from cleo import Command
from cleo.helpers import option

from netaudio.dante.browser import DanteBrowser


class SubscriptionRemoveCommand(Command):
    name = "remove"
    description = "Remove a subscription"

    options = [
        option("rx-channel-name", None, "Specify Rx channel by name", flag=False),
        option("rx-channel-number", None, "Specify Rx channel by number", flag=False),
        option("rx-device-host", None, "Specify Rx device by host", flag=False),
        option("rx-device-name", None, "Specify Rx device by name", flag=False),
    ]

    async def subscription_add(self):
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        dante_devices = await dante_browser.get_devices()

        for _, device in dante_devices.items():
            await device.get_controls()

        rx_channel = None
        rx_device = None

        if self.option("rx-device-name"):
            rx_device = next(
                filter(
                    lambda d: d[1].name == self.option("rx-device-name"),
                    dante_devices.items(),
                )
            )[1]
        elif self.option("rx-device-host"):
            rx_device = next(
                filter(
                    lambda d: d[1].ipv4 == self.option("rx-device-host"),
                    dante_devices.items(),
                )
            )[1]

        if self.option("rx-channel-name"):
            rx_channel = next(
                filter(
                    lambda c: c[1].name == self.option("rx-channel-name"),
                    rx_device.rx_channels.items(),
                )
            )[1]
        elif self.option("rx-channel-number"):
            rx_channel = next(
                filter(
                    lambda c: c[1].number == self.option("rx-channel-number"),
                    rx_device.rx_channels.items(),
                )
            )[1]

        if rx_channel and rx_device:
            await rx_device.remove_subscription(rx_channel)

    def handle(self):
        asyncio.run(self.subscription_add())
