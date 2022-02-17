import asyncio

from cleo import Command
from cleo.helpers import option

from netaudio.dante.browser import DanteBrowser


class SubscriptionAddCommand(Command):
    name = "add"
    description = "Add a subscription"

    options = [
        option("rx-channel-name", None, "Specify Rx channel by name", flag=False),
        option("rx-channel-number", None, "Specify Rx channel by number", flag=False),
        option("rx-device-host", None, "Specify Tx device by host", flag=False),
        option("rx-device-name", None, "Specify Tx device by name", flag=False),
        option("tx-channel-name", None, "Specify Tx channel by name", flag=False),
        option("tx-channel-number", None, "Specify Tx channel by number", flag=False),
        option("tx-device-host", None, "Specify Tx device by host", flag=False),
        option("tx-device-name", None, "Specify Tx device by name", flag=False),
    ]

    async def subscription_add(self):
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        dante_devices = await dante_browser.get_devices()

        for _, device in dante_devices.items():
            await device.get_controls()

        rx_channel = None
        rx_device = None
        tx_channel = None
        tx_device = None

        if self.option("tx-device-name"):
            tx_device = next(
                filter(
                    lambda d: d[1].name == self.option("tx-device-name"),
                    dante_devices.items(),
                )
            )[1]
        elif self.option("tx-device-host"):
            tx_device = next(
                filter(
                    lambda d: d[1].ipv4 == self.option("tx-device-host"),
                    dante_devices.items(),
                )
            )[1]

        if self.option("tx-channel-name"):
            tx_channel = next(
                filter(
                    lambda c: self.option("tx-channel-name") == c[1].friendly_name
                    or self.option("tx-channel-name") == c[1].name
                    and not c[1].friendly_name,
                    tx_device.tx_channels.items(),
                )
            )[1]
        elif self.option("tx-channel-number"):
            tx_channel = next(
                filter(
                    lambda c: c[1].number == self.option("tx-channel-number"),
                    tx_device.tx_channels.items(),
                )
            )[1]

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

        if rx_device and not tx_device:
            tx_device = rx_device

        if rx_channel and rx_device and tx_channel and tx_channel:
            self.line(
                f"{rx_channel.name}@{rx_device.name} <- {tx_channel.name}@{tx_device.name}"
            )
            await rx_device.add_subscription(rx_channel, tx_channel, tx_device)

    def handle(self):
        asyncio.run(self.subscription_add())
