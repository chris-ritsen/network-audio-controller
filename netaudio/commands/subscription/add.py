from netaudio.dante.browser import DanteBrowser
from netaudio.commands.cli_utils import FireTyped

@FireTyped
async def subscription_add(
        rx_channel_name: str = None,
        rx_channel_number: int = None,
        rx_device_host: str = None,
        rx_device_name: str = None,

        tx_channel_name: str = None,
        tx_channel_number: int = None,
        tx_device_host: str = None,
        tx_device_name: str = None
) -> None:
    dante_browser = DanteBrowser(mdns_timeout=1.5)
    dante_devices = await dante_browser.get_devices()

    for _, device in dante_devices.items():
        await device.get_controls()

    rx_channel = None
    rx_device = None
    tx_channel = None
    tx_device = None

    if tx_device_name:
        tx_device = next(
            filter(
                lambda d: d[1].name == tx_device_name,
                dante_devices.items(),
            )
        )[1]
    elif tx_device_host:
        tx_device = next(
            filter(
                lambda d: d[1].ipv4 == tx_device_host,
                dante_devices.items(),
            )
        )[1]

    if tx_channel_name:
        tx_channel = next(
            filter(
                lambda c: tx_channel_name == c[1].friendly_name
                or tx_channel_name == c[1].name
                and not c[1].friendly_name,
                tx_device.tx_channels.items(),
            )
        )[1]
    elif tx_channel_number:
        tx_channel = next(
            filter(
                lambda c: c[1].number == tx_channel_number,
                tx_device.tx_channels.items(),
            )
        )[1]

    if rx_device_name:
        rx_device = next(
            filter(
                lambda d: d[1].name == rx_device_name,
                dante_devices.items(),
            )
        )[1]
    elif rx_device_host:
        rx_device = next(
            filter(
                lambda d: d[1].ipv4 == rx_device_host,
                dante_devices.items(),
            )
        )[1]

    if rx_channel_name:
        rx_channel = next(
            filter(
                lambda c: c[1].name == rx_channel_name,
                rx_device.rx_channels.items(),
            )
        )[1]
    elif rx_channel_number:
        rx_channel = next(
            filter(
                lambda c: c[1].number == rx_channel_number,
                rx_device.rx_channels.items(),
            )
        )[1]

    if rx_device and not tx_device:
        tx_device = rx_device

    if rx_channel and rx_device and tx_channel and tx_channel:
        print(
            f"{rx_channel.name}@{rx_device.name} <- {tx_channel.name}@{tx_device.name}"
        )
        await rx_device.add_subscription(rx_channel, tx_channel, tx_device)
