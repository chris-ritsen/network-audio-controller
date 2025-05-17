import asyncio

import typer
from typing_extensions import Annotated

from netaudio.common.app_config import settings as app_settings
from netaudio.dante.browser import DanteBrowser
from netaudio.dante.const import SERVICE_CMC


async def subscription_add(
    rx_channel_name: Annotated[
        str, typer.Option(help="Specify Rx channel by name")
    ] = None,
    rx_channel_number: Annotated[
        str, typer.Option(help="Specify Rx channel by number")
    ] = None,
    rx_device_host: Annotated[
        str, typer.Option(help="Specify Rx device by host")
    ] = None,
    rx_device_name: Annotated[
        str, typer.Option(help="Specify Rx device by name")
    ] = None,
    tx_channel_name: Annotated[
        str, typer.Option(help="Specify Tx channel by name")
    ] = None,
    tx_channel_number: Annotated[
        str, typer.Option(help="Specify Tx channel by number")
    ] = None,
    tx_device_host: Annotated[
        str, typer.Option(help="Specify Tx device by host")
    ] = None,
    tx_device_name: Annotated[
        str, typer.Option(help="Specify Tx device by name")
    ] = None,
):
    try:
        dante_browser = DanteBrowser(mdns_timeout=app_settings.mdns_timeout)
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
                ),
                None,
            )
        elif tx_device_host:
            tx_device = next(
                filter(
                    lambda d: d[1].ipv4 == tx_device_host,
                    dante_devices.items(),
                ),
                None,
            )

        if tx_device:
            if tx_channel_name:
                tx_channel = next(
                    filter(
                        lambda c: tx_channel_name == c[1].friendly_name
                        or (tx_channel_name == c[1].name and not c[1].friendly_name),
                        tx_device.tx_channels.items(),
                    ),
                    None,
                )
            elif tx_channel_number:
                tx_channel_number_str = str(tx_channel_number)
                tx_channel = next(
                    filter(
                        lambda c: str(c[1].number) == tx_channel_number_str,
                        tx_device.tx_channels.items(),
                    ),
                    None,
                )

        if rx_device_name:
            rx_device = next(
                filter(
                    lambda d: d[1].name == rx_device_name,
                    dante_devices.items(),
                ),
                None,
            )
        elif rx_device_host:
            rx_device = next(
                filter(
                    lambda d: d[1].ipv4 == rx_device_host,
                    dante_devices.items(),
                ),
                None,
            )

        if rx_device:
            if rx_channel_name:
                rx_channel = next(
                    filter(
                        lambda c: c[1].name == rx_channel_name,
                        rx_device.rx_channels.items(),
                    ),
                    None,
                )
            elif rx_channel_number:
                rx_channel_number_str = str(rx_channel_number)
                rx_channel = next(
                    filter(
                        lambda c: str(c[1].number) == rx_channel_number_str,
                        rx_device.rx_channels.items(),
                    ),
                    None,
                )

        if rx_device and not tx_device and rx_channel:
            tx_device = rx_device

        if rx_channel and rx_device and tx_channel and tx_device:
            print(
                f"{rx_channel.name}@{rx_device.name} <- {tx_channel.name}@{tx_device.name}"
            )
            await rx_device.add_subscription(rx_channel, tx_channel, tx_device)
            print("Subscription added successfully.")
        else:
            error_messages = []
            if not rx_device:
                error_messages.append("Receiver device not found or specified.")
            elif not rx_channel:
                error_messages.append(
                    f"Receiver channel not found or specified for device {getattr(rx_device, 'name', 'Unknown')}."
                )
            if not tx_device:
                error_messages.append("Transmitter device not found or specified.")
            elif not tx_channel:
                error_messages.append(
                    f"Transmitter channel not found or specified for device {getattr(tx_device, 'name', 'Unknown')}."
                )

            if (
                not (rx_channel and rx_device and tx_channel and tx_device)
                and not error_messages
            ):
                error_messages.append(
                    "Could not form subscription. Ensure all device and channel parameters are correct."
                )

            for msg in error_messages:
                print(f"Error: {msg}")
            if error_messages:
                raise typer.Exit(code=1)
    except Exception as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)
