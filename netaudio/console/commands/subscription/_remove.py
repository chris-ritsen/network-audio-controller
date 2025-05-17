import asyncio

import typer
from typing_extensions import Annotated

from netaudio.common.app_config import settings as app_settings
from netaudio.dante.browser import DanteBrowser
from netaudio.dante.device import DanteDevice


async def subscription_remove(
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
):
    dante_browser = DanteBrowser(mdns_timeout=app_settings.mdns_timeout)
    dante_devices = await dante_browser.get_devices()

    if not dante_devices:
        print("Error: No Dante devices found on the network.")
        raise typer.Exit(code=1)

    for _, device_obj in dante_devices.items():
        try:
            await device_obj.get_controls()
        except Exception as e:
            print(
                f"Warning: Could not get controls for device {getattr(device_obj, 'name', 'Unknown')}: {e}"
            )

    rx_device = None

    if rx_device_name:
        rx_device = next(
            filter(lambda d: d[1].name == rx_device_name, dante_devices.items()),
            (None, None),
        )[1]

        if not rx_device:
            print(f"Error: Receiver device named '{rx_device_name}' not found.")
            raise typer.Exit(code=1)
    elif rx_device_host:
        rx_device = next(
            filter(lambda d: d[1].ipv4 == rx_device_host, dante_devices.items()),
            (None, None),
        )[1]

        if not rx_device:
            print(f"Error: Receiver device with host '{rx_device_host}' not found.")
            raise typer.Exit(code=1)
    else:
        print(
            "Error: No receiver device specified. Use --rx-device-name or --rx-device-host."
        )
        raise typer.Exit(code=1)

    rx_channel = None

    if rx_channel_name:
        if not rx_device.rx_channels:
            print(
                f"Error: Device '{rx_device.name}' has no receivable channels listed or attribute 'rx_channels' is missing."
            )
            raise typer.Exit(code=1)

        rx_channel = next(
            filter(
                lambda c: c[1].name == rx_channel_name, rx_device.rx_channels.items()
            ),
            (None, None),
        )[1]

        if not rx_channel:
            print(
                f"Error: Receiver channel named '{rx_channel_name}' not found on device '{rx_device.name}'."
            )
            raise typer.Exit(code=1)
    elif rx_channel_number:
        if not rx_device.rx_channels:
            print(
                f"Error: Device '{rx_device.name}' has no receivable channels listed or attribute 'rx_channels' is missing."
            )
            raise typer.Exit(code=1)

        rx_channel_number_str = str(rx_channel_number)
        rx_channel = next(
            filter(
                lambda c: str(c[1].number) == rx_channel_number_str,
                rx_device.rx_channels.items(),
            ),
            (None, None),
        )[1]

        if not rx_channel:
            print(
                f"Error: Receiver channel number '{rx_channel_number_str}' not found on device '{rx_device.name}'."
            )
            raise typer.Exit(code=1)
    else:
        print(
            f"Error: No receiver channel specified for device '{rx_device.name}'. Use --rx-channel-name or --rx-channel-number."
        )
        raise typer.Exit(code=1)

    try:
        print(
            f"Attempting to remove subscription for channel '{getattr(rx_channel, 'name', 'Unknown')}' on device '{getattr(rx_device, 'name', 'Unknown')}'..."
        )
        await rx_device.remove_subscription(rx_channel)
        print(
            f"Successfully removed subscription for channel '{getattr(rx_channel, 'name', 'Unknown')}' on device '{getattr(rx_device, 'name', 'Unknown')}'."
        )
    except Exception as e:
        print(f"Error removing subscription: {e}")
        raise typer.Exit(code=1)
