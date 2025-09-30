# ~ from ipaddress import IPv4Address
import time

from termcolor import colored, cprint

from netaudio.dante2.application import DanteApplication
from netaudio.dante2.channel import DanteChannelType


def subscription_add(
        # ~ interfaces: list[str] = None,

        rx_channel_name: str = None,
        rx_channel_number: int = None,
        # ~ rx_device_host: str = None,
        rx_device_name: str = None,

        tx_channel_name: str = None,
        tx_channel_number: int = None,
        # ~ tx_device_host: str = None,
        tx_device_name: str = None,
) -> None:
    """
    Subscribe a Receiving channel to a Transmitting channel.
    """
    # TODO: implement remaining arguments above
    app = DanteApplication()
    app.startup()
    time.sleep(1)

    try:
        rx_device = app.get_device_by_name(rx_device_name)
        if not rx_device:
            cprint("No matching RX Device found.", "red")
            app.shutdown()
            return

        tx_device = app.get_device_by_name(tx_device_name)
        if not tx_device:
            tx_device = rx_device

        if rx_channel_name:
            rx_channel = rx_device.get_channel_by_name(DanteChannelType.RX, str(rx_channel_name))
        elif rx_channel_number:
            rx_channel = rx_device.get_channel_by_number(DanteChannelType.RX, rx_channel_number)
        else:
            rx_channel = None

        if tx_channel_name:
            tx_channel = tx_device.get_channel_by_name(DanteChannelType.TX, str(tx_channel_name))
        elif tx_channel_number:
            tx_channel = tx_device.get_channel_by_number(DanteChannelType.TX, tx_channel_number)
        else:
            tx_channel = None

        if rx_channel and tx_channel:
            rx_channel.subscribe(tx_channel)
            print(f"{colored(rx_channel, 'blue', attrs=['bold'])} <- {colored(tx_channel, 'cyan', attrs=['bold'])}")
            time.sleep(1)
        else:
            cprint("No matching RX or TX channels found.", "red")

    except Exception as exception:
        raise exception
    finally:
        app.shutdown()
