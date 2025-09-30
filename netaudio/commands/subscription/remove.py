# ~ from ipaddress import IPv4Address
import time

from termcolor import colored, cprint

from netaudio.dante2.application import DanteApplication
from netaudio.dante2.channel import DanteChannelType

def subscription_remove(
        # ~ interfaces: list[str] = None,

        rx_channel_name: str = None,
        rx_channel_number: int = None,
        # ~ rx_device_host: str = None,
        rx_device_name: str = None,
) -> None:
    """
    Remove the subscription from a Receiving Channel.
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

        if rx_channel_name:
            rx_channel = rx_device.get_channel_by_name(DanteChannelType.RX, str(rx_channel_name))
        elif rx_channel_number:
            rx_channel = rx_device.get_channel_by_number(DanteChannelType.RX, rx_channel_number)
        else:
            rx_channel = None

        if rx_channel:
            rx_channel.unsubscribe()
            print(f"Removing subscription from {colored(rx_channel, 'blue', attrs=['bold'])}")
            time.sleep(1)
        else:
            cprint("No matching RX Channel found.", "red")

    except Exception as exception:
        raise exception
    finally:
        app.shutdown()
