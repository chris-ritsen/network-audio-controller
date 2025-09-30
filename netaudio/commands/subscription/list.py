# ~ from ipaddress import IPv4Address
import time

from termcolor import colored, cprint

from netaudio.dante2.application import DanteApplication
from netaudio.utils.json_encoder import dump_json_formatted


def subscription_list(
    # ~ interfaces: list[str] = None,
    json: bool = False,
) -> None:
    """
    List all subscriptions.
    """
    # TODO: implement remaining argument above
    app = DanteApplication()
    app.startup()
    time.sleep(1)

    try:

        if json:
            subscriptions: list[dict] = []
            for device in app.devices:
                rx_channels = device.rx_channels
                if rx_channels:
                    subscriptions.extend([chan.subscription for chan in device.rx_channels])
            print(dump_json_formatted(subscriptions))

        else:
            for device in app.devices:
                rx_channels = device.rx_channels
                if rx_channels:
                    for channel in rx_channels:
                        rx_text = colored(channel, 'blue', attrs=['bold'])
                        if channel.subscription.tx_channel:
                            tx_text = f" <- {colored(channel.subscription.tx_channel, 'cyan', attrs=['bold'])}"
                        else:
                            tx_text = ""
                        status_text = colored(", ".join(channel.subscription.status_text), 'light_grey')
                        print(f"{rx_text}{tx_text} [{status_text}]")

    except Exception as exception:
        raise exception
    finally:
        app.shutdown()
