# ~ from ipaddress import IPv4Address
import time

from termcolor import cprint

from netaudio.dante2.application import DanteApplication
from netaudio.utils.json_encoder import dump_json_formatted

def channel_list(
    # ~ name: str = None,
    # ~ host: str = None,
    # ~ interfaces: list[str] = None,
    json: bool = False,
) -> None:
    """
    List channels discoverable on the network.
    """
    # TODO: implement remaining arguments above
    app = DanteApplication()
    app.startup()
    time.sleep(1)
    try:

        if json:
            channels: dict[str, list[Any]] = {}
            for device in app.devices:
                channels[device.name] = {
                    "receivers": {chan.number: chan for chan in device.rx_channels},
                    "transmitters": {chan.number: chan for chan in device.tx_channels},
                }
            print(dump_json_formatted(channels))

        else:
            for device in app.devices:
                cprint(device.name, attrs=["bold"])

                tx_channels = device.tx_channels
                if tx_channels:
                    cprint("tx channels", "cyan", attrs=["bold"])
                    for channel in tx_channels:
                        print(f"\t{channel.number}: {channel.name}")

                rx_channels = device.rx_channels
                if rx_channels:
                    if tx_channels:
                        print("")
                    cprint("rx channels", "blue", attrs=["bold"])
                    for channel in rx_channels:
                        print(f"\t{channel.number}: {channel.name}")
                print()

    except Exception as exception:
        raise exception
    finally:
        app.shutdown()
