import time

from termcolor import colored, cprint

from netaudio.dante2.application import DanteApplication
from netaudio.utils.json_encoder import dump_json_formatted


def device_list(
    # ~ name: str = None,
    # ~ host: str = None,
    # ~ interfaces: list[str] = None,
    json: bool = False,
) -> None:
    """
    List devices discovered on the network.
    """
    # TODO: implement remaining parameters above
    app = DanteApplication()
    app.startup()
    time.sleep(1)
    try:

        if json:
            print(dump_json_formatted(app.devices))

        else:
            for device in app.devices:
                name = colored(device.name, attrs=['bold'])
                rx_count = colored(len(device.rx_channels), 'blue', attrs=['bold'])
                tx_count = colored(len(device.tx_channels), 'cyan', attrs=['bold'])
                print(f"{name} ({tx_count} x {rx_count})")

    except Exception as exception:
        raise exception
    finally:
        app.shutdown()
