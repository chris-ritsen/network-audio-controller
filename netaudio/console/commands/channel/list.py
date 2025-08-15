from termcolor import cprint

from netaudio.dante.browser import DanteBrowser

from typing import Any, Dict, Optional
from netaudio.console.json_encoder import dump_json_formatted

def _print_channel_list(devices: Dict[str, Any], as_json: Optional[bool] = False) -> None:
    if as_json:
        channels: Dict[str, Any] = {}

        for _, device in devices.items():
            channels[device.name] = {
                "receivers": device.rx_channels,
                "transmitters": device.tx_channels,
            }

        print(dump_json_formatted(channels))
    else:
        for index, (_, device) in enumerate(devices.items()):
            cprint(device.name, attrs=["bold"])
            if device.tx_channels:
                cprint("tx channels", "blue", attrs=["bold"])

            for _, channel in device.tx_channels.items():
                print(f"\t{channel}")

            if device.rx_channels:
                if device.tx_channels:
                    print("")

                cprint("rx channels", "blue", attrs=["bold"])

            for _, channel in device.rx_channels.items():
                print(f"\t{channel}")

            if index < len(devices) - 1:
                print("")

async def channel_list(name:Optional[str]=None,
                        host:Optional[str]=None,
                        json:Optional[bool]=False) -> None:

    dante_browser = DanteBrowser(mdns_timeout=1.5)

    devices = await dante_browser.get_devices(
        filter_name=name,
        filter_host=host
    )

    devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

    for _, device in devices.items():
        await device.get_controls()


    _print_channel_list(devices)
