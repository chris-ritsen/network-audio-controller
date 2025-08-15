import asyncio
import json

from json import JSONEncoder

from netaudio.dante.browser import DanteBrowser

from typing import Any, Dict, Optional


def _default(self: Any, obj: Any) -> Any:
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default



def _print_channel_list(devices: Dict[str, Any], as_json: Optional[bool] = False) -> None:
    if as_json:
        channels: Dict[str, Any] = {}

        for _, device in devices.items():
            channels[device.name] = {
                "receivers": device.rx_channels,
                "transmitters": device.tx_channels,
            }

        json_object = json.dumps(channels, indent=2)
        print(f"{json_object}")
    else:
        for index, (_, device) in enumerate(devices.items()):
            print(f"<info>{device.name}</info>")
            if device.tx_channels:
                print("<info>tx channels</info>")

            for _, channel in device.tx_channels.items():
                print(f"{channel}")

            if device.rx_channels:
                if device.tx_channels:
                    print("")

                print("<info>rx channels</info>")

            for _, channel in device.rx_channels.items():
                print(f"{channel}")

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
