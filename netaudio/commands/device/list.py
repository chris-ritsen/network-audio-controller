from netaudio.utils.json_encoder import dump_json_formatted
from netaudio.utils.cli import FireTyped

from netaudio.dante.browser import DanteBrowser

@FireTyped
async def device_list(
    name: str | None = None,
    host: str | None = None,
    json: bool = False
) -> None:
    cached_devices = None #_get_devices_from_redis()

    dante_browser = DanteBrowser(mdns_timeout=1.5)

    devices = cached_devices if cached_devices is not None else await dante_browser.get_devices(
            filter_name=name,
            filter_host=host
        )

    for _, device in devices.items():
        await device.get_controls()

    devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

    if json:
        print(dump_json_formatted(devices))
    else:
        for _, device in devices.items():
            print(device)
