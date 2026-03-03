# netaudio-lib

Python library for controlling Audinate Dante network audio devices.

## Installation

```bash
pip install netaudio-lib
```

## Usage

```python
import asyncio
from netaudio_lib import DanteBrowser

async def main():
    browser = DanteBrowser(mdns_timeout=5)
    devices = await browser.get_devices()

    for server_name, device in devices.items():
        await device.get_controls()
        print(f"{device.name} ({device.ipv4})")

asyncio.run(main())
```
