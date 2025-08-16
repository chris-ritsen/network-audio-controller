from enum import Enum
from typing import List

from netaudio.dante.device import DanteDevice
from netaudio.dante.browser import DanteBrowser
from netaudio.utils.cli import FireTyped
from netaudio.dante.channel import ChannelType
from netaudio.dante.protocols import Encoding, SampleRate

class GainLevel(int, Enum):
    LEVEL_1 = 1
    LEVEL_2 = 2
    LEVEL_3 = 3
    LEVEL_4 = 4
    LEVEL_5 = 5

async def set_gain_level_func(device:DanteDevice, channel_number:str, gain_level:GainLevel):
    raise NotImplementedError("This function is not ported yet")

    device_type = None
    label = None

    if device.model_id in ["DAI1", "DAI2"]:
        device_type = "input"

        label = {
            1: "+24 dBu",
            2: "+4dBu",
            3: "+0 dBu",
            4: "0 dBV",
            5: "-10 dBV",
        }
    elif device.model_id in ["DAO1", "DAO2"]:
        device_type = "output"

        label = {
            1: "+18 dBu",
            2: "+4 dBu",
            3: "+0 dBu",
            4: "0 dBV",
            5: "-10 dBV",
        }

    try:
        gain_level = int(gain_level)
    except ValueError:
        print("Invalid value for gain level")
        return

    try:
        channel_number = int(channel_number)
    except ValueError:
        print("Invalid channel number")
        return

    if channel_number:
        if (
            device_type == "output" and channel_number not in device.rx_channels
        ) or (device_type == "input" and channel_number not in device.tx_channels):
            print("Invalid channel number")
            return

    if device_type:
        print(
            f"Setting gain level of {device.name} {device.ipv4} to {label[gain_level]} on channel {channel_number}"
        )
        await device.set_gain_level(channel_number, gain_level, device_type)
    else:
        print("This device does not support gain control")

@FireTyped
async def device_configure(
        # Device filtering:
        name:str = None,
        host:str = None,
        interfaces:List[str] = None,
        mdns_timeout:float=1.5,

        # Configuration options:
        channel_number: int = None,
        channel_type: ChannelType = None,
        reset_channel_name: bool = None,
        reset_device_name: bool = None,

        identify: bool = None,
        set_channel_name: str = None,
        set_device_name: str = None,
        set_encoding: Encoding = None,
        set_gain_level: GainLevel = None,
        set_latency: int = None,
        set_sample_rate: SampleRate = None,
        aes67_enable: bool = None,
        aes67_disable: bool = None
):
    """
    Configure a device's parameters
    """
    dante_browser = DanteBrowser(mdns_timeout=mdns_timeout)

    devices = await dante_browser.get_devices(
        filter_name=name,
        filter_host=host,
        interfaces=interfaces
    )

    for _, device in devices.items():
        await device.begin()

    devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

    try:
        device = list(devices.values()).pop()
        print(f"Chose device {device.name} out of {len(devices)} devices")
    except IndexError:
        print("Device not found")
        return

    if reset_channel_name or set_channel_name:
        if channel_number:
            channel_number = int(channel_number)
        else:
            print("Must specify a channel number")
            return

        if not channel_type:
            print("Must specify a channel type")
            return

        if channel_number and channel_type:
            if reset_channel_name:
                print(
                    f"Resetting name of {channel_type} channel {channel_number} for {device.name} {device.ipv4}"
                )
                await device.reset_channel_name(channel_type, channel_number)
            elif set_channel_name:
                if len(set_channel_name) > 31:
                    print("New channel name will be truncated")
                    set_channel_name = set_channel_name[:31]

                print(
                    f"Setting name of {channel_type} channel {channel_number} for {device.name} {device.ipv4} to {set_channel_name}"
                )
                await device.set_channel_name(
                    channel_type, channel_number, set_channel_name
                )

    if reset_device_name:
        print(f"Resetting device name for {device.name} {device.ipv4}")
        await device.reset_name()

    if identify:
        print(f"Identifying device {device.name} {device.ipv4}")
        await device.identify()

    if set_device_name:
        if len(set_device_name) > 31:
            print("New device name will be truncated")
            set_device_name = set_device_name[:31]

        print(
            f"Setting device name for {device.name} {device.ipv4} to {set_device_name}"
        )
        await device.set_name(set_device_name)

    if set_latency:
        print(f"Setting latency of {device} to {set_latency:g} ms")
        await device.set_latency(set_latency)

    if set_sample_rate:
        print(
            f"Setting sample rate of {device.name} {device.ipv4} to {set_sample_rate}"
        )
        await device.set_sample_rate(set_sample_rate)

    if set_encoding:
        print(
            f"Setting encoding of {device.name} {device.ipv4} to {set_encoding}"
        )
        await device.set_encoding(set_encoding)

    if set_gain_level:
        if channel_number is None:
            print("Must specify a channel number for gain level setting")
            return
        print(
            f"Setting gain level of {device.name} {device.ipv4} to {set_gain_level}"
        )
        await set_gain_level_func(
            device, channel_number, set_gain_level
        )

    if aes67_enable:
        is_enabled = True
        print(
            f"Setting AES67 of {device.name} {device.ipv4} to {is_enabled}"
        )
        await device.enable_aes67(is_enabled)

    if aes67_disable:
        print(
            f"Setting AES67 of {device.name} {device.ipv4} to {is_enabled}"
        )
        is_enabled = False
        await device.enable_aes67(is_enabled)
