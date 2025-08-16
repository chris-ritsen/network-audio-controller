from enum import Enum

from netaudio.dante.device import DanteDevice
from netaudio.dante.browser import DanteBrowser
from netaudio.commands.cli_utils import FireTyped

class ChannelType(str, Enum):
    RX = "rx"
    TX = "tx"

class Encoding(int, Enum):
    PCM_16 = 16
    PCM_24 = 24
    PCM_32 = 32

    # Accept initializing enum from string
    @classmethod
    def from_string(cls, value: str) -> "Encoding":
        try:
            return cls(int(value))
        except KeyError:
            raise ValueError(f"Invalid Encoding value: {value}")

class SampleRate(int, Enum):
    SR_44100 = 44100
    SR_48000 = 48000
    SR_88200 = 88200
    SR_96000 = 96000
    SR_176400 = 176400
    SR_192000 = 192000

class GainLevel(int, Enum):
    LEVEL_1 = 1
    LEVEL_2 = 2
    LEVEL_3 = 3
    LEVEL_4 = 4
    LEVEL_5 = 5

options_channel_type = [ChannelType.RX, ChannelType.TX]
options_encoding = [Encoding.PCM_16, Encoding.PCM_24, Encoding.PCM_32]
options_rate = [SampleRate.SR_44100, SampleRate.SR_48000, SampleRate.SR_88200, SampleRate.SR_96000, SampleRate.SR_176400, SampleRate.SR_192000]
options_gain_level = [GainLevel.LEVEL_1, GainLevel.LEVEL_2, GainLevel.LEVEL_3, GainLevel.LEVEL_4, GainLevel.LEVEL_5]

async def set_gain_level_func(device:DanteDevice, channel_number:str, gain_level:GainLevel):
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
    dante_browser = DanteBrowser(mdns_timeout=1.5)

    devices = await dante_browser.get_devices(
        filter_name=name,
        filter_host=host
    )

    for _, device in devices.items():
        await device.get_controls()

    devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

    try:
        device = list(devices.values()).pop()
    except IndexError:
        print("Device not found")
        return

    if reset_channel_name or set_channel_name:
        if channel_number:
            channel_number = int(channel_number)
        else:
            print("Must specify a channel number")

        if channel_type and channel_type in options_channel_type:
            pass
        elif channel_type:
            print("Invalid channel type")
        else:
            print("Must specify a channel type")

        if channel_number and channel_type:
            if reset_channel_name:
                print(
                    f"Resetting name of {channel_type} channel {channel_number} for {device.name} {device.ipv4}"
                )
                await device.reset_channel_name(channel_type, channel_number)
            elif set_channel_name:
                new_channel_name = set_channel_name

                if len(new_channel_name) > 31:
                    print("New channel name will be truncated")
                    new_channel_name = new_channel_name[:31]

                print(
                    f"Setting name of {channel_type} channel {channel_number} for {device.name} {device.ipv4} to {new_channel_name}"
                )
                await device.set_channel_name(
                    channel_type, channel_number, new_channel_name
                )

    if reset_device_name:
        print(f"Resetting device name for {device.name} {device.ipv4}")
        await device.reset_name()

    if identify:
        print(f"Identifying device {device.name} {device.ipv4}")
        await device.identify()

    if set_device_name:
        new_device_name = set_device_name

        if len(new_device_name) > 31:
            print("New device name will be truncated")
            new_device_name = new_device_name[:31]

        print(
            f"Setting device name for {device.name} {device.ipv4} to {new_device_name}"
        )
        await device.set_name(set_device_name)

    if set_latency:
        latency = int(set_latency)
        print(f"Setting latency of {device} to {latency:g} ms")
        await device.set_latency(latency)

    if set_sample_rate:
        sample_rate = int(set_sample_rate)
        print(
            f"Setting sample rate of {device.name} {device.ipv4} to {sample_rate}"
        )
        await device.set_sample_rate(sample_rate)

    if set_encoding:
        encoding = int(set_encoding)

        print(
            f"Setting encoding of {device.name} {device.ipv4} to {encoding}"
        )
        await device.set_encoding(encoding)

    if set_gain_level:
        await set_gain_level_func(
            device, channel_number, set_gain_level
        )

    if aes67_enable:
        is_enabled = True
        await device.enable_aes67(is_enabled)

    if aes67_disable:
        is_enabled = False
        await device.enable_aes67(is_enabled)
