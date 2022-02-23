import asyncio
import ipaddress
import json

from json import JSONEncoder

from cleo import Command
from cleo.helpers import option

from netaudio.dante.browser import DanteBrowser
from netaudio.utils import get_host_by_name


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class ConfigCommand(Command):
    name = "config"
    description = "Configure devices"

    options_channel_type = ["rx", "tx"]
    options_encoding = [16, 24, 32]
    options_rate = [44100, 48000, 88200, 96000, 176400, 192000]
    options_gain_level = list(range(1, 6))

    options = [
        option(
            "channel-number",
            None,
            "Specify a channel for control by number",
            flag=False,
        ),
        option(
            "channel-type",
            None,
            "Specify a channel for control by number {options_channel_type}",
            flag=False,
        ),
        option(
            "device-host",
            None,
            "Specify a device to configure by network address",
            flag=False,
        ),
        option(
            "device-name", None, "Specify a device to configure by name", flag=False
        ),
        option("reset-channel-name", None, "Reset a channel name", flag=True),
        option("reset-device-name", None, "Set the device name", flag=True),
        option("identify", None, "Identify the device by flashing an LED", flag=True),
        option("set-channel-name", None, "Set a channel name", flag=False),
        option("set-device-name", None, "Set the device name", flag=False),
        option(
            "set-encoding", None, f"Set the encoding. {options_encoding}", flag=False
        ),
        option(
            "set-gain-level",
            None,
            f"Set the gain level on a an AVIO device. Lower numbers are higher gain. {options_gain_level}",
            flag=False,
        ),
        option(
            "set-latency", None, "Set the device latency in milliseconds", flag=False
        ),
        option(
            "set-sample-rate",
            None,
            f"Set the sample rate of a device {options_rate}",
            flag=False,
        ),
    ]

    async def set_gain_level(self, device, channel_number, gain_level):
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
            self.line("Invalid value for gain level")
            return

        try:
            channel_number = int(channel_number)
        except ValueError:
            self.line("Invalid channel number")
            return

        if channel_number:
            if (
                device_type == "output" and channel_number not in device.rx_channels
            ) or (device_type == "input" and channel_number not in device.tx_channels):
                self.line("Invalid channel number")
                return

        if gain_level not in self.options_gain_level:
            self.line("Invalid value for gain level")
            return

        if device_type:
            self.line(
                f"Setting gain level of {device.name} {device.ipv4} to {label[gain_level]} on channel {channel_number}"
            )
            await device.set_gain_level(channel_number, gain_level, device_type)
        else:
            self.line("This device does not support gain control")

    def filter_devices(self, devices):
        if self.option("device-name"):
            devices = dict(
                filter(
                    lambda d: d[1].name == self.option("device-name"), devices.items()
                )
            )
        elif self.option("device-host"):
            host = self.option("device-host")
            ipv4 = None

            try:
                ipv4 = ipaddress.ip_address(host)
            except ValueError:
                pass

            possible_names = set([host, host + ".local.", host + "."])

            if possible_names.intersection(set(devices.keys())):
                devices = dict(
                    filter(
                        lambda d: d[1].server_name in possible_names, devices.items()
                    )
                )
            else:
                try:
                    ipv4 = get_host_by_name(host)
                except TimeoutError:
                    pass

                devices = dict(filter(lambda d: d[1].ipv4 == ipv4, devices.items()))

        return devices

    async def device_configure(self):
        option_names = list(map(lambda o: o.long_name, self.options))
        options_given = any(list([self.option(o) for o in option_names]))

        if not options_given:
            return self.call("help", self._config.name)

        dante_browser = DanteBrowser(mdns_timeout=1.5)
        devices = await dante_browser.get_devices()

        for _, device in devices.items():
            await device.get_controls()

        devices = self.filter_devices(devices)
        devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

        try:
            device = list(devices.values()).pop()
        except IndexError:
            self.line("Device not found")
            return

        if self.option("reset-channel-name") or self.option("set-channel-name"):
            if self.option("channel-number"):
                channel_number = int(self.option("channel-number"))
            else:
                self.line("Must specify a channel number")

            if (
                self.option("channel-type")
                and self.option("channel-type") in self.options_channel_type
            ):
                channel_type = self.option("channel-type")
            elif self.option("channel-type"):
                self.line("Invalid channel type")
            else:
                self.line("Must specify a channel type")

            if channel_number and channel_type:
                if self.option("reset-channel-name"):
                    self.line(
                        f"Resetting name of {channel_type} channel {channel_number} for {device.name} {device.ipv4}"
                    )
                    await device.reset_channel_name(channel_type, channel_number)
                elif self.option("set-channel-name"):
                    new_channel_name = self.option("set-channel-name")

                    if len(new_channel_name) > 31:
                        self.line("New channel name will be truncated")
                        new_channel_name = new_channel_name[:31]

                    self.line(
                        f"Setting name of {channel_type} channel {channel_number} for {device.name} {device.ipv4} to {new_channel_name}"
                    )
                    await device.set_channel_name(
                        channel_type, channel_number, new_channel_name
                    )

        if self.option("reset-device-name"):
            self.line(f"Resetting device name for {device.name} {device.ipv4}")
            await device.reset_name()

        if self.option("identify"):
            self.line(f"Identifying device {device.name} {device.ipv4}")
            await device.identify()

        if self.option("set-device-name"):
            new_device_name = self.option("set-device-name")

            if len(new_device_name) > 31:
                self.line("New device name will be truncated")
                new_device_name = new_device_name[:31]

            self.line(
                f"Setting device name for {device.name} {device.ipv4} to {new_device_name}"
            )
            await device.set_name(self.option("set-device-name"))

        if self.option("set-latency"):
            latency = int(self.option("set-latency"))
            self.line(f"Setting latency of {device} to {latency:g} ms")
            await device.set_latency(latency)

        if self.option("set-sample-rate"):
            sample_rate = int(self.option("set-sample-rate"))
            if sample_rate in self.options_rate:
                self.line(
                    f"Setting sample rate of {device.name} {device.ipv4} to {sample_rate}"
                )
                await device.set_sample_rate(sample_rate)
            else:
                self.line("Invalid sample rate")

        if self.option("set-encoding"):
            encoding = int(self.option("set-encoding"))

            if encoding in self.options_encoding:
                self.line(
                    f"Setting encoding of {device.name} {device.ipv4} to {encoding}"
                )
                await device.set_encoding(encoding)
            else:
                self.line("Invalid encoding")

        if self.option("set-gain-level"):
            await self.set_gain_level(
                device, self.option("channel-number"), self.option("set-gain-level")
            )

    def handle(self):
        asyncio.run(self.device_configure())
