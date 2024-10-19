import codecs
import ipaddress
import pprint
import random

from netaudio.dante.const import (
    DEVICE_CONTROL_PORT,
    DEVICE_SETTINGS_PORT,
    SERVICE_ARC,
)


class DanteCommandBuilder:
    def __init__(self, device):
        self.device = device

    def command_string(
        self,
        command=None,
        command_str=None,
        command_args="0000",
        sequence1="00",
        sequence2=None,
    ):
        command_str = bytes.fromhex(command_str) if command_str else None
        command_args = bytes.fromhex(command_args)
        sequence1 = bytes.fromhex(sequence1)

        if sequence2 is None:
            sequence2 = random.randint(0, 65535)

        command_length = b"\x00"

        if command == "get_channel_count":
            command_str = b"\x10\x00"
        if command == "get_device_info":
            command_str = b"\x10\x03"
        if command == "device_name":
            command_str = b"\x10\x02"
        if command == "rx_channels":
            command_str = b"\x30\x00"
        if command == "reset_device_name":
            command_str = b"\x10\x01"
            command_args = b"\x00\x00"
        if command == "set_device_name":
            command_str = b"\x10\x01"

        sequence_id = sequence2.to_bytes(2, "big")

        command_bytes = (
            b"\x27"
            + sequence1
            + b"\x00"
            + command_length
            + sequence_id
            + command_str
            + command_args
        )

        command_length = len(command_bytes).to_bytes(1, "big")
        command_bytes = bytearray(command_bytes)
        command_bytes[3] = command_length[0]

        return command_bytes.hex()

    def enable_aes67(self, is_enabled: bool):
        data_len = "24"  # == 0x24
        enable = int(is_enabled)

        # 22d after sequence ID is 1da for other dev, but works still
        sequence_id = 0xFF

        command_string = f"ffff00\
                {data_len}\
                {sequence_id:04x}22dc525400385eba0000417564696e6174650734100600000064000100\
                {enable:02x}"

        # Remove whitespace in string, that comes from formatting above
        command_string = "".join(command_string.split())

        return (command_string, None, DEVICE_SETTINGS_PORT)

    def get_transmitters(self, page=0, friendly_names=False):
        if friendly_names:
            command_str = "2010"
        else:
            command_str = "2000"

        command_args = self._channel_pagination(page=page)

        return (
            self.command_string(
                "tx_channels",
                command_str=command_str,
                command_args=command_args,
            ),
            SERVICE_ARC,
        )

    def get_device_name(self):
        return (self.command_string("device_name"), SERVICE_ARC)

    def reset_device_name(self):
        return (self.command_string("reset_name"), SERVICE_ARC)

    def set_device_name(self, name):
        args_length = chr(len(name.encode("utf-8")) + 11)
        args_length = bytes(args_length.encode("utf-8")).hex()

        return (
            self.command_string(
                "set_device_name",
                command_args=self._device_name(name),
            ),
            SERVICE_ARC,
        )

    def set_channel_name(
        self, channel_type: str, channel_number: int, new_channel_name: str = None
    ) -> tuple:
        """
        Sets or resets the name of a specified channel.

        Args:
            channel_type (str): Type of channel, either 'rx' or 'tx'.
            channel_number (int): Channel number to set or reset the name for.
            new_channel_name (str, optional): The new name to set for the channel. If None, the channel name will be reset.

        Returns:
            tuple: A tuple containing the command string and the service type.
        """

        args_length = None
        command_str = None
        command_args = None

        if channel_type not in ("rx", "tx"):
            raise ValueError("channel_type must be 'rx' or 'tx'")
        if not isinstance(channel_number, int) or channel_number < 0:
            raise ValueError("channel_number must be a non-negative integer")

        channel_hex = f"{channel_number:02x}"

        if new_channel_name is None:
            if channel_type == "rx":
                args_length = "15"
                command_args = f"0000020100{channel_hex}00140000000000"
                command_str = "3001"
            elif channel_type == "tx":
                args_length = "19"
                command_args = f"00000201000000{channel_hex}001800000000000000"
                command_str = "2013"
        else:
            if not isinstance(new_channel_name, str) or len(new_channel_name) == 0:
                raise ValueError("new_channel_name must be a non-empty string")

            name_hex = new_channel_name.encode().hex()

            if channel_type == "rx":
                command_str = "3001"
                command_args = f"0000020100{channel_hex}001400000000{name_hex}00"
                args_length = len(new_channel_name.encode("utf-8")) + 21
            elif channel_type == "tx":
                command_str = "2013"
                command_args = (
                    f"00000201000000{channel_hex}0018000000000000{name_hex}00"
                )
                args_length = len(new_channel_name.encode("utf-8")) + 25

            args_length = f"{args_length:02x}"

        return (
            self.command_string(
                "set_channel_name" if new_channel_name else "reset_channel_name",
                command_str=command_str,
                command_args=command_args,
            ),
            SERVICE_ARC,
        )

    def reset_channel_name(self, channel_type: str, channel_number: int) -> tuple:
        """
        Resets the name of a specified channel.

        Args:
            channel_type (str): Type of channel, either 'rx' or 'tx'.
            channel_number (int): Channel number to reset.

        Returns:
            tuple: A tuple containing the command string and the service type.
        """
        return self.set_channel_name(channel_type, channel_number, None)

    def get_receivers(self, page=0):
        return (
            self.command_string(
                "rx_channels", command_args=self._channel_pagination(page)
            ),
            SERVICE_ARC,
        )

    def _channel_pagination(self, page):
        page_hex = format(page, "x")
        command_args = f"0000000100{page_hex}10000"

        return command_args

    def _device_name(self, name):
        name_hex = name.encode().hex()
        return f"0000{name_hex}00"

    def get_channel_count(self):
        return (self.command_string("get_channel_count"), SERVICE_ARC)

    def get_device_info(self):
        return (self.command_string("get_device_info"), SERVICE_ARC)

    def remove_subscription(self, rx_channel):
        rx_channel_hex = f"{int(rx_channel):02x}"
        command_str = "3014"
        args_length = "10"
        command_args = f"00000001000000{rx_channel_hex}"

        return (
            self.command_string(
                "remove_subscription",
                command_str=command_str,
                command_args=command_args,
            ),
            SERVICE_ARC,
        )

    def add_subscription(self, rx_channel_number, tx_channel_name, tx_device_name):
        rx_channel_hex = f"{int(rx_channel_number):02x}"
        command_str = "3010"
        tx_channel_name_hex = tx_channel_name.encode().hex()
        tx_device_name_hex = tx_device_name.encode().hex()

        tx_channel_name_offset = f"{52:02x}"
        tx_device_name_offset = f"{52 + (len(tx_channel_name) + 1):02x}"

        command_args = f"0000020100{rx_channel_hex}00{tx_channel_name_offset}00{tx_device_name_offset}00000000000000000000000000000000000000000000000000000000000000000000{tx_channel_name_hex}00{tx_device_name_hex}00"

        return (
            self.command_string(
                "add_subscription", command_str=command_str, command_args=command_args
            ),
            SERVICE_ARC,
        )

    def command_set_sample_rate(self, sample_rate):
        data_len = 40

        command_string = f"ffff00{data_len:02x}03d400005254000000000000417564696e61746507270081000000640000000100{sample_rate:06x}"

        return (command_string, None, DEVICE_SETTINGS_PORT)

    def set_gain_level(self, channel_number, gain_level, device_type):
        data_len = 52
        target = None

        if device_type == "input":
            target = f"ffff00{data_len:02x}034400005254000000000000417564696e6174650727100a0000000000010001000c001001020000000000"
        elif device_type == "output":
            target = f"ffff00{data_len:02x}032600005254000000000000417564696e6174650727100a0000000000010001000c001002010000000000"

        command_string = f"{target}{channel_number:02x}000000{gain_level:02x}"

        return (command_string, None, DEVICE_SETTINGS_PORT)

    def set_encoding(self, encoding):
        data_len = 40

        command_string = f"ffff00{data_len}03d700005254000000000000417564696e617465072700830000006400000001000000{encoding:02x}"

        return (command_string, None, DEVICE_SETTINGS_PORT)

    def identify_device(self):
        mac = "000000000000"
        data_len = 32

        command_string = (
            f"ffff00{data_len:02x}0bc80000{mac}0000417564696e6174650731006300000064"
        )

        return (command_string, None, DEVICE_SETTINGS_PORT)

    def set_latency(self, latency):
        command_str = "1101"
        latency = int(latency * 1000000)
        latency_hex = f"{latency:06x}"

        command_args = f"00000503820500200211001083010024821983018302830600{latency_hex}00{latency_hex}"

        return (
            self.command_string(
                "set_latency",
                command_str=command_str,
                command_args=command_args,
            ),
            SERVICE_ARC,
        )

    def volume_start(self, device_name, ipv4, mac, port, timeout=True):
        data_len = 0
        device_name_hex = device_name.encode().hex()
        ip_hex = ipv4.packed.hex()

        name_len1, name_len2, name_len3 = self._get_name_lengths(device_name)

        if len(device_name) % 2 == 0:
            device_name_hex = f"{device_name_hex}00"

        if len(device_name) < 2:
            data_len = 54
        elif len(device_name) < 4:
            data_len = 56
        else:
            data_len = len(device_name) + (len(device_name) & 1) + 54

        unknown_arg = "16"
        command_string = f"120000{data_len:02x}ffff301000000000{mac}0000000400{name_len1:02x}000100{name_len2:02x}000a{device_name_hex}{unknown_arg}0001000100{name_len3:02x}0001{port:04x}{timeout:04x}0000{ip_hex}{port:04x}0000"

        return (command_string, None, DEVICE_CONTROL_PORT)

    def volume_stop(self, device_name, ipv4, mac, port):
        data_len = 0
        device_name_hex = device_name.encode().hex()
        ip_hex = ipaddress.IPv4Address(0).packed.hex()

        name_len1, name_len2, name_len3 = self._get_name_lengths(device_name)

        if len(device_name) % 2 == 0:
            device_name_hex = f"{device_name_hex}00"

        if len(device_name) < 2:
            data_len = 54
        elif len(device_name) < 4:
            data_len = 56
        else:
            data_len = len(device_name) + (len(device_name) & 1) + 54

        command_string = f"120000{data_len:02x}ffff301000000000{mac}0000000400{name_len1:02x}000100{name_len2:02x}000a{device_name_hex}010016000100{name_len3:02x}0001{port:04x}00010000{ip_hex}{0:04x}0000"

        return (command_string, None, DEVICE_CONTROL_PORT)

    def make_model(self, mac):
        cmd_args = "00c100000000"
        command_string = f"ffff00200fdb0000{mac}0000417564696e6174650731{cmd_args}"

        return command_string

    def dante_model(self, mac):
        cmd_args = "006100000000"
        command_string = f"ffff00200fdb0000{mac}0000417564696e6174650731{cmd_args}"

        return command_string

    def _get_name_lengths(self, device_name):
        name_len = len(device_name)
        offset = (name_len & 1) - 2
        padding = 10 - (name_len + offset)
        name_len1 = (len(device_name) * 2) + padding
        name_len2 = name_len1 + 2
        name_len3 = name_len2 + 4

        return (name_len1, name_len2, name_len3)
