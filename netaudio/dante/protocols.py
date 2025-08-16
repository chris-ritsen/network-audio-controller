import random
import codecs
import traceback
import logging

from enum import Enum
from ipaddress import IPv4Address
from socket import socket, AF_INET, SOCK_DGRAM

from netaudio.dante.channel import DanteChannel, ChannelType
from netaudio.dante.subscription import DanteSubscription

logger = logging.getLogger("netaudio-dante-protocol")

class ValueInitializableIntEnum(int, Enum):
    # Accept initializing enum from string
    @classmethod
    def from_string(cls, value: str) -> "ValueInitializableIntEnum":
        try:
            return cls(int(value))
        except Exception:
            try:
                return cls[value.upper()]
            except KeyError:
                raise ValueError(f"Invalid {cls.__name__} value: {value}")

class Encoding(ValueInitializableIntEnum):
    PCM_16 = 16
    PCM_24 = 24
    PCM_32 = 32

class SampleRate(ValueInitializableIntEnum):
    SR_44100 = 44100
    SR_48000 = 48000
    SR_88200 = 88200
    SR_96000 = 96000
    SR_176400 = 176400
    SR_192000 = 192000

class DanteService:
    sock:socket

    def begin(self, ipv4:IPv4Address):
        self.sock = socket(AF_INET, SOCK_DGRAM)
        self.sock.bind(("", 0))
        self.sock.settimeout(1)
        # self.sock.settimeout(0.01)
        self.sock.connect((str(ipv4), self.port))

    async def _send(self, cmd:str, recv:int|None=None) -> bytes:
        binary_str = codecs.decode(cmd, "hex")

        try:
            self.sock.send(binary_str)
        except Exception as e:
            print(e)
            traceback.print_exc()

        if recv:
            try:
                return self.sock.recvfrom(recv)[0]
            except TimeoutError:
                pass
                # raise TimeoutError("Dante command timed out waiting for response")

class ServiceARC(DanteService):
    """
    Dante Audio Routing Channel
    """
    name: str = "_netaudio-arc._udp.local."
    port: int = 4440

    async def begin(self, *args, **kwargs):
        super().begin(*args, **kwargs)

    def _command(
        self,
        command:str,
        command_args="0000",
        command_length="0a",
    ):
        sequence2 = random.randint(0, 65535)
        sequence_id = f"{sequence2:04x}"

        command_hex = (
            f"27ff00{command_length}{sequence_id}{command}{command_args}"
        )

        if command == "3010": # add subscription. Reset command length
            command_length = f"{int(len(command_hex) / 2):02x}"
            command_hex = f"27ff00{command_length}{sequence_id}{command}{command_args}"

        return command_hex


    async def set_latency(self, latency:int) -> None:
        latency = int(latency * 1000000)
        latency_hex = f"{latency:08x}"

        command_args = f"000005038205002002110010830100248219830183028306{latency_hex}{latency_hex}"

        return await self._send(self._command(
            "1101",
            command_length="28",
            command_args=command_args,
        ))

    async def add_subscription(
        self, rx_channel_number:int, tx_channel_name:str, tx_device_name:str
    ):
        rx_channel_hex = f"{int(rx_channel_number):02x}"
        tx_channel_name_hex = tx_channel_name.encode().hex()
        tx_device_name_hex = tx_device_name.encode().hex()

        tx_channel_name_offset = f"{52:02x}"
        tx_device_name_offset = f"{52 + (len(tx_channel_name) + 1):02x}"

        command_args = f"0000020100{rx_channel_hex}00{tx_channel_name_offset}00{tx_device_name_offset}00000000000000000000000000000000000000000000000000000000000000000000{tx_channel_name_hex}00{tx_device_name_hex}00"

        return await self._send(self._command(
            "3010",
            # command_length = f"{int(len(command_hex) / 2):02x}"
            command_args=command_args
        ))

    async def remove_subscription(self, rx_channel:int):
        rx_channel_hex = f"{int(rx_channel):02x}"

        return await self._send(self._command(
                "3014",
                command_length="10",
                command_args=f"00000001000000{rx_channel_hex}",
            ))

    async def get_channel_count(self) -> tuple[int, int] | None:
        response = await self._send(self._command("1000"), 2048)
        if response:
            rx = int.from_bytes(
                response[15:16], "big"
            )
            tx = int.from_bytes(
                response[13:14], "big"
            )

            return (rx, tx)
        else:
            logger.warning("Failed to get Dante channel counts")
            return None

    async def set_name(self, name):
        args_length = chr(len(name.encode("utf-8")) + 11)
        args_length = bytes(args_length.encode("utf-8")).hex()

        return await self._send(self._command(
            "1001",
            command_length=args_length,
            command_args=f"0000{name.encode().hex()}00",
        ))

    async def reset_name(self):
        return await self._send(self._command(
            "1001",
            command_args="0000"
        ))

    async def get_device_name(self) -> str | None:
        response = await self._send(self._command("1002"), 2048)
        if response:
            return response[10:-1].decode("ascii")
        else:
            logger.warning("Failed to get Dante device name")
            return None

    async def command_device_info(self):
        return await self._send(self._command("1003"))

    async def set_channel_name(self, channel_type:ChannelType, channel_number:int, new_channel_name:str):
        name_hex = new_channel_name.encode().hex()
        channel_hex = f"{channel_number:02x}"

        if channel_type == ChannelType.RX:
            command_str = "3001"
            command_args = f"0000020100{channel_hex}001400000000{name_hex}00"
            args_length = chr(len(new_channel_name.encode("utf-8")) + 21)
        if channel_type == ChannelType.TX:
            command_str = "2013"
            command_args = f"00000201000000{channel_hex}0018000000000000{name_hex}00"
            args_length = chr(len(new_channel_name.encode("utf-8")) + 25)

        args_length = bytes(args_length.encode("utf-8")).hex()

        return await self._send(self._command(
                command_str=command_str,
                command_length=args_length,
                command_args=command_args,
            ))

    async def reset_channel_name(self, channel_type:ChannelType, channel_number):
        channel_hex = f"{channel_number:02x}"

        if channel_type == ChannelType.RX:
            args_length = f"{int(21):02x}"
            command_args = f"0000020100{channel_hex}00140000000000"
            command_str = "3001"
        if channel_type == ChannelType.TX:
            args_length = f"{int(25):02x}"
            command_args = f"00000201000000{channel_hex}001800000000000000"
            command_str = "2013"

        return await self._send(self._command(
                command_str=command_str,
                command_args=command_args,
                command_length=args_length,
            ))

    def _channel_pagination(self, page):
        return f"0000000100{format(page, "x")}10000"
    
    def _get_label(self, hex_str, offset):
        try:
            hex_substring = hex_str[int(offset, 16) * 2 :]
            partitioned_bytes = bytes.fromhex(hex_substring).partition(b"\x00")[0]
            return partitioned_bytes.decode("utf-8")
        except Exception:
            return None

    async def get_channels_subscriptions(self, channel_count:tuple[int, int], device_name:str):
        rx_channels:dict[str, DanteChannel] = {}
        tx_channels:dict[str, DanteChannel] = {}
        subscriptions:list[DanteSubscription] = []
        sample_rate:int = 0

        ch_count = channel_count if channel_count else await self.get_channel_count()
        rx_count = ch_count[0]
        tx_count = ch_count[1]
        ##################### RX Channels #####################
        try:
            for page in range(0, max(int(rx_count / 16), 1)):
                receivers = await self._send(self._command(
                    "3000",
                    command_length="10",
                    command_args=self._channel_pagination(page)
                ), 2048)

                hex_rx_response = receivers.hex()

                for index in range(0, min(rx_count, 16)):
                    n = 4
                    str1 = hex_rx_response[(24 + (index * 40)) : (56 + (index * 40))]
                    channel = [str1[i : i + n] for i in range(0, len(str1), n)]

                    if channel:
                        channel_number = int(channel[0], 16)
                        channel_offset = channel[3]
                        device_offset = channel[4]
                        rx_channel_offset = channel[5]
                        rx_channel_status_code = int(channel[6], 16)
                        subscription_status_code = int(channel[7], 16)

                        rx_channel_name = self._get_label(
                            hex_rx_response, rx_channel_offset
                        )

                        tx_device_name = self._get_label(hex_rx_response, device_offset)

                        if not channel_offset == "0000":
                            tx_channel_name = self._get_label(
                                hex_rx_response, channel_offset
                            )
                        else:
                            tx_channel_name = rx_channel_name

                        if index == 0 and not device_offset == "0000":
                            o1 = (int(channel[2], 16) * 2) + 2
                            o2 = o1 + 6
                            sample_rate = int(hex_rx_response[o1:o2], 16)

                        rx_channels[channel_number] = DanteChannel(
                            type=ChannelType.RX,
                            friendly_name=rx_channel_name,
                            name=rx_channel_name,
                            number=channel_number,
                            status_code=rx_channel_status_code
                        )

                        subscription = DanteSubscription(
                            rx_channel_name=rx_channel_name,
                            rx_device_name=device_name,
                            tx_channel_name=tx_channel_name,
                            status_code=subscription_status_code,
                            rx_channel_status_code=rx_channel_status_code,

                            tx_device_name=device_name if tx_device_name == "." else tx_device_name,
                        )

                        subscriptions.append(subscription)
        except Exception as e:
            self.error = e
            print(e)
            traceback.print_exc()

        ##################### TX Channels #####################

        try:
            tx_friendly_channel_names = {}

            for page in range(0, max(1, int(tx_count / 16)), 2):
                friendly_names = True
                response = await self._send(self._command(
                    "2010" if friendly_names else "2000",
                    command_length="10",
                    command_args=self._channel_pagination(page=page),
                ), 2048)
                tx_friendly_names = response.hex()

                for index in range(0, min(tx_count, 32)):
                    str1 = tx_friendly_names[(24 + (index * 12)) : (36 + (index * 12))]
                    n = 4
                    channel = [str1[i : i + 4] for i in range(0, len(str1), n)]
                    #  channel_index = int(channel[0], 16)
                    channel_number = int(channel[1], 16)
                    channel_offset = channel[2]
                    tx_channel_friendly_name = self._get_label(
                        tx_friendly_names, channel_offset
                    )

                    if tx_channel_friendly_name:
                        tx_friendly_channel_names[channel_number] = (
                            tx_channel_friendly_name
                        )

            for page in range(0, max(1, int(tx_count / 16)), 2):
                friendly_names = False
                response = await self._send(self._command(
                    "2010" if friendly_names else "2000",
                    command_length="10",
                    command_args=self._channel_pagination(page=page),
                ), 2048)
                transmitters = response.hex()

                has_disabled_channels = False

                # TODO: Find the sample rate in the response instead of relying on it being already set from elsewhere
                if sample_rate:
                    has_disabled_channels = (
                        transmitters.count(f"{sample_rate:06x}") == 2
                    )

                first_channel = []

                for index in range(0, min(tx_count, 32)):
                    str1 = transmitters[(24 + (index * 16)) : (40 + (index * 16))]
                    n = 4
                    channel = [str1[i : i + 4] for i in range(0, len(str1), n)]

                    if index == 0:
                        first_channel = channel

                    if channel:
                        o1 = (int(channel[2], 16) * 2) + 2
                        o2 = o1 + 6
                        sample_rate_hex = transmitters[o1:o2]

                        if sample_rate_hex != "000000":
                            sample_rate = int(sample_rate_hex, 16)

                        channel_number = int(channel[0], 16)
                        # channel_status = channel[1][2:]
                        channel_group = channel[2]
                        channel_offset = channel[3]

                        #  channel_enabled = channel_group == first_channel[2]
                        channel_disabled = channel_group != first_channel[2]

                        if channel_disabled:
                            break

                        tx_channel_name = self._get_label(transmitters, channel_offset)


                        tx_channels[channel_number] = DanteChannel(
                            type=ChannelType.TX,
                            number=channel_number,
                            name=tx_channel_name,
                            friendly_name=tx_friendly_channel_names[
                                channel_number
                            ] if channel_number in tx_friendly_channel_names else None,
                        )

                if has_disabled_channels:
                    break

        except Exception as e:
            self.error = e
            print(e)
            traceback.print_exc()

        ##################### Return #####################
        return (
            rx_channels,
            tx_channels,
            subscriptions,
            sample_rate
        )

class ServiceSettings(DanteService):
    """
    Multicast Control and Monitoring
    """
    name = None
    port = 8700

    async def begin(self, *args, **kwargs):
        super().begin(*args, **kwargs)

    def _command(
            self,
            sequence_id:str|None,
            cmd_args:str,
            mac:str,
            data_len:str,
            part1:str="0000"
    ):
        if (not sequence_id):
            sequence2 = random.randint(0, 65535)
            sequence_id = f"{sequence2:04x}"

        return f"ffff00{data_len}{sequence_id}{part1}{mac}0000417564696e61746507{cmd_args}"

    async def command_identify(self):
        cmd = self._command(
            data_len=f"{32:02x}",
            sequence_id="0bc8", # probably sequence_id!!
            mac="000000000000",
            cmd_args="31006300000064"
        )
        return await self._send(cmd)

    async def command_dante_model(self, mac:str):
        cmd = self._command(
            data_len="20",
            sequence_id="0fdb",
            mac=mac,
            cmd_args="31006100000000"
        )
        return await self._send(cmd)

    async def command_make_model(self, mac):
        cmd = self._command(
            data_len="20",
            sequence_id="0fdb",
            mac=mac,
            cmd_args="3100c100000000"
        )
        return await self._send(cmd)

    async def set_encoding(self, encoding:Encoding):
        cmd = self._command(
            data_len="40",
            sequence_id="03d7",
            mac="27525400000000",
            cmd_args=f"2700830000006400000001000000{encoding.value:02x}"
        )
        return await self._send(cmd)

    async def set_gain_level(self, channel_number:int, gain_level:int, device_type:ChannelType):
        cmd = self._command(
            data_len="52",
            sequence_id="0344",
            mac="525400000000",
            cmd_args=f"27100a0000000000010001000c00100{'102' if device_type == ChannelType.RX else '201'}0000000000{channel_number:02x}000000{gain_level:02x}"
        )
        return await self._send(cmd)

    async def set_sample_rate(self, sample_rate:SampleRate):
        cmd = self._command(
            data_len="40",
            sequence_id="03d4",
            mac="525400000000",
            cmd_args=f"270081000000640000000100{sample_rate.value:06x}"
        )
        return await self._send(cmd)

    async def set_aes67(self, is_enabled: bool):
        cmd = self._command(
            data_len="24",
            sequence_id="00ff",
            part1="22dc",
            mac="525400385eba",
            cmd_args=f"34100600000064000100{is_enabled:02x}"
        )
        return await self._send(cmd)

#########################################################
# UNIMPLEMENTED
#########################################################

class ServiceDBC(DanteService):
    """
    Dante Broadcast Control Channel
    Audio Control (excluding Via)
    """
    name:str = "_netaudio-dbc._udp.local."
    port:int = 4455 # or 4440, 4444

class ServiceCHAN:
    name: str = "_netaudio-chan._udp.local."
    # port: int = 4444
    port = None

class ServiceCMC(DanteService):
    """
    Dante Control Monitoring Channel
    (excluding DVS-4.0 & Via)
    """
    name: str = "_netaudio-cmc._udp.local."
    port: int = 8800

    async def begin(self, *args, **kwargs):
        super().begin(*args, **kwargs)

    def _get_name_lengths(self, device_name):
        name_len = len(device_name)
        offset = (name_len & 1) - 2
        padding = 10 - (name_len + offset)
        name_len1 = (len(device_name) * 2) + padding
        name_len2 = name_len1 + 2
        name_len3 = name_len2 + 4

        return (name_len1, name_len2, name_len3)

    def _command_volume_start(self, device_name:str, ipv4:str, mac:str, port:int, timeout=True):
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
        return f"120000{data_len:02x}ffff301000000000{mac}0000000400{name_len1:02x}000100{name_len2:02x}000a{device_name_hex}{unknown_arg}0001000100{name_len3:02x}0001{port:04x}{timeout:04x}0000{ip_hex}{port:04x}0000"

    def _command_volume_stop(self, device_name:str, ipv4:str, mac:str, port:int):
        data_len = 0
        device_name_hex = device_name.encode().hex()
        ip_hex = IPv4Address(0).packed.hex()

        name_len1, name_len2, name_len3 = self._get_name_lengths(device_name)

        if len(device_name) % 2 == 0:
            device_name_hex = f"{device_name_hex}00"

        if len(device_name) < 2:
            data_len = 54
        elif len(device_name) < 4:
            data_len = 56
        else:
            data_len = len(device_name) + (len(device_name) & 1) + 54

        return f"120000{data_len:02x}ffff301000000000{mac}0000000400{name_len1:02x}000100{name_len2:02x}000a{device_name_hex}010016000100{name_len3:02x}0001{port:04x}00010000{ip_hex}{0:04x}0000"

    # def _parse_volume(self, bytes_volume):
    #     rx_channels = bytes_volume[-1 - self.rx_count_raw : -1]
    #     tx_channels = bytes_volume[
    #         -1 - self.rx_count_raw - self.tx_count_raw : -1 - self.rx_count_raw
    #     ]

    #     try:
    #         for _, channel in self.tx_channels.items():
    #             channel.volume = tx_channels[channel.number - 1]

    #         for _, channel in self.rx_channels.items():
    #             channel.volume = rx_channels[channel.number - 1]

    #     except Exception as e:
    #         print(e)
    #         traceback.print_exc()


    # async def get_volume(self, ipv4, mac, port):
    #     try:
    #         if self.software or (self.model_id in FEATURE_VOLUME_UNSUPPORTED):
    #             return
    #         if port in sockets:
    #             sock = sockets[port]
    #         else:
    #             sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #             sock.settimeout(0.1)
    #             sock.bind((str(ipv4), port))
    #             sockets[port] = sock
    #         volume_start = await self._send(self._command_volume_start(self.name, ipv4, mac, port))
    #         if volume_start[15:16] == b"\xff":
    #             logger.debug(f"Volume level command is unsupported on {self.name}")
    #             return
    #         while True:
    #             try:
    #                 data, addr = sock.recvfrom(2048)
    #                 if addr[0] == str(self.ipv4):
    #                     await self._send(self._command_volume_stop(self.name, ipv4, mac, port))
    #                     self._parse_volume(data)
    #                 break
    #             except socket.timeout:
    #                 break
    #             except Exception as e:
    #                 print(e)
    #                 traceback.print_exc()
    #                 break
    #     except Exception as e:
    #         traceback.print_exc()
    #         print(e)
