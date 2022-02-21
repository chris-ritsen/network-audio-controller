import codecs
import ipaddress
import logging
import random
import socket
import traceback

from netaudio.dante.channel import DanteChannel
from netaudio.dante.subscription import DanteSubscription

from netaudio.dante.const import (
    DEVICE_CONTROL_PORT,
    DEVICE_SETTINGS_PORT,
    FEATURE_VOLUME_UNSUPPORTED,
    PORTS,
    SERVICE_ARC,
    SERVICE_CHAN,
)

logger = logging.getLogger("netaudio")
sockets = {}


class DanteDevice:
    def __init__(self, server_name=""):
        self._dante_model = ""
        self._dante_model_id = ""
        self._error = None
        self._ipv4 = None
        self._latency = None
        self._mac_address = None
        self._manufacturer = ""
        self._model = ""
        self._model_id = ""
        self._name = ""
        self._rx_channels = {}
        self._rx_count = 0
        self._rx_count_raw = 0
        self._sample_rate = None
        self._server_name = server_name
        self._services = {}
        self._sockets = {}
        self._software = None
        self._subscriptions = []
        self._tx_channels = {}
        self._tx_count = 0
        self._tx_count_raw = 0

    def __str__(self):
        return f"{self.name}"

    def dante_command_new(self, command, control):
        response = None

        binary_str = codecs.decode(command, "hex")
        response = control.sendMessage(binary_str)

        return response

    async def dante_send_command(self, command, service_type=None, port=None):
        if service_type:
            service = self.get_service(service_type)
            sock = self.sockets[service["port"]]

        if port:
            sock = self.sockets[port]

        binary_str = codecs.decode(command, "hex")

        try:
            sock.send(binary_str)
        except Exception as e:
            print(e)
            traceback.print_exc()

    async def dante_command(self, command, service_type=None, port=None):
        response = None
        sock = None

        if service_type:
            service = self.get_service(service_type)

            if service and service["port"] and service["port"] in self.sockets:
                sock = self.sockets[service["port"]]

        if port:
            sock = self.sockets[port]

        if not sock:
            return

        binary_str = codecs.decode(command, "hex")

        try:
            sock.send(binary_str)
            response = sock.recvfrom(2048)[0]
        except TimeoutError:
            pass

        return response

    async def set_channel_name(self, channel_type, channel_number, new_channel_name):
        response = await self.dante_command(
            *self.command_set_channel_name(
                channel_type, channel_number, new_channel_name
            )
        )

        return response

    async def identify(self):
        command_identify = self.command_identify()
        response = await self.dante_command(*command_identify)

        return response

    async def set_latency(self, latency):
        response = await self.dante_command(*self.command_set_latency(latency))

        return response

    async def set_gain_level(self, channel_number, gain_level, device_type):
        response = await self.dante_command(
            *self.command_set_gain_level(channel_number, gain_level, device_type)
        )

        return response

    async def set_encoding(self, encoding):
        response = await self.dante_command(*self.command_set_encoding(encoding))

        return response

    async def set_sample_rate(self, sample_rate):
        response = await self.dante_command(*self.command_set_sample_rate(sample_rate))

        return response

    async def add_subscription(self, rx_channel, tx_channel, tx_device):
        response = await self.dante_command(
            *self.command_add_subscription(
                rx_channel.number, tx_channel.name, tx_device.name
            )
        )

        return response

    async def remove_subscription(self, rx_channel):
        response = await self.dante_command(
            *self.command_remove_subscription(rx_channel.number)
        )

        return response

    async def reset_channel_name(self, channel_type, channel_number):
        response = await self.dante_command(
            *self.command_reset_channel_name(channel_type, channel_number)
        )

        return response

    async def set_name(self, name):
        response = await self.dante_command(*self.command_set_name(name))

        return response

    async def reset_name(self):
        response = await self.dante_command(*self.command_reset_name())

        return response

    def get_service(self, service_type):
        service = None

        try:
            service = next(
                filter(
                    lambda x: x
                    and x[1]
                    and "type" in x[1]
                    and x[1]["type"] == service_type,
                    self.services.items(),
                )
            )[1]
        except Exception as e:
            logger.warning(f"Failed to get a service by type. {e}")
            self.error = e

        return service

    #  @on("init")
    #  def event_handler(self, *args, **kwargs):
    #      task_name = kwargs["task_name"]
    #      self.tasks.remove(task_name)

    #      if len(self.tasks) == 0:
    #          self.initialized = True
    #          ee.emit("init_check")

    #  @on("dante_model_info")
    #  def event_handler(self, *args, **kwargs):
    #      model = kwargs["model"]
    #      model_id = kwargs["model_id"]

    #      self.dante_model = model
    #      self.dante_model_id = model_id
    #      #  self.event_emitter.emit('init', task_name=TASK_GET_DANTE_MODEL_INFO)

    #  @on("parse_dante_model_info")
    #  def event_handler(self, *args, **kwargs):
    #      addr = kwargs["addr"]
    #      data = kwargs["data"]
    #      mac = kwargs["mac"]

    #      ipv4 = addr[0]

    #      model = data[88:].partition(b"\x00")[0].decode("utf-8")
    #      model_id = data[43:].partition(b"\x00")[0].decode("utf-8").replace("\u0003", "")

    #      self.event_emitter.emit(
    #          "dante_model_info", model_id=model_id, model=model, ipv4=ipv4, mac=mac
    #      )

    #  @on("device_make_model_info")
    #  def event_handler(self, *args, **kwargs):
    #      manufacturer = kwargs["manufacturer"]
    #      model = kwargs["model"]

    #      self.manufacturer = manufacturer
    #      self.model = model
    #      #  self.event_emitter.emit('init', task_name=TASK_GET_MODEL_INFO)

    #  @on("parse_device_make_model_info")
    #  def event_handler(self, *args, **kwargs):
    #      addr = kwargs["addr"]
    #      data = kwargs["data"]
    #      mac = kwargs["mac"]

    #      ipv4 = addr[0]

    #      manufacturer = data[76:].partition(b"\x00")[0].decode("utf-8")
    #      model = data[204:].partition(b"\x00")[0].decode("utf-8")

    #      self.event_emitter.emit(
    #          "device_make_model_info",
    #          manufacturer=manufacturer,
    #          model=model,
    #          ipv4=ipv4,
    #          mac=mac,
    #      )

    async def get_controls(self):
        try:
            for _, service in self.services.items():
                if service["type"] == SERVICE_CHAN:
                    continue

                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind(("", 0))
                sock.settimeout(1)
                sock.connect((str(self.ipv4), service["port"]))
                self.sockets[service["port"]] = sock

            for port in PORTS:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind(("", 0))
                sock.settimeout(0.01)
                sock.connect((str(self.ipv4), port))
                self.sockets[port] = sock
        except Exception as e:
            self.error = e
            print(e)
            traceback.print_exc()

        try:
            if not self.name:
                response = await self.dante_command(*self.command_device_name())

                if response:
                    self.name = response[10:-1].decode("ascii")
                else:
                    logger.warning("Failed to get Dante device name")

            # get reported rx/tx channel counts
            if not self.rx_count or not self.tx_count:
                channel_count = await self.dante_command(*self.command_channel_count())
                if channel_count:
                    self.rx_count_raw = self.rx_count = int.from_bytes(
                        channel_count[15:16], "big"
                    )
                    self.tx_count_raw = self.tx_count = int.from_bytes(
                        channel_count[13:14], "big"
                    )
                else:
                    logger.warning("Failed to get Dante channel counts")

            # get tx channels
            if not self.tx_channels and self.tx_count:
                await self.get_tx_channels()

            # get rx channels
            if not self.rx_channels and self.rx_count:
                await self.get_rx_channels()

            self.error = None
        except Exception as e:
            self.error = e
            print(e)
            traceback.print_exc()

    def parse_volume(self, bytes_volume):
        rx_channels = bytes_volume[-1 - self.rx_count_raw : -1]
        tx_channels = bytes_volume[
            -1 - self.rx_count_raw - self.tx_count_raw : -1 - self.rx_count_raw
        ]

        try:
            for _, channel in self.tx_channels.items():
                channel.volume = tx_channels[channel.number - 1]

            for _, channel in self.rx_channels.items():
                channel.volume = rx_channels[channel.number - 1]

        except Exception as e:
            print(e)
            traceback.print_exc()

    async def get_volume(self, ipv4, mac, port):
        try:
            if self.software or (self.model_id in FEATURE_VOLUME_UNSUPPORTED):
                return

            if port in sockets:
                sock = sockets[port]
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(0.1)
                sock.bind((str(ipv4), port))
                sockets[port] = sock

            volume_start = await self.dante_command(
                *self.command_volume_start(self.name, ipv4, mac, port)
            )

            if volume_start[15:16] == b"\xff":
                logger.debug(f"Volume level command is unsupported on {self.name}")

                return

            while True:
                try:
                    data, addr = sock.recvfrom(2048)

                    if addr[0] == str(self.ipv4):
                        await self.dante_send_command(
                            *self.command_volume_stop(self.name, ipv4, mac, port)
                        )
                        self.parse_volume(data)

                    break
                except socket.timeout:
                    break
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    break

        except Exception as e:
            traceback.print_exc()
            print(e)

    async def get_rx_channels(self):
        rx_channels = {}
        subscriptions = []

        try:
            for page in range(0, max(int(self.rx_count / 16), 1)):
                receivers = await self.dante_command(*self.command_receivers(page))
                hex_rx_response = receivers.hex()

                for index in range(0, min(self.rx_count, 16)):
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

                        rx_channel_name = self.get_label(
                            hex_rx_response, rx_channel_offset
                        )

                        tx_device_name = self.get_label(hex_rx_response, device_offset)

                        if not channel_offset == "0000":
                            tx_channel_name = self.get_label(
                                hex_rx_response, channel_offset
                            )
                        else:
                            tx_channel_name = rx_channel_name

                        if index == 0 and not device_offset == "0000":
                            o1 = (int(channel[2], 16) * 2) + 2
                            o2 = o1 + 6
                            sample_rate = int(hex_rx_response[o1:o2], 16)

                            if sample_rate:
                                self.sample_rate = sample_rate

                        channel_status_text = None

                        subscription = DanteSubscription()
                        rx_channel = DanteChannel()

                        rx_channel.channel_type = "rx"
                        rx_channel.device = self
                        rx_channel.name = rx_channel_name
                        rx_channel.number = channel_number
                        rx_channel.status_code = rx_channel_status_code

                        if channel_status_text:
                            rx_channel.status_text = channel_status_text

                        rx_channels[channel_number] = rx_channel

                        if tx_device_name:
                            subscription.rx_channel_name = rx_channel_name
                            subscription.rx_device_name = self.name
                            subscription.tx_channel_name = tx_channel_name
                            subscription.status_code = subscription_status_code

                            if tx_device_name == ".":
                                subscription.tx_device_name = self.name
                            else:
                                subscription.tx_device_name = tx_device_name

                            subscriptions.append(subscription)
        except Exception as e:
            self.error = e
            print(e)
            traceback.print_exc()

        self.rx_channels = rx_channels
        self.subscriptions = subscriptions

    async def get_tx_channels(self):
        tx_channels = {}
        tx_friendly_channel_names = {}

        try:
            for page in range(0, max(1, int(self.tx_count / 16)), 2):
                response = await self.dante_command(
                    *self.command_transmitters(page, friendly_names=True)
                )
                tx_friendly_names = response.hex()

                for index in range(0, min(self.tx_count, 32)):
                    str1 = tx_friendly_names[(24 + (index * 12)) : (36 + (index * 12))]
                    n = 4
                    channel = [str1[i : i + 4] for i in range(0, len(str1), n)]
                    #  channel_index = int(channel[0], 16)
                    channel_number = int(channel[1], 16)
                    channel_offset = channel[2]
                    tx_channel_friendly_name = self.get_label(
                        tx_friendly_names, channel_offset
                    )

                    if tx_channel_friendly_name:
                        tx_friendly_channel_names[
                            channel_number
                        ] = tx_channel_friendly_name

            for page in range(0, max(1, int(self.tx_count / 16)), 2):
                response = await self.dante_command(
                    *self.command_transmitters(page, friendly_names=False)
                )
                transmitters = response.hex()

                has_disabled_channels = False

                # TODO: Find the sample rate in the response instead of relying on it being already set from elsewhere
                if self.sample_rate:
                    has_disabled_channels = (
                        transmitters.count(f"{self.sample_rate:06x}") == 2
                    )

                first_channel = []

                for index in range(0, min(self.tx_count, 32)):
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
                            self.sample_rate = int(sample_rate_hex, 16)

                        channel_number = int(channel[0], 16)
                        #  channel_status = channel[1][2:]
                        channel_group = channel[2]
                        channel_offset = channel[3]

                        #  channel_enabled = channel_group == first_channel[2]
                        channel_disabled = channel_group != first_channel[2]

                        if channel_disabled:
                            break

                        tx_channel_name = self.get_label(transmitters, channel_offset)

                        tx_channel = DanteChannel()
                        tx_channel.channel_type = "tx"
                        tx_channel.number = channel_number
                        tx_channel.device = self
                        tx_channel.name = tx_channel_name

                        if channel_number in tx_friendly_channel_names:
                            tx_channel.friendly_name = tx_friendly_channel_names[
                                channel_number
                            ]

                        tx_channels[channel_number] = tx_channel

                if has_disabled_channels:
                    break

        except Exception as e:
            self.error = e
            print(e)
            traceback.print_exc()

        self.tx_channels = tx_channels

    @property
    def ipv4(self):
        return self._ipv4

    @ipv4.setter
    def ipv4(self, ipv4):
        self._ipv4 = ipaddress.ip_address(ipv4)

    @property
    def dante_model(self):
        return self._dante_model

    @dante_model.setter
    def dante_model(self, dante_model):
        self._dante_model = dante_model

    @property
    def dante_model_id(self):
        return self._dante_model_id

    @dante_model_id.setter
    def dante_model_id(self, dante_model_id):
        self._dante_model_id = dante_model_id

    @property
    def model(self):
        return self._model

    @model.setter
    def model(self, model):
        self._model = model

    @property
    def model_id(self):
        return self._model_id

    @model_id.setter
    def model_id(self, model_id):
        self._model_id = model_id

    @property
    def latency(self):
        return self._latency

    @latency.setter
    def latency(self, latency):
        self._latency = latency

    @property
    def mac_address(self):
        return self._mac_address

    @mac_address.setter
    def mac_address(self, mac_address):
        self._mac_address = mac_address

    @property
    def manufacturer(self):
        return self._manufacturer

    @manufacturer.setter
    def manufacturer(self, manufacturer):
        self._manufacturer = manufacturer

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, error):
        self._error = error

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def sample_rate(self):
        return self._sample_rate

    @sample_rate.setter
    def sample_rate(self, sample_rate):
        self._sample_rate = sample_rate

    @property
    def server_name(self):
        return self._server_name

    @server_name.setter
    def server_name(self, server_name):
        self._server_name = server_name

    @property
    def sockets(self):
        return self._sockets

    @sockets.setter
    def sockets(self, _sockets):
        self._sockets = _sockets

    @property
    def software(self):
        return self._software

    @software.setter
    def software(self, software):
        self._software = software

    @property
    def rx_channels(self):
        return self._rx_channels

    @rx_channels.setter
    def rx_channels(self, rx_channels):
        self._rx_channels = rx_channels

    @property
    def services(self):
        return self._services

    @services.setter
    def services(self, services):
        self._services = services

    @property
    def tx_channels(self):
        return self._tx_channels

    @tx_channels.setter
    def tx_channels(self, tx_channels):
        self._tx_channels = tx_channels

    @property
    def subscriptions(self):
        return self._subscriptions

    @subscriptions.setter
    def subscriptions(self, subscriptions):
        self._subscriptions = subscriptions

    @property
    def tx_count(self):
        return self._tx_count

    @tx_count.setter
    def tx_count(self, tx_count):
        self._tx_count = tx_count

    @property
    def rx_count(self):
        return self._rx_count

    @rx_count.setter
    def rx_count(self, rx_count):
        self._rx_count = rx_count

    @property
    def tx_count_raw(self):
        return self._tx_count_raw

    @tx_count_raw.setter
    def tx_count_raw(self, tx_count_raw):
        self._tx_count_raw = tx_count_raw

    @property
    def rx_count_raw(self):
        return self._rx_count_raw

    @rx_count_raw.setter
    def rx_count_raw(self, rx_count_raw):
        self._rx_count_raw = rx_count_raw

    def to_json(self):
        rx_channels = dict(sorted(self.rx_channels.items(), key=lambda x: x[1].number))
        tx_channels = dict(sorted(self.tx_channels.items(), key=lambda x: x[1].number))

        as_json = {
            "channels": {"receivers": rx_channels, "transmitters": tx_channels},
            "ipv4": str(self.ipv4),
            "name": self.name,
            "server_name": self.server_name,
            "services": self.services,
            "subscriptions": self.subscriptions,
        }

        if self.sample_rate:
            as_json["sample_rate"] = self.sample_rate

        if self.latency:
            as_json["latency"] = self.latency

        if self.manufacturer:
            as_json["manufacturer"] = self.manufacturer

        if self.dante_model:
            as_json["dante_model"] = self.dante_model

        if self.dante_model_id:
            as_json["dante_model_id"] = self.dante_model_id

        if self.model:
            as_json["model"] = self.model

        if self.model_id:
            as_json["model_id"] = self.model_id

        if self.mac_address:
            as_json["mac_address"] = self.mac_address

        return {key: as_json[key] for key in sorted(as_json.keys())}

    def get_label(self, hex_str, offset):
        parsed_get_label = None

        try:
            hex_substring = hex_str[int(offset, 16) * 2 :]
            partitioned_bytes = bytes.fromhex(hex_substring).partition(b"\x00")[0]
            parsed_get_label = partitioned_bytes.decode("utf-8")
        except Exception:
            pass
            #  traceback.print_exc()

        return parsed_get_label

    def command_string(
        self,
        command=None,
        command_str=None,
        command_args="0000",
        command_length="00",
        sequence1="ff",
        sequence2=0,
    ):
        if command == "channel_count":
            command_length = "0a"
            command_str = "1000"
        if command == "device_info":
            command_length = "0a"
            command_str = "1003"
        if command == "device_name":
            command_length = "0a"
            command_str = "1002"
        if command == "rx_channels":
            command_length = "10"
            command_str = "3000"
        if command == "reset_name":
            command_length = "0a"
            command_str = "1001"
            command_args = "0000"
        if command == "set_name":
            command_str = "1001"

        sequence2 = random.randint(0, 65535)
        sequence_id = f"{sequence2:04x}"

        command_hex = (
            f"27{sequence1}00{command_length}{sequence_id}{command_str}{command_args}"
        )

        if command == "add_subscription":
            command_length = f"{int(len(command_hex) / 2):02x}"
            command_hex = f"27{sequence1}00{command_length}{sequence_id}{command_str}{command_args}"

        return command_hex

    def get_name_lengths(self, device_name):
        name_len = len(device_name)
        offset = (name_len & 1) - 2
        padding = 10 - (name_len + offset)
        name_len1 = (len(device_name) * 2) + padding
        name_len2 = name_len1 + 2
        name_len3 = name_len2 + 4

        return (name_len1, name_len2, name_len3)

    def command_make_model(self, mac):
        cmd_args = "00c100000000"
        command_string = f"ffff00200fdb0000{mac}0000417564696e6174650731{cmd_args}"

        return command_string

    def command_dante_model(self, mac):
        cmd_args = "006100000000"
        command_string = f"ffff00200fdb0000{mac}0000417564696e6174650731{cmd_args}"

        return command_string

    def command_volume_start(self, device_name, ipv4, mac, port, timeout=True):
        data_len = 0
        device_name_hex = device_name.encode().hex()
        ip_hex = ipv4.packed.hex()

        name_len1, name_len2, name_len3 = self.get_name_lengths(device_name)

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

    def command_volume_stop(self, device_name, ipv4, mac, port):
        data_len = 0
        device_name_hex = device_name.encode().hex()
        ip_hex = ipaddress.IPv4Address(0).packed.hex()

        name_len1, name_len2, name_len3 = self.get_name_lengths(device_name)

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

    def command_set_latency(self, latency):
        command_str = "1101"
        command_length = "28"
        latency = int(latency * 1000000)
        latency_hex = f"{latency:06x}"

        command_args = f"00000503820500200211001083010024821983018302830600{latency_hex}00{latency_hex}"

        return (
            self.command_string(
                "set_latency",
                command_length=command_length,
                command_str=command_str,
                command_args=command_args,
            ),
            SERVICE_ARC,
        )

    def command_identify(self):
        mac = "000000000000"
        data_len = 32

        command_string = (
            f"ffff00{data_len:02x}0bc80000{mac}0000417564696e6174650731006300000064"
        )

        return (command_string, None, DEVICE_SETTINGS_PORT)

    def command_set_encoding(self, encoding):
        data_len = 40

        command_string = f"ffff00{data_len}03d700005254000000000000417564696e617465072700830000006400000001000000{encoding:02x}"

        return (command_string, None, DEVICE_SETTINGS_PORT)

    def command_set_gain_level(self, channel_number, gain_level, device_type):
        data_len = 52

        if device_type == "input":
            target = f"ffff00{data_len:02x}034400005254000000000000417564696e6174650727100a0000000000010001000c001001020000000000"
        elif device_type == "output":
            target = f"ffff00{data_len:02x}032600005254000000000000417564696e6174650727100a0000000000010001000c001002010000000000"

        command_string = f"{target}{channel_number:02x}000000{gain_level:02x}"

        return (command_string, None, DEVICE_SETTINGS_PORT)

    def command_set_sample_rate(self, sample_rate):
        data_len = 40

        command_string = f"ffff00{data_len:02x}03d400005254000000000000417564696e61746507270081000000640000000100{sample_rate:06x}"

        return (command_string, None, DEVICE_SETTINGS_PORT)

    def command_add_subscription(
        self, rx_channel_number, tx_channel_name, tx_device_name
    ):
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

    def command_remove_subscription(self, rx_channel):
        rx_channel_hex = f"{int(rx_channel):02x}"
        command_str = "3014"
        args_length = "10"
        command_args = f"00000001000000{rx_channel_hex}"

        return (
            self.command_string(
                "remove_subscription",
                command_str=command_str,
                command_length=args_length,
                command_args=command_args,
            ),
            SERVICE_ARC,
        )

    def command_device_info(self):
        return (self.command_string("device_info"), SERVICE_ARC)

    def command_device_name(self):
        return (self.command_string("device_name"), SERVICE_ARC)

    def command_channel_count(self):
        return (self.command_string("channel_count"), SERVICE_ARC)

    def command_set_name(self, name):
        args_length = chr(len(name.encode("utf-8")) + 11)
        args_length = bytes(args_length.encode("utf-8")).hex()

        return (
            self.command_string(
                "set_name",
                command_length=args_length,
                command_args=self.device_name(name),
            ),
            SERVICE_ARC,
        )

    def command_reset_name(self):
        return (self.command_string("reset_name"), SERVICE_ARC)

    def command_reset_channel_name(self, channel_type, channel_number):
        channel_hex = f"{channel_number:02x}"

        if channel_type == "rx":
            args_length = f"{int(21):02x}"
            command_args = f"0000020100{channel_hex}00140000000000"
            command_str = "3001"
        if channel_type == "tx":
            args_length = f"{int(25):02x}"
            command_args = f"00000201000000{channel_hex}001800000000000000"
            command_str = "2013"

        return (
            self.command_string(
                "reset_channel_name",
                command_str=command_str,
                command_args=command_args,
                command_length=args_length,
            ),
            SERVICE_ARC,
        )

    def command_set_channel_name(self, channel_type, channel_number, new_channel_name):
        name_hex = new_channel_name.encode().hex()
        channel_hex = f"{channel_number:02x}"

        if channel_type == "rx":
            command_str = "3001"
            command_args = f"0000020100{channel_hex}001400000000{name_hex}00"
            args_length = chr(len(new_channel_name.encode("utf-8")) + 21)
        if channel_type == "tx":
            command_str = "2013"
            command_args = f"00000201000000{channel_hex}0018000000000000{name_hex}00"
            args_length = chr(len(new_channel_name.encode("utf-8")) + 25)

        args_length = bytes(args_length.encode("utf-8")).hex()

        return (
            self.command_string(
                "set_channel_name",
                command_str=command_str,
                command_length=args_length,
                command_args=command_args,
            ),
            SERVICE_ARC,
        )

    def device_name(self, name):
        name_hex = name.encode().hex()
        return f"0000{name_hex}00"

    def channel_pagination(self, page):
        page_hex = format(page, "x")
        command_args = f"0000000100{page_hex}10000"

        return command_args

    def command_receivers(self, page=0):
        return (
            self.command_string(
                "rx_channels", command_args=self.channel_pagination(page)
            ),
            SERVICE_ARC,
        )

    def command_transmitters(self, page=0, friendly_names=False):
        if friendly_names:
            command_str = "2010"
        else:
            command_str = "2000"

        command_length = "10"
        command_args = self.channel_pagination(page=page)

        return (
            self.command_string(
                "tx_channels",
                command_length=command_length,
                command_str=command_str,
                command_args=command_args,
            ),
            SERVICE_ARC,
        )
