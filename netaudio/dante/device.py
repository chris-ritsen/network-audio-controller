import codecs
import ipaddress
import logging
import pprint
import socket
import traceback

from netaudio.dante.channel_helper import DanteChannelHelper
from netaudio.dante.command import DanteCommand
from netaudio.dante.command_builder import DanteCommandBuilder
from netaudio.dante.socket_manager import DanteSocketManager

from netaudio.dante.const import (
    FEATURE_VOLUME_UNSUPPORTED,
)

logger = logging.getLogger("netaudio")

# used for the undocumented volume level reporting feature of some devices
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
        self._rx_count = None
        self._rx_count_raw = None
        self._sample_rate = None
        self._server_name = server_name
        self._services = {}
        self._sockets = DanteSocketManager(self)
        self._software = None
        self._subscriptions = []
        self._tx_channels = {}
        self._tx_count = None
        self._tx_count_raw = None
        self.channel_helper = DanteChannelHelper(self)
        self.command = DanteCommand(self)
        self.command_builder = DanteCommandBuilder(self)

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
        return self._sockets.sockets

    @sockets.setter
    def sockets(self, _sockets):
        self._sockets.sockets = _sockets

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

    def __str__(self):
        return f"{self.name}"

    async def _execute_command(self, command_func, *args):
        return await self.command.send(*command_func(*args))

    async def add_subscription(self, rx_channel, tx_channel, tx_device):
        return await self._execute_command(
            self.command_builder.add_subscription,
            rx_channel.number,
            tx_channel.name,
            tx_device.name,
        )

    async def enable_aes67(self, is_enabled: bool):
        return await self._execute_command(
            self.command_builder.enable_aes67, is_enabled
        )

    async def identify(self):
        return await self._execute_command(self.command_builder.identify_device)

    async def remove_subscription(self, rx_channel):
        return await self._execute_command(
            self.command_builder.remove_subscription, rx_channel.number
        )

    async def reset_channel_name(self, channel_type, channel_number):
        return await self._execute_command(
            self.command_builder.reset_channel_name, channel_type, channel_number
        )

    async def reset_name(self):
        return await self._execute_command(self.command_builder.reset_device_name)

    async def set_channel_name(self, channel_type, channel_number, new_channel_name):
        return await self._execute_command(
            self.command_builder.set_channel_name,
            channel_type,
            channel_number,
            new_channel_name,
        )

    async def set_encoding(self, encoding):
        return await self._execute_command(self.command_builder.set_encoding, encoding)

    async def set_gain_level(self, channel_number, gain_level, device_type):
        return await self._execute_command(
            self.command_builder.set_gain_level, channel_number, gain_level, device_type
        )

    async def set_latency(self, latency):
        return await self._execute_command(self.command_builder.set_latency, latency)

    async def set_name(self, name):
        return await self._execute_command(self.command_builder.set_device_name, name)

    async def set_sample_rate(self, sample_rate):
        return await self._execute_command(
            self.command_builder.set_sample_rate, sample_rate
        )

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
            self._sockets.create_service_sockets()
            self._sockets.create_port_sockets()
        except Exception as e:
            self.error = e
            print(e)
            traceback.print_exc()

        try:
            if not self.name:
                response = await self.command.send(
                    *self.command_builder.get_device_name()
                )

                if response:
                    self.name = response[10:-1].decode("ascii")
                else:
                    logger.warning("Failed to get Dante device name")

            # get reported rx/tx channel counts
            if self._rx_count is None or self._tx_count is None:
                channel_count = await self.command.send(
                    *self.command_builder.get_channel_count()
                )

                if channel_count:
                    self.rx_count_raw = self.rx_count = int.from_bytes(
                        channel_count[15:16], "big"
                    )
                    self.tx_count_raw = self.tx_count = int.from_bytes(
                        channel_count[13:14], "big"
                    )
                else:
                    logger.warning("Failed to get Dante channel counts")

            if not self.tx_channels and self.tx_count:
                await self.channel_helper.get_tx_channels()

            if not self.rx_channels and self.rx_count:
                await self.channel_helper.get_rx_channels()

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

            sock = self._sockets.get_or_create_socket(ipv4, port)

            volume_start = await self.command.send(
                *self.command_builder.volume_start(self.name, ipv4, mac, port)
            )

            if volume_start and volume_start[15:16] == b"\xff":
                logger.debug(f"Volume level command is unsupported on {self.name}")

                return

            while True:
                try:
                    data, addr = sock.recvfrom(2048)

                    if addr[0] == str(self.ipv4):
                        await self.command.send_command(
                            *self.command_builder.volume_stop(
                                self.name, ipv4, mac, port
                            ),
                            expect_response=False,
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
