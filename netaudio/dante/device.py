import logging
import socket

from ipaddress import IPv4Address
from typing import Any, Dict, List
from pydantic import BaseModel

from netaudio.dante.channel import DanteChannel, ChannelType
from netaudio.dante.subscription import DanteSubscription
from .protocols import ServiceARC, ServiceCMC, ServiceSettings, Encoding

logger = logging.getLogger("netaudio")
sockets:dict[int, socket.socket] = {}

class NetworkInfo(BaseModel):
    server_name: str
    name: str
    ipv4: IPv4Address
    port: int
    type: str

class DanteDevice(BaseModel):
    # Arguments (required)
    ipv4: IPv4Address
    hostname: str

    # Services
    _cmc:ServiceCMC = ServiceCMC()
    _arc:ServiceARC = ServiceARC()
    _ssc:ServiceSettings = ServiceSettings()

    # State
    error:Exception | None = None
    name:str = ""
    sample_rate:int = 0
    channel_count:tuple[int, int] = (0, 0)
    rx_channels:Dict[str, DanteChannel] = {}
    tx_channels:Dict[str, DanteChannel] = {}
    subscriptions:List[DanteSubscription] = []

    class Config:
        arbitrary_types_allowed = True

    # def __init__(self, ipv4: IPv4Address, hostname: str):
    #     self._dante_model: str = ""
    #     self._dante_model_id: str = ""
    #     self._error: str = None
    #     self._latency: float = None
    #     self._mac_address: str = None
    #     self._manufacturer: str = ""
    #     self._model: str = ""
    #     self._model_id: str = ""
    #     self._name: str = ""
    #     self._software: str = None


    def __str__(self):
        return f"DanteDevice <{self.name}> ({self.channel_count[0]}x{self.channel_count[1]})"

    async def begin(self):
        await self._cmc.begin(self.ipv4)
        await self._arc.begin(self.ipv4)
        await self._ssc.begin(self.ipv4)

        self.name = await self._arc.get_device_name()
        self.channel_count = await self._arc.get_channel_count()

        chSubscriptionsResult = await self._arc.get_channels_subscriptions(self.channel_count, self.name)
        self.rx_channels = chSubscriptionsResult[0]
        self.tx_channels = chSubscriptionsResult[1]
        self.subscriptions = chSubscriptionsResult[2]
        self.sample_rate = chSubscriptionsResult[3]

    #########################################################
    # COMMANDS
    #########################################################

    async def identify(self):
        return await self._ssc.command_identify()

    async def set_gain_level(self, channel_number:int, gain_level:int, device_type:str):
        return await self._ssc.set_gain_level(channel_number, gain_level, device_type)

    async def enable_aes67(self, is_enabled: bool):
        await self._ssc.set_aes67(is_enabled=is_enabled)

    async def set_encoding(self, encoding:Encoding):
        await self._ssc.set_encoding(encoding)

    async def set_sample_rate(self, sample_rate:int):
        await self._ssc.set_sample_rate(sample_rate)

    async def set_latency(self, latency:int):
        return await self._arc.set_latency(latency)

    async def add_subscription(self, rx_channel:DanteChannel, tx_channel:DanteChannel, tx_device:"DanteDevice"):
        '''
        This device will be the rx device
        '''
        return await self._arc.add_subscription(
            rx_channel.number, tx_channel.name, tx_device.name
        )

    async def remove_subscription(self, rx_channel:DanteChannel):
        return await self._arc.remove_subscription(rx_channel.number)

    async def set_channel_name(self, channel_type:ChannelType, channel_number:int, new_channel_name:str):
        return await self._arc.set_channel_name(
                channel_type, channel_number, new_channel_name
            )

    async def reset_channel_name(self, channel_type:ChannelType, channel_number:int):
        return await self._arc.reset_channel_name(channel_type, channel_number)

    async def set_name(self, name:str):
        return await self._arc.set_name(name)

    async def reset_name(self):
        return await self._arc.reset_name()


    #########################################################
    # GETTERS
    #########################################################

    # def to_json(self):
    #     rx_channels = dict(sorted(self.rx_channels.items(), key=lambda x: x[1].number))
    #     tx_channels = dict(sorted(self.tx_channels.items(), key=lambda x: x[1].number))

    #     as_json = {
    #         "channels": {"receivers": rx_channels, "transmitters": tx_channels},
    #         "ipv4": str(self.ipv4),
    #         "name": self.name,
    #         # "server_name": self.hostname,
    #         "services": self.services,
    #         "subscriptions": self.subscriptions,
    #     }

    #     if self.sample_rate:
    #         as_json["sample_rate"] = self.sample_rate

    #     if self.latency:
    #         as_json["latency"] = self.latency

    #     if self.manufacturer:
    #         as_json["manufacturer"] = self.manufacturer

    #     if self.dante_model:
    #         as_json["dante_model"] = self.dante_model

    #     if self.dante_model_id:
    #         as_json["dante_model_id"] = self.dante_model_id

    #     if self.model:
    #         as_json["model"] = self.model

    #     if self.model_id:
    #         as_json["model_id"] = self.model_id

    #     if self.mac_address:
    #         as_json["mac_address"] = self.mac_address

    #     return {key: as_json[key] for key in sorted(as_json.keys())}
