# TODO: Many lines below access "protected" members of classes, and shouldn't. Find a way to resolve this.
# pylint: disable=protected-access

from __future__ import annotations
import math
from typing import TypeAlias, TYPE_CHECKING

from .channel import DanteChannelType, DanteRxChannel, DanteTxChannel
from .subscription import DanteSubscription, DanteSubscriptionStatus
from .util import (
    decode_integer,
    decode_string,
    encode_integer,
    encode_string,
    NULL_HEXTET,
)

if TYPE_CHECKING:
    from .application import DanteApplication
    from .arc_service import DanteARCServiceDescriptor
    from .cmc_service import DanteCMCServiceDescriptor
    from .dbc_service import DanteDBCServiceDescriptor
    from .util import (
        ProtocolVersion,
    )

# Py312 has new syntax for this
ChannelCounts: TypeAlias = dict[DanteChannelType.RX: int, DanteChannelType.TX: int]
ChannelContainer: TypeAlias = dict[DanteChannelType.RX: list[DanteRxChannel], DanteChannelType.TX: list[DanteTxChannel]]


class DanteDevice:

    def __init__(self, application: DanteApplication, service_descriptors: dict = {}):
        self._app: DanteApplication = application

        self._service_descriptors = service_descriptors

        self._name: str = ''
        self._sample_rate: int = 0

        self._channel_counts: ChannelCounts = {DanteChannelType.RX: 0, DanteChannelType.TX: 0}
        self._channels: ChannelContainer = {DanteChannelType.RX: [], DanteChannelType.TX: []}

        self.request_name()
        self.request_all_channels()

    @property
    def arc(self) -> DanteARCServiceDescriptor:
        return self._service_descriptors['arc']

    @property
    def cmc(self) -> DanteCMCServiceDescriptor:
        return self._service_descriptors['cmc']

    @property
    def dbc(self) -> DanteDBCServiceDescriptor:
        return self._service_descriptors['dbc']

    @property
    def ipv4(self):
        return self._service_descriptors['ipv4']

    @property
    def name(self):
        return self._name

    @property
    def rx_channels(self):
        return self._channels[DanteChannelType.RX]

    @property
    def tx_channels(self):
        return self._channels[DanteChannelType.TX]

    # ~ @property
    # ~ def settings(self):
        # ~ return self._settings

    def get_channel_by_name(self, channel_type: DanteChannelType, channel_name: str) -> DanteRxChannel | DanteTxChannel | None:
        # Names are unique on the device, but case-insensitive
        channel_name = channel_name.lower()
        try:
            return next(
                filter(
                    lambda chan: chan.name.lower() == channel_name,
                    self._channels[channel_type]
                )
            )
        except StopIteration:
            return None

    def get_channel_by_number(self, channel_type: DanteChannelType, channel_number: int) -> DanteRxChannel | DanteTxChannel | None:
        try:
            return next(
                filter(
                    lambda chan: chan.number == channel_number,
                    self._channels[channel_type]
                )
            )
        except StopIteration:
            return None

    def json(self):
        return {
            "name": self._name,
            "ipv4": str(self.ipv4),
            "channel_count": [
                self._channel_counts[DanteChannelType.RX],
                self._channel_counts[DanteChannelType.TX],
            ],
            "arc_version": '.'.join([str(x) for x in self.arc.protocol_version]),
            "cmc_version": '.'.join([str(x) for x in self.cmc.protocol_version]),
            "sample_rate": self._sample_rate,
            # ~ "rx_channels": self.rx_channels,
            # ~ "tx_channels": self.tx_channels,
        }

    def request_all_channels(self) -> None:
        self._app.arc_service.command(self, b'\x10\x00', (), callback=self.__cb_request_all_channels)

    def __cb_request_all_channels(self, response: bytes) -> None:
        self._channel_counts = {
            DanteChannelType.RX: decode_integer(response, 14),
            DanteChannelType.TX: decode_integer(response, 12),
        }
        self.request_tx_channels()
        self.request_rx_channels()

    def request_device_info(self) -> None:
        self._app.arc_service.command(self, b'\x10\x03', (), callback=self.__cb_request_device_info)

    def __cb_request_device_info(self, response: bytes) -> None:
        self._name = decode_string(response, decode_integer(response, 22)) # or 26
        # ~ model = decode_string(response, decode_integer(response, 24))
        # ~ manufacturer = decode_string(response, decode_integer(response, 16))
        # ~ debug_string = decode_string(response, decode_integer(response, 18))

    def request_name(self) -> None:
        self._app.arc_service.command(self, b'\x10\x02', (), callback=self.__cb_request_name)

    def __cb_request_name(self, response: bytes) -> None:
        self._name = decode_string(response, 10)

    def request_rx_channels(self) -> None:
        protocol_version = self.arc.protocol_version
        rx_count = self._channel_counts[DanteChannelType.RX]
        for page in range(math.ceil(rx_count / self._app.arc_service.MAX_CHANNELS_PER_PAGE)):
            if protocol_version >= (2, 8, 2):
                code = b'\x34\x00'
                # TODO: One of the \x00\x01 hextets is the page num. Determine which one.
                body = (
                    NULL_HEXTET * 3,
                    b'\x00\x01',
                    b'\x00\x01',
                    b'\x00\x01',
                    NULL_HEXTET * 6,
                )
            else:
                code = b'\x30\x00'
                body = (
                    b'\x00\x01',
                    encode_integer((page << 4) + 1),
                    NULL_HEXTET,
                )

            self._app.arc_service.command(self, code, body, callback=self.__cb_request_rx_channels)

    def __cb_request_rx_channels(self, response: bytes) -> None:
        protocol_version = self.arc.protocol_version
        rx_count = self._channel_counts[DanteChannelType.RX]

        # TODO: get page from response
        page = 1

        # Properties common to all channels on this device
        # (Location of this is specified inside each specific channel definition)
        common_definition = None

        for index in range(self._app.arc_service.channels_on_page(page, rx_count)):
            if protocol_version >= (2, 8, 2):
                def_start_ptr = 18
                definition_length = 56

                definition_start = decode_integer(response, def_start_ptr + 2 * index)
                definition_end = definition_start + definition_length
                channel_definition = response[definition_start:definition_end]

                common_definition_ptr = 22

                rx_channel_name = decode_string(response, decode_integer(channel_definition, 20))
                rx_channel_number = decode_integer(channel_definition, 2)
                rx_channel_status = DanteSubscriptionStatus.derive(
                    decode_integer(channel_definition, 50)
                )

                tx_channel_name = decode_string(response, decode_integer(channel_definition, 44))
                tx_device_name = decode_string(response, decode_integer(channel_definition, 46))

                subscription_status = DanteSubscriptionStatus.derive(
                    decode_integer(channel_definition, 48)
                )

            else:
                def_start = 12
                definition_length = 16

                definition_start = def_start + 20 * index
                definition_end = definition_start + definition_length
                channel_definition = response[definition_start:definition_end]

                common_definition_ptr = 4

                rx_channel_name = decode_string(response, decode_integer(channel_definition, 10))
                rx_channel_number = decode_integer(channel_definition, 0)
                rx_channel_status = DanteSubscriptionStatus.derive(
                    decode_integer(channel_definition, 12)
                )

                tx_channel_name = decode_string(response, decode_integer(channel_definition, 6))
                tx_device_name = decode_string(response, decode_integer(channel_definition, 8))

                subscription_status = DanteSubscriptionStatus.derive(
                    decode_integer(channel_definition, 14)
                )

            if not common_definition:
                definition_start = decode_integer(channel_definition, common_definition_ptr)
                definition_end = definition_start + 16
                common_definition = response[definition_start:definition_end]

            rx_channel = self.get_channel_by_number(DanteChannelType.RX, rx_channel_number)
            if not rx_channel:
                rx_channel = DanteRxChannel(
                    application = self._app,
                    device = self,
                    number = rx_channel_number,
                    name = rx_channel_name,
                    status = rx_channel_status,
                )
                self._channels[DanteChannelType.RX].append(rx_channel)
                subscription = None
            else:
                # TODO: internal access
                rx_channel._name = rx_channel_name
                rx_channel._status = rx_channel_status
                subscription = rx_channel.subscription

            if not tx_device_name:
                tx_channel = None
            else:
                if tx_device_name == '.':
                    tx_device = self
                else:
                    tx_device = self._app.get_device_by_name(tx_device_name)

                if tx_device:
                    tx_channel = tx_device.get_channel_by_name(DanteChannelType.TX, tx_channel_name)
                else:
                    tx_channel = self._app.retrieve_orphaned_tx_channel(tx_device_name, tx_channel_name)

                if not tx_channel:
                    tx_channel =  DanteTxChannel(
                        application = self._app,
                        device = tx_device or tx_device_name,
                        number = -1, # Not contained within response
                        name = tx_channel_name,
                    )
                    if tx_device:
                        tx_device._channels[DanteChannelType.TX].append(tx_channel)
                    else:
                        self._app.append_orphaned_tx_channel(tx_device_name, tx_channel)

            if not subscription:
                subscription = DanteSubscription(
                    rx_channel=rx_channel,
                    tx_channel=tx_channel,
                    status=subscription_status,
                )
                rx_channel._subscription = subscription # TODO: internal access
                if tx_channel:
                    tx_channel._subscriptions.append(subscription)  # TODO: internal access
            else:
                if subscription.tx_channel and subscription.tx_channel != tx_channel:
                    subscription.tx_channel._subscriptions.remove(subscription) # TODO: internal access
                    subscription._tx_channel = tx_channel # TODO: internal access
                elif tx_channel:
                    subscription._tx_channel = tx_channel # TODO: internal access
                else:
                    subscription._tx_channel = None # TODO: internal access

                if tx_channel:
                    tx_channel._subscriptions.append(subscription)  # TODO: internal access

                subscription._status = subscription_status # TODO: internal access

        if not self._sample_rate:
            self._sample_rate = decode_integer(common_definition, 0, 4)

    def request_tx_channels(self, friendly_names: bool = False) -> None:
        protocol_version = self.arc.protocol_version
        tx_count = self._channel_counts[DanteChannelType.TX]
        callback = self.__cb_request_tx_channels

        for page in range(math.ceil(tx_count / self._app.arc_service.MAX_CHANNELS_PER_PAGE)):
            if protocol_version >= (2, 8, 2):
                code = b'\x24\x00'
                # TODO: One of the \x00\x01 hextets is the page num. Determine which one.
                body = (
                    NULL_HEXTET * 3,
                    b'\x00\x01',
                    b'\x00\x01',
                    b'\x00\x01',
                    NULL_HEXTET * 6,
                )
            else:
                if friendly_names:
                    callback = self.__cb_request_tx_channels_friendly
                    code = b'\x20\x10'
                else:
                    code = b'\x20\x00'
                body = (
                    b'\x00\x01',
                    encode_integer((page << 4) + 1),
                    NULL_HEXTET,
                )

            self._app.arc_service.command(self, code, body, callback=callback)

    def __cb_request_tx_channels(self, response: bytes) -> None:
        protocol_version = self.arc.protocol_version
        tx_count = self._channel_counts[DanteChannelType.TX]

        # TODO: Get page number from response
        page = 1

        # Properties common to all channels on this device
        # (Location of this is specified inside each specific channel definition)
        common_definition = None

        for index in range(self._app.arc_service.channels_on_page(page, tx_count)):

            if protocol_version >= (2, 8, 2):
                definitions_start_ptr = 18
                definition_length = 40

                definition_start = decode_integer(response, definitions_start_ptr + 2 * index)
                definition_end = definition_start + definition_length
                channel_definition = response[definition_start:definition_end]

                if not common_definition:
                    definition_start = decode_integer(channel_definition, 22)
                    definition_end = definition_start + 16
                    common_definition = response[definition_start:definition_end]

                channel_number = decode_integer(channel_definition, 2)
                channel_name_default = decode_string(response, decode_integer(channel_definition, 30))
                channel_name_friendly = decode_string(response, decode_integer(channel_definition, 20))

            else: ## protocol_version < (2, 8, 2)

                definitions_start = 12
                definition_length = 8

                definition_start = definitions_start + definition_length * index
                definition_end = definition_start + definition_length
                channel_definition = response[definition_start:definition_end]

                if not common_definition:
                    definition_start = decode_integer(channel_definition, 4)
                    definition_end = definition_start + 16
                    common_definition = response[definition_start:definition_end]

                channel_number = decode_integer(channel_definition, 0)
                channel_name_default = decode_string(response, decode_integer(channel_definition, 6))
                channel_name_friendly = None # Acquired elsewhere

            ## endif protocol_version

            channel = self.get_channel_by_number(DanteChannelType.TX, channel_number)
            if not channel:
                # If the channel was previously "orphaned", then the channel number won't be known
                channel = self.get_channel_by_name(DanteChannelType.TX, channel_name_friendly or channel_name_default)
                if channel:
                    channel._number = channel_number # TODO: internal access
                else:
                    # If still not found, the channel is not known
                    channel = DanteTxChannel(
                        application = self._app,
                        device = self,
                        number = channel_number,
                        name = channel_name_friendly or channel_name_default,
                    )
                self._channels[DanteChannelType.TX].append(channel)
            else:
                channel._name = channel_name_friendly or channel_name_default # TODO: internal access

        if not self._sample_rate:
            self._sample_rate = decode_integer(common_definition, 0, 4)

    def __cb_request_tx_channels_friendly(self, device: DanteDevice, response: bytes) -> None:
        protocol_version = self.arc.protocol_version
        tx_count = device.channel_counts[DanteChannelType.TX]
        if protocol_version >= (2, 8, 2):
            return

        # TODO: Get page number from response
        page = 1

        definitions_start = 12
        definition_length = 6
        for index in range(self._app.arc_service.channels_on_page(page, tx_count)):
            definition_start = definitions_start + definition_length * index
            definition_end = definition_start + definition_length
            channel_definition = response[definition_start:definition_end]

            channel = self.get_channel_by_number(
                DanteChannelType.TX,
                decode_integer(channel_definition, 2)
            )
            channel._name = decode_string(response, decode_integer(channel_definition, 4)) # TODO: internal access

    def reset_name(self) -> None:
        self.set_name('')

    def set_latency(self, latency: int) -> None:
        latency_encoded = encode_integer(latency * 1000000, 4)
        code = b'\x11\x01'
        # TODO: Work out what the other hextets signify
        body = (
            b'\x05\x03',
            b'\x82\x05',
            encode_integer(self._app.arc_service.SERVICE_HEADER_LENGTH + 22), # location of first `latency_encoded` below
            b'\x02\x11',
            b'\x00\x10',
            b'\x83\x01',
            encode_integer(self._app.arc_service.SERVICE_HEADER_LENGTH + 22 + 4), # location of second `latency_encoded` below
            b'\x82\x19',
            b'\x83\x01',
            b'\x83\x02',
            b'\x83\x06',
            latency_encoded,
            latency_encoded,
        )
        self._app.arc_service.command(self, code, body, callback=self.__cb_set_latency)

    def __cb_set_latency(self, response: bytes) -> None:
        print(response) # TODO: Process this

    def set_name(self, new_name: str) -> None:
        # TODO: validate new name:
        # * max. 31 chars
        # * chars: `a-zA-Z0-9` and literals `-`
        # * may not start or end with `-`
        # * unique on network
        code = b'\x10\x01'
        body = (
            encode_string(new_name),
        )
        self._app.arc_service.command(self, code, body, self.__cb_set_name)

    def __cb_set_name(self, response: bytes) -> None:
        # pylint: disable=unused-argument
        # New name is not contained within response, and may differ from what we wished to set,
        # particularly in the case of name reset.
        self.request_name()
