from __future__ import annotations
from enum import Enum
from typing import TYPE_CHECKING

from .util import (
    encode_integer,
    encode_string,
    decode_integer,
    decode_string,
    NULL_HEXTET,
)

if TYPE_CHECKING:
    from .application import DanteApplication
    from .device import DanteDevice
    from .subscription import DanteSubscription, DanteSubscriptionStatus


class DanteChannelType(Enum):
    RX = "rx"
    TX = "tx"


class _DanteChannel:

    TYPE: DanteChannelType

    def __init__(
        self,
        application: "DanteApplication",
        device: DanteDevice | str,
        number: int,
        name: str
    ):
        self._app = application

        self._device: DanteDevice | str = device
        self._name: str = name
        self._number: int = number
        self._volume: int | None = None

    @property
    def name(self):
        return self._name

    @property
    def number(self):
        return self._number

    @property
    def volume(self):
        return self._volume

    def json(self):
        return {
            "type": self.TYPE.value,
            "number": self._number,
            "name": self._name,
            # ~ "volume": self._volume,
        }

    def reset_name(self):
        self.set_name('')

    def set_name(self, new_name: str) -> None:
        raise NotImplementedError

    def _set_name(self, code: bytes, preamble: tuple[bytes], new_name: str) -> None:
        # pylint: disable=unused-private-member
        # (Is used in child classes)
        if not self._device or isinstance(self._device, str):
            return

        preamble = b''.join(preamble)
        body = (
            preamble,
            encode_integer(self._app.arc_service.SERVICE_HEADER_LENGTH + len(preamble) + 2),
            # packet traces had null hextets here, for padding(?): 23/30/2 for RX; 47/45/0 for TX (2.8.9, 2.8.1, 2.7.x)
            encode_string(new_name),
        )
        self._app.arc_service.command(self._device, code, body, callback=self.__cb_set_name)

    def __cb_set_name(self, response: bytes) -> None:
        protocol_version = self._device.arc.protocol_version
        if protocol_version < (2, 8, 2):
            # These versions don't return the new name in the response.
            # Is there a way to request the name of a single channel? Haven't observed a way yet.
            if self.TYPE == DanteChannelType.TX:
                self._device.request_tx_channels()
            else:
                self._device.request_rx_channels()
            return

        channel_number = decode_integer(response, 20)
        if channel_number == 0:
            # Setting failed
            return

        name_ptr = decode_integer(response, 24)
        if not name_ptr:
            # If name is reset, then no name will be returned
            if self.TYPE == DanteChannelType.TX:
                self._device.request_tx_channels()
            else:
                self._device.request_rx_channels()
            return

        self._name = decode_string(response, name_ptr)

    def _validate_name(self, name: str) -> str:
        # * max. 31 chars
        # * any printable ascii, except `=`, `@`, `.`
        # * unique (for channel type) on device
        new_name = []
        for idx in range(min(len(name), 31)):
            char = name[idx]
            if char.isascii() and char.isprintable() and char not in ['=', '@', '.']:
                new_name.append(char)
        name = ''.join(new_name)

        count = 2
        while self._device.get_channel_by_name(self.TYPE, name):
            suffix = f"~{count}"
            if (len(name) + len(suffix)) > 31:
                name = f"{name[:-len(suffix)]}{suffix}"
            else:
                name = f"{''.join(new_name)}{suffix}"
            count = count + 1
        return name


class DanteRxChannel(_DanteChannel):

    TYPE: DanteChannelType = DanteChannelType.RX

    def __init__(
        self,
        application: DanteApplication,
        device: DanteDevice,
        number: int,
        name: str,
        status: DanteSubscriptionStatus,
    ):
        super().__init__(application, device, number, name)
        self._status: DanteSubscriptionStatus = status
        self._subscription: DanteSubscription

    @property
    def device(self) -> DanteDevice:
        return self._device

    @property
    def status(self) -> DanteSubscriptionStatus:
        return self._status

    @property
    def subscription(self) -> DanteSubscription:
        return self._subscription

    def json(self):
        return {
            **super().json(),
            "status_code": self._status.value,
            "subscription": str(self._subscription.tx_channel) if self._subscription.tx_channel else None,
        }

    def __str__(self):
        return f"{self._name}@{self._device.name}"

    def set_name(self, new_name: str) -> None:
        if new_name == self._name:
            return
        protocol_version = self._device.arc.protocol_version
        new_name = self._validate_name(new_name)
        if protocol_version >= (2, 8, 2):
            code = b'\x34\x01'
            preamble = (
                NULL_HEXTET * 4,    # packet trace had 3 null hextets, then b'\x06\x00'
                b'\x00\x01',        # pkttrace='\x08\x01'; second byte seems to control how many times string appears in response
                encode_integer(self._number),
                b'\x00\x03',        # Must be equal to either `1` or `3` (packet trace had `3`)
            )

        else: # protocol_version == (2,8,1) or (2,7,*)
            code = b'\x30\x01'
            preamble = (
                b'\x00\x01',    # must be > 1; packet traces had b'\x10\x01' (2, 8, 1) and b'\x02\x01'
                encode_integer(self._number),
            )
        self._set_name(code, preamble, new_name)

    def subscribe(self, tx_channel: DanteTxChannel) -> None:
        if tx_channel == self._subscription.tx_channel:
            # Already subscribed to this channel
            return

        protocol_version = self._device.arc.protocol_version
        if protocol_version >= (2, 8, 2):
            padding = NULL_HEXTET * 2 # originally 28
            code = b'\x34\x10'
            preamble = (
                NULL_HEXTET * 3,
                b'\x08\x00',
                b'\x02\x01',
                encode_integer(self._number),
                b'\x00\x03',
            )

        else: # protocol_version == 2.8.1, 2.7.*
            padding = NULL_HEXTET * 3 # arc 2.7.* had 17 hextets; 2.8.1 had 157
            code = b'\x30\x10'
            preamble = (
                b'\x10\01' if protocol_version == (2, 8, 1) else b'\x02\x01', # does this value matter?
                encode_integer(self._number),
            )

        preamble = b''.join(preamble)
        string_idx = self._app.arc_service.SERVICE_HEADER_LENGTH + len(preamble) + 4 + len(padding)
        tx_channel_name_encoded = encode_string(tx_channel.name)
        body = (
            preamble,
            encode_integer(string_idx),
            encode_integer(string_idx + len(tx_channel_name_encoded)),
            padding,
            tx_channel_name_encoded,
            encode_string(tx_channel.device.name),
        )
        self._app.arc_service.command(self._device, code, body)
        # Response doesn't appear to contain anything of import, so request all RX channels again.
        self._device.request_rx_channels()

    def unsubscribe(self) -> None:
        protocol_version = self._device.arc.protocol_version
        if protocol_version >= (2, 8, 2):
            code = b'\x34\x10'
            body = (
                NULL_HEXTET * 3,
                b'\x08\x00',
                b'\x08\x01',
                encode_integer(self._number),
                b'\x00\x03',
                NULL_HEXTET * 2,   # packet traces had 60 null octets here
            )
        else:
            # either (taken from packet trace of protocol_version == (2, 8, 1))
            code = b'\x30\x10'
            body = (
                b'\x10\x01',
                encode_integer(self._number),
                # packet traces had 318 (!) null octets here
            )
            # or (taken from earlier work)
            # ~ code = b'\x30\x14'
            # ~ body = (
                # ~ b'\x00\x01',
                # ~ NULL_HEXTET,
                # ~ encode_integer(self._number),
            # ~ )

        self._app.arc_service.command(self._device, code, body)
        # Response doesn't appear to contain anything of import, so request all RX channels again.
        self._device.request_rx_channels()


class DanteTxChannel(_DanteChannel):

    TYPE: DanteChannelType = DanteChannelType.TX

    def __init__(
        self,
        application: DanteApplication,
        device: DanteDevice | str,
        number: int,
        name: str
    ):
        super().__init__(application, device, number, name)

        self._device: DanteDevice | str = device
        self._subscriptions: list[DanteSubscription] = []

    @property
    def device(self) -> DanteDevice | str:
        return self._device

    @property
    def subscriptions(self):
        return self._subscriptions

    def __str__(self):
        if isinstance(self._device, str):
            return f"{self._name}@{self._device}"
        return f"{self._name}@{self._device.name}"

    def json(self):
        return {
            **super().json(),
            "subscribing": [str(sub.rx_channel) for sub in self._subscriptions],
        }

    def set_name(self, new_name: str) -> None:
        if new_name == self._name:
            return
        protocol_version = self._device.arc.protocol_version
        new_name = self._validate_name(new_name)
        if protocol_version >= (2, 8, 2):
            code = b'\x24\x38'
            preamble = (
                NULL_HEXTET * 4,    # packet trace had 3 null hextets, then b'\x06\x00'
                b'\x00\x01',        # pkttrace='\x10\x01'; second byte seems to control how many times string appears in response
                encode_integer(self._number),
                b'\x00\x03',        # Must be equal to either `1` or `3` (packet trace had `3`)
            )

        else: # protocol_version == (2,8,1) or (2,7,*)
            code = b'\x20\x13'
            preamble = (
                b'\x00\x01',    # second byte must be == 1; packet traces had b'\x10\x01' (2, 8, 1) and b'\x02\x01'
                NULL_HEXTET,
                encode_integer(self._number),
            )
        self._set_name(code, preamble, new_name)
