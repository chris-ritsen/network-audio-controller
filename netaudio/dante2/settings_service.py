# ~ from __future__ import annotations
# ~ from typing import TYPE_CHECKING

from .channel import DanteChannelType
from .service import DanteService
from .util import (
    NULL_HEXTET,
    # ~ decode_integer,
    # ~ decode_string,
    encode_integer,
    # ~ encode_string,
    encode_mac_address,
    get_mac_addr_serving_ipv4,
)

# ~ if TYPE_CHECKING:
from .device import DanteDevice
from .util import (
    Encoding,
    SampleRate,
)


class DanteSettingsServiceDescriptor:
    pass


class DanteSettingsService(DanteService):
    """
    Multicast Control and Monitoring
    """
    SERVICE_HEADER_LENGTH: int = 24
    SERVICE_PORT: int = 8700
    SERVICE_TYPE_MDNS: None = None
    SERVICE_TYPE_SHORT = 'settings'

    def command(
        self,
        device: DanteDevice,
        payload: tuple[bytes],
        mac_address: bytes,
        part1: bytes | None = None,
    ) -> None:
        ipv4 = device.ipv4
        message_idx = self._message_index.generate()

        command = b''.join((
            b'\xff\xff',
            NULL_HEXTET,                        # message length, calculated below
            encode_integer(message_idx),
            part1[0:2] if part1 else NULL_HEXTET,
            mac_address,
            NULL_HEXTET,                        # message type/direction?
            b'Audinate',                        # no null terminator
            *payload,
        ))
        command = command[:2] + encode_integer(len(command)) + command[4:]

        self._message_store[message_idx] = {
            'device': device,
            'command': command,
        }
        print(command)
        self._send_queue.put(((str(ipv4), self.SERVICE_PORT), command))

    def get_dante_model(
        self,
        device: DanteDevice,
    ):
        mac_address = get_mac_addr_serving_ipv4(device.ipv4)
        payload = (
            b'\x07\x31',
            b'\x00\x61',
            b'\x00\x00', # < null
            b'\x00\x00', # < null
        )
        self.command(device, payload, encode_mac_address(mac_address))

    def get_make_model(
        self,
        device: DanteDevice,
    ):
        mac_address = get_mac_addr_serving_ipv4(device.ipv4)
        payload = (
            b'\x07\x31',
            b'\x00\xc1',
            b'\x00\x00', # < null
            b'\x00\x00', # < null
        )
        self.command(device, payload, encode_mac_address(mac_address))

    def set_aes67(
        self,
        device: DanteDevice,
        is_enabled: bool,
    ):
        payload = (
            b'\x07\x34',
            b'\x10\x06',
            b'\x00\x00', # < null
            b'\x00\x64',
            b'\x00\x01',
            encode_integer(is_enabled),
        )
        pseudo_mac_address = b'\x52\x54\x00\x38\x5e\xba'
        self.command(device, payload, pseudo_mac_address, b'\x22\xdc')

    def set_encoding(
        self,
        device: DanteDevice,
        encoding: Encoding,
    ):
        # ~ mac_address = b'\x52\x54\x00\x00\x00\x00' # (upstream)
        # (packet capture) actual mac address of transmitting device (e.g. us)
        mac_address = get_mac_addr_serving_ipv4(device.ipv4)

        # part1 = 0000; 03e4 (upstream; pkt-capt)
        payload = (
            b'\x07\x27', # 0727, 0734,
            b'\x00\x83',
            b'\x00\x00', # < null
            b'\x00\x64',
            b'\x00\x00', # < null
            b'\x00\x01',
            b'\x00\x00', # < null
            encode_integer(encoding.value),
        )
        self.command(device, payload, mac_address)

    def set_gain_level(
        self,
        device: DanteDevice,
        channel_type: DanteChannelType,
        channel_number: int,
        gain_level: int
    ):
        payload = (
            b'\x07\x27',
            b'\x10\x0a',
            b'\x00\x00', # < null
            b'\x00\x00', # < null
            b'\x00\x01',
            b'\x00\x01',
            b'\x00\x0c',
            b'\x00\x10',
            b'\x01x\02' if channel_type == DanteChannelType.RX else b'\x02\x01',
            b'\x00\x00', # < null
            b'\x00\x00', # < null
            encode_integer(channel_number),
            b'\x00\x00', # < null
            encode_integer(gain_level),
        )
        self.command(device, payload, b'\x52\x54\x00\x00\x00\x00')

    def set_sample_rate(
        self,
        device: DanteDevice,
        sample_rate: SampleRate
    ):
        payload = (
            b'\x07\x27',
            b'\x00\x81',
            b'\x00\x00', # < null
            b'\x00\x64',
            b'\x00\x00', # < null
            b'\x00\x01',
            encode_integer(sample_rate.value, 4),
        )
        self.command(device, payload, b'\x52\x54\x00\x00\x00\x00')

    def trigger_identify(self, device: DanteDevice):
        payload = (
            b'\x07\x31',
            b'\x00\x63',
            b'\x00\x00', # < null
            b'\x00\x64',
        )
        self.command(device, payload, NULL_HEXTET * 3)
