# ~ from __future__ import annotations
from typing import NamedTuple#, TYPE_CHECKING

# ~ if TYPE_CHECKING:
from zeroconf import ServiceInfo as MDNSServiceInfo

from .service import DanteUnicastService, MessageType
from .util import (
    CommandCallback,
    NULL_HEXTET,
    encode_mac_address,
    encode_integer,
    encode_string,
    decode_protocol_version_from_mdns,
    encode_protocol_version,
    get_ip_addr_serving_ipv4,
    get_mac_addr_serving_ipv4,
)

# ~ if TYPE_CHECKING:
from .device import DanteDevice
from .util import ProtocolVersion


class DanteCMCServiceDescriptor(NamedTuple):
    port: int
    protocol_version: ProtocolVersion


class DanteCMCService(DanteUnicastService):
    """
    Dante Control Monitoring Channel
    (excluding DVS-4.0 & Via)
    """
    SERVICE_HEADER_LENGTH: int = 10
    SERVICE_PORT: int = 8800
    SERVICE_TYPE_MDNS: str = "_netaudio-cmc._udp.local."
    SERVICE_TYPE_SHORT: str = 'cmc'

    @classmethod
    def build_service_descriptor(cls, mdns_service_info: MDNSServiceInfo) -> DanteCMCServiceDescriptor:
        return DanteCMCServiceDescriptor(**{
            'port': mdns_service_info.port,
            'protocol_version': decode_protocol_version_from_mdns(mdns_service_info.properties[b'cmcp_vers']),
        })

    def command(
        self,
        device: DanteDevice,
        command_code: bytes,
        command_body: tuple,
        callback: CommandCallback | None = None,
    ) -> None:
        port = device.cmc.port
        ipv4 = device.ipv4
        message_idx = self._message_index.generate()

        command = b''.join((
            encode_protocol_version(device.cmc.protocol_version),
            NULL_HEXTET,    # message length, calculated below
            encode_integer(message_idx),
            command_code,
            MessageType.SEND,
            *command_body,
        ))
        command = command[:2] + encode_integer(len(command)) + command[4:]

        self._message_store[message_idx] = {
            'device': device,
            'command_code': command_code,
            'command': command,
            'callback': callback,
        }
        self._send_queue.put(((str(ipv4), port), command))

    def _get_lengths(self, device_name: str):
        length = len(device_name)
        length = 12 - (length + length % 2) + length * 2
        return (
            length,
            length + 2,
            length + 6,
        )

    def metering_start(
        self,
        device: DanteDevice,
        timeout: bool = True,
    ):
        ipv4 = device.ipv4
        port = self._app.metering_service.port

        len1, len2, len3 = self._get_lengths(device.name)
        code = b'\x30\x10'
        body = (
            NULL_HEXTET,              # or b'\x03\xe4' -- must match the same octet in metering_stop()
            encode_mac_address(get_mac_addr_serving_ipv4(ipv4)),
            NULL_HEXTET,
            b'\x00\x04',
            encode_integer(len1),
            b'\x00\x01',              # {$LENMOD} (see below)
            encode_integer(len2),
            b'\x00\x0a',
            encode_string(device.name),
            b'\x00' * (3 if len(device.name) % 2 else 4),  # b'\x01\x00\x16\x00\x01', # (even number of chars)
            b'\x01',
            encode_integer(len3),
            b'\x00\x01',
            encode_integer(port),
            encode_integer(timeout),
            NULL_HEXTET,
            get_ip_addr_serving_ipv4(ipv4).packed,
            encode_integer(port),
            # ~ NULL_HEXTET * 3,      # if {$LENMOD} above is == 2
            # ~ encode_integer(port), # if {$LENMOD} above is == 2
            NULL_HEXTET,
        )
        self.command(device, code, body, self.__cb_null)


    def metering_stop(
        self,
        device: DanteDevice,
    ):
        ipv4 = device.ipv4
        port = self._app.metering_service.port

        len1, len2, len3 = self._get_lengths(device.name)
        code = b'\x30\x10'
        body = (
            NULL_HEXTET,     # or b'\x03\xe4' -- must match the same octet in metering_start()
            encode_mac_address(get_mac_addr_serving_ipv4(ipv4)),
            NULL_HEXTET,
            b'\x00\x04',
            encode_integer(len1),
            b'\x00\x01',     # {$LENMOD}
            encode_integer(len2),
            b'\x00\x0a',
            encode_string(device.name),
            b'\x00' * (3 if len(device.name) % 2 else 4),  # b'\x01\x00\x16\x00\x01', # (even number of chars)
            b'\x01',
            encode_integer(len3),
            b'\x00\x01',
            encode_integer(port),
            b'\x00\x01',
            NULL_HEXTET * 5, # if {$LENMOD} above == 2 : If == 2 then 9*NULL_HEXTETs (instead of 5) here
        )
        self.command(device, code, body, self.__cb_null)

    def __cb_null(self, response: bytes) -> None:
        """We don't currently care for the response of the metering_* methods,
        so this handles that to prevent it from being printed to the console line
        (which is what happens if a callback is not set)."""
