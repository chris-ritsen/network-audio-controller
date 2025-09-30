from collections.abc import Callable
from typing import NamedTuple, TypeAlias#, TYPE_CHECKING

# ~ if TYPE_CHECKING:
from zeroconf import ServiceInfo as MDNSServiceInfo

from .service import DanteService, MessageType
from .util import (
    NULL_HEXTET,
    encode_integer,
    decode_protocol_version_from_mdns,
    encode_protocol_version,
)

# ~ if TYPE_CHECKING:
from .device import DanteDevice
from .util import ProtocolVersion


# Py312 has new syntax for this
CommandCallback: TypeAlias = Callable[[bytes], None]


class DanteARCServiceDescriptor(NamedTuple):
    port: int
    protocol_version: ProtocolVersion


class DanteARCService(DanteService):
    '''
    Dante Audio Routing Channel
    '''
    SERVICE_HEADER_LENGTH: int = 10
    SERVICE_PORT: int = 4440
    SERVICE_TYPE_MDNS: str = '_netaudio-arc._udp.local.'
    SERVICE_TYPE_SHORT: str = 'arc'

    MAX_CHANNELS_PER_PAGE: int = 16


    @classmethod
    def build_service_descriptor(cls, mdns_service_info: MDNSServiceInfo) -> DanteARCServiceDescriptor:
        return DanteARCServiceDescriptor(**{
            'port': mdns_service_info.port,
            # ~ 'protocol_version': (2,6,0),
            'protocol_version': decode_protocol_version_from_mdns(mdns_service_info.properties[b'arcp_vers']),
        })

    def channels_on_page(self, current_page: int, total_channel_count: int):
        '''How many channels are expected on a given page.'''
        if total_channel_count // self.MAX_CHANNELS_PER_PAGE >= current_page:
            return self.MAX_CHANNELS_PER_PAGE
        return total_channel_count % self.MAX_CHANNELS_PER_PAGE

    def command(
        self,
        device: DanteDevice,
        command_code: bytes,
        command_body: tuple,
        callback: CommandCallback | None = None,
    ) -> None:
        port = device.arc.port
        ipv4 = device.ipv4
        message_idx = self._message_index.generate()

        command = b''.join((
            encode_protocol_version(device.arc.protocol_version),
            NULL_HEXTET, # message length, calculated below
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
