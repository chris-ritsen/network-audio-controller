# ~ from __future__ import annotations
from typing import NamedTuple#, TYPE_CHECKING

# ~ if TYPE_CHECKING:
from zeroconf import ServiceInfo as MDNSServiceInfo

from .service import DanteService, MessageType
from .util import (
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


class DanteCMCService(DanteService):
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
        # ~ callback: CommandCallback | None = None,
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
            # ~ 'callback': callback,
        }

        print(command)
        self._send_queue.put(((str(ipv4), port), command))

    def _get_lengths(self, device_name: str):
        length = len(device_name)
        length = 12 - (length + length % 2) + length * 2
        return (
            length,
            length + 2,
            length + 6,
        )

    def _command_volume_start(
        self,
        device: DanteDevice,
        timeout: bool = True,
    ):
        ipv4 = device.ipv4
        port = self._app.volume_service.port

        len1, len2, len3 = self._get_lengths(device.name)
        code = b'\x30\x10'
        body = (
            b'\x03\xe4', #NULL_HEXTET,
            encode_mac_address(get_mac_addr_serving_ipv4(ipv4)),
            NULL_HEXTET,
            b'\x00\x04',
            encode_integer(len1),
            b'\x00\x02',
            encode_integer(len2),
            b'\x00\x0a',
            encode_string(device.name),
            b'\x00\x00\x00\x00\x01', # b'\x00' * (4 if len(device.name) % 2 else 5),  # b'\x01\x00\x16\x00\x01', # (even number of chars)
            encode_integer(len3),
            b'\x00\x01',
            encode_integer(port),
            encode_integer(timeout),
            NULL_HEXTET,
            get_ip_addr_serving_ipv4(ipv4).packed,
            encode_integer(port),
            NULL_HEXTET * 3,
            encode_integer(port),
            NULL_HEXTET,
        )
        self.command(device, code, body)


    def _command_volume_stop(
        self,
        device: DanteDevice,
    ):
        ipv4 = device.ipv4
        port = self._app.volume_service.port

        len1, len2, len3 = self._get_lengths(device.name)
        code = b'\x30\x10'
        body = (
            NULL_HEXTET,
            encode_mac_address(get_mac_addr_serving_ipv4(ipv4)),
            NULL_HEXTET,
            b'\x00\x04',
            encode_integer(len1),
            b'\x00\x01',
            encode_integer(len2),
            b'\x00\x0a',
            encode_string(device.name),
            b'\x00' * (4 if len(device.name) % 2 else 5), # b'\x01\x00\x16\x00\x01', # (even number of chars)
            encode_integer(len3),
            b'\x00\x01',
            encode_integer(port),
            b'\x00\x01', # timeout in volume_start command
            NULL_HEXTET,
            get_ip_addr_serving_ipv4(ipv4).packed,
            NULL_HEXTET,
            NULL_HEXTET,
        )
        self.command(device, code, body)

    # ~ def get_volume(self, device: DanteDevice):


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
