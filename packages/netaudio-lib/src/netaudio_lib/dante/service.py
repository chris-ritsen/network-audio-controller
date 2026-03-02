import asyncio
import logging
import socket
import struct

from netaudio_lib.dante.transport import DanteMulticastProtocol, DanteUnicastProtocol

logger = logging.getLogger("netaudio")


class DanteUnicastService:
    def __init__(self, packet_store=None):
        self._protocol: DanteUnicastProtocol | None = None
        self._packet_store = packet_store
        self._transaction_counter = 0

    def _next_transaction_id(self) -> int:
        self._transaction_counter = (self._transaction_counter + 1) & 0xFFFF
        return self._transaction_counter

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        _, protocol = await loop.create_datagram_endpoint(
            DanteUnicastProtocol,
            local_addr=("0.0.0.0", 0),
            family=socket.AF_INET,
        )
        self._protocol = protocol

    async def stop(self) -> None:
        if self._protocol is not None:
            self._protocol.close()
            self._protocol = None

    async def request(
        self,
        packet: bytes,
        device_ip: str,
        port: int,
        timeout: float = 0.5,
        device_name: str = "",
        logical_command_name: str = "unknown",
    ) -> bytes | None:
        if self._protocol is None:
            logger.debug("Service not started, cannot send request")
            return None

        transaction_id = self._extract_transaction_id(packet)

        if self._packet_store:
            try:
                self._packet_store.store_packet(
                    payload=packet,
                    source_type="netaudio_request",
                    device_name=device_name,
                    device_ip=device_ip,
                    direction="request",
                )
            except Exception as exception:
                logger.debug(f"PacketStore error (request): {exception}")

        response = await self._protocol.send_and_expect(
            packet, (device_ip, port), transaction_id, timeout=timeout
        )

        if self._packet_store and response is not None:
            try:
                self._packet_store.store_packet(
                    payload=response,
                    source_type="netaudio_response",
                    device_name=device_name,
                    device_ip=device_ip,
                    direction="response",
                )
            except Exception as exception:
                logger.debug(f"PacketStore error (response): {exception}")

        return response

    def send(self, packet: bytes, device_ip: str, port: int) -> None:
        if self._protocol is None:
            logger.debug("Service not started, cannot send")
            return
        self._protocol.send_fire_and_forget(packet, (device_ip, port))

    @staticmethod
    def _extract_transaction_id(packet: bytes) -> int:
        if len(packet) >= 6:
            return struct.unpack(">H", packet[4:6])[0]
        return 0


class DanteMulticastService:
    def __init__(self, multicast_group: str, multicast_port: int, packet_store=None):
        self._multicast_group = multicast_group
        self._multicast_port = multicast_port
        self._protocol: DanteMulticastProtocol | None = None
        self._packet_store = packet_store

    async def start(self) -> None:
        loop = asyncio.get_running_loop()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", self._multicast_port))

        membership_request = struct.pack(
            "4s4s",
            socket.inet_aton(self._multicast_group),
            socket.inet_aton("0.0.0.0"),
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership_request)

        _, protocol = await loop.create_datagram_endpoint(
            lambda: DanteMulticastProtocol(self._on_packet),
            sock=sock,
        )
        self._protocol = protocol
        logger.info(
            f"Multicast service started on {self._multicast_group}:{self._multicast_port}"
        )

    async def stop(self) -> None:
        if self._protocol is not None:
            self._protocol.close()
            self._protocol = None

    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        pass
