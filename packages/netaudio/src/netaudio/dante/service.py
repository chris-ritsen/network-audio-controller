from __future__ import annotations

import asyncio
import logging
import socket
import struct
from netaudio.dante.transport import DanteMulticastProtocol

logger = logging.getLogger("netaudio")


class DanteMulticastService:
    def __init__(
        self,
        multicast_group: str,
        multicast_port: int,
        packet_store=None,
        interface_ip: str | None = None,
        dissect: bool = False,
    ):
        self._multicast_group = multicast_group
        self._multicast_port = multicast_port
        self._protocol: DanteMulticastProtocol | None = None
        self._packet_store = packet_store
        self._dissect = dissect
        self._session_id: int | None = None
        self._interface_ip = interface_ip

    @property
    def session_id(self) -> int | None:
        return self._session_id

    @session_id.setter
    def session_id(self, value: int | None) -> None:
        self._session_id = value

    async def start(self) -> None:
        loop = asyncio.get_running_loop()

        local_ip = self._interface_ip or self._detect_interface_ip()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.bind(("0.0.0.0", self._multicast_port))

            membership_request = struct.pack(
                "4s4s",
                socket.inet_aton(self._multicast_group),
                socket.inet_aton(local_ip),
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership_request)

            _, protocol = await loop.create_datagram_endpoint(
                lambda: DanteMulticastProtocol(self._on_packet),
                sock=sock,
            )
        except BaseException:
            sock.close()
            raise
        self._protocol = protocol
        logger.info(
            f"Multicast service started on {self._multicast_group}:{self._multicast_port} (interface {local_ip})"
        )

    def _detect_interface_ip(self) -> str:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect((self._multicast_group, 1))
            local_ip = sock.getsockname()[0]
            sock.close()
            return local_ip
        except OSError as exception:
            logger.debug(f"Failed to determine local IP: {exception}")
            return "0.0.0.0"

    async def stop(self) -> None:
        if self._protocol is not None:
            self._protocol.close()
            self._protocol = None

    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        pass
