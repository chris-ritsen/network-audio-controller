import asyncio
import logging
import socket
import struct
from netaudio.dante.transport import DanteMulticastProtocol, DanteUnicastProtocol

logger = logging.getLogger("netaudio")


class DanteUnicastService:
    def __init__(self, packet_store=None, dissect=False):
        self._protocol: DanteUnicastProtocol | None = None
        self._packet_store = packet_store
        self._dissect = dissect
        self._transaction_counter = 0
        self._session_id: int | None = None

    @property
    def session_id(self) -> int | None:
        return self._session_id

    @session_id.setter
    def session_id(self, value: int | None) -> None:
        self._session_id = value

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

        local_addr = self._protocol.transport.get_extra_info("sockname") if self._protocol.transport else None
        src_ip = local_addr[0] if local_addr else None
        src_port = local_addr[1] if local_addr else None

        if self._packet_store:
            try:
                self._packet_store.store_packet(
                    payload=packet,
                    source_type="netaudio_request",
                    device_name=device_name,
                    device_ip=device_ip,
                    src_ip=src_ip,
                    src_port=src_port,
                    dst_ip=device_ip,
                    dst_port=port,
                    direction="request",
                    session_id=self._session_id,
                )
            except Exception as exception:
                logger.debug(f"PacketStore error (request): {exception}")

        if self._dissect:
            self._log_dissected(packet, device_ip, port, direction="request", command_name=logical_command_name)

        response = await self._protocol.send_and_expect(
            packet, (device_ip, port), transaction_id,
            timeout=timeout, logical_command_name=logical_command_name,
        )

        if self._dissect and response is not None:
            self._log_dissected(response, device_ip, port, direction="response", command_name=logical_command_name)

        if self._packet_store and response is not None:
            try:
                self._packet_store.store_packet(
                    payload=response,
                    source_type="netaudio_response",
                    device_name=device_name,
                    device_ip=device_ip,
                    src_ip=device_ip,
                    src_port=port,
                    dst_ip=src_ip,
                    dst_port=src_port,
                    direction="response",
                    session_id=self._session_id,
                )
            except Exception as exception:
                logger.debug(f"PacketStore error (response): {exception}")

        return response

    def send(self, packet: bytes, device_ip: str, port: int) -> None:
        if self._protocol is None:
            logger.debug("Service not started, cannot send")
            return
        if self._dissect:
            self._log_dissected(packet, device_ip, port, direction="send")
        self._protocol.send_fire_and_forget(packet, (device_ip, port))

    def _log_dissected(self, payload: bytes, device_ip: str, port: int, direction: str = "", command_name: str = "") -> None:
        try:
            from netaudio.common.app_config import settings as app_settings
            from netaudio.dante.packet_dissector import dissect_and_render, format_dissect_label
            color = not app_settings.no_color
            label = format_dissect_label(direction, f"{device_ip}:{port}", command_name=command_name, color=color)
            rendered = dissect_and_render(payload, indent="  ", color=color)
            logger.debug(f"Dissect [{label}] {len(payload)}B:\n{rendered}")
        except Exception as exception:
            logger.debug(f"Dissect error: {exception}")

    @staticmethod
    def _extract_transaction_id(packet: bytes) -> int:
        if len(packet) >= 6:
            return struct.unpack(">H", packet[4:6])[0]
        return 0


class DanteMulticastService:
    def __init__(self, multicast_group: str, multicast_port: int, packet_store=None, interface_ip: str | None = None, dissect: bool = False):
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
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", self._multicast_port))

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
        self._protocol = protocol
        logger.info(f"Multicast service started on {self._multicast_group}:{self._multicast_port} (interface {local_ip})")

    def _detect_interface_ip(self) -> str:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect((self._multicast_group, 1))
            local_ip = sock.getsockname()[0]
            sock.close()
            return local_ip
        except Exception:
            return "0.0.0.0"

    async def stop(self) -> None:
        if self._protocol is not None:
            self._protocol.close()
            self._protocol = None

    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        pass
