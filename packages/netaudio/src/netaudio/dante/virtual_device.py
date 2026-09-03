from __future__ import annotations

import asyncio
import logging
import random
import socket
import struct
from dataclasses import dataclass, field

from zeroconf import IPVersion, ServiceInfo
from zeroconf.asyncio import AsyncZeroconf

from netaudio.dante.const import (
    DEVICE_ARC_PORT,
    DEVICE_CONTROL_PORT,
    DEVICE_HEARTBEAT_PORT,
    DEVICE_INFO_PORT,
    DEVICE_SETTINGS_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
    MULTICAST_GROUP_HEARTBEAT,
    SERVICE_ARC,
    SERVICE_CHAN,
    SERVICE_CMC,
)
from netaudio.dante.device_kind import VIRTUAL_DEVICE_MANUFACTURER, VIRTUAL_DEVICE_MODEL
from netaudio.dante.const import (
    DEVICE_ARC_SECONDARY_PORT,
    MCAST_HEADER_LENGTH,
    PCM_ENCODING_CAPABILITY_BITS,
    PCM_ENCODING_OCTETS,
)
from netaudio.dante.virtual_device_requests import VirtualDeviceRequestHandler

logger = logging.getLogger("netaudio")


@dataclass
class VirtualDeviceConfig:
    name: str = "netaudio-virtual"
    model: str = VIRTUAL_DEVICE_MODEL
    manufacturer: str = VIRTUAL_DEVICE_MANUFACTURER
    tx_channels: list[str] = field(default_factory=lambda: ["Ch 1", "Ch 2"])
    rx_channels: list[str] = field(default_factory=lambda: ["Ch 1", "Ch 2"])
    sample_rate: int = 48000
    supported_sample_rates: list[int] | None = None
    encoding: int = 24
    supported_encodings: list[int] = field(default_factory=lambda: [24, 16, 32])
    configured_latency_ns: int = 1_000_000
    active_latency_ns: int | None = None
    default_latency_ns: int = 1_000_000
    minimum_latency_ns: int = 150_000
    maximum_latency_ns: int = 21_333_334
    interface_ip: str | None = None

    def __post_init__(self):
        if self.supported_sample_rates is None:
            self.supported_sample_rates = [self.sample_rate]
        if self.sample_rate not in self.supported_sample_rates:
            raise ValueError("sample_rate must be present in supported_sample_rates")
        if self.encoding not in self.supported_encodings:
            raise ValueError("encoding must be present in supported_encodings")
        if self.active_latency_ns is None:
            self.active_latency_ns = self.configured_latency_ns


class VirtualDevice(VirtualDeviceRequestHandler):
    def __init__(self, config: VirtualDeviceConfig | None = None):
        self._config = config or VirtualDeviceConfig()
        self._mac = self._generate_mac()
        self._running = False
        self._heartbeat_task: asyncio.Task | None = None
        self._transports: list[asyncio.DatagramTransport] = []
        self._zeroconf: AsyncZeroconf | None = None
        self._service_infos: list[ServiceInfo] = []
        self._arc_port = DEVICE_ARC_PORT
        self._local_ip: str | None = None
        self._mcast_seqnum: int = 1
        self._mcast_sock: socket.socket | None = None
        self._mcast_transport: asyncio.DatagramTransport | None = None
        self._subscriptions: dict[int, tuple[str, str]] = {}

    @property
    def config(self) -> VirtualDeviceConfig:
        return self._config

    @property
    def mac(self) -> str:
        return self._mac

    def _generate_mac(self) -> str:
        octets = [
            0x02,
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
        ]
        return ":".join(f"{b:02x}" for b in octets)

    def _build_mcast_packet(self, start_code: int, opcode: bytes, content: bytes) -> bytes:
        total_length = MCAST_HEADER_LENGTH + len(content)
        ip_bytes = socket.inet_aton(self._local_ip) if self._local_ip else b"\x00\x00\x00\x00"
        device_id = b"\x00\x00" + ip_bytes + b"\x00\x00"
        vendor = b"Audinate\x00"[:8].ljust(8, b"\x00")

        header = struct.pack(">HHH", start_code, total_length, self._mcast_seqnum)
        header += struct.pack(">H", 0)
        header += device_id
        header += vendor
        header += opcode[:8].ljust(8, b"\x00")

        self._mcast_seqnum = (self._mcast_seqnum + 1) & 0xFFFF
        return header + content

    def _detect_local_ip(self) -> str:
        if self._config.interface_ip:
            return self._config.interface_ip
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.connect((MULTICAST_GROUP_HEARTBEAT, 1))
            return sock.getsockname()[0]
        except OSError:
            return "0.0.0.0"
        finally:
            sock.close()

    async def start(self) -> None:
        self._local_ip = self._detect_local_ip()
        logger.info(f"Starting virtual device '{self._config.name}' on {self._local_ip}")

        await self._start_responders()
        await self._start_mcast_server()
        await self._register_mdns()

        self._send_mcast_board_info()
        self._send_mcast_product_info()

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._running = True

        ports = [t.get_extra_info("sockname")[1] for t in self._transports]
        logger.info(
            f"Virtual device '{self._config.name}' running "
            f"(MAC={self._mac}, ports={ports}, "
            f"TX={len(self._config.tx_channels)}, RX={len(self._config.rx_channels)})"
        )

    async def stop(self) -> None:
        self._running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        for transport in self._transports:
            transport.close()
        self._transports.clear()

        self._mcast_transport = None

        await self._unregister_mdns()

        logger.info(f"Virtual device '{self._config.name}' stopped")

    def _mcast_send(self, dest_ip: str, dest_port: int, start_code: int, opcode: bytes, content: bytes) -> None:
        if self._mcast_transport:
            packet = self._build_mcast_packet(start_code, opcode, content)
            self._mcast_transport.sendto(packet, (dest_ip, dest_port))
        elif self._mcast_sock:
            packet = self._build_mcast_packet(start_code, opcode, content)
            self._mcast_sock.sendto(packet, (dest_ip, dest_port))
        else:
            logger.warning("no mcast socket available")

    def _send_mcast_board_info(self) -> None:
        self._mcast_send(
            MULTICAST_GROUP_CONTROL_MONITORING,
            DEVICE_INFO_PORT,
            0xFFFF,
            bytes([0x07, 0x2A, 0x00, 0x60, 0, 0, 0, 0]),
            self._build_board_info_content(),
        )
        logger.debug("Sent mcast board_info")

    def _send_mcast_product_info(self) -> None:
        self._mcast_send(
            MULTICAST_GROUP_CONTROL_MONITORING,
            DEVICE_INFO_PORT,
            0xFFFF,
            bytes([0x07, 0x2A, 0x00, 0xC0, 0, 0, 0, 0]),
            self._build_product_info_content(),
        )
        logger.debug("Sent mcast product_info")

    def _send_mcast_clock_stats(self) -> None:
        mac_bytes = bytes.fromhex(self._mac.replace(":", ""))
        content = bytearray(120)
        content[0:8] = bytes([0x00, 0x03, 0x00, 0x03, 0x00, 0x00, 0x00, 0x9F])
        struct.pack_into(">i", content, 8, 0)
        content[12:18] = mac_bytes
        self._mcast_send(
            MULTICAST_GROUP_CONTROL_MONITORING,
            DEVICE_INFO_PORT,
            0xFFFF,
            bytes([0x07, 0x2A, 0x00, 0x20, 0, 0, 0, 0]),
            bytes(content),
        )

    def _send_mcast_network_info(self) -> None:
        ip_bytes = socket.inet_aton(self._local_ip) if self._local_ip else b"\x00\x00\x00\x00"
        mac_bytes = bytes.fromhex(self._mac.replace(":", ""))
        content = bytearray()
        content += bytes([0x00, 0x01, 0x00, 0x00, 0x00, 0x00])
        content += struct.pack(">H", 1000)
        content += struct.pack(">H", 1)
        content += mac_bytes
        content += ip_bytes
        content += bytes([255, 255, 255, 0])
        content += ip_bytes
        content += ip_bytes
        content += bytes(32)
        self._mcast_send(
            MULTICAST_GROUP_CONTROL_MONITORING,
            DEVICE_INFO_PORT,
            0xFFFF,
            bytes([0x07, 0x2A, 0x00, 0x11, 0, 0, 0, 0]),
            bytes(content),
        )

    def _build_audio_capability_status_packet(
        self,
        status_opcode: int,
        current_value: int,
        supported_values: list[int],
    ) -> bytes:
        content = struct.pack(">HHI", 0x0018, len(supported_values), current_value)
        content += struct.pack(">II", 0, 0x00020000)
        content += b"".join(struct.pack(">I", value) for value in supported_values)
        return self._build_mcast_packet(
            0xFFFF,
            bytes([0x07, 0x24]) + struct.pack(">H", status_opcode) + bytes(4),
            content,
        )

    def _send_audio_capability_status(
        self,
        status_opcode: int,
        current_value: int,
        supported_values: list[int],
    ) -> None:
        packet = self._build_audio_capability_status_packet(status_opcode, current_value, supported_values)
        if self._mcast_transport:
            self._mcast_transport.sendto(packet, (MULTICAST_GROUP_CONTROL_MONITORING, DEVICE_INFO_PORT))
        elif self._mcast_sock:
            self._mcast_sock.sendto(packet, (MULTICAST_GROUP_CONTROL_MONITORING, DEVICE_INFO_PORT))
        else:
            logger.warning("no mcast socket available")

    def _send_sample_rate_status(self) -> None:
        self._send_audio_capability_status(
            0x0080,
            self._config.sample_rate,
            self._config.supported_sample_rates,
        )

    def _send_encoding_status(self) -> None:
        self._send_audio_capability_status(
            0x0082,
            self._config.encoding,
            self._config.supported_encodings,
        )

    def _legacy_pcm_capability(self) -> tuple[int, int] | None:
        current_encoding_octets = PCM_ENCODING_OCTETS.get(self._config.encoding)
        if current_encoding_octets is None:
            return None

        capability_bitmap = 0
        for supported_encoding in self._config.supported_encodings:
            capability_bit = PCM_ENCODING_CAPABILITY_BITS.get(supported_encoding)
            if capability_bit is None:
                return None
            capability_bitmap |= capability_bit

        return current_encoding_octets, capability_bitmap

    def _pcm_capability_property(self) -> str | None:
        legacy_pcm_capability = self._legacy_pcm_capability()
        if legacy_pcm_capability is None:
            return None
        current_encoding_octets, capability_bitmap = legacy_pcm_capability

        return f"{current_encoding_octets} 0x{capability_bitmap:x}"

    def _build_channel_metadata(self) -> bytes | None:
        legacy_pcm_capability = self._legacy_pcm_capability()
        if legacy_pcm_capability is None:
            return None
        _, capability_bitmap = legacy_pcm_capability
        return struct.pack(
            ">IHHHHHH",
            self._config.sample_rate,
            0x0101,
            self._config.encoding,
            0x0400,
            self._config.encoding,
            self._config.encoding,
            capability_bitmap,
        )

    async def _start_mcast_server(self) -> None:
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 255)
        if self._local_ip and self._local_ip != "0.0.0.0":
            sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_IF,
                socket.inet_aton(self._local_ip),
            )

        try:
            sock.bind(("", DEVICE_SETTINGS_PORT))
        except OSError as e:
            logger.warning(f"Could not bind mcast server on :{DEVICE_SETTINGS_PORT}: {e}")
            sock.close()
            return

        mreq = struct.pack(
            "4s4s",
            socket.inet_aton(MULTICAST_GROUP_CONTROL_MONITORING),
            socket.inet_aton(self._local_ip if self._local_ip else "0.0.0.0"),
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.setblocking(False)

        transport, _ = await loop.create_datagram_endpoint(
            lambda: _McastInfoProtocol(self),
            sock=sock,
        )
        self._transports.append(transport)
        self._mcast_transport = transport
        logger.info(f"Multicast server on :{DEVICE_SETTINGS_PORT} (group {MULTICAST_GROUP_CONTROL_MONITORING})")

    async def _start_responders(self) -> None:
        loop = asyncio.get_running_loop()

        for port in [DEVICE_ARC_PORT, DEVICE_ARC_SECONDARY_PORT, DEVICE_CONTROL_PORT]:
            try:
                transport, _ = await loop.create_datagram_endpoint(
                    lambda: _ARCProtocol(self),
                    local_addr=("0.0.0.0", port),
                    family=socket.AF_INET,
                )
                self._transports.append(transport)
                if port == DEVICE_ARC_PORT:
                    self._arc_port = port
                logger.info(f"Responder listening on 0.0.0.0:{port}")
            except OSError as e:
                logger.warning(f"Could not bind port {port}: {e}")

    async def _register_mdns(self) -> None:
        ip_bytes = socket.inet_aton(self._local_ip)

        server_name = f"{self._config.name}.local."

        arc_info = ServiceInfo(
            SERVICE_ARC,
            f"{self._config.name}.{SERVICE_ARC}",
            addresses=[ip_bytes],
            port=self._arc_port,
            properties={
                "arcp_vers": "2.7.41",
                "arcp_min": "0.2.4",
                "router_vers": "4.0.2",
                "router_info": self._config.model,
                "mf": self._config.manufacturer,
                "model": self._config.model,
            },
            server=server_name,
        )

        cmc_info = ServiceInfo(
            SERVICE_CMC,
            f"{self._config.name}.{SERVICE_CMC}",
            addresses=[ip_bytes],
            port=DEVICE_CONTROL_PORT,
            properties={
                "id": "0000" + ip_bytes.hex() + "0000",
                "process": "0",
                "cmcp_vers": "1.2.0",
                "cmcp_min": "1.0.0",
                "server_vers": "4.0.2",
                "channels": "0x6000004d",
                "mf": self._config.manufacturer,
                "model": self._config.model,
            },
            server=server_name,
        )

        self._service_infos = [arc_info, cmc_info]

        for i, ch_name in enumerate(self._config.tx_channels):
            channel_properties = {
                "txtvers": "2",
                "dbcp1": "0x1102",
                "dbcp": "0x1004",
                "id": str(i + 1),
                "rate": str(self._config.sample_rate),
                "enc": str(self._config.encoding),
                "en": str(self._config.encoding),
                "latency_ns": str(self._config.configured_latency_ns),
                "fpp": "32,2",
                "nchan": "8",
            }
            pcm_capability_property = self._pcm_capability_property()
            if pcm_capability_property is not None:
                channel_properties["pcm"] = pcm_capability_property
            chan_info = ServiceInfo(
                SERVICE_CHAN,
                f"{ch_name}@{self._config.name}.{SERVICE_CHAN}",
                addresses=[ip_bytes],
                port=DEVICE_ARC_SECONDARY_PORT,
                properties=channel_properties,
                server=server_name,
            )
            self._service_infos.append(chan_info)

        self._zeroconf = AsyncZeroconf(
            interfaces=[self._local_ip],
            ip_version=IPVersion.V4Only,
        )

        for info in self._service_infos:
            await self._zeroconf.async_register_service(info)

        logger.info(f"mDNS services registered for '{self._config.name}' ({len(self._service_infos)} services)")

    async def _unregister_mdns(self) -> None:
        if self._zeroconf:
            for info in self._service_infos:
                await self._zeroconf.async_unregister_service(info)
            await self._zeroconf.async_close()
            self._zeroconf = None

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                self._send_heartbeat()
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass

    def _send_heartbeat(self) -> None:
        ctr = self._mcast_seqnum
        tx_count = len(self._config.tx_channels)
        rx_count = len(self._config.rx_channels)

        hb_content = bytearray()

        hb_content += struct.pack(">HH", 16, 0x8001)
        hb_content += struct.pack(">HH", 4, 4)
        hb_content += struct.pack(">HH", ctr, 0)
        hb_content += struct.pack(">i", 0)

        total_peaks = tx_count + rx_count
        payload_length = (12 + total_peaks + 3) & ~3
        record_length = 12 + payload_length
        padding_length = record_length - 24 - total_peaks
        hb_content += struct.pack(">HH", record_length, 0x8002)
        hb_content += struct.pack(">HH", 4, payload_length)
        hb_content += struct.pack(">HH", ctr, 0)
        hb_content += struct.pack(">HH", tx_count, 0)
        hb_content += struct.pack(">HH", rx_count, 0)
        hb_content += struct.pack(">HH", 24, 0)
        hb_content += b"\xff" * total_peaks
        hb_content += b"\x00" * padding_length

        self._mcast_send(
            MULTICAST_GROUP_HEARTBEAT,
            DEVICE_HEARTBEAT_PORT,
            0xFFFE,
            bytes([0, 8, 0, 1, 0x10, 0, 0, 0]),
            bytes(hb_content),
        )
        logger.debug(f"Heartbeat sent ({len(hb_content) + MCAST_HEADER_LENGTH}B)")


class _ARCProtocol(asyncio.DatagramProtocol):
    def __init__(self, device: VirtualDevice):
        self._device = device
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        local = self.transport.get_extra_info("sockname") if self.transport else None
        logger.debug(f"RECV {addr} -> :{local[1] if local else '?'} ({len(data)}B): {data[:20].hex()}")
        response = self._device._handle_request(data, addr)
        if response and self.transport:
            self.transport.sendto(response, addr)


class _McastInfoProtocol(asyncio.DatagramProtocol):
    def __init__(self, device: VirtualDevice):
        self._device = device
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        logger.debug(f"MCAST_RECV {addr} ({len(data)}B)")
        if len(data) < MCAST_HEADER_LENGTH:
            return
        self._device._handle_mcast_format_request(data, addr)
