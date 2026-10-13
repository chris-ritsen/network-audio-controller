from __future__ import annotations

import asyncio
import logging
import math
import random
import socket
import struct
from dataclasses import dataclass, field

from zeroconf import IPVersion, ServiceInfo
from zeroconf.asyncio import AsyncZeroconf

from netaudio import core
from netaudio.dante.const import (
    DEVICE_ARC_PORT,
    DEVICE_CONTROL_PORT,
    DEVICE_HEARTBEAT_PORT,
    DEVICE_INFO_PORT,
    DEVICE_SETTINGS_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
    MULTICAST_GROUP_HEARTBEAT,
)
from netaudio.dante.device_kind import VIRTUAL_DEVICE_MANUFACTURER, VIRTUAL_DEVICE_MODEL
from netaudio.dante.const import (
    DEVICE_ARC_SECONDARY_PORT,
)
from netaudio.dante.virtual_device_requests import VirtualDeviceRequestHandler

logger = logging.getLogger("netaudio")

METERING_INTERVAL_SECONDS = 0.1
METERING_LEASE_SECONDS = 10.0


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
    arc_port: int = DEVICE_ARC_PORT
    control_port: int = DEVICE_CONTROL_PORT
    settings_port: int = DEVICE_SETTINGS_PORT
    process_id: int = 0
    detailed_metering: bool = False

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
        self._metering_destinations: dict[tuple[str, int], tuple[str, int, float]] = {}
        self._metering_sequence = 0
        self._metering_task: asyncio.Task | None = None
        self._metering_transport: asyncio.DatagramTransport | None = None

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

    def _advertised_device_id(self) -> str:
        address = socket.inet_aton(self._local_ip or "0.0.0.0")
        return (b"\x00\x00" + address + self._config.process_id.to_bytes(2, "big")).hex()

    def _build_publication(self, publication: dict) -> bytes:
        packet = core.build_publication(
            {
                "source_ip": self._local_ip or "0.0.0.0",
                "message_id": self._mcast_seqnum,
                "device_id": list(bytes.fromhex(self._advertised_device_id())),
                "publication": publication,
            }
        )
        self._mcast_seqnum = core.next_publication_id(self._mcast_seqnum)

        return packet

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
        await self._start_metering_transport()
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

        for task in (self._heartbeat_task, self._metering_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._heartbeat_task = None
        self._metering_task = None
        self._metering_destinations.clear()
        self._metering_transport = None

        for transport in self._transports:
            transport.close()
        self._transports.clear()

        self._mcast_transport = None

        await self._unregister_mdns()

        logger.info(f"Virtual device '{self._config.name}' stopped")

    def _send_status_publication(self, publication: dict) -> None:
        self._send_publication_packet(
            self._build_publication(publication), MULTICAST_GROUP_CONTROL_MONITORING, DEVICE_INFO_PORT
        )

    def _send_publication_packet(self, packet: bytes, dest_ip: str, dest_port: int) -> None:
        if self._mcast_transport:
            self._mcast_transport.sendto(packet, (dest_ip, dest_port))
        elif self._mcast_sock:
            self._mcast_sock.sendto(packet, (dest_ip, dest_port))
        else:
            logger.warning("no mcast socket available")

    def _board_info(self) -> dict:
        if not self._config.detailed_metering:
            return {"kind": "board_info", "name": self._config.name}
        return {"kind": "board_info", "name": self._config.name, "detailed_metering": True}

    def _send_mcast_board_info(self) -> None:
        self._send_status_publication(self._board_info())

    def _send_mcast_product_info(self) -> None:
        self._send_status_publication(
            {
                "kind": "product_info",
                "name": self._config.name,
                "manufacturer": self._config.manufacturer,
                "model": self._config.model,
            }
        )

    def _send_mcast_clock_stats(self) -> None:
        self._send_status_publication(
            {"kind": "clock_status", "mac_address": list(bytes.fromhex(self._mac.replace(":", "")))}
        )

    def _send_mcast_network_info(self) -> None:
        self._send_status_publication(
            {"kind": "interface_status", "mac_address": list(bytes.fromhex(self._mac.replace(":", "")))}
        )

    def _build_audio_capability_status_packet(
        self,
        kind: str,
        current_value: int,
        supported_values: list[int],
    ) -> bytes:
        return self._build_publication(
            {
                "kind": "audio",
                "capability": kind,
                "current_value": current_value,
                "supported_values": supported_values,
            }
        )

    def _send_audio_capability_status(
        self,
        kind: str,
        current_value: int,
        supported_values: list[int],
    ) -> None:
        packet = self._build_audio_capability_status_packet(kind, current_value, supported_values)

        self._send_publication_packet(packet, MULTICAST_GROUP_CONTROL_MONITORING, DEVICE_INFO_PORT)

    def _send_sample_rate_status(self) -> None:
        assert self._config.supported_sample_rates is not None

        self._send_audio_capability_status(
            "sample_rate",
            self._config.sample_rate,
            self._config.supported_sample_rates,
        )

    def _send_encoding_status(self) -> None:
        self._send_audio_capability_status(
            "encoding",
            self._config.encoding,
            self._config.supported_encodings,
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
            sock.bind(("", self._config.settings_port))
        except OSError as e:
            logger.warning(f"Could not bind mcast server on :{self._config.settings_port}: {e}")
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
        logger.info(f"Multicast server on :{self._config.settings_port} (group {MULTICAST_GROUP_CONTROL_MONITORING})")

    async def _start_metering_transport(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        if self._local_ip and self._local_ip != "0.0.0.0":
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self._local_ip))
        sock.bind(("", 0))
        sock.setblocking(False)
        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(asyncio.DatagramProtocol, sock=sock)
        self._transports.append(transport)
        self._metering_transport = transport

    def _update_metering_destination(self, request: dict) -> None:
        key = (request["owner"], request["discriminator"])
        destination = request["destination"]

        if destination is None:
            if self._metering_destinations.pop(key, None):
                logger.info(f"Metering stopped for owner {key[0]} discriminator {key[1]}")
            return

        expires_at = asyncio.get_running_loop().time() + METERING_LEASE_SECONDS
        if key not in self._metering_destinations:
            logger.info(
                f"Metering to {destination['ipv4']}:{destination['port']} for owner {key[0]} discriminator {key[1]}"
            )
        self._metering_destinations[key] = (destination["ipv4"], destination["port"], expires_at)

        if self._running and (self._metering_task is None or self._metering_task.done()):
            self._metering_task = asyncio.create_task(self._metering_loop())

    async def _metering_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while self._metering_destinations:
            now = loop.time()
            for key, (_, _, expires_at) in list(self._metering_destinations.items()):
                if expires_at <= now:
                    del self._metering_destinations[key]
                    logger.info(f"Metering lease expired for owner {key[0]} discriminator {key[1]}")
            if self._metering_destinations:
                self._send_metering_frame(now)
            await asyncio.sleep(METERING_INTERVAL_SECONDS)

    def _synthetic_levels(self, count: int, phase: float, now: float) -> list[int]:
        levels = []
        for index in range(count):
            swing = math.sin(now * 2.0 + index * 0.7 + phase + self._config.process_id * 1.3)
            levels.append(1 + round(2 * (30 - 24 * swing)))
        return levels

    def _send_metering_frame(self, now: float) -> None:
        if not self._metering_transport:
            return

        packet = core.build_publication(
            {
                "source_ip": self._local_ip or "0.0.0.0",
                "message_id": self._metering_sequence,
                "device_id": list(bytes.fromhex(self._advertised_device_id())),
                "publication": {
                    "kind": "metering",
                    "tx_levels": self._synthetic_levels(len(self._config.tx_channels), 0.0, now),
                    "rx_levels": self._synthetic_levels(len(self._config.rx_channels), math.pi, now),
                },
            }
        )
        self._metering_sequence = (self._metering_sequence + 1) & 0xFFFF

        for ipv4, port in {(ipv4, port) for ipv4, port, _ in self._metering_destinations.values()}:
            self._metering_transport.sendto(packet, (ipv4, port))

    async def _start_responders(self) -> None:
        loop = asyncio.get_running_loop()
        ports = [self._config.arc_port, self._config.control_port]
        if self._config.arc_port == DEVICE_ARC_PORT:
            ports.insert(1, DEVICE_ARC_SECONDARY_PORT)

        for port in ports:
            try:
                transport, _ = await loop.create_datagram_endpoint(
                    lambda: _ARCProtocol(self),
                    local_addr=("0.0.0.0", port),
                    family=socket.AF_INET,
                )
                self._transports.append(transport)
                if port == self._config.arc_port:
                    self._arc_port = port
                logger.info(f"Responder listening on 0.0.0.0:{port}")
            except OSError as e:
                logger.warning(f"Could not bind port {port}: {e}")

    async def _register_mdns(self) -> None:
        if self._local_ip is None:
            raise RuntimeError("A local address is required before registering discovery services")

        advertisements = core.virtual_device_advertisements(
            {
                "name": self._config.name,
                "model": self._config.model,
                "manufacturer": self._config.manufacturer,
                "address": self._local_ip,
                "arc_port": self._arc_port,
                "control_port": self._config.control_port,
                "process_id": self._config.process_id,
                "tx_channels": self._config.tx_channels,
                "sample_rate": self._config.sample_rate,
                "encoding": self._config.encoding,
                "supported_encodings": self._config.supported_encodings,
                "configured_latency_ns": self._config.configured_latency_ns,
            }
        )
        self._service_infos = [
            ServiceInfo(
                advertisement["service_type"],
                advertisement["name"],
                addresses=[bytes(advertisement["address"])],
                port=advertisement["port"],
                properties=advertisement["properties"],
                server=advertisement["server"],
            )
            for advertisement in advertisements
        ]

        self._zeroconf = AsyncZeroconf(
            interfaces=[self._local_ip],
            ip_version=IPVersion.V4Only,
        )

        await asyncio.gather(*(self._zeroconf.async_register_service(info) for info in self._service_infos))

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
        packet = self._build_publication(
            {
                "kind": "heartbeat",
                "tx_count": len(self._config.tx_channels),
                "rx_count": len(self._config.rx_channels),
            }
        )

        self._send_publication_packet(packet, MULTICAST_GROUP_HEARTBEAT, DEVICE_HEARTBEAT_PORT)


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
        self._device._handle_request(data, addr, settings_only=True)
