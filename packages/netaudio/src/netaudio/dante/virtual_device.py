from __future__ import annotations

import asyncio
import logging
import random
import socket
import struct
from dataclasses import dataclass, field
from enum import IntEnum

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
    PROTOCOL_ID,
    RESULT_CODE_SUCCESS,
    SERVICE_ARC,
    SERVICE_CHAN,
    SERVICE_CMC,
)


class Protocol(IntEnum):
    CONTROL = 0x27FF
    SETTINGS = 0xFFFF
    CMC = 0x1200
    AES67_CONFIG = 0x2809


class Opcode(IntEnum):
    CHANNEL_COUNT = 0x1000
    DEVICE_NAME_SET = 0x1001
    DEVICE_NAME = 0x1002
    DEVICE_INFO = 0x1003
    DEVICE_SETTINGS = 0x1100
    DEVICE_SETTINGS_SET = 0x1101
    TX_CHANNELS = 0x2000
    TX_CHANNEL_NAMES = 0x2010
    TX_CHANNEL_NAME_SET = 0x2013
    RX_CHANNELS = 0x3000
    RX_CHANNEL_NAME_SET = 0x3001
    SUBSCRIPTION_ADD = 0x3010
    SUBSCRIPTION_REMOVE = 0x3014


DEVICE_ARC_SECONDARY_PORT = 4455
MCAST_HEADER_LENGTH = 32

logger = logging.getLogger("netaudio")


@dataclass
class VirtualDeviceConfig:
    name: str = "netaudio-virtual"
    model: str = "netaudio"
    manufacturer: str = "netaudio"
    tx_channels: list[str] = field(default_factory=lambda: ["Ch 1", "Ch 2"])
    rx_channels: list[str] = field(default_factory=lambda: ["Ch 1", "Ch 2"])
    sample_rate: int = 48000
    latency_ns: int = 1_000_000
    interface_ip: str | None = None


class VirtualDevice:
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
        except Exception:
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

    async def _start_mcast_server(self) -> None:
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass
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
        mac_hex = self._mac.replace(":", "")
        mac_eui64 = mac_hex[:6] + "fffe" + mac_hex[6:]
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
            chan_info = ServiceInfo(
                SERVICE_CHAN,
                f"{ch_name}@{self._config.name}.{SERVICE_CHAN}",
                addresses=[ip_bytes],
                port=DEVICE_ARC_SECONDARY_PORT,
                properties={
                    "txtvers": "2",
                    "dbcp1": "0x1102",
                    "dbcp": "0x1004",
                    "id": str(i + 1),
                    "rate": str(self._config.sample_rate),
                    "pcm": "3 0xe",
                    "enc": "24",
                    "en": "24",
                    "latency_ns": str(self._config.latency_ns),
                    "fpp": "32,2",
                    "nchan": "8",
                },
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
        peaks_block_len = 24 + total_peaks
        while peaks_block_len % 4 != 0:
            peaks_block_len += 1
        hb_content += struct.pack(">HH", 24 + total_peaks, 0x8002)
        hb_content += struct.pack(">HH", 4, 12 + total_peaks)
        hb_content += struct.pack(">HH", ctr, 0)
        hb_content += struct.pack(">HH", tx_count, 0)
        hb_content += struct.pack(">HH", rx_count, 0)
        hb_content += struct.pack(">HH", 24, 0)
        hb_content += bytes(total_peaks)
        while len(hb_content) % 4 != 0:
            hb_content += b"\x00"

        self._mcast_send(
            MULTICAST_GROUP_HEARTBEAT,
            DEVICE_HEARTBEAT_PORT,
            0xFFFE,
            bytes([0, 8, 0, 1, 0x10, 0, 0, 0]),
            bytes(hb_content),
        )
        logger.debug(f"Heartbeat sent ({len(hb_content) + MCAST_HEADER_LENGTH}B)")

    def _handle_request(self, data: bytes, addr: tuple[str, int]) -> bytes | None:
        if len(data) < 10:
            return None

        start_code = struct.unpack(">H", data[:2])[0]

        if start_code in (0xFFFF, 0xFFFE) and len(data) >= MCAST_HEADER_LENGTH:
            return self._handle_mcast_format_request(data, addr)

        length, seqnum, opcode1, opcode2 = struct.unpack(">HHHH", data[2:10])
        content = data[10:]

        if start_code in (PROTOCOL_ID, 0x2729, 0x2809):
            handler = self._opcode_handlers.get(opcode1)
            if handler:
                return handler(self, seqnum, data, start_code)
            logger.debug(f"Unhandled ARC opcode 0x{opcode1:04x} from {addr}")
            return None

        if start_code == Protocol.AES67_CONFIG:
            return self._handle_aes67_config(start_code, seqnum, opcode1, content)

        if opcode1 == 0x1001 and opcode2 == 0:
            return self._handle_cmc_advertisement(start_code, seqnum, opcode1)

        logger.debug(
            f"Unhandled packet from {addr}: start=0x{start_code:04x} "
            f"op1=0x{opcode1:04x} op2=0x{opcode2:04x} len={length} "
            f"data={data[: min(32, len(data))].hex()}"
        )
        return None

    def _handle_mcast_format_request(self, data: bytes, addr: tuple[str, int]) -> None:
        opcode = data[24:32]
        info_type = opcode[3]
        logger.debug(f"Mcast-format request from {addr}: opcode={opcode.hex()} info_type=0x{info_type:02x}")

        if info_type == 0x61:
            self._send_mcast_board_info()
            self._send_unicast_from_settings(
                addr, bytes([0x07, 0x2A, 0x00, 0x60, 0, 0, 0, 0]), self._build_board_info_content()
            )
        elif info_type == 0xC1:
            self._send_mcast_product_info()
            self._send_unicast_from_settings(
                addr, bytes([0x07, 0x2A, 0x00, 0xC0, 0, 0, 0, 0]), self._build_product_info_content()
            )
        elif info_type == 0x21:
            self._send_mcast_clock_stats()
        elif info_type == 0x13:
            self._send_mcast_network_info()
        elif info_type == 0x77:
            self._mcast_send(
                MULTICAST_GROUP_CONTROL_MONITORING,
                DEVICE_INFO_PORT,
                0xFFFF,
                bytes([0x07, 0x2A, 0x00, 0x78, 0, 0, 0, 0]),
                bytes([0, 0, 0, 3, 0, 0, 0, 0]),
            )
        else:
            logger.debug(f"Unhandled mcast info_type 0x{info_type:02x} from {addr}")

    def _send_unicast_from_settings(self, addr: tuple[str, int], opcode: bytes, content: bytes) -> None:
        if self._mcast_transport:
            packet = self._build_mcast_packet(0xFFFF, opcode, content)
            self._mcast_transport.sendto(packet, addr)
            logger.debug(f"Sent unicast response to {addr} from port 8700 ({len(packet)}B)")

    def _build_board_info_content(self) -> bytes:
        content = bytearray(200)
        content[0:4] = bytes([4, 1, 0, 6])
        content[0x23] = 2
        content[4:8] = bytes([4, 1, 0, 3])
        content[0x27] = 1
        content[0x28:0x2C] = bytes([1, 0, 0, 0])
        content[0x14] = 0
        content[0x15] = 0
        content[0x16] = 0x10
        content[0x17] = 0
        content[0xBB] = 0x1F
        board_name = self._config.name.encode("utf-8")[:8]
        content[12 : 12 + len(board_name)] = board_name
        content[0x38 : 0x38 + min(len(board_name), 16)] = board_name[:16]
        return bytes(content)

    def _build_product_info_content(self) -> bytes:
        content = bytearray(336)
        mfr = self._config.manufacturer.encode("utf-8")
        model = self._config.model.encode("utf-8")
        board = self._config.name.encode("utf-8")
        content[0 : min(8, len(mfr))] = mfr[:8]
        content[8 : 8 + min(8, len(board))] = board[:8]
        content[0x2C : 0x2C + min(16, len(mfr))] = mfr[:16]
        content[0xAC : 0xAC + min(16, len(model))] = model[:16]
        content[0x1C:0x20] = bytes([0, 1, 0, 0])
        return bytes(content)

    def _handle_cmc_advertisement(self, start_code: int, seqnum: int, opcode1: int) -> bytes:
        ip_bytes = socket.inet_aton(self._local_ip) if self._local_ip else b"\x00\x00\x00\x00"
        device_id = b"\x00\x00" + ip_bytes + b"\x00\x00"

        body = struct.pack(">H", 0x0000)
        body += device_id
        body += struct.pack(">H", 1)
        body += struct.pack(">H", 0)
        body += ip_bytes
        body += struct.pack(">H", DEVICE_SETTINGS_PORT)
        body += struct.pack(">H", 0)

        length = 10 + len(body)
        header = struct.pack(">HHHHH", start_code, length, seqnum, opcode1, RESULT_CODE_SUCCESS)
        return header + body

    def _handle_aes67_config(self, start_code: int, seqnum: int, opcode1: int, content: bytes) -> bytes:
        body = b"\x63\x00\x01"
        length = 10 + len(body)
        header = struct.pack(">HHHHH", start_code, length, seqnum, opcode1, RESULT_CODE_SUCCESS)
        return header + body

    def _build_response(self, transaction_id: int, opcode: int, body: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        length = 10 + len(body)
        header = struct.pack(">HHHHH", protocol_id, length, transaction_id, opcode, RESULT_CODE_SUCCESS)
        return header + body

    def _handle_device_name(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        name_bytes = self._config.name.encode("utf-8") + b"\x00"
        return self._build_response(transaction_id, Opcode.DEVICE_NAME, name_bytes, protocol_id)

    def _handle_channel_count(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        tx_count = len(self._config.tx_channels)
        rx_count = len(self._config.rx_channels)
        body = struct.pack(">BB", 0, 0x30)
        body += struct.pack(">HH", tx_count, rx_count)
        body += struct.pack(">H", 4)
        body += struct.pack(">H", 8)
        body += struct.pack(">H", 8)
        body += struct.pack(">HH", 32, 32)
        body += struct.pack(">H", tx_count + rx_count)
        body += struct.pack(">HH", 1, 1)
        body += bytes(12)
        return self._build_response(transaction_id, Opcode.CHANNEL_COUNT, body, protocol_id)

    def _handle_device_info(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        fixed_header_size = 34
        strings_base = 10 + fixed_header_size

        board_name = self._config.model.encode("utf-8") + b"\x00"
        revision_str = b"1.0.0\x00"
        friendly_hostname = self._config.name.encode("utf-8") + b"\x00"
        mac_hex = self._mac.replace(":", "")
        factory_hostname = f"netaudio-{mac_hex}".encode("utf-8") + b"\x00"

        board_name_offset = strings_base
        revision_offset = board_name_offset + len(board_name)
        friendly_hostname_offset = revision_offset + len(revision_str)
        factory_hostname_offset = friendly_hostname_offset + len(friendly_hostname)

        body = bytearray(fixed_header_size)
        struct.pack_into(">H", body, 0, 0)
        struct.pack_into(">H", body, 2, 0)
        struct.pack_into(">H", body, 4, 0)
        struct.pack_into(">H", body, 6, board_name_offset)
        struct.pack_into(">H", body, 8, revision_offset)
        struct.pack_into(">H", body, 10, 0x0500)
        struct.pack_into(">H", body, 12, friendly_hostname_offset)
        struct.pack_into(">H", body, 14, factory_hostname_offset)
        struct.pack_into(">H", body, 16, friendly_hostname_offset)
        body[18:30] = bytes(12)
        struct.pack_into(">H", body, 30, 0x2729)
        struct.pack_into(">H", body, 32, 0)

        body += board_name + revision_str + friendly_hostname + factory_hostname

        return self._build_response(transaction_id, Opcode.DEVICE_INFO, bytes(body), protocol_id)

    def _handle_tx_channels(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        channels = self._config.tx_channels
        num_ch = len(channels)
        record_size = 8
        header_size = 10
        body_header_size = 2

        sample_rate_area_offset = header_size + body_header_size + (num_ch * record_size)
        metadata = struct.pack(">I", self._config.sample_rate)
        metadata += bytes([0x01, 0x01, 0x00, 0x18, 0x04, 0x00, 0x00, 0x18, 0x00, 0x18, 0x00, 0x0E])
        string_area_offset = sample_rate_area_offset + len(metadata)

        name_offsets = []
        string_table = bytearray()
        for ch_name in channels:
            name_offsets.append(string_area_offset + len(string_table))
            string_table += ch_name.encode("utf-8") + b"\x00"

        body = struct.pack(">BB", 0x02, num_ch)
        for i in range(num_ch):
            body += struct.pack(
                ">HHHH",
                i + 1,
                0x0007,
                sample_rate_area_offset,
                name_offsets[i],
            )
        body += metadata + bytes(string_table)

        return self._build_response(transaction_id, Opcode.TX_CHANNELS, body, protocol_id)

    def _handle_tx_channel_names(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        channels = self._config.tx_channels
        num_ch = len(channels)
        record_size = 6
        header_size = 10
        body_header_size = 2

        string_area_offset = header_size + body_header_size + (num_ch * record_size)
        pad_to_align = (4 - (string_area_offset % 4)) % 4
        string_area_offset += pad_to_align

        name_offsets = []
        string_table = bytearray()
        for ch_name in channels:
            name_offsets.append(string_area_offset + len(string_table))
            string_table += ch_name.encode("utf-8") + b"\x00"

        body = struct.pack(">BB", 0x02, num_ch)
        for i in range(num_ch):
            body += struct.pack(">HHH", i + 1, i + 1, name_offsets[i])
        body += bytes(pad_to_align)
        body += bytes(string_table)

        return self._build_response(transaction_id, 0x2010, body, protocol_id)

    def _handle_rx_channels(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        channels = self._config.rx_channels
        num_ch = len(channels)
        record_size = 20
        header_size = 10
        body_header_size = 2

        sample_rate_area_offset = header_size + body_header_size + (num_ch * record_size)
        metadata = struct.pack(">I", self._config.sample_rate)
        metadata += bytes([0x01, 0x01, 0x00, 0x18, 0x04, 0x00, 0x00, 0x18, 0x00, 0x18, 0x00, 0x0E])

        string_table = bytearray()
        string_base = sample_rate_area_offset + len(metadata)

        name_offsets = []
        src_ch_offsets = []
        src_dev_offsets = []
        rx_status_codes = []
        sub_status_codes = []

        for i, ch_name in enumerate(channels):
            ch_num = i + 1
            name_offsets.append(string_base + len(string_table))
            string_table += ch_name.encode("utf-8") + b"\x00"

            sub = self._subscriptions.get(ch_num)
            if sub:
                src_ch_name, src_dev_name = sub
                src_ch_offsets.append(string_base + len(string_table))
                string_table += src_ch_name.encode("utf-8") + b"\x00"
                src_dev_offsets.append(string_base + len(string_table))
                string_table += src_dev_name.encode("utf-8") + b"\x00"
                rx_status_codes.append(0x0000)
                sub_status_codes.append(0x0009)
            else:
                src_ch_offsets.append(0)
                src_dev_offsets.append(0)
                rx_status_codes.append(0x0000)
                sub_status_codes.append(0x0000)

        body = struct.pack(">BB", 0x02, num_ch)
        for i in range(num_ch):
            body += struct.pack(
                ">HHHHHH",
                i + 1,
                0x0006,
                sample_rate_area_offset,
                src_ch_offsets[i],
                src_dev_offsets[i],
                name_offsets[i],
            )
            body += struct.pack(">HH", rx_status_codes[i], sub_status_codes[i])
            body += struct.pack(">I", 0)
        body += metadata + bytes(string_table)

        return self._build_response(transaction_id, Opcode.RX_CHANNELS, body, protocol_id)

    def _handle_property_directory(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        properties = [
            (0x8020, 0x0001),
            (0x8021, 0x0003),
            (0x0022, 0x0003),
            (0x0023, 0x0003),
            (0x0024, 0x0001),
            (0x8060, 0x0003),
            (0x0062, 0x0003),
            (0x0063, 0x0001),
            (0x0201, 0x0003),
            (0x8204, 0x0003),
            (0x8205, 0x0003),
            (0x020A, 0x0001),
            (0x020B, 0x0001),
            (0x0210, 0x0003),
            (0x0211, 0x0003),
            (0x0212, 0x0003),
            (0x0213, 0x0001),
            (0x0214, 0x0001),
            (0x0222, 0x0003),
            (0x8301, 0x0003),
            (0x8306, 0x0001),
            (0x8302, 0x0001),
            (0x8321, 0x0001),
            (0x0310, 0x0001),
            (0x0311, 0x0001),
            (0x0312, 0x0001),
            (0x0303, 0x0003),
            (0x83F0, 0x0001),
            (0x0601, 0x0001),
            (0x0309, 0x0001),
            (0x0209, 0x0001),
        ]
        body = struct.pack(">H", len(properties))
        for prop_id, flags in properties:
            body += struct.pack(">HH", prop_id, flags)
        return self._build_response(transaction_id, 0x1102, body, protocol_id)

    def _handle_device_settings(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        settings = [
            (0x8020, self._config.sample_rate),
            (0x8204, 1_000_000),
            (0x8205, self._config.latency_ns),
            (0x8302, 21_333_334),
            (0x8306, 150_000),
        ]
        first_value_offset = 10 + 2 + len(settings) * 4
        body = struct.pack(">BB", 2, len(settings))
        for index, (info_code, _) in enumerate(settings):
            body += struct.pack(">HH", info_code, first_value_offset + index * 4)
        for _, value in settings:
            body += struct.pack(">I", value)

        return self._build_response(transaction_id, Opcode.DEVICE_SETTINGS, body, protocol_id)

    def _handle_tx_flows(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = struct.pack(">BB", 0x02, 0x00)
        return self._build_response(transaction_id, 0x2200, body, protocol_id)

    def _handle_rx_flows(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = struct.pack(">BB", 0x02, 0x00)
        return self._build_response(transaction_id, 0x3200, body, protocol_id)

    def _handle_rx_subscriptions(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = bytes([0x38, 0x00, 0x38, 0xFD, 0x38, 0xFE, 0x38, 0xFF])
        return self._build_response(transaction_id, 0x3300, body, protocol_id)

    def _handle_tx_flow_labels(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = struct.pack(">BB", 0x02, 0x00)
        return self._build_response(transaction_id, 0x2204, body, protocol_id)

    def _handle_channel_name_set(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        opcode = struct.unpack(">H", data[6:8])[0]
        body = data[10:]
        if len(body) >= 4:
            ch_num = struct.unpack(">H", body[0:2])[0]
            name_off = struct.unpack(">H", body[2:4])[0]
            new_name = self._get_string_from_data(data, name_off)
            if opcode == Opcode.TX_CHANNEL_NAME_SET:
                idx = ch_num - 1
                if 0 <= idx < len(self._config.tx_channels):
                    self._config.tx_channels[idx] = new_name
                    logger.info(f"Renamed TX ch {ch_num} -> '{new_name}'")
            elif opcode == Opcode.RX_CHANNEL_NAME_SET:
                idx = ch_num - 1
                if 0 <= idx < len(self._config.rx_channels):
                    self._config.rx_channels[idx] = new_name
                    logger.info(f"Renamed RX ch {ch_num} -> '{new_name}'")
        return self._build_response(transaction_id, opcode, b"", protocol_id)

    def _handle_subscription_add(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = data[10:]
        if len(body) < 8:
            return self._build_response(transaction_id, 0x3010, b"", protocol_id)

        num_records = body[1]
        offset = 2
        for _ in range(num_records):
            if offset + 6 > len(body):
                break
            ch_num, src_ch_off, src_dev_off = struct.unpack(">HHH", body[offset : offset + 6])
            offset += 6
            if ch_num == 0:
                continue
            if src_ch_off == 0 and src_dev_off == 0:
                self._subscriptions.pop(ch_num, None)
                logger.info(f"Unsubscribed RX ch {ch_num}")
            else:
                src_ch_name = self._get_string_from_data(data, src_ch_off)
                src_dev_name = self._get_string_from_data(data, src_dev_off)
                self._subscriptions[ch_num] = (src_ch_name, src_dev_name)
                logger.info(f"Subscribed RX ch {ch_num} <- {src_ch_name}@{src_dev_name}")

        return self._build_response(transaction_id, 0x3010, b"", protocol_id)

    def _handle_subscription_remove(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = data[10:]
        if len(body) >= 2:
            ch_num = struct.unpack(">H", body[0:2])[0]
            self._subscriptions.pop(ch_num, None)
            logger.info(f"Unsubscribed RX ch {ch_num}")
        return self._build_response(transaction_id, 0x3014, b"", protocol_id)

    def _get_string_from_data(self, data: bytes, offset: int) -> str:
        if offset == 0 or offset >= len(data):
            return ""
        end = data.index(0, offset) if 0 in data[offset:] else len(data)
        return data[offset:end].decode("utf-8", errors="replace")

    def _handle_unsupported(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        opcode = struct.unpack(">H", data[6:8])[0]
        length = 10
        header = struct.pack(">HHHHH", protocol_id, length, transaction_id, opcode, 0x0030)
        return header

    _opcode_handlers = {
        Opcode.DEVICE_NAME: _handle_device_name,
        Opcode.CHANNEL_COUNT: _handle_channel_count,
        Opcode.DEVICE_INFO: _handle_device_info,
        Opcode.TX_CHANNELS: _handle_tx_channels,
        Opcode.TX_CHANNEL_NAMES: _handle_tx_channel_names,
        Opcode.RX_CHANNELS: _handle_rx_channels,
        Opcode.DEVICE_SETTINGS: _handle_device_settings,
        0x1102: _handle_property_directory,
        0x2200: _handle_tx_flows,
        0x2204: _handle_tx_flow_labels,
        0x2320: _handle_unsupported,
        Opcode.TX_CHANNEL_NAME_SET: _handle_channel_name_set,
        Opcode.RX_CHANNEL_NAME_SET: _handle_channel_name_set,
        0x3010: _handle_subscription_add,
        0x3014: _handle_subscription_remove,
        0x3200: _handle_rx_flows,
        0x3300: _handle_rx_subscriptions,
    }


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

        if self._device._local_ip and addr[0] == self._device._local_ip:
            return

        if len(data) < MCAST_HEADER_LENGTH:
            return

        opcode = data[24:32]

        info_type = opcode[3] if opcode[0] == 0x07 and opcode[2] == 0 else None

        if info_type == 0x61:
            logger.debug(f"Mcast request: board_info from {addr}")
            self._device._send_mcast_board_info()
            self._device._send_unicast_from_settings(
                addr, bytes([0x07, 0x2A, 0x00, 0x60, 0, 0, 0, 0]), self._device._build_board_info_content()
            )
        elif info_type == 0xC1:
            logger.debug(f"Mcast request: product_info from {addr}")
            self._device._send_mcast_product_info()
            self._device._send_unicast_from_settings(
                addr, bytes([0x07, 0x2A, 0x00, 0xC0, 0, 0, 0, 0]), self._device._build_product_info_content()
            )
        elif info_type == 0x21:
            logger.debug(f"Mcast request: clock_stats from {addr}")
            self._device._send_mcast_clock_stats()
        elif info_type == 0x13:
            logger.debug(f"Mcast request: network_info from {addr}")
            self._device._send_mcast_network_info()
        elif info_type == 0x77:
            logger.debug(f"Mcast request: capability from {addr}")
            self._device._mcast_send(
                MULTICAST_GROUP_CONTROL_MONITORING,
                DEVICE_INFO_PORT,
                0xFFFF,
                bytes([0x07, 0x2A, 0x00, 0x78, 0, 0, 0, 0]),
                bytes([0, 0, 0, 3, 0, 0, 0, 0]),
            )
