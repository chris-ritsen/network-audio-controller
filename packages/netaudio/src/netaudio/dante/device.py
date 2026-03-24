import asyncio
import ipaddress
import logging
import socket
import struct
import time
import traceback
import warnings

from netaudio.dante.channel import DanteChannel
from netaudio.dante.const import (
    DEVICE_CONTROL_PORT,
    DEVICE_INFO_PORT,
    DEVICE_SETTINGS_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
    PORTS,
    SERVICE_ARC,
    SERVICE_CHAN,
)
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_network import DanteDeviceNetwork
from netaudio.dante.device_operations import DanteDeviceOperations
from netaudio.dante.device_parser import DanteDeviceParser
from netaudio.dante.device_protocol import DanteDeviceProtocol
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.subscription import DanteSubscription

logger = logging.getLogger("netaudio")
sockets = {}


class DanteDevice:
    def __init__(self, server_name="", dump_payloads=False, debug=False, app=None):
        self.bluetooth_device = None
        self.is_locked = None
        self.dante_model = ""
        self.dante_model_id = ""
        self.error = None
        self._ipv4 = None
        self.latency = None
        self.mac_address = None
        self.manufacturer = ""
        self.manufacturer_mdns = ""
        self.model = ""
        self.model_id = ""
        self.name = ""
        self.rx_channels = {}
        self.rx_count = None
        self.rx_count_raw = None
        self.sample_rate = None
        self.aes67_enabled = None
        self.server_name = server_name
        self.services = {}
        self.sockets = {}
        self.software = None
        self.subscriptions = []
        self.tx_channels = {}
        self.tx_count = None
        self.tx_count_raw = None
        self.online: bool = True
        self.last_seen: float | None = None
        self.tx_flow_count: int | None = None
        self.rx_flow_count: int | None = None
        self.flow_protocol_id: int | None = None
        self.num_networks: int | None = None
        self.encoding: int | None = None
        self.bit_depth: int | None = None
        self.software_version: str | None = None
        self.firmware_version: str | None = None
        self.clock_role: str | None = None
        self.clock_mac: str | None = None
        self.min_latency: float | None = None
        self.max_latency: float | None = None
        self.product_version: str | None = None
        self.board_name: str | None = None

        self._app = app

        self.commands = DanteDeviceCommands()
        self.parser = DanteDeviceParser()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            self.protocol = DanteDeviceProtocol(dump_payloads, debug)
            self.network = DanteDeviceNetwork(self)
        self.operations = DanteDeviceOperations(self)

    @property
    def ipv4(self):
        return self._ipv4

    @ipv4.setter
    def ipv4(self, value):
        self._ipv4 = ipaddress.ip_address(value) if value is not None else None

    def update_last_seen(self):
        self.last_seen = time.time()

    def __str__(self):
        return f"{self.name}"

    async def dante_send_command(self, command, service_type=None, port=None):
        sock = None

        if service_type:
            service = self.network.get_service(service_type)
            sock = self.sockets[service["port"]]

        if port:
            sock = self.sockets[port]

        await self.protocol.dante_send_command(command, sock)

    async def dante_command(
        self,
        command,
        service_type=None,
        port=None,
        logical_command_name: str = "unknown",
    ):
        if self._app is not None:
            return await self._dante_command_via_app(
                command, service_type, port, logical_command_name
            )

        sock = None

        if service_type:
            service = self.network.get_service(service_type)

            if service and service["port"] and service["port"] in self.sockets:
                sock = self.sockets[service["port"]]

        if port:
            sock = self.sockets[port]

        return await self.protocol.dante_command(
            command, sock, self.name, self.ipv4, logical_command_name
        )

    async def _dante_command_via_app(
        self,
        command,
        service_type=None,
        port=None,
        logical_command_name: str = "unknown",
    ):
        device_ip = str(self.ipv4) if self.ipv4 else None
        if not device_ip:
            return None

        target_port = port

        if service_type:
            service = self.network.get_service(service_type)
            if service and service.get("port"):
                target_port = service["port"]

        if target_port is None:
            return None

        if target_port == DEVICE_SETTINGS_PORT:
            return await self._app.settings.request(
                command, device_ip, target_port,
                device_name=self.name,
                logical_command_name=logical_command_name,
            )
        elif target_port == DEVICE_CONTROL_PORT:
            return await self._app.cmc.request(
                command, device_ip, target_port,
                device_name=self.name,
                logical_command_name=logical_command_name,
            )
        else:
            return await self._app.arc.request(
                command, device_ip, target_port,
                device_name=self.name,
                logical_command_name=logical_command_name,
            )

    async def get_controls(self):
        await self.network.get_controls()

    def parse_volume(self, bytes_volume):
        self.parser.parse_volume(
            bytes_volume,
            self.rx_count_raw,
            self.tx_count_raw,
            self.tx_channels,
            self.rx_channels,
        )

    async def get_volume(self, ipv4, mac, port):
        await self.network.get_volume(ipv4, mac, port)

    async def get_rx_channels(self):
        rx_channels, subscriptions = await self.parser.get_rx_channels(
            self, self.dante_command
        )
        self.rx_channels = rx_channels
        self.subscriptions = subscriptions

    async def get_tx_channels(self):
        tx_channels = await self.parser.get_tx_channels(self, self.dante_command)
        self.tx_channels = tx_channels

    async def get_bluetooth_status(self, host_mac=None):
        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac
            host_mac = _get_host_mac()
        packet, _, _ = self.commands.command_bluetooth_status(host_mac=host_mac)
        device_ip = str(self.ipv4)

        def _query():
            mcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            mcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                mcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            mcast_sock.bind(("", DEVICE_INFO_PORT))
            mreq = struct.pack(
                "4s4s",
                socket.inet_aton(MULTICAST_GROUP_CONTROL_MONITORING),
                socket.inet_aton("0.0.0.0"),
            )
            mcast_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            mcast_sock.settimeout(2)

            send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            send_sock.bind(("", DEVICE_SETTINGS_PORT))

            try:
                send_sock.sendto(packet, (device_ip, DEVICE_SETTINGS_PORT))
                while True:
                    data, addr = mcast_sock.recvfrom(4096)
                    if addr[0] == device_ip:
                        return data
            finally:
                send_sock.close()
                mcast_sock.close()

        try:
            response = await asyncio.to_thread(_query)
            name = self.parser.parse_bluetooth_status(response)
            self.bluetooth_device = name
            return name
        except (TimeoutError, socket.timeout):
            logger.debug(f"Timeout waiting for bluetooth status from {self.name}")
            self.bluetooth_device = None
            return None

    async def get_clocking_status(self, host_mac=None):
        if (
            not hasattr(self.commands, "command_clocking_status")
            or not hasattr(self.parser, "parse_clocking_status")
        ):
            return None

        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac
            host_mac = _get_host_mac()
        packet, _, _ = self.commands.command_clocking_status(host_mac=host_mac)
        device_ip = str(self.ipv4)

        def _query():
            mcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            mcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                mcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            mcast_sock.bind(("", DEVICE_INFO_PORT))
            mreq = struct.pack(
                "4s4s",
                socket.inet_aton(MULTICAST_GROUP_CONTROL_MONITORING),
                socket.inet_aton("0.0.0.0"),
            )
            mcast_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            mcast_sock.settimeout(2)

            send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            send_sock.bind(("", DEVICE_SETTINGS_PORT))

            try:
                send_sock.sendto(packet, (device_ip, DEVICE_SETTINGS_PORT))
                while True:
                    data, addr = mcast_sock.recvfrom(4096)
                    if addr[0] == device_ip:
                        return data
            finally:
                send_sock.close()
                mcast_sock.close()

        try:
            response = await asyncio.to_thread(_query)
            result = self.parser.parse_clocking_status(response)
            if result:
                self.clock_role = result["clock_role"]
                self.clock_mac = result["device_clock_mac"]
            return result
        except (TimeoutError, socket.timeout):
            logger.debug(f"Timeout waiting for clocking status from {self.name}")
            return None

    def to_json(self):
        return DanteDeviceSerializer.to_json(self)
