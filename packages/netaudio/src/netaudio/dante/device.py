from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import struct
import time

from netaudio.dante.channel import DanteChannel
from netaudio.dante.const import (
    DEVICE_INFO_PORT,
    DEVICE_SETTINGS_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
    SERVICE_ARC,
)
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_operations import DanteDeviceOperations
from netaudio.dante.device_parser import DanteDeviceParser
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.latency import latency_controls_from_settings, standard_latency_choices_for_range
from netaudio.dante.subscription import DanteSubscription

logger = logging.getLogger("netaudio")


class DanteDevice:
    def __init__(self, server_name="", dump_payloads=False, debug=False, app=None):
        self.bluetooth_device = None
        self.is_locked = None
        self.dante_model = ""
        self.dante_model_id = ""
        self.error = None
        self._ipv4 = None
        self.latency = None
        self.active_latency: float | None = None
        self.configured_latency: float | None = None
        self.default_latency: float | None = None
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
        self.supported_sample_rates: list[int] | None = None
        self.aes67_configured = None
        self.aes67_current = None
        self.preferred_leader = None
        self.ptp_v1_role = None
        self.server_name = server_name
        self.services = {}
        self.sockets = {}
        self.software: str | None = None
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
        self.supported_encodings: list[int] | None = None
        self.bit_depth: int | None = None
        self.software_version: str | None = None
        self.firmware_version: str | None = None
        self.clock_role: str | None = None
        self.clock_mac: str | None = None
        self.min_latency: float | None = None
        self.max_latency: float | None = None
        self.product_version: str | None = None
        self.board_name: str | None = None
        self.interfaces: list[dict] | None = None
        self.interface_reboot_required: bool = False
        self.interface_pending_config: dict | None = None

        self._app = app
        self._core = None
        self._core_key = None

        self.commands = DanteDeviceCommands()
        self.parser = DanteDeviceParser()
        self.operations = DanteDeviceOperations(self)

    def __getstate__(self):
        state = self.__dict__.copy()
        for key in ("_core", "_core_key", "_app", "commands", "parser", "operations", "sockets"):
            state.pop(key, None)
        state["error"] = str(self.error) if self.error else None
        return state

    def __setstate__(self, state):
        type(self).__init__(self, server_name=state.get("server_name", ""))
        self.__dict__.update(state)

    @property
    def ipv4(self):
        return self._ipv4

    @ipv4.setter
    def ipv4(self, value):
        self._ipv4 = ipaddress.ip_address(value) if value is not None else None

    @property
    def standard_latency_choices(self):
        return standard_latency_choices_for_range(self.min_latency, self.max_latency)

    def update_last_seen(self):
        self.last_seen = time.time()

    def __str__(self):
        return f"{self.name}"

    def get_service(self, service_type):
        if not self.services:
            return None
        for service in self.services.values():
            if service and service.get("type") == service_type:
                return service
        return None

    def _arc_port(self) -> int:
        service = self.get_service(SERVICE_ARC)
        if service and service.get("port"):
            return service["port"]
        return 4440

    def _resolve_target_port(self, service_type, port):
        if port:
            return port
        if service_type:
            service = self.get_service(service_type)
            if service and service.get("port"):
                return service["port"]
        return None

    def _core_client(self):
        from netaudio import core

        ip = str(self.ipv4) if self.ipv4 else None
        if not ip:
            return None
        arc_port = self._arc_port()
        key = (ip, arc_port)
        if self._core is None or self._core_key != key:
            if self._core is not None:
                self._core.close()
            self._core = core.CoreClient(ip, arc_port=arc_port)
            mac = core.host_mac()
            if mac:
                self._core.set_host_mac(mac)
            observer = self._app.core_observer if self._app is not None else None
            if observer is not None:
                self._core.observer = observer
            self._core_key = key
        return self._core

    async def dante_send_command(self, command, service_type=None, port=None):
        client = self._core_client()
        target_port = self._resolve_target_port(service_type, port)
        if client is None or target_port is None:
            return
        await asyncio.to_thread(client.request, command, target_port, False, 1, 0)

    async def dante_command(
        self,
        command,
        service_type=None,
        port=None,
        logical_command_name: str = "unknown",
    ):
        client = self._core_client()
        target_port = self._resolve_target_port(service_type, port)
        if client is None or target_port is None:
            return None
        return await asyncio.to_thread(client.request, command, target_port, True)

    def _build_rx_from_records(self, records):
        rx_channels = {}
        subscriptions = []
        for record in records:
            channel = DanteChannel()
            channel.channel_type = "rx"
            channel.device = self
            channel.name = record["rx_channel_name"]
            channel.number = record["number"]
            channel.status_code = record["rx_status_code"]
            rx_channels[record["number"]] = channel

            subscription = DanteSubscription()
            subscription.rx_channel_name = record["rx_channel_name"]
            subscription.rx_device_name = self.name
            subscription.tx_channel_name = record["tx_channel_name"]
            subscription.status_code = record["subscription_status_code"]
            subscription.rx_channel_status_code = record["rx_status_code"]
            tx_device_name = record["tx_device_name"]
            subscription.tx_device_name = self.name if tx_device_name == "." else tx_device_name
            subscriptions.append(subscription)
        return rx_channels, subscriptions

    def _build_tx_from_records(self, records):
        tx_channels = {}
        for record in records:
            channel = DanteChannel()
            channel.channel_type = "tx"
            channel.device = self
            channel.number = record["number"]
            channel.name = record["name"]
            channel.friendly_name = record["friendly_name"]
            tx_channels[record["number"]] = channel
        return tx_channels

    async def get_rx_channels(self):
        client = self._core_client()
        if client is None:
            return
        if client.observer is not None:
            from netaudio._capture import fetch_rx_records

            records = await asyncio.to_thread(fetch_rx_records, client, self._arc_port())
        else:
            records = await asyncio.to_thread(client.get_rx_channels)
        self.rx_channels, self.subscriptions = self._build_rx_from_records(records)

    async def get_tx_channels(self):
        client = self._core_client()
        if client is None:
            return
        if client.observer is not None:
            from netaudio._capture import _query, fetch_tx_records

            if self.tx_count is None:
                channel_counts = await asyncio.to_thread(
                    _query,
                    client,
                    {"command": "channel_count"},
                    self._arc_port(),
                    "channel_count",
                )
                if channel_counts is not None:
                    self.tx_count = channel_counts["tx_count"]
                    self.rx_count = channel_counts["rx_count"]

            records = await asyncio.to_thread(fetch_tx_records, client, self._arc_port(), self.tx_count or 0)
        else:
            records = await asyncio.to_thread(client.get_tx_channels)
        self.tx_channels = self._build_tx_from_records(records)

    async def fetch_device_name(self):
        client = self._core_client()
        if client is None:
            return None
        if client.observer is not None:
            from netaudio._capture import fetch_device_name

            return await asyncio.to_thread(fetch_device_name, client, self._arc_port())
        return await asyncio.to_thread(client.get_device_name)

    async def fetch_controls_data(self):
        client = self._core_client()
        if client is None:
            return None

        if client.observer is not None:
            from netaudio._capture import _fetch_instrumented

            raw = await asyncio.to_thread(_fetch_instrumented, client, self._arc_port())
            return self.controls_data_from_core(raw)

        def _work():
            result = {
                "name": client.get_device_name(),
                "counts": client.get_channel_count(),
                "rx": client.get_rx_channels(),
                "tx": client.get_tx_channels(),
            }
            from netaudio.core import NetaudioCoreError

            try:
                result["settings"] = client.get_device_settings()
            except NetaudioCoreError:
                result["settings"] = None
            try:
                result["aes67"] = client.get_aes67_configured()
            except NetaudioCoreError:
                result["aes67"] = None
            return result

        return self.controls_data_from_core(await asyncio.to_thread(_work))

    def controls_data_from_core(self, data):
        controls = {}
        if data["name"]:
            controls["name"] = data["name"]
        tx_count, rx_count, locked = data["counts"]
        controls["tx_count"] = tx_count
        controls["rx_count"] = rx_count
        if locked is not None:
            controls["is_locked"] = locked
        if data.get("aes67") is not None:
            controls["aes67_configured"] = data["aes67"]
        settings_data = data.get("settings")
        if settings_data:
            if settings_data.get("sample_rate"):
                controls["sample_rate"] = settings_data["sample_rate"]
            controls.update(latency_controls_from_settings(settings_data))
        rx_channels, subscriptions = self._build_rx_from_records(data["rx"])
        if rx_channels:
            controls["rx_channels"] = rx_channels
            controls["subscriptions"] = subscriptions
        tx_channels = self._build_tx_from_records(data["tx"])
        if tx_channels:
            controls["tx_channels"] = tx_channels
        return controls

    async def populate_from_core(self):
        controls = await self.fetch_controls_data()
        if controls is None:
            return False
        self.apply_controls(controls)
        return True

    def apply_controls(self, data):
        if data.get("name"):
            self.name = data["name"]
        if data.get("sample_rate"):
            self.sample_rate = data["sample_rate"]
        if "latency" in data:
            self.latency = data["latency"]
        if "active_latency" in data:
            self.active_latency = data["active_latency"]
        if "configured_latency" in data:
            self.configured_latency = data["configured_latency"]
        if "default_latency" in data:
            self.default_latency = data["default_latency"]
        if "min_latency" in data:
            self.min_latency = data["min_latency"]
        if "max_latency" in data:
            self.max_latency = data["max_latency"]
        if "tx_count" in data:
            self.tx_count = self.tx_count_raw = data["tx_count"]
        if "rx_count" in data:
            self.rx_count = self.rx_count_raw = data["rx_count"]
        if "is_locked" in data:
            self.is_locked = data["is_locked"]
        if "aes67_configured" in data:
            self.aes67_configured = data["aes67_configured"]
        if data.get("tx_channels"):
            self.tx_channels = data["tx_channels"]
        if data.get("rx_channels"):
            self.rx_channels = data["rx_channels"]
            self.subscriptions = data.get("subscriptions", [])
        self.error = None

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
        return None

    def to_json(self):
        return DanteDeviceSerializer.to_json(self)
