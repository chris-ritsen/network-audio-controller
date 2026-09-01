from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import struct
import time

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.channel import DanteChannel
from netaudio.dante.clock_identity import canonical_clock_identity
from netaudio.dante.const import (
    DEVICE_INFO_PORT,
    DEVICE_SETTINGS_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
    SERVICE_ARC,
)
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_kind import device_kind
from netaudio.dante.device_operations import DanteDeviceOperations
from netaudio.dante.device_parser import DanteDeviceParser
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.gain import gain_channel_type, gain_level_choices, gain_level_label
from netaudio.dante.latency import latency_controls_from_settings, standard_latency_choices_for_range
from netaudio.dante.subscription import DanteSubscription

logger = logging.getLogger("netaudio")

AES67_MULTICAST_PREFIX_PROPERTY_ID = 0x8060


def device_advertises_aes67_multicast_prefix(device) -> bool:
    if getattr(device, "aes67_multicast_prefix", None):
        return True
    for entry in getattr(device, "settings_properties", None) or []:
        if isinstance(entry, dict) and entry.get("property_id") == AES67_MULTICAST_PREFIX_PROPERTY_ID:
            return True
    return False


class DanteDevice:
    def __init__(self, server_name="", dump_payloads=False, debug=False, app=None):
        self.bluetooth_device = None
        self.bluetooth_connected: bool | None = None
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
        self.routing_capacity_receive_channel_count: int | None = None
        self.routing_capacity_transmit_channel_count: int | None = None
        self.routing_ready: bool | None = None
        self.routing_ready_state_code: int | None = None
        self.sample_rate = None
        self.supported_sample_rates: list[int] | None = None
        self.sample_rate_pullup_raw_value: int | None = None
        self.requested_sample_rate_pullup_raw_value: int | None = None
        self.supported_sample_rate_pullup_raw_values: list[int] | None = None
        self.aes67_configured = None
        self.aes67_current = None
        self.aes67_supported: bool | None = None
        self.aes67_multicast_prefix = None
        self.settings_properties: list[dict] | None = None
        self.preferred_leader = None
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
        self.transmitter_flows = None
        self.rx_flow_count: int | None = None
        self.receiver_flow_latency_nanoseconds: int | None = None
        self.receiver_flows: list[dict] | None = None
        self.flow_protocol_id: int | None = None
        self.receiver_channel_name_protocol_identifier: int | None = None
        self.transmitter_channel_name_protocol_identifier: int | None = None
        self.num_networks: int | None = None
        self.encoding: int | None = None
        self.supported_encodings: list[int] | None = None
        self.gain_device_type: str | None = None
        self.gain_levels: list[int] | None = None
        self.supported_gain_levels: list[int] | None = None
        self.bit_depth: int | None = None
        self.software_version: str | None = None
        self.firmware_version: str | None = None
        self.clock_source_code: int | None = None
        self.clock_subdomain: bytes | None = None
        self.clock_frequency_offset_parts_per_billion: int | None = None
        self.clock_port_state_code: int | None = None
        self.clock_role: str | None = None
        self.clock_port_records: list[dict] | None = None
        self.clock_identity: str | None = None
        self.leader_clock_identity: str | None = None
        self.min_latency: float | None = None
        self.max_latency: float | None = None
        self.product_version: str | None = None
        self.board_name: str | None = None
        self.interfaces: list[dict] | None = None
        self.network_interface_traffic: dict | None = None
        self.receiver_flow_connection_health: dict | None = None
        self.link_speed_mbps: int | None = None
        self.interface_reboot_required: bool = False
        self.interface_pending_config: dict | None = None
        self.lock_reset_status: dict | None = None
        self.clear_configuration_status: dict | None = None
        self.diagnostic_log_export_supported: bool | None = None
        self.license_signature_length_bytes: int | None = None
        self.licensed_receive_channel_count: int | None = None
        self.licensed_transmit_channel_count: int | None = None
        self.licensed_redundancy_enabled: bool | None = None
        self.sample_rate_channel_capacities: list[dict] | None = None
        self.failed_queries: set[str] = set()

        self._app = app
        self._core = None
        self._core_key = None
        self._topology_mutation_lock = DeferredAsyncioLock()

        self.commands = DanteDeviceCommands()
        self.parser = DanteDeviceParser()
        self.operations = DanteDeviceOperations(self)

    def __getstate__(self):
        state = self.__dict__.copy()
        for key in (
            "_core",
            "_core_key",
            "_app",
            "_topology_mutation_lock",
            "commands",
            "parser",
            "operations",
            "sockets",
        ):
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
    def topology_mutation_lock(self) -> DeferredAsyncioLock:
        return self._topology_mutation_lock

    @property
    def kind(self) -> str:
        return device_kind(self)

    @property
    def standard_latency_choices(self):
        return standard_latency_choices_for_range(self.min_latency, self.max_latency)

    @property
    def encoding_configurable(self):
        if self.supported_encodings is None:
            return None
        return len(set(self.supported_encodings)) > 1

    @property
    def gain_configurable(self):
        if self.supported_gain_levels is None:
            return None
        return bool(self.supported_gain_levels)

    @property
    def advertises_aes67_multicast_prefix(self) -> bool:
        return device_advertises_aes67_multicast_prefix(self)

    @property
    def is_licensed(self) -> bool | None:
        for service in self.services.values():
            if service.get("type") != SERVICE_ARC:
                continue
            properties = service.get("properties")
            if isinstance(properties, dict) and "unlicensed" in properties:
                return False
        return None

    @property
    def gain_level_choices(self):
        if self.gain_device_type is None:
            return None
        return gain_level_choices(self.gain_device_type, self.supported_gain_levels)

    def gain_level_for_channel(self, channel_number: int, channel_type: str) -> int | None:
        if self.gain_levels is None or gain_channel_type(self.gain_device_type or "") != channel_type:
            return None
        channel_index = channel_number - 1
        if not 0 <= channel_index < len(self.gain_levels):
            return None
        return self.gain_levels[channel_index]

    def gain_level_label_for_channel(self, channel_number: int, channel_type: str) -> str | None:
        gain_level = self.gain_level_for_channel(channel_number, channel_type)
        if gain_level is None or self.gain_device_type is None:
            return None
        return gain_level_label(self.gain_device_type, gain_level)

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

    def _core_client(self, timeout_milliseconds: int = 1000, attempts: int = 3):
        from netaudio import core

        ip = str(self.ipv4) if self.ipv4 else None
        if not ip:
            return None
        arc_port = self._arc_port()
        key = (ip, arc_port, timeout_milliseconds, attempts)
        if self._core is None or self._core_key != key:
            if self._core is not None:
                self._core.close()
            self._core = core.CoreClient(
                ip,
                arc_port=arc_port,
                timeout_ms=timeout_milliseconds,
                attempts=attempts,
            )
            mac = core.host_mac()
            if mac:
                self._core.set_host_mac(mac)
            observer = self._app.core_observer if self._app is not None else None
            if observer is not None:
                self._core.observer = observer
            self._core_key = key
        return self._core

    def _core_client_for_request(
        self,
        request_timeout_milliseconds: int | None,
        request_attempts: int | None,
    ):
        if request_timeout_milliseconds is None and request_attempts is None:
            return self._core_client()
        timeout_milliseconds = request_timeout_milliseconds if request_timeout_milliseconds is not None else 1000
        attempts = request_attempts if request_attempts is not None else 3
        return self._core_client(timeout_milliseconds=timeout_milliseconds, attempts=attempts)

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
            subscription.rx_channel = channel
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

    def apply_receiver_flow_status_page(self, page: dict) -> None:
        flows = [dict(flow) for flow in page.get("flows") or [] if isinstance(flow, dict)]
        self.receiver_flows = flows
        reported_flow_count = page.get("reported_flow_count")
        if isinstance(reported_flow_count, int):
            self.rx_flow_count = reported_flow_count
        else:
            self.rx_flow_count = len(flows)
        if not flows:
            self.receiver_flow_latency_nanoseconds = None
            return
        latency_nanoseconds = flows[0].get("latency_nanoseconds")
        if isinstance(latency_nanoseconds, int):
            self.receiver_flow_latency_nanoseconds = latency_nanoseconds

    def apply_transmitter_channel_status_page(self, page: dict) -> None:
        for record in page.get("records") or []:
            channel_number = record.get("channel_number")
            channel = self.tx_channels.get(channel_number)
            if channel is None:
                continue
            factory_name = record.get("friendly_channel_name")
            if factory_name:
                channel.factory_name = factory_name

    def apply_transmitter_flow_status_page(self, page: dict) -> None:
        reported_flow_count = page.get("reported_flow_count")
        if isinstance(reported_flow_count, int):
            self.tx_flow_count = reported_flow_count
        transmitter_flows = []
        for flow in page.get("flows") or []:
            if not isinstance(flow, dict):
                continue
            retained_flow = {
                "flow_number": flow.get("flow_number"),
                "flow_type": flow.get("flow_type"),
                "flow_type_code": flow.get("flow_type_code"),
                "channel_count": flow.get("channel_count"),
                "sample_rate": flow.get("sample_rate"),
                "encoding": flow.get("encoding"),
                "destination_internet_protocol_version_four_address": flow.get(
                    "destination_internet_protocol_version_four_address"
                ),
                "destination_user_datagram_port": flow.get("destination_user_datagram_port"),
                "subscriber_device_name": flow.get("subscriber_device_name"),
                "subscriber_flow_name": flow.get("subscriber_flow_name"),
            }
            modern_fields = (
                "global_flow_id",
                "media_type",
                "media_local_flow_id",
                "channel_slot_segment_header",
                "channel_slot_count",
                "transmitter_channel_ids_by_slot",
                "populated_transmitter_channel_ids",
                "populated_slot_count",
            )
            retained_flow.update({field: flow[field] for field in modern_fields if field in flow})
            transmitter_flows.append(retained_flow)
        self.transmitter_flows = transmitter_flows

    def apply_receiver_channel_status_page(self, page: dict) -> None:
        for record in page.get("records") or []:
            channel_number = record.get("channel_number")
            channel = self.rx_channels.get(channel_number)
            if channel is None:
                continue
            factory_name = record.get("friendly_channel_name")
            if factory_name:
                channel.factory_name = factory_name
            status_code = record.get("subscription_status_code")
            if isinstance(status_code, int):
                channel.status_code = status_code
            source_device_name = record.get("source_device_name")
            source_channel_name = record.get("source_channel_name")
            if not source_device_name or not source_channel_name:
                continue
            subscription = next(
                (entry for entry in self.subscriptions if entry.rx_channel_name == channel.name),
                None,
            )
            if subscription is None:
                continue
            if not subscription.tx_device_name or not subscription.tx_channel_name:
                subscription.tx_device_name = source_device_name
                subscription.tx_channel_name = source_channel_name
            if isinstance(status_code, int):
                subscription.status_code = status_code
            receiver_status_code = record.get("receiver_status_code")
            if isinstance(receiver_status_code, int):
                subscription.rx_channel_status_code = receiver_status_code

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

    async def fetch_device_name(
        self,
        request_timeout_milliseconds: int | None = None,
        request_attempts: int | None = None,
    ):
        client = self._core_client_for_request(request_timeout_milliseconds, request_attempts)
        if client is None:
            return None
        if client.observer is not None:
            from netaudio._capture import fetch_device_name

            return await asyncio.to_thread(fetch_device_name, client, self._arc_port())
        return await asyncio.to_thread(client.get_device_name)

    async def fetch_controls_data(
        self,
        include_channels: bool = True,
        request_timeout_milliseconds: int | None = None,
        request_attempts: int | None = None,
    ):
        client = self._core_client_for_request(request_timeout_milliseconds, request_attempts)
        if client is None:
            return None

        if client.observer is not None:
            from netaudio._capture import _fetch_instrumented

            raw = await asyncio.to_thread(
                _fetch_instrumented,
                client,
                self._arc_port(),
                include_channels,
            )
            raw["channels_included"] = include_channels
            return self.controls_data_from_core(raw)

        def _work():
            counts = client.get_channel_count()
            tx_count, rx_count, _ = counts
            if include_channels:
                rx_inventory = client.get_rx_inventory(rx_count)
                rx_channels = rx_inventory["channels"]
                tx_channels = client.get_tx_channels()
                channel_audio_metadata = rx_inventory.get("channel_audio_metadata")
                if channel_audio_metadata is None and tx_count > 0:
                    channel_audio_metadata = client.get_channel_audio_metadata(tx_count, 0)
            else:
                rx_channels = []
                tx_channels = []
                channel_audio_metadata = client.get_channel_audio_metadata(tx_count, rx_count)
            result = {
                "name": client.get_device_name(),
                "counts": counts,
                "rx": rx_channels,
                "tx": tx_channels,
                "channels_included": include_channels,
                "channel_audio_metadata": channel_audio_metadata,
            }
            from netaudio.core import NetaudioCoreError

            try:
                result["settings"] = client.get_device_settings()
            except NetaudioCoreError:
                result["settings"] = None
            if self.settings_properties is None:
                try:
                    result["property_directory"] = client.get_property_directory()
                except NetaudioCoreError:
                    result["property_directory"] = None
            else:
                result["property_directory"] = None
            aes67_supported = self.aes67_supported
            if result["property_directory"] is not None:
                aes67_supported = result["property_directory"]["aes67_supported"]
            if aes67_supported is False:
                result["aes67"] = None
                result["aes67_multicast_prefix"] = None
            else:
                try:
                    from netaudio import core

                    packet = core.build_command({"command": "query_latency_config"})
                    response = client.request(packet, client._arc_port)
                    if response is None:
                        result["aes67"] = None
                        result["aes67_multicast_prefix"] = None
                    else:
                        result["aes67"] = core.parse_response("aes67_configured", response)
                        settings = core.parse_response("device_settings", response)
                        result["aes67_multicast_prefix"] = (
                            settings.get("aes67_multicast_prefix") if isinstance(settings, dict) else None
                        )
                except NetaudioCoreError:
                    result["aes67"] = None
                    result["aes67_multicast_prefix"] = None
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
        if data.get("aes67_multicast_prefix") is not None:
            controls["aes67_multicast_prefix"] = data["aes67_multicast_prefix"]
        property_directory = data.get("property_directory")
        if property_directory is not None:
            controls["settings_properties"] = property_directory["properties"]
            controls["aes67_supported"] = property_directory["aes67_supported"]
        settings_data = data.get("settings")
        if settings_data:
            if settings_data.get("sample_rate"):
                controls["sample_rate"] = settings_data["sample_rate"]
            controls.update(latency_controls_from_settings(settings_data))
        channel_audio_metadata = data.get("channel_audio_metadata")
        if channel_audio_metadata:
            current_encoding = channel_audio_metadata.get("current_encoding")
            supported_encodings = channel_audio_metadata.get("supported_encodings")
            if current_encoding and supported_encodings and current_encoding in supported_encodings:
                controls["channel_metadata_encoding"] = current_encoding
                controls["channel_metadata_supported_encodings"] = supported_encodings
        channels_included = data.get("channels_included", True)
        rx_channels, subscriptions = self._build_rx_from_records(data["rx"])
        if channels_included:
            controls["rx_channels"] = rx_channels
            controls["subscriptions"] = subscriptions
        tx_channels = self._build_tx_from_records(data["tx"])
        if channels_included:
            controls["tx_channels"] = tx_channels
        return controls

    async def populate_from_core(
        self,
        include_channels: bool = True,
        request_timeout_milliseconds: int | None = None,
        request_attempts: int | None = None,
    ):
        controls = await self.fetch_controls_data(
            include_channels=include_channels,
            request_timeout_milliseconds=request_timeout_milliseconds,
            request_attempts=request_attempts,
        )
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
        if "receiver_flow_latency_nanoseconds" in data:
            self.receiver_flow_latency_nanoseconds = data["receiver_flow_latency_nanoseconds"]
        if "tx_count" in data:
            self.tx_count = self.tx_count_raw = data["tx_count"]
        if "rx_count" in data:
            self.rx_count = self.rx_count_raw = data["rx_count"]
        if "is_locked" in data:
            self.is_locked = data["is_locked"]
        if "aes67_configured" in data:
            self.aes67_configured = data["aes67_configured"]
        if "aes67_multicast_prefix" in data:
            self.aes67_multicast_prefix = data["aes67_multicast_prefix"]
        if "aes67_supported" in data:
            self.aes67_supported = data["aes67_supported"]
        if "settings_properties" in data:
            self.settings_properties = data["settings_properties"]
        if self.supported_encodings is None and "channel_metadata_supported_encodings" in data:
            channel_metadata_encoding = data["channel_metadata_encoding"]
            if self.encoding is None or self.encoding == channel_metadata_encoding:
                self.encoding = channel_metadata_encoding
                self.supported_encodings = data["channel_metadata_supported_encodings"]
        if "tx_channels" in data:
            self.tx_channels = data["tx_channels"]
            self.tx_count = len(self.tx_channels)
        if "rx_channels" in data:
            self.rx_channels = data["rx_channels"]
            self.rx_count = len(self.rx_channels)
            self.subscriptions = data.get("subscriptions", [])
        self.error = None

    def _receive_solicited_control_publication(self, packet, device_ip, parse_packet):
        from netaudio.common.app_config import settings as app_settings

        multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        multicast_socket.bind((MULTICAST_GROUP_CONTROL_MONITORING, DEVICE_INFO_PORT))
        interface_ip = app_settings.interface_ip or "0.0.0.0"
        membership_request = struct.pack(
            "4s4s",
            socket.inet_aton(MULTICAST_GROUP_CONTROL_MONITORING),
            socket.inet_aton(interface_ip),
        )
        multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership_request)
        multicast_socket.settimeout(2)

        send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        send_socket.settimeout(2)
        try:
            send_socket.sendto(packet, (device_ip, DEVICE_SETTINGS_PORT))
            while True:
                data, address = multicast_socket.recvfrom(4096)
                if address[0] != device_ip:
                    continue
                parsed = parse_packet(data)
                if parsed is not None:
                    return parsed
        finally:
            send_socket.close()
            multicast_socket.close()

    async def get_bluetooth_status(self, host_mac=None):
        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac

            host_mac = _get_host_mac()
        packet, _, _ = self.commands.command_bluetooth_status(host_mac=host_mac)
        device_ip = str(self.ipv4)

        def _query():
            return self._receive_solicited_control_publication(
                packet,
                device_ip,
                DanteDeviceParser.parse_bluetooth_status_state,
            )

        try:
            status = await asyncio.to_thread(_query)
            if not isinstance(status, dict):
                self.bluetooth_connected = None
                self.bluetooth_device = None
                return None
            self.bluetooth_connected = status["connected"]
            self.bluetooth_device = status["device_name"]
            return self.bluetooth_device
        except (TimeoutError, socket.timeout):
            logger.debug(f"Timeout waiting for bluetooth status from {self.name}")
            self.bluetooth_connected = None
            self.bluetooth_device = None
            return None
        except OSError as exception:
            logger.debug(f"Bluetooth status listener unavailable for {self.name}: {exception}")
            self.bluetooth_connected = None
            self.bluetooth_device = None
            return None

    async def get_clocking_status(self, host_mac=None, sequence=0x0021):
        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac

            host_mac = _get_host_mac()
        packet, _, _ = self.commands.command_refresh_clock_status(host_mac=host_mac, sequence=sequence)
        device_ip = str(self.ipv4)

        def _query():
            from netaudio import core

            def parse_packet(data):
                try:
                    return core.parse_response("ptp_clock_status", data)
                except core.NetaudioCoreError:
                    return None

            return self._receive_solicited_control_publication(packet, device_ip, parse_packet)

        try:
            parsed_response = await asyncio.to_thread(_query)
        except (TimeoutError, socket.timeout):
            logger.debug(f"Timeout waiting for clock status from {self.name}")
            return None
        except OSError as exception:
            raise RuntimeError(
                "clock status listener is unavailable because another netaudio process "
                "already owns the control-monitoring socket"
            ) from exception

        self.preferred_leader = parsed_response["preferred_leader"]
        self.clock_source_code = parsed_response["clock_source_code"]
        clock_subdomain = parsed_response.get("clock_subdomain")
        self.clock_subdomain = bytes(clock_subdomain) if clock_subdomain is not None else None
        self.clock_frequency_offset_parts_per_billion = parsed_response["clock_frequency_offset_parts_per_billion"]
        self.clock_port_state_code = parsed_response["clock_port_state_code"]
        self.clock_role = parsed_response.get("clock_role")
        self.clock_port_records = parsed_response.get("clock_port_records")
        self.clock_identity = canonical_clock_identity(parsed_response.get("clock_identity"))
        self.leader_clock_identity = canonical_clock_identity(parsed_response.get("leader_clock_identity"))
        return parsed_response

    def to_json(self):
        return DanteDeviceSerializer.to_json(self)
