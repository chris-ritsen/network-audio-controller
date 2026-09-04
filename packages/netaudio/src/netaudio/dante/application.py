from __future__ import annotations

import asyncio
import ipaddress
import logging
import math
import time
from collections.abc import Awaitable, Callable

from netaudio.dante.capability_partition import (
    CapabilityPartitionExport,
    parse_capability_partition_export,
)
from netaudio.dante.channel_frontend import (
    channel_result_code,
    receiver_channel_name_protocol_identifier_from_probe,
    transmitter_channel_name_protocol_identifier_from_probe,
)
from netaudio.dante.channel_status_paging import (
    ChannelStatusPageAccumulator,
    modern_arc_protocol_identifier_for_device,
)
from netaudio.dante.commands import DanteCommands, channel_status_query_specification, validate_dante_name
from netaudio.dante.conmon_export import ConmonExport, ConmonExportUnavailableError
from netaudio.dante.const import (
    BLUETOOTH_MODEL_IDS,
    DEVICE_ARC_PORT,
    OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809,
    OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
    PROTOCOL_ARC_2809,
    RESULT_CODE_SUCCESS,
    RESULT_CODE_SUCCESS_EXTENDED,
    SERVICE_ARC,
    SERVICE_CMC,
    SERVICE_DBC,
    SERVICES,
)
from netaudio.dante.core_transport import CoreTransport
from netaudio.dante.diagnostic_logs import (
    DeviceLogExport,
    device_audio_capability_fields,
    parse_device_log_export,
)
from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio.dante.gain import SUPPORTED_GAIN_LEVELS
from netaudio.dante.latency import latency_controls_from_settings, nanoseconds_to_milliseconds
from netaudio.dante.link_status import LinkStatusObservation
from netaudio.dante.lock import _validate_lock_key, core_lock_device, core_unlock_device
from netaudio.dante.lock_status import LockStatusObservation
from netaudio.dante.services.cmc import DanteCMCService
from netaudio.dante.services.notification import (
    NOTIFICATION_LATENCY_CHANGE,
    NOTIFICATION_PROPERTY_CHANGE,
    NOTIFICATION_ROUTING_DEVICE_CHANGE,
    NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_RX_FLOW_CHANGE,
    NOTIFICATION_SETTINGS_CHANGE,
    NOTIFICATION_TX_CHANNEL_CHANGE,
    NOTIFICATION_TX_LABEL_CHANGE,
    DanteNotificationService,
    mutate_and_wait_for_capability_value,
    mutate_and_wait_for_clear_configuration_status,
    request_and_wait_for_conmon_export,
    send_and_wait_for_gain_status,
)
from netaudio.dante.services.notification_packet_handlers import (
    STATUS_KIND_AES67,
    STATUS_KIND_BLUETOOTH,
    STATUS_KIND_CLOCK,
    STATUS_KIND_ENCODING,
    STATUS_KIND_GAIN,
    STATUS_KIND_SAMPLE_RATE,
    STATUS_KIND_SAMPLE_RATE_PULLUP,
)
from netaudio.dante.state import STATUS_KIND_DIAGNOSTIC_LOG_EXPORT, DanteStateService, apply_device_status

logger = logging.getLogger("netaudio")

CHANNEL_NAME_NOTIFICATION_IDS = {
    "rx": (NOTIFICATION_RX_CHANNEL_CHANGE, NOTIFICATION_PROPERTY_CHANGE),
    "tx": (NOTIFICATION_TX_CHANNEL_CHANGE, NOTIFICATION_TX_LABEL_CHANGE, NOTIFICATION_PROPERTY_CHANGE),
}
DEVICE_NAME_NOTIFICATION_IDS = (NOTIFICATION_ROUTING_DEVICE_CHANGE, NOTIFICATION_SETTINGS_CHANGE)
SUBSCRIPTION_NOTIFICATION_IDS = (
    NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_RX_FLOW_CHANGE,
    NOTIFICATION_ROUTING_DEVICE_CHANGE,
)


class CapabilityProbeTimeout(RuntimeError):
    pass


def _clock_status_snapshot(device) -> dict:
    return {
        "clock_frequency_offset_parts_per_billion": device.clock_frequency_offset_parts_per_billion,
        "clock_identity": device.clock_identity,
        "clock_port_records": device.clock_port_records,
        "clock_port_state_code": device.clock_port_state_code,
        "clock_role": device.clock_role,
        "clock_source_code": device.clock_source_code,
        "clock_subdomain": device.clock_subdomain,
        "leader_clock_identity": device.leader_clock_identity,
        "preferred_leader": device.preferred_leader,
    }


class DanteApplication:
    def __init__(self, packet_store=None, dissect=False, session_id=None, managed_transport=None):
        from netaudio.common.app_config import settings as app_settings

        self.devices: dict = {}
        self.media_services: dict[str, dict] = {}
        self.dispatcher = DanteEventDispatcher()
        self._packet_store = packet_store
        self._dissect = dissect
        self.capture_session_id: int | None = session_id
        self._capture_queue: asyncio.Queue | None = None
        self._capture_loop: asyncio.AbstractEventLoop | None = None
        self._capture_writer_task: asyncio.Task | None = None
        self.transport = CoreTransport(observer=self._observe_wire if (packet_store is not None or dissect) else None)
        self.commands = DanteCommands()
        self.cmc = DanteCMCService(self.transport, interface_name=app_settings.interface)
        self.notifications = DanteNotificationService(
            dispatcher=self.dispatcher,
            device_lookup=self._device_by_control_key,
            packet_store=packet_store,
            interface_ip=app_settings.interface_ip,
            dissect=dissect,
        )
        self.notifications.session_id = session_id
        self.state = DanteStateService(self)
        self._browser = None
        self._started = False
        self._capability_probe_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._managed_transport = managed_transport
        self._managed_transports: dict[str, object] = {}

    @staticmethod
    def _apply_device_settings(device, settings) -> None:
        if not isinstance(settings, dict):
            return
        controls = latency_controls_from_settings(settings)
        if settings.get("sample_rate"):
            controls["sample_rate"] = settings["sample_rate"]
        device.apply_controls(controls)

    def _apply_discovered_services(self, server_name: str, device_services: dict):
        if server_name in self.devices:
            device = self.devices[server_name]
        else:
            from netaudio.dante.device import DanteDevice

            device = DanteDevice(server_name=server_name, app=self)
            self.register_device(server_name, device)

        device.services = dict(sorted(device_services.items()))
        arc_service = next(
            (service for service in device_services.values() if service["type"] == SERVICE_ARC),
            None,
        )
        if arc_service is not None:
            device.ipv4 = arc_service["ipv4"]
        for service in device_services.values():
            if not device.ipv4:
                device.ipv4 = service["ipv4"]
            service_properties = service.get("properties", {})
            if "id" in service_properties and service["type"] == SERVICE_CMC:
                device.mac_address = service_properties["id"]
            if "model" in service_properties:
                device.model_id = service_properties["model"]
            if "mf" in service_properties:
                device.manufacturer_mdns = service_properties["mf"]
                if not device.manufacturer:
                    device.manufacturer = service_properties["mf"]
            if "server_vers" in service_properties and service["type"] == SERVICE_CMC:
                device.software_version = service_properties["server_vers"]
            if "router_vers" in service_properties:
                device.firmware_version = service_properties["router_vers"]
            if "router_info" in service_properties and service_properties["router_info"] == '"Dante Via"':
                device.software = "Dante Via"
            if "rate" in service_properties:
                device.sample_rate = int(service_properties["rate"])
            if "latency_ns" in service_properties:
                device.latency = nanoseconds_to_milliseconds(service_properties["latency_ns"])
        return device

    @staticmethod
    def _apply_encoding_capability(device, current_encoding: int, supported_encodings: list[int]) -> None:
        apply_device_status(
            device,
            STATUS_KIND_ENCODING,
            {"encoding": current_encoding, "supported_encodings": supported_encodings},
        )

    @staticmethod
    def _apply_gain_capability(device, device_type: str, channel_levels: list[int]) -> None:
        apply_device_status(
            device,
            STATUS_KIND_GAIN,
            {
                "gain_device_type": device_type,
                "gain_levels": channel_levels,
                "supported_gain_levels": list(SUPPORTED_GAIN_LEVELS),
            },
        )

    @staticmethod
    def _apply_sample_rate_capability(device, current_sample_rate: int, supported_sample_rates: list[int]) -> None:
        apply_device_status(
            device,
            STATUS_KIND_SAMPLE_RATE,
            {"sample_rate": current_sample_rate, "supported_sample_rates": supported_sample_rates},
        )

    @staticmethod
    def _apply_sample_rate_pullup_capability(
        device,
        current_raw_value: int,
        supported_raw_values: list[int],
    ) -> None:
        apply_device_status(
            device,
            STATUS_KIND_SAMPLE_RATE_PULLUP,
            {
                "sample_rate_pullup_raw_value": current_raw_value,
                "supported_sample_rate_pullup_raw_values": supported_raw_values,
            },
        )

    def _capability_probe_lock(self, capability_name: str, device_ip_address: str) -> asyncio.Lock:
        lock_key = (capability_name, device_ip_address)
        lock = self._capability_probe_locks.get(lock_key)
        if lock is None:
            lock = asyncio.Lock()
            self._capability_probe_locks[lock_key] = lock
        return lock

    async def _capture_writer(self) -> None:
        from netaudio._capture import _dissect, _record

        while True:
            item = await self._capture_queue.get()
            if item is None:
                return
            payload, device_ip, port, direction, source_type = item
            if self._dissect:
                _dissect(payload, device_ip, port, direction)
            if self._packet_store is not None:
                _record(self._packet_store, self.capture_session_id, payload, device_ip, port, direction, source_type)

    def _devices_by_ip(self, ip_address: str) -> list:
        return [
            device
            for device in self.devices.values()
            if device.ipv4 is not None and str(device.ipv4) == str(ip_address)
        ]

    def _device_by_ip(self, ip_address: str):
        matches = self._devices_by_ip(ip_address)
        return matches[0] if len(matches) == 1 else None

    @staticmethod
    def _managed_control_key(device) -> str:
        device_id = getattr(device, "ddm_device_id", None)
        if not isinstance(device_id, str) or not device_id:
            raise RuntimeError(f"{device.name or device.server_name} is managed but has no DDM device ID")
        server = getattr(device, "ddm_server_profile", None)
        context = getattr(device, "ddm_context", None)
        domain = getattr(device, "ddm_domain_id", None)
        missing = [
            name
            for name, value in (("server profile", server), ("context", context), ("domain ID", domain))
            if not isinstance(value, str) or not value
        ]
        if missing:
            fields = ", ".join(missing)
            raise RuntimeError(f"{device.name or device.server_name} is managed but has no {fields}")
        return f"ddm:{server}:{context}:{domain}:{device_id}"

    def _control_key(self, target) -> str:
        if getattr(target, "requires_managed_control", False):
            return self._managed_control_key(target)
        if hasattr(target, "_require_address"):
            return target._require_address()
        if getattr(target, "ipv4", None) is not None:
            return str(target.ipv4)
        return str(target)

    def _device_by_control_key(self, key: str):
        for device in self.devices.values():
            if not device.requires_managed_control:
                continue
            try:
                if self._managed_control_key(device) == key:
                    return device
            except RuntimeError:
                continue
        return self._device_by_ip(key)

    def _control_target(self, target):
        if hasattr(target, "requires_managed_control"):
            return target
        matches = self._devices_by_ip(str(target))
        if len(matches) > 1:
            names = ", ".join(sorted(device.server_name or device.name for device in matches))
            raise RuntimeError(f"control address {target} is ambiguous across devices: {names}")
        return matches[0] if matches else target

    async def _export_conmon_data(
        self,
        device_ip_address: str,
        expected_echoed_tag: bytes,
        expected_selector_value: int,
        request,
        timeout: float,
        operation_name: str,
    ) -> ConmonExport:
        async with self._capability_probe_lock("conmon_export", device_ip_address):
            result = await request_and_wait_for_conmon_export(
                self.notifications,
                device_ip_address,
                expected_echoed_tag,
                expected_selector_value,
                request,
                timeout,
            )
        if result is None:
            raise CapabilityProbeTimeout(f"{operation_name} timed out for {device_ip_address}")
        return result

    async def _mutate_and_take_result(
        self,
        kind: str,
        target,
        mutate: Callable[[], Awaitable[None]],
        timeout: float,
    ):
        key = self._control_key(target)
        waiter = self.notifications.register_waiter(kind, key)
        try:
            await mutate()
            try:
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug(f"{kind} write verification timeout for {key}")
            return waiter.latest_result
        finally:
            self.notifications.unregister_waiter(waiter)

    def _observe_wire(self, payload: bytes, device_ip: str, port: int, direction: str) -> None:
        loop = self._capture_loop
        queue = self._capture_queue
        if loop is None or queue is None:
            return
        source_type = "netaudio_request" if direction == "request" else "netaudio_response"
        loop.call_soon_threadsafe(queue.put_nowait, (payload, device_ip, port, direction, source_type))

    @staticmethod
    def _parse_status_page(response, description, page_kind):
        from netaudio import core

        try:
            page = core.parse_response(page_kind, response)
        except core.NetaudioCoreError as exception:
            raise RuntimeError(f"{description} returned an invalid status page") from exception
        if not isinstance(page, dict):
            raise RuntimeError(f"{description} returned an invalid status page")
        return page

    async def _populate_device_controls(
        self,
        device,
        include_channels: bool = True,
        request_timeout_milliseconds: int | None = None,
        request_attempts: int | None = None,
    ) -> None:
        try:
            if device.requires_managed_control:
                fresh = await self.managed_transport(device).fetch_device(device)
                if fresh.identity is not None:
                    device.name = fresh.identity.actual_name or fresh.identity.default_name or fresh.name
                elif fresh.name:
                    device.name = fresh.name
                if fresh.clock_preferences is not None:
                    device.preferred_leader = fresh.clock_preferences.leader
                await self.get_latency_settings(device)
                if include_channels:
                    await self.apply_modern_arc_status_pages(device)
                try:
                    await self.probe_interface_status(device)
                except (RuntimeError, OSError) as exception:
                    logger.debug(f"Interface status unavailable for {device.server_name}: {exception}")
                return
            await device.populate_from_core(
                include_channels=include_channels,
                request_timeout_milliseconds=request_timeout_milliseconds,
                request_attempts=request_attempts,
            )
            if include_channels:
                await self.apply_modern_arc_status_pages(device)
        except (RuntimeError, OSError) as exception:
            device.error = exception
            logger.debug(f"Error populating controls for {device.server_name}: {exception}")

    async def _probe_aes67_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_all(
            "aes67",
            self.probe_aes67_state,
            devices,
            timeout,
            "AES67",
            skip_device=lambda device: device.aes67_current is not None,
        )

    async def _probe_all(
        self,
        kind: str,
        probe,
        devices: dict | None,
        timeout: float,
        description: str,
        skip_device=None,
    ) -> None:
        target_devices = self.devices if devices is None else devices
        probe_tasks = {}
        for device in target_devices.values():
            if (not device.requires_managed_control and not device.ipv4) or (
                skip_device is not None and skip_device(device)
            ):
                continue
            target = device if device.requires_managed_control else str(device.ipv4)
            key = self._control_key(target)
            if key in probe_tasks:
                continue
            probe_tasks[key] = asyncio.create_task(probe(target, timeout=timeout))

        if not probe_tasks:
            return

        logger.debug(f"Probed {description} for {len(probe_tasks)} device addresses")
        results = await asyncio.gather(*probe_tasks.values(), return_exceptions=True)
        response_count = 0
        for key, result in zip(probe_tasks, results):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, CapabilityProbeTimeout):
                logger.debug(f"{description} probe timed out for {key}")
                continue
            if isinstance(result, Exception):
                logger.warning(f"Failed to probe {description} for {key}: {result}")
                continue
            response_count += 1
        logger.debug(f"{description.capitalize()}: {response_count}/{len(probe_tasks)} device addresses responded")

    async def _probe_capabilities_all(
        self,
        capability_is_known,
        apply_capability,
        probe_status,
        capability_description: str,
        timeout: float,
        devices: dict | None = None,
    ) -> None:
        target_devices = self.devices if devices is None else devices
        probe_tasks = {}
        target_devices_by_key = {}
        for device in target_devices.values():
            if (
                not device.online
                or (not device.requires_managed_control and not device.ipv4)
                or capability_is_known(device)
            ):
                continue
            target = device if device.requires_managed_control else str(device.ipv4)
            key = self._control_key(target)
            target_devices_by_key.setdefault(key, []).append(device)
            if key in probe_tasks:
                continue
            probe_tasks[key] = asyncio.create_task(probe_status(target, timeout=timeout))

        if not probe_tasks:
            return

        logger.debug(f"Probed {capability_description} for {len(probe_tasks)} device addresses")

        probe_results = await asyncio.gather(*probe_tasks.values(), return_exceptions=True)
        response_count = 0
        for key, result in zip(probe_tasks, probe_results):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, CapabilityProbeTimeout):
                logger.debug(f"{capability_description} probe timed out for {key}")
                continue
            if isinstance(result, Exception):
                logger.warning(f"Failed to probe {capability_description} for {key}: {result}")
                continue
            response_count += 1
            current_value, supported_values = result
            for device in target_devices_by_key[key]:
                if device.online:
                    apply_capability(device, current_value, supported_values)

        logger.debug(
            f"{capability_description.capitalize()}: {response_count}/{len(probe_tasks)} device addresses responded"
        )

    async def _probe_encodings_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_capabilities_all(
            lambda device: device.supported_encodings is not None,
            self._apply_encoding_capability,
            self.probe_encoding_status,
            "encodings",
            timeout,
            devices,
        )

    async def _probe_gain_levels_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_capabilities_all(
            lambda device: device.supported_gain_levels is not None,
            self._apply_gain_capability,
            self.probe_gain_status,
            "gain levels",
            timeout,
            devices,
        )

    async def _probe_interface_status(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_all("interface", self.probe_interface_status, devices, timeout, "interface status")

    async def _probe_once(
        self,
        capability_name: str,
        target,
        send_probe: Callable[[object], Awaitable[None]],
        timeout: float,
        description: str,
    ):
        key = self._control_key(target)
        async with self._capability_probe_lock(capability_name, key):
            waiter = self.notifications.register_waiter(capability_name, key)
            try:
                await send_probe(target)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    if waiter.latest_result is None:
                        raise CapabilityProbeTimeout(f"{description} readback timed out for {key}") from None
                if waiter.latest_result is None:
                    raise RuntimeError(f"{description} readback was unavailable for {key}")
                return waiter.latest_result
            finally:
                self.notifications.unregister_waiter(waiter)

    async def _probe_preferred_leader_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_all(
            "preferred_leader",
            self.probe_preferred_leader_state,
            devices,
            timeout,
            "preferred leader",
            skip_device=lambda device: device.preferred_leader is not None,
        )

    async def _probe_sample_rate_pullups_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_capabilities_all(
            lambda device: device.supported_sample_rate_pullup_raw_values is not None,
            self._apply_sample_rate_pullup_capability,
            self.probe_sample_rate_pullup_status,
            "sample rate pull-ups",
            timeout,
            devices,
        )

    async def _probe_sample_rates_all(self, timeout: float = 3.0, devices: dict | None = None) -> None:
        await self._probe_capabilities_all(
            lambda device: device.supported_sample_rates is not None,
            self._apply_sample_rate_capability,
            self.probe_sample_rate_status,
            "sample rates",
            timeout,
            devices,
        )

    async def _probe_with_retries(
        self,
        capability_name: str,
        target,
        send_probe: Callable[[object], Awaitable[None]],
        timeout: float,
        description: str,
    ):
        key = self._control_key(target)
        async with self._capability_probe_lock(capability_name, key):
            waiter = self.notifications.register_waiter(capability_name, key)
            try:
                event_loop = asyncio.get_running_loop()
                deadline = event_loop.time() + timeout
                attempt_count = 3
                for attempt_number in range(attempt_count):
                    await send_probe(target)
                    remaining_time = max(0.0, deadline - event_loop.time())
                    if remaining_time == 0:
                        break
                    if attempt_number == attempt_count - 1:
                        attempt_timeout = remaining_time
                    else:
                        attempt_timeout = min(remaining_time, timeout / 4)
                    try:
                        await asyncio.wait_for(waiter.wait(), timeout=attempt_timeout)
                    except asyncio.TimeoutError:
                        continue
                    if waiter.latest_result is not None:
                        return waiter.latest_result
                    waiter.clear()
                if waiter.latest_result is None:
                    raise CapabilityProbeTimeout(f"{description} readback timed out for {key}")
                return waiter.latest_result
            finally:
                self.notifications.unregister_waiter(waiter)

    async def _query_channel_status_pages(self, device, channel_type):
        protocol_id = modern_arc_protocol_identifier_for_device(device)
        if channel_type == "rx":
            opcode = OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809
            page_kind = "modern_arc_receiver_channel_status_page"
            description = "receiver channel status query"
            cache_attribute = "receiver_channel_name_protocol_identifier"
        else:
            opcode = OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809
            page_kind = "modern_arc_transmitter_channel_status_page"
            description = "transmitter channel status query"
            cache_attribute = "transmitter_channel_name_protocol_identifier"

        accumulator = ChannelStatusPageAccumulator(protocol_id, opcode)
        request_range = (1, 1, 0)
        while request_range is not None:
            media_selector, starting_channel_identifier, ending_channel_identifier = request_range
            response = await device.execute(
                channel_status_query_specification(
                    channel_type,
                    protocol_id=protocol_id,
                    media_selector=media_selector,
                    starting_channel_identifier=starting_channel_identifier,
                    ending_channel_identifier=ending_channel_identifier,
                )
            )
            result_code = channel_result_code(response, description)
            if result_code not in (RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED):
                raise RuntimeError(f"{description} failed with result 0x{result_code:04X}")
            page = self._parse_status_page(response, description, page_kind)
            request_range = accumulator.add(page)

        setattr(device, cache_attribute, protocol_id)
        return accumulator.result()

    async def _query_conmon_all(self, timeout: float = 10.0, devices: dict | None = None) -> None:
        target_devices = self.devices if devices is None else devices
        deadline = time.monotonic() + timeout
        incomplete_devices = []
        for device in target_devices.values():
            if deadline - time.monotonic() <= 0:
                logger.debug("Conmon query timeout reached, skipping remaining devices")
                break
            if device.requires_managed_control or not device.ipv4 or not device.mac_address:
                continue
            if not await self._query_conmon_for_device(device, deadline):
                incomplete_devices.append(device)
        for retry in range(2):
            if not incomplete_devices or deadline - time.monotonic() <= 0:
                break
            incomplete_devices = await self._retry_conmon_query(incomplete_devices, deadline, retry)

    async def _query_conmon_for_device(self, device, deadline: float) -> bool:
        device_ip = str(device.ipv4)
        waiter = self.notifications.register_conmon_waiter(device_ip)
        try:
            await self._send_conmon_query_for_device(device, self.send_make_model_request)
            await self._send_conmon_query_for_device(device, self.send_dante_model_request)
            per_device_timeout = min(deadline - time.monotonic(), 1.0)
            try:
                await asyncio.wait_for(waiter.wait(), timeout=per_device_timeout)
                logger.debug(f"Conmon responses received for {device.server_name}")
            except asyncio.TimeoutError:
                logger.debug(f"Conmon query partial/timeout for {device.server_name}")
                received = self.notifications._conmon_received.get(device_ip, set())
                return len(received) >= 2
        finally:
            self.notifications.unregister_waiter(waiter)
        return True

    async def _retry_conmon_query(self, incomplete_devices: list, deadline: float, retry: int) -> list:
        still_incomplete = []
        for device in incomplete_devices:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            device_ip = str(device.ipv4)
            needs_make_model = not device.dante_model
            needs_dante_model = not device.dante_model_id
            expected_count = int(needs_make_model) + int(needs_dante_model)
            if expected_count == 0:
                continue
            waiter = self.notifications.register_conmon_waiter(device_ip, expected_count=expected_count)
            try:
                if needs_make_model:
                    await self._send_conmon_query_for_device(device, self.send_make_model_request)
                if needs_dante_model:
                    await self._send_conmon_query_for_device(device, self.send_dante_model_request)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=min(remaining, 2.0))
                    logger.debug(f"Conmon retry {retry + 1} succeeded for {device.server_name}")
                except asyncio.TimeoutError:
                    logger.debug(f"Conmon retry {retry + 1} timeout for {device.server_name}")
                    if not device.dante_model_id:
                        still_incomplete.append(device)
            finally:
                self.notifications.unregister_waiter(waiter)
        return still_incomplete

    async def _query_settings_fields(self, devices: dict | None = None) -> None:
        target_devices = self.devices if devices is None else devices
        tasks = [
            self.probe_bluetooth_status(device)
            for device in target_devices.values()
            if (device.requires_managed_control or device.ipv4) and device.model_id in BLUETOOTH_MODEL_IDS
        ]
        if not tasks:
            return
        for result in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, Exception):
                logger.debug(f"Bluetooth status unavailable: {result}")

    async def _query_modern_arc_status_page(self, device, specification, description, page_kind):
        from netaudio import core

        response = await device.execute(specification)
        if response is None:
            raise RuntimeError(f"{description} did not receive a response")
        try:
            result_code = core.parse_response("result_code", response)
        except core.NetaudioCoreError as exception:
            raise RuntimeError(f"{description} returned an invalid response") from exception
        if not isinstance(result_code, int):
            raise RuntimeError(f"{description} returned an invalid response")
        if result_code != RESULT_CODE_SUCCESS:
            raise RuntimeError(f"{description} failed with result 0x{result_code:04X}")
        return self._parse_status_page(response, description, page_kind)

    async def _send_conmon_query_for_device(self, device, request: Callable[[object, str], Awaitable[None]]) -> None:
        from netaudio import core

        if device.requires_managed_control or not device.ipv4 or not device.mac_address:
            return

        mac_hex = device.mac_address.replace(":", "").replace("-", "")

        if len(mac_hex) == 16 and mac_hex[6:10].upper() == "FFFE":
            mac_hex = mac_hex[:6] + mac_hex[10:]
        elif len(mac_hex) == 16 and mac_hex.upper().endswith("0000"):
            mac_hex = mac_hex[:12]

        try:
            await request(device, mac_hex)
        except (core.NetaudioCoreError, OSError) as exception:
            logger.warning(f"Failed to send conmon {request.__name__} to {device.server_name}: {exception}")

    async def _send_registered_system_reset(self, device, build_specification, host_mac) -> None:
        if getattr(device, "requires_managed_control", False):
            raise RuntimeError("system reset has no verified DDM completion path and was not sent")
        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac

            host_mac = _get_host_mac()
        await self.cmc.require_registration(device._require_address(), host_mac)
        await device.execute(build_specification(host_mac))

    def managed_transport(self, device=None):
        if device is not None and getattr(device, "requires_managed_control", False):
            self._managed_control_key(device)
        if self._managed_transport is not None:
            return self._managed_transport

        from netaudio.common.config_loader import default_config_path, load_config_document
        from netaudio.common.managed_api import resolve_ddm_configuration
        from netaudio.ddm.device_transport import ManagedDeviceTransport

        config_path = default_config_path()
        configuration = resolve_ddm_configuration(
            load_config_document(config_path),
            base_directory=config_path.parent,
        )
        server_profile = getattr(device, "ddm_server_profile", None)
        context_name = getattr(device, "ddm_context", None)
        if context_name is not None:
            context = configuration.context(context_name)
            if server_profile is not None and context.server != server_profile:
                raise RuntimeError(
                    f"managed device context {context_name!r} belongs to server {context.server!r}, "
                    f"not {server_profile!r}"
                )
            device_domain_id = getattr(device, "ddm_domain_id", None)
            if device_domain_id is not None and context.domain_id != device_domain_id:
                raise RuntimeError(
                    f"managed device context {context_name!r} belongs to domain {context.domain_id}, "
                    f"not {device_domain_id}"
                )
            server = configuration.server(context.server)
        elif server_profile is not None:
            server = configuration.server(server_profile)
        else:
            server = configuration.selected_server(context_name)
        transport = self._managed_transports.get(server.name)
        if transport is None:
            transport = ManagedDeviceTransport(server)
            self._managed_transports[server.name] = transport
        return transport

    async def execute_managed(self, device, specification: dict) -> bytes | None:
        return await self.managed_transport(device).execute(device, specification)

    async def _send_settings(self, target, specification: dict) -> bytes | None:
        resolved = self._control_target(target)
        if getattr(resolved, "requires_managed_control", False):
            key = self._control_key(resolved)
            response = await resolved.execute(specification)
            if response is not None:
                self.notifications.receive_settings_response(response, key)
            return response
        address = resolved._require_address() if hasattr(resolved, "_require_address") else str(resolved)
        return await self.transport.execute(address, specification)

    async def add_subscriptions(self, device, records):
        if getattr(device, "requires_managed_control", False):
            async with device.topology_mutation_lock:
                return await self.managed_transport(device).set_subscriptions(device, records)
        return await self.mutate_and_wait_for_notification(
            device,
            lambda: self.send_add_subscriptions(device, records),
            SUBSCRIPTION_NOTIFICATION_IDS,
        )

    async def apply_modern_arc_status_pages(self, device) -> None:
        pages = (
            (self.query_modern_arc_receiver_flow_status, device.apply_receiver_flow_status_page),
            (self.query_modern_arc_transmitter_channel_status, device.apply_transmitter_channel_status_page),
            (self.query_modern_arc_transmitter_flow_status, device.apply_transmitter_flow_status_page),
            (self.query_modern_arc_receiver_channel_status, device.apply_receiver_channel_status_page),
        )
        for query, apply in pages:
            try:
                page = await query(device)
            except RuntimeError as exception:
                query_name = getattr(query, "__name__", type(query).__name__)
                logger.debug(f"{query_name} unavailable for {device.server_name}: {exception}")
                continue
            if page is not None:
                apply(page)
        if getattr(self, "media_services", None):
            self._attach_media_services(device)

    def _attach_media_services(self, device) -> None:
        device_names = {str(device.name).casefold(), str(device.server_name).removesuffix(".local.").casefold()}
        for service in self.media_services.values():
            service_type = service.get("type")
            service_name = service.get("name", "")
            suffix = f".{service_type}"
            if not service_name.casefold().endswith(suffix.casefold()):
                continue
            source_name = service_name[: -len(suffix)]
            if "@" not in source_name:
                continue
            channel_name, source_device_name = source_name.rsplit("@", 1)
            if source_device_name.casefold() not in device_names:
                continue
            channel = next(
                (entry for entry in device.tx_channels.values() if entry.name == channel_name),
                None,
            )
            if channel is not None:
                channel.media_service = service

    def arc_port_for_address(self, device_ip_address: str) -> int:
        device = self._device_by_ip(device_ip_address)
        if device is None:
            return DEVICE_ARC_PORT
        return self.get_arc_port(device) or DEVICE_ARC_PORT

    def attach_devices(self, devices: dict) -> None:
        for server_name, device in devices.items():
            device._app = self
            self.devices[server_name] = device

    async def clear_configuration(
        self,
        target,
        preserve_internet_protocol_settings: bool,
        timeout: float = 2.0,
    ) -> dict:
        key = self._control_key(target)
        expected_action_result_code = 2 if preserve_internet_protocol_settings else 1
        command = (
            self.send_clear_all_configuration_preserving_internet_protocol_settings
            if preserve_internet_protocol_settings
            else self.send_clear_all_configuration
        )

        async def mutate() -> None:
            await command(target)

        async with self._capability_probe_lock("clear_configuration_action", key):
            status = await mutate_and_wait_for_clear_configuration_status(
                self.notifications,
                key,
                expected_action_result_code,
                mutate,
                timeout,
            )
        if status is None:
            raise CapabilityProbeTimeout(f"clear-configuration status timed out for {key}")
        if status["action_result_code"] != expected_action_result_code:
            raise RuntimeError(
                f"clear-configuration returned result {status['action_result_code']} "
                f"instead of {expected_action_result_code} for {key}"
            )
        return status

    async def discover_and_populate(self, timeout: float = 5.0) -> dict:
        from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

        from netaudio.dante.browser import DanteBrowser

        discovery_time = min(timeout * 0.4, 2.0)
        populate_time = timeout - discovery_time

        browser = DanteBrowser(mdns_timeout=0, app=self)
        self._browser = browser

        browser.aio_zc = AsyncZeroconf(**browser.get_zeroconf_kwargs())
        browser.aio_browser = AsyncServiceBrowser(
            browser.aio_zc.zeroconf,
            SERVICES,
            handlers=[browser.async_on_service_state_change],
        )

        await asyncio.sleep(discovery_time)

        if browser.services:
            await asyncio.gather(*browser.services, return_exceptions=True)

        browser._assemble_completed_services()
        await browser.async_close()
        self._browser = None

        device_ips = [
            str(device.ipv4) for device in self.devices.values() if device.ipv4 and not device.requires_managed_control
        ]
        if device_ips:
            await self.cmc.register_all(device_ips)

        populate_tasks = []
        for device in self.devices.values():
            if self.get_arc_port(device):
                populate_tasks.append(self._populate_device_controls(device))

        if populate_tasks:
            done, pending = await asyncio.wait(
                [asyncio.create_task(task) for task in populate_tasks],
                timeout=populate_time,
            )
            for task in pending:
                task.cancel()

        await self._query_settings_fields()

        await self._query_conmon_all()

        await self._probe_interface_status()
        await self._probe_preferred_leader_all()
        await asyncio.gather(
            self._probe_aes67_all(),
            self._probe_sample_rates_all(),
            self._probe_encodings_all(),
            self._probe_gain_levels_all(),
            self._probe_sample_rate_pullups_all(),
        )

        return self.devices

    async def discover_named_device(self, device_name: str, timeout: float = 2.0) -> dict:
        from zeroconf import ServiceStateChange
        from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

        from netaudio.dante.browser import DanteBrowser

        browser = DanteBrowser(mdns_timeout=0, app=self)
        self._browser = browser
        browser.aio_zc = AsyncZeroconf(**browser.get_zeroconf_kwargs())
        event_loop = asyncio.get_running_loop()
        deadline = event_loop.time() + timeout
        expected_arc_service_name = f"{device_name}.{SERVICE_ARC}"
        arc_service_future = event_loop.create_future()
        arc_resolution_tasks = set()

        async def resolve_arc_service(zeroconf, service_type, service_name):
            try:
                service = await browser.async_parse_netaudio_service(zeroconf, service_type, service_name)
            except (OSError, ValueError) as exception:
                logger.warning(f"Failed to resolve {service_name}: {exception}")
                return
            if service is not None and not arc_service_future.done():
                arc_service_future.set_result(service)

        def handle_arc_service(zeroconf, service_type, name, state_change):
            if state_change is ServiceStateChange.Removed:
                return
            if name.casefold() != expected_arc_service_name.casefold():
                return

            resolution_task = asyncio.create_task(resolve_arc_service(zeroconf, service_type, name))
            arc_resolution_tasks.add(resolution_task)
            resolution_task.add_done_callback(arc_resolution_tasks.discard)

        browser.aio_browser = AsyncServiceBrowser(
            browser.aio_zc.zeroconf,
            [SERVICE_ARC],
            handlers=[handle_arc_service],
        )
        direct_resolution_task = asyncio.create_task(
            resolve_arc_service(
                browser.aio_zc.zeroconf,
                SERVICE_ARC,
                expected_arc_service_name,
            )
        )
        arc_resolution_tasks.add(direct_resolution_task)
        direct_resolution_task.add_done_callback(arc_resolution_tasks.discard)
        optional_service_tasks = {}
        completed_optional_tasks = set()
        try:
            try:
                arc_service = await asyncio.wait_for(arc_service_future, timeout=timeout)
            except asyncio.TimeoutError:
                return {}

            remaining_time = max(0.0, deadline - event_loop.time())
            optional_service_tasks = {
                asyncio.create_task(
                    browser.async_parse_netaudio_service(
                        browser.aio_zc.zeroconf,
                        service_type,
                        f"{device_name}.{service_type}",
                    )
                ): service_type
                for service_type in (SERVICE_CMC, SERVICE_DBC)
            }
            if remaining_time > 0:
                completed_optional_tasks, _ = await asyncio.wait(optional_service_tasks, timeout=remaining_time)
        finally:
            unfinished_tasks = [task for task in (*arc_resolution_tasks, *optional_service_tasks) if not task.done()]
            for task in unfinished_tasks:
                task.cancel()
            if unfinished_tasks:
                await asyncio.gather(*unfinished_tasks, return_exceptions=True)
            await browser.async_close()
            self._browser = None

        services = [arc_service]
        for task in completed_optional_tasks:
            exception = task.exception()
            if exception is not None:
                logger.warning(f"Failed to resolve {device_name} {optional_service_tasks[task]} service: {exception}")
                continue
            service = task.result()
            if service is not None:
                services.append(service)

        server_name = arc_service["server_name"]
        matching_services = {service["name"]: service for service in services}
        if len({service["server_name"] for service in services}) > 1:
            server_name = f"{device_name}.local."
        device = self._apply_discovered_services(server_name, matching_services)
        return {server_name: device}

    async def execute(self, target, specification: dict) -> bytes | None:
        resolved = self._control_target(target)
        if getattr(resolved, "requires_managed_control", False):
            return await resolved.execute(specification)
        address = resolved._require_address() if hasattr(resolved, "_require_address") else str(resolved)
        return await self.transport.execute(address, specification, arc_port=self.arc_port_for_address(address))

    async def export_capability_partition(
        self,
        device_ip_address: str,
        timeout: float = 15.0,
    ) -> CapabilityPartitionExport:
        device_ip_address = str(device_ip_address)

        async def request() -> None:
            await self.send_capability_partition_export_request(device_ip_address)

        export = await self._export_conmon_data(
            device_ip_address,
            b"CAP1",
            2,
            request,
            timeout,
            "CAP1 partition export",
        )
        return parse_capability_partition_export(export)

    async def export_device_logs(
        self,
        device_ip_address: str,
        timeout: float = 15.0,
    ) -> DeviceLogExport:
        device_ip_address = str(device_ip_address)

        async def request() -> None:
            await self.send_device_log_export_request(device_ip_address)

        try:
            export = await self._export_conmon_data(
                device_ip_address,
                b"LOGS",
                1,
                request,
                timeout,
                "device log export",
            )
        except ConmonExportUnavailableError:
            device = self._device_by_ip(device_ip_address)
            if device is not None:
                apply_device_status(
                    device,
                    STATUS_KIND_DIAGNOSTIC_LOG_EXPORT,
                    {"diagnostic_log_export_supported": False},
                )
            raise
        result = parse_device_log_export(export)
        device = self._device_by_ip(device_ip_address)
        if device is not None:
            apply_device_status(
                device,
                STATUS_KIND_DIAGNOSTIC_LOG_EXPORT,
                device_audio_capability_fields(result.audio_capabilities),
            )
        return result

    async def factory_reset(self, device, host_mac=None) -> None:
        await self._send_registered_system_reset(device, self.commands.factory_reset, host_mac)

    async def get_aes67_configured(self, device):
        from netaudio import core

        if not getattr(device, "requires_managed_control", False) and device.ipv4 is None:
            return None
        try:
            response = await device.execute(self.commands.query_latency_config())
        except core.NetaudioCoreError as error:
            if error.status != core.STATUS_TIMEOUT:
                raise
            return None
        if response is None:
            return None
        configured = core.parse_response("aes67_configured", response)
        settings = core.parse_response("device_settings", response)
        prefix = settings.get("aes67_multicast_prefix") if isinstance(settings, dict) else None
        apply_device_status(
            device,
            STATUS_KIND_AES67,
            {"aes67_configured": configured, "aes67_multicast_prefix": prefix},
        )
        return configured

    def get_arc_port(self, device) -> int | None:
        if not device.services:
            return None

        for service_data in device.services.values():
            if service_data.get("type") == SERVICE_ARC:
                return service_data.get("port")

        return None

    async def get_device_settings(self, device):
        if getattr(device, "requires_managed_control", False):
            return await self.get_latency_settings(device)
        if device.ipv4 is None:
            return None
        settings = await device.call_core(lambda client: client.get_device_settings())
        self._apply_device_settings(device, settings)
        return settings

    async def get_latency_settings(self, device):
        from netaudio import core

        response = await device.execute(self.commands.query_latency_config())
        if response is None:
            return None
        settings = core.parse_response("device_settings", response)
        self._apply_device_settings(device, settings)
        return settings

    async def identify(self, device) -> None:
        await self.send_identify(device)

    async def lock_device(self, device, pin: str, key: bytes) -> dict:
        key_error = _validate_lock_key(key)
        if key_error:
            return key_error
        if getattr(device, "requires_managed_control", False):
            raise RuntimeError("device lock has no verified DDM operation and was not sent")
        if getattr(device, "ipv4", None) is None:
            raise RuntimeError("device has no control address")
        return await core_lock_device(str(device.ipv4), pin, key)

    def mark_device_offline(self, server_name: str) -> None:
        device = self.devices.get(server_name)
        if device and device.online:
            device.online = False
            device.supported_sample_rates = None
            device.supported_encodings = None
            device.aes67_supported = None
            device.aes67_multicast_prefix = None
            device.settings_properties = None
            device.sample_rate_pullup_raw_value = None
            device.requested_sample_rate_pullup_raw_value = None
            device.supported_sample_rate_pullup_raw_values = None
            device.transmitter_flows = None
            device.tx_flow_count = None
            device.receiver_flows = None
            device.rx_flow_count = None
            device.gain_device_type = None
            device.gain_levels = None
            device.supported_gain_levels = None
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_REMOVED,
                    device_name=device.name,
                    server_name=server_name,
                )
            )

    async def mutate_and_wait_for_capability_value(
        self,
        device,
        mutate: Callable[[], Awaitable[object]],
        capability_name: str,
        expected_value: int,
        probe_status: Callable[[], Awaitable[tuple[int, list[int]] | None]],
        timeout: float = 2.0,
    ) -> tuple[int, list[int]] | None:
        key = self._control_key(device)

        async def mutate_without_result() -> None:
            await mutate()

        return await mutate_and_wait_for_capability_value(
            self.notifications,
            capability_name,
            key,
            expected_value,
            mutate_without_result,
            probe_status,
            timeout,
        )

    async def mutate_and_wait_for_notification(
        self,
        device,
        mutate: Callable[[], Awaitable[object]],
        notification_ids,
        timeout: float = 2.0,
    ):
        if getattr(device, "requires_managed_control", False):
            return await mutate()
        device_ip_address = str(device.ipv4)
        waiter = self.notifications.register_notification_waiter(device_ip_address, notification_ids)
        try:
            response = await mutate()
            try:
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug(f"No mutation notification from {device_ip_address} within {timeout}s")
            return response
        finally:
            self.notifications.unregister_waiter(waiter)

    async def populate_controls(self, devices: dict | None = None, include_channels: bool = True) -> None:
        if devices is None:
            devices = self.devices

        tasks = []
        for device in devices.values():
            if device.requires_managed_control or self.get_arc_port(device):
                tasks.append(self._populate_device_controls(device, include_channels=include_channels))
            else:
                logger.debug(f"No ARC port for {device.server_name}, skipping controls")

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def populate_device_names(
        self,
        devices: dict,
        request_timeout_milliseconds: int | None = None,
        request_attempts: int | None = None,
    ) -> None:
        name_tasks = {
            asyncio.create_task(
                device.fetch_device_name(
                    request_timeout_milliseconds=request_timeout_milliseconds,
                    request_attempts=request_attempts,
                )
            ): device
            for device in devices.values()
            if device.requires_managed_control or self.get_arc_port(device)
        }
        if not name_tasks:
            return

        name_results = await asyncio.gather(*name_tasks, return_exceptions=True)
        for task, result in zip(name_tasks, name_results):
            device = name_tasks[task]
            if isinstance(result, Exception):
                logger.debug(f"Failed to read device name from {device.server_name}: {result}")
            elif result:
                device.name = result

    async def populate_devices(
        self,
        devices: dict,
        timeout: float = 2.0,
        include_channels: bool = True,
    ) -> None:
        device_ip_addresses = [
            str(device.ipv4) for device in devices.values() if device.ipv4 and not device.requires_managed_control
        ]
        request_timeout_milliseconds = None if include_channels else 500
        request_attempts = None if include_channels else 1

        phase_coroutines = [
            ("settings", self._query_settings_fields(devices)),
            ("ConMon", self._query_conmon_all(timeout=timeout, devices=devices)),
            ("interfaces", self._probe_interface_status(timeout=timeout, devices=devices)),
            ("preferred leader", self._probe_preferred_leader_all(timeout=timeout, devices=devices)),
            ("AES67", self._probe_aes67_all(timeout=timeout, devices=devices)),
            ("sample rates", self._probe_sample_rates_all(timeout=timeout, devices=devices)),
            ("encodings", self._probe_encodings_all(timeout=timeout, devices=devices)),
            ("gain levels", self._probe_gain_levels_all(timeout=timeout, devices=devices)),
            ("sample rate pull-ups", self._probe_sample_rate_pullups_all(timeout=timeout, devices=devices)),
        ]
        if device_ip_addresses:
            phase_coroutines.append(("CMC registration", self.cmc.register_all(device_ip_addresses)))

        control_coroutines = []
        for device in devices.values():
            if device.requires_managed_control or self.get_arc_port(device):
                control_coroutines.append(
                    self._populate_device_controls(
                        device,
                        include_channels=include_channels,
                        request_timeout_milliseconds=request_timeout_milliseconds,
                        request_attempts=request_attempts,
                    )
                )

        phase_tasks = {
            asyncio.create_task(phase_coroutine): phase_name for phase_name, phase_coroutine in phase_coroutines
        }
        control_tasks = [asyncio.create_task(control_coroutine) for control_coroutine in control_coroutines]
        try:
            completed_tasks, pending_tasks = await asyncio.wait(phase_tasks, timeout=timeout)
        finally:
            unfinished_tasks = [task for task in phase_tasks if not task.done()]
            for task in unfinished_tasks:
                task.cancel()
            if unfinished_tasks:
                await asyncio.gather(*unfinished_tasks, return_exceptions=True)
            if control_tasks:
                await asyncio.gather(*control_tasks, return_exceptions=True)
        if pending_tasks:
            pending_names = ", ".join(sorted(phase_tasks[task] for task in pending_tasks))
            logger.debug(f"Device detail deadline reached while waiting for: {pending_names}")
        for task in completed_tasks:
            exception = task.exception()
            if exception is not None:
                logger.warning(f"Failed to populate {phase_tasks[task]}: {exception}")

    async def probe_aes67_state(self, target, timeout: float = 2.0) -> tuple[bool | None, bool | None]:
        return await self._probe_once(
            "aes67",
            target,
            self.send_probe_aes67,
            timeout,
            "AES67 status",
        )

    async def probe_bluetooth_status(self, device, timeout: float = 2.0) -> dict:
        status = await self._probe_once(
            "bluetooth_status",
            device,
            self.send_bluetooth_status_request,
            timeout,
            "bluetooth status",
        )
        apply_device_status(device, STATUS_KIND_BLUETOOTH, status)
        return status

    async def probe_clear_configuration_status(
        self,
        target,
        timeout: float = 2.0,
    ) -> dict:
        return await self._probe_once(
            "clear_configuration_status",
            target,
            self.send_probe_clear_configuration_status,
            timeout,
            "clear-configuration status",
        )

    async def probe_clocking_status(self, device, timeout: float = 3.0) -> dict:
        key = self._control_key(device)
        async with self._capability_probe_lock("clock_status", key):
            waiter = self.notifications.register_waiter(STATUS_KIND_CLOCK, key)
            try:
                await self.send_refresh_clock_status(device)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    logger.debug(f"Clock status probe timed out for {key}")
                    if waiter.latest_result is None:
                        raise CapabilityProbeTimeout(f"clock status probe timed out for {key}") from None
            finally:
                self.notifications.unregister_waiter(waiter)
        if waiter.latest_result is None:
            raise RuntimeError(f"clock status readback was unavailable for {key}")
        apply_device_status(device, STATUS_KIND_CLOCK, waiter.latest_result)
        return _clock_status_snapshot(device)

    async def probe_encoding_status(self, target, timeout: float = 2.0) -> tuple[int, list[int]]:
        return await self._probe_with_retries(
            "encoding",
            target,
            self.send_probe_encoding,
            timeout,
            "encoding",
        )

    async def probe_gain_status(
        self,
        target,
        timeout: float = 2.0,
    ) -> tuple[str, list[int]]:
        key = self._control_key(target)
        async with self._capability_probe_lock("gain", key):
            result = await send_and_wait_for_gain_status(
                self.notifications,
                key,
                lambda: self.send_probe_gain_level(target),
                timeout,
            )
        if result is None:
            raise CapabilityProbeTimeout(f"gain status readback timed out for {key}")
        return result

    async def probe_interface_status(self, target, timeout: float = 2.0) -> list[dict]:
        return await self._probe_once(
            "interface",
            target,
            self.send_probe_interface_status,
            timeout,
            "interface status",
        )

    async def probe_link_status(
        self,
        target,
        timeout: float = 2.0,
    ) -> LinkStatusObservation:
        return await self._probe_once(
            "link_status",
            target,
            self.send_probe_link_status,
            timeout,
            "link status",
        )

    async def probe_lock_status(
        self,
        target,
        timeout: float = 2.0,
    ) -> LockStatusObservation:
        return await self._probe_once(
            "lock_status",
            target,
            self.send_probe_lock_reset_status,
            timeout,
            "lock status",
        )

    async def probe_preferred_leader_state(self, target, timeout: float = 2.0) -> bool | None:
        resolved = self._control_target(target)
        if getattr(resolved, "requires_managed_control", False):
            fresh = await self.managed_transport(resolved).fetch_device(resolved)
            if fresh.clock_preferences is None or fresh.clock_preferences.leader is None:
                raise RuntimeError(f"preferred leader readback was unavailable for {self._control_key(resolved)}")
            resolved.preferred_leader = fresh.clock_preferences.leader
            return fresh.clock_preferences.leader
        return await self._probe_once(
            "preferred_leader",
            resolved,
            self.send_probe_preferred_leader,
            timeout,
            "preferred leader",
        )

    async def probe_sample_rate_pullup_status(
        self,
        target,
        timeout: float = 2.0,
    ) -> tuple[int, list[int]]:
        return await self._probe_with_retries(
            "sample_rate_pullup",
            target,
            self.send_probe_sample_rate_pullup,
            timeout,
            "sample rate pull-up",
        )

    async def probe_sample_rate_status(self, target, timeout: float = 2.0) -> tuple[int, list[int]]:
        return await self._probe_with_retries(
            "sample_rate",
            target,
            self.send_probe_sample_rate,
            timeout,
            "sample rate",
        )

    async def probe_switch_configuration(
        self,
        target,
        timeout: float = 2.0,
    ) -> dict:
        return await self._probe_once(
            "switch_configuration",
            target,
            self.send_probe_switch_configuration,
            timeout,
            "switch configuration",
        )

    async def query_modern_arc_receiver_channel_status(self, device):
        return await self._query_channel_status_pages(device, "rx")

    async def query_modern_arc_receiver_flow_status(self, device):
        protocol_id = modern_arc_protocol_identifier_for_device(device)
        return await self._query_modern_arc_status_page(
            device,
            self.commands.query_modern_arc_receiver_flow_status(protocol_id),
            "receiver flow status query",
            "modern_arc_receiver_flow_status_page",
        )

    async def query_modern_arc_transmitter_channel_status(self, device):
        return await self._query_channel_status_pages(device, "tx")

    async def query_modern_arc_transmitter_flow_status(self, device):
        protocol_id = modern_arc_protocol_identifier_for_device(device)
        return await self._query_modern_arc_status_page(
            device,
            self.commands.query_modern_arc_transmitter_flow_status(protocol_id),
            "transmitter flow status query",
            "transmitter_flow_status_page",
        )

    async def reboot(self, device, host_mac=None) -> None:
        await self._send_registered_system_reset(device, self.commands.reboot, host_mac)

    def register_device(self, server_name: str, device) -> None:
        existing = self.devices.get(server_name)

        if existing is not None:
            if not existing.online:
                existing.online = True
                existing.update_last_seen()
                if device.ipv4:
                    existing.ipv4 = device.ipv4
                if device.services:
                    existing.services = device.services

            self.devices[server_name] = existing
            self.state.apply_pending_for_device(existing)
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_UPDATED,
                    device_name=existing.name,
                    server_name=server_name,
                )
            )
        else:
            device._app = self
            device.update_last_seen()
            self.devices[server_name] = device
            self.state.apply_pending_for_device(device)
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_DISCOVERED,
                    device_name=device.name,
                    server_name=server_name,
                )
            )

    async def remove_subscriptions(self, device, channel_numbers):
        if getattr(device, "requires_managed_control", False):
            async with device.topology_mutation_lock:
                return await self.managed_transport(device).remove_subscriptions(device, channel_numbers)
        return await self.mutate_and_wait_for_notification(
            device,
            lambda: self.send_remove_subscriptions(device, channel_numbers),
            SUBSCRIPTION_NOTIFICATION_IDS,
        )

    async def reset_channel_name(self, device, channel_type: str, channel_number: int):
        return await device.execute(self.commands.reset_channel_name(channel_type, channel_number))

    async def reset_device_name(self, device):
        if getattr(device, "requires_managed_control", False):
            return await self.managed_transport(device).reset_device_name(device)
        return await device.execute(self.commands.reset_name())

    async def resolve_channel_name_protocol_identifier(self, device, channel_type: str):
        if channel_type == "rx":
            attribute_name = "receiver_channel_name_protocol_identifier"
            resolve = receiver_channel_name_protocol_identifier_from_probe
        else:
            attribute_name = "transmitter_channel_name_protocol_identifier"
            resolve = transmitter_channel_name_protocol_identifier_from_probe
        cached_protocol_identifier = getattr(device, attribute_name, None)
        if cached_protocol_identifier is not None:
            return cached_protocol_identifier

        try:
            protocol_identifier = modern_arc_protocol_identifier_for_device(device)
        except RuntimeError:
            protocol_identifier = PROTOCOL_ARC_2809
        response = await device.execute(
            channel_status_query_specification(channel_type, protocol_id=protocol_identifier)
        )
        protocol_identifier = resolve(response)
        setattr(device, attribute_name, protocol_identifier)
        return protocol_identifier

    async def send_add_subscriptions(self, device, records):
        async with device.topology_mutation_lock:
            if self._uses_modern_arc_280f(device):
                return await self._send_modern_arc_subscription_records(
                    device,
                    [
                        {
                            "action": "set",
                            "rx_channel": rx_channel,
                            "tx_channel": tx_channel,
                            "tx_device": tx_device,
                        }
                        for rx_channel, tx_channel, tx_device in records
                    ],
                )
            return await device.execute(self.commands.add_subscriptions(records))

    @staticmethod
    def _uses_modern_arc_280f(device) -> bool:
        return any(
            service.get("type") == SERVICE_ARC and (service.get("properties") or {}).get("arcp_vers") == "2.8.15"
            for service in (getattr(device, "services", None) or {}).values()
            if isinstance(service, dict)
        )

    async def _send_modern_arc_subscription_records(self, device, records: list[dict]):
        protocol_id = modern_arc_protocol_identifier_for_device(device)
        receiver_count = len(device.rx_channels)
        if receiver_count == 0:
            raise RuntimeError("modern ARC subscription requires populated receiver channels")
        page_capacity = min(32, receiver_count)
        grouped: dict[int, list[dict]] = {}
        for record in records:
            channel_number = record["rx_channel"]
            channel = device.rx_channels.get(channel_number)
            media_type_code = getattr(channel, "media_type_code", None)
            if media_type_code not in (3, 4):
                raise RuntimeError(f"receiver channel {channel_number} has no supported media type")
            grouped.setdefault(media_type_code, []).append(record)

        response = None
        for media_type_code, media_records in grouped.items():
            for start in range(0, len(media_records), page_capacity):
                response = await device.execute(
                    self.commands.modern_arc_subscription_page(
                        protocol_id,
                        page_capacity,
                        media_type_code,
                        media_records[start : start + page_capacity],
                    )
                )
        return response

    async def send_bluetooth_status_request(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.bluetooth_status(host_mac))

    async def send_capability_partition_export_request(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.capability_partition_export(host_mac))

    async def send_clear_all_configuration(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.clear_all_configuration(host_mac))

    async def send_clear_all_configuration_preserving_internet_protocol_settings(
        self,
        device_ip_address,
        host_mac=None,
    ) -> None:
        await self._send_settings(
            device_ip_address,
            self.commands.clear_all_configuration_preserving_internet_protocol_settings(host_mac),
        )

    async def send_dante_model_request(self, device_ip_address, mac) -> None:
        await self._send_settings(device_ip_address, self.commands.dante_model(mac))

    async def send_device_log_export_request(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.device_log_export(host_mac))

    async def send_enable_aes67(self, device_ip_address, is_enabled: bool, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.enable_aes67(is_enabled, host_mac))

    async def send_identify(self, device_ip_address) -> None:
        await self._send_settings(device_ip_address, self.commands.identify())

    async def send_make_model_request(self, device_ip_address, mac) -> None:
        await self._send_settings(device_ip_address, self.commands.make_model(mac))

    async def send_probe_aes67(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_aes67(host_mac))

    async def send_probe_clear_configuration_status(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_clear_configuration_status(host_mac))

    async def send_probe_encoding(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_encoding(host_mac))

    async def send_probe_gain_level(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_gain_level(host_mac))

    async def send_probe_interface_status(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_interface_status(host_mac))

    async def send_probe_link_status(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_link_status(host_mac))

    async def send_probe_lock_reset_status(self, device_ip_address, host_mac=None, request_value: int = 100) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_lock_reset_status(host_mac, request_value))

    async def send_probe_preferred_leader(self, device_ip_address, clock_source: int = 0, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_preferred_leader(clock_source, host_mac))

    async def send_probe_sample_rate(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_sample_rate(host_mac))

    async def send_probe_sample_rate_pullup(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_sample_rate_pullup(host_mac))

    async def send_probe_switch_configuration(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.probe_switch_configuration(host_mac))

    async def send_refresh_clock_status(self, device_ip_address, host_mac=None, sequence: int = 0x0021) -> None:
        await self._send_settings(device_ip_address, self.commands.refresh_clock_status(host_mac, sequence))

    async def send_remove_subscriptions(self, device, channel_numbers):
        async with device.topology_mutation_lock:
            if self._uses_modern_arc_280f(device):
                return await self._send_modern_arc_subscription_records(
                    device,
                    [{"action": "clear", "rx_channel": channel_number} for channel_number in channel_numbers],
                )
            return await device.execute(self.commands.remove_subscriptions(channel_numbers))

    async def send_set_channel_name(self, device, channel_type, channel_number, name, protocol_id=None):
        if protocol_id is None:
            protocol_id = await self.resolve_channel_name_protocol_identifier(device, channel_type)
        return await device.execute(self.commands.set_channel_name(channel_type, channel_number, name, protocol_id))

    async def send_set_clock_source(self, device_ip_address, clock_source: int, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.set_clock_source(clock_source, host_mac))

    async def send_set_clock_subdomain(self, device_ip_address, subdomain, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.set_clock_subdomain(subdomain, host_mac))

    async def send_set_encoding(self, device, encoding: int) -> None:
        supported_encodings = device.supported_encodings
        if supported_encodings is not None and encoding not in supported_encodings:
            raise ValueError(f"requested encoding {encoding} is not supported; device reports {supported_encodings}")
        await self._send_settings(device, self.commands.set_encoding(encoding))

    async def send_set_gain_level(
        self,
        device_ip_address,
        channel_number: int,
        gain_level: int,
        device_type: str,
        host_mac=None,
    ) -> None:
        await self._send_settings(
            device_ip_address,
            self.commands.set_gain_level(channel_number, gain_level, device_type, host_mac),
        )

    async def send_set_interface_dhcp(self, device_ip_address, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.set_interface_dhcp(host_mac))

    async def send_set_interface_static(
        self,
        device_ip_address,
        ip_address: str,
        netmask: str,
        dns_server: str,
        gateway: str,
        host_mac=None,
    ) -> None:
        await self._send_settings(
            device_ip_address,
            self.commands.set_interface_static(ip_address, netmask, dns_server, gateway, host_mac),
        )

    async def send_set_preferred_leader(
        self,
        device_ip_address,
        is_preferred: bool,
        clock_source: int = 0,
        host_mac=None,
    ) -> None:
        await self._send_settings(
            device_ip_address,
            self.commands.set_preferred_leader(is_preferred, clock_source, host_mac),
        )

    async def send_set_sample_rate(self, device_ip_address, sample_rate: int) -> None:
        await self._send_settings(device_ip_address, self.commands.set_sample_rate(sample_rate))

    async def send_set_sample_rate_pullup(self, device_ip_address, raw_value: int, host_mac=None) -> None:
        await self._send_settings(device_ip_address, self.commands.set_sample_rate_pullup(raw_value, host_mac))

    async def set_aes67_enabled(self, device, is_enabled: bool, timeout: float = 2.0):
        async def mutate() -> None:
            await self.send_enable_aes67(device, is_enabled)
            await self.send_probe_aes67(device)

        return await self._mutate_and_take_result("aes67", device, mutate, timeout)

    async def set_aes67_multicast_prefix(self, device, prefix: str) -> str | None:
        from netaudio.dante.device import device_advertises_aes67_multicast_prefix

        try:
            normalized_prefix = str(ipaddress.IPv4Address(prefix))
        except (ipaddress.AddressValueError, ValueError) as exception:
            raise ValueError("AES67 multicast prefix must be an IPv4 address") from exception
        if not device_advertises_aes67_multicast_prefix(device):
            raise ValueError("device does not advertise an AES67 multicast prefix")
        await self.mutate_and_wait_for_notification(
            device,
            lambda: device.execute(self.commands.set_aes67_multicast_prefix(normalized_prefix)),
            (NOTIFICATION_SETTINGS_CHANGE,),
        )
        await self.get_aes67_configured(device)
        return device.aes67_multicast_prefix

    async def set_channel_name(self, device, channel_type: str, channel_number: int, name: str):
        return await self.mutate_and_wait_for_notification(
            device,
            lambda: self.send_set_channel_name(device, channel_type, channel_number, name),
            CHANNEL_NAME_NOTIFICATION_IDS[channel_type],
        )

    async def set_clock_source(self, device, clock_source: int, timeout: float = 4.0) -> int | None:
        if isinstance(clock_source, bool) or not isinstance(clock_source, int) or not 0 <= clock_source <= 0xFFFF:
            raise ValueError("clock_source must be an integer from 0 through 65535")
        await self.send_set_clock_source(device, clock_source)
        parsed = await self.probe_clocking_status(device, timeout=timeout)
        return parsed["clock_source_code"]

    async def set_clock_subdomain(self, device, subdomain, timeout: float = 4.0) -> bytes | None:
        from netaudio.dante.clock_config import clock_subdomain_bytes

        normalized = clock_subdomain_bytes(subdomain)
        if normalized is None:
            raise ValueError("clock subdomain must be at most 16 bytes")
        await self.send_set_clock_subdomain(device, normalized)
        parsed = await self.probe_clocking_status(device, timeout=timeout)
        clock_subdomain = parsed.get("clock_subdomain")
        return bytes(clock_subdomain) if clock_subdomain is not None else None

    async def set_device_name(self, device, name: str):
        error = validate_dante_name(name)
        if error:
            raise ValueError(error)
        if getattr(device, "requires_managed_control", False):
            return await self.managed_transport(device).set_device_name(device, name)
        return await self.mutate_and_wait_for_notification(
            device,
            lambda: device.execute(self.commands.set_name(name)),
            DEVICE_NAME_NOTIFICATION_IDS,
        )

    async def set_encoding(self, device, encoding: int, timeout: float = 2.0) -> tuple[int, list[int]] | None:
        if isinstance(encoding, bool) or not isinstance(encoding, int) or not 0 < encoding <= 0xFFFFFFFF:
            raise ValueError("encoding must be an integer from 1 through 4294967295")
        supported_encodings = device.supported_encodings
        if supported_encodings is not None and encoding not in supported_encodings:
            raise ValueError(f"requested encoding {encoding} is not supported; device reports {supported_encodings}")

        async def mutate() -> None:
            await self.send_set_encoding(device, encoding)

        result = await self.mutate_and_wait_for_capability_value(
            device,
            mutate,
            "encoding",
            encoding,
            lambda: self.probe_encoding_status(device, timeout=timeout),
            timeout,
        )
        if result is not None:
            self._apply_encoding_capability(device, *result)
        return result

    async def set_gain_level(
        self,
        device,
        channel_number: int,
        gain_level: int,
        device_type: str,
        timeout: float = 4.0,
    ) -> tuple[str, list[int]] | None:
        if device_type not in ("input", "output"):
            raise ValueError("device_type must be 'input' or 'output'")
        if isinstance(channel_number, bool) or not isinstance(channel_number, int) or not 1 <= channel_number <= 0xFFFF:
            raise ValueError("channel_number must be an integer from 1 through 65535")
        if isinstance(gain_level, bool) or not isinstance(gain_level, int) or gain_level not in SUPPORTED_GAIN_LEVELS:
            raise ValueError("gain_level must be an integer from 1 through 5")
        if device.gain_device_type is not None and device.gain_device_type != device_type:
            raise ValueError(f"device reports {device.gain_device_type} gain controls, not {device_type}")
        if device.supported_gain_levels is not None and gain_level not in device.supported_gain_levels:
            raise ValueError(
                f"requested gain level {gain_level} is not supported; device reports {device.supported_gain_levels}"
            )

        key = self._control_key(device)
        async with self._capability_probe_lock("gain", key):
            result = await send_and_wait_for_gain_status(
                self.notifications,
                key,
                lambda: self.send_set_gain_level(
                    device,
                    channel_number,
                    gain_level,
                    device_type,
                ),
                timeout,
                expected_device_type=device_type,
                channel_number=channel_number,
                expected_level=gain_level,
            )
            if result is not None:
                observed_device_type, channel_levels = result
                self._apply_gain_capability(device, observed_device_type, channel_levels)
            return result

    async def set_interface(self, device, mode: str, static_configuration: dict | None = None) -> list[dict] | None:
        if mode == "dhcp":
            return await self.set_interface_dhcp(device)
        return await self.set_interface_static(
            device,
            static_configuration["ip_address"],
            static_configuration["netmask"],
            static_configuration["dns_server"],
            static_configuration["gateway"],
        )

    async def set_interface_dhcp(self, target, timeout: float = 2.0) -> list[dict] | None:
        return await self._mutate_and_take_result(
            "interface",
            target,
            lambda: self.send_set_interface_dhcp(target),
            timeout,
        )

    async def set_interface_static(
        self,
        target,
        ip_address: str,
        netmask: str,
        dns_server: str,
        gateway: str,
        timeout: float = 2.0,
    ) -> list[dict] | None:
        return await self._mutate_and_take_result(
            "interface",
            target,
            lambda: self.send_set_interface_static(target, ip_address, netmask, dns_server, gateway),
            timeout,
        )

    async def set_latency(self, device, milliseconds: float):
        latency_milliseconds = float(milliseconds)
        if not math.isfinite(latency_milliseconds) or latency_milliseconds < 0:
            raise ValueError("latency must be a finite, nonnegative number")
        return await self.mutate_and_wait_for_notification(
            device,
            lambda: device.execute(self.commands.set_latency(latency_milliseconds)),
            (NOTIFICATION_LATENCY_CHANGE, NOTIFICATION_SETTINGS_CHANGE),
        )

    async def set_preferred_leader(
        self,
        device,
        is_preferred: bool,
        timeout: float = 2.0,
    ) -> bool | None:
        if getattr(device, "requires_managed_control", False):
            await self.managed_transport(device).set_preferred_leader(device, is_preferred)
            return await self.probe_preferred_leader_state(device, timeout=timeout)
        device_ip_address = device._require_address()

        async def mutate() -> None:
            await self.send_set_preferred_leader(device_ip_address, is_preferred)
            await self.send_probe_preferred_leader(device_ip_address)

        return await self._mutate_and_take_result("preferred_leader", device_ip_address, mutate, timeout)

    async def set_sample_rate(
        self,
        device,
        sample_rate_hertz: int,
        confirm_destructive: bool = False,
        timeout: float = 4.0,
    ):
        from netaudio.dante.sample_rate_topology import change_sample_rate_topology_safe

        async def probe():
            return await self.probe_sample_rate_status(device, timeout=timeout)

        async def mutate() -> None:
            await self.send_set_sample_rate(device, sample_rate_hertz)

        async with device.topology_mutation_lock:
            return await change_sample_rate_topology_safe(
                device,
                sample_rate_hertz,
                probe,
                mutate,
                confirm_destructive=confirm_destructive,
            )

    async def set_sample_rate_pullup(
        self,
        device,
        raw_value: int,
        timeout: float = 4.0,
    ) -> tuple[int, list[int]] | None:
        if isinstance(raw_value, bool) or not isinstance(raw_value, int) or not 0 <= raw_value <= 0xFFFFFFFF:
            raise ValueError("raw_value must be an integer from 0 through 4294967295")
        supported_raw_values = device.supported_sample_rate_pullup_raw_values
        if supported_raw_values is not None and raw_value not in supported_raw_values:
            raise ValueError(
                f"requested sample rate pull-up value {raw_value} is not supported; "
                f"device reports {supported_raw_values}"
            )

        async def mutate() -> None:
            await self.send_set_sample_rate_pullup(device, raw_value)

        result = await self.mutate_and_wait_for_capability_value(
            device,
            mutate,
            "sample_rate_pullup_raw_value",
            raw_value,
            lambda: self.probe_sample_rate_pullup_status(device, timeout=timeout),
            timeout,
        )
        if result is not None:
            self._apply_sample_rate_pullup_capability(device, *result)
        return result

    async def shutdown(self) -> None:
        if not self._started:
            return

        await self.notifications.stop()
        await self.cmc.stop()
        await self.dispatcher.stop()

        if self._capture_writer_task is not None:
            self._capture_queue.put_nowait(None)
            try:
                await asyncio.wait_for(self._capture_writer_task, timeout=5)
            except asyncio.TimeoutError:
                logger.warning("Capture writer did not drain within 5s")
            self._capture_writer_task = None
            self._capture_queue = None
            self._capture_loop = None

        if self._browser:
            try:
                await self._browser.async_close()
            except OSError as exception:
                logger.warning(f"Failed to close discovery browser: {exception}")
            self._browser = None

        self.transport.close()
        self._capability_probe_locks.clear()
        self._started = False
        logger.info("DanteApplication shut down")

    async def startup(self) -> None:
        if self._started:
            return

        self._started = True
        if self.transport.observer is not None:
            self._capture_loop = asyncio.get_running_loop()
            self._capture_queue = asyncio.Queue()
            self._capture_writer_task = asyncio.create_task(self._capture_writer())

        try:
            self.state.attach()
            await self.dispatcher.start()
            await self.notifications.start()
        except BaseException:
            await self.shutdown()
            raise
        logger.info("DanteApplication started")

    async def unlock_device(self, device, pin: str, key: bytes) -> dict:
        key_error = _validate_lock_key(key)
        if key_error:
            return key_error
        if getattr(device, "requires_managed_control", False):
            raise RuntimeError("device unlock has no verified DDM operation and was not sent")
        if getattr(device, "ipv4", None) is None:
            raise RuntimeError("device has no control address")
        return await core_unlock_device(str(device.ipv4), pin, key)

    def unregister_device(self, server_name: str) -> None:
        device = self.devices.pop(server_name, None)
        if device:
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_REMOVED,
                    device_name=device.name,
                    server_name=server_name,
                )
            )

    async def wait_for_discovery(self, timeout: float = 5.0) -> dict:
        from netaudio.dante.browser import DanteBrowser

        browser = DanteBrowser(mdns_timeout=timeout, app=self)
        self._browser = browser
        try:
            devices = await asyncio.wait_for(browser.get_devices(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.debug(f"mDNS discovery reached its {timeout}s deadline")
            devices = browser.devices
        finally:
            await browser.async_close()
            if self._browser is browser:
                self._browser = None

        if devices:
            self.devices.update(devices)
        self.media_services = dict(getattr(browser, "media_services", {}))

        return self.devices
