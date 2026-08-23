from __future__ import annotations

import asyncio
import csv
import io
import json as json_module
import logging
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from dataclasses import dataclass
from fnmatch import fnmatch
from glob import has_magic
from typing import Any, Awaitable, Callable, Optional

import typer

from netaudio import DanteDevice
from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.common.app_config import settings
from netaudio.daemon.client import get_devices_from_daemon
from netaudio.dante.application import DanteApplication
from netaudio.dante.capability_partition import (
    CapabilityPartitionExport,
    parse_capability_partition_export,
)
from netaudio.dante.conmon_export import ConmonExport, ConmonExportUnavailableError
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.core_capability_probe_operations import CoreCapabilityProbeOperations
from netaudio.dante.diagnostic_logs import (
    DeviceLogExport,
    apply_device_audio_capabilities,
    parse_device_log_export,
)
from netaudio.dante.latency import milliseconds_to_microseconds
from netaudio.dante.lock_status import LockStatusObservation

from netaudio._exit_codes import ExitCode
from netaudio.icons import icon

logger = logging.getLogger("netaudio")


class CapabilityProbeTimeout(RuntimeError):
    pass


@dataclass(frozen=True)
class ReadbackResult:
    matched: bool
    observed: Any = None
    observed_available: bool = False
    error: Optional[Exception] = None


async def readback_after_notification(
    read: Callable[[], Awaitable[Any]],
    expected: Any,
) -> ReadbackResult:
    try:
        observed = await read()
    except Exception as exception:
        return ReadbackResult(matched=False, error=exception)
    return ReadbackResult(
        matched=observed == expected,
        observed=observed,
        observed_available=True,
    )


class CoreCommandSender(CoreCapabilityProbeOperations):
    def __init__(self, observer=None, devices=None, packet_store=None, session_id=None):
        from netaudio import core

        self._core = core
        self._clients: dict[str, Any] = {}
        self._host_mac = core.host_mac()
        self._observer = observer
        self._devices = devices or {}
        self._packet_store = packet_store
        self._session_id = session_id
        self._dispatcher = None
        self._notifications = None
        self._settings_service = None
        self._notification_start_lock = DeferredAsyncioLock()
        self._settings_start_lock = DeferredAsyncioLock()
        self._capability_probe_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._client_request_locks: dict[str, asyncio.Lock] = {}

    async def __call__(
        self,
        packet: bytes,
        device_ip_address,
        port: int,
        *,
        expect_response: bool = True,
        repeat: int = 1,
        interval_ms: int = 0,
    ) -> bytes | None:
        address = str(device_ip_address)
        request_lock = self._client_request_locks.setdefault(address, asyncio.Lock())
        async with request_lock:
            client = self._clients.get(address)
            if client is None:
                client = self._core.CoreClient(address)
                if self._host_mac:
                    client.set_host_mac(self._host_mac)
                client.observer = self._observer
                self._clients[address] = client
            return await asyncio.to_thread(
                client.request,
                packet,
                port,
                expect_response,
                repeat,
                interval_ms,
            )

    async def _ensure_notifications(self):
        if self._notifications is not None:
            return self._notifications
        async with self._notification_start_lock:
            if self._notifications is not None:
                return self._notifications

            from netaudio.common.app_config import settings as app_settings
            from netaudio.dante.events import DanteEventDispatcher
            from netaudio.dante.services.notification import DanteNotificationService

            def device_lookup(device_ip_address):
                for device in self._devices.values():
                    if device.ipv4 and str(device.ipv4) == device_ip_address:
                        return device
                return None

            dispatcher = DanteEventDispatcher()
            notifications = DanteNotificationService(
                dispatcher=dispatcher,
                device_lookup=device_lookup,
                packet_store=self._packet_store,
                interface_ip=app_settings.interface_ip,
                dissect=_get_state().dissect,
            )
            notifications.session_id = self._session_id
            await dispatcher.start()
            try:
                await notifications.start()
            except BaseException:
                await dispatcher.stop()
                raise
            self._dispatcher = dispatcher
            self._notifications = notifications
            return notifications

    async def _ensure_settings_service(self):
        if self._settings_service is not None:
            return self._settings_service
        async with self._settings_start_lock:
            if self._settings_service is not None:
                return self._settings_service

            from netaudio.dante.services.settings import DanteSettingsService

            settings_service = DanteSettingsService(
                packet_store=self._packet_store,
                dissect=_get_state().dissect,
            )
            settings_service.session_id = self._session_id
            await settings_service.start()
            self._settings_service = settings_service
            return settings_service

    async def send_and_wait_for_notification(
        self,
        packet: bytes,
        device_ip_address,
        port: int,
        notification_ids,
        *,
        notification_timeout: float = 2.0,
        **send_options,
    ) -> bytes | None:
        notifications = await self._ensure_notifications()
        waiter = notifications.register_notification_waiter(str(device_ip_address), notification_ids)
        try:
            response = await self(packet, device_ip_address, port, **send_options)
            try:
                await asyncio.wait_for(waiter.event.wait(), timeout=notification_timeout)
            except asyncio.TimeoutError:
                return response
            return response
        finally:
            notifications.unregister_notification_waiter(waiter)

    async def send_and_wait_for_capability_value(
        self,
        packet: bytes,
        device_ip_address,
        port: int,
        capability_name: str,
        expected_value: int,
        probe_status,
        *,
        capability_timeout: float = 2.0,
        **send_options,
    ) -> tuple[int, list[int]] | None:
        from netaudio.dante.services.notification import mutate_and_wait_for_capability_value

        notifications = await self._ensure_notifications()
        address = str(device_ip_address)

        async def mutate() -> None:
            await self(packet, address, port, **send_options)

        return await mutate_and_wait_for_capability_value(
            notifications,
            capability_name,
            address,
            expected_value,
            mutate,
            probe_status,
            capability_timeout,
        )

    async def probe_clocking_status(self, device, timeout: float = 3.0) -> dict:
        notifications = await self._ensure_notifications()
        settings_service = await self._ensure_settings_service()
        device_ip_address = str(device.ipv4)
        waiter = notifications.register_preferred_leader_waiter(device_ip_address)
        try:
            settings_service.refresh_clock_status(device_ip_address)
            try:
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                pass
        finally:
            notifications.unregister_preferred_leader_waiter(device_ip_address)
        if device.clock_source_code is None:
            raise RuntimeError("clock status readback was unavailable")
        return {
            "clock_source_code": device.clock_source_code,
            "clock_subdomain": device.clock_subdomain,
            "preferred_leader": device.preferred_leader,
            "clock_role": device.clock_role,
            "clock_identity": device.clock_identity,
            "leader_clock_identity": device.leader_clock_identity,
            "clock_port_state_code": device.clock_port_state_code,
            "clock_port_records": device.clock_port_records,
            "clock_frequency_offset_parts_per_billion": device.clock_frequency_offset_parts_per_billion,
        }

    async def export_device_logs(self, device_ip_address, timeout: float = 15.0) -> DeviceLogExport:
        address = str(device_ip_address)
        settings_service = await self._ensure_settings_service()

        async def request() -> None:
            settings_service.request_device_log_export(address)

        try:
            export = await self._export_conmon_data(
                address,
                b"LOGS",
                1,
                request,
                timeout,
                "device log export",
            )
        except ConmonExportUnavailableError:
            for device in self._devices.values():
                if device.ipv4 and str(device.ipv4) == address:
                    device.diagnostic_log_export_supported = False
            raise
        result = parse_device_log_export(export)
        for device in self._devices.values():
            if device.ipv4 and str(device.ipv4) == address:
                apply_device_audio_capabilities(device, result.audio_capabilities)
        return result

    async def export_capability_partition(
        self,
        device_ip_address,
        timeout: float = 15.0,
    ) -> CapabilityPartitionExport:
        address = str(device_ip_address)
        settings_service = await self._ensure_settings_service()

        async def request() -> None:
            settings_service.request_capability_partition_export(address)

        export = await self._export_conmon_data(
            address,
            b"CAP1",
            2,
            request,
            timeout,
            "CAP1 partition export",
        )
        return parse_capability_partition_export(export)

    async def _export_conmon_data(
        self,
        device_ip_address: str,
        expected_echoed_tag: bytes,
        expected_selector_value: int,
        request,
        timeout: float,
        operation_name: str,
    ) -> ConmonExport:
        from netaudio.dante.services.notification import request_and_wait_for_conmon_export

        operation_lock = self._capability_probe_locks.setdefault(
            ("conmon_export", device_ip_address),
            asyncio.Lock(),
        )
        async with operation_lock:
            notifications = await self._ensure_notifications()
            result = await request_and_wait_for_conmon_export(
                notifications,
                device_ip_address,
                expected_echoed_tag,
                expected_selector_value,
                request,
                timeout,
            )
        if result is None:
            raise CapabilityProbeTimeout(f"{operation_name} timed out for {device_ip_address}")
        return result

    async def _probe_audio_capability(
        self,
        capability_name: str,
        device_ip_address,
        command_builder,
        register_waiter_name: str,
        get_result_name: str,
        unregister_waiter_name: str,
        capability_description: str,
        timeout: float,
    ):
        address = str(device_ip_address)
        probe_lock = self._capability_probe_locks.setdefault((capability_name, address), asyncio.Lock())
        async with probe_lock:
            notifications = await self._ensure_notifications()
            waiter = getattr(notifications, register_waiter_name)(address)
            try:
                packet, _, port = command_builder()
                await self(packet, address, port, expect_response=False)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    result = getattr(notifications, get_result_name)(address)
                    if result is None:
                        raise CapabilityProbeTimeout(f"{capability_description} readback timed out for {address}")
                    return result
                result = getattr(notifications, get_result_name)(address)
                if result is None:
                    raise RuntimeError(f"{capability_description} readback was unavailable for {address}")
                return result
            finally:
                getattr(notifications, unregister_waiter_name)(address)

    async def probe_gain_status(self, device_ip_address, timeout: float = 2.0):
        from netaudio.dante.services.notification import send_and_wait_for_gain_status

        address = str(device_ip_address)
        probe_lock = self._capability_probe_locks.setdefault(("gain", address), asyncio.Lock())
        async with probe_lock:
            notifications = await self._ensure_notifications()
            settings_service = await self._ensure_settings_service()
            return await send_and_wait_for_gain_status(
                notifications,
                address,
                lambda: settings_service.probe_gain_level(address, host_mac=self._host_mac),
                timeout,
            )

    async def set_gain_level(
        self,
        device_ip_address,
        channel_number: int,
        gain_level: int,
        device_type: str,
        timeout: float = 4.0,
    ):
        from netaudio.dante.services.notification import send_and_wait_for_gain_status

        address = str(device_ip_address)
        probe_lock = self._capability_probe_locks.setdefault(("gain", address), asyncio.Lock())
        async with probe_lock:
            notifications = await self._ensure_notifications()
            settings_service = await self._ensure_settings_service()
            return await send_and_wait_for_gain_status(
                notifications,
                address,
                lambda: settings_service.set_gain_level(
                    address,
                    channel_number,
                    gain_level,
                    device_type,
                    host_mac=self._host_mac,
                ),
                timeout,
                expected_device_type=device_type,
                channel_number=channel_number,
                expected_level=gain_level,
            )

    async def clear_configuration(
        self,
        device_ip_address,
        preserve_internet_protocol_settings: bool,
        timeout: float = 2.0,
    ) -> dict:
        from netaudio.dante.device_commands import DanteDeviceCommands
        from netaudio.dante.services.notification import mutate_and_wait_for_clear_configuration_status

        address = str(device_ip_address)
        commands = DanteDeviceCommands(host_mac=self._host_mac)
        command_builder = (
            commands.command_clear_all_configuration_preserving_internet_protocol_settings
            if preserve_internet_protocol_settings
            else commands.command_clear_all_configuration
        )
        packet, _, port = command_builder()
        expected_action_result_code = 2 if preserve_internet_protocol_settings else 1
        notifications = await self._ensure_notifications()
        operation_lock = self._capability_probe_locks.setdefault(
            ("clear_configuration_action", address), asyncio.Lock()
        )

        async def mutate() -> None:
            await self(packet, address, port, expect_response=False)

        async with operation_lock:
            status = await mutate_and_wait_for_clear_configuration_status(
                notifications,
                address,
                expected_action_result_code,
                mutate,
                timeout,
            )
        if status is None:
            raise CapabilityProbeTimeout(f"clear-configuration status timed out for {address}")
        if status["action_result_code"] != expected_action_result_code:
            raise RuntimeError(
                f"clear-configuration returned result {status['action_result_code']} "
                f"instead of {expected_action_result_code} for {address}"
            )
        return status

    async def close(self) -> None:
        notifications = self._notifications
        dispatcher = self._dispatcher
        settings_service = self._settings_service
        self._notifications = None
        self._dispatcher = None
        self._settings_service = None
        if settings_service is not None:
            await settings_service.stop()
        if notifications is not None:
            await notifications.stop()
        if dispatcher is not None:
            await dispatcher.stop()
        clients = list(self._clients.values())
        self._clients.clear()
        self._client_request_locks.clear()
        self._capability_probe_locks.clear()
        for client in clients:
            client.close()


async def send_and_wait_for_notification(
    send,
    packet,
    device_ip_address,
    port,
    notification_ids,
    **send_options,
):
    notification_sender = getattr(send, "send_and_wait_for_notification", None)
    if notification_sender is not None:
        return await notification_sender(
            packet,
            device_ip_address,
            port,
            notification_ids,
            **send_options,
        )
    return await send(packet, device_ip_address, port, **send_options)


def ansi(code: str, text: str) -> str:
    if settings.no_color:
        return str(text)
    return f"\033[{code}m{text}\033[0m"


from netaudio._common_cli import HEADER_ICONS, _get_state, _iconize_headers


def _make_dante_application(packet_store=None, session_id=None) -> DanteApplication:
    application = DanteApplication(packet_store=packet_store, dissect=_get_state().dissect)
    if packet_store and session_id:
        application.capture_session_id = session_id
        for service in (application.settings, application.cmc, application.notifications):
            service.session_id = session_id
    return application


async def _probe_lock_status_once(
    device_ip_address: str,
    timeout: float | None = None,
) -> LockStatusObservation | None:
    """Return a valid lock-status publication observed after sending 0x1008."""
    application = _make_dante_application()
    try:
        await application.startup()
        return await application.probe_lock_status(
            str(device_ip_address),
            timeout=settings.lock_state_timeout if timeout is None else timeout,
        )
    except Exception as exception:
        logger.debug(f"Lock status unavailable for {device_ip_address}: {exception}")
        return None
    finally:
        try:
            await application.shutdown()
        except Exception as exception:
            logger.debug(f"Lock status application shutdown failed for {device_ip_address}: {exception}")


async def _discover(packet_store=None, session_id=None) -> dict[str, DanteDevice]:
    devices = await get_devices_from_daemon()

    if devices is None:
        owns_store = False
        if packet_store is None:
            from netaudio._capture import open_capture_session

            packet_store, session_id = open_capture_session()
            owns_store = packet_store is not None
        application = _make_dante_application(packet_store=packet_store, session_id=session_id)
        await application.startup()
        try:
            devices = await application.discover_and_populate(timeout=settings.mdns_timeout)
        finally:
            await application.shutdown()
            if owns_store:
                packet_store.close()

    return devices or {}


async def _apply_avio_status_pages(device) -> None:
    try:
        receiver_flow_page = await device.operations.query_receiver_flow_status_2809()
    except RuntimeError:
        receiver_flow_page = None
    if receiver_flow_page is not None:
        device.apply_receiver_flow_status_page(receiver_flow_page)
    try:
        transmitter_channel_page = await device.operations.query_transmitter_channel_status_2809()
    except RuntimeError:
        transmitter_channel_page = None
    if transmitter_channel_page is not None:
        device.apply_transmitter_channel_status_page(transmitter_channel_page)
    try:
        transmitter_flow_page = await device.operations.query_transmitter_flow_status_2809()
    except RuntimeError:
        transmitter_flow_page = None
    if transmitter_flow_page is not None:
        device.apply_transmitter_flow_status_page(transmitter_flow_page)
    try:
        receiver_channel_page = await device.operations.query_receiver_channel_status_2809()
    except RuntimeError:
        receiver_channel_page = None
    if receiver_channel_page is not None:
        device.apply_receiver_channel_status_page(receiver_channel_page)


async def _populate_show_details(device, include_channels: bool) -> None:
    if device.clock_source_code is None or device.clock_subdomain is None:
        try:
            await device.get_clocking_status()
        except Exception as exception:
            logger.debug(f"Clock status unavailable for {device.server_name or device.name}: {exception}")
    if device.aes67_multicast_prefix is None and device.aes67_supported is not False:
        try:
            await device.operations.get_aes67_configured()
        except Exception as exception:
            logger.debug(f"AES67 multicast prefix unavailable for {device.server_name or device.name}: {exception}")
    if include_channels or device.transmitter_flows is None:
        await _apply_avio_status_pages(device)


async def _load_device_for_show(include_channels: bool) -> tuple[str, DanteDevice]:
    devices = await get_devices_from_daemon()
    if devices is not None:
        selected_devices = filter_devices(devices)
        server_name, device = _resolve_one(selected_devices)
        if include_channels:
            await _populate_controls({server_name: device})
        await _populate_show_details(device, include_channels=include_channels)
        return server_name, device

    from netaudio._capture import open_capture_session

    packet_store, session_id = open_capture_session()
    application = _make_dante_application(packet_store=packet_store, session_id=session_id)
    try:
        await application.startup()
        state = _get_state()
        show_timeout = settings.mdns_timeout if state.timeout_explicit else min(settings.mdns_timeout, 2.0)
        selected_devices = {}
        literal_name = state.names[0] if len(state.names) == 1 and not has_magic(state.names[0]) else None
        if literal_name is not None:
            exact_devices = await application.discover_named_device(literal_name, timeout=show_timeout)
            exact_candidates = filter_devices(exact_devices, include_names=False)
            await application.populate_device_names(
                exact_candidates,
                request_timeout_milliseconds=500,
                request_attempts=1,
            )
            selected_devices = filter_devices(exact_candidates)
        if not selected_devices:
            discovered_devices = await application.wait_for_discovery(timeout=show_timeout)
            identity_candidates = filter_devices(discovered_devices, include_names=False)
            await application.populate_device_names(
                identity_candidates,
                request_timeout_milliseconds=500,
                request_attempts=1,
            )
            selected_devices = filter_devices(identity_candidates)

        server_name, device = _resolve_one(selected_devices)
        selected_devices = {server_name: device}
        await application.populate_devices(
            selected_devices,
            timeout=show_timeout,
            include_channels=include_channels,
        )
        server_name, device = _resolve_one(filter_devices(selected_devices))
        await _populate_show_details(device, include_channels=include_channels)
        return server_name, device
    finally:
        await application.shutdown()
        if packet_store is not None:
            packet_store.close()


def discover() -> dict[str, DanteDevice]:
    return asyncio.run(_discover())


def _get_arc_port(device: DanteDevice) -> int:
    if device.services:
        for service_data in device.services.values():
            if service_data.get("type") == SERVICE_ARC:
                return service_data.get("port", 4440)
    return 4440


def _resolve_one(devices: dict[str, DanteDevice]) -> tuple[str, DanteDevice]:
    if len(devices) == 0:
        typer.echo("Error: device not found.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    if len(devices) > 1:
        names = ", ".join(d.name or sn for sn, d in devices.items())
        typer.echo(f"Error: multiple devices matched: {names}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    return next(iter(devices.items()))


def _make_core_sender(observer=None, devices=None, packet_store=None, session_id=None) -> CoreCommandSender:
    return CoreCommandSender(
        observer=observer,
        devices=devices,
        packet_store=packet_store,
        session_id=session_id,
    )


def _capture_observer():
    state = _get_state()
    if not state.capture and not state.dissect:
        return None, None
    from netaudio._capture import make_observer, open_capture_session

    store, session_id = open_capture_session()
    observer = make_observer(store, session_id, state.dissect)
    if store and session_id:
        typer.echo(f"Capture: recording to session #{session_id}", err=True)
    return observer, store


@asynccontextmanager
async def _command_context():
    observer, store = _capture_observer()
    session_id = None
    if store:
        active = store.get_latest_session(active_only=True)
        session_id = active["id"] if active else None
    try:
        devices = await get_devices_from_daemon()
        if devices is None:
            devices = await _discover(packet_store=store, session_id=session_id)
            if observer is not None:
                for device in devices.values():
                    device.rx_channels = {}
                    device.tx_channels = {}
            await _populate_controls(devices, observer=observer, strict=False)

        devices = devices or {}
        sender = _make_core_sender(
            observer=observer,
            devices=devices,
            packet_store=store,
            session_id=session_id,
        )
        try:
            yield devices, sender
        finally:
            await sender.close()
    finally:
        if observer is not None:
            observer.flush()
        if store:
            store.close()


async def _populate_controls(devices: dict[str, DanteDevice], observer=None, strict: bool = True) -> None:
    unpopulated = [
        device for device in devices.values() if not device.tx_channels and not device.rx_channels and device.ipv4
    ]

    if not unpopulated:
        return

    if observer is not None:
        from netaudio._capture import populate_instrumented

        population_results = await asyncio.gather(
            *(populate_instrumented(device, observer) for device in unpopulated),
            return_exceptions=True,
        )
    else:
        population_results = await asyncio.gather(
            *(device.populate_from_core() for device in unpopulated),
            return_exceptions=True,
        )

    failures = [
        (device, result) for device, result in zip(unpopulated, population_results) if isinstance(result, BaseException)
    ]
    for device, exception in failures:
        logger.error(
            f"Failed to populate controls for {device.server_name or device.name}: {exception}",
            exc_info=(type(exception), exception, exception.__traceback__),
        )
    if strict and failures:
        device, exception = failures[0]
        raise RuntimeError(
            f"failed to populate controls for {device.server_name or device.name}: {exception}"
        ) from exception


async def _enrich_clock_fields(devices: dict[str, DanteDevice]) -> None:
    missing = [
        device
        for device in devices.values()
        if device.ipv4 is not None and (device.clock_role is None or device.clock_source_code is None)
    ]
    if not missing:
        return
    sender = _make_core_sender(devices=devices)
    try:
        await asyncio.gather(
            *[sender.probe_clocking_status(device) for device in missing],
            return_exceptions=True,
        )
    finally:
        await sender.close()


async def _enrich_lock_states(devices: dict[str, DanteDevice]) -> None:
    candidates = {server_name: device for server_name, device in devices.items() if device.ipv4 is not None}
    if not candidates:
        return

    application = _make_dante_application()
    application.devices.update(candidates)
    await application.startup()
    try:
        results = await asyncio.gather(
            *[
                application.probe_lock_status(
                    str(device.ipv4),
                    timeout=settings.lock_state_timeout,
                )
                for device in candidates.values()
            ],
            return_exceptions=True,
        )
        for device, result in zip(candidates.values(), results):
            if isinstance(result, BaseException):
                logger.debug(f"Lock status unavailable for {device.server_name or device.name}: {result}")
                device.is_locked = None
            elif result is None:
                device.is_locked = None
            else:
                device.is_locked = result.is_locked
    finally:
        await application.shutdown()


async def _load_display_devices(include_channels: bool = False) -> dict[str, DanteDevice]:
    devices = await get_devices_from_daemon()
    if devices is None:
        devices = await _discover()
        await _populate_controls(devices, strict=False)
    devices = filter_devices(devices or {})
    if include_channels:
        for device in devices.values():
            await _populate_show_details(device, include_channels=True)
    await _enrich_clock_fields(devices)
    await _enrich_lock_states(devices)
    return devices


from netaudio._common_output import (
    _device_to_preset_xml,
    _format_csv,
    _format_json,
    _format_table,
    _format_text,
    _format_yaml,
    _hex_encode,
    _sub_text,
    format_devices_xml,
    output_single,
    output_table,
)
from netaudio._common_selection import (
    _mac_matches,
    _normalize_mac,
    _strip_separators,
    filter_devices,
    find_channel,
    find_device,
    parse_qualified_name,
    set_device_filter,
    sort_devices,
)
