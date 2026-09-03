from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field as dataclass_field

from netaudio.asynchronous_primitives import DeferredAsyncioEvent
from netaudio.dante.const import (
    DEVICE_INFO_PORT,
    DEVICE_SETTINGS_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
    NOTIFICATION_AES67_STATUS,
    NOTIFICATION_CLEAR_CONFIG_STATUS,
    NOTIFICATION_CLOCKING_STATUS,
    NOTIFICATION_DEVICE_REBOOT,
    NOTIFICATION_ENCODING_STATUS,
    NOTIFICATION_GAIN_STATUS,
    NOTIFICATION_INTERFACE_STATUS,
    NOTIFICATION_LATENCY_CHANGE,
    NOTIFICATION_MANF_VERSIONS_STATUS,
    NOTIFICATION_NAMES,
    NOTIFICATION_PROPERTY_CHANGE,
    NOTIFICATION_ROUTING_DEVICE_CHANGE,
    NOTIFICATION_ROUTING_READY,
    NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_RX_FLOW_CHANGE,
    NOTIFICATION_SAMPLE_RATE_PULLUP_STATUS,
    NOTIFICATION_SAMPLE_RATE_STATUS,
    NOTIFICATION_SETTINGS_CHANGE,
    NOTIFICATION_TOPOLOGY_CHANGE,
    NOTIFICATION_TX_CHANNEL_CHANGE,
    NOTIFICATION_TX_FLOW_CHANGE,
    NOTIFICATION_TX_LABEL_CHANGE,
    NOTIFICATION_VERSIONS_STATUS,
)
from netaudio.dante.conmon_export import (
    ConmonExport,
    ConmonExportCollector,
    ConmonExportError,
    ConmonExportUnavailableError,
)
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.service import DanteMulticastService
from netaudio.dante.services.notification_packet_handlers import NotificationPacketHandlers

__all__ = [
    "ConmonExportWaiter",
    "DanteNotificationService",
    "NOTIFICATION_AES67_STATUS",
    "NOTIFICATION_CLEAR_CONFIG_STATUS",
    "NOTIFICATION_CLOCKING_STATUS",
    "NOTIFICATION_DEVICE_REBOOT",
    "NOTIFICATION_ENCODING_STATUS",
    "NOTIFICATION_GAIN_STATUS",
    "NOTIFICATION_INTERFACE_STATUS",
    "NOTIFICATION_LATENCY_CHANGE",
    "NOTIFICATION_MANF_VERSIONS_STATUS",
    "NOTIFICATION_NAMES",
    "NOTIFICATION_PROPERTY_CHANGE",
    "NOTIFICATION_ROUTING_DEVICE_CHANGE",
    "NOTIFICATION_ROUTING_READY",
    "NOTIFICATION_RX_CHANNEL_CHANGE",
    "NOTIFICATION_RX_FLOW_CHANGE",
    "NOTIFICATION_SAMPLE_RATE_PULLUP_STATUS",
    "NOTIFICATION_SAMPLE_RATE_STATUS",
    "NOTIFICATION_SETTINGS_CHANGE",
    "NOTIFICATION_TOPOLOGY_CHANGE",
    "NOTIFICATION_TX_CHANNEL_CHANGE",
    "NOTIFICATION_TX_FLOW_CHANGE",
    "NOTIFICATION_TX_LABEL_CHANGE",
    "NOTIFICATION_VERSIONS_STATUS",
    "Waiter",
    "mutate_and_wait_for_capability_value",
    "mutate_and_wait_for_clear_configuration_status",
    "request_and_wait_for_conmon_export",
    "send_and_wait_for_gain_status",
]

logger = logging.getLogger("netaudio")

WAITER_KIND_NOTIFICATION = "notification"


@dataclass(eq=False)
class Waiter:
    kind: str
    key: str
    accept: Callable[[object], bool] | None = None
    event: DeferredAsyncioEvent = dataclass_field(default_factory=DeferredAsyncioEvent)
    latest_result: object = None

    def clear(self) -> None:
        self.event.clear()

    def is_set(self) -> bool:
        return self.event.is_set()

    def observe(self, result) -> None:
        if self.event.is_set():
            return
        self.latest_result = result
        if self.accept is None or self.accept(result):
            self.event.set()

    async def wait(self) -> None:
        await self.event.wait()


@dataclass(eq=False)
class ConmonExportWaiter(Waiter):
    collector: ConmonExportCollector | None = None
    error: ConmonExportError | None = None
    result: ConmonExport | None = None

    def observe_unavailable(self) -> None:
        if self.event.is_set():
            return
        self.error = ConmonExportUnavailableError(
            f"ConMon export is unavailable on {self.key}: device returned an empty response"
        )
        self.event.set()

    def observe(self, fragment) -> None:
        if self.event.is_set():
            return
        try:
            self.result = self.collector.observe(fragment)
        except ConmonExportError as exception:
            self.error = exception
            self.event.set()
            return
        if self.result is not None:
            self.latest_result = self.result
            self.event.set()


def _gain_status_accepts(
    expected_device_type: str | None,
    channel_number: int | None,
    expected_level: int | None,
) -> Callable[[tuple[str, list[int]]], bool]:
    def accept(result: tuple[str, list[int]]) -> bool:
        device_type, channel_levels = result
        if expected_device_type is not None and device_type != expected_device_type:
            return False
        if channel_number is None or expected_level is None:
            return True
        channel_index = channel_number - 1
        return 0 <= channel_index < len(channel_levels) and channel_levels[channel_index] == expected_level

    return accept


async def send_and_wait_for_gain_status(
    notifications: DanteNotificationService,
    device_ip_address: str,
    send_operation: Callable[[], Awaitable[None]],
    timeout: float,
    expected_device_type: str | None = None,
    channel_number: int | None = None,
    expected_level: int | None = None,
) -> tuple[str, list[int]] | None:
    waiter = notifications.register_waiter(
        "gain",
        device_ip_address,
        accept=_gain_status_accepts(expected_device_type, channel_number, expected_level),
    )
    try:
        event_loop = asyncio.get_running_loop()
        deadline = event_loop.time() + timeout
        attempt_count = 3
        for attempt_number in range(attempt_count):
            await send_operation()
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
            return waiter.latest_result
        return waiter.latest_result
    finally:
        notifications.unregister_waiter(waiter)


class DanteNotificationService(NotificationPacketHandlers, DanteMulticastService):
    def __init__(
        self,
        dispatcher: DanteEventDispatcher,
        device_lookup=None,
        packet_store=None,
        interface_ip: str | None = None,
        dissect: bool = False,
    ):
        super().__init__(
            multicast_group=MULTICAST_GROUP_CONTROL_MONITORING,
            multicast_port=DEVICE_INFO_PORT,
            packet_store=packet_store,
            interface_ip=interface_ip,
            dissect=dissect,
        )
        self._conmon_expected_count: dict[str, int] = {}
        self._conmon_received: dict[str, set[int]] = {}
        self._device_lookup = device_lookup
        self._dispatcher = dispatcher
        self._pending_conmon: dict[str, dict] = {}
        self._waiters: dict[tuple[str, str], set[Waiter]] = {}

    def set_device_lookup(self, lookup_func):
        self._device_lookup = lookup_func

    def receive_settings_response(self, data: bytes, source_ip: str) -> None:
        """Process a native settings publication returned through a managed transport."""
        self._on_packet(data, (source_ip, DEVICE_SETTINGS_PORT))

    def register_waiter(self, kind: str, key: str, accept: Callable[[object], bool] | None = None) -> Waiter:
        waiter = Waiter(kind=kind, key=key, accept=accept)
        self._waiters.setdefault((kind, key), set()).add(waiter)
        return waiter

    def register_notification_waiter(self, device_ip_address: str, notification_ids) -> Waiter:
        accepted_ids = frozenset(notification_ids)
        if not accepted_ids:
            raise ValueError("notification_ids cannot be empty")
        return self.register_waiter(
            WAITER_KIND_NOTIFICATION,
            device_ip_address,
            accept=lambda notification_id: notification_id in accepted_ids,
        )

    def register_conmon_waiter(self, device_ip: str, expected_count: int = 2) -> Waiter:
        self._conmon_received[device_ip] = set()
        self._conmon_expected_count[device_ip] = expected_count
        return self.register_waiter("conmon", device_ip)

    def register_conmon_export_waiter(
        self,
        device_ip_address: str,
        expected_echoed_tag: bytes,
        expected_selector_value: int,
    ) -> ConmonExportWaiter:
        if self.is_waiting("conmon_export", device_ip_address):
            raise RuntimeError(f"ConMon export is already active for {device_ip_address}")
        waiter = ConmonExportWaiter(
            kind="conmon_export",
            key=device_ip_address,
            collector=ConmonExportCollector(
                expected_echoed_tag=expected_echoed_tag,
                expected_selector_value=expected_selector_value,
            ),
        )
        self._waiters.setdefault(("conmon_export", device_ip_address), set()).add(waiter)
        return waiter

    def unregister_waiter(self, waiter: Waiter) -> None:
        registry_key = (waiter.kind, waiter.key)
        waiters = self._waiters.get(registry_key)
        if waiters is None:
            return
        waiters.discard(waiter)
        if not waiters:
            self._waiters.pop(registry_key, None)
        if waiter.kind == "conmon":
            self._conmon_received.pop(waiter.key, None)
            self._conmon_expected_count.pop(waiter.key, None)

    def is_waiting(self, kind: str, key: str) -> bool:
        return bool(self._waiters.get((kind, key)))

    def waiters_for(self, kind: str, key: str) -> tuple[Waiter, ...]:
        return tuple(self._waiters.get((kind, key), ()))

    def notify_waiters(self, kind: str, key: str, result) -> None:
        for waiter in self.waiters_for(kind, key):
            waiter.observe(result)

    def notify_conmon_response(self, source_ip: str, opcode: int) -> None:
        if not self.is_waiting("conmon", source_ip):
            return

        self._conmon_received.setdefault(source_ip, set()).add(opcode)
        expected = self._conmon_expected_count.get(source_ip, 2)

        if len(self._conmon_received[source_ip]) >= expected:
            self.notify_waiters("conmon", source_ip, True)


async def mutate_and_wait_for_capability_value(
    notifications: DanteNotificationService,
    capability_name: str,
    device_ip_address: str,
    expected_value: int,
    mutate,
    probe_status,
    timeout: float,
) -> tuple[int, list[int]] | None:
    waiter = notifications.register_waiter(
        capability_name,
        device_ip_address,
        accept=lambda result: result[0] == expected_value,
    )
    probe_task = None

    async def probe_and_observe() -> None:
        try:
            status = await probe_status()
        except (RuntimeError, OSError) as exception:
            logger.warning(f"Failed to probe {capability_name} status for {device_ip_address}: {exception}")
            return
        if status is None:
            logger.debug(f"{capability_name} status unavailable for {device_ip_address}")
            return
        waiter.observe(status)

    try:
        await mutate()
        if not waiter.is_set():
            probe_task = asyncio.create_task(probe_and_observe())
            try:
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug(f"Timed out waiting for {capability_name}={expected_value} from {device_ip_address}")
        return waiter.latest_result
    finally:
        notifications.unregister_waiter(waiter)
        if probe_task is not None and not probe_task.done():
            probe_task.cancel()
        if probe_task is not None:
            await asyncio.gather(probe_task, return_exceptions=True)


async def mutate_and_wait_for_clear_configuration_status(
    notifications: DanteNotificationService,
    device_ip_address: str,
    expected_action_result_code: int,
    mutate,
    timeout: float,
) -> dict | None:
    waiter = notifications.register_waiter(
        "clear_configuration_status",
        device_ip_address,
        accept=lambda status: status["action_result_code"] == expected_action_result_code,
    )
    try:
        await mutate()
        try:
            await asyncio.wait_for(waiter.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.debug(
                f"Timed out waiting for clear-configuration result "
                f"{expected_action_result_code} from {device_ip_address}"
            )
        return waiter.latest_result
    finally:
        notifications.unregister_waiter(waiter)


async def request_and_wait_for_conmon_export(
    notifications: DanteNotificationService,
    device_ip_address: str,
    expected_echoed_tag: bytes,
    expected_selector_value: int,
    request: Callable[[], Awaitable[None]],
    timeout: float,
) -> ConmonExport | None:
    waiter = notifications.register_conmon_export_waiter(
        device_ip_address,
        expected_echoed_tag,
        expected_selector_value,
    )
    try:
        await request()
        try:
            await asyncio.wait_for(waiter.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        if waiter.error is not None:
            raise waiter.error
        return waiter.result
    finally:
        notifications.unregister_waiter(waiter)
