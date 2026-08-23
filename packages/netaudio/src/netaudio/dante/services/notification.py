from __future__ import annotations

import asyncio
import logging
import struct
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field as dataclass_field

from netaudio.asynchronous_primitives import DeferredAsyncioEvent
from netaudio.dante.const import (
    DEVICE_INFO_PORT,
    DEVICE_SETTINGS_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
)
from netaudio.dante.conmon_export import (
    ConmonExport,
    ConmonExportCollector,
    ConmonExportError,
    ConmonExportUnavailableError,
)
from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio.dante.gain import SUPPORTED_GAIN_LEVELS
from netaudio.dante.lock_status import LockStatusObservation
from netaudio.dante.service import DanteMulticastService
from netaudio.dante.services.notification_packet_handlers import NotificationPacketHandlers
from netaudio.dante.services.notification_protocol import (
    AES67_CURRENT_NEW_MAP as AES67_CURRENT_NEW_MAP,
    CLOCK_PORT_ROLE_MAP as CLOCK_PORT_ROLE_MAP,
    CLOCK_PORT_STATE_FOLLOWER as CLOCK_PORT_STATE_FOLLOWER,
    CLOCK_PORT_STATE_LEADER as CLOCK_PORT_STATE_LEADER,
    CONMON_AES67_CURRENT_NEW_OFFSET as CONMON_AES67_CURRENT_NEW_OFFSET,
    CONMON_CLOCK_FREQUENCY_OFFSET_PARTS_PER_BILLION_OFFSET as CONMON_CLOCK_FREQUENCY_OFFSET_PARTS_PER_BILLION_OFFSET,
    CONMON_CLOCK_PORT_STATE_OFFSET as CONMON_CLOCK_PORT_STATE_OFFSET,
    CONMON_OPCODE_AES67_CURRENT_NEW as CONMON_OPCODE_AES67_CURRENT_NEW,
    CONMON_OPCODE_GAIN_STATUS as CONMON_OPCODE_GAIN_STATUS,
    CONMON_OPCODE_INTERFACE_STATUS as CONMON_OPCODE_INTERFACE_STATUS,
    CONMON_OPCODE_PTP_CLOCK_STATUS as CONMON_OPCODE_PTP_CLOCK_STATUS,
    CONMON_OPCODE_ROUTING_CAPACITY_STATUS as CONMON_OPCODE_ROUTING_CAPACITY_STATUS,
    CONMON_PREFERRED_LEADER_OFFSET as CONMON_PREFERRED_LEADER_OFFSET,
    NOTIFICATION_AES67_STATUS as NOTIFICATION_AES67_STATUS,
    NOTIFICATION_CLEAR_CONFIG_STATUS as NOTIFICATION_CLEAR_CONFIG_STATUS,
    NOTIFICATION_CLOCKING_STATUS as NOTIFICATION_CLOCKING_STATUS,
    NOTIFICATION_DEVICE_REBOOT as NOTIFICATION_DEVICE_REBOOT,
    NOTIFICATION_ENCODING_STATUS as NOTIFICATION_ENCODING_STATUS,
    NOTIFICATION_GAIN_STATUS as NOTIFICATION_GAIN_STATUS,
    NOTIFICATION_INTERFACE_STATUS as NOTIFICATION_INTERFACE_STATUS,
    NOTIFICATION_LATENCY_CHANGE as NOTIFICATION_LATENCY_CHANGE,
    NOTIFICATION_MANF_VERSIONS_STATUS as NOTIFICATION_MANF_VERSIONS_STATUS,
    NOTIFICATION_NAMES as NOTIFICATION_NAMES,
    NOTIFICATION_PROPERTY_CHANGE as NOTIFICATION_PROPERTY_CHANGE,
    NOTIFICATION_ROUTING_DEVICE_CHANGE as NOTIFICATION_ROUTING_DEVICE_CHANGE,
    NOTIFICATION_ROUTING_READY as NOTIFICATION_ROUTING_READY,
    NOTIFICATION_RX_CHANNEL_CHANGE as NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_RX_FLOW_CHANGE as NOTIFICATION_RX_FLOW_CHANGE,
    NOTIFICATION_SAMPLE_RATE_PULLUP_STATUS as NOTIFICATION_SAMPLE_RATE_PULLUP_STATUS,
    NOTIFICATION_SAMPLE_RATE_STATUS as NOTIFICATION_SAMPLE_RATE_STATUS,
    NOTIFICATION_SETTINGS_CHANGE as NOTIFICATION_SETTINGS_CHANGE,
    NOTIFICATION_TOPOLOGY_CHANGE as NOTIFICATION_TOPOLOGY_CHANGE,
    NOTIFICATION_TX_CHANNEL_CHANGE as NOTIFICATION_TX_CHANNEL_CHANGE,
    NOTIFICATION_TX_FLOW_CHANGE as NOTIFICATION_TX_FLOW_CHANGE,
    NOTIFICATION_TX_LABEL_CHANGE as NOTIFICATION_TX_LABEL_CHANGE,
    NOTIFICATION_VERSIONS_STATUS as NOTIFICATION_VERSIONS_STATUS,
    parse_aes67_current_new_byte as parse_aes67_current_new_byte,
)

logger = logging.getLogger("netaudio")


class _WaiterRegistry:
    def __init__(self):
        self._waiters: dict[tuple[str, str], DeferredAsyncioEvent] = {}
        self._results: dict[tuple[str, str], object] = {}

    def register(self, kind: str, key: str) -> DeferredAsyncioEvent:
        event = DeferredAsyncioEvent()
        self._waiters[(kind, key)] = event
        self._results.pop((kind, key), None)
        return event

    def unregister(self, kind: str, key: str) -> None:
        self._waiters.pop((kind, key), None)

    def is_registered(self, kind: str, key: str) -> bool:
        return (kind, key) in self._waiters

    def take_result(self, kind: str, key: str):
        return self._results.pop((kind, key), None)

    def notify(self, kind: str, key: str, result) -> None:
        waiter = self._waiters.get((kind, key))
        if waiter is None:
            return
        self._results[(kind, key)] = result
        waiter.set()


@dataclass(eq=False)
class NotificationWaiter:
    device_ip_address: str
    notification_ids: frozenset[int]
    event: DeferredAsyncioEvent = dataclass_field(default_factory=DeferredAsyncioEvent)
    notification_id: int | None = None


@dataclass(eq=False)
class CapabilityValueWaiter:
    capability_name: str
    device_ip_address: str
    value_matches: Callable[[int], bool]
    event: DeferredAsyncioEvent = dataclass_field(default_factory=DeferredAsyncioEvent)
    latest_result: tuple[int, list[int]] | None = None

    def observe(self, current_value: int, supported_values: list[int]) -> None:
        if self.event.is_set():
            return
        self.latest_result = (current_value, supported_values)
        if self.value_matches(current_value):
            self.event.set()


@dataclass(eq=False)
class GainStatusWaiter:
    device_ip_address: str
    expected_device_type: str | None = None
    channel_number: int | None = None
    expected_level: int | None = None
    event: DeferredAsyncioEvent = dataclass_field(default_factory=DeferredAsyncioEvent)
    latest_result: tuple[str, list[int]] | None = None

    def observe(self, device_type: str, channel_levels: list[int]) -> None:
        if self.event.is_set():
            return
        self.latest_result = (device_type, channel_levels)
        if self.expected_device_type is not None and device_type != self.expected_device_type:
            return
        if self.channel_number is None or self.expected_level is None:
            self.event.set()
            return
        channel_index = self.channel_number - 1
        if 0 <= channel_index < len(channel_levels) and channel_levels[channel_index] == self.expected_level:
            self.event.set()


@dataclass(eq=False)
class ClearConfigurationStatusWaiter:
    device_ip_address: str
    expected_action_result_code: int
    event: DeferredAsyncioEvent = dataclass_field(default_factory=DeferredAsyncioEvent)
    latest_result: dict | None = None

    def observe(self, status: dict) -> None:
        if self.event.is_set():
            return
        self.latest_result = status
        if status["action_result_code"] == self.expected_action_result_code:
            self.event.set()


@dataclass(eq=False)
class ConmonExportWaiter:
    device_ip_address: str
    collector: ConmonExportCollector
    event: DeferredAsyncioEvent = dataclass_field(default_factory=DeferredAsyncioEvent)
    result: ConmonExport | None = None
    error: ConmonExportError | None = None

    def observe_unavailable(self) -> None:
        if self.event.is_set():
            return
        self.error = ConmonExportUnavailableError(
            f"ConMon export is unavailable on {self.device_ip_address}: device returned an empty response"
        )
        self.event.set()

    def observe(self, fragment: dict) -> None:
        if self.event.is_set():
            return
        try:
            self.result = self.collector.observe(fragment)
        except ConmonExportError as exception:
            self.error = exception
            self.event.set()
            return
        if self.result is not None:
            self.event.set()


async def send_and_wait_for_gain_status(
    notifications: DanteNotificationService,
    device_ip_address: str,
    send_operation: Callable[[], None],
    timeout: float,
    expected_device_type: str | None = None,
    channel_number: int | None = None,
    expected_level: int | None = None,
) -> tuple[str, list[int]] | None:
    waiter = notifications.register_gain_status_waiter(
        device_ip_address,
        expected_device_type=expected_device_type,
        channel_number=channel_number,
        expected_level=expected_level,
    )
    try:
        event_loop = asyncio.get_running_loop()
        deadline = event_loop.time() + timeout
        attempt_count = 3
        for attempt_number in range(attempt_count):
            send_operation()
            remaining_time = max(0.0, deadline - event_loop.time())
            if remaining_time == 0:
                break
            if attempt_number == attempt_count - 1:
                attempt_timeout = remaining_time
            else:
                attempt_timeout = min(remaining_time, timeout / 4)
            try:
                await asyncio.wait_for(waiter.event.wait(), timeout=attempt_timeout)
            except asyncio.TimeoutError:
                continue
            return waiter.latest_result
        return waiter.latest_result
    finally:
        notifications.unregister_gain_status_waiter(waiter)


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
        self._dispatcher = dispatcher
        self._device_lookup = device_lookup
        self._pending_conmon: dict[str, dict] = {}
        self._waiters = _WaiterRegistry()
        self._conmon_received: dict[str, set[int]] = {}
        self._conmon_expected_count: dict[str, int] = {}
        self._notification_waiters: dict[str, set[NotificationWaiter]] = {}
        self._capability_value_waiters: dict[tuple[str, str], set[CapabilityValueWaiter]] = {}
        self._gain_status_waiters: dict[str, set[GainStatusWaiter]] = {}
        self._clear_configuration_status_waiters: dict[
            str,
            set[ClearConfigurationStatusWaiter],
        ] = {}
        self._conmon_export_waiters: dict[str, ConmonExportWaiter] = {}

    def set_device_lookup(self, lookup_func):
        self._device_lookup = lookup_func

    def register_conmon_waiter(self, device_ip: str, expected_count: int = 2) -> DeferredAsyncioEvent:
        self._conmon_received[device_ip] = set()
        self._conmon_expected_count[device_ip] = expected_count
        return self._waiters.register("conmon", device_ip)

    def register_conmon_export_waiter(
        self,
        device_ip_address: str,
        expected_echoed_tag: bytes,
        expected_selector_value: int,
    ) -> ConmonExportWaiter:
        if device_ip_address in self._conmon_export_waiters:
            raise RuntimeError(f"ConMon export is already active for {device_ip_address}")
        waiter = ConmonExportWaiter(
            device_ip_address=device_ip_address,
            collector=ConmonExportCollector(
                expected_echoed_tag=expected_echoed_tag,
                expected_selector_value=expected_selector_value,
            ),
        )
        self._conmon_export_waiters[device_ip_address] = waiter
        return waiter

    def unregister_conmon_export_waiter(self, waiter: ConmonExportWaiter) -> None:
        if self._conmon_export_waiters.get(waiter.device_ip_address) is waiter:
            self._conmon_export_waiters.pop(waiter.device_ip_address, None)

    def unregister_conmon_waiter(self, device_ip: str) -> None:
        self._waiters.unregister("conmon", device_ip)
        self._conmon_received.pop(device_ip, None)
        self._conmon_expected_count.pop(device_ip, None)

    def register_aes67_waiter(self, device_ip: str) -> DeferredAsyncioEvent:
        return self._waiters.register("aes67", device_ip)

    def unregister_aes67_waiter(self, device_ip: str) -> None:
        self._waiters.unregister("aes67", device_ip)

    def get_aes67_result(self, device_ip: str) -> tuple[bool | None, bool | None] | None:
        return self._waiters.take_result("aes67", device_ip)

    def _notify_aes67_waiter(self, source_ip: str, current: bool | None, configured: bool | None) -> None:
        self._waiters.notify("aes67", source_ip, (current, configured))

    def register_sample_rate_waiter(self, device_ip_address: str) -> DeferredAsyncioEvent:
        return self._waiters.register("sample_rate", device_ip_address)

    def unregister_sample_rate_waiter(self, device_ip_address: str) -> None:
        self._waiters.unregister("sample_rate", device_ip_address)

    def get_sample_rate_result(self, device_ip_address: str) -> tuple[int, list[int]] | None:
        return self._waiters.take_result("sample_rate", device_ip_address)

    def _notify_sample_rate_waiter(
        self,
        source_ip_address: str,
        current_sample_rate: int,
        supported_sample_rates: list[int],
    ) -> None:
        self._waiters.notify("sample_rate", source_ip_address, (current_sample_rate, supported_sample_rates))

    def register_encoding_waiter(self, device_ip_address: str) -> DeferredAsyncioEvent:
        return self._waiters.register("encoding", device_ip_address)

    def unregister_encoding_waiter(self, device_ip_address: str) -> None:
        self._waiters.unregister("encoding", device_ip_address)

    def get_encoding_result(self, device_ip_address: str) -> tuple[int, list[int]] | None:
        return self._waiters.take_result("encoding", device_ip_address)

    def _notify_encoding_waiter(
        self,
        source_ip_address: str,
        current_encoding: int,
        supported_encodings: list[int],
    ) -> None:
        self._waiters.notify("encoding", source_ip_address, (current_encoding, supported_encodings))

    def register_sample_rate_pullup_waiter(self, device_ip_address: str) -> DeferredAsyncioEvent:
        return self._waiters.register("sample_rate_pullup", device_ip_address)

    def unregister_sample_rate_pullup_waiter(self, device_ip_address: str) -> None:
        self._waiters.unregister("sample_rate_pullup", device_ip_address)

    def get_sample_rate_pullup_result(self, device_ip_address: str) -> tuple[int, list[int]] | None:
        return self._waiters.take_result("sample_rate_pullup", device_ip_address)

    def _notify_sample_rate_pullup_waiter(
        self,
        source_ip_address: str,
        current_raw_value: int,
        supported_raw_values: list[int],
    ) -> None:
        self._waiters.notify(
            "sample_rate_pullup",
            source_ip_address,
            (current_raw_value, supported_raw_values),
        )

    def register_gain_status_waiter(
        self,
        device_ip_address: str,
        expected_device_type: str | None = None,
        channel_number: int | None = None,
        expected_level: int | None = None,
    ) -> GainStatusWaiter:
        waiter = GainStatusWaiter(
            device_ip_address=device_ip_address,
            expected_device_type=expected_device_type,
            channel_number=channel_number,
            expected_level=expected_level,
        )
        self._gain_status_waiters.setdefault(device_ip_address, set()).add(waiter)
        return waiter

    def unregister_gain_status_waiter(self, waiter: GainStatusWaiter) -> None:
        waiters = self._gain_status_waiters.get(waiter.device_ip_address)
        if waiters is None:
            return
        waiters.discard(waiter)
        if not waiters:
            self._gain_status_waiters.pop(waiter.device_ip_address, None)

    def _notify_gain_status_waiters(
        self,
        source_ip_address: str,
        device_type: str,
        channel_levels: list[int],
    ) -> None:
        for waiter in tuple(self._gain_status_waiters.get(source_ip_address, ())):
            waiter.observe(device_type, channel_levels)

    def register_capability_value_waiter(
        self,
        capability_name: str,
        device_ip_address: str,
        value_matches: Callable[[int], bool],
    ) -> CapabilityValueWaiter:
        waiter = CapabilityValueWaiter(
            capability_name=capability_name,
            device_ip_address=device_ip_address,
            value_matches=value_matches,
        )
        waiter_key = (capability_name, device_ip_address)
        self._capability_value_waiters.setdefault(waiter_key, set()).add(waiter)
        return waiter

    def unregister_capability_value_waiter(self, waiter: CapabilityValueWaiter) -> None:
        waiter_key = (waiter.capability_name, waiter.device_ip_address)
        waiters = self._capability_value_waiters.get(waiter_key)
        if waiters is None:
            return
        waiters.discard(waiter)
        if not waiters:
            self._capability_value_waiters.pop(waiter_key, None)

    def _notify_capability_value_waiters(
        self,
        capability_name: str,
        source_ip_address: str,
        current_value: int,
        supported_values: list[int],
    ) -> None:
        waiter_key = (capability_name, source_ip_address)
        for waiter in tuple(self._capability_value_waiters.get(waiter_key, ())):
            waiter.observe(current_value, supported_values)

    def register_preferred_leader_waiter(self, device_ip: str) -> DeferredAsyncioEvent:
        return self._waiters.register("preferred_leader", device_ip)

    def unregister_preferred_leader_waiter(self, device_ip: str) -> None:
        self._waiters.unregister("preferred_leader", device_ip)

    def get_preferred_leader_result(self, device_ip: str) -> bool | None:
        return self._waiters.take_result("preferred_leader", device_ip)

    def _notify_preferred_leader_waiter(self, source_ip: str, preferred_leader: bool | None) -> None:
        self._waiters.notify("preferred_leader", source_ip, preferred_leader)

    def register_interface_waiter(self, device_ip: str) -> DeferredAsyncioEvent:
        return self._waiters.register("interface", device_ip)

    def unregister_interface_waiter(self, device_ip: str) -> None:
        self._waiters.unregister("interface", device_ip)

    def get_interface_result(self, device_ip: str) -> list[dict] | None:
        return self._waiters.take_result("interface", device_ip)

    def _notify_interface_waiter(self, source_ip: str, interfaces: list[dict]) -> None:
        self._waiters.notify("interface", source_ip, interfaces)

    def register_link_status_waiter(self, device_ip_address: str) -> DeferredAsyncioEvent:
        return self._waiters.register("link_status", device_ip_address)

    def unregister_link_status_waiter(self, device_ip_address: str) -> None:
        self._waiters.unregister("link_status", device_ip_address)

    def get_link_status_result(self, device_ip_address: str):
        return self._waiters.take_result("link_status", device_ip_address)

    def _notify_link_status_waiter(self, source_ip_address: str, observation) -> None:
        self._waiters.notify("link_status", source_ip_address, observation)

    def register_switch_configuration_waiter(self, device_ip_address: str) -> DeferredAsyncioEvent:
        return self._waiters.register("switch_configuration", device_ip_address)

    def unregister_switch_configuration_waiter(self, device_ip_address: str) -> None:
        self._waiters.unregister("switch_configuration", device_ip_address)

    def get_switch_configuration_result(self, device_ip_address: str) -> dict | None:
        return self._waiters.take_result("switch_configuration", device_ip_address)

    def _notify_switch_configuration_waiter(
        self,
        source_ip_address: str,
        observation: dict,
    ) -> None:
        self._waiters.notify("switch_configuration", source_ip_address, observation)

    def register_lock_status_waiter(self, device_ip_address: str) -> DeferredAsyncioEvent:
        return self._waiters.register("lock_status", device_ip_address)

    def unregister_lock_status_waiter(self, device_ip_address: str) -> None:
        self._waiters.unregister("lock_status", device_ip_address)
        self._waiters.take_result("lock_status", device_ip_address)

    def get_lock_status_result(
        self,
        device_ip_address: str,
    ) -> LockStatusObservation | None:
        return self._waiters.take_result("lock_status", device_ip_address)

    def _notify_lock_status_waiter(
        self,
        source_ip_address: str,
        observation: LockStatusObservation,
    ) -> None:
        self._waiters.notify("lock_status", source_ip_address, observation)

    def register_clear_configuration_status_waiter(self, device_ip_address: str) -> DeferredAsyncioEvent:
        return self._waiters.register("clear_configuration_status", device_ip_address)

    def unregister_clear_configuration_status_waiter(self, device_ip_address: str) -> None:
        self._waiters.unregister("clear_configuration_status", device_ip_address)

    def get_clear_configuration_status_result(self, device_ip_address: str) -> dict | None:
        return self._waiters.take_result("clear_configuration_status", device_ip_address)

    def _notify_clear_configuration_status_waiter(
        self,
        source_ip_address: str,
        status: dict,
    ) -> None:
        self._waiters.notify("clear_configuration_status", source_ip_address, status)
        for waiter in tuple(self._clear_configuration_status_waiters.get(source_ip_address, ())):
            waiter.observe(status)

    def register_clear_configuration_action_waiter(
        self,
        device_ip_address: str,
        expected_action_result_code: int,
    ) -> ClearConfigurationStatusWaiter:
        waiter = ClearConfigurationStatusWaiter(
            device_ip_address=device_ip_address,
            expected_action_result_code=expected_action_result_code,
        )
        self._clear_configuration_status_waiters.setdefault(device_ip_address, set()).add(waiter)
        return waiter

    def unregister_clear_configuration_action_waiter(
        self,
        waiter: ClearConfigurationStatusWaiter,
    ) -> None:
        waiters = self._clear_configuration_status_waiters.get(waiter.device_ip_address)
        if waiters is None:
            return
        waiters.discard(waiter)
        if not waiters:
            self._clear_configuration_status_waiters.pop(waiter.device_ip_address, None)

    def register_notification_waiter(
        self,
        device_ip_address: str,
        notification_ids,
    ) -> NotificationWaiter:
        waiter = NotificationWaiter(
            device_ip_address=device_ip_address,
            notification_ids=frozenset(notification_ids),
        )
        if not waiter.notification_ids:
            raise ValueError("notification_ids cannot be empty")
        self._notification_waiters.setdefault(device_ip_address, set()).add(waiter)
        return waiter

    def unregister_notification_waiter(self, waiter: NotificationWaiter) -> None:
        waiters = self._notification_waiters.get(waiter.device_ip_address)
        if waiters is None:
            return
        waiters.discard(waiter)
        if not waiters:
            self._notification_waiters.pop(waiter.device_ip_address, None)

    def _notify_notification_waiters(self, source_ip: str, notification_id: int) -> None:
        for waiter in tuple(self._notification_waiters.get(source_ip, ())):
            if notification_id in waiter.notification_ids:
                waiter.notification_id = notification_id
                waiter.event.set()

    def _notify_conmon_waiter(self, source_ip: str, opcode: int) -> None:
        if not self._waiters.is_registered("conmon", source_ip):
            return

        self._conmon_received.setdefault(source_ip, set()).add(opcode)
        expected = self._conmon_expected_count.get(source_ip, 2)

        if len(self._conmon_received[source_ip]) >= expected:
            self._waiters.notify("conmon", source_ip, True)


async def mutate_and_wait_for_capability_value(
    notifications: DanteNotificationService,
    capability_name: str,
    device_ip_address: str,
    expected_value: int,
    mutate,
    probe_status,
    timeout: float,
) -> tuple[int, list[int]] | None:
    waiter = notifications.register_capability_value_waiter(
        capability_name,
        device_ip_address,
        lambda current_value: current_value == expected_value,
    )
    probe_task = None

    async def probe_and_observe() -> None:
        try:
            status = await probe_status()
        except Exception as exception:
            logger.warning(f"Failed to probe {capability_name} status for {device_ip_address}: {exception}")
            return
        if status is None:
            logger.debug(f"{capability_name} status unavailable for {device_ip_address}")
            return
        current_value, supported_values = status
        waiter.observe(current_value, supported_values)

    try:
        await mutate()
        if not waiter.event.is_set():
            probe_task = asyncio.create_task(probe_and_observe())
            try:
                await asyncio.wait_for(waiter.event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug(f"Timed out waiting for {capability_name}={expected_value} from {device_ip_address}")
        return waiter.latest_result
    finally:
        notifications.unregister_capability_value_waiter(waiter)
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
    waiter = notifications.register_clear_configuration_action_waiter(
        device_ip_address,
        expected_action_result_code,
    )
    try:
        await mutate()
        try:
            await asyncio.wait_for(waiter.event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.debug(
                f"Timed out waiting for clear-configuration result "
                f"{expected_action_result_code} from {device_ip_address}"
            )
        return waiter.latest_result
    finally:
        notifications.unregister_clear_configuration_action_waiter(waiter)


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
            await asyncio.wait_for(waiter.event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        if waiter.error is not None:
            raise waiter.error
        return waiter.result
    finally:
        notifications.unregister_conmon_export_waiter(waiter)
