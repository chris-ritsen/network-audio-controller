from __future__ import annotations

import asyncio
import logging
import socket
import struct
from collections.abc import Callable
from dataclasses import dataclass, field as dataclass_field

from netaudio.dante.const import (
    DEVICE_INFO_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
)
from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio.dante.service import DanteMulticastService

logger = logging.getLogger("netaudio")

NOTIFICATION_TOPOLOGY_CHANGE = 16
NOTIFICATION_INTERFACE_STATUS = 17
NOTIFICATION_CLOCKING_STATUS = 32
NOTIFICATION_VERSIONS_STATUS = 96
NOTIFICATION_CLEAR_CONFIG_STATUS = 120
NOTIFICATION_SAMPLE_RATE_STATUS = 128
NOTIFICATION_ENCODING_STATUS = 130
NOTIFICATION_DEVICE_REBOOT = 146
NOTIFICATION_MANF_VERSIONS_STATUS = 192
NOTIFICATION_ROUTING_READY = 256
NOTIFICATION_TX_CHANNEL_CHANGE = 257
NOTIFICATION_RX_CHANNEL_CHANGE = 258
NOTIFICATION_TX_LABEL_CHANGE = 259
NOTIFICATION_TX_FLOW_CHANGE = 260
NOTIFICATION_RX_FLOW_CHANGE = 261
NOTIFICATION_PROPERTY_CHANGE = 262
NOTIFICATION_LATENCY_CHANGE = 262
NOTIFICATION_ROUTING_DEVICE_CHANGE = 288
NOTIFICATION_SETTINGS_CHANGE = 4110
NOTIFICATION_AES67_STATUS = 4103

NOTIFICATION_NAMES = {
    NOTIFICATION_TOPOLOGY_CHANGE: "Topology Change",
    NOTIFICATION_INTERFACE_STATUS: "Interface Status",
    NOTIFICATION_CLOCKING_STATUS: "Clocking Status",
    NOTIFICATION_VERSIONS_STATUS: "Versions Status",
    NOTIFICATION_CLEAR_CONFIG_STATUS: "Clear Config Status",
    NOTIFICATION_SAMPLE_RATE_STATUS: "Sample Rate Status",
    NOTIFICATION_ENCODING_STATUS: "Encoding Status",
    NOTIFICATION_DEVICE_REBOOT: "Device Reboot",
    NOTIFICATION_MANF_VERSIONS_STATUS: "Manufacturer Versions Status",
    NOTIFICATION_ROUTING_READY: "Routing Ready",
    NOTIFICATION_TX_CHANNEL_CHANGE: "TX Channel Change",
    NOTIFICATION_RX_CHANNEL_CHANGE: "RX Channel Change",
    NOTIFICATION_TX_LABEL_CHANGE: "TX Label Change",
    NOTIFICATION_TX_FLOW_CHANGE: "TX Flow Change",
    NOTIFICATION_RX_FLOW_CHANGE: "RX Flow Change",
    NOTIFICATION_PROPERTY_CHANGE: "Property Change",
    NOTIFICATION_ROUTING_DEVICE_CHANGE: "Routing Device Change",
    NOTIFICATION_SETTINGS_CHANGE: "Settings Change",
    NOTIFICATION_AES67_STATUS: "AES67 Status",
}

CONMON_OPCODE_INTERFACE_STATUS = 0x0011
CONMON_OPCODE_MAKE_MODEL_RESPONSE = 0x00C0
CONMON_OPCODE_DANTE_MODEL_RESPONSE = 0x0060
CONMON_OPCODE_SAMPLE_RATE_STATUS = 0x0080
CONMON_OPCODE_ENCODING_STATUS = 0x0082
CONMON_OPCODE_AES67_CURRENT_NEW = 0x1007
CONMON_AES67_CURRENT_NEW_OFFSET = 0x21
CONMON_OPCODE_PTP_CLOCK_STATUS = 0x0020
CONMON_PREFERRED_LEADER_OFFSET = 0x26
CONMON_PTP_V1_ROLE_OFFSET = 0x48
PTP_V1_ROLE_MASTER = 0x0006
PTP_V1_ROLE_SLAVE = 0x0009

PTP_V1_ROLE_MAP = {
    PTP_V1_ROLE_MASTER: "Leader",
    PTP_V1_ROLE_SLAVE: "Follower",
}
PROTOCOL_SETTINGS = 0xFFFF
PROTOCOL_CONTROL = 0x27FF

INTERFACE_MODE_DYNAMIC = 0x0001
INTERFACE_MODE_STATIC = 0x0003

INTERFACE_MODE_NAMES = {
    INTERFACE_MODE_DYNAMIC: "dynamic",
    INTERFACE_MODE_STATIC: "static",
}

INTERFACE_RECORD_SIZE = 20
INTERFACE_CONFIGURED_RECORD_SIZE = 24
INTERFACE_CONFIGURED_RECORD_STRIDE = 28

AES67_CURRENT_NEW_MAP = {
    0x00: (False, False),
    0x01: (True, False),
    0x02: (False, True),
    0x03: (True, True),
}


def parse_aes67_current_new_byte(state_byte: int) -> tuple[bool | None, bool | None]:
    result = AES67_CURRENT_NEW_MAP.get(state_byte)
    if result is not None:
        return result
    return (None, None)


class _WaiterRegistry:
    def __init__(self):
        self._waiters: dict[tuple[str, str], asyncio.Event] = {}
        self._results: dict[tuple[str, str], object] = {}

    def register(self, kind: str, key: str) -> asyncio.Event:
        event = asyncio.Event()
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
    event: asyncio.Event = dataclass_field(default_factory=asyncio.Event)
    notification_id: int | None = None


@dataclass(eq=False)
class CapabilityValueWaiter:
    capability_name: str
    device_ip_address: str
    value_matches: Callable[[int], bool]
    event: asyncio.Event = dataclass_field(default_factory=asyncio.Event)
    latest_result: tuple[int, list[int]] | None = None

    def observe(self, current_value: int, supported_values: list[int]) -> None:
        if self.event.is_set():
            return
        self.latest_result = (current_value, supported_values)
        if self.value_matches(current_value):
            self.event.set()


@dataclass(frozen=True)
class _CapabilityStatusChanges:
    device_state_changed: bool = False
    current_value_changed: bool = False
    supported_values_changed: bool = False


class DanteNotificationService(DanteMulticastService):
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

    def set_device_lookup(self, lookup_func):
        self._device_lookup = lookup_func

    def register_conmon_waiter(self, device_ip: str, expected_count: int = 2) -> asyncio.Event:
        self._conmon_received[device_ip] = set()
        self._conmon_expected_count[device_ip] = expected_count
        return self._waiters.register("conmon", device_ip)

    def unregister_conmon_waiter(self, device_ip: str) -> None:
        self._waiters.unregister("conmon", device_ip)
        self._conmon_received.pop(device_ip, None)
        self._conmon_expected_count.pop(device_ip, None)

    def register_aes67_waiter(self, device_ip: str) -> asyncio.Event:
        return self._waiters.register("aes67", device_ip)

    def unregister_aes67_waiter(self, device_ip: str) -> None:
        self._waiters.unregister("aes67", device_ip)

    def get_aes67_result(self, device_ip: str) -> tuple[bool | None, bool | None] | None:
        return self._waiters.take_result("aes67", device_ip)

    def _notify_aes67_waiter(self, source_ip: str, current: bool | None, configured: bool | None) -> None:
        self._waiters.notify("aes67", source_ip, (current, configured))

    def register_sample_rate_waiter(self, device_ip_address: str) -> asyncio.Event:
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

    def register_encoding_waiter(self, device_ip_address: str) -> asyncio.Event:
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

    def register_preferred_leader_waiter(self, device_ip: str) -> asyncio.Event:
        return self._waiters.register("preferred_leader", device_ip)

    def unregister_preferred_leader_waiter(self, device_ip: str) -> None:
        self._waiters.unregister("preferred_leader", device_ip)

    def get_preferred_leader_result(self, device_ip: str) -> bool | None:
        return self._waiters.take_result("preferred_leader", device_ip)

    def _notify_preferred_leader_waiter(self, source_ip: str, preferred_leader: bool | None) -> None:
        self._waiters.notify("preferred_leader", source_ip, preferred_leader)

    def register_interface_waiter(self, device_ip: str) -> asyncio.Event:
        return self._waiters.register("interface", device_ip)

    def unregister_interface_waiter(self, device_ip: str) -> None:
        self._waiters.unregister("interface", device_ip)

    def get_interface_result(self, device_ip: str) -> list[dict] | None:
        return self._waiters.take_result("interface", device_ip)

    def _notify_interface_waiter(self, source_ip: str, interfaces: list[dict]) -> None:
        self._waiters.notify("interface", source_ip, interfaces)

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

    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        if len(data) < 4:
            return

        source_ip = addr[0]

        if self._dissect:
            try:
                from netaudio.common.app_config import settings as app_settings
                from netaudio.dante.packet_dissector import dissect_and_render, format_dissect_label

                color = not app_settings.no_color
                label = format_dissect_label("multicast", f"{source_ip}:{addr[1]}", color=color)
                rendered = dissect_and_render(data, indent="  ", color=color)
                logger.debug(f"Dissect [{label}] {len(data)}B:\n{rendered}")
            except Exception as exception:
                logger.debug(f"Dissect error: {exception}")

        if self._packet_store:
            device = self._lookup_device(source_ip)
            try:
                self._packet_store.store_packet(
                    payload=data,
                    source_type="multicast",
                    src_ip=source_ip,
                    src_port=addr[1],
                    device_name=device.name if device else None,
                    device_ip=source_ip,
                    multicast_group=self._multicast_group,
                    multicast_port=self._multicast_port,
                    session_id=self._session_id,
                )
            except Exception as exception:
                logger.debug(f"PacketStore error (notification): {exception}")

        protocol_id = struct.unpack(">H", data[0:2])[0]

        if protocol_id == PROTOCOL_SETTINGS:
            if self._handle_conmon_response(data, source_ip):
                return
            self._handle_settings_notification(data, source_ip)
            return

        device = self._lookup_device(source_ip)
        device_name = device.name if device else ""
        server_name = device.server_name if device else ""

        if len(data) < 28:
            logger.debug(
                f"Short multicast packet from {source_ip} ({device_name}), "
                f"{len(data)} bytes, protocol=0x{protocol_id:04X}, hex={data.hex()}"
            )
            return

        notification_id = struct.unpack(">H", data[26:28])[0]
        notification_name = NOTIFICATION_NAMES.get(notification_id, f"Unknown(0x{notification_id:04X})")
        self._notify_notification_waiters(source_ip, notification_id)

        logger.debug(f"Notification from {source_ip} ({device_name}): {notification_name} (id={notification_id})")

        self._dispatcher.emit_nowait(
            DanteEvent(
                type=EventType.NOTIFICATION_RECEIVED,
                device_name=device_name,
                server_name=server_name,
                data={
                    "notification_id": notification_id,
                    "notification_name": notification_name,
                    "source_ip": source_ip,
                    "raw": data,
                },
            )
        )

    def _handle_settings_notification(
        self,
        data: bytes,
        source_ip: str,
        state_applied: bool = False,
        conmon_response: bool = False,
        current_value_changed: bool = False,
        supported_values_changed: bool = False,
    ) -> None:
        device = self._lookup_device(source_ip)
        device_name = device.name if device else ""
        server_name = device.server_name if device else ""

        if len(data) >= 28:
            notification_id = struct.unpack(">H", data[26:28])[0]
        else:
            notification_id = None

        logger.debug(
            f"Settings notification from {source_ip} ({device_name}), "
            f"{len(data)} bytes, notification_id={notification_id}, hex={data.hex()}"
        )

        if notification_id is not None:
            notification_name = NOTIFICATION_NAMES.get(notification_id, f"Unknown(0x{notification_id:04X})")
            self._notify_notification_waiters(source_ip, notification_id)
            self._dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.NOTIFICATION_RECEIVED,
                    device_name=device_name,
                    server_name=server_name,
                    data={
                        "notification_id": notification_id,
                        "notification_name": notification_name,
                        "source_ip": source_ip,
                        "state_applied": state_applied,
                        "conmon_response": conmon_response,
                        "current_value_changed": current_value_changed,
                        "supported_values_changed": supported_values_changed,
                        "raw": data,
                    },
                )
            )

    def _handle_conmon_response(self, data: bytes, source_ip: str) -> bool:
        opcode = self._extract_conmon_opcode(data)

        if opcode is None:
            return False

        if opcode == CONMON_OPCODE_INTERFACE_STATUS:
            self._handle_interface_status(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_SAMPLE_RATE_STATUS:
            changes = self._handle_sample_rate_status(data, source_ip)
            self._handle_settings_notification(
                data,
                source_ip,
                state_applied=changes.device_state_changed,
                conmon_response=True,
                current_value_changed=changes.current_value_changed,
                supported_values_changed=changes.supported_values_changed,
            )
            return True
        elif opcode == CONMON_OPCODE_ENCODING_STATUS:
            changes = self._handle_encoding_status(data, source_ip)
            self._handle_settings_notification(
                data,
                source_ip,
                state_applied=changes.device_state_changed,
                conmon_response=True,
                current_value_changed=changes.current_value_changed,
                supported_values_changed=changes.supported_values_changed,
            )
            return True
        elif opcode == CONMON_OPCODE_MAKE_MODEL_RESPONSE:
            self._handle_make_model_response(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_DANTE_MODEL_RESPONSE:
            self._handle_dante_model_response(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_AES67_CURRENT_NEW:
            self._handle_aes67_current_new(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_PTP_CLOCK_STATUS:
            self._handle_ptp_clock_status(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True

        return False

    def _handle_sample_rate_status(self, data: bytes, source_ip: str) -> _CapabilityStatusChanges:
        return self._handle_capability_status(
            data,
            source_ip,
            "sample_rate_status",
            "sample rate",
            "current_sample_rate",
            "supported_sample_rates",
            "sample_rate",
            "supported_sample_rates",
            self._notify_sample_rate_waiter,
            self._apply_sample_rate_status,
            defer_current_value_change=True,
        )

    def _handle_encoding_status(self, data: bytes, source_ip: str) -> _CapabilityStatusChanges:
        return self._handle_capability_status(
            data,
            source_ip,
            "encoding_status",
            "encoding",
            "current_encoding",
            "supported_encodings",
            "encoding",
            "supported_encodings",
            self._notify_encoding_waiter,
            self._apply_encoding_status,
            defer_current_value_change=False,
        )

    def _handle_capability_status(
        self,
        data: bytes,
        source_ip: str,
        response_kind: str,
        capability_name: str,
        current_response_field_name: str,
        supported_response_field_name: str,
        current_device_field_name: str,
        supported_device_field_name: str,
        notify_waiter,
        apply_status,
        defer_current_value_change: bool,
    ) -> _CapabilityStatusChanges:
        from netaudio import core

        try:
            parsed_response = core.parse_response(response_kind, data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid {capability_name} status from {source_ip}: {exception}")
            return _CapabilityStatusChanges()

        current_value = parsed_response[current_response_field_name]
        supported_values = parsed_response[supported_response_field_name]
        notify_waiter(source_ip, current_value, supported_values)
        self._notify_capability_value_waiters(
            current_device_field_name,
            source_ip,
            current_value,
            supported_values,
        )
        logger.debug(
            f"Conmon {response_kind} from {source_ip} ({len(data)}B): "
            f"current={current_value} supported={supported_values}"
        )

        device = self._lookup_device(source_ip)
        if device is None:
            self._cache_pending(
                source_ip,
                {
                    current_device_field_name: current_value,
                    supported_device_field_name: supported_values,
                },
            )
            return _CapabilityStatusChanges()
        if not device.online:
            return _CapabilityStatusChanges()

        changes = apply_status(device, current_value, supported_values)
        should_emit_update = changes.device_state_changed and not (
            defer_current_value_change and changes.current_value_changed
        )
        if should_emit_update:
            self._dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_UPDATED,
                    device_name=device.name,
                    server_name=device.server_name,
                )
            )
        return changes

    @staticmethod
    def _apply_sample_rate_status(
        device,
        current_sample_rate: int,
        supported_sample_rates: list[int],
    ) -> _CapabilityStatusChanges:
        current_value_changed = device.sample_rate is not None and device.sample_rate != current_sample_rate
        supported_values_changed = device.supported_sample_rates != supported_sample_rates
        device_state_changed = device.sample_rate != current_sample_rate or supported_values_changed
        device.sample_rate = current_sample_rate
        device.supported_sample_rates = supported_sample_rates
        return _CapabilityStatusChanges(
            device_state_changed=device_state_changed,
            current_value_changed=current_value_changed,
            supported_values_changed=supported_values_changed,
        )

    @staticmethod
    def _apply_encoding_status(
        device,
        current_encoding: int,
        supported_encodings: list[int],
    ) -> _CapabilityStatusChanges:
        current_value_changed = device.encoding is not None and device.encoding != current_encoding
        supported_values_changed = device.supported_encodings != supported_encodings
        device_state_changed = device.encoding != current_encoding or supported_values_changed
        device.encoding = current_encoding
        device.supported_encodings = supported_encodings
        return _CapabilityStatusChanges(
            device_state_changed=device_state_changed,
            current_value_changed=current_value_changed,
            supported_values_changed=supported_values_changed,
        )

    def _handle_make_model_response(self, data: bytes, source_ip: str) -> None:
        product_name, product_version, manufacturer = self.parse_make_model_response(data)
        logger.debug(
            f"Conmon make_model from {source_ip} ({len(data)}B): "
            f"name={product_name!r} version={product_version!r} manufacturer={manufacturer!r}"
        )

        parsed = {}

        if product_name:
            parsed["dante_model"] = product_name

        if product_version:
            parsed["product_version"] = product_version

        if manufacturer:
            parsed["manufacturer"] = manufacturer

        if not parsed:
            return

        device = self._lookup_device(source_ip)

        if device is None:
            self._cache_pending(source_ip, parsed)
            return

        self._apply_conmon_data(device, parsed)

    def _handle_dante_model_response(self, data: bytes, source_ip: str) -> None:
        board_codename, board_name = self.parse_dante_model_response(data)
        logger.debug(
            f"Conmon dante_model from {source_ip} ({len(data)}B): codename={board_codename!r} board_name={board_name!r}"
        )

        parsed = {}

        if board_codename:
            parsed["dante_model_id"] = board_codename

        if board_name:
            parsed["board_name"] = board_name

        if not parsed:
            return

        device = self._lookup_device(source_ip)

        if device is None:
            self._cache_pending(source_ip, parsed)
            return

        self._apply_conmon_data(device, parsed)

    def _handle_aes67_current_new(self, data: bytes, source_ip: str) -> None:
        if len(data) <= CONMON_AES67_CURRENT_NEW_OFFSET:
            return

        state_byte = data[CONMON_AES67_CURRENT_NEW_OFFSET]
        aes67_current, aes67_configured = parse_aes67_current_new_byte(state_byte)

        logger.debug(
            f"Conmon aes67_current_new from {source_ip} ({len(data)}B): "
            f"byte=0x{state_byte:02X} current={aes67_current} configured={aes67_configured}"
        )

        device = self._lookup_device(source_ip)

        if device is None:
            parsed = {}
            if aes67_current is not None:
                parsed["aes67_current"] = aes67_current
            if aes67_configured is not None:
                parsed["aes67_configured"] = aes67_configured
            if parsed:
                self._cache_pending(source_ip, parsed)
        else:
            if aes67_current is not None:
                device.aes67_current = aes67_current
            if aes67_configured is not None:
                device.aes67_configured = aes67_configured

        self._notify_aes67_waiter(source_ip, aes67_current, aes67_configured)

    def _handle_ptp_clock_status(self, data: bytes, source_ip: str) -> None:
        if len(data) <= CONMON_PREFERRED_LEADER_OFFSET:
            return

        preferred_leader_byte = data[CONMON_PREFERRED_LEADER_OFFSET]
        preferred_leader = preferred_leader_byte == 0x01

        ptp_v1_role = None
        if len(data) >= CONMON_PTP_V1_ROLE_OFFSET + 2:
            role_value = struct.unpack(">H", data[CONMON_PTP_V1_ROLE_OFFSET : CONMON_PTP_V1_ROLE_OFFSET + 2])[0]
            ptp_v1_role = PTP_V1_ROLE_MAP.get(role_value)

        logger.debug(
            f"Conmon ptp_clock_status from {source_ip} ({len(data)}B): "
            f"preferred_leader=0x{preferred_leader_byte:02X} ({preferred_leader}) "
            f"ptp_v1_role={ptp_v1_role}"
        )

        device = self._lookup_device(source_ip)

        parsed = {"preferred_leader": preferred_leader}
        if ptp_v1_role is not None:
            parsed["ptp_v1_role"] = ptp_v1_role

        if device is None:
            self._cache_pending(source_ip, parsed)
        else:
            device.preferred_leader = preferred_leader
            if ptp_v1_role is not None:
                device.ptp_v1_role = ptp_v1_role

        self._notify_preferred_leader_waiter(source_ip, preferred_leader)

    def _handle_interface_status(self, data: bytes, source_ip: str) -> None:
        if len(data) < 0x40:
            return

        interface_count = struct.unpack(">H", data[0x20:0x22])[0]
        interfaces = []

        offset = 0x28
        for _ in range(interface_count):
            if offset + INTERFACE_RECORD_SIZE > len(data):
                break

            mode_value = struct.unpack(">H", data[offset : offset + 2])[0]
            mode = INTERFACE_MODE_NAMES.get(mode_value, f"unknown(0x{mode_value:04X})")
            configured = mode in ("dynamic", "static")
            record_size = INTERFACE_CONFIGURED_RECORD_SIZE if configured else INTERFACE_RECORD_SIZE
            if offset + record_size > len(data):
                break

            mac_bytes = data[offset + 2 : offset + 8]
            mac_address = ":".join(f"{byte:02X}" for byte in mac_bytes)
            ip_address = socket.inet_ntoa(data[offset + 8 : offset + 12])
            netmask = socket.inet_ntoa(data[offset + 12 : offset + 16])

            interface_info = {
                "mode": mode,
                "mac_address": mac_address,
                "ip_address": ip_address,
                "netmask": netmask,
            }

            if mode == "dynamic":
                gateway = socket.inet_ntoa(data[offset + 16 : offset + 20])
                dns_server = socket.inet_ntoa(data[offset + 20 : offset + 24])
                interface_info["gateway"] = gateway
                interface_info["dns_server"] = dns_server
                offset += INTERFACE_CONFIGURED_RECORD_STRIDE
            elif mode == "static":
                dns_server = socket.inet_ntoa(data[offset + 16 : offset + 20])
                gateway = socket.inet_ntoa(data[offset + 20 : offset + 24])
                interface_info["dns_server"] = dns_server
                interface_info["gateway"] = gateway
                offset += INTERFACE_CONFIGURED_RECORD_STRIDE
            else:
                offset += INTERFACE_RECORD_SIZE

            interfaces.append(interface_info)

        reboot_required = False
        pending_config = None
        if interface_count == 1 and len(data) > 0x49:
            reboot_flag = struct.unpack(">H", data[0x48:0x4A])[0]
            reboot_required = reboot_flag != 0

            if reboot_flag == 0x0004:
                pending_config = {"mode": "dynamic"}
            elif reboot_flag == 0x0006 and len(data) >= 0x5C:
                pending_ip = socket.inet_ntoa(data[0x4C:0x50])
                pending_mask = socket.inet_ntoa(data[0x50:0x54])
                pending_dns = socket.inet_ntoa(data[0x54:0x58])
                pending_gw = socket.inet_ntoa(data[0x58:0x5C])
                pending_config = {
                    "mode": "static",
                    "ip_address": pending_ip,
                    "netmask": pending_mask,
                    "dns_server": pending_dns,
                    "gateway": pending_gw,
                }

        logger.debug(
            f"Conmon interface_status from {source_ip} ({len(data)}B): "
            f"interface_count={interface_count} reboot_required={reboot_required} "
            f"pending_config={pending_config} interfaces={interfaces}"
        )

        device = self._lookup_device(source_ip)

        if device is None:
            self._cache_pending(
                source_ip,
                {
                    "interfaces": interfaces,
                    "interface_reboot_required": reboot_required,
                    "interface_pending_config": pending_config,
                },
            )
        else:
            device.interfaces = interfaces
            device.interface_reboot_required = reboot_required
            device.interface_pending_config = pending_config

        self._notify_interface_waiter(source_ip, interfaces)

    def _cache_pending(self, source_ip: str, parsed: dict) -> None:
        if source_ip not in self._pending_conmon:
            self._pending_conmon[source_ip] = {}
        self._pending_conmon[source_ip].update(parsed)

    @staticmethod
    def _apply_conmon_data(device, parsed: dict) -> None:
        for field, value in parsed.items():
            if field in (
                "manufacturer",
                "sample_rate",
                "supported_sample_rates",
                "encoding",
                "supported_encodings",
            ):
                setattr(device, field, value)
            elif not getattr(device, field, None):
                setattr(device, field, value)

    def apply_pending_for_device(self, device) -> None:
        if not device.ipv4:
            return

        ip_str = str(device.ipv4)
        pending = self._pending_conmon.pop(ip_str, None)

        if pending:
            self._apply_conmon_data(device, pending)
            logger.debug(f"Applied pending conmon data for {ip_str}: {list(pending.keys())}")

    @staticmethod
    def _extract_conmon_opcode(data: bytes) -> int | None:
        if len(data) < 0x20:
            return None

        magic_pos = data.find(b"Audinate", 4)

        if magic_pos < 0:
            return None

        opcode_pos = magic_pos + 10

        if opcode_pos + 2 > len(data):
            return None

        return struct.unpack(">H", data[opcode_pos : opcode_pos + 2])[0]

    @staticmethod
    def parse_make_model_response(data: bytes) -> tuple[str, str, str]:
        from netaudio import core

        parsed = core.parse_response("make_model", data)
        return parsed["product_name"], parsed["product_version"], parsed["manufacturer"]

    @staticmethod
    def parse_dante_model_response(data: bytes) -> tuple[str, str]:
        from netaudio import core

        parsed = core.parse_response("dante_model", data)
        return parsed["board_codename"], parsed["board_name"]

    def _lookup_device(self, ip_str: str):
        if self._device_lookup:
            return self._device_lookup(ip_str)
        return None


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
                logger.debug(
                    f"Timed out waiting for {capability_name}={expected_value} from {device_ip_address}"
                )
        return waiter.latest_result
    finally:
        notifications.unregister_capability_value_waiter(waiter)
        if probe_task is not None and not probe_task.done():
            probe_task.cancel()
        if probe_task is not None:
            await asyncio.gather(probe_task, return_exceptions=True)
