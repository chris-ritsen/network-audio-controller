from __future__ import annotations

import logging
import struct

from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.gain import SUPPORTED_GAIN_LEVELS
from netaudio.dante.lock_status import LockStatusObservation
from netaudio.dante.services.link_status_packet_handler import LinkStatusPacketHandler
from netaudio.dante.services.notification_protocol import (
    CONMON_AES67_CURRENT_NEW_OFFSET,
    CONMON_OPCODE_AES67_CURRENT_NEW,
    CONMON_OPCODE_CLEAR_CONFIGURATION_STATUS,
    CONMON_OPCODE_DANTE_MODEL_RESPONSE,
    CONMON_OPCODE_ENCODING_STATUS,
    CONMON_OPCODE_EXPORT_FRAGMENT,
    CONMON_OPCODE_GAIN_STATUS,
    CONMON_OPCODE_INTERFACE_STATUS,
    CONMON_OPCODE_LINK_STATUS,
    CONMON_OPCODE_LOCK_RESET_STATUS,
    CONMON_OPCODE_MAKE_MODEL_RESPONSE,
    CONMON_OPCODE_PTP_CLOCK_STATUS,
    CONMON_OPCODE_ROUTING_CAPACITY_STATUS,
    CONMON_OPCODE_SAMPLE_RATE_PULLUP_STATUS,
    CONMON_OPCODE_SAMPLE_RATE_STATUS,
    CONMON_OPCODE_SWITCH_CONFIGURATION_STATUS,
    NOTIFICATION_NAMES,
    PROTOCOL_SETTINGS,
    CapabilityStatusChanges,
    extract_conmon_opcode,
    parse_aes67_current_new_byte,
    parse_dante_model_response,
    parse_make_model_response,
)


logger = logging.getLogger("netaudio")


class NotificationPacketHandlers(LinkStatusPacketHandler):
    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        source_ip = addr[0]
        if not data and addr[1] == DEVICE_SETTINGS_PORT:
            waiter = self._conmon_export_waiters.get(source_ip)
            if waiter is not None:
                waiter.observe_unavailable()
            return
        if len(data) < 4:
            return

        if self._dissect:
            try:
                from netaudio.common.app_config import settings as app_settings
                from netaudio.dante.packet_dissection_rendering import dissect_and_render, format_dissect_label

                color = not app_settings.no_color
                label = format_dissect_label("multicast", f"{source_ip}:{addr[1]}", color=color)
                rendered = dissect_and_render(data, indent="  ", color=color)
                logger.debug(f"Dissect [{label}] {len(data)}B:\n{rendered}")
            except Exception as exception:
                logger.warning(f"Dissect error: {exception}", exc_info=True)

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
                logger.warning(f"PacketStore error (notification): {exception}", exc_info=True)

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
        opcode = extract_conmon_opcode(data)

        if opcode is None:
            return False

        if opcode == CONMON_OPCODE_EXPORT_FRAGMENT:
            return self._handle_conmon_export_fragment(data, source_ip)
        if opcode == CONMON_OPCODE_LINK_STATUS:
            self._handle_link_status(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        if opcode == CONMON_OPCODE_SWITCH_CONFIGURATION_STATUS:
            self._handle_switch_configuration_status(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        if opcode == CONMON_OPCODE_INTERFACE_STATUS:
            self._handle_interface_status(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_ROUTING_CAPACITY_STATUS:
            state_changed = self._handle_routing_capacity_status(data, source_ip)
            if state_changed is not None:
                self._handle_settings_notification(
                    data,
                    source_ip,
                    state_applied=state_changed,
                    conmon_response=True,
                )
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
        elif opcode == CONMON_OPCODE_SAMPLE_RATE_PULLUP_STATUS:
            changes = self._handle_sample_rate_pullup_status(data, source_ip)
            self._handle_settings_notification(
                data,
                source_ip,
                state_applied=changes.device_state_changed,
                conmon_response=True,
                current_value_changed=changes.current_value_changed,
                supported_values_changed=changes.supported_values_changed,
            )
            return True
        elif opcode == CONMON_OPCODE_GAIN_STATUS:
            changes = self._handle_gain_status(data, source_ip)
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
        elif opcode == CONMON_OPCODE_LOCK_RESET_STATUS:
            self._handle_lock_reset_status(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_CLEAR_CONFIGURATION_STATUS:
            self._handle_clear_configuration_status(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True

        return False

    def _handle_switch_configuration_status(self, data: bytes, source_ip: str) -> None:
        from netaudio import core

        try:
            parsed_response = core.parse_response("switch_configuration_status", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid switch configuration status from {source_ip}: {exception}")
            return
        self._notify_switch_configuration_waiter(source_ip, parsed_response)

    def _handle_conmon_export_fragment(self, data: bytes, source_ip: str) -> bool:
        waiter = self._conmon_export_waiters.get(source_ip)
        if waiter is None:
            return False
        from netaudio import core

        try:
            fragment = core.parse_response("conmon_export_fragment", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid ConMon export fragment from {source_ip}: {exception}")
            return True
        if not waiter.collector.matches(fragment):
            return False
        waiter.observe(fragment)
        return True

    def _handle_routing_capacity_status(self, data: bytes, source_ip: str) -> bool | None:
        from netaudio import core

        try:
            parsed_response = core.parse_response("routing_capacity_status", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid routing-capacity status from {source_ip}: {exception}")
            return None

        routing_ready = parsed_response["routing_ready"]
        transmit_channel_count = parsed_response["transmit_channel_count"]
        receive_channel_count = parsed_response["receive_channel_count"]
        state = {
            "routing_ready": routing_ready,
            "routing_ready_state_code": parsed_response["state_code"],
            "routing_capacity_transmit_channel_count": transmit_channel_count,
            "routing_capacity_receive_channel_count": receive_channel_count,
        }
        device = self._lookup_device(source_ip)
        if device is None:
            self._cache_pending(source_ip, state)
            return False
        if not device.online:
            return False

        if routing_ready is True and device.tx_count is None:
            state["tx_count"] = transmit_channel_count
            state["tx_count_raw"] = transmit_channel_count
        if routing_ready is True and device.rx_count is None:
            state["rx_count"] = receive_channel_count
            state["rx_count_raw"] = receive_channel_count
        state_changed = any(getattr(device, field_name) != value for field_name, value in state.items())
        for field_name, value in state.items():
            setattr(device, field_name, value)
        if state_changed:
            self._dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_UPDATED,
                    device_name=device.name,
                    server_name=device.server_name,
                )
            )
        return state_changed

    def _handle_sample_rate_status(self, data: bytes, source_ip: str) -> CapabilityStatusChanges:
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

    def _handle_encoding_status(self, data: bytes, source_ip: str) -> CapabilityStatusChanges:
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

    def _handle_sample_rate_pullup_status(self, data: bytes, source_ip: str) -> CapabilityStatusChanges:
        from netaudio import core

        try:
            parsed_response = core.parse_response("sample_rate_pullup_status", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid sample rate pull-up status from {source_ip}: {exception}")
            return CapabilityStatusChanges()

        current_raw_value = parsed_response["applied_value"]["raw_value"]
        requested_raw_value = parsed_response["requested_value"]["raw_value"]
        supported_raw_values = [value["raw_value"] for value in parsed_response["supported_values"]]
        self._notify_sample_rate_pullup_waiter(source_ip, current_raw_value, supported_raw_values)
        self._notify_capability_value_waiters(
            "sample_rate_pullup_raw_value",
            source_ip,
            current_raw_value,
            supported_raw_values,
        )

        device = self._lookup_device(source_ip)
        pending_state = {
            "sample_rate_pullup_raw_value": current_raw_value,
            "requested_sample_rate_pullup_raw_value": requested_raw_value,
            "supported_sample_rate_pullup_raw_values": supported_raw_values,
        }
        if device is None:
            self._cache_pending(source_ip, pending_state)
            return CapabilityStatusChanges()
        if not device.online:
            return CapabilityStatusChanges()

        current_value_changed = (
            device.sample_rate_pullup_raw_value is not None and device.sample_rate_pullup_raw_value != current_raw_value
        )
        supported_values_changed = device.supported_sample_rate_pullup_raw_values != supported_raw_values
        device_state_changed = any(getattr(device, name) != value for name, value in pending_state.items())
        for name, value in pending_state.items():
            setattr(device, name, value)
        if device_state_changed:
            self._dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_UPDATED,
                    device_name=device.name,
                    server_name=device.server_name,
                )
            )
        return CapabilityStatusChanges(
            device_state_changed=device_state_changed,
            current_value_changed=current_value_changed,
            supported_values_changed=supported_values_changed,
        )

    def _handle_gain_status(self, data: bytes, source_ip: str) -> CapabilityStatusChanges:
        from netaudio import core

        try:
            parsed_response = core.parse_response("gain_status", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid gain status from {source_ip}: {exception}")
            return CapabilityStatusChanges()

        device_type = parsed_response["device_type"]
        channel_levels = parsed_response["channel_levels"]
        supported_gain_levels = list(SUPPORTED_GAIN_LEVELS)
        self._notify_gain_status_waiters(source_ip, device_type, channel_levels)

        device = self._lookup_device(source_ip)
        if device is None:
            self._cache_pending(
                source_ip,
                {
                    "gain_device_type": device_type,
                    "gain_levels": channel_levels,
                    "supported_gain_levels": supported_gain_levels,
                },
            )
            return CapabilityStatusChanges()
        if not device.online:
            return CapabilityStatusChanges()

        current_value_changed = device.gain_levels is not None and device.gain_levels != channel_levels
        supported_values_changed = device.supported_gain_levels != supported_gain_levels
        device_state_changed = (
            device.gain_device_type != device_type or device.gain_levels != channel_levels or supported_values_changed
        )
        device.gain_device_type = device_type
        device.gain_levels = channel_levels
        device.supported_gain_levels = supported_gain_levels
        if device_state_changed:
            self._dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_UPDATED,
                    device_name=device.name,
                    server_name=device.server_name,
                )
            )
        return CapabilityStatusChanges(
            device_state_changed=device_state_changed,
            current_value_changed=current_value_changed,
            supported_values_changed=supported_values_changed,
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
    ) -> CapabilityStatusChanges:
        from netaudio import core

        try:
            parsed_response = core.parse_response(response_kind, data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid {capability_name} status from {source_ip}: {exception}")
            return CapabilityStatusChanges()

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
            return CapabilityStatusChanges()
        if not device.online:
            return CapabilityStatusChanges()

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
    ) -> CapabilityStatusChanges:
        current_value_changed = device.sample_rate is not None and device.sample_rate != current_sample_rate
        supported_values_changed = device.supported_sample_rates != supported_sample_rates
        device_state_changed = device.sample_rate != current_sample_rate or supported_values_changed
        device.sample_rate = current_sample_rate
        device.supported_sample_rates = supported_sample_rates
        return CapabilityStatusChanges(
            device_state_changed=device_state_changed,
            current_value_changed=current_value_changed,
            supported_values_changed=supported_values_changed,
        )

    @staticmethod
    def _apply_encoding_status(
        device,
        current_encoding: int,
        supported_encodings: list[int],
    ) -> CapabilityStatusChanges:
        current_value_changed = device.encoding is not None and device.encoding != current_encoding
        supported_values_changed = device.supported_encodings != supported_encodings
        device_state_changed = device.encoding != current_encoding or supported_values_changed
        device.encoding = current_encoding
        device.supported_encodings = supported_encodings
        return CapabilityStatusChanges(
            device_state_changed=device_state_changed,
            current_value_changed=current_value_changed,
            supported_values_changed=supported_values_changed,
        )

    def _handle_make_model_response(self, data: bytes, source_ip: str) -> None:
        product_name, product_version, manufacturer = parse_make_model_response(data)
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
        board_codename, board_name = parse_dante_model_response(data)
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
        from netaudio import core
        from netaudio.dante.clock_identity import canonical_clock_identity

        try:
            parsed_response = core.parse_response("ptp_clock_status", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid PTP clock status from {source_ip}: {exception}")
            return

        preferred_leader = parsed_response["preferred_leader"]
        clock_source_code = parsed_response["clock_source_code"]
        clock_subdomain = parsed_response.get("clock_subdomain")
        clock_frequency_offset_parts_per_billion = parsed_response["clock_frequency_offset_parts_per_billion"]
        clock_port_state_code = parsed_response["clock_port_state_code"]
        clock_role = parsed_response.get("clock_role")
        clock_port_records = parsed_response.get("clock_port_records")
        clock_identity = canonical_clock_identity(parsed_response.get("clock_identity"))
        leader_clock_identity = canonical_clock_identity(parsed_response.get("leader_clock_identity"))

        logger.debug(
            f"Conmon ptp_clock_status from {source_ip} ({len(data)}B): "
            f"preferred_leader={preferred_leader} "
            f"clock_source_code=0x{clock_source_code:04X} "
            f"clock_frequency_offset_parts_per_billion={clock_frequency_offset_parts_per_billion} "
            f"clock_port_state_code=0x{clock_port_state_code:04X} "
            f"clock_role={clock_role} "
            f"clock_identity={clock_identity} "
            f"leader_clock_identity={leader_clock_identity} "
            f"clock_port_record_count={0 if clock_port_records is None else len(clock_port_records)}"
        )

        device = self._lookup_device(source_ip)

        parsed = {
            "preferred_leader": preferred_leader,
            "clock_source_code": clock_source_code,
            "clock_subdomain": clock_subdomain,
            "clock_frequency_offset_parts_per_billion": clock_frequency_offset_parts_per_billion,
            "clock_port_state_code": clock_port_state_code,
            "clock_role": clock_role,
            "clock_port_records": clock_port_records,
            "clock_identity": clock_identity,
            "leader_clock_identity": leader_clock_identity,
        }

        if device is None:
            self._cache_pending(source_ip, parsed)
        else:
            parsed["clock_subdomain"] = bytes(clock_subdomain) if clock_subdomain is not None else None
            state_changed = any(getattr(device, field_name) != value for field_name, value in parsed.items())
            for field_name, value in parsed.items():
                setattr(device, field_name, value)
            if state_changed:
                self._dispatcher.emit_nowait(
                    DanteEvent(
                        type=EventType.DEVICE_UPDATED,
                        device_name=device.name,
                        server_name=device.server_name,
                    )
                )

        self._notify_preferred_leader_waiter(source_ip, preferred_leader)

    def _handle_interface_status(self, data: bytes, source_ip: str) -> None:
        from netaudio import core

        try:
            parsed_response = core.parse_response("interface_status", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid interface status from {source_ip}: {exception}")
            return

        interfaces = parsed_response["interfaces"]
        link_speed_mbps = parsed_response["link_speed_mbps"]
        reboot_required = parsed_response["reboot_required"]
        pending_config = parsed_response["pending_config"]

        logger.debug(
            f"Conmon interface_status from {source_ip} ({len(data)}B): "
            f"interface_count={len(interfaces)} link_speed_mbps={link_speed_mbps} "
            f"reboot_required={reboot_required} "
            f"pending_config={pending_config} interfaces={interfaces}"
        )

        device = self._lookup_device(source_ip)

        if device is None:
            self._cache_pending(
                source_ip,
                {
                    "interfaces": interfaces,
                    "link_speed_mbps": link_speed_mbps,
                    "interface_reboot_required": reboot_required,
                    "interface_pending_config": pending_config,
                },
            )
        else:
            device_state_changed = (
                device.interfaces != interfaces
                or device.link_speed_mbps != link_speed_mbps
                or device.interface_reboot_required != reboot_required
                or device.interface_pending_config != pending_config
            )
            device.interfaces = interfaces
            device.link_speed_mbps = link_speed_mbps
            device.interface_reboot_required = reboot_required
            device.interface_pending_config = pending_config
            if device_state_changed:
                self._dispatcher.emit_nowait(
                    DanteEvent(
                        type=EventType.DEVICE_UPDATED,
                        device_name=device.name,
                        server_name=device.server_name,
                    )
                )

        self._notify_interface_waiter(source_ip, interfaces)

    def _handle_lock_reset_status(self, data: bytes, source_ip: str) -> None:
        from netaudio import core

        try:
            parsed_response = core.parse_response("lock_reset_status", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid lock-reset status from {source_ip}: {exception}")
            return

        logger.debug(
            f"Conmon lock_reset_status from {source_ip} ({len(data)}B): "
            f"lock_state_code=0x{parsed_response['lock_state_code']:04X} "
            f"status_code=0x{parsed_response['status_code']:04X} "
            f"lock_identifier_count={parsed_response['lock_identifier_count']}"
        )

        self._notify_lock_status_waiter(
            source_ip,
            LockStatusObservation.from_lock_reset_status(parsed_response),
        )

        device = self._lookup_device(source_ip)
        if device is None:
            self._cache_pending(
                source_ip,
                {
                    "lock_reset_status": parsed_response,
                    "is_locked": parsed_response["is_locked"],
                },
            )
            return

        device_state_changed = (
            device.lock_reset_status != parsed_response or device.is_locked != parsed_response["is_locked"]
        )
        device.lock_reset_status = parsed_response
        device.is_locked = parsed_response["is_locked"]
        if device_state_changed:
            self._dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_UPDATED,
                    device_name=device.name,
                    server_name=device.server_name,
                )
            )

    def _handle_clear_configuration_status(self, data: bytes, source_ip: str) -> None:
        from netaudio import core

        try:
            parsed_response = core.parse_response("clear_configuration_status", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid clear-configuration status from {source_ip}: {exception}")
            return

        logger.debug(
            f"Conmon clear_configuration_status from {source_ip} ({len(data)}B): "
            f"available_actions_mask=0x{parsed_response['available_actions_mask']:08X} "
            f"action_result_code=0x{parsed_response['action_result_code']:08X}"
        )
        self._notify_clear_configuration_status_waiter(source_ip, parsed_response)

        device = self._lookup_device(source_ip)
        if device is None:
            self._cache_pending(source_ip, {"clear_configuration_status": parsed_response})
            return

        if device.clear_configuration_status != parsed_response:
            device.clear_configuration_status = parsed_response
            self._dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_UPDATED,
                    device_name=device.name,
                    server_name=device.server_name,
                )
            )

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
                "gain_device_type",
                "gain_levels",
                "supported_gain_levels",
                "routing_capacity_receive_channel_count",
                "routing_capacity_transmit_channel_count",
                "routing_ready",
                "routing_ready_state_code",
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

    def _lookup_device(self, ip_str: str):
        if self._device_lookup:
            return self._device_lookup(ip_str)
        return None
