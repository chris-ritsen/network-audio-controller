from __future__ import annotations

import logging
import struct
from dataclasses import dataclass

from netaudio.dante.clock_identity import canonical_clock_identity
from netaudio.dante.const import (
    CONMON_HEADER_LENGTH,
    CONMON_MAGIC,
    CONMON_OPCODE_AES67_CURRENT_NEW,
    CONMON_OPCODE_BLUETOOTH_STATUS,
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
    DEVICE_SETTINGS_PORT,
    NOTIFICATION_NAMES,
    PROTOCOL_SETTINGS,
)
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.gain import SUPPORTED_GAIN_LEVELS
from netaudio.dante.link_status import LinkStatusObservation
from netaudio.dante.lock_status import LockStatusObservation
from netaudio.dante.packet_store import PacketRecord

logger = logging.getLogger("netaudio")

BLUETOOTH_SETTINGS_SUBTYPE = 0x000C
STATUS_KIND_AES67 = "aes67"
STATUS_KIND_BLUETOOTH = "bluetooth_status"
STATUS_KIND_CLEAR_CONFIGURATION = "clear_configuration_status"
STATUS_KIND_CLOCK = "clock_status"
STATUS_KIND_DANTE_MODEL = "dante_model"
STATUS_KIND_ENCODING = "encoding"
STATUS_KIND_GAIN = "gain"
STATUS_KIND_INTERFACE = "interface"
STATUS_KIND_LINK = "link_status"
STATUS_KIND_LOCK = "lock_status"
STATUS_KIND_MAKE_MODEL = "make_model"
STATUS_KIND_ROUTING_CAPACITY = "routing_capacity"
STATUS_KIND_SAMPLE_RATE = "sample_rate"
STATUS_KIND_SAMPLE_RATE_PULLUP = "sample_rate_pullup"
STATUS_KIND_SWITCH_CONFIGURATION = "switch_configuration"
WAITER_KIND_PREFERRED_LEADER = "preferred_leader"


@dataclass(frozen=True)
class ParsedStatus:
    kind: str
    status: object
    waiter_result: object


def extract_conmon_opcode(data: bytes) -> int | None:
    if len(data) < CONMON_HEADER_LENGTH:
        return None
    magic_position = data.find(CONMON_MAGIC, 4)
    if magic_position < 0:
        return None
    opcode_position = magic_position + 10
    if opcode_position + 2 > len(data):
        return None
    return struct.unpack(">H", data[opcode_position : opcode_position + 2])[0]


def _is_bluetooth_status(data: bytes) -> bool:
    return len(data) >= 36 and struct.unpack(">H", data[34:36])[0] == BLUETOOTH_SETTINGS_SUBTYPE


def _core_parse(kind: str, data: bytes, source_ip: str, description: str):
    from netaudio import core

    try:
        return core.parse_response(kind, data)
    except core.NetaudioCoreError as exception:
        logger.warning(f"Invalid {description} from {source_ip}: {exception}")
        return None


def _parse_aes67_current_new(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("aes67_status", data, source_ip, "AES67 status")
    if parsed is None:
        return None
    aes67_current = parsed["aes67_current"]
    aes67_configured = parsed["aes67_configured"]
    logger.debug(
        f"Conmon aes67_current_new from {source_ip} ({len(data)}B): "
        f"current={aes67_current} configured={aes67_configured}"
    )
    status = {"aes67_configured": aes67_configured, "aes67_current": aes67_current}
    return ParsedStatus(STATUS_KIND_AES67, status, (aes67_current, aes67_configured))


def _parse_bluetooth_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("bluetooth_status", data, source_ip, "bluetooth status")
    if parsed is None:
        return None
    return ParsedStatus(STATUS_KIND_BLUETOOTH, parsed, parsed)


def _parse_clear_configuration_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("clear_configuration_status", data, source_ip, "clear-configuration status")
    if parsed is None:
        return None
    logger.debug(
        f"Conmon clear_configuration_status from {source_ip} ({len(data)}B): "
        f"available_actions_mask=0x{parsed['available_actions_mask']:08X} "
        f"action_result_code=0x{parsed['action_result_code']:08X}"
    )
    return ParsedStatus(STATUS_KIND_CLEAR_CONFIGURATION, parsed, parsed)


def _parse_dante_model(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("dante_model", data, source_ip, "dante_model response")
    if parsed is None:
        return None
    logger.debug(
        f"Conmon dante_model from {source_ip} ({len(data)}B): "
        f"codename={parsed['board_codename']!r} board_name={parsed['board_name']!r}"
    )
    status = {}
    if parsed["board_codename"]:
        status["dante_model_id"] = parsed["board_codename"]
    if parsed["board_name"]:
        status["board_name"] = parsed["board_name"]
    return ParsedStatus(STATUS_KIND_DANTE_MODEL, status, parsed)


def _parse_gain_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("gain_status", data, source_ip, "gain status")
    if parsed is None:
        return None
    status = {
        "gain_device_type": parsed["device_type"],
        "gain_levels": parsed["channel_levels"],
        "supported_gain_levels": list(SUPPORTED_GAIN_LEVELS),
    }
    return ParsedStatus(STATUS_KIND_GAIN, status, (parsed["device_type"], parsed["channel_levels"]))


def _parse_interface_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("interface_status", data, source_ip, "interface status")
    if parsed is None:
        return None
    logger.debug(
        f"Conmon interface_status from {source_ip} ({len(data)}B): "
        f"interface_count={len(parsed['interfaces'])} link_speed_mbps={parsed['link_speed_mbps']} "
        f"reboot_required={parsed['reboot_required']} "
        f"pending_config={parsed['pending_config']} interfaces={parsed['interfaces']}"
    )
    status = {
        "interface_pending_config": parsed["pending_config"],
        "interface_reboot_required": parsed["reboot_required"],
        "interfaces": parsed["interfaces"],
        "link_speed_mbps": parsed["link_speed_mbps"],
    }
    return ParsedStatus(STATUS_KIND_INTERFACE, status, parsed["interfaces"])


def _parse_link_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    from netaudio import core

    try:
        parsed = core.parse_response("unmapped_0040_status", data)
        observation = LinkStatusObservation.from_core(parsed, device=device)
    except (core.NetaudioCoreError, KeyError, TypeError, ValueError) as exception:
        logger.warning(f"Invalid link status from {source_ip}: {exception}")
        return None
    return ParsedStatus(STATUS_KIND_LINK, observation, observation)


def _parse_lock_reset_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("lock_reset_status", data, source_ip, "lock-reset status")
    if parsed is None:
        return None
    logger.debug(
        f"Conmon lock_reset_status from {source_ip} ({len(data)}B): "
        f"lock_state_code=0x{parsed['lock_state_code']:04X} "
        f"status_code=0x{parsed['status_code']:04X} "
        f"lock_identifier_count={parsed['lock_identifier_count']}"
    )
    return ParsedStatus(STATUS_KIND_LOCK, parsed, LockStatusObservation.from_lock_reset_status(parsed))


def _parse_make_model(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("make_model", data, source_ip, "make_model response")
    if parsed is None:
        return None
    logger.debug(
        f"Conmon make_model from {source_ip} ({len(data)}B): "
        f"name={parsed['product_name']!r} version={parsed['product_version']!r} "
        f"manufacturer={parsed['manufacturer']!r}"
    )
    status = {}
    if parsed["product_name"]:
        status["dante_model"] = parsed["product_name"]
    if parsed["product_version"]:
        status["product_version"] = parsed["product_version"]
    if parsed["manufacturer"]:
        status["manufacturer"] = parsed["manufacturer"]
    return ParsedStatus(STATUS_KIND_MAKE_MODEL, status, parsed)


def _parse_ptp_clock_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("ptp_clock_status", data, source_ip, "PTP clock status")
    if parsed is None:
        return None
    clock_subdomain = parsed.get("clock_subdomain")
    status = {
        "clock_frequency_offset_parts_per_billion": parsed["clock_frequency_offset_parts_per_billion"],
        "clock_identity": canonical_clock_identity(parsed.get("clock_identity")),
        "clock_port_records": parsed.get("clock_port_records"),
        "clock_port_state_code": parsed["clock_port_state_code"],
        "clock_role": parsed.get("clock_role"),
        "clock_source_code": parsed["clock_source_code"],
        "clock_subdomain": bytes(clock_subdomain) if clock_subdomain is not None else None,
        "leader_clock_identity": canonical_clock_identity(parsed.get("leader_clock_identity")),
        "preferred_leader": parsed["preferred_leader"],
    }
    logger.debug(
        f"Conmon ptp_clock_status from {source_ip} ({len(data)}B): "
        f"preferred_leader={status['preferred_leader']} "
        f"clock_source_code=0x{status['clock_source_code']:04X} "
        f"clock_frequency_offset_parts_per_billion={status['clock_frequency_offset_parts_per_billion']} "
        f"clock_port_state_code=0x{status['clock_port_state_code']:04X} "
        f"clock_role={status['clock_role']} "
        f"clock_identity={status['clock_identity']} "
        f"leader_clock_identity={status['leader_clock_identity']} "
        f"clock_port_record_count={0 if status['clock_port_records'] is None else len(status['clock_port_records'])}"
    )
    return ParsedStatus(STATUS_KIND_CLOCK, status, status)


def _parse_routing_capacity_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("routing_capacity_status", data, source_ip, "routing-capacity status")
    if parsed is None:
        return None
    status = {
        "routing_capacity_receive_channel_count": parsed["receive_channel_count"],
        "routing_capacity_transmit_channel_count": parsed["transmit_channel_count"],
        "routing_ready": parsed["routing_ready"],
        "routing_ready_state_code": parsed["state_code"],
    }
    return ParsedStatus(STATUS_KIND_ROUTING_CAPACITY, status, parsed)


def _parse_capability_status(
    kind: str,
    response_kind: str,
    current_field: str,
    supported_field: str,
    description: str,
):
    def parse(data: bytes, source_ip: str, device) -> ParsedStatus | None:
        parsed = _core_parse(response_kind, data, source_ip, f"{description} status")
        if parsed is None:
            return None
        current_value = parsed[current_field]
        supported_values = parsed[supported_field]
        logger.debug(
            f"Conmon {response_kind} from {source_ip} ({len(data)}B): "
            f"current={current_value} supported={supported_values}"
        )
        status = {kind: current_value, f"supported_{kind}s": supported_values}
        return ParsedStatus(kind, status, (current_value, supported_values))

    return parse


def _parse_sample_rate_pullup_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("sample_rate_pullup_status", data, source_ip, "sample rate pull-up status")
    if parsed is None:
        return None
    current_raw_value = parsed["applied_value"]["raw_value"]
    supported_raw_values = [value["raw_value"] for value in parsed["supported_values"]]
    status = {
        "requested_sample_rate_pullup_raw_value": parsed["requested_value"]["raw_value"],
        "sample_rate_pullup_raw_value": current_raw_value,
        "supported_sample_rate_pullup_raw_values": supported_raw_values,
    }
    return ParsedStatus(STATUS_KIND_SAMPLE_RATE_PULLUP, status, (current_raw_value, supported_raw_values))


def _parse_switch_configuration_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("switch_configuration_status", data, source_ip, "switch configuration status")
    if parsed is None:
        return None
    return ParsedStatus(STATUS_KIND_SWITCH_CONFIGURATION, parsed, parsed)


CONMON_STATUS_PARSERS = {
    CONMON_OPCODE_AES67_CURRENT_NEW: _parse_aes67_current_new,
    CONMON_OPCODE_BLUETOOTH_STATUS: _parse_bluetooth_status,
    CONMON_OPCODE_CLEAR_CONFIGURATION_STATUS: _parse_clear_configuration_status,
    CONMON_OPCODE_DANTE_MODEL_RESPONSE: _parse_dante_model,
    CONMON_OPCODE_ENCODING_STATUS: _parse_capability_status(
        STATUS_KIND_ENCODING,
        "encoding_status",
        "current_encoding",
        "supported_encodings",
        "encoding",
    ),
    CONMON_OPCODE_GAIN_STATUS: _parse_gain_status,
    CONMON_OPCODE_INTERFACE_STATUS: _parse_interface_status,
    CONMON_OPCODE_LINK_STATUS: _parse_link_status,
    CONMON_OPCODE_LOCK_RESET_STATUS: _parse_lock_reset_status,
    CONMON_OPCODE_MAKE_MODEL_RESPONSE: _parse_make_model,
    CONMON_OPCODE_PTP_CLOCK_STATUS: _parse_ptp_clock_status,
    CONMON_OPCODE_ROUTING_CAPACITY_STATUS: _parse_routing_capacity_status,
    CONMON_OPCODE_SAMPLE_RATE_PULLUP_STATUS: _parse_sample_rate_pullup_status,
    CONMON_OPCODE_SAMPLE_RATE_STATUS: _parse_capability_status(
        STATUS_KIND_SAMPLE_RATE,
        "sample_rate_status",
        "current_sample_rate",
        "supported_sample_rates",
        "sample rate",
    ),
    CONMON_OPCODE_SWITCH_CONFIGURATION_STATUS: _parse_switch_configuration_status,
}


class NotificationPacketHandlers:
    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        source_ip = addr[0]
        if not data and addr[1] == DEVICE_SETTINGS_PORT:
            for waiter in self.waiters_for("conmon_export", source_ip):
                waiter.observe_unavailable()
            return
        if len(data) < 4:
            return

        if self._dissect:
            self._log_dissected(data, source_ip, addr[1])

        if self._packet_store:
            self._store_notification(data, source_ip, addr[1])

        protocol_id = struct.unpack(">H", data[0:2])[0]

        if protocol_id == PROTOCOL_SETTINGS:
            if self._handle_conmon_response(data, source_ip):
                return
            self._handle_settings_notification(data, source_ip)
            return

        if len(data) < 28:
            logger.debug(
                f"Short multicast packet from {source_ip}, "
                f"{len(data)} bytes, protocol=0x{protocol_id:04X}, hex={data.hex()}"
            )
            return

        notification_id = struct.unpack(">H", data[26:28])[0]
        self._emit_notification(data, source_ip, notification_id)

    def _log_dissected(self, data: bytes, source_ip: str, source_port: int) -> None:
        from netaudio.common.app_config import settings as app_settings
        from netaudio.dante.packet_dissection_rendering import dissect_and_render, format_dissect_label

        color = not app_settings.no_color
        label = format_dissect_label("multicast", f"{source_ip}:{source_port}", color=color)
        rendered = dissect_and_render(data, indent="  ", color=color)
        logger.debug(f"Dissect [{label}] {len(data)}B:\n{rendered}")

    def _store_notification(self, data: bytes, source_ip: str, source_port: int) -> None:
        device = self._lookup_device(source_ip)
        self._packet_store.store_packet(
            PacketRecord(
                payload=data,
                source_type="multicast",
                src_ip=source_ip,
                src_port=source_port,
                device_name=device.name if device else None,
                device_ip=source_ip,
                multicast_group=self._multicast_group,
                multicast_port=self._multicast_port,
                session_id=self._session_id,
            )
        )

    def _emit_notification(self, data: bytes, source_ip: str, notification_id: int) -> None:
        device = self._lookup_device(source_ip)
        notification_name = NOTIFICATION_NAMES.get(notification_id, f"Unknown(0x{notification_id:04X})")
        self.notify_waiters("notification", source_ip, notification_id)
        logger.debug(
            f"Notification from {source_ip} ({device.name if device else ''}): "
            f"{notification_name} (id={notification_id})"
        )
        self._dispatcher.emit_nowait(
            DanteEvent(
                type=EventType.NOTIFICATION_RECEIVED,
                device_name=device.name if device else "",
                server_name=device.server_name if device else "",
                data={
                    "notification_id": notification_id,
                    "notification_name": notification_name,
                    "raw": data,
                    "source_ip": source_ip,
                },
            )
        )

    def _handle_settings_notification(self, data: bytes, source_ip: str) -> None:
        notification_id = struct.unpack(">H", data[26:28])[0] if len(data) >= 28 else None
        logger.debug(
            f"Settings notification from {source_ip}, "
            f"{len(data)} bytes, notification_id={notification_id}, hex={data.hex()}"
        )
        if notification_id is not None:
            self._emit_notification(data, source_ip, notification_id)

    def _handle_conmon_response(self, data: bytes, source_ip: str) -> bool:
        opcode = extract_conmon_opcode(data)

        if opcode is None:
            return False

        if opcode == CONMON_OPCODE_EXPORT_FRAGMENT:
            return self._handle_conmon_export_fragment(data, source_ip)

        parse = CONMON_STATUS_PARSERS.get(opcode)
        if parse is None:
            return False
        if opcode == CONMON_OPCODE_BLUETOOTH_STATUS and not _is_bluetooth_status(data):
            return False

        parsed = parse(data, source_ip, self._lookup_device(source_ip))
        self.notify_waiters("notification", source_ip, opcode)
        if parsed is None:
            return True

        self.notify_waiters(parsed.kind, source_ip, parsed.waiter_result)
        if parsed.kind == STATUS_KIND_CLOCK:
            self.notify_waiters(WAITER_KIND_PREFERRED_LEADER, source_ip, parsed.status["preferred_leader"])
        self.notify_conmon_response(source_ip, opcode)

        device = self._lookup_device(source_ip)
        self._dispatcher.emit_nowait(
            DanteEvent(
                type=EventType.DEVICE_STATUS_RECEIVED,
                device_name=device.name if device else "",
                server_name=device.server_name if device else "",
                data={
                    "kind": parsed.kind,
                    "notification_id": opcode,
                    "raw": data,
                    "source_ip": source_ip,
                    "status": parsed.status,
                },
            )
        )
        return True

    def _handle_conmon_export_fragment(self, data: bytes, source_ip: str) -> bool:
        waiters = self.waiters_for("conmon_export", source_ip)
        if not waiters:
            return False
        from netaudio import core

        try:
            fragment = core.parse_response("conmon_export_fragment", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid ConMon export fragment from {source_ip}: {exception}")
            return True
        handled = False
        for waiter in waiters:
            if waiter.collector.matches(fragment):
                waiter.observe(fragment)
                handled = True
        return handled

    def _lookup_device(self, ip_str: str):
        if self._device_lookup:
            return self._device_lookup(ip_str)
        return None
