from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from netaudio import core
from netaudio.dante.audio_capabilities import audio_capability_fields
from netaudio.dante.ptpv1_uuid import canonical_ptpv1_uuid
from netaudio.dante.const import (
    NOTIFICATION_VERSIONS_STATUS,
    DEVICE_SETTINGS_PORT,
)
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.gain import codec_status_fields
from netaudio.dante.interface_statistics import InterfaceStatisticsObservation
from netaudio.dante.lock_status import LockStatusObservation
from netaudio.dante.network_configuration import interface_redundancy_status, switch_configuration_fields
from netaudio.dante.packet_store import PacketRecord

logger = logging.getLogger("netaudio")

STATUS_KIND_AES67 = "aes67"
STATUS_KIND_CLEAR_CONFIGURATION = "clear_configuration_status"
STATUS_KIND_CLOCK = "clock_status"
STATUS_KIND_DANTE_MODEL = "dante_model"
STATUS_KIND_ENCODING = "encoding"
STATUS_KIND_CODEC = "codec"
STATUS_KIND_INTERFACE = "interface"
STATUS_KIND_INTERFACE_STATISTICS = "interface_statistics"
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


def _core_parse(kind: str, data: bytes, source_ip: str, description: str):
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


def _parse_panel_status(data: bytes, source_ip: str, device) -> ParsedStatus:
    from netaudio.dante.panel_state import observation_time, panel_family

    family = panel_family(device)
    kind = {"bluetooth": "panel_bluetooth_status", "dante_av": "panel_video_status"}.get(family, "panel_status")
    parsed = _core_parse(kind, data, source_ip, "panel status")
    if parsed is None:
        parsed = {"diagnostic_error": "Malformed panel record", "raw_record": list(data), "observations": []}
    parsed["observed_at_unix"] = observation_time()
    return ParsedStatus("panel_status", parsed, parsed)


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
        f"identifier={parsed['platform_model_identifier']!r} "
        f"model_name={parsed['platform_model_name']!r} "
        f"software={parsed['platform_software_version']!r}"
    )
    status = {"platform_versions_record": parsed}
    for field_name in (
        "platform_model_identifier",
        "platform_model_name",
        "platform_software_version",
        "platform_hardware_version",
        "platform_api_version",
        "rom_boot_version",
    ):
        if parsed.get(field_name) is not None:
            status[field_name] = parsed[field_name]
    status["dante_model_record_protocol_version"] = parsed["record_protocol_version"]
    status["dante_model_primary_capabilities"] = parsed["primary_capabilities"]
    status["dante_model_read_only_capabilities"] = parsed["read_only_capabilities"]
    status["dante_model_monitoring_capabilities"] = parsed["monitoring_capabilities"]
    status["dante_model_secondary_capabilities"] = parsed["secondary_capabilities"]
    status["dante_model_domain_capability_values"] = parsed["domain_capability_values"]
    status["dante_model_domain_capability_validity"] = parsed["domain_capability_validity"]
    observed_at = time.time()
    source = {
        "kind": "conmon_dante_model",
        "opcode": NOTIFICATION_VERSIONS_STATUS,
        "record_protocol_version": parsed["record_protocol_version"],
        "observed_at_unix": observed_at,
        "fresh": True,
    }
    status["redundancy_advertised_support_source"] = {
        **source,
        "field_reported": parsed["switch_redundancy_supported"] is not None,
    }
    status["redundancy_read_only_source"] = {
        **source,
        "field_applicable": parsed["switch_redundancy_read_only"] is not None,
        "field_reported": parsed["switch_redundancy_read_only"] is not None,
        **(
            {}
            if parsed["switch_redundancy_read_only"] is not None
            else {"unavailable_reason": "record_protocol_version"}
        ),
    }
    for field_name in (
        "identify_supported",
        "sample_rate_configuration_supported",
        "encoding_configuration_supported",
        "sample_rate_pullup_configuration_supported",
        "switch_redundancy_supported",
        "static_ipv4_configuration_supported",
        "detailed_metering_supported",
        "aes67_configuration_supported",
        "device_locking_supported",
        "external_word_clock_read_only",
        "switch_redundancy_read_only",
        "static_ipv4_configuration_read_only",
        "virtual_panel_supported",
        "video_transmission_supported",
        "video_reception_supported",
        "generic_codec_control_supported",
        "interface_statistics_supported",
        "clock_monitoring_supported",
        "per_channel_signal_presence_supported",
        "rx_flow_maximum_latency_monitoring_supported",
        "rx_flow_late_packet_monitoring_supported",
    ):
        status[field_name] = parsed[field_name]
    return ParsedStatus(STATUS_KIND_DANTE_MODEL, status, parsed)


def _parse_codec_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("codec_status", data, source_ip, "codec status")
    if parsed is None:
        return None
    return ParsedStatus(STATUS_KIND_CODEC, codec_status_fields(parsed), parsed)


def _parse_interface_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("interface_status", data, source_ip, "interface status")
    if parsed is None:
        return None
    logger.debug(
        f"Conmon interface_status from {source_ip} ({len(data)}B): "
        f"interface_count={len(parsed['interfaces'])} link_speed_mbps={parsed['link_speed_mbps']} "
        f"reboot_required={parsed['reboot_required']} "
        f"interfaces={parsed['interfaces']}"
    )
    status = {
        "interface_status_protocol": parsed["record_protocol_identifier"],
        "interface_reboot_required": parsed["reboot_required"],
        "interfaces": parsed["interfaces"],
        "link_speed_mbps": parsed["link_speed_mbps"],
    }
    redundancy = interface_redundancy_status(parsed, device)
    if redundancy is not None:
        status["dante_redundancy"] = redundancy
    elif isinstance(getattr(device, "dante_redundancy", None), dict):
        status["dante_redundancy"] = {**device.dante_redundancy, "state_fresh": False}
    return ParsedStatus(STATUS_KIND_INTERFACE, status, status)


def _parse_interface_statistics(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    try:
        parsed = core.parse_response("interface_statistics_status", data)
        observation = InterfaceStatisticsObservation.from_core(parsed, source_ip)
    except (core.NetaudioCoreError, KeyError, TypeError, ValueError) as exception:
        logger.warning(f"Invalid interface statistics from {source_ip}: {exception}")
        return None
    return ParsedStatus(STATUS_KIND_INTERFACE_STATISTICS, observation, observation)


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
    status = {"manufacturer_versions_record": parsed}
    if parsed["product_name"]:
        status["product_name"] = parsed["product_name"]
    for field_name in (
        "product_version",
        "friendly_product_version",
        "manufacturer_software_version",
        "manufacturer_firmware_version",
    ):
        if parsed.get(field_name) is not None:
            status[field_name] = parsed[field_name]
    if parsed["manufacturer"]:
        status["manufacturer"] = parsed["manufacturer"]
    return ParsedStatus(STATUS_KIND_MAKE_MODEL, status, parsed)


def _parse_ptp_clock_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("ptp_clock_status", data, source_ip, "PTP clock status")
    from datetime import datetime, timezone

    if parsed is None:
        return ParsedStatus(
            STATUS_KIND_CLOCK,
            {
                "clock_status": {"status_supported": False, "error": "malformed", "raw_record": list(data[24:])},
                "clock_observed_at": None,
            },
            None,
        )
    for field in ("ptpv1_device_uuid", "ptpv1_master_uuid", "ptpv1_grandmaster_uuid"):
        parsed[field] = canonical_ptpv1_uuid(parsed.get(field))
    fields = (
        "clock_frequency_offset_parts_per_billion",
        "clock_port_records",
        "clock_port_state_code",
        "clock_role",
        "clock_source_code",
        "preferred_leader",
        "ptpv1_device_uuid",
        "ptpv1_master_uuid",
        "ptpv1_grandmaster_uuid",
    )
    status = {field: parsed.get(field) for field in fields}
    name = parsed.get("clock_subdomain")
    status["clock_subdomain"] = bytes(name) if name is not None else None
    status["clock_status"] = parsed
    status["clock_observed_at"] = datetime.now(timezone.utc).isoformat()
    status["_clock_received_monotonic"] = time.monotonic()
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
    description: str,
):
    def parse(data: bytes, source_ip: str, device) -> ParsedStatus | None:
        parsed = _core_parse(response_kind, data, source_ip, f"{description} status")
        if parsed is None:
            return None
        current_value = parsed["current_value"]
        supported_values = parsed["available_values"]
        logger.debug(
            f"Conmon {response_kind} from {source_ip} ({len(data)}B): "
            f"current={current_value} supported={supported_values}"
        )
        status = audio_capability_fields(parsed, kind=kind)
        return ParsedStatus(kind, status, parsed)

    return parse


def _parse_sample_rate_pullup_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("sample_rate_pullup_status", data, source_ip, "sample rate pull-up status")
    if parsed is None:
        return None
    status = audio_capability_fields(parsed, kind="sample_rate_pullup")
    return ParsedStatus(STATUS_KIND_SAMPLE_RATE_PULLUP, status, parsed)


def _parse_switch_configuration_status(data: bytes, source_ip: str, device) -> ParsedStatus | None:
    parsed = _core_parse("switch_configuration_status", data, source_ip, "switch configuration status")
    if parsed is None:
        return None
    return ParsedStatus(STATUS_KIND_SWITCH_CONFIGURATION, switch_configuration_fields(parsed), parsed)


def _clock_diagnostic_parser(response_kind: str):
    def parse(data: bytes, source_ip: str, device) -> ParsedStatus | None:
        parsed = _core_parse(response_kind, data, source_ip, "clock diagnostic")
        if parsed is None:
            return None
        diagnostics = dict(getattr(device, "clock_diagnostics", None) or {})
        diagnostics[response_kind] = parsed
        return ParsedStatus("clock_diagnostics", {"clock_diagnostics": diagnostics}, parsed)

    return parse


CONMON_STATUS_PARSERS = {
    "clock_master_status": _clock_diagnostic_parser("clock_master_status"),
    "clock_unicast_status": _clock_diagnostic_parser("clock_unicast_status"),
    "clock_identifier_status": _clock_diagnostic_parser("clock_identifier_status"),
    "aes67_status": _parse_aes67_current_new,
    "panel_status": _parse_panel_status,
    "clear_configuration_status": _parse_clear_configuration_status,
    "dante_model": _parse_dante_model,
    "encoding_status": _parse_capability_status(
        STATUS_KIND_ENCODING,
        "encoding_status",
        "encoding",
    ),
    "codec_status": _parse_codec_status,
    "interface_status": _parse_interface_status,
    "interface_statistics_status": _parse_interface_statistics,
    "lock_reset_status": _parse_lock_reset_status,
    "make_model": _parse_make_model,
    "ptp_clock_status": _parse_ptp_clock_status,
    "routing_capacity_status": _parse_routing_capacity_status,
    "sample_rate_pullup_status": _parse_sample_rate_pullup_status,
    "sample_rate_status": _parse_capability_status(
        STATUS_KIND_SAMPLE_RATE,
        "sample_rate_status",
        "sample rate",
    ),
    "switch_configuration_status": _parse_switch_configuration_status,
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

        try:
            envelope = core.parse_response("notification_envelope", data)
        except core.NetaudioCoreError:
            return

        notification_id = envelope["notification_id"]

        if envelope["is_conmon"] and self._handle_conmon_response(
            data, source_ip, notification_id, envelope["response_kind"]
        ):
            return

        self._emit_notification(data, source_ip, notification_id, envelope["notification_name"])

    def _log_dissected(self, data: bytes, source_ip: str, source_port: int) -> None:
        from netaudio.common.app_config import settings as app_settings
        from netaudio.dante.dissection.rendering import dissect_and_render, format_dissect_label

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

    def _emit_notification(
        self, data: bytes, source_ip: str, notification_id: int, notification_name: str | None
    ) -> None:
        device = self._lookup_device(source_ip)
        notification_name = notification_name or "Unknown notification"
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

    def _handle_conmon_response(self, data: bytes, source_ip: str, opcode: int, response_kind: str | None) -> bool:
        if response_kind == "conmon_export_fragment":
            return self._handle_conmon_export_fragment(data, source_ip)

        parse = CONMON_STATUS_PARSERS.get(response_kind)
        if parse is None:
            return False

        parsed = parse(data, source_ip, self._lookup_device(source_ip))
        self.notify_waiters("notification", source_ip, opcode)
        if parsed is None:
            return True
        if parsed.kind == STATUS_KIND_INTERFACE_STATISTICS:
            observation = self._interface_statistics_error_baselines.apply(parsed.status)
            parsed = ParsedStatus(parsed.kind, observation, observation)

        if parsed.kind == "panel_status":
            parsed.status["correlated"] = any(
                waiter.accept is not None and waiter.accept(parsed.status)
                for waiter in self.waiters_for("panel_status", source_ip)
            )
        self.notify_waiters(parsed.kind, source_ip, parsed.waiter_result)
        if parsed.kind == STATUS_KIND_CLOCK:
            self.notify_waiters(WAITER_KIND_PREFERRED_LEADER, source_ip, parsed.status.get("preferred_leader"))
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
        try:
            fragment = core.parse_response("conmon_export_fragment", data)
        except core.NetaudioCoreError as exception:
            logger.warning(f"Invalid ConMon export fragment from {source_ip}: {exception}")
            return True
        handled = False
        for waiter in waiters:
            waiter.observe(fragment)
            handled = handled or waiter.collector.matched
        return handled

    def _lookup_device(self, ip_str: str):
        if self._device_lookup:
            return self._device_lookup(ip_str)
        return None
