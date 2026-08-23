from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Span:
    offset: int
    length: int
    name: str
    raw: bytes
    value: str
    detail: str = ""
    fact_ref: str = ""
    section: str = ""
    dtype: str = ""


@dataclass
class DissectedPacket:
    payload: bytes
    spans: list[Span] = field(default_factory=list)
    sections: list[tuple[str, str]] = field(default_factory=list)
    header_summary: str = ""
    fact_refs: list[str] = field(default_factory=list)


PROTOCOL_ID_NAMES = {
    0x1200: "CMC",
    0x2729: "ARC",
    0x27FF: "ARC",
    0x2801: "ARC",
    0x2809: "ARC",
    0xFFFF: "Conmon/Settings",
}


ARC_PROTOCOL_IDENTIFIERS = {0x2729, 0x27FF, 0x2801, 0x2809}


DEVICE_SETTINGS_PROPERTY_NAMES = {
    0x8020: "sample_rate",
    0x8204: "default_latency",
    0x8205: "configured_latency",
    0x8301: "active_latency",
    0x8302: "max_latency",
    0x8306: "min_latency",
    0x0063: "aes67_configured",
    0x8060: "aes67_multicast_prefix",
}


ARC_STATUS_NAMES = {
    0x0000: "request",
    0x0001: "success",
    0x0022: "error",
    0x8112: "success (paginated)",
}


ARC_SUCCESS_STATUSES = {0x0001, 0x8112}


NANOSECOND_FIELD_NAMES = {
    "default_latency",
    "configured_latency",
    "active_latency",
    "current_latency",
    "dup_latency",
    "max_latency",
    "min_latency",
    "target_latency",
    "latency",
}


DECIMAL_FIELD_NAMES = {
    "packet_length",
    "rx_channel",
    "max_per_page",
    "named_count",
    "property_count",
    "supported_encoding_count",
    "channel_number",
    "tx_count",
    "rx_count",
    "interface_count",
    "link_speed_mbps",
}


CONMON_MESSAGE_NAMES = {
    0x0011: "interface_status_announcement",
    0x0013: "interface_configuration",
    0x0020: "ptp_clock_status",
    0x0021: "ptp_clock_config",
    0x0022: "unmapped_0022_status",
    0x0023: "unmapped_0023_control",
    0x0024: "unmapped_0024_status",
    0x0025: "unmapped_0025_control",
    0x0026: "unmapped_0026_status",
    0x0027: "unmapped_0027_control",
    0x0040: "unmapped_0040_status",
    0x0041: "unmapped_0041_control",
    0x0060: "board_model_publication",
    0x0061: "board_model_query",
    0x0077: "clear_configuration",
    0x0078: "clear_configuration_status",
    0x0080: "sample_rate_status",
    0x0081: "sample_rate_control",
    0x0082: "encoding_status",
    0x0083: "encoding_control",
    0x0084: "sample_rate_pullup_status",
    0x0085: "sample_rate_pullup_control",
    0x0086: "unmapped_0086_status",
    0x00E0: "unmapped_00e0_status",
    0x00E1: "unmapped_00e1_control",
    0x0102: "unmapped_0102_status",
    0x0103: "unmapped_0103_control",
    0x0106: "unmapped_0106_status",
    0x0107: "unmapped_0107_control",
    0x0090: "system_reset_control",
    0x0092: "system_reset_status",
    0x00C0: "make_model_publication",
    0x00C1: "make_model_query",
    0x0100: "routing_capacity_status",
    0x01FE: "metering_data",
    0x0063: "identify",
    0x100A: "gain_control",
    0x100B: "gain_status",
    0x1006: "set_aes67",
    0x1008: "lock_reset_query",
    0x1009: "lock_reset_status",
    0x40FE: "metering_data_extended",
    0xFF04: "conmon_export_request",
    0xFF05: "conmon_export_fragment",
}
