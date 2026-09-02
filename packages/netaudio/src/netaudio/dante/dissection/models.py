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
    core_kind: str | None = None
    core_fields: dict | None = None


NANOSECOND_FIELD_NAMES = {
    "active_latency",
    "configured_latency",
    "current_latency",
    "default_latency",
    "dup_latency",
    "latency",
    "max_latency",
    "min_latency",
    "target_latency",
}


DECIMAL_FIELD_NAMES = {
    "channel_number",
    "interface_count",
    "link_speed_mbps",
    "max_per_page",
    "named_count",
    "packet_length",
    "property_count",
    "rx_channel",
    "rx_count",
    "supported_encoding_count",
    "tx_count",
}
