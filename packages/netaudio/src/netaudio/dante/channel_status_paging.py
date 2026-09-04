from __future__ import annotations

from netaudio.dante.const import (
    ARC_PROTOCOL_IDS,
    MODERN_ARC_PROTOCOL_IDS,
    PROTOCOL_ARC_2809,
    SERVICE_ARC,
)


MAX_CHANNEL_STATUS_PAGES = 256


class ChannelStatusPaginationError(RuntimeError):
    pass


def _arc_version_protocol_identifier(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    parts = value.split(".")
    if len(parts) != 3 or not all(part.isdecimal() for part in parts):
        return None
    major, minor, patch = (int(part) for part in parts)
    if not (0 <= major <= 0xF and 0 <= minor <= 0xF and 0 <= patch <= 0xFF):
        return None
    return (major << 12) | (minor << 8) | patch


def modern_arc_protocol_identifier_for_device(device) -> int:
    protocol_id = advertised_arc_protocol_identifier_for_device(device)
    if protocol_id in MODERN_ARC_PROTOCOL_IDS:
        return protocol_id
    if protocol_id is None:
        raise ChannelStatusPaginationError("device has no ARC service metadata")
    raise ChannelStatusPaginationError(f"unsupported ARC protocol version 0x{protocol_id:04X}")


def advertised_arc_protocol_identifier_for_device(device) -> int | None:
    if getattr(device, "requires_managed_control", False):
        return PROTOCOL_ARC_2809
    services = getattr(device, "services", None)
    if not isinstance(services, dict):
        return None
    for service in services.values():
        if not isinstance(service, dict) or service.get("type") != SERVICE_ARC:
            continue
        properties = service.get("properties")
        if not isinstance(properties, dict):
            return None
        advertised_version = properties.get("arcp_vers")
        if advertised_version is None:
            return None
        protocol_id = _arc_version_protocol_identifier(advertised_version)
        if protocol_id in ARC_PROTOCOL_IDS:
            return protocol_id
        raise ChannelStatusPaginationError(f"unsupported ARC protocol version {advertised_version!r}")
    return None


def _record_identity(record: dict) -> tuple[int, int]:
    media_type = record.get("media_type_code")
    media_local_id = record.get("media_local_channel_id")
    if not isinstance(media_type, int) or not isinstance(media_local_id, int):
        raise ChannelStatusPaginationError("channel page record has no media-aware identity")
    if media_type <= 0 or media_local_id <= 0:
        raise ChannelStatusPaginationError("channel page record has an invalid media-aware identity")
    return media_type, media_local_id


def _stable_record(record: dict) -> dict:
    return {
        key: value
        for key, value in record.items()
        if key != "record_pointer" and not key.endswith("_pointer") and key != "raw_record_hexadecimal"
    }


class ChannelStatusPageAccumulator:
    def __init__(self, protocol_id: int, opcode: int, maximum_pages: int = MAX_CHANNEL_STATUS_PAGES):
        self.protocol_id = protocol_id
        self.opcode = opcode
        self.maximum_pages = maximum_pages
        self._records: dict[tuple[int, int], dict] = {}
        self._global_identities: dict[int, tuple[int, int]] = {}
        self._capacities: list[int] = []
        self._raw_bodies: list[str] = []
        self._final_page: dict | None = None

    def _validate_page(self, page: dict) -> list[dict]:
        if page.get("protocol_id") != self.protocol_id or page.get("opcode") != self.opcode:
            raise ChannelStatusPaginationError("channel page envelope changed during pagination")
        records = page.get("records")
        capacity = page.get("page_capacity")
        reported = page.get("reported_record_count")
        if not isinstance(records, list) or not isinstance(capacity, int) or reported != len(records):
            raise ChannelStatusPaginationError("channel page metadata is inconsistent")
        if capacity <= 0 or len(records) > capacity:
            raise ChannelStatusPaginationError("channel page exceeds its reported capacity")
        return records

    def _merge_record(self, record: dict) -> bool:
        identity = _record_identity(record)
        global_id = record.get("channel_number")
        if not isinstance(global_id, int) or global_id <= 0:
            raise ChannelStatusPaginationError("channel page record has an invalid global ID")
        existing_identity = self._global_identities.get(global_id)
        if existing_identity is not None and existing_identity != identity:
            raise ChannelStatusPaginationError("channel pages contain a conflicting global ID")
        existing = self._records.get(identity)
        if existing is not None:
            if _stable_record(existing) != _stable_record(record):
                raise ChannelStatusPaginationError("channel pages contain a conflicting duplicate")
            return False
        self._records[identity] = dict(record)
        self._global_identities[global_id] = identity
        return True

    def _continuation(self, records: list[dict]) -> tuple[int, int, int]:
        if not records:
            raise ChannelStatusPaginationError("partial channel page made no progress")
        media_type, _ = _record_identity(records[-1])
        media_local_ids = {identity[1] for identity in self._records if identity[0] == media_type}
        next_id = 1
        while next_id in media_local_ids:
            next_id += 1
        if next_id > 0xFFFF:
            raise ChannelStatusPaginationError("channel pagination exhausted the media-local ID space")
        return media_type, next_id, 0

    def add(self, page: dict) -> tuple[int, int, int] | None:
        if len(self._capacities) >= self.maximum_pages:
            raise ChannelStatusPaginationError("channel pagination exceeded its page limit")
        records = self._validate_page(page)
        added = sum(self._merge_record(record) for record in records)
        if self._capacities and added == 0:
            raise ChannelStatusPaginationError("channel page made no progress")
        self._capacities.append(page["page_capacity"])
        self._raw_bodies.append(page.get("raw_body_hexadecimal", ""))
        disposition = page.get("page_disposition")
        if disposition == "more_pages":
            return self._continuation(records)
        if disposition != "complete":
            raise ChannelStatusPaginationError("channel page has an invalid disposition")
        self._final_page = dict(page)
        return None

    def result(self) -> dict:
        if self._final_page is None:
            raise ChannelStatusPaginationError("channel pagination did not reach a complete page")
        result = dict(self._final_page)
        result["records"] = sorted(
            self._records.values(),
            key=lambda record: (
                record["channel_number"],
                record["media_type_code"],
                record["media_local_channel_id"],
            ),
        )
        result["reported_record_count"] = len(result["records"])
        result["total_record_count"] = len(result["records"])
        result["page_count"] = len(self._capacities)
        result["page_capacities"] = list(self._capacities)
        result["raw_page_body_hexadecimal"] = list(self._raw_bodies)
        return result
