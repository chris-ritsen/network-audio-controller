from __future__ import annotations

import time
from binascii import Error as HexadecimalDecodeError
from binascii import unhexlify
from dataclasses import dataclass, replace
from datetime import datetime, timezone


INTERFACE_STATISTICS_FRESHNESS_SECONDS = 5.0
INTERFACE_STATISTICS_TRANSPORT_SOURCE = "conmon_0x0040"


def _timestamp(wall_time: float) -> str:
    return datetime.fromtimestamp(wall_time, timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _validated_hexadecimal(value: object, expected_size: int, description: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{description} is not hexadecimal text")
    try:
        decoded = unhexlify(value)
    except (HexadecimalDecodeError, ValueError) as exception:
        raise ValueError(f"{description} is not valid hexadecimal") from exception
    if len(decoded) != expected_size:
        raise ValueError(f"{description} length does not match its declared size")
    return value


@dataclass(frozen=True)
class InterfaceStatisticsRecord:
    record_pointer: int
    record_size_bytes: int
    transmit_raw_bytes_per_second: int
    receive_raw_bytes_per_second: int
    transmit_bits_per_second: int
    receive_bits_per_second: int
    cumulative_transmit_errors: int
    cumulative_receive_errors: int
    discriminator_status_word: int
    speed_megabits_per_second: int
    extension_hexadecimal: str
    raw_record_hexadecimal: str
    transmit_errors_since_local_reset: int | None = None
    receive_errors_since_local_reset: int | None = None

    @classmethod
    def from_core(cls, parsed: dict) -> InterfaceStatisticsRecord:
        record_size_bytes = parsed["record_size_bytes"]
        if not isinstance(record_size_bytes, int) or record_size_bytes < 24:
            raise ValueError("interface-statistics record is shorter than its common fields")
        raw_record_hexadecimal = _validated_hexadecimal(
            parsed["raw_record_hexadecimal"],
            record_size_bytes,
            "interface-statistics raw record",
        )
        extension_size = record_size_bytes - 24
        extension_hexadecimal = _validated_hexadecimal(
            parsed["extension_hexadecimal"],
            extension_size,
            "interface-statistics record extension",
        )
        transmit_raw_bytes_per_second = parsed["transmit_raw_bytes_per_second"]
        receive_raw_bytes_per_second = parsed["receive_raw_bytes_per_second"]
        transmit_bits_per_second = parsed["transmit_bits_per_second"]
        receive_bits_per_second = parsed["receive_bits_per_second"]
        if transmit_bits_per_second != transmit_raw_bytes_per_second * 8:
            raise ValueError("interface-statistics transmit bit rate does not match the raw byte rate")
        if receive_bits_per_second != receive_raw_bytes_per_second * 8:
            raise ValueError("interface-statistics receive bit rate does not match the raw byte rate")
        return cls(
            record_pointer=parsed["record_pointer"],
            record_size_bytes=record_size_bytes,
            transmit_raw_bytes_per_second=transmit_raw_bytes_per_second,
            receive_raw_bytes_per_second=receive_raw_bytes_per_second,
            transmit_bits_per_second=transmit_bits_per_second,
            receive_bits_per_second=receive_bits_per_second,
            cumulative_transmit_errors=parsed["cumulative_transmit_errors"],
            cumulative_receive_errors=parsed["cumulative_receive_errors"],
            discriminator_status_word=parsed["discriminator_status_word"],
            speed_megabits_per_second=parsed["speed_megabits_per_second"],
            extension_hexadecimal=extension_hexadecimal,
            raw_record_hexadecimal=raw_record_hexadecimal,
        )

    def to_dict(self, observation: InterfaceStatisticsObservation | None = None) -> dict:
        result = {
            "record_pointer": self.record_pointer,
            "record_size_bytes": self.record_size_bytes,
            "transmit_raw_bytes_per_second": self.transmit_raw_bytes_per_second,
            "receive_raw_bytes_per_second": self.receive_raw_bytes_per_second,
            "transmit_bits_per_second": self.transmit_bits_per_second,
            "receive_bits_per_second": self.receive_bits_per_second,
            "cumulative_transmit_errors": self.cumulative_transmit_errors,
            "cumulative_receive_errors": self.cumulative_receive_errors,
            "transmit_errors_since_local_reset": self.transmit_errors_since_local_reset,
            "receive_errors_since_local_reset": self.receive_errors_since_local_reset,
            "discriminator_status_word": self.discriminator_status_word,
            "speed_megabits_per_second": self.speed_megabits_per_second,
            "extension_hexadecimal": self.extension_hexadecimal,
            "raw_record_hexadecimal": self.raw_record_hexadecimal,
        }
        if observation is not None:
            result.update(
                packet_source=observation.packet_source,
                received_at=observation.received_at,
                fresh=observation.fresh,
                transport_source=observation.transport_source,
            )
        return result


@dataclass(frozen=True)
class InterfaceStatisticsGroup:
    group_index: int
    group_pointer: int
    record_count: int
    record_pointers: tuple[int, ...]
    selected_stats: InterfaceStatisticsRecord | None
    raw_records: tuple[InterfaceStatisticsRecord, ...]

    @classmethod
    def from_core(cls, parsed: dict) -> InterfaceStatisticsGroup:
        record_count = parsed["record_count"]
        record_pointers = tuple(parsed["record_pointers"])
        parsed_records = parsed["raw_records"]
        if record_count != len(record_pointers) or record_count != len(parsed_records):
            raise ValueError("interface-statistics record count does not match its pointer table")
        raw_records = tuple(InterfaceStatisticsRecord.from_core(record) for record in parsed_records)
        if tuple(record.record_pointer for record in raw_records) != record_pointers:
            raise ValueError("interface-statistics record pointers do not match their pointer table")
        selected_parsed = parsed.get("selected_stats")
        selected_stats = InterfaceStatisticsRecord.from_core(selected_parsed) if selected_parsed is not None else None
        if selected_stats is not None:
            if selected_stats.discriminator_status_word & 0xFFFF_0000:
                raise ValueError("interface-statistics selected record has a nonzero discriminator")
            if selected_stats not in raw_records:
                raise ValueError("interface-statistics selected record is absent from the raw record list")
            expected = next(
                (record for record in raw_records if record.discriminator_status_word & 0xFFFF_0000 == 0),
                None,
            )
            if selected_stats != expected:
                raise ValueError("interface-statistics selected record is not the first eligible record")
        return cls(
            group_index=parsed["group_index"],
            group_pointer=parsed["group_pointer"],
            record_count=record_count,
            record_pointers=record_pointers,
            selected_stats=selected_stats,
            raw_records=raw_records,
        )

    def to_dict(self, observation: InterfaceStatisticsObservation | None = None) -> dict:
        return {
            "group_index": self.group_index,
            "group_pointer": self.group_pointer,
            "record_count": self.record_count,
            "record_pointers": list(self.record_pointers),
            "selected_stats": self.selected_stats.to_dict(observation) if self.selected_stats is not None else None,
            "raw_records": [record.to_dict(observation) for record in self.raw_records],
        }


@dataclass(frozen=True)
class InterfaceStatisticsObservation:
    record_protocol_version: int
    header_record_pointer: int
    header_record_size_bytes: int
    header_record_hexadecimal: str
    capability_mask: int
    utilization_supported: bool
    errors_supported: bool
    clear_errors_supported: bool
    interface_group_count: int
    interface_group_pointers: tuple[int, ...]
    interface_groups: tuple[InterfaceStatisticsGroup, ...]
    raw_body_hexadecimal: str
    packet_source: str
    received_at: str
    received_monotonic: float
    freshness_seconds: float
    fresh: bool = True
    transport_source: str = INTERFACE_STATISTICS_TRANSPORT_SOURCE
    observation_timestamp_source: str = "local_receive_time"

    @classmethod
    def from_core(
        cls,
        parsed: dict,
        packet_source: str,
        *,
        received_at: str | None = None,
        received_monotonic: float | None = None,
        wall_time: float | None = None,
        freshness_seconds: float = INTERFACE_STATISTICS_FRESHNESS_SECONDS,
    ) -> InterfaceStatisticsObservation:
        if not isinstance(packet_source, str) or not packet_source:
            raise ValueError("interface-statistics packet source is required")
        if freshness_seconds <= 0:
            raise ValueError("interface-statistics freshness interval must be positive")
        if received_monotonic is None:
            received_monotonic = time.monotonic()
        if received_at is None:
            received_at = _timestamp(time.time() if wall_time is None else wall_time)
        interface_group_count = parsed["interface_group_count"]
        interface_group_pointers = tuple(parsed["interface_group_pointers"])
        parsed_groups = parsed["interface_groups"]
        if interface_group_count != len(interface_group_pointers) or interface_group_count != len(parsed_groups):
            raise ValueError("interface-statistics group count does not match its pointer table")
        interface_groups = tuple(InterfaceStatisticsGroup.from_core(group) for group in parsed_groups)
        if tuple(group.group_pointer for group in interface_groups) != interface_group_pointers:
            raise ValueError("interface-statistics group pointers do not match their pointer table")
        header_record_size_bytes = parsed["header_record_size_bytes"]
        if not isinstance(header_record_size_bytes, int) or header_record_size_bytes <= 0:
            raise ValueError("interface-statistics header record is empty")
        header_record_hexadecimal = _validated_hexadecimal(
            parsed["header_record_hexadecimal"],
            header_record_size_bytes,
            "interface-statistics header record",
        )
        raw_body_hexadecimal = parsed["raw_body_hexadecimal"]
        if not isinstance(raw_body_hexadecimal, str):
            raise ValueError("interface-statistics raw body is not hexadecimal text")
        try:
            unhexlify(raw_body_hexadecimal)
        except (HexadecimalDecodeError, ValueError) as exception:
            raise ValueError("interface-statistics raw body is not valid hexadecimal") from exception
        return cls(
            record_protocol_version=parsed["record_protocol_version"],
            header_record_pointer=parsed["header_record_pointer"],
            header_record_size_bytes=header_record_size_bytes,
            header_record_hexadecimal=header_record_hexadecimal,
            capability_mask=parsed["capability_mask"],
            utilization_supported=parsed["utilization_supported"],
            errors_supported=parsed["errors_supported"],
            clear_errors_supported=parsed["clear_errors_supported"],
            interface_group_count=interface_group_count,
            interface_group_pointers=interface_group_pointers,
            interface_groups=interface_groups,
            raw_body_hexadecimal=raw_body_hexadecimal,
            packet_source=packet_source,
            received_at=received_at,
            received_monotonic=received_monotonic,
            freshness_seconds=freshness_seconds,
        )

    def at(self, observed_monotonic: float) -> InterfaceStatisticsObservation:
        fresh = observed_monotonic - self.received_monotonic < self.freshness_seconds
        return self if fresh == self.fresh else replace(self, fresh=fresh)

    def to_dict(self, observed_monotonic: float | None = None) -> dict:
        observation = self.at(time.monotonic() if observed_monotonic is None else observed_monotonic)
        return {
            "record_protocol_version": observation.record_protocol_version,
            "header_record_pointer": observation.header_record_pointer,
            "header_record_size_bytes": observation.header_record_size_bytes,
            "header_record_hexadecimal": observation.header_record_hexadecimal,
            "capability_mask": observation.capability_mask,
            "utilization_supported": observation.utilization_supported,
            "errors_supported": observation.errors_supported,
            "clear_errors_supported": observation.clear_errors_supported,
            "interface_group_count": observation.interface_group_count,
            "interface_group_pointers": list(observation.interface_group_pointers),
            "interface_groups": [group.to_dict(observation) for group in observation.interface_groups],
            "raw_body_hexadecimal": observation.raw_body_hexadecimal,
            "packet_source": observation.packet_source,
            "received_at": observation.received_at,
            "fresh": observation.fresh,
            "freshness_seconds": observation.freshness_seconds,
            "transport_source": observation.transport_source,
            "observation_timestamp_source": observation.observation_timestamp_source,
        }


class InterfaceStatisticsErrorBaselines:
    def __init__(self) -> None:
        self._baselines: dict[tuple[str, int], tuple[int, int]] = {}

    def apply(self, observation: InterfaceStatisticsObservation) -> InterfaceStatisticsObservation:
        groups = []
        for group in observation.interface_groups:
            selected = group.selected_stats
            if selected is None:
                groups.append(group)
                continue
            key = (observation.packet_source, group.group_index)
            baseline = self._baselines.get(key)
            if baseline is None or selected.cumulative_transmit_errors < baseline[0]:
                transmit_baseline = selected.cumulative_transmit_errors
            else:
                transmit_baseline = baseline[0]
            if baseline is None or selected.cumulative_receive_errors < baseline[1]:
                receive_baseline = selected.cumulative_receive_errors
            else:
                receive_baseline = baseline[1]
            self._baselines[key] = (transmit_baseline, receive_baseline)
            displayed = replace(
                selected,
                transmit_errors_since_local_reset=selected.cumulative_transmit_errors - transmit_baseline,
                receive_errors_since_local_reset=selected.cumulative_receive_errors - receive_baseline,
            )
            groups.append(replace(group, selected_stats=displayed))
        return replace(observation, interface_groups=tuple(groups))

    def reset(self, observation: InterfaceStatisticsObservation) -> InterfaceStatisticsObservation:
        for group in observation.interface_groups:
            selected = group.selected_stats
            if selected is not None:
                self._baselines[(observation.packet_source, group.group_index)] = (
                    selected.cumulative_transmit_errors,
                    selected.cumulative_receive_errors,
                )
        return self.apply(observation)

    def discard(self, packet_source: str) -> None:
        for key in tuple(self._baselines):
            if key[0] == packet_source:
                del self._baselines[key]
