from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing_extensions import TypeGuard

CONNECTION_HEALTH_FRESHNESS_SECONDS = 5.0
CONNECTION_HEALTH_HISTORY_LIMIT = 300


@dataclass
class _FlowState:
    history: deque
    raw_impairment_value: int


@dataclass
class _DeviceState:
    device_extended_unique_identifier: str
    sequence: int
    observed_at: str
    observed_monotonic: float
    fresh: bool
    flows: dict[int, _FlowState]


class ReceiverFlowConnectionHealthTracker:
    def __init__(
        self,
        freshness_seconds: float = CONNECTION_HEALTH_FRESHNESS_SECONDS,
        history_limit: int = CONNECTION_HEALTH_HISTORY_LIMIT,
    ):
        if freshness_seconds <= 0:
            raise ValueError("freshness_seconds must be positive")
        if history_limit <= 0:
            raise ValueError("history_limit must be positive")
        self._freshness_seconds = freshness_seconds
        self._history_limit = history_limit
        self._devices: dict[str, _DeviceState] = {}

    @property
    def freshness_seconds(self) -> float:
        return self._freshness_seconds

    def update(
        self,
        parsed_records: dict,
        observed_at: str,
        observed_monotonic: float,
    ) -> dict | None:
        paired = self._pair_records(parsed_records)
        if paired is None:
            return None
        device_extended_unique_identifier, sequence, measurements = paired
        previous_device = self._devices.get(device_extended_unique_identifier)
        preserve_history = False
        consecutive_sequence = False

        if previous_device is not None:
            if observed_monotonic < previous_device.observed_monotonic:
                return None
            previous_is_fresh = (
                previous_device.fresh
                and observed_monotonic - previous_device.observed_monotonic < self._freshness_seconds
            )
            if previous_is_fresh:
                sequence_delta = (sequence - previous_device.sequence) & 0xFFFF
                if sequence_delta == 0 or sequence_delta >= 0x8000:
                    return None
                preserve_history = True
                consecutive_sequence = sequence_delta == 1
            else:
                previous_device.fresh = False

        previous_flows = previous_device.flows if preserve_history and previous_device else {}
        flows = {}
        for measurement in measurements:
            receiver_flow_index = measurement["receiver_flow_index"]
            latency_sample_count = measurement["latency_sample_count"]
            raw_impairment_value = measurement["raw_impairment_value"]
            if latency_sample_count == 0 and raw_impairment_value == 0:
                continue

            previous_flow = previous_flows.get(receiver_flow_index)
            if previous_flow is not None and raw_impairment_value < previous_flow.raw_impairment_value:
                previous_flow = None
            history = (
                deque(previous_flow.history, maxlen=self._history_limit)
                if previous_flow is not None
                else deque(maxlen=self._history_limit)
            )
            raw_impairment_delta = (
                raw_impairment_value - previous_flow.raw_impairment_value
                if previous_flow is not None and consecutive_sequence
                else None
            )
            sample_rate_hertz = measurement["sample_rate_hertz"]
            latency_nanoseconds = self._latency_nanoseconds(latency_sample_count, sample_rate_hertz)
            history.append(
                {
                    "sequence": sequence,
                    "observed_at": observed_at,
                    "latency_sample_count": latency_sample_count,
                    "sample_rate_hertz": sample_rate_hertz,
                    "latency_nanoseconds": latency_nanoseconds,
                    "raw_impairment_value": raw_impairment_value,
                    "raw_impairment_delta": raw_impairment_delta,
                }
            )
            flows[receiver_flow_index] = _FlowState(
                history=history,
                raw_impairment_value=raw_impairment_value,
            )

        device_state = _DeviceState(
            device_extended_unique_identifier=device_extended_unique_identifier,
            sequence=sequence,
            observed_at=observed_at,
            observed_monotonic=observed_monotonic,
            fresh=True,
            flows=flows,
        )
        self._devices[device_extended_unique_identifier] = device_state
        return self._serialize(device_state)

    def history_snapshot(self, device_extended_unique_identifier: str) -> dict | None:
        device_state = self._devices.get(device_extended_unique_identifier)
        return self._serialize(device_state, include_history=True) if device_state is not None else None

    def remove_device(self, device_extended_unique_identifier: str) -> bool:
        return self._devices.pop(device_extended_unique_identifier, None) is not None

    def seconds_until_expiry(
        self,
        device_extended_unique_identifier: str,
        observed_monotonic: float,
    ) -> float | None:
        device_state = self._devices.get(device_extended_unique_identifier)
        if device_state is None or not device_state.fresh:
            return None
        age = observed_monotonic - device_state.observed_monotonic
        return max(0.0, self._freshness_seconds - age)

    def expire_device(self, device_extended_unique_identifier: str, observed_monotonic: float) -> dict | None:
        device_state = self._devices.get(device_extended_unique_identifier)
        if device_state is None or not device_state.fresh:
            return None
        if observed_monotonic - device_state.observed_monotonic < self._freshness_seconds:
            return None
        device_state.fresh = False
        return self._serialize(device_state)

    def expire(self, observed_monotonic: float) -> list[tuple[str, dict]]:
        expired = []
        for device_extended_unique_identifier in tuple(self._devices):
            state = self.expire_device(device_extended_unique_identifier, observed_monotonic)
            if state is not None:
                expired.append((device_extended_unique_identifier, state))
        return expired

    def _pair_records(self, parsed_records: dict) -> tuple[str, int, list[dict]] | None:
        if not isinstance(parsed_records, dict):
            return None
        device_extended_unique_identifier = parsed_records.get("device_extended_unique_identifier")
        if not self._is_device_extended_unique_identifier(device_extended_unique_identifier):
            return None
        latency_records = parsed_records.get("latency_records")
        raw_impairment_records = parsed_records.get("raw_impairment_records")
        if not isinstance(latency_records, list) or not isinstance(raw_impairment_records, list):
            return None
        if not latency_records or len(latency_records) != len(raw_impairment_records):
            return None

        latency_by_key = self._records_by_key(latency_records)
        raw_impairment_by_key = self._records_by_key(raw_impairment_records)
        if latency_by_key is None or raw_impairment_by_key is None:
            return None
        if set(latency_by_key) != set(raw_impairment_by_key):
            return None

        sequences = {key[0] for key in latency_by_key}
        if len(sequences) != 1:
            return None
        sequence = sequences.pop()
        measurements = []
        occupied_indices = set()
        for key in sorted(latency_by_key, key=lambda record_key: record_key[1]):
            latency_record = latency_by_key[key]
            raw_impairment_record = raw_impairment_by_key[key]
            sample_rate_hertz = latency_record.get("sample_rate_hertz")
            if not self._is_unsigned_integer(sample_rate_hertz, 32) or sample_rate_hertz == 0:
                return None
            latency_entries = latency_record.get("entries")
            raw_impairment_entries = raw_impairment_record.get("entries")
            if not isinstance(latency_entries, list) or not isinstance(raw_impairment_entries, list):
                return None
            if len(latency_entries) != key[2] or len(raw_impairment_entries) != key[2]:
                return None

            for entry_offset, (latency_entry, raw_impairment_entry) in enumerate(
                zip(latency_entries, raw_impairment_entries)
            ):
                if not isinstance(latency_entry, dict) or not isinstance(raw_impairment_entry, dict):
                    return None
                receiver_flow_index = latency_entry.get("receiver_flow_index")
                if receiver_flow_index != raw_impairment_entry.get("receiver_flow_index"):
                    return None
                if not self._is_unsigned_integer(receiver_flow_index, 16):
                    return None
                if receiver_flow_index != key[1] + entry_offset:
                    return None
                if receiver_flow_index in occupied_indices:
                    return None
                latency_sample_count = latency_entry.get("latency_sample_count")
                raw_impairment_value = raw_impairment_entry.get("raw_impairment_value")
                if not self._is_unsigned_integer(latency_sample_count, 32):
                    return None
                if not self._is_unsigned_integer(raw_impairment_value, 32):
                    return None
                occupied_indices.add(receiver_flow_index)
                measurements.append(
                    {
                        "receiver_flow_index": receiver_flow_index,
                        "latency_sample_count": latency_sample_count,
                        "sample_rate_hertz": sample_rate_hertz,
                        "raw_impairment_value": raw_impairment_value,
                    }
                )
        return device_extended_unique_identifier, sequence, measurements

    def _records_by_key(self, records: list[dict]) -> dict[tuple[int, int, int], dict] | None:
        indexed = {}
        for record in records:
            if not isinstance(record, dict):
                return None
            sequence = record.get("sequence")
            start_receiver_flow_index = record.get("start_receiver_flow_index")
            entry_count = record.get("entry_count")
            if not self._is_unsigned_integer(sequence, 16):
                return None
            if not self._is_unsigned_integer(start_receiver_flow_index, 16):
                return None
            if not self._is_unsigned_integer(entry_count, 16) or entry_count == 0:
                return None
            if start_receiver_flow_index + entry_count > 1 << 16:
                return None
            key = (sequence, start_receiver_flow_index, entry_count)
            if key in indexed:
                return None
            indexed[key] = record
        return indexed

    def _serialize(self, device_state: _DeviceState, include_history: bool = False) -> dict:
        flows = []
        for receiver_flow_index, flow_state in sorted(device_state.flows.items()):
            history = list(flow_state.history)
            latency_values = [
                sample["latency_nanoseconds"] for sample in history if sample["latency_nanoseconds"] is not None
            ]
            current = history[-1]
            flow: dict = {
                "receiver_flow_index": receiver_flow_index,
                "receiver_flow_slot": receiver_flow_index + 1,
                "current_latency_nanoseconds": current["latency_nanoseconds"],
                "average_latency_nanoseconds": self._rounded_average(latency_values),
                "peak_latency_nanoseconds": max(latency_values) if latency_values else None,
                "latency_sample_count": current["latency_sample_count"],
                "sample_rate_hertz": current["sample_rate_hertz"],
                "raw_impairment_value": current["raw_impairment_value"],
                "raw_impairment_delta": current["raw_impairment_delta"],
                "history_sample_count": len(history),
            }
            if include_history:
                flow["history"] = history
            flows.append(flow)
        return {
            "device_extended_unique_identifier": device_state.device_extended_unique_identifier,
            "fresh": device_state.fresh,
            "freshness_seconds": self._freshness_seconds,
            "history_limit": self._history_limit,
            "sequence": device_state.sequence,
            "observed_at": device_state.observed_at,
            "observation_timestamp_source": "local_receive_time",
            "flows": flows,
        }

    @staticmethod
    def _is_device_extended_unique_identifier(value: object) -> TypeGuard[str]:
        return (
            isinstance(value, str)
            and len(value) == 16
            and value not in {"0000000000000000", "ffffffffffffffff"}
            and all(character in "0123456789abcdef" for character in value)
        )

    @staticmethod
    def _is_unsigned_integer(value: object, bits: int) -> TypeGuard[int]:
        return type(value) is int and 0 <= value < 1 << bits

    @staticmethod
    def _latency_nanoseconds(latency_sample_count: int, sample_rate_hertz: int) -> int | None:
        if latency_sample_count == 0:
            return None
        numerator = latency_sample_count * 1_000_000_000
        return (numerator + sample_rate_hertz // 2) // sample_rate_hertz

    @staticmethod
    def _rounded_average(values: list[int]) -> int | None:
        if not values:
            return None
        return (sum(values) + len(values) // 2) // len(values)
