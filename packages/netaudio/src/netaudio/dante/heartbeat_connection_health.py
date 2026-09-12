from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing_extensions import TypeGuard

CONNECTION_HEALTH_FRESHNESS_SECONDS = 5.0
CONNECTION_HEALTH_HISTORY_LIMIT = 300


@dataclass
class _LatencyFlowState:
    history: deque


@dataclass
class _LatePacketFlowState:
    history: deque
    count: int


@dataclass
class _StreamState:
    sequence: int
    observed_at: str
    observed_monotonic: float
    fresh: bool
    flows: dict


@dataclass
class _DeviceState:
    device_extended_unique_identifier: str
    latency: _StreamState | None = None
    late_packets: _StreamState | None = None


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

    def update(self, parsed_records: dict, observed_at: str, observed_monotonic: float) -> dict | None:
        if not isinstance(parsed_records, dict):
            return None
        device_id = parsed_records.get("device_extended_unique_identifier")
        if not self._is_device_extended_unique_identifier(device_id):
            return None
        latency_records = parsed_records.get("latency_records")
        late_packet_records = parsed_records.get("late_packet_records")
        if not isinstance(latency_records, list) or not isinstance(late_packet_records, list):
            return None
        if not latency_records and not late_packet_records:
            return None

        state = self._devices.get(device_id) or _DeviceState(device_extended_unique_identifier=device_id)
        changed = False
        if latency_records:
            measurements = self._latency_measurements(latency_records)
            if measurements is None:
                return None
            updated = self._update_latency_stream(state.latency, measurements, observed_at, observed_monotonic)
            if updated is not None:
                state.latency = updated
                changed = True
        if late_packet_records:
            measurements = self._late_packet_measurements(late_packet_records)
            if measurements is None:
                return None
            updated = self._update_late_packet_stream(state.late_packets, measurements, observed_at, observed_monotonic)
            if updated is not None:
                state.late_packets = updated
                changed = True
        if not changed:
            return None
        self._devices[device_id] = state
        return self._serialize(state)

    def history_snapshot(self, device_extended_unique_identifier: str) -> dict | None:
        state = self._devices.get(device_extended_unique_identifier)
        return self._serialize(state, include_history=True) if state is not None else None

    def remove_device(self, device_extended_unique_identifier: str) -> bool:
        return self._devices.pop(device_extended_unique_identifier, None) is not None

    def seconds_until_expiry(self, device_extended_unique_identifier: str, observed_monotonic: float) -> float | None:
        state = self._devices.get(device_extended_unique_identifier)
        if state is None:
            return None
        remaining = [
            max(0.0, self._freshness_seconds - (observed_monotonic - stream.observed_monotonic))
            for stream in (state.latency, state.late_packets)
            if stream is not None and stream.fresh
        ]
        return min(remaining) if remaining else None

    def expire_device(self, device_extended_unique_identifier: str, observed_monotonic: float) -> dict | None:
        state = self._devices.get(device_extended_unique_identifier)
        if state is None:
            return None
        changed = False
        for stream in (state.latency, state.late_packets):
            if (
                stream is not None
                and stream.fresh
                and observed_monotonic - stream.observed_monotonic >= self._freshness_seconds
            ):
                stream.fresh = False
                changed = True
        return self._serialize(state) if changed else None

    def expire(self, observed_monotonic: float) -> list[tuple[str, dict]]:
        expired = []
        for device_id in tuple(self._devices):
            state = self.expire_device(device_id, observed_monotonic)
            if state is not None:
                expired.append((device_id, state))
        return expired

    def _stream_is_current(self, stream: _StreamState | None, sequence: int, observed_monotonic: float):
        if stream is None:
            return True, False, False
        if observed_monotonic < stream.observed_monotonic:
            return False, False, False
        was_fresh = stream.fresh and observed_monotonic - stream.observed_monotonic < self._freshness_seconds
        if not was_fresh:
            stream.fresh = False
            return True, False, False
        sequence_delta = (sequence - stream.sequence) & 0xFFFF
        if sequence_delta == 0 or sequence_delta >= 0x8000:
            return False, False, False
        return True, True, sequence_delta == 1

    def _update_latency_stream(self, previous, measurements, observed_at, observed_monotonic):
        sequence, values = measurements
        accepted, preserve, _ = self._stream_is_current(previous, sequence, observed_monotonic)
        if not accepted:
            return None
        previous_flows = previous.flows if preserve and previous else {}
        flows = {}
        for flow_index, sample_count, sample_rate_hertz in values:
            prior = previous_flows.get(flow_index)
            history = deque(prior.history, maxlen=self._history_limit) if prior else deque(maxlen=self._history_limit)
            latency_nanoseconds = self._latency_nanoseconds(sample_count, sample_rate_hertz)
            history.append(
                {
                    "sequence": sequence,
                    "observed_at": observed_at,
                    "latency_sample_count": sample_count,
                    "sample_rate_hertz": sample_rate_hertz,
                    "latency_nanoseconds": latency_nanoseconds,
                }
            )
            flows[flow_index] = _LatencyFlowState(history=history)
        return _StreamState(sequence, observed_at, observed_monotonic, True, flows)

    def _update_late_packet_stream(self, previous, measurements, observed_at, observed_monotonic):
        sequence, values = measurements
        accepted, preserve, consecutive = self._stream_is_current(previous, sequence, observed_monotonic)
        if not accepted:
            return None
        previous_flows = previous.flows if preserve and previous else {}
        flows = {}
        for flow_index, count in values:
            prior = previous_flows.get(flow_index)
            if prior is not None and count < prior.count:
                prior = None
            history = deque(prior.history, maxlen=self._history_limit) if prior else deque(maxlen=self._history_limit)
            delta = count - prior.count if prior is not None and consecutive else None
            history.append(
                {
                    "sequence": sequence,
                    "observed_at": observed_at,
                    "late_packet_count": count,
                    "late_packet_delta": delta,
                }
            )
            flows[flow_index] = _LatePacketFlowState(history=history, count=count)
        return _StreamState(sequence, observed_at, observed_monotonic, True, flows)

    def _latency_measurements(self, records):
        sequence, entries = self._records(records, require_sample_rate=True)
        if sequence is None:
            return None
        return sequence, [
            (entry["receiver_flow_index"], entry["latency_sample_count"], sample_rate) for entry, sample_rate in entries
        ]

    def _late_packet_measurements(self, records):
        sequence, entries = self._records(records, require_sample_rate=False)
        if sequence is None:
            return None
        return sequence, [(entry["receiver_flow_index"], entry["late_packet_count"]) for entry, _ in entries]

    def _records(self, records, *, require_sample_rate):
        sequences = set()
        occupied = set()
        measurements = []
        for record in records:
            if not isinstance(record, dict):
                return None, None
            sequence = record.get("sequence")
            entries = record.get("entries")
            if not self._is_unsigned_integer(sequence, 16) or not isinstance(entries, list):
                return None, None
            sample_rate = record.get("sample_rate_hertz") if require_sample_rate else None
            if require_sample_rate and (not self._is_unsigned_integer(sample_rate, 32) or sample_rate == 0):
                return None, None
            sequences.add(sequence)
            for entry in entries:
                if not isinstance(entry, dict):
                    return None, None
                flow_index = entry.get("receiver_flow_index")
                field = "latency_sample_count" if require_sample_rate else "late_packet_count"
                value = entry.get(field)
                if (
                    not self._is_unsigned_integer(flow_index, 16)
                    or not self._is_unsigned_integer(value, 32)
                    or flow_index in occupied
                ):
                    return None, None
                occupied.add(flow_index)
                measurements.append((entry, sample_rate))
        if len(sequences) != 1:
            return None, None
        return sequences.pop(), measurements

    def _serialize(self, state: _DeviceState, include_history: bool = False) -> dict:
        flow_indices = set()
        for stream in (state.latency, state.late_packets):
            if stream is not None:
                flow_indices.update(stream.flows)
        flows = []
        for flow_index in sorted(flow_indices):
            latency_flow = state.latency.flows.get(flow_index) if state.latency else None
            late_flow = state.late_packets.flows.get(flow_index) if state.late_packets else None
            latency_history = list(latency_flow.history) if latency_flow else []
            late_history = list(late_flow.history) if late_flow else []
            flow = {"receiver_flow_index": flow_index, "receiver_flow_slot": flow_index + 1}
            if latency_history:
                latencies = [item["latency_nanoseconds"] for item in latency_history]
                flow.update(
                    current_latency_nanoseconds=latencies[-1],
                    average_latency_nanoseconds=round(sum(latencies) / len(latencies)),
                    peak_latency_nanoseconds=max(latencies),
                    latency_history_sample_count=len(latency_history),
                )
                if include_history:
                    flow["latency_history"] = latency_history
            if late_history:
                flow.update(
                    late_packet_count=late_history[-1]["late_packet_count"],
                    late_packet_delta=late_history[-1]["late_packet_delta"],
                    late_packet_history_sample_count=len(late_history),
                )
                if include_history:
                    flow["late_packet_history"] = late_history
            flows.append(flow)
        return {
            "device_extended_unique_identifier": state.device_extended_unique_identifier,
            "fresh": any(stream is not None and stream.fresh for stream in (state.latency, state.late_packets)),
            "latency_stream": self._stream_metadata(state.latency),
            "late_packet_stream": self._stream_metadata(state.late_packets),
            "observation_timestamp_source": "local_receive_time",
            "flows": flows,
        }

    @staticmethod
    def _stream_metadata(stream):
        if stream is None:
            return None
        return {"sequence": stream.sequence, "observed_at": stream.observed_at, "fresh": stream.fresh}

    @staticmethod
    def _latency_nanoseconds(sample_count: int, sample_rate_hertz: int) -> int:
        return round(sample_count * 1_000_000_000 / sample_rate_hertz)

    @staticmethod
    def _is_unsigned_integer(value: object, bits: int) -> TypeGuard[int]:
        return isinstance(value, int) and not isinstance(value, bool) and 0 <= value < 1 << bits

    @staticmethod
    def _is_device_extended_unique_identifier(value: object) -> TypeGuard[str]:
        if not isinstance(value, str) or len(value) != 16:
            return False
        try:
            decoded = bytes.fromhex(value)
        except ValueError:
            return False
        return len(decoded) == 8 and any(decoded)
