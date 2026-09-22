from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from netaudio import core
from netaudio.core import _requests

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

    def update(
        self, parsed_records: _requests.HeartbeatConnectionHealthRecords, observed_at: str, observed_monotonic: float
    ) -> dict | None:
        if not isinstance(parsed_records, dict):
            return None

        device_id = parsed_records.get("device_extended_unique_identifier")

        if not isinstance(device_id, str):
            return None
        state = self._devices.get(device_id) or _DeviceState(device_extended_unique_identifier=device_id)

        try:
            update = core.connection_health_update(
                {
                    "records": parsed_records,
                    "freshness_seconds": self._freshness_seconds,
                    "latency_previous": self._prior_stream(state.latency, observed_monotonic),
                    "late_previous": self._prior_stream(state.late_packets, observed_monotonic),
                }
            )
        except (core.NetaudioCoreError, core.NetaudioCoreJsonError):
            return None

        if update is None:
            return None

        if update["latency"] is not None:
            state.latency = self._update_latency_stream(
                state.latency, update["latency"], observed_at, observed_monotonic
            )

        if update["late_packets"] is not None:
            state.late_packets = self._update_late_packet_stream(
                state.late_packets, update["late_packets"], observed_at, observed_monotonic
            )

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

    def _prior_stream(self, stream: _StreamState | None, observed_monotonic: float) -> _requests.PreviousStream | None:
        if stream is None:
            return None

        return {
            "sequence": stream.sequence,
            "elapsed_seconds": observed_monotonic - stream.observed_monotonic,
            "fresh": stream.fresh,
            "late_counts": [
                {"receiver_flow_index": index, "late_packet_count": flow.count}
                for index, flow in stream.flows.items()
                if isinstance(flow, _LatePacketFlowState)
            ],
        }

    def _update_latency_stream(self, previous, measurements, observed_at, observed_monotonic):
        sequence = measurements["sequence"]
        previous_flows = previous.flows if measurements["preserve_history"] and previous else {}
        flows = {}
        for sample in measurements["samples"]:
            flow_index = sample["receiver_flow_index"]
            prior = previous_flows.get(flow_index)
            history = deque(prior.history, maxlen=self._history_limit) if prior else deque(maxlen=self._history_limit)
            history.append(
                {
                    "sequence": sequence,
                    "observed_at": observed_at,
                    "latency_sample_count": sample["latency_sample_count"],
                    "sample_rate_hertz": sample["sample_rate_hertz"],
                    "latency_nanoseconds": sample["latency_nanoseconds"],
                }
            )
            flows[flow_index] = _LatencyFlowState(history=history)
        return _StreamState(sequence, observed_at, observed_monotonic, True, flows)

    def _update_late_packet_stream(self, previous, measurements, observed_at, observed_monotonic):
        sequence = measurements["sequence"]
        previous_flows = previous.flows if previous else {}
        flows = {}
        for sample in measurements["samples"]:
            flow_index = sample["receiver_flow_index"]
            count = sample["late_packet_count"]
            prior = previous_flows.get(flow_index) if sample["preserve_history"] else None
            history = deque(prior.history, maxlen=self._history_limit) if prior else deque(maxlen=self._history_limit)
            history.append(
                {
                    "sequence": sequence,
                    "observed_at": observed_at,
                    "late_packet_count": count,
                    "late_packet_delta": sample["late_packet_delta"],
                }
            )
            flows[flow_index] = _LatePacketFlowState(history=history, count=count)
        return _StreamState(sequence, observed_at, observed_monotonic, True, flows)

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
