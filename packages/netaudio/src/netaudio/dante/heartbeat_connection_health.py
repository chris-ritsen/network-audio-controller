from __future__ import annotations

from typing import cast
from netaudio import core
from netaudio.core import _requests

CONNECTION_HEALTH_FRESHNESS_SECONDS = 5.0
CONNECTION_HEALTH_HISTORY_LIMIT = 300


class ReceiverFlowConnectionHealthTracker:
    """Own device lifetimes; the core owns observation acceptance and statistics."""

    def __init__(
        self, freshness_seconds=CONNECTION_HEALTH_FRESHNESS_SECONDS, history_limit=CONNECTION_HEALTH_HISTORY_LIMIT
    ):
        if freshness_seconds <= 0 or not 1 <= history_limit <= 10000:
            raise ValueError("Invalid freshness or history limit")
        self._freshness_seconds = freshness_seconds
        self._history_limit = history_limit
        self._devices = {}
        self._trackers = {}

    @property
    def freshness_seconds(self):
        return self._freshness_seconds

    def update(
        self,
        parsed_records: _requests.HeartbeatConnectionHealthRecords,
        observed_at: str,
        observed_monotonic: float,
        topology: _requests.Topology | None = None,
        *,
        reset=False,
        refresh_only=False,
    ):
        if not isinstance(parsed_records, dict):
            return None

        identity = parsed_records.get("device_extended_unique_identifier")
        try:
            tracker = self._trackers.get(identity)
            if tracker is None:
                tracker = core.ReceiverTracker()
            state = tracker.update(
                {
                    "records": cast(_requests.HeartbeatConnectionHealthRecords, parsed_records),
                    "previous": None,
                    "topology": topology or {"capacity": None, "complete": False, "flows": []},
                    "observed_at": observed_at,
                    "observed_monotonic": observed_monotonic,
                    "freshness_seconds": self._freshness_seconds,
                    "history_limit": self._history_limit,
                    "reset": reset,
                    "refresh_only": refresh_only,
                }
            )
        except (core.NetaudioCoreError, core.NetaudioCoreJsonError):
            return None

        self._trackers[identity] = tracker
        if state is not None:
            self._devices[identity] = state
            return state

        return None

    def history_snapshot(self, identity):
        tracker = self._trackers.get(identity)
        return tracker.snapshot(include_history=True) if identity in self._devices and tracker is not None else None

    def remove_device(self, identity):
        tracker = self._trackers.pop(identity, None)
        if tracker is not None:
            tracker.close()
        return self._devices.pop(identity, None) is not None

    def seconds_until_expiry(self, identity, now):
        state = self._devices.get(identity)
        if state is None:
            return None

        remaining = [
            max(0.0, self._freshness_seconds - (now - metric["current"]["observed_monotonic"]))
            for path in state["paths"]
            for metric in (path["latency"], path["late_packets"])
            if metric["fresh"] and metric["current"] is not None
        ]
        return min(remaining) if remaining else None

    def expire_device(self, identity, now):
        if identity not in self._devices:
            return None

        return self.update(
            {
                "device_extended_unique_identifier": identity,
                "latency_records": [],
                "late_packet_records": [],
                "diagnostics": [],
            },
            "",
            now,
        )

    def reset(self, identity, now):
        if identity not in self._devices:
            return None

        return self.update(
            {
                "device_extended_unique_identifier": identity,
                "latency_records": [],
                "late_packet_records": [],
                "diagnostics": [],
            },
            "",
            now,
            reset=True,
        )

    def expire(self, now):
        return [
            (identity, state)
            for identity in tuple(self._devices)
            if (state := self.expire_device(identity, now)) is not None
        ]
