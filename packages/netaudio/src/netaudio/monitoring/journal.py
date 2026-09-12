from __future__ import annotations

import json
import logging
import os
import tempfile
from collections import deque
from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from netaudio.monitoring.model import (
    DEFAULT_EVENT_HISTORY_LIMIT,
    EVENT_JOURNAL_SCHEMA_VERSION,
    DerivationStatus,
    EventJournalThresholds,
    EventSeverity,
    MonitoringEvent,
    MonitoringEventKind,
    OperationLifecyclePhase,
    _json_safe,
    _parse_timestamp,
)
from netaudio.monitoring.issues import IssueEngine, IssueEvidenceClass, IssueTransition, IssueTransitionKind
from netaudio.monitoring.signals import (
    _channel_map,
    _clock_evidence,
    _configured_flow_latency,
    _device_identity,
    _flow_map,
    _interface_map,
    _observation_context,
    _positive_integer,
    _positive_number,
    _ptp_port_map,
    _ptp_state,
    _subscription_failure_state,
    _subscription_map,
    _unsigned_integer,
    _unsigned_number,
    snapshot_from_device,
)

logger = logging.getLogger("netaudio")


class MonitoringEventJournal:
    """Daemon-owned, bounded history derived from already-observed device state."""

    def __init__(
        self,
        path: Path | None,
        *,
        max_events: int = DEFAULT_EVENT_HISTORY_LIMIT,
        thresholds: EventJournalThresholds | None = None,
        wall_clock: Callable[[], datetime] | None = None,
    ):
        if isinstance(max_events, bool) or not isinstance(max_events, int) or max_events <= 0:
            raise ValueError("max_events must be a positive integer")
        self.path = path
        self.max_events = max_events
        self.thresholds = thresholds or EventJournalThresholds()
        self._wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self._events: deque[MonitoringEvent] = deque(maxlen=max_events)
        self._snapshots: dict[str, dict[str, Any]] = {}
        self._conditions: dict[tuple[str, str, str], bool] = {}
        self._next_sequence = 1
        self.issue_engine = IssueEngine(history_limit=max_events)
        self._load()

    @classmethod
    def from_daemon_config(
        cls,
        path: Path | None,
        daemon_config: Mapping[str, Any],
        *,
        wall_clock: Callable[[], datetime] | None = None,
    ) -> MonitoringEventJournal:
        raw_limit = daemon_config.get("event_history_limit", DEFAULT_EVENT_HISTORY_LIMIT)
        if isinstance(raw_limit, bool) or not isinstance(raw_limit, int) or raw_limit <= 0:
            raise ValueError("daemon.event_history_limit must be a positive integer")
        return cls(
            path,
            max_events=raw_limit,
            thresholds=EventJournalThresholds.from_daemon_config(daemon_config),
            wall_clock=wall_clock,
        )

    def observe_device(self, device, *, timestamp: str | None = None) -> list[MonitoringEvent]:
        return self.observe_snapshot(snapshot_from_device(device), timestamp=timestamp)

    def observe_snapshot(self, snapshot: Mapping[str, Any], *, timestamp: str | None = None) -> list[MonitoringEvent]:
        current = _json_safe(dict(snapshot))
        identity = _device_identity(current)
        current["device_identity"] = identity
        previous = self._snapshots.get(identity)
        self._snapshots[identity] = deepcopy(current)
        observed_at = timestamp or self._timestamp()
        issue_revision = self.issue_engine.revision
        issue_transitions = self.issue_engine.observe_snapshot(
            current,
            timestamp=observed_at,
            emit_transitions=previous is not None,
        )
        if previous is None:
            self._prime_conditions(current)
            self._persist()
            return []

        generated: list[MonitoringEvent] = []
        self._observe_presence(previous, current, observed_at, generated)
        self._observe_clock(previous, current, observed_at, generated)
        self._observe_ptp_ports(previous, current, observed_at, generated)
        self._observe_mutes(previous, current, observed_at, generated)
        self._observe_subscriptions(previous, current, observed_at, generated)
        self._observe_connection_health(previous, current, observed_at, generated)
        self._observe_interface_traffic(previous, current, observed_at, generated)
        self._append_issue_transitions(issue_transitions, generated)
        if generated or self.issue_engine.revision != issue_revision:
            self._persist()
        return generated

    def observe_disappearance(self, device_or_snapshot, *, timestamp: str | None = None) -> list[MonitoringEvent]:
        if isinstance(device_or_snapshot, Mapping):
            current = _json_safe(dict(device_or_snapshot))
        else:
            current = snapshot_from_device(device_or_snapshot)
        identity = _device_identity(current)
        current["device_identity"] = identity
        previous = self._snapshots.get(identity)
        if previous is not None and previous.get("online") is False:
            return []
        current["online"] = False
        self._snapshots[identity] = deepcopy(current)
        issue_transitions = self.issue_engine.observe_snapshot(
            current,
            timestamp=timestamp or self._timestamp(),
            emit_transitions=previous is not None,
        )
        event = self._append(
            current,
            timestamp or self._timestamp(),
            MonitoringEventKind.DEVICE_DISAPPEARED,
            EventSeverity.ERROR,
            previous.get("online") if previous is not None else None,
            False,
            {
                "previous_observation": self._presence_evidence(previous),
                "current_observation": self._presence_evidence(current),
            },
            "device_lifecycle",
            DerivationStatus.OBSERVED,
        )
        generated = [event]
        self._append_issue_transitions(issue_transitions, generated)
        self._persist()
        return generated

    def export_issues(self, **filters) -> dict[str, Any]:
        return self.issue_engine.export(**filters)

    def list_events(
        self,
        *,
        device: str | None = None,
        kind: str | MonitoringEventKind | None = None,
        severity: str | EventSeverity | None = None,
        since: str | datetime | None = None,
        limit: int | None = None,
    ) -> list[MonitoringEvent]:
        kind_value = MonitoringEventKind(kind).value if kind is not None else None
        severity_value = EventSeverity(severity).value if severity is not None else None
        since_value = _parse_timestamp(since) if since is not None else None
        if since is not None and since_value is None:
            raise ValueError("since must be an ISO-8601 timestamp")
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer")
        if limit == 0:
            return []
        device_value = device.casefold() if device is not None else None
        matches = []
        for event in reversed(self._events):
            if kind_value is not None and event.kind.value != kind_value:
                continue
            if severity_value is not None and event.severity.value != severity_value:
                continue
            if device_value is not None and device_value not in {
                event.device_identity.casefold(),
                event.device_name.casefold(),
                event.server_name.casefold(),
            }:
                continue
            if since_value is not None:
                event_time = _parse_timestamp(event.timestamp)
                if event_time is None or event_time < since_value:
                    continue
            matches.append(event)
            if limit is not None and len(matches) >= limit:
                break
        return matches

    def export(self, **filters) -> dict[str, Any]:
        events = [event.to_dict() for event in self.list_events(**filters)]
        return {
            "schema_version": EVENT_JOURNAL_SCHEMA_VERSION,
            "retention_limit": self.max_events,
            "count": len(events),
            "events": events,
        }

    def clear(self) -> int:
        count = len(self._events)
        if count == 0:
            return 0
        self._events.clear()
        self._persist()
        return count

    def record_operation_transition(
        self,
        device_or_snapshot,
        *,
        kind: MonitoringEventKind,
        operation_id: str,
        correlation_id: str,
        operation_name: str,
        lifecycle_phase: str,
        requested_values: Any = None,
        acknowledgement_result_code: int | None = None,
        transport: str | None = None,
        effective_values: Any = None,
        final_operation_state: str | None = None,
        persistence_request_acknowledgement: Any = None,
        persistence_confirmation: bool | None = None,
        evidence: Mapping[str, Any] | None = None,
        observation_source: str,
        derivation_status: DerivationStatus,
        parent_preset_run_id: str | None = None,
        interface_identity: str | None = None,
        channel_identity: str | None = None,
        flow_identity: str | None = None,
        severity: EventSeverity = EventSeverity.INFO,
        timestamp: str | None = None,
    ) -> MonitoringEvent:
        """Persist one meaningful configuration-operation lifecycle transition."""
        for label, value in (
            ("operation_id", operation_id),
            ("correlation_id", correlation_id),
            ("operation_name", operation_name),
            ("observation_source", observation_source),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{label} must be a non-empty string")
        lifecycle_phase = OperationLifecyclePhase(lifecycle_phase).value
        if parent_preset_run_id is not None and (not isinstance(parent_preset_run_id, str) or not parent_preset_run_id):
            raise ValueError("parent_preset_run_id must be a non-empty string or null")
        if acknowledgement_result_code is not None and (
            isinstance(acknowledgement_result_code, bool) or not isinstance(acknowledgement_result_code, int)
        ):
            raise ValueError("acknowledgement_result_code must be an integer or null")
        if persistence_confirmation is not None and not isinstance(persistence_confirmation, bool):
            raise ValueError("persistence_confirmation must be Boolean or null")
        if isinstance(device_or_snapshot, Mapping):
            snapshot = {
                "device_identity": device_or_snapshot.get("device_identity"),
                "server_name": device_or_snapshot.get("server_name"),
                "name": device_or_snapshot.get("name"),
                "inventory_id": device_or_snapshot.get("inventory_id"),
                "mac_address": device_or_snapshot.get("mac_address"),
            }
        else:
            snapshot = {
                "server_name": getattr(device_or_snapshot, "server_name", None),
                "name": getattr(device_or_snapshot, "name", None),
                "inventory_id": getattr(device_or_snapshot, "inventory_id", None),
                "mac_address": getattr(device_or_snapshot, "mac_address", None),
            }
        snapshot["device_identity"] = _device_identity(snapshot)
        current_value = {
            "operation": operation_name,
            "phase": lifecycle_phase,
            "state": final_operation_state,
        }
        event = self._append(
            snapshot,
            timestamp or self._timestamp(),
            kind,
            severity,
            None,
            current_value,
            evidence or {},
            observation_source,
            derivation_status,
            interface_identity=interface_identity,
            channel_identity=channel_identity,
            flow_identity=flow_identity,
            operation_id=operation_id,
            correlation_id=correlation_id,
            parent_preset_run_id=parent_preset_run_id,
            operation_name=operation_name,
            lifecycle_phase=lifecycle_phase,
            requested_values=requested_values,
            acknowledgement_result_code=acknowledgement_result_code,
            transport=transport,
            effective_values=effective_values,
            final_operation_state=final_operation_state,
            persistence_request_acknowledgement=persistence_request_acknowledgement,
            persistence_confirmation=persistence_confirmation,
        )
        self._persist()
        return event

    def _timestamp(self) -> str:
        value = self._wall_clock()
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")

    def _append(
        self,
        snapshot: Mapping[str, Any],
        timestamp: str,
        kind: MonitoringEventKind,
        severity: EventSeverity,
        previous_value: Any,
        current_value: Any,
        raw: Mapping[str, Any],
        observation_source: str,
        derivation_status: DerivationStatus,
        *,
        interface_identity: str | None = None,
        channel_identity: str | None = None,
        flow_identity: str | None = None,
        operation_id: str | None = None,
        correlation_id: str | None = None,
        parent_preset_run_id: str | None = None,
        operation_name: str | None = None,
        lifecycle_phase: str | None = None,
        requested_values: Any = None,
        acknowledgement_result_code: int | None = None,
        transport: str | None = None,
        effective_values: Any = None,
        final_operation_state: str | None = None,
        persistence_request_acknowledgement: Any = None,
        persistence_confirmation: bool | None = None,
    ) -> MonitoringEvent:
        context = _observation_context(snapshot)
        raw_evidence = dict(raw)
        if context:
            raw_evidence["observation_context"] = context
        event = MonitoringEvent(
            sequence=self._next_sequence,
            timestamp=timestamp,
            kind=kind,
            severity=severity,
            device_identity=str(snapshot["device_identity"]),
            device_name=str(snapshot.get("name") or ""),
            server_name=str(snapshot.get("server_name") or ""),
            interface_identity=interface_identity,
            channel_identity=channel_identity,
            flow_identity=flow_identity,
            previous_value=_json_safe(previous_value),
            current_value=_json_safe(current_value),
            raw=_json_safe(raw_evidence),
            observation_source=observation_source,
            derivation_status=derivation_status,
            operation_id=operation_id,
            correlation_id=correlation_id,
            parent_preset_run_id=parent_preset_run_id,
            operation_name=operation_name,
            lifecycle_phase=lifecycle_phase,
            requested_values=_json_safe(requested_values),
            acknowledgement_result_code=acknowledgement_result_code,
            transport=transport,
            effective_values=_json_safe(effective_values),
            final_operation_state=final_operation_state,
            persistence_request_acknowledgement=_json_safe(persistence_request_acknowledgement),
            persistence_confirmation=persistence_confirmation,
        )
        self._next_sequence += 1
        self._events.append(event)
        return event

    def _append_issue_transitions(
        self,
        transitions: list[IssueTransition],
        generated: list[MonitoringEvent],
    ) -> None:
        kinds = {
            IssueTransitionKind.OPENED: MonitoringEventKind.ISSUE_OPENED,
            IssueTransitionKind.UPDATED: MonitoringEventKind.ISSUE_UPDATED,
            IssueTransitionKind.RESOLVED: MonitoringEventKind.ISSUE_RESOLVED,
        }
        for transition in transitions:
            issue = transition.current
            snapshot = {
                "device_identity": issue.scope.device_identity,
                "name": issue.scope.device_name,
                "server_name": issue.scope.server_name,
            }
            generated.append(
                self._append(
                    snapshot,
                    issue.resolved_at or issue.last_seen,
                    kinds[transition.kind],
                    EventSeverity.INFO if transition.kind is IssueTransitionKind.RESOLVED else issue.severity,
                    transition.previous.to_dict() if transition.previous is not None else None,
                    issue.to_dict(),
                    {
                        "issue_id": issue.issue_id,
                        "issue_kind": issue.kind.value,
                        "evidence_class": issue.evidence_class.value,
                        "raw_source_fields": issue.raw_source_fields,
                        "suggested_remediation": issue.suggested_remediation,
                    },
                    issue.evidence_source,
                    (
                        DerivationStatus.OBSERVED
                        if issue.evidence_class is IssueEvidenceClass.DIRECT_OBSERVATION
                        else DerivationStatus.DERIVED
                    ),
                    interface_identity=issue.scope.interface_identity,
                    channel_identity=issue.scope.channel_identity,
                    flow_identity=issue.scope.flow_identity,
                )
            )

    def _observe_presence(self, previous, current, timestamp, generated) -> None:
        if previous.get("online") is False and current.get("online") is True:
            generated.append(
                self._append(
                    current,
                    timestamp,
                    MonitoringEventKind.DEVICE_REAPPEARED,
                    EventSeverity.INFO,
                    False,
                    True,
                    {
                        "previous_observation": self._presence_evidence(previous),
                        "current_observation": self._presence_evidence(current),
                    },
                    "device_lifecycle",
                    DerivationStatus.OBSERVED,
                )
            )

    @staticmethod
    def _presence_evidence(snapshot) -> dict[str, Any] | None:
        if snapshot is None:
            return None
        return {
            "online": snapshot.get("online"),
            "availability_state": snapshot.get("availability_state"),
            "last_seen": snapshot.get("last_seen"),
        }

    def _observe_clock(self, previous, current, timestamp, generated) -> None:
        for field, kind, severity in (
            ("clock_role", MonitoringEventKind.CLOCK_ROLE_CHANGED, EventSeverity.INFO),
            ("leader_clock_identity", MonitoringEventKind.LEADER_IDENTITY_CHANGED, EventSeverity.WARNING),
        ):
            before = previous.get(field)
            after = current.get(field)
            if before is None or after is None or before == after:
                continue
            generated.append(
                self._append(
                    current,
                    timestamp,
                    kind,
                    severity,
                    before,
                    after,
                    {
                        "previous_clock_state": _clock_evidence(previous),
                        "current_clock_state": _clock_evidence(current),
                    },
                    "device_clock_status",
                    DerivationStatus.OBSERVED,
                )
            )

    def _observe_ptp_ports(self, previous, current, timestamp, generated) -> None:
        before_ports = _ptp_port_map(previous.get("clock_port_records"))
        after_ports = _ptp_port_map(current.get("clock_port_records"))
        for identity in sorted(before_ports.keys() & after_ports.keys()):
            before = _ptp_state(before_ports[identity])
            after = _ptp_state(after_ports[identity])
            if before.get("state_code") is None or after.get("state_code") is None or before == after:
                continue
            generated.append(
                self._append(
                    current,
                    timestamp,
                    MonitoringEventKind.PTP_PORT_STATE_CHANGED,
                    EventSeverity.WARNING if after.get("link_down") is True else EventSeverity.INFO,
                    before,
                    after,
                    {
                        "previous_port_record": before_ports[identity],
                        "current_port_record": after_ports[identity],
                    },
                    "device_clock_status",
                    DerivationStatus.OBSERVED,
                    interface_identity=identity,
                )
            )

    def _observe_mutes(self, previous, current, timestamp, generated) -> None:
        before_channels = _channel_map(previous.get("channels"))
        after_channels = _channel_map(current.get("channels"))
        for identity in sorted(before_channels.keys() & after_channels.keys()):
            before = before_channels[identity].get("muted")
            after = after_channels[identity].get("muted")
            if not isinstance(before, bool) or not isinstance(after, bool) or before == after:
                continue
            generated.append(
                self._append(
                    current,
                    timestamp,
                    MonitoringEventKind.MUTE_STATE_CHANGED,
                    EventSeverity.WARNING if after else EventSeverity.INFO,
                    before,
                    after,
                    {
                        "previous_channel": before_channels[identity],
                        "current_channel": after_channels[identity],
                    },
                    "device_channel_status",
                    DerivationStatus.OBSERVED,
                    channel_identity=identity,
                )
            )

    def _observe_subscriptions(self, previous, current, timestamp, generated) -> None:
        before_subscriptions = _subscription_map(previous.get("subscriptions"))
        after_subscriptions = _subscription_map(current.get("subscriptions"))
        for identity in sorted(before_subscriptions.keys() & after_subscriptions.keys()):
            before = before_subscriptions[identity]
            after = after_subscriptions[identity]
            before_state = _subscription_failure_state(before)
            after_state = _subscription_failure_state(after)
            if before_state is None or after_state is None or before_state == after_state:
                continue
            if after_state:
                status = after.get("status") or {}
                severity = EventSeverity.ERROR if status.get("severity") == "error" else EventSeverity.WARNING
                kind = MonitoringEventKind.SUBSCRIPTION_FAILED
            else:
                severity = EventSeverity.INFO
                kind = MonitoringEventKind.SUBSCRIPTION_RECOVERED
            generated.append(
                self._append(
                    current,
                    timestamp,
                    kind,
                    severity,
                    before.get("status"),
                    after.get("status"),
                    {"previous_subscription": before, "current_subscription": after},
                    "receiver_subscription_status",
                    DerivationStatus.OBSERVED,
                    channel_identity=identity,
                )
            )

    def _observe_connection_health(self, previous, current, timestamp, generated) -> None:
        before_health = previous.get("receiver_flow_connection_health")
        after_health = current.get("receiver_flow_connection_health")
        before_flows = _flow_map(before_health)
        after_flows = _flow_map(after_health)
        late_stream = after_health.get("late_packet_stream") if isinstance(after_health, dict) else None
        late_stream_fresh = isinstance(late_stream, dict) and late_stream.get("fresh") is True
        for identity in sorted(before_flows.keys() & after_flows.keys()):
            before_flow = before_flows[identity]
            after_flow = after_flows[identity]
            if late_stream_fresh:
                self._observe_counter(
                    current,
                    timestamp,
                    generated,
                    identity=identity,
                    identity_keyword="flow_identity",
                    previous_measurement=before_flow,
                    current_measurement=after_flow,
                    field="late_packet_count",
                    increase_kind=MonitoringEventKind.LATE_PACKET_COUNT_INCREASED,
                    reset_kind=MonitoringEventKind.LATE_PACKET_COUNTER_RESET,
                    source="heartbeat_receiver_flow_late_packets",
                )
        self._observe_flow_latency(current, after_health, timestamp, generated)

    def _observe_flow_latency(self, current, health, timestamp, generated) -> None:
        if not isinstance(health, dict):
            return
        latency_stream = health.get("latency_stream")
        if not isinstance(latency_stream, dict) or latency_stream.get("fresh") is not True:
            return
        for identity, flow in _flow_map(health).items():
            latency = flow.get("current_latency_nanoseconds")
            configured = _configured_flow_latency(current, flow)
            if not _unsigned_integer(latency) or not _positive_integer(configured):
                continue
            warning = round(configured * self.thresholds.flow_latency_warning_ratio)
            recovery = round(configured * self.thresholds.flow_latency_recovery_ratio)
            condition_key = (str(current["device_identity"]), "flow_latency", identity)
            previous_condition = self._conditions.get(condition_key)
            if previous_condition is None:
                self._conditions[condition_key] = latency >= warning
                continue
            next_condition = previous_condition
            kind = None
            if not previous_condition and latency >= warning:
                next_condition = True
                kind = MonitoringEventKind.RECEIVER_FLOW_LATENCY_HIGH
            elif previous_condition and latency <= recovery:
                next_condition = False
                kind = MonitoringEventKind.RECEIVER_FLOW_LATENCY_RECOVERED
            self._conditions[condition_key] = next_condition
            if kind is None:
                continue
            generated.append(
                self._append(
                    current,
                    timestamp,
                    kind,
                    EventSeverity.WARNING if next_condition else EventSeverity.INFO,
                    previous_condition,
                    next_condition,
                    {
                        "flow_measurement": flow,
                        "configured_latency_nanoseconds": configured,
                        "warning_threshold_nanoseconds": warning,
                        "recovery_threshold_nanoseconds": recovery,
                        "latency_stream": health.get("latency_stream"),
                    },
                    "heartbeat_receiver_flow_latency",
                    DerivationStatus.DERIVED,
                    flow_identity=identity,
                )
            )

    def _observe_interface_traffic(self, previous, current, timestamp, generated) -> None:
        before_traffic = previous.get("network_interface_traffic")
        after_traffic = current.get("network_interface_traffic")
        before_interfaces = _interface_map(before_traffic)
        after_interfaces = _interface_map(after_traffic)
        for identity in sorted(before_interfaces.keys() & after_interfaces.keys()):
            before_interface = before_interfaces[identity]
            after_interface = after_interfaces[identity]
            for field in ("transmit_error_count", "receive_error_count"):
                self._observe_counter(
                    current,
                    timestamp,
                    generated,
                    identity=identity,
                    identity_keyword="interface_identity",
                    previous_measurement=before_interface,
                    current_measurement=after_interface,
                    field=field,
                    increase_kind=MonitoringEventKind.INTERFACE_ERROR_COUNTER_INCREASED,
                    reset_kind=MonitoringEventKind.INTERFACE_ERROR_COUNTER_RESET,
                    source="heartbeat_interface_statistics",
                )
        self._observe_interface_utilization(current, after_traffic, timestamp, generated)

    def _observe_interface_utilization(self, current, traffic, timestamp, generated) -> None:
        link_speed_mbps = current.get("link_speed_mbps")
        if not _positive_number(link_speed_mbps):
            return
        capacity = float(link_speed_mbps) * 1_000_000
        for identity, interface in _interface_map(traffic).items():
            transmit = interface.get("transmit_rate_bits_per_second")
            receive = interface.get("receive_rate_bits_per_second")
            if not _unsigned_number(transmit) or not _unsigned_number(receive):
                continue
            utilization = max(float(transmit), float(receive)) / capacity * 100
            condition_key = (str(current["device_identity"]), "interface_utilization", identity)
            previous_condition = self._conditions.get(condition_key)
            if previous_condition is None:
                self._conditions[condition_key] = utilization >= self.thresholds.interface_utilization_warning_percent
                continue
            next_condition = previous_condition
            kind = None
            if not previous_condition and utilization >= self.thresholds.interface_utilization_warning_percent:
                next_condition = True
                kind = MonitoringEventKind.INTERFACE_UTILIZATION_HIGH
            elif previous_condition and utilization <= self.thresholds.interface_utilization_recovery_percent:
                next_condition = False
                kind = MonitoringEventKind.INTERFACE_UTILIZATION_RECOVERED
            self._conditions[condition_key] = next_condition
            if kind is None:
                continue
            generated.append(
                self._append(
                    current,
                    timestamp,
                    kind,
                    EventSeverity.WARNING if next_condition else EventSeverity.INFO,
                    previous_condition,
                    next_condition,
                    {
                        "interface_measurement": interface,
                        "link_speed_mbps": link_speed_mbps,
                        "utilization_percent": utilization,
                        "warning_threshold_percent": self.thresholds.interface_utilization_warning_percent,
                        "recovery_threshold_percent": self.thresholds.interface_utilization_recovery_percent,
                        "traffic_sequence": traffic.get("sequence") if isinstance(traffic, dict) else None,
                    },
                    "heartbeat_interface_statistics",
                    DerivationStatus.DERIVED,
                    interface_identity=identity,
                )
            )

    def _observe_counter(
        self,
        snapshot,
        timestamp,
        generated,
        *,
        identity,
        identity_keyword,
        previous_measurement,
        current_measurement,
        field,
        increase_kind,
        reset_kind,
        source,
    ) -> None:
        before = previous_measurement.get(field)
        after = current_measurement.get(field)
        if not _unsigned_integer(before) or not _unsigned_integer(after) or before == after:
            return
        reset = after < before
        raw = {
            "counter": field,
            "previous_measurement": previous_measurement,
            "current_measurement": current_measurement,
            "delta": None if reset else after - before,
        }
        generated.append(
            self._append(
                snapshot,
                timestamp,
                reset_kind if reset else increase_kind,
                EventSeverity.INFO if reset else EventSeverity.WARNING,
                before,
                after,
                raw,
                source,
                DerivationStatus.OBSERVED,
                **{identity_keyword: identity},
            )
        )

    def _prime_conditions(self, snapshot) -> None:
        health = snapshot.get("receiver_flow_connection_health")
        for identity, flow in _flow_map(health).items():
            latency = flow.get("current_latency_nanoseconds")
            configured = _configured_flow_latency(snapshot, flow)
            if _unsigned_integer(latency) and _positive_integer(configured):
                warning = round(configured * self.thresholds.flow_latency_warning_ratio)
                self._conditions[(str(snapshot["device_identity"]), "flow_latency", identity)] = latency >= warning
        link_speed_mbps = snapshot.get("link_speed_mbps")
        if not _positive_number(link_speed_mbps):
            return
        capacity = float(link_speed_mbps) * 1_000_000
        for identity, interface in _interface_map(snapshot.get("network_interface_traffic")).items():
            transmit = interface.get("transmit_rate_bits_per_second")
            receive = interface.get("receive_rate_bits_per_second")
            if _unsigned_number(transmit) and _unsigned_number(receive):
                utilization = max(float(transmit), float(receive)) / capacity * 100
                self._conditions[(str(snapshot["device_identity"]), "interface_utilization", identity)] = (
                    utilization >= self.thresholds.interface_utilization_warning_percent
                )

    def _load(self) -> None:
        if self.path is None:
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if not isinstance(payload, dict) or payload.get("schema_version") != EVENT_JOURNAL_SCHEMA_VERSION:
            return
        self.issue_engine.load_payload(payload.get("issues"))
        raw_events = payload.get("events")
        if not isinstance(raw_events, list):
            return
        loaded = []
        for raw_event in raw_events:
            if not isinstance(raw_event, dict):
                continue
            try:
                loaded.append(MonitoringEvent.from_dict(raw_event))
            except (KeyError, TypeError, ValueError):
                continue
        loaded.sort(key=lambda event: event.sequence)
        self._events.extend(loaded[-self.max_events :])
        highest_sequence = max((event.sequence for event in self._events), default=0)
        raw_next_sequence = payload.get("next_sequence")
        if isinstance(raw_next_sequence, int) and not isinstance(raw_next_sequence, bool):
            self._next_sequence = max(highest_sequence + 1, raw_next_sequence, 1)
        else:
            self._next_sequence = highest_sequence + 1

    def _persist(self) -> None:
        if self.path is None:
            return
        payload = {
            "schema_version": EVENT_JOURNAL_SCHEMA_VERSION,
            "retention_limit": self.max_events,
            "next_sequence": self._next_sequence,
            "events": [event.to_dict() for event in self._events],
            "issues": self.issue_engine.persistence_payload(),
        }
        temporary = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary = tempfile.mkstemp(dir=self.path.parent, prefix=".event-journal-")
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        except OSError as exception:
            logger.warning("Could not save event journal: %s", exception)
        finally:
            if temporary is not None and os.path.exists(temporary):
                os.unlink(temporary)
