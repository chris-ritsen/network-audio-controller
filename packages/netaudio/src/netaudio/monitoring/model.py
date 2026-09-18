from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


EVENT_JOURNAL_SCHEMA_VERSION = 1
DEFAULT_EVENT_HISTORY_LIMIT = 1000
DEFAULT_FLOW_LATENCY_WARNING_RATIO = 0.8
DEFAULT_FLOW_LATENCY_RECOVERY_RATIO = 0.6
DEFAULT_INTERFACE_UTILIZATION_WARNING_PERCENT = 80.0
DEFAULT_INTERFACE_UTILIZATION_RECOVERY_PERCENT = 60.0


class EventSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class DerivationStatus(str, Enum):
    OBSERVED = "observed"
    DERIVED = "derived"


class OperationLifecyclePhase(str, Enum):
    REQUESTED = "requested"
    REQUEST_ACKNOWLEDGED = "request_acknowledged"
    REQUEST_REJECTED = "request_rejected"
    EFFECTIVE_STATE_CONFIRMED = "effective_state_confirmed"
    PARTIAL_UNOBSERVABLE = "partial_unobservable"
    INCONSISTENT = "inconsistent"
    TRANSPORT_OR_VALIDATION_FAILURE = "transport_or_validation_failure"
    PERSISTENCE_REQUEST_ACKNOWLEDGED = "persistence_request_acknowledged"
    PERSISTENCE_CONFIRMED = "persistence_confirmed"


class MonitoringEventKind(str, Enum):
    CLOCK_ROLE_CHANGED = "clock_role_changed"
    CLOCK_STATUS_CHANGED = "clock_status_changed"
    DEVICE_DISAPPEARED = "device_disappeared"
    DEVICE_REAPPEARED = "device_reappeared"
    INTERFACE_ERROR_COUNTER_INCREASED = "interface_error_counter_increased"
    INTERFACE_ERROR_COUNTER_RESET = "interface_error_counter_reset"
    INTERFACE_UTILIZATION_HIGH = "interface_utilization_high"
    INTERFACE_UTILIZATION_RECOVERED = "interface_utilization_recovered"
    ISSUE_OPENED = "issue_opened"
    ISSUE_RESOLVED = "issue_resolved"
    ISSUE_UPDATED = "issue_updated"
    LEADER_IDENTITY_CHANGED = "leader_identity_changed"
    LATE_PACKET_COUNT_INCREASED = "late_packet_count_increased"
    LATE_PACKET_COUNTER_RESET = "late_packet_counter_reset"
    MUTE_STATE_CHANGED = "mute_state_changed"
    PTP_PORT_STATE_CHANGED = "ptp_port_state_changed"
    RECEIVER_FLOW_LATENCY_HIGH = "receiver_flow_latency_high"
    RECEIVER_FLOW_LATENCY_RECOVERED = "receiver_flow_latency_recovered"
    SUBSCRIPTION_FAILED = "subscription_failed"
    SUBSCRIPTION_RECOVERED = "subscription_recovered"
    CONFIGURATION_OPERATION = "configuration_operation"
    PRESET_RUN = "preset_run"


@dataclass(frozen=True)
class EventJournalThresholds:
    flow_latency_warning_ratio: float = DEFAULT_FLOW_LATENCY_WARNING_RATIO
    flow_latency_recovery_ratio: float = DEFAULT_FLOW_LATENCY_RECOVERY_RATIO
    interface_utilization_warning_percent: float = DEFAULT_INTERFACE_UTILIZATION_WARNING_PERCENT
    interface_utilization_recovery_percent: float = DEFAULT_INTERFACE_UTILIZATION_RECOVERY_PERCENT

    def __post_init__(self) -> None:
        if not 0 < self.flow_latency_recovery_ratio < self.flow_latency_warning_ratio <= 1:
            raise ValueError("flow latency ratios must satisfy 0 < recovery < warning <= 1")
        if not (0 <= self.interface_utilization_recovery_percent < self.interface_utilization_warning_percent <= 100):
            raise ValueError("interface utilization percentages must satisfy 0 <= recovery < warning <= 100")

    @classmethod
    def from_daemon_config(cls, daemon_config: Mapping[str, Any]) -> EventJournalThresholds:
        return cls(
            flow_latency_warning_ratio=_finite_number(
                daemon_config.get("event_flow_latency_warning_ratio"),
                DEFAULT_FLOW_LATENCY_WARNING_RATIO,
                "daemon.event_flow_latency_warning_ratio",
            ),
            flow_latency_recovery_ratio=_finite_number(
                daemon_config.get("event_flow_latency_recovery_ratio"),
                DEFAULT_FLOW_LATENCY_RECOVERY_RATIO,
                "daemon.event_flow_latency_recovery_ratio",
            ),
            interface_utilization_warning_percent=_finite_number(
                daemon_config.get("event_interface_utilization_warning_percent"),
                DEFAULT_INTERFACE_UTILIZATION_WARNING_PERCENT,
                "daemon.event_interface_utilization_warning_percent",
            ),
            interface_utilization_recovery_percent=_finite_number(
                daemon_config.get("event_interface_utilization_recovery_percent"),
                DEFAULT_INTERFACE_UTILIZATION_RECOVERY_PERCENT,
                "daemon.event_interface_utilization_recovery_percent",
            ),
        )


@dataclass(frozen=True)
class MonitoringEvent:
    sequence: int
    timestamp: str
    kind: MonitoringEventKind
    severity: EventSeverity
    device_identity: str
    device_name: str
    server_name: str
    previous_value: Any
    current_value: Any
    raw: dict[str, Any]
    observation_source: str
    derivation_status: DerivationStatus
    interface_identity: str | None = None
    channel_identity: str | None = None
    flow_identity: str | None = None
    operation_id: str | None = None
    correlation_id: str | None = None
    parent_preset_run_id: str | None = None
    operation_name: str | None = None
    lifecycle_phase: str | None = None
    requested_values: Any = None
    acknowledgement_result_code: int | None = None
    transport: str | None = None
    effective_values: Any = None
    final_operation_state: str | None = None
    persistence_request_acknowledgement: Any = None
    persistence_confirmation: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "kind": self.kind.value,
            "severity": self.severity.value,
            "device_identity": self.device_identity,
            "device_name": self.device_name,
            "server_name": self.server_name,
            "interface_identity": self.interface_identity,
            "channel_identity": self.channel_identity,
            "flow_identity": self.flow_identity,
            "previous_value": _json_safe(self.previous_value),
            "current_value": _json_safe(self.current_value),
            "raw": _json_safe(self.raw),
            "observation_source": self.observation_source,
            "derivation_status": self.derivation_status.value,
            "operation_id": self.operation_id,
            "correlation_id": self.correlation_id,
            "parent_preset_run_id": self.parent_preset_run_id,
            "operation_name": self.operation_name,
            "lifecycle_phase": self.lifecycle_phase,
            "requested_values": _json_safe(self.requested_values),
            "acknowledgement_result_code": self.acknowledgement_result_code,
            "transport": self.transport,
            "effective_values": _json_safe(self.effective_values),
            "final_operation_state": self.final_operation_state,
            "persistence_request_acknowledgement": _json_safe(self.persistence_request_acknowledgement),
            "persistence_confirmation": self.persistence_confirmation,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> MonitoringEvent:
        sequence = value.get("sequence")
        timestamp = value.get("timestamp")
        device_identity = value.get("device_identity")
        observation_source = value.get("observation_source")
        raw = value.get("raw")
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
            raise ValueError("event sequence must be a positive integer")
        if not isinstance(timestamp, str) or _parse_timestamp(timestamp) is None:
            raise ValueError("event timestamp must be an ISO-8601 timestamp")
        if not isinstance(device_identity, str) or not device_identity:
            raise ValueError("event device_identity must be a non-empty string")
        if not isinstance(observation_source, str) or not observation_source:
            raise ValueError("event observation_source must be a non-empty string")
        if not isinstance(raw, dict):
            raise ValueError("event raw evidence must be an object")
        return cls(
            sequence=sequence,
            timestamp=timestamp,
            kind=MonitoringEventKind(value["kind"]),
            severity=EventSeverity(value["severity"]),
            device_identity=device_identity,
            device_name=str(value.get("device_name") or ""),
            server_name=str(value.get("server_name") or ""),
            interface_identity=_optional_string(value.get("interface_identity")),
            channel_identity=_optional_string(value.get("channel_identity")),
            flow_identity=_optional_string(value.get("flow_identity")),
            previous_value=_json_safe(value.get("previous_value")),
            current_value=_json_safe(value.get("current_value")),
            raw=_json_safe(raw),
            observation_source=observation_source,
            derivation_status=DerivationStatus(value["derivation_status"]),
            operation_id=_optional_string(value.get("operation_id")),
            correlation_id=_optional_string(value.get("correlation_id")),
            parent_preset_run_id=_optional_string(value.get("parent_preset_run_id")),
            operation_name=_optional_string(value.get("operation_name")),
            lifecycle_phase=_optional_string(value.get("lifecycle_phase")),
            requested_values=_json_safe(value.get("requested_values")),
            acknowledgement_result_code=_optional_integer(value.get("acknowledgement_result_code")),
            transport=_optional_string(value.get("transport")),
            effective_values=_json_safe(value.get("effective_values")),
            final_operation_state=_optional_string(value.get("final_operation_state")),
            persistence_request_acknowledgement=_json_safe(value.get("persistence_request_acknowledgement")),
            persistence_confirmation=_optional_boolean(value.get("persistence_confirmation")),
        )


def _finite_number(value: Any, default: float, label: str) -> float:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{label} must be a finite number")
    return float(value)


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_integer(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _optional_boolean(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (bytes, bytearray)):
        return list(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_json_safe(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
    return str(value)
