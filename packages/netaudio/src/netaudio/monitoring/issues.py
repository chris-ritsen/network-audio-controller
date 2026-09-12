from __future__ import annotations

import hashlib
import ipaddress
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any

from netaudio.monitoring.model import EventSeverity, _json_safe
from netaudio.monitoring.signals import _device_identity, _flow_map, _subscription_failure_state, _subscription_map

ISSUE_SCHEMA_VERSION = 1


class IssueKind(str, Enum):
    ADDRESS_CONFLICT = "address_conflict"
    SUBNET_CONFLICT = "subnet_conflict"
    CLOCK_SYNCHRONIZATION = "clock_synchronization"
    PULLUP_MISMATCH = "pullup_mismatch"
    SUBSCRIPTION_FAILURE = "subscription_failure"
    RECEIVER_HEALTH_DEGRADED = "receiver_health_degraded"
    LICENSING_FAILURE = "licensing_failure"
    SAFE_STATE = "safe_state"
    UPGRADE_REQUIRED = "upgrade_required"
    REBOOT_REQUIRED = "reboot_required"
    CONFIGURATION_PARTITION_FAULT = "configuration_partition_fault"
    PERSISTENCE_FAULT = "persistence_fault"
    TELEMETRY_STALE = "telemetry_stale"
    TELEMETRY_MISSING = "telemetry_missing"
    CONFIGURATION_DIVERGENCE = "configuration_divergence"


class IssueLifecycleState(str, Enum):
    OPEN = "open"
    RESOLVED = "resolved"


class IssueObservationState(str, Enum):
    OBSERVED = "observed"
    UNOBSERVABLE = "unobservable"


class IssueEvidenceClass(str, Enum):
    DIRECT_OBSERVATION = "direct_observation"
    DERIVED_STATE = "derived_state"
    INFERENCE = "inference"


class IssueTransitionKind(str, Enum):
    OPENED = "opened"
    UPDATED = "updated"
    RESOLVED = "resolved"


@dataclass(frozen=True)
class IssueScope:
    device_identity: str
    device_name: str
    server_name: str
    interface_identity: str | None = None
    channel_identity: str | None = None
    flow_identity: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "device_identity": self.device_identity,
            "device_name": self.device_name,
            "server_name": self.server_name,
            "interface_identity": self.interface_identity,
            "channel_identity": self.channel_identity,
            "flow_identity": self.flow_identity,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> IssueScope:
        identity = value.get("device_identity")
        if not isinstance(identity, str) or not identity:
            raise ValueError("issue scope requires device_identity")
        return cls(
            device_identity=identity,
            device_name=str(value.get("device_name") or ""),
            server_name=str(value.get("server_name") or ""),
            interface_identity=_optional_text(value.get("interface_identity")),
            channel_identity=_optional_text(value.get("channel_identity")),
            flow_identity=_optional_text(value.get("flow_identity")),
        )


@dataclass(frozen=True)
class MonitoringIssue:
    issue_id: str
    kind: IssueKind
    severity: EventSeverity
    state: IssueLifecycleState
    first_seen: str
    last_seen: str
    resolved_at: str | None
    occurrence_count: int
    observation_state: IssueObservationState
    scope: IssueScope
    title: str
    summary: str
    raw_source_fields: dict[str, Any]
    evidence_source: str
    evidence_class: IssueEvidenceClass
    suggested_remediation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": ISSUE_SCHEMA_VERSION,
            "issue_id": self.issue_id,
            "kind": self.kind.value,
            "severity": self.severity.value,
            "state": self.state.value,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "resolved_at": self.resolved_at,
            "occurrence_count": self.occurrence_count,
            "observation_state": self.observation_state.value,
            "scope": self.scope.to_dict(),
            "title": self.title,
            "summary": self.summary,
            "raw_source_fields": _json_safe(self.raw_source_fields),
            "evidence_source": self.evidence_source,
            "evidence_class": self.evidence_class.value,
            "suggested_remediation": self.suggested_remediation,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> MonitoringIssue:
        if value.get("schema_version", ISSUE_SCHEMA_VERSION) != ISSUE_SCHEMA_VERSION:
            raise ValueError("unsupported issue schema version")
        count = value.get("occurrence_count")
        raw = value.get("raw_source_fields")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError("issue occurrence_count must be positive")
        if not isinstance(raw, dict):
            raise ValueError("issue raw_source_fields must be an object")
        return cls(
            issue_id=_required_text(value.get("issue_id"), "issue_id"),
            kind=IssueKind(value["kind"]),
            severity=EventSeverity(value["severity"]),
            state=IssueLifecycleState(value["state"]),
            first_seen=_required_text(value.get("first_seen"), "first_seen"),
            last_seen=_required_text(value.get("last_seen"), "last_seen"),
            resolved_at=_optional_text(value.get("resolved_at")),
            occurrence_count=count,
            observation_state=IssueObservationState(
                value.get("observation_state", IssueObservationState.OBSERVED.value)
            ),
            scope=IssueScope.from_dict(value["scope"]),
            title=_required_text(value.get("title"), "title"),
            summary=_required_text(value.get("summary"), "summary"),
            raw_source_fields=_json_safe(raw),
            evidence_source=_required_text(value.get("evidence_source"), "evidence_source"),
            evidence_class=IssueEvidenceClass(value["evidence_class"]),
            suggested_remediation=_required_text(value.get("suggested_remediation"), "suggested_remediation"),
        )

    def material_signature(self) -> tuple[Any, ...]:
        return (
            self.kind,
            self.severity,
            self.observation_state,
            self.scope,
            self.title,
            self.summary,
            _stable_json(self.raw_source_fields),
            self.evidence_source,
            self.evidence_class,
            self.suggested_remediation,
        )


@dataclass(frozen=True)
class IssueTransition:
    kind: IssueTransitionKind
    previous: MonitoringIssue | None
    current: MonitoringIssue


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"issue {label} must be a non-empty string")
    return value


def _optional_text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _stable_json(value: Any) -> str:
    import json

    return json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"))


def _issue_id(kind: IssueKind, scope: IssueScope) -> str:
    material = "|".join(
        (
            kind.value,
            scope.device_identity,
            scope.interface_identity or "",
            scope.channel_identity or "",
            scope.flow_identity or "",
        )
    )
    return f"{kind.value}:{hashlib.sha256(material.encode()).hexdigest()[:20]}"


def _scope(snapshot: Mapping[str, Any], **identities: str | None) -> IssueScope:
    return IssueScope(
        device_identity=str(snapshot["device_identity"]),
        device_name=str(snapshot.get("name") or ""),
        server_name=str(snapshot.get("server_name") or ""),
        interface_identity=identities.get("interface_identity"),
        channel_identity=identities.get("channel_identity"),
        flow_identity=identities.get("flow_identity"),
    )


def _candidate(
    snapshot: Mapping[str, Any],
    timestamp: str,
    kind: IssueKind,
    severity: EventSeverity,
    title: str,
    summary: str,
    raw: Mapping[str, Any],
    source: str,
    evidence_class: IssueEvidenceClass,
    remediation: str,
    **identities: str | None,
) -> MonitoringIssue:
    scope = _scope(snapshot, **identities)
    return MonitoringIssue(
        issue_id=_issue_id(kind, scope),
        kind=kind,
        severity=severity,
        state=IssueLifecycleState.OPEN,
        first_seen=timestamp,
        last_seen=timestamp,
        resolved_at=None,
        occurrence_count=1,
        observation_state=IssueObservationState.OBSERVED,
        scope=scope,
        title=title,
        summary=summary,
        raw_source_fields=_json_safe(dict(raw)),
        evidence_source=source,
        evidence_class=evidence_class,
        suggested_remediation=remediation,
    )


def _interfaces(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    result = []
    for index, raw in enumerate(snapshot.get("interfaces") or []):
        if not isinstance(raw, dict):
            continue
        effective = raw.get("active") if isinstance(raw.get("active"), dict) else raw
        configured = raw.get("configured") if isinstance(raw.get("configured"), dict) else raw
        identity = str(raw.get("interface") or raw.get("identity") or index + 1)
        address = effective.get("ip_address") or effective.get("address")
        netmask = effective.get("netmask") or effective.get("subnet_mask")
        result.append(
            {
                "identity": identity,
                "address": address,
                "netmask": netmask,
                "gateway": configured.get("gateway"),
                "active": deepcopy(effective),
                "configured": deepcopy(configured),
                "raw": deepcopy(raw),
            }
        )
    return result


class IssueEngine:
    """Derive lifecycle-managed issues from the daemon's existing observations.

    ``occurrence_count`` is the number of relevant observations that supported
    the issue. ``last_seen`` is the timestamp of the newest such observation.
    Engine evaluations and unavailable observations do not advance either.
    """

    _CROSS_DEVICE_KINDS = frozenset({IssueKind.ADDRESS_CONFLICT, IssueKind.PULLUP_MISMATCH})

    def __init__(self, *, history_limit: int = 1000):
        if isinstance(history_limit, bool) or not isinstance(history_limit, int) or history_limit <= 0:
            raise ValueError("issue history_limit must be a positive integer")
        self.history_limit = history_limit
        self._snapshots: dict[str, dict[str, Any]] = {}
        self._active: dict[str, MonitoringIssue] = {}
        self._history: list[MonitoringIssue] = []
        self._revision = 0

    @property
    def revision(self) -> int:
        """Monotonic in-memory revision for durable issue-record mutations."""
        return self._revision

    def observe_snapshot(
        self,
        snapshot: Mapping[str, Any],
        *,
        timestamp: str,
        emit_transitions: bool = True,
    ) -> list[IssueTransition]:
        current = _json_safe(dict(snapshot))
        identity = current.get("device_identity") or _device_identity(current)
        current["device_identity"] = identity
        identity = str(identity)
        self._snapshots[identity] = current
        if current.get("online") is False:
            self._mark_unobservable(self._issues_depending_on(identity))
            return []

        local_candidates = {issue.issue_id: issue for issue in self._detect_device(current, timestamp)}
        local_issue_ids = {
            issue_id
            for issue_id, issue in self._active.items()
            if issue.scope.device_identity == identity and issue.kind not in self._CROSS_DEVICE_KINDS
        } | set(local_candidates)
        transitions = self._reconcile_subset(
            local_candidates,
            local_issue_ids,
            timestamp,
            emit_transitions,
        )

        cross_candidates = self._detect_cross_device(timestamp)
        affected_cross_ids = {
            issue_id for issue_id, issue in cross_candidates.items() if identity in self._cross_dependencies(issue)
        } | {
            issue_id
            for issue_id, issue in self._active.items()
            if issue.kind in self._CROSS_DEVICE_KINDS and identity in self._cross_dependencies(issue)
        }
        transitions.extend(
            self._reconcile_subset(
                {issue_id: issue for issue_id, issue in cross_candidates.items() if issue_id in affected_cross_ids},
                affected_cross_ids,
                timestamp,
                emit_transitions,
            )
        )
        return transitions

    def remove_snapshot(self, identity: str, *, timestamp: str, emit_transitions: bool = True) -> list[IssueTransition]:
        del timestamp, emit_transitions
        affected = self._issues_depending_on(identity)
        self._snapshots.pop(identity, None)
        self._mark_unobservable(affected)
        return []

    def list_issues(
        self,
        *,
        state: str | IssueLifecycleState | None = None,
        kind: str | IssueKind | None = None,
        severity: str | EventSeverity | None = None,
        device: str | None = None,
        limit: int | None = None,
    ) -> list[MonitoringIssue]:
        state_value = IssueLifecycleState(state).value if state is not None else None
        kind_value = IssueKind(kind).value if kind is not None else None
        severity_value = EventSeverity(severity).value if severity is not None else None
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer")
        values = [*self._active.values(), *self._history]
        values.sort(key=lambda issue: (issue.last_seen, issue.issue_id), reverse=True)
        needle = device.casefold() if isinstance(device, str) else None
        result = []
        for issue in values:
            if state_value is not None and issue.state.value != state_value:
                continue
            if kind_value is not None and issue.kind.value != kind_value:
                continue
            if severity_value is not None and issue.severity.value != severity_value:
                continue
            if needle is not None and needle not in {
                issue.scope.device_identity.casefold(),
                issue.scope.device_name.casefold(),
                issue.scope.server_name.casefold(),
            }:
                continue
            result.append(issue)
            if limit is not None and len(result) >= limit:
                break
        return result

    def export(self, **filters: Any) -> dict[str, Any]:
        issues = [issue.to_dict() for issue in self.list_issues(**filters)]
        return {
            "schema_version": ISSUE_SCHEMA_VERSION,
            "history_limit": self.history_limit,
            "count": len(issues),
            "active_count": sum(issue["state"] == IssueLifecycleState.OPEN.value for issue in issues),
            "issues": issues,
        }

    def persistence_payload(self) -> dict[str, Any]:
        return {
            "schema_version": ISSUE_SCHEMA_VERSION,
            "history_limit": self.history_limit,
            "active": [issue.to_dict() for issue in self._active.values()],
            "history": [issue.to_dict() for issue in self._history],
        }

    def load_payload(self, value: Any) -> None:
        if not isinstance(value, dict) or value.get("schema_version") != ISSUE_SCHEMA_VERSION:
            return
        active = self._load_records(value.get("active"), IssueLifecycleState.OPEN)
        history = self._load_records(value.get("history"), IssueLifecycleState.RESOLVED)
        self._active = {issue.issue_id: issue for issue in active}
        self._history = history[-self.history_limit :]
        self._revision = 0

    @staticmethod
    def _load_records(value: Any, state: IssueLifecycleState) -> list[MonitoringIssue]:
        result = []
        for raw in value if isinstance(value, list) else []:
            if not isinstance(raw, dict):
                continue
            try:
                issue = MonitoringIssue.from_dict(raw)
            except (KeyError, TypeError, ValueError):
                continue
            if issue.state is state:
                result.append(issue)
        return result

    def _reconcile_subset(
        self,
        candidates: dict[str, MonitoringIssue],
        relevant_issue_ids: set[str],
        timestamp: str,
        emit_transitions: bool,
    ) -> list[IssueTransition]:
        transitions = []
        for issue_id, candidate in candidates.items():
            if issue_id not in relevant_issue_ids:
                continue
            previous = self._active.get(issue_id)
            if previous is None:
                self._active[issue_id] = candidate
                self._revision += 1
                if emit_transitions:
                    transitions.append(IssueTransition(IssueTransitionKind.OPENED, None, candidate))
                continue
            current = replace(
                candidate,
                first_seen=previous.first_seen,
                last_seen=timestamp,
                occurrence_count=previous.occurrence_count + 1,
                observation_state=IssueObservationState.OBSERVED,
            )
            self._active[issue_id] = current
            self._revision += 1
            if emit_transitions and previous.material_signature() != current.material_signature():
                transitions.append(IssueTransition(IssueTransitionKind.UPDATED, previous, current))
        for issue_id in sorted(relevant_issue_ids - set(candidates)):
            if issue_id not in self._active:
                continue
            previous = self._active[issue_id]
            if not self._has_resolution_evidence(previous):
                self._mark_unobservable({issue_id})
                continue
            self._active.pop(issue_id)
            resolved = replace(
                previous,
                state=IssueLifecycleState.RESOLVED,
                observation_state=IssueObservationState.OBSERVED,
                resolved_at=timestamp,
            )
            self._history.append(resolved)
            self._history = self._history[-self.history_limit :]
            self._revision += 1
            if emit_transitions:
                transitions.append(IssueTransition(IssueTransitionKind.RESOLVED, previous, resolved))
        return transitions

    def _mark_unobservable(self, issue_ids: set[str]) -> None:
        for issue_id in issue_ids:
            previous = self._active.get(issue_id)
            if previous is None or previous.observation_state is IssueObservationState.UNOBSERVABLE:
                continue
            self._active[issue_id] = replace(previous, observation_state=IssueObservationState.UNOBSERVABLE)
            self._revision += 1

    def _issues_depending_on(self, identity: str) -> set[str]:
        return {
            issue_id
            for issue_id, issue in self._active.items()
            if issue.scope.device_identity == identity or identity in self._cross_dependencies(issue)
        }

    @staticmethod
    def _cross_dependencies(issue: MonitoringIssue) -> set[str]:
        if issue.kind is IssueKind.ADDRESS_CONFLICT:
            reporting = issue.raw_source_fields.get("reporting_interfaces")
            if not isinstance(reporting, list):
                return set()
            return {
                str(record["device_identity"])
                for record in reporting
                if isinstance(record, dict) and record.get("device_identity")
            }
        if issue.kind is IssueKind.PULLUP_MISMATCH:
            reported = issue.raw_source_fields.get("reported_values")
            return {str(identity) for identity in reported} if isinstance(reported, dict) else set()
        return {issue.scope.device_identity}

    def _has_resolution_evidence(self, issue: MonitoringIssue) -> bool:
        if issue.kind in self._CROSS_DEVICE_KINDS:
            dependencies = self._cross_dependencies(issue)
            if not dependencies:
                return False
            for identity in dependencies:
                snapshot = self._snapshots.get(identity)
                if snapshot is None or snapshot.get("online") is False:
                    return False
                if issue.kind is IssueKind.ADDRESS_CONFLICT:
                    if not isinstance(snapshot.get("interfaces"), list):
                        return False
                elif snapshot.get("sample_rate_pullup_raw_value") is None:
                    return False
            return True

        snapshot = self._snapshots.get(issue.scope.device_identity)
        if snapshot is None:
            return False
        if snapshot.get("online") is False:
            return False
        if issue.kind is IssueKind.SUBSCRIPTION_FAILURE:
            subscription = _subscription_map(snapshot.get("subscriptions")).get(issue.scope.channel_identity or "")
            return subscription is not None and _subscription_failure_state(subscription) is False
        if issue.kind is IssueKind.RECEIVER_HEALTH_DEGRADED:
            return issue.scope.flow_identity in _flow_map(snapshot.get("receiver_flow_connection_health"))
        if issue.kind is IssueKind.CLOCK_SYNCHRONIZATION:
            managed = snapshot.get("ddm_clocking_state")
            ports = snapshot.get("clock_port_records")
            return isinstance(managed, dict) or isinstance(ports, list)
        if issue.kind is IssueKind.TELEMETRY_MISSING:
            return isinstance(snapshot.get("failed_queries"), list)
        if issue.kind is IssueKind.TELEMETRY_STALE:
            return any(
                isinstance(snapshot.get(field), dict)
                for field in ("receiver_flow_connection_health", "network_interface_traffic")
            )
        if issue.kind in {IssueKind.ADDRESS_CONFLICT, IssueKind.SUBNET_CONFLICT}:
            return bool(_interfaces(snapshot))
        if issue.kind is IssueKind.PULLUP_MISMATCH:
            return snapshot.get("sample_rate_pullup_raw_value") is not None
        if issue.kind is IssueKind.LICENSING_FAILURE:
            return "is_licensed" in snapshot or "license_valid" in snapshot
        if issue.kind is IssueKind.SAFE_STATE:
            return "safe_mode" in snapshot or "availability_state" in snapshot
        if issue.kind is IssueKind.UPGRADE_REQUIRED:
            return "upgrade_required" in snapshot or "availability_state" in snapshot
        if issue.kind is IssueKind.REBOOT_REQUIRED:
            return "interface_reboot_required" in snapshot or isinstance(snapshot.get("dante_redundancy"), dict)
        if issue.kind is IssueKind.CONFIGURATION_PARTITION_FAULT:
            return "configuration_partition_status" in snapshot
        if issue.kind is IssueKind.PERSISTENCE_FAULT:
            return "persistence_status" in snapshot
        if issue.kind is IssueKind.CONFIGURATION_DIVERGENCE:
            return any(
                requested in snapshot and effective in snapshot
                for requested, effective in (
                    ("requested_sample_rate", "sample_rate_hz"),
                    ("requested_encoding", "encoding"),
                    ("requested_sample_rate_pullup_raw_value", "sample_rate_pullup_raw_value"),
                    ("aes67_configured", "aes67_current"),
                )
            ) or bool(_interfaces(snapshot))
        return False

    def _detect_cross_device(self, timestamp: str) -> dict[str, MonitoringIssue]:
        candidates: dict[str, MonitoringIssue] = {}
        snapshots = [snapshot for snapshot in self._snapshots.values() if snapshot.get("online") is not False]
        for issue in self._detect_address_conflicts(snapshots, timestamp):
            candidates[issue.issue_id] = issue
        for issue in self._detect_pullup_mismatches(snapshots, timestamp):
            candidates[issue.issue_id] = issue
        return candidates

    def _detect_device(self, snapshot: Mapping[str, Any], timestamp: str) -> list[MonitoringIssue]:
        result = []
        result.extend(self._detect_network(snapshot, timestamp))
        result.extend(self._detect_clock(snapshot, timestamp))
        result.extend(self._detect_subscriptions(snapshot, timestamp))
        result.extend(self._detect_device_states(snapshot, timestamp))
        result.extend(self._detect_telemetry(snapshot, timestamp))
        result.extend(self._detect_divergence(snapshot, timestamp))
        return result

    def _detect_network(self, snapshot: Mapping[str, Any], timestamp: str) -> list[MonitoringIssue]:
        result = []
        networks = []
        for interface in _interfaces(snapshot):
            try:
                network = ipaddress.IPv4Network(f"{interface['address']}/{interface['netmask']}", strict=False)
            except (ipaddress.AddressValueError, ipaddress.NetmaskValueError, TypeError):
                continue
            networks.append((interface, network))
            gateway = interface.get("gateway")
            if gateway and gateway != "0.0.0.0":
                try:
                    gateway_address = ipaddress.IPv4Address(gateway)
                except ipaddress.AddressValueError:
                    gateway_address = None
                if gateway_address is None or gateway_address not in network:
                    result.append(
                        _candidate(
                            snapshot,
                            timestamp,
                            IssueKind.SUBNET_CONFLICT,
                            EventSeverity.ERROR,
                            "Gateway is outside the interface subnet",
                            f"Interface {interface['identity']} reports gateway {gateway} outside {network}",
                            interface,
                            "interface_status",
                            IssueEvidenceClass.DERIVED_STATE,
                            "Review the configured address, netmask and gateway; no change is sent automatically.",
                            interface_identity=interface["identity"],
                        )
                    )
        for index, (left, left_network) in enumerate(networks):
            for right, right_network in networks[index + 1 :]:
                if not left_network.overlaps(right_network):
                    continue
                result.append(
                    _candidate(
                        snapshot,
                        timestamp,
                        IssueKind.SUBNET_CONFLICT,
                        EventSeverity.WARNING,
                        "Device interfaces use overlapping subnets",
                        f"Interfaces {left['identity']} and {right['identity']} overlap",
                        {"left": left, "right": right},
                        "interface_status",
                        IssueEvidenceClass.DERIVED_STATE,
                        "Confirm the intended switched or redundant topology and assign distinct subnets when required.",
                        interface_identity=f"{left['identity']}+{right['identity']}",
                    )
                )
        return result

    def _detect_clock(self, snapshot: Mapping[str, Any], timestamp: str) -> list[MonitoringIssue]:
        evidence = {}
        managed = snapshot.get("ddm_clocking_state")
        if isinstance(managed, dict):
            evidence["ddm_clocking_state"] = managed
            locked = managed.get("locked")
            if locked is False or (isinstance(locked, str) and locked.casefold() not in {"locked", "true", "ok"}):
                return [
                    _candidate(
                        snapshot,
                        timestamp,
                        IssueKind.CLOCK_SYNCHRONIZATION,
                        EventSeverity.ERROR,
                        "Clock is not synchronized",
                        f"Managed clock lock state is {locked!r}",
                        evidence,
                        "ddm_clocking_state",
                        IssueEvidenceClass.DIRECT_OBSERVATION,
                        "Inspect leader availability, clock paths and pull-up settings.",
                    )
                ]
            if managed.get("follower_without_leader") is True:
                return [
                    _candidate(
                        snapshot,
                        timestamp,
                        IssueKind.CLOCK_SYNCHRONIZATION,
                        EventSeverity.ERROR,
                        "Follower has no leader",
                        "The managed clocking state reports follower_without_leader",
                        evidence,
                        "ddm_clocking_state",
                        IssueEvidenceClass.DIRECT_OBSERVATION,
                        "Restore a reachable leader and inspect the clock domain configuration.",
                    )
                ]
        down = [
            record
            for record in snapshot.get("clock_port_records") or []
            if isinstance(record, dict) and record.get("link_down") is True
        ]
        if down:
            return [
                _candidate(
                    snapshot,
                    timestamp,
                    IssueKind.CLOCK_SYNCHRONIZATION,
                    EventSeverity.WARNING,
                    "Clock transport port is down",
                    f"{len(down)} clock port record(s) report link_down",
                    {"clock_port_records": down},
                    "device_clock_status",
                    IssueEvidenceClass.DIRECT_OBSERVATION,
                    "Inspect the affected interface and upstream clock transport.",
                )
            ]
        return []

    def _detect_subscriptions(self, snapshot: Mapping[str, Any], timestamp: str) -> list[MonitoringIssue]:
        result = []
        for identity, subscription in _subscription_map(snapshot.get("subscriptions")).items():
            if _subscription_failure_state(subscription) is not True:
                continue
            status = subscription.get("status") if isinstance(subscription.get("status"), dict) else {}
            severity = EventSeverity.ERROR if status.get("severity") == "error" else EventSeverity.WARNING
            result.append(
                _candidate(
                    snapshot,
                    timestamp,
                    IssueKind.SUBSCRIPTION_FAILURE,
                    severity,
                    "Receiver subscription is not healthy",
                    str(status.get("detail") or status.get("status") or "receiver reports a failure"),
                    subscription,
                    "receiver_subscription_status",
                    IssueEvidenceClass.DIRECT_OBSERVATION,
                    "Inspect the source identity, format, clock domain and network path before changing routing.",
                    channel_identity=identity,
                )
            )
        health = snapshot.get("receiver_flow_connection_health")
        for identity, flow in _flow_map(health).items():
            degraded_fields = {}
            for key in ("status", "severity", "connected", "healthy", "error", "error_code"):
                value = flow.get(key)
                if key in {"connected", "healthy"} and value is False:
                    degraded_fields[key] = value
                elif key in {"error", "error_code"} and value not in (None, False, 0, ""):
                    degraded_fields[key] = value
                elif (
                    key in {"status", "severity"}
                    and isinstance(value, str)
                    and value.casefold()
                    in {
                        "degraded",
                        "error",
                        "failed",
                        "warning",
                    }
                ):
                    degraded_fields[key] = value
            if not degraded_fields:
                continue
            result.append(
                _candidate(
                    snapshot,
                    timestamp,
                    IssueKind.RECEIVER_HEALTH_DEGRADED,
                    EventSeverity.WARNING,
                    "Receiver flow health is degraded",
                    f"Receiver flow {identity} reports degraded state",
                    {"flow": flow, "health": health},
                    "receiver_flow_connection_health",
                    IssueEvidenceClass.DIRECT_OBSERVATION,
                    "Inspect receiver status and path counters; keep subscription status and flow health separate.",
                    flow_identity=identity,
                )
            )
        return result

    def _detect_device_states(self, snapshot: Mapping[str, Any], timestamp: str) -> list[MonitoringIssue]:
        result = []
        if snapshot.get("is_licensed") is False or snapshot.get("license_valid") is False:
            result.append(
                _candidate(
                    snapshot,
                    timestamp,
                    IssueKind.LICENSING_FAILURE,
                    EventSeverity.ERROR,
                    "Device licensing failed",
                    "The device explicitly reports an invalid or unavailable license",
                    {key: snapshot.get(key) for key in ("is_licensed", "license_valid", "ddm_status")},
                    "device_license_status",
                    IssueEvidenceClass.DIRECT_OBSERVATION,
                    "Review the reported license state and vendor licensing workflow.",
                )
            )
        availability = str(snapshot.get("availability_state") or "").casefold()
        safe = snapshot.get("safe_mode") is True or availability in {"safe", "safe_mode"}
        if safe:
            result.append(
                _candidate(
                    snapshot,
                    timestamp,
                    IssueKind.SAFE_STATE,
                    EventSeverity.ERROR,
                    "Device is in a safe state",
                    f"Availability state is {snapshot.get('availability_state')!r}",
                    {"safe_mode": snapshot.get("safe_mode"), "availability_state": snapshot.get("availability_state")},
                    "device_status",
                    IssueEvidenceClass.DIRECT_OBSERVATION,
                    "Review device diagnostics and vendor recovery guidance before making configuration changes.",
                )
            )
        if snapshot.get("upgrade_required") is True or availability == "upgrade_required":
            result.append(
                _candidate(
                    snapshot,
                    timestamp,
                    IssueKind.UPGRADE_REQUIRED,
                    EventSeverity.WARNING,
                    "Device upgrade is required",
                    "The device explicitly reports that an upgrade is required",
                    {
                        "upgrade_required": snapshot.get("upgrade_required"),
                        "availability_state": snapshot.get("availability_state"),
                    },
                    "device_status",
                    IssueEvidenceClass.DIRECT_OBSERVATION,
                    "Review the vendor-supported upgrade path; NetAudio does not start upgrades automatically.",
                )
            )
        redundancy = snapshot.get("dante_redundancy")
        reboot = snapshot.get("interface_reboot_required") is True or (
            isinstance(redundancy, dict) and redundancy.get("reboot_required") is True
        )
        if reboot:
            result.append(
                _candidate(
                    snapshot,
                    timestamp,
                    IssueKind.REBOOT_REQUIRED,
                    EventSeverity.WARNING,
                    "Configuration requires a reboot",
                    "Reported network configuration has not become effective",
                    {"interface_reboot_required": snapshot.get("interface_reboot_required"), "redundancy": redundancy},
                    "interface_status",
                    IssueEvidenceClass.DIRECT_OBSERVATION,
                    "Schedule and authorize a device reboot after reviewing the pending configuration.",
                )
            )
        for field, kind, title in (
            (
                "configuration_partition_status",
                IssueKind.CONFIGURATION_PARTITION_FAULT,
                "Configuration partition fault",
            ),
            ("persistence_status", IssueKind.PERSISTENCE_FAULT, "Configuration persistence fault"),
        ):
            value = snapshot.get(field)
            failed = value is False or (isinstance(value, str) and value.casefold() in {"error", "failed", "fault"})
            if failed:
                result.append(
                    _candidate(
                        snapshot,
                        timestamp,
                        kind,
                        EventSeverity.ERROR,
                        title,
                        f"{field} reports {value!r}",
                        {field: value},
                        "device_configuration_status",
                        IssueEvidenceClass.DIRECT_OBSERVATION,
                        "Inspect device diagnostics and preserve the current configuration before recovery work.",
                    )
                )
        return result

    def _detect_telemetry(self, snapshot: Mapping[str, Any], timestamp: str) -> list[MonitoringIssue]:
        result = []
        stale = []
        for field in ("receiver_flow_connection_health", "network_interface_traffic"):
            value = snapshot.get(field)
            if isinstance(value, dict) and value.get("fresh") is False:
                stale.append({"field": field, "value": value})
            if isinstance(value, dict):
                for stream_name in ("latency_stream", "late_packet_stream"):
                    stream = value.get(stream_name)
                    if isinstance(stream, dict) and stream.get("fresh") is False:
                        stale.append({"field": f"{field}.{stream_name}", "value": stream})
        if stale:
            result.append(
                _candidate(
                    snapshot,
                    timestamp,
                    IssueKind.TELEMETRY_STALE,
                    EventSeverity.WARNING,
                    "Monitoring telemetry is stale",
                    ", ".join(item["field"] for item in stale),
                    {"stale_streams": stale},
                    "telemetry_freshness",
                    IssueEvidenceClass.DIRECT_OBSERVATION,
                    "Inspect the monitoring path and device availability; absence is not treated as silence or health.",
                )
            )
        failed_queries = snapshot.get("failed_queries")
        if isinstance(failed_queries, list) and failed_queries:
            result.append(
                _candidate(
                    snapshot,
                    timestamp,
                    IssueKind.TELEMETRY_MISSING,
                    EventSeverity.WARNING,
                    "Required telemetry is missing",
                    f"{len(failed_queries)} query or observation source(s) failed",
                    {"failed_queries": failed_queries, "device_error": snapshot.get("error")},
                    "observation_failures",
                    IssueEvidenceClass.DIRECT_OBSERVATION,
                    "Restore the failed observation path before drawing conclusions from missing fields.",
                )
            )
        return result

    def _detect_divergence(self, snapshot: Mapping[str, Any], timestamp: str) -> list[MonitoringIssue]:
        differences = {}
        for requested, effective in (
            ("requested_sample_rate", "sample_rate_hz"),
            ("requested_encoding", "encoding"),
            ("requested_sample_rate_pullup_raw_value", "sample_rate_pullup_raw_value"),
            ("aes67_configured", "aes67_current"),
        ):
            requested_value = snapshot.get(requested)
            effective_value = snapshot.get(effective)
            if requested_value is not None and effective_value is not None and requested_value != effective_value:
                differences[effective] = {"requested": requested_value, "effective": effective_value}
        for interface in _interfaces(snapshot):
            configured = interface["configured"]
            active = interface["active"]
            comparable = set(configured) & set(active) & {"mode", "ip_address", "netmask", "gateway", "dns_server"}
            changed = {
                key: {"requested": configured[key], "effective": active[key]}
                for key in comparable
                if configured[key] != active[key]
            }
            if changed:
                differences[f"interface:{interface['identity']}"] = changed
        if not differences:
            return []
        return [
            _candidate(
                snapshot,
                timestamp,
                IssueKind.CONFIGURATION_DIVERGENCE,
                EventSeverity.WARNING,
                "Requested and effective configuration differ",
                f"{len(differences)} requested setting group(s) have not become effective",
                {"differences": differences},
                "configuration_readback",
                IssueEvidenceClass.DERIVED_STATE,
                "Review pending reboot state and fresh readback before retrying any mutation.",
            )
        ]

    def _detect_address_conflicts(self, snapshots: list[Mapping[str, Any]], timestamp: str) -> list[MonitoringIssue]:
        addresses: dict[str, list[tuple[Mapping[str, Any], dict[str, Any]]]] = {}
        for snapshot in snapshots:
            for interface in _interfaces(snapshot):
                address = interface.get("address")
                try:
                    parsed = ipaddress.IPv4Address(address)
                except (ipaddress.AddressValueError, TypeError):
                    continue
                if parsed.is_unspecified:
                    continue
                addresses.setdefault(str(parsed), []).append((snapshot, interface))
        result = []
        for address, records in addresses.items():
            if len({record[0]["device_identity"] for record in records}) < 2:
                continue
            peers = [
                {"device_identity": snapshot["device_identity"], "interface": interface}
                for snapshot, interface in records
            ]
            for snapshot, interface in records:
                result.append(
                    _candidate(
                        snapshot,
                        timestamp,
                        IssueKind.ADDRESS_CONFLICT,
                        EventSeverity.ERROR,
                        "IPv4 address is used by multiple devices",
                        f"Address {address} is reported by {len(records)} interfaces",
                        {"address": address, "reporting_interfaces": peers},
                        "interface_status",
                        IssueEvidenceClass.DERIVED_STATE,
                        "Resolve the duplicate address in DHCP or static configuration before changing routes.",
                        interface_identity=interface["identity"],
                    )
                )
        return result

    def _detect_pullup_mismatches(self, snapshots: list[Mapping[str, Any]], timestamp: str) -> list[MonitoringIssue]:
        reported = {
            str(snapshot["device_identity"]): snapshot.get("sample_rate_pullup_raw_value")
            for snapshot in snapshots
            if snapshot.get("sample_rate_pullup_raw_value") is not None
        }
        if len(set(reported.values())) <= 1:
            return []
        result = []
        for snapshot in snapshots:
            identity = str(snapshot["device_identity"])
            if identity not in reported:
                continue
            result.append(
                _candidate(
                    snapshot,
                    timestamp,
                    IssueKind.PULLUP_MISMATCH,
                    EventSeverity.WARNING,
                    "Sample-rate pull-up differs across devices",
                    f"This device reports raw value {reported[identity]!r}; the observed group reports {sorted(set(reported.values()))}",
                    {"reported_values": reported},
                    "sample_rate_pullup_status",
                    IssueEvidenceClass.DERIVED_STATE,
                    "Confirm which devices share an audio clock domain before aligning pull-up settings.",
                )
            )
        return result
