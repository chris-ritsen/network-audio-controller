from __future__ import annotations

import contextlib
import contextvars
import inspect
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any

from netaudio.monitoring.journal import MonitoringEventJournal
from netaudio.monitoring.model import (
    DerivationStatus,
    EventSeverity,
    MonitoringEvent,
    MonitoringEventKind,
    _json_safe,
)
from netaudio.monitoring.signals import snapshot_from_device

_MAX_EVIDENCE_DEPTH = 6
_MAX_EVIDENCE_ITEMS = 64
_MAX_EVIDENCE_STRING = 2048
_SENSITIVE_KEY_PARTS = ("authorization", "credential", "password", "secret", "token")


def _bounded_evidence(value: Any, *, depth: int = 0) -> Any:
    if depth >= _MAX_EVIDENCE_DEPTH:
        return "[depth limit]"
    if isinstance(value, str):
        if len(value) <= _MAX_EVIDENCE_STRING:
            return value
        return value[:_MAX_EVIDENCE_STRING] + "…"
    if hasattr(value, "to_dict"):
        return _bounded_evidence(value.to_dict(), depth=depth)
    if is_dataclass(value) and not isinstance(value, type):
        return _bounded_evidence(asdict(value), depth=depth)
    if isinstance(value, Mapping):
        bounded = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= _MAX_EVIDENCE_ITEMS:
                bounded["_truncated"] = len(value) - _MAX_EVIDENCE_ITEMS
                break
            label = str(key)
            if any(part in label.casefold() for part in _SENSITIVE_KEY_PARTS):
                bounded[label] = "[redacted]"
            else:
                bounded[label] = _bounded_evidence(item, depth=depth + 1)
        return bounded
    if isinstance(value, (list, tuple)):
        bounded = [_bounded_evidence(item, depth=depth + 1) for item in value[:_MAX_EVIDENCE_ITEMS]]
        if len(value) > _MAX_EVIDENCE_ITEMS:
            bounded.append({"_truncated": len(value) - _MAX_EVIDENCE_ITEMS})
        return bounded
    return _json_safe(value)


def _device_transport(device) -> str:
    return "ddm" if getattr(device, "requires_managed_control", False) else "direct"


def _identity_fields(operation_name: str, requested: Any, payload: Mapping[str, Any]) -> dict[str, str | None]:
    flow_identity = payload.get("flow_identity")
    if isinstance(flow_identity, Mapping):
        source = flow_identity.get("source_ipv4")
        session = flow_identity.get("session_id")
        flow_identity = f"rtp:{source}/{session}" if source is not None and session is not None else None
    elif operation_name in {"create_transmit_flow", "delete_transmit_flow", "transmit_flow"}:
        effective = _effective_values(payload)
        candidate = effective if isinstance(effective, Mapping) else requested
        identity = candidate.get("identity") if isinstance(candidate, Mapping) else None
        flow_id = identity.get("global_flow_id") if isinstance(identity, Mapping) else None
        if flow_id is None and isinstance(requested, Mapping):
            flow_id = requested.get("flow_id")
        flow_identity = f"tx:{flow_id}" if flow_id is not None else None
    elif not isinstance(flow_identity, str):
        flow_identity = None
    channel_identity = None
    channel_ids = payload.get("receiver_channel_ids")
    if not isinstance(channel_ids, list) and isinstance(requested, Mapping):
        channel_ids = requested.get("receiver_channel_ids")
    if isinstance(channel_ids, list) and len(channel_ids) == 1:
        channel_identity = f"rx:{channel_ids[0]}"
    return {
        "flow_identity": flow_identity,
        "channel_identity": channel_identity,
        "interface_identity": None,
    }


def _result_payload(result: Any) -> dict[str, Any]:
    if hasattr(result, "to_dict"):
        result = result.to_dict()
    if isinstance(result, Mapping):
        return dict(result)
    return {"result": result}


def _acknowledgement(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    value = payload.get("request_acknowledgement")
    if isinstance(value, Mapping):
        return value
    if "request_acknowledged" in payload or "result_code" in payload:
        return {
            "accepted": payload.get("request_acknowledged") is True,
            "result_code": payload.get("result_code"),
            "received": payload.get("result_code") is not None,
        }
    return None


def _result_code(acknowledgement: Mapping[str, Any] | None) -> int | None:
    if acknowledgement is None:
        return None
    value = acknowledgement.get("result_code")
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _effective_values(payload: Mapping[str, Any]) -> Any:
    for name in ("effective", "effective_properties", "effective_values"):
        if name in payload:
            return payload[name]
    return None


def _requested_values(default: Any, payload: Mapping[str, Any]) -> Any:
    for name in ("requested", "requested_properties", "requested_values"):
        if name in payload:
            return payload[name]
    return default


def _result_state(payload: Mapping[str, Any]) -> str:
    state = payload.get("state")
    if isinstance(state, str) and state:
        return state
    acknowledgement = _acknowledgement(payload)
    if acknowledgement is not None and acknowledgement.get("accepted") is True:
        return "request_acknowledged"
    if acknowledgement is not None and acknowledgement.get("accepted") is False:
        return "rejected"
    return "unverified"


def _has_fresh_readback(payload: Mapping[str, Any]) -> bool:
    if _effective_values(payload) not in (None, {}, []):
        return True
    observations = payload.get("verification_observations")
    if not isinstance(observations, (list, tuple)):
        return False
    for observation in observations:
        if not isinstance(observation, Mapping):
            continue
        if observation.get("phase") == "readback" and observation.get("outcome") != "unavailable":
            return True
        if isinstance(observation.get("inventory"), Mapping):
            return True
    return False


@dataclass(frozen=True)
class OperationHandle:
    operation_id: str
    correlation_id: str
    parent_preset_run_id: str | None
    operation_name: str
    requested_values: Any
    transport: str
    kind: MonitoringEventKind


EmitTransition = Callable[[Any, dict[str, Any], bool], Awaitable[list[MonitoringEvent]]]


class MutationAuditRecorder:
    """One recorder for durable operation transitions across every user surface."""

    def __init__(self, emit_transition: EmitTransition):
        self._emit_transition = emit_transition
        self._automatic_suppressed: contextvars.ContextVar[bool] = contextvars.ContextVar(
            "netaudio_operation_audit_suppressed", default=False
        )

    @classmethod
    def from_journal(
        cls,
        journal: MonitoringEventJournal,
        *,
        publish: Callable[[MonitoringEvent], Awaitable[None]] | None = None,
    ) -> MutationAuditRecorder:
        async def emit(device, fields: dict[str, Any], observe_device: bool) -> list[MonitoringEvent]:
            event = journal.record_operation_transition(device, **fields)
            generated = [event]
            if publish is not None:
                await publish(event)
            if observe_device:
                observed = journal.observe_device(device)
                generated.extend(observed)
                if publish is not None:
                    for observed_event in observed:
                        await publish(observed_event)
            return generated

        return cls(emit)

    @contextlib.contextmanager
    def suppress_automatic_operations(self):
        token = self._automatic_suppressed.set(True)
        try:
            yield
        finally:
            self._automatic_suppressed.reset(token)

    async def begin_operation(
        self,
        device,
        operation_name: str,
        requested_values: Any,
        *,
        transport: str | None = None,
        parent_preset_run_id: str | None = None,
        operation_id: str | None = None,
        kind: MonitoringEventKind = MonitoringEventKind.CONFIGURATION_OPERATION,
    ) -> OperationHandle:
        operation_id = operation_id or str(uuid.uuid4())
        correlation_id = parent_preset_run_id or operation_id
        handle = OperationHandle(
            operation_id=operation_id,
            correlation_id=correlation_id,
            parent_preset_run_id=parent_preset_run_id,
            operation_name=operation_name,
            requested_values=requested_values,
            transport=transport or _device_transport(device),
            kind=kind,
        )
        await self._emit(
            device,
            handle,
            lifecycle_phase="requested",
            final_operation_state="requested",
            observation_source="user_request",
            derivation_status=DerivationStatus.DERIVED,
            evidence={"request": handle.requested_values},
        )
        return handle

    async def complete_operation(self, device, handle: OperationHandle, result: Any) -> None:
        payload = _result_payload(result)
        acknowledgement = _acknowledgement(payload)
        state = _result_state(payload)
        requested = _requested_values(handle.requested_values, payload)
        effective = _effective_values(payload)
        persistence_ack = payload.get("persistence_request_acknowledgement")
        persistence_confirmation = payload.get("persistence_confirmation")
        if not isinstance(persistence_confirmation, bool):
            persistence_confirmation = None
        common = {
            "requested_values": requested,
            "acknowledgement_result_code": _result_code(
                acknowledgement if acknowledgement is not None else payload.get("persistence_request_acknowledgement")
            ),
            "effective_values": effective,
            "final_operation_state": state,
            "persistence_request_acknowledgement": persistence_ack,
            "persistence_confirmation": persistence_confirmation,
            "evidence": {
                "message": payload.get("message"),
                "acknowledgement": acknowledgement,
                "verification_observations": payload.get("verification_observations", []),
                "device_confirmation": payload.get("device_confirmation"),
                "effective_state_confirmation": payload.get("effective_state_confirmation"),
            },
            **_identity_fields(handle.operation_name, requested, payload),
        }

        persistence_accepted = (
            isinstance(payload.get("persistence_request_acknowledgement"), Mapping)
            and payload["persistence_request_acknowledgement"].get("accepted") is True
        )
        if persistence_accepted:
            await self._emit(
                device,
                handle,
                lifecycle_phase="persistence_request_acknowledged",
                observation_source="configuration_storage_acknowledgement",
                derivation_status=DerivationStatus.OBSERVED,
                **common,
            )
            if persistence_confirmation is True:
                await self._emit(
                    device,
                    handle,
                    lifecycle_phase="persistence_confirmed",
                    observation_source="independent_persistence_observation",
                    derivation_status=DerivationStatus.OBSERVED,
                    observe_device=True,
                    **common,
                )
            return

        accepted = acknowledgement is not None and acknowledgement.get("accepted") is True
        rejected = acknowledgement is not None and acknowledgement.get("accepted") is False
        if accepted:
            await self._emit(
                device,
                handle,
                lifecycle_phase="request_acknowledged",
                observation_source="operation_transport_acknowledgement",
                derivation_status=DerivationStatus.OBSERVED,
                **common,
            )
        elif rejected or state in {"rejected", "unsupported"}:
            await self._emit(
                device,
                handle,
                lifecycle_phase="request_rejected",
                observation_source="operation_transport_acknowledgement" if acknowledgement else "operation_validation",
                derivation_status=DerivationStatus.OBSERVED if acknowledgement else DerivationStatus.DERIVED,
                severity=EventSeverity.WARNING,
                **common,
            )
            return

        if state == "failed":
            await self._emit(
                device,
                handle,
                lifecycle_phase="transport_or_validation_failure",
                observation_source="operation_result",
                derivation_status=DerivationStatus.DERIVED,
                severity=EventSeverity.ERROR,
                **common,
            )
            return

        effective_confirmation = payload.get("effective_state_confirmation")
        if effective_confirmation is True or state in {"confirmed", "deleted"}:
            await self._emit(
                device,
                handle,
                lifecycle_phase="effective_state_confirmed",
                observation_source="fresh_parsed_readback",
                derivation_status=DerivationStatus.OBSERVED,
                observe_device=True,
                **common,
            )
        elif effective_confirmation is False or state in {"contradicted", "inconsistent"}:
            await self._emit(
                device,
                handle,
                lifecycle_phase="inconsistent",
                observation_source="fresh_parsed_readback",
                derivation_status=DerivationStatus.OBSERVED,
                severity=EventSeverity.ERROR,
                observe_device=True,
                **common,
            )
        elif state not in {"request_acknowledged", "acknowledged"} or not accepted:
            await self._emit(
                device,
                handle,
                lifecycle_phase="partial_unobservable",
                observation_source="bounded_verification",
                derivation_status=DerivationStatus.DERIVED,
                severity=EventSeverity.WARNING,
                observe_device=_has_fresh_readback(payload),
                **common,
            )
        elif accepted:
            await self._emit(
                device,
                handle,
                lifecycle_phase="partial_unobservable",
                observation_source="bounded_verification",
                derivation_status=DerivationStatus.DERIVED,
                severity=EventSeverity.WARNING,
                observe_device=_has_fresh_readback(payload),
                **common,
            )

    async def fail_operation(self, device, handle: OperationHandle, exception: BaseException) -> None:
        await self._emit(
            device,
            handle,
            lifecycle_phase="transport_or_validation_failure",
            final_operation_state="failed",
            observation_source="operation_exception",
            derivation_status=DerivationStatus.OBSERVED,
            severity=EventSeverity.ERROR,
            evidence={"exception_type": type(exception).__name__, "message": str(exception)},
        )

    async def run_operation(
        self,
        device,
        operation_name: str,
        requested_values: Any,
        operation: Callable[[], Awaitable[Any]],
        *,
        transport: str | None = None,
        result_adapter: Callable[[Any], Any] | None = None,
    ) -> Any:
        if self._automatic_suppressed.get():
            return await operation()
        handle = await self.begin_operation(device, operation_name, requested_values, transport=transport)
        try:
            result = await operation()
        except Exception as exception:
            await self.fail_operation(device, handle, exception)
            raise
        await self.complete_operation(device, handle, result_adapter(result) if result_adapter is not None else result)
        return result

    async def _emit(
        self,
        device,
        handle: OperationHandle,
        *,
        lifecycle_phase: str,
        final_operation_state: str,
        observation_source: str,
        derivation_status: DerivationStatus,
        requested_values: Any = None,
        acknowledgement_result_code: int | None = None,
        effective_values: Any = None,
        persistence_request_acknowledgement: Any = None,
        persistence_confirmation: bool | None = None,
        evidence: Mapping[str, Any] | None = None,
        interface_identity: str | None = None,
        channel_identity: str | None = None,
        flow_identity: str | None = None,
        severity: EventSeverity = EventSeverity.INFO,
        observe_device: bool = False,
    ) -> list[MonitoringEvent]:
        fields = {
            "kind": handle.kind,
            "operation_id": handle.operation_id,
            "correlation_id": handle.correlation_id,
            "parent_preset_run_id": handle.parent_preset_run_id,
            "operation_name": handle.operation_name,
            "lifecycle_phase": lifecycle_phase,
            "requested_values": _bounded_evidence(
                handle.requested_values if requested_values is None else requested_values
            ),
            "acknowledgement_result_code": acknowledgement_result_code,
            "transport": handle.transport,
            "effective_values": _bounded_evidence(effective_values),
            "final_operation_state": final_operation_state,
            "persistence_request_acknowledgement": _bounded_evidence(persistence_request_acknowledgement),
            "persistence_confirmation": persistence_confirmation,
            "evidence": _bounded_evidence(evidence or {}),
            "observation_source": observation_source,
            "derivation_status": derivation_status,
            "interface_identity": interface_identity,
            "channel_identity": channel_identity,
            "flow_identity": flow_identity,
            "severity": severity,
        }
        return await self._emit_transition(
            device,
            fields,
            observe_device and handle.kind is MonitoringEventKind.CONFIGURATION_OPERATION,
        )


def remote_recorder(
    append: Callable[[dict[str, Any]], Awaitable[Any]],
) -> MutationAuditRecorder:
    async def emit(device, fields: dict[str, Any], observe_device: bool) -> list[MonitoringEvent]:
        payload = dict(fields)
        payload["kind"] = fields["kind"].value
        payload["derivation_status"] = fields["derivation_status"].value
        payload["severity"] = fields["severity"].value
        payload["device_snapshot"] = (
            _json_safe(dict(device)) if isinstance(device, Mapping) else snapshot_from_device(device)
        )
        payload["observe_device"] = observe_device
        result = append(_json_safe(payload))
        if inspect.isawaitable(result):
            await result
        return []

    return MutationAuditRecorder(emit)
