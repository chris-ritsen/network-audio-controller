from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from netaudio import core
from netaudio.core import _requests, _types


class FlowLifecycleState(str, Enum):
    PLANNED = "planned"
    UNSUPPORTED = "unsupported"
    REJECTED = "rejected"
    PENDING = "pending"
    PARTIAL = "partial"
    INCONSISTENT = "inconsistent"
    CONFIRMED = "confirmed"
    DELETED = "deleted"


def parse_transmit_flow_specification(value: Any) -> _requests.TransmitFlowSpecification:
    try:
        return core.normalize_transmit_flow_specification(value)
    except core.NetaudioCoreError as error:
        raise ValueError(str(error)) from error


@dataclass(frozen=True)
class FlowOperationPlan:
    operation: str
    state: FlowLifecycleState
    transport: str
    protocol_id: int | None
    serializer_cohort: str | None
    supported: bool
    reasons: tuple[str, ...]
    specification: _requests.TransmitFlowSpecification | None = None
    flow_id: int | None = None
    command_specification: dict[str, Any] | None = None
    wire_authored_fields: tuple[str, ...] = ()
    state_preconditions: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "state": self.state.value,
            "transport": self.transport,
            "protocol_id": self.protocol_id,
            "serializer_cohort": self.serializer_cohort,
            "supported": self.supported,
            "reasons": list(self.reasons),
            "specification": copy.deepcopy(self.specification),
            "flow_id": self.flow_id,
            "command_specification": copy.deepcopy(self.command_specification),
            "wire_authored_fields": list(self.wire_authored_fields),
            "state_preconditions": copy.deepcopy(self.state_preconditions),
        }


@dataclass(frozen=True)
class FlowOperationResult:
    operation: str
    state: FlowLifecycleState
    transport: str
    request_acknowledgement: _types.CommandReceipt | None
    device_confirmation: bool | None
    persistence_confirmation: bool | None
    effective_state_confirmation: bool | None
    requested: _requests.TransmitFlowSpecification | _types.ObservedTransmitFlowSpecification | None
    effective: _types.ObservedTransmitFlowSpecification | None
    comparison: _types.FlowComparison | None
    message: str
    verification_observations: tuple[dict[str, Any], ...] = ()
    media_packet_reception_confirmed: bool = False
    clock_lock_confirmed: bool = False
    decoded_audio_confirmed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "state": self.state.value,
            "transport": self.transport,
            "request_acknowledgement": copy.deepcopy(self.request_acknowledgement),
            "device_confirmation": self.device_confirmation,
            "persistence_confirmation": self.persistence_confirmation,
            "effective_state_confirmation": self.effective_state_confirmation,
            "requested": copy.deepcopy(self.requested),
            "effective": copy.deepcopy(self.effective),
            "comparison": copy.deepcopy(self.comparison),
            "message": self.message,
            "verification_observations": copy.deepcopy(list(self.verification_observations)),
            "media_packet_reception_confirmed": self.media_packet_reception_confirmed,
            "clock_lock_confirmed": self.clock_lock_confirmed,
            "decoded_audio_confirmed": self.decoded_audio_confirmed,
        }
