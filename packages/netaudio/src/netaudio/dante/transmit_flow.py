from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, TypeVar
from netaudio.core import _requests, _types


FLOW_SPECIFICATION_SCHEMA_VERSION = 1


class MediaMode(str, Enum):
    UNKNOWN = "unknown"
    NATIVE_DANTE = "native_dante"
    RTP_AES67 = "rtp_aes67"


class FlowType(str, Enum):
    UNICAST = "unicast"
    MULTICAST = "multicast"


class RedundancyConstraint(str, Enum):
    DEVICE_DEFAULT = "device_default"
    NONE = "none"
    OPTIONAL = "optional"
    REQUIRED = "required"


class FlowLifecycleState(str, Enum):
    PLANNED = "planned"
    UNSUPPORTED = "unsupported"
    REJECTED = "rejected"
    PENDING = "pending"
    PARTIAL = "partial"
    INCONSISTENT = "inconsistent"
    CONFIRMED = "confirmed"
    DELETED = "deleted"


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _copy_mapping(value: Any, label: str) -> dict[str, Any]:
    return copy.deepcopy(dict(_mapping(value, label)))


_FlowValue = TypeVar("_FlowValue", bound=Mapping[str, Any])


def _with_extra_fields(value: _FlowValue, extra_fields: Mapping[str, Any]) -> _FlowValue:
    if not isinstance(value, dict):
        raise TypeError("serialized flow value must be an object")

    for key, item in extra_fields.items():
        if key not in value:
            value[key] = copy.deepcopy(item)

    return value


def _required(value: Mapping[str, Any], name: str) -> Any:
    if name not in value:
        raise ValueError(f"flow value requires {name}")

    return value[name]


@dataclass(frozen=True)
class FlowSocket:
    address: str
    port: int
    interface: str | None = None
    extra_fields: dict[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.extra_fields, dict):
            raise ValueError("flow destination extra_fields must be an object")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> FlowSocket:
        value = _mapping(value, "flow destination")
        known = {"address", "port", "interface"}
        return cls(
            address=_required(value, "address"),
            port=_required(value, "port"),
            interface=value.get("interface"),
            extra_fields={key: copy.deepcopy(item) for key, item in value.items() if key not in known},
        )

    def to_dict(self) -> _requests.Destination:
        value: _requests.Destination = {
            "address": self.address,
            "port": self.port,
            "interface": self.interface,
        }
        return _with_extra_fields(value, self.extra_fields)


@dataclass(frozen=True)
class TransmitterChannelSlot:
    slot: int
    transmitter_channel: int
    extra_fields: dict[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.extra_fields, dict):
            raise ValueError("flow channel slot extra_fields must be an object")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> TransmitterChannelSlot:
        value = _mapping(value, "flow channel slot")
        known = {"slot", "transmitter_channel"}
        return cls(
            slot=_required(value, "slot"),
            transmitter_channel=_required(value, "transmitter_channel"),
            extra_fields={key: copy.deepcopy(item) for key, item in value.items() if key not in known},
        )

    def to_dict(self) -> _requests.ChannelSlot:
        value: _requests.ChannelSlot = {"slot": self.slot, "transmitter_channel": self.transmitter_channel}
        return _with_extra_fields(value, self.extra_fields)


@dataclass(frozen=True)
class FlowIdentity:
    global_flow_id: int | None = None
    media_type_code: int | None = None
    media_local_flow_id: int | None = None
    extra_fields: dict[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.extra_fields, dict):
            raise ValueError("flow identity extra_fields must be an object")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> FlowIdentity:
        value = _mapping(value, "flow identity")
        known = {"global_flow_id", "media_type_code", "media_local_flow_id"}
        return cls(
            global_flow_id=value.get("global_flow_id"),
            media_type_code=value.get("media_type_code"),
            media_local_flow_id=value.get("media_local_flow_id"),
            extra_fields={key: copy.deepcopy(item) for key, item in value.items() if key not in known},
        )

    def to_dict(self) -> _requests.FlowIdentity:
        value: _requests.FlowIdentity = {
            "global_flow_id": self.global_flow_id,
            "media_type_code": self.media_type_code,
            "media_local_flow_id": self.media_local_flow_id,
        }
        return _with_extra_fields(value, self.extra_fields)


@dataclass(frozen=True)
class FlowProtocolRequirements:
    protocol_id: int | None = None
    protocol_version: str | None = None
    cohort: str | None = None
    required_capabilities: tuple[str, ...] = ()
    extra_fields: dict[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.required_capabilities, tuple):
            raise ValueError("required_capabilities must be a tuple")

        if not isinstance(self.extra_fields, dict):
            raise ValueError("flow protocol extra_fields must be an object")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> FlowProtocolRequirements:
        value = _mapping(value, "flow protocol requirements")
        raw_capabilities = value.get("required_capabilities", ())
        if not isinstance(raw_capabilities, (list, tuple)):
            raise ValueError("required_capabilities must be a list")
        known = {"protocol_id", "protocol_version", "cohort", "required_capabilities"}
        return cls(
            protocol_id=value.get("protocol_id"),
            protocol_version=value.get("protocol_version"),
            cohort=value.get("cohort"),
            required_capabilities=tuple(raw_capabilities),
            extra_fields={key: copy.deepcopy(item) for key, item in value.items() if key not in known},
        )

    def to_dict(self) -> _requests.ProtocolRequirements:
        value: _requests.ProtocolRequirements = {
            "protocol_id": self.protocol_id,
            "protocol_version": self.protocol_version,
            "cohort": self.cohort,
            "required_capabilities": list(self.required_capabilities),
        }
        return _with_extra_fields(value, self.extra_fields)


@dataclass(frozen=True)
class TransmitFlowSpecification:
    media_mode: MediaMode
    flow_type: FlowType
    channel_slots: tuple[TransmitterChannelSlot, ...]
    name: str | None = None
    sample_rate_hz: int | None = None
    encoding_bits: int | None = None
    frames_per_packet: int | None = None
    primary_destination: FlowSocket | None = None
    secondary_destination: FlowSocket | None = None
    redundancy: RedundancyConstraint = RedundancyConstraint.DEVICE_DEFAULT
    identity: FlowIdentity = field(default_factory=FlowIdentity)
    protocol: FlowProtocolRequirements = field(default_factory=FlowProtocolRequirements)
    raw_fields: dict[str, Any] = field(default_factory=dict, compare=False)
    extra_fields: dict[str, Any] = field(default_factory=dict, compare=False)
    schema_version: int = FLOW_SPECIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        from netaudio import core

        if not isinstance(self.media_mode, MediaMode) or not isinstance(self.flow_type, FlowType):
            raise ValueError("media_mode and flow_type must be typed flow values")

        if not isinstance(self.redundancy, RedundancyConstraint):
            raise ValueError("redundancy must be a typed constraint")

        if not isinstance(self.channel_slots, tuple) or any(
            not isinstance(item, TransmitterChannelSlot) for item in self.channel_slots
        ):
            raise ValueError("channel_slots must contain typed channel slots")

        for destination in (self.primary_destination, self.secondary_destination):
            if destination is not None and not isinstance(destination, FlowSocket):
                raise ValueError("destination must be a flow socket")

        if not isinstance(self.identity, FlowIdentity) or not isinstance(self.protocol, FlowProtocolRequirements):
            raise ValueError("identity and protocol must be typed flow values")

        if not isinstance(self.raw_fields, dict) or not isinstance(self.extra_fields, dict):
            raise ValueError("raw_fields and extra_fields must be objects")

        try:
            core.validate_transmit_flow_specification(self.to_dict())
        except core.NetaudioCoreError as error:
            raise ValueError(str(error)) from error

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> TransmitFlowSpecification:
        value = _mapping(value, "transmit flow specification")
        raw_slots = value.get("channel_slots")
        if not isinstance(raw_slots, (list, tuple)):
            raise ValueError("channel_slots must be a list")
        known = {
            "schema_version",
            "media_mode",
            "flow_type",
            "name",
            "channel_slots",
            "sample_rate_hz",
            "encoding_bits",
            "frames_per_packet",
            "primary_destination",
            "secondary_destination",
            "redundancy",
            "identity",
            "protocol",
            "raw_fields",
        }
        try:
            media_mode = MediaMode(value.get("media_mode"))
        except (TypeError, ValueError) as exception:
            raise ValueError("media_mode must be unknown, native_dante or rtp_aes67") from exception
        try:
            flow_type = FlowType(value.get("flow_type"))
        except (TypeError, ValueError) as exception:
            raise ValueError("flow_type must be unicast or multicast") from exception
        try:
            redundancy = RedundancyConstraint(value.get("redundancy", RedundancyConstraint.DEVICE_DEFAULT.value))
        except (TypeError, ValueError) as exception:
            raise ValueError("redundancy must be device_default, none, optional or required") from exception
        return cls(
            schema_version=value.get("schema_version", FLOW_SPECIFICATION_SCHEMA_VERSION),
            media_mode=media_mode,
            flow_type=flow_type,
            name=value.get("name"),
            channel_slots=tuple(TransmitterChannelSlot.from_dict(item) for item in raw_slots),
            sample_rate_hz=value.get("sample_rate_hz"),
            encoding_bits=value.get("encoding_bits"),
            frames_per_packet=value.get("frames_per_packet"),
            primary_destination=(
                FlowSocket.from_dict(value["primary_destination"])
                if value.get("primary_destination") is not None
                else None
            ),
            secondary_destination=(
                FlowSocket.from_dict(value["secondary_destination"])
                if value.get("secondary_destination") is not None
                else None
            ),
            redundancy=redundancy,
            identity=FlowIdentity.from_dict(value.get("identity", {})),
            protocol=FlowProtocolRequirements.from_dict(value.get("protocol", {})),
            raw_fields=_copy_mapping(value.get("raw_fields", {}), "raw_fields"),
            extra_fields={key: copy.deepcopy(item) for key, item in value.items() if key not in known},
        )

    @classmethod
    def from_inventory_record(
        cls,
        record: Mapping[str, Any],
        *,
        protocol_id: int,
    ) -> TransmitFlowSpecification:
        from netaudio import core

        record = _mapping(record, "transmitter flow inventory record")

        try:
            specification = core.transmit_flow_specification(dict(record), protocol_id=protocol_id)
        except core.NetaudioCoreError as error:
            raise ValueError(str(error)) from error

        return cls.from_dict(specification)

    @property
    def channels(self) -> list[int]:
        return [item.transmitter_channel for item in self.channel_slots]

    def to_dict(self) -> _requests.TransmitFlowSpecification:
        value: _requests.TransmitFlowSpecification = {
            "schema_version": self.schema_version,
            "media_mode": self.media_mode.value,
            "flow_type": self.flow_type.value,
            "name": self.name,
            "channel_slots": [item.to_dict() for item in self.channel_slots],
            "sample_rate_hz": self.sample_rate_hz,
            "encoding_bits": self.encoding_bits,
            "frames_per_packet": self.frames_per_packet,
            "primary_destination": (
                self.primary_destination.to_dict() if self.primary_destination is not None else None
            ),
            "secondary_destination": (
                self.secondary_destination.to_dict() if self.secondary_destination is not None else None
            ),
            "redundancy": self.redundancy.value,
            "identity": self.identity.to_dict(),
            "protocol": self.protocol.to_dict(),
            "raw_fields": copy.deepcopy(self.raw_fields),
        }
        return _with_extra_fields(value, self.extra_fields)


@dataclass(frozen=True)
class FlowDifference:
    field: str
    requested: Any
    effective: Any

    def to_dict(self) -> _requests.FlowDifference:
        return {
            "field": self.field,
            "requested": copy.deepcopy(self.requested),
            "effective": copy.deepcopy(self.effective),
        }


@dataclass(frozen=True)
class FlowComparison:
    matches: bool
    differences: tuple[FlowDifference, ...]
    unavailable_fields: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, value) -> FlowComparison:
        return cls(
            matches=value["matches"],
            differences=tuple(FlowDifference(**difference) for difference in value["differences"]),
            unavailable_fields=tuple(value["unavailable_fields"]),
        )

    def to_dict(self) -> _requests.FlowComparison:
        return {
            "matches": self.matches,
            "differences": [item.to_dict() for item in self.differences],
            "unavailable_fields": list(self.unavailable_fields),
        }


def compare_transmit_flows(
    requested: TransmitFlowSpecification,
    effective: TransmitFlowSpecification,
) -> FlowComparison:
    from netaudio import core

    result = core.compare_transmit_flows(requested.to_dict(), effective.to_dict())

    return FlowComparison.from_dict(result)


@dataclass(frozen=True)
class FlowOperationPlan:
    operation: str
    state: FlowLifecycleState
    transport: str
    protocol_id: int | None
    serializer_cohort: str | None
    supported: bool
    reasons: tuple[str, ...]
    specification: TransmitFlowSpecification | None = None
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
            "specification": self.specification.to_dict() if self.specification else None,
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
    requested: TransmitFlowSpecification | None
    effective: TransmitFlowSpecification | None
    comparison: FlowComparison | None
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
            "requested": self.requested.to_dict() if self.requested else None,
            "effective": self.effective.to_dict() if self.effective else None,
            "comparison": self.comparison.to_dict() if self.comparison else None,
            "message": self.message,
            "verification_observations": copy.deepcopy(list(self.verification_observations)),
            "media_packet_reception_confirmed": self.media_packet_reception_confirmed,
            "clock_lock_confirmed": self.clock_lock_confirmed,
            "decoded_audio_confirmed": self.decoded_audio_confirmed,
        }
