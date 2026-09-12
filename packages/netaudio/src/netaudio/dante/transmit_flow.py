from __future__ import annotations

import copy
import ipaddress
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping


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


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{label} must be an integer from {minimum} through {maximum}")
    return value


def _optional_integer(value: Any, label: str, minimum: int, maximum: int) -> int | None:
    return None if value is None else _integer(value, label, minimum, maximum)


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string or null")
    return value


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _copy_mapping(value: Any, label: str) -> dict[str, Any]:
    return copy.deepcopy(dict(_mapping(value, label)))


@dataclass(frozen=True)
class FlowSocket:
    address: str
    port: int
    interface: str | None = None
    extra_fields: dict[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        try:
            address = ipaddress.IPv4Address(self.address)
        except (ipaddress.AddressValueError, TypeError) as exception:
            raise ValueError("flow destination address must be IPv4") from exception
        if address.is_unspecified or address == ipaddress.IPv4Address("255.255.255.255"):
            raise ValueError("flow destination address must not be unspecified or limited broadcast")
        _integer(self.port, "flow destination port", 1, 65535)
        _optional_text(self.interface, "flow destination interface")
        if not isinstance(self.extra_fields, dict):
            raise ValueError("flow destination extra_fields must be an object")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> FlowSocket:
        value = _mapping(value, "flow destination")
        known = {"address", "port", "interface"}
        return cls(
            address=value.get("address"),
            port=value.get("port"),
            interface=value.get("interface"),
            extra_fields={key: copy.deepcopy(item) for key, item in value.items() if key not in known},
        )

    def to_dict(self) -> dict[str, Any]:
        value = copy.deepcopy(self.extra_fields)
        value.update({"address": self.address, "port": self.port, "interface": self.interface})
        return value


@dataclass(frozen=True)
class TransmitterChannelSlot:
    slot: int
    transmitter_channel: int
    extra_fields: dict[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        _integer(self.slot, "flow channel slot", 1, 65535)
        _integer(self.transmitter_channel, "transmitter channel", 1, 65535)
        if not isinstance(self.extra_fields, dict):
            raise ValueError("flow channel slot extra_fields must be an object")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> TransmitterChannelSlot:
        value = _mapping(value, "flow channel slot")
        known = {"slot", "transmitter_channel"}
        return cls(
            slot=value.get("slot"),
            transmitter_channel=value.get("transmitter_channel"),
            extra_fields={key: copy.deepcopy(item) for key, item in value.items() if key not in known},
        )

    def to_dict(self) -> dict[str, Any]:
        value = copy.deepcopy(self.extra_fields)
        value.update({"slot": self.slot, "transmitter_channel": self.transmitter_channel})
        return value


@dataclass(frozen=True)
class FlowIdentity:
    global_flow_id: int | None = None
    media_type_code: int | None = None
    media_local_flow_id: int | None = None
    extra_fields: dict[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        _optional_integer(self.global_flow_id, "global flow identifier", 1, 65535)
        _optional_integer(self.media_type_code, "media type code", 0, 65535)
        _optional_integer(self.media_local_flow_id, "media-local flow identifier", 1, 65535)
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

    def to_dict(self) -> dict[str, Any]:
        value = copy.deepcopy(self.extra_fields)
        value.update(
            {
                "global_flow_id": self.global_flow_id,
                "media_type_code": self.media_type_code,
                "media_local_flow_id": self.media_local_flow_id,
            }
        )
        return value


@dataclass(frozen=True)
class FlowProtocolRequirements:
    protocol_id: int | None = None
    protocol_version: str | None = None
    cohort: str | None = None
    required_capabilities: tuple[str, ...] = ()
    extra_fields: dict[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        _optional_integer(self.protocol_id, "flow protocol identifier", 0, 65535)
        _optional_text(self.protocol_version, "flow protocol version")
        _optional_text(self.cohort, "flow protocol cohort")
        if not isinstance(self.required_capabilities, tuple) or any(
            not isinstance(item, str) or not item for item in self.required_capabilities
        ):
            raise ValueError("required_capabilities must contain non-empty strings")
        if len(set(self.required_capabilities)) != len(self.required_capabilities):
            raise ValueError("required_capabilities must not contain duplicates")
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

    def to_dict(self) -> dict[str, Any]:
        value = copy.deepcopy(self.extra_fields)
        value.update(
            {
                "protocol_id": self.protocol_id,
                "protocol_version": self.protocol_version,
                "cohort": self.cohort,
                "required_capabilities": list(self.required_capabilities),
            }
        )
        return value


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
        if self.schema_version != FLOW_SPECIFICATION_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported transmit flow schema_version {self.schema_version}; "
                f"expected {FLOW_SPECIFICATION_SCHEMA_VERSION}"
            )
        if not isinstance(self.media_mode, MediaMode):
            raise ValueError("media_mode must be unknown, native_dante or rtp_aes67")
        if not isinstance(self.flow_type, FlowType):
            raise ValueError("flow_type must be unicast or multicast")
        if not isinstance(self.channel_slots, tuple) or not self.channel_slots:
            raise ValueError("channel_slots must be a non-empty ordered list")
        if any(not isinstance(item, TransmitterChannelSlot) for item in self.channel_slots):
            raise ValueError("channel_slots contains an invalid entry")
        slots = [item.slot for item in self.channel_slots]
        channels = [item.transmitter_channel for item in self.channel_slots]
        if slots != sorted(slots) or len(slots) != len(set(slots)):
            raise ValueError("flow channel slots must be unique and strictly ascending")
        if len(channels) != len(set(channels)):
            raise ValueError("transmitter channels must not contain duplicates")
        _optional_text(self.name, "flow name")
        if self.name is not None and "\0" in self.name:
            raise ValueError("flow name must not contain NUL")
        _optional_integer(self.sample_rate_hz, "sample rate", 1, 0xFFFFFFFF)
        _optional_integer(self.encoding_bits, "encoding", 1, 0xFFFF)
        _optional_integer(self.frames_per_packet, "frames per packet", 1, 0xFFFF)
        if self.secondary_destination is not None and self.primary_destination is None:
            raise ValueError("secondary destination requires a primary destination")
        if self.redundancy is RedundancyConstraint.NONE and self.secondary_destination is not None:
            raise ValueError("redundancy none forbids a secondary destination")
        if self.redundancy is RedundancyConstraint.REQUIRED and self.secondary_destination is None:
            raise ValueError("redundancy required needs a secondary destination")
        if self.secondary_destination is not None:
            if self.secondary_destination == self.primary_destination:
                raise ValueError("primary and secondary destinations must differ")
            primary_interface = self.primary_destination.interface if self.primary_destination else None
            if primary_interface and self.secondary_destination.interface == primary_interface:
                raise ValueError("primary and secondary destinations must use different interfaces")
        for destination in (self.primary_destination, self.secondary_destination):
            if destination is None:
                continue
            address = ipaddress.IPv4Address(destination.address)
            if self.flow_type is FlowType.MULTICAST and not address.is_multicast:
                raise ValueError("multicast flow destinations must use multicast IPv4 addresses")
            if self.flow_type is FlowType.UNICAST and address.is_multicast:
                raise ValueError("unicast flow destinations must not use multicast IPv4 addresses")
        if not isinstance(self.identity, FlowIdentity):
            raise ValueError("identity must be a flow identity")
        if not isinstance(self.protocol, FlowProtocolRequirements):
            raise ValueError("protocol must be flow protocol requirements")
        if not isinstance(self.raw_fields, dict) or not isinstance(self.extra_fields, dict):
            raise ValueError("raw_fields and extra_fields must be objects")

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
        media_mode: MediaMode | None = None,
    ) -> TransmitFlowSpecification:
        record = _mapping(record, "transmitter flow inventory record")
        channels = record.get("transmitter_channel_ids_by_slot")
        if not isinstance(channels, list):
            channels = record.get("channels")
        if not isinstance(channels, list) or not channels:
            raise ValueError("transmitter flow inventory has no ordered channel-slot mapping")
        populated = [(index, channel) for index, channel in enumerate(channels, start=1) if channel]
        if not populated:
            raise ValueError("transmitter flow inventory has no populated channel slots")
        flow_type_value = record.get("flow_type")
        if flow_type_value not in {item.value for item in FlowType}:
            raise ValueError("transmitter flow inventory has an unknown flow type")
        media_mode_value = record.get("media_mode")
        if media_mode_value is None:
            observed_media_mode = media_mode or (
                MediaMode.NATIVE_DANTE if protocol_id in (0x2729, 0x2801) else MediaMode.UNKNOWN
            )
        else:
            try:
                observed_media_mode = MediaMode(media_mode_value)
            except (TypeError, ValueError) as exception:
                raise ValueError("transmitter flow inventory has an unknown media mode") from exception
        primary_value = record.get("primary_destination")
        secondary_value = record.get("secondary_destination")
        if primary_value is not None:
            primary_destination = FlowSocket.from_dict(_mapping(primary_value, "primary destination"))
        else:
            address = record.get("destination_internet_protocol_version_four_address")
            port = record.get("destination_user_datagram_port")
            primary_destination = FlowSocket(str(address), port) if address and port else None
        secondary_destination = (
            FlowSocket.from_dict(_mapping(secondary_value, "secondary destination"))
            if secondary_value is not None
            else None
        )
        global_identifier = record.get("global_flow_id", record.get("flow_number"))
        known_protocol_cohort = {
            0x2729: "legacy_2729",
            0x2801: "legacy_2801",
            0x2809: "modern_2809",
        }.get(protocol_id, "unknown")
        return cls(
            media_mode=observed_media_mode,
            flow_type=FlowType(flow_type_value),
            name=record.get("flow_name"),
            channel_slots=tuple(
                TransmitterChannelSlot(slot=index, transmitter_channel=channel) for index, channel in populated
            ),
            sample_rate_hz=record.get("sample_rate"),
            encoding_bits=record.get("encoding"),
            frames_per_packet=record.get("frames_per_packet"),
            primary_destination=primary_destination,
            secondary_destination=secondary_destination,
            redundancy=RedundancyConstraint.DEVICE_DEFAULT,
            identity=FlowIdentity(
                global_flow_id=global_identifier,
                media_type_code=record.get("media_type_code"),
                media_local_flow_id=record.get("media_local_flow_id"),
            ),
            protocol=FlowProtocolRequirements(protocol_id=protocol_id, cohort=known_protocol_cohort),
            raw_fields=copy.deepcopy(dict(record)),
        )

    @property
    def channels(self) -> list[int]:
        return [item.transmitter_channel for item in self.channel_slots]

    def to_dict(self) -> dict[str, Any]:
        value = copy.deepcopy(self.extra_fields)
        value.update(
            {
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
        )
        return value


@dataclass(frozen=True)
class FlowDifference:
    field: str
    requested: Any
    effective: Any

    def to_dict(self) -> dict[str, Any]:
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "matches": self.matches,
            "differences": [item.to_dict() for item in self.differences],
            "unavailable_fields": list(self.unavailable_fields),
        }


def compare_transmit_flows(
    requested: TransmitFlowSpecification,
    effective: TransmitFlowSpecification,
) -> FlowComparison:
    differences = []
    unavailable_fields = []
    raw = effective.raw_fields
    inventory_evidence = bool(raw)

    def observed(*field_names: str, inferred: bool = False) -> bool:
        return not inventory_evidence or inferred or any(field_name in raw for field_name in field_names)

    def compare(
        field_name: str,
        requested_value: Any,
        effective_value: Any,
        *,
        optional: bool = False,
        is_observed: bool = True,
    ) -> None:
        if optional and requested_value is None:
            return
        if not is_observed:
            unavailable_fields.append(field_name)
            return
        if requested_value != effective_value:
            differences.append(FlowDifference(field_name, requested_value, effective_value))

    compare(
        "media_mode",
        requested.media_mode.value,
        effective.media_mode.value,
        is_observed=observed(
            "media_mode",
            inferred=(
                effective.protocol.protocol_id in (0x2729, 0x2801) and effective.media_mode is MediaMode.NATIVE_DANTE
            ),
        ),
    )
    compare(
        "flow_type",
        requested.flow_type.value,
        effective.flow_type.value,
        is_observed=observed("flow_type", "flow_type_code"),
    )
    compare(
        "name",
        requested.name,
        effective.name,
        optional=True,
        is_observed=observed("flow_name"),
    )
    compare(
        "channel_slots",
        [item.to_dict() for item in requested.channel_slots],
        [item.to_dict() for item in effective.channel_slots],
        is_observed=observed("transmitter_channel_ids_by_slot", "channels"),
    )
    compare(
        "sample_rate_hz",
        requested.sample_rate_hz,
        effective.sample_rate_hz,
        optional=True,
        is_observed=observed("sample_rate"),
    )
    compare(
        "encoding_bits",
        requested.encoding_bits,
        effective.encoding_bits,
        optional=True,
        is_observed=observed("encoding"),
    )
    compare(
        "frames_per_packet",
        requested.frames_per_packet,
        effective.frames_per_packet,
        optional=True,
        is_observed=observed("frames_per_packet"),
    )
    compare(
        "primary_destination",
        requested.primary_destination.to_dict() if requested.primary_destination else None,
        effective.primary_destination.to_dict() if effective.primary_destination else None,
        optional=True,
        is_observed=observed(
            "primary_destination",
            inferred=(
                "destination_internet_protocol_version_four_address" in raw and "destination_user_datagram_port" in raw
            ),
        ),
    )
    compare(
        "secondary_destination",
        requested.secondary_destination.to_dict() if requested.secondary_destination else None,
        effective.secondary_destination.to_dict() if effective.secondary_destination else None,
        optional=True,
        is_observed=observed("secondary_destination"),
    )
    if requested.redundancy is not RedundancyConstraint.DEVICE_DEFAULT:
        compare("redundancy", requested.redundancy.value, effective.redundancy.value)
    compare(
        "identity.global_flow_id",
        requested.identity.global_flow_id,
        effective.identity.global_flow_id,
        optional=True,
        is_observed=observed("global_flow_id", "flow_number"),
    )
    compare(
        "identity.media_type_code",
        requested.identity.media_type_code,
        effective.identity.media_type_code,
        optional=True,
        is_observed=observed("media_type_code"),
    )
    compare(
        "identity.media_local_flow_id",
        requested.identity.media_local_flow_id,
        effective.identity.media_local_flow_id,
        optional=True,
        is_observed=observed("media_local_flow_id"),
    )
    compare(
        "protocol.protocol_id",
        requested.protocol.protocol_id,
        effective.protocol.protocol_id,
        optional=True,
        is_observed=True,
    )
    return FlowComparison(
        matches=not differences and not unavailable_fields,
        differences=tuple(differences),
        unavailable_fields=tuple(unavailable_fields),
    )


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
    wire_options: dict[str, Any] = field(default_factory=dict)
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
            "wire_options": copy.deepcopy(self.wire_options),
            "wire_authored_fields": list(self.wire_authored_fields),
            "state_preconditions": copy.deepcopy(self.state_preconditions),
        }


@dataclass(frozen=True)
class FlowOperationResult:
    operation: str
    state: FlowLifecycleState
    transport: str
    request_acknowledgement: dict[str, Any] | None
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


def packet_time_to_frames(packet_time_microseconds: float | int | None, sample_rate_hz: int | None) -> int | None:
    if packet_time_microseconds is None or sample_rate_hz is None:
        return None
    if isinstance(packet_time_microseconds, bool) or not isinstance(packet_time_microseconds, (int, float)):
        raise ValueError("packet time must be a number")
    if not math.isfinite(float(packet_time_microseconds)) or packet_time_microseconds <= 0:
        raise ValueError("packet time must be positive and finite")
    frames = float(packet_time_microseconds) * sample_rate_hz / 1_000_000
    rounded = round(frames)
    if not math.isclose(frames, rounded, rel_tol=0, abs_tol=1e-9):
        raise ValueError("packet time does not represent a whole number of frames")
    return _integer(rounded, "frames per packet", 1, 0xFFFF)
