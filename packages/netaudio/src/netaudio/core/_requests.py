# Generated from Rust schemas by scripts/generate_core_types.py. Do not edit.
import typing as _typing
import typing_extensions as _extensions


JsonValue = _typing.Union[None, bool, int, float, str, list["JsonValue"], dict[str, "JsonValue"]]


class AudioCapabilityControl(_extensions.TypedDict):
    available_values: _extensions.NotRequired[_typing.Union[list[int], None]]
    host_disabled: _extensions.NotRequired[_typing.Union[bool, None]]
    requested_value: _extensions.NotRequired[_typing.Union[int, None]]
    update_mode: _extensions.NotRequired[_typing.Union[int, None]]


Operation = _typing.Literal[
    "identify",
    "sample_rate",
    "encoding",
    "sample_rate_pullup",
    "aes67",
    "static_ipv4",
    "redundancy",
    "codec_control",
    "locking",
]


class ReportedSource(_extensions.TypedDict):
    field_applicable: _extensions.NotRequired[_typing.Union[bool, None]]
    field_reported: _extensions.NotRequired[_typing.Union[bool, None]]
    fresh: _extensions.NotRequired[_typing.Union[bool, None]]


DanteRedundancyMode = _typing.Literal["switched", "redundant", "split_redundant"]


class RedundancyReadback(_extensions.TypedDict):
    after_interfaces: list[JsonValue]
    before_interfaces: list[JsonValue]
    before_mode: DanteRedundancyMode


class RedundancyControlRequest(_extensions.TypedDict):
    mode: _extensions.NotRequired[_typing.Union[DanteRedundancyMode, None]]
    readback: _extensions.NotRequired[_typing.Union[RedundancyReadback, None]]
    state: JsonValue


class AvailabilityRequest(_extensions.TypedDict):
    audio: _extensions.NotRequired[_typing.Union[AudioCapabilityControl, None]]
    has_adapter: bool
    locked: _extensions.NotRequired[_typing.Union[bool, None]]
    managed: bool
    operation: Operation
    permission: _extensions.NotRequired[_typing.Union[bool, None]]
    read_only: _extensions.NotRequired[_typing.Union[bool, None]]
    read_only_source: _extensions.NotRequired[_typing.Union[ReportedSource, None]]
    readable: bool
    redundancy: _extensions.NotRequired[_typing.Union[RedundancyControlRequest, None]]
    supported: _extensions.NotRequired[_typing.Union[bool, None]]
    supported_source: _extensions.NotRequired[_typing.Union[ReportedSource, None]]
    transport_available: bool


class ClockSourceFacts(_extensions.TypedDict):
    current: _extensions.NotRequired[JsonValue]
    supported: list[JsonValue]


class DeviceIdentityRequestMac(_extensions.TypedDict):
    kind: _typing.Literal["mac"]
    value: JsonValue


class DeviceIdentityRequestPtpv1(_extensions.TypedDict):
    kind: _typing.Literal["ptpv1"]
    value: JsonValue


class DeviceIdentityRequestManagedDevice(_extensions.TypedDict):
    kind: _typing.Literal["managed_device"]
    value: JsonValue


class DeviceIdentityRequestManagedInventory(_extensions.TypedDict):
    kind: _typing.Literal["managed_inventory"]
    value: JsonValue


class DeviceIdentityRequestManagedDomain(_extensions.TypedDict):
    kind: _typing.Literal["managed_domain"]
    value: JsonValue


class DeviceIdentityRequestManagedPrimary(_extensions.TypedDict):
    kind: _typing.Literal["managed_primary"]
    value: JsonValue


DeviceIdentityRequest = _typing.Union[
    DeviceIdentityRequestMac,
    DeviceIdentityRequestPtpv1,
    DeviceIdentityRequestManagedDevice,
    DeviceIdentityRequestManagedInventory,
    DeviceIdentityRequestManagedDomain,
    DeviceIdentityRequestManagedPrimary,
]
