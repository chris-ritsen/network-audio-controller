# Generated from Rust schemas by scripts/generate_core_types.py. Do not edit.
import typing as _typing
import typing_extensions as _extensions


class Availability(_extensions.TypedDict):
    read_only: _typing.Union[bool, None]
    readable: bool
    reasons: list[str]
    supported: _typing.Union[bool, None]
    writable: bool
    write_permitted: bool


class ClockSourceChoice(_extensions.TypedDict):
    code: int
    label: str


class ClockSources(_extensions.TypedDict):
    choices: list[ClockSourceChoice]
    current: _typing.Union[str, None]


class ClockSubdomainPresentation(_extensions.TypedDict):
    label: str
    text: _typing.Union[str, None]


class MeteringValue(_extensions.TypedDict):
    dbfs: _typing.Union[float, None]
    display_state: str
    raw: int
    source: str
    state: str


class MeteringScales(_extensions.TypedDict):
    detailed: list[MeteringValue]
    signal_presence: list[MeteringValue]


SubscriptionTransport = _typing.Literal["unicast", "multicast"]


class SubscriptionStatus(_extensions.TypedDict):
    code: int
    detail: _typing.Union[str, None]
    interpretation: str
    label: str
    observed_summary: _typing.Union[str, None]
    receiver_status_code: _typing.Union[int, None]
    settled: bool
    severity: str
    state: str
    status: _typing.Union[str, None]
    transport: _typing.Union[SubscriptionTransport, None]
