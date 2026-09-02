from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping, TypeVar, overload


class ModelDecodeError(ValueError):
    pass


@dataclass(frozen=True)
class GraphQLIssue:
    message: str
    raw: Mapping[str, Any]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> GraphQLIssue:
        message = value.get("message")
        if not isinstance(message, str):
            raise ModelDecodeError("GraphQL error message must be a string")
        return cls(message=message, raw=dict(value))


@dataclass(frozen=True)
class DeviceIdentity:
    id: str
    instance_id: str
    default_name: str | None
    actual_name: str | None
    product_model_id: str | None
    product_model_name: str | None
    product_version: str | None
    product_software_version: str | None
    dante_version: str | None
    dante_hardware_version: str | None


@dataclass(frozen=True)
class NamedEntity:
    id: str
    name: str


@dataclass(frozen=True)
class Platform:
    id: str
    name: str
    platform_id: str | None


@dataclass(frozen=True)
class NetworkInterface:
    id: str
    mac_address: str | None
    address: str | None
    netmask: int | None
    subnet: str | None


@dataclass(frozen=True)
class DeviceConnection:
    id: str
    state: str | None
    last_changed: str | None


@dataclass(frozen=True)
class ClockPreferences:
    id: str
    external_word_clock: bool | None
    leader: bool | None
    unicast_clocking: bool | None
    v1_unicast_delay_requests: bool | None


@dataclass(frozen=True)
class Capabilities:
    id: str
    can_write_preferred_master: bool | None
    can_write_external_word_clock: bool | None
    can_write_follower_only: bool | None
    can_write_unicast_delay_requests: bool | None
    can_unicast_clocking: bool | None
    ddm_v1_1_clock_messages_supported: bool | None
    can_encrypt_media: bool | None
    can_reset: bool | None
    rtp_audio_supported: bool | None
    rtp_audio_support_suppressed: bool | None
    media_types: int | None


@dataclass(frozen=True)
class ClockingState:
    id: str
    locked: str | None
    grand_leader: bool | None
    follower_without_leader: bool | None
    multicast_leader: bool | None
    unicast_leader: bool | None
    unicast_follower: bool | None
    mute_status: str | None
    frequency_offset: int | None


@dataclass(frozen=True)
class AlertMessage:
    id: str
    connectivity: str | None
    clocking: str | None
    latency: str | None
    subscriptions: str | None


@dataclass(frozen=True)
class DeviceStatus:
    id: str
    summary: str | None
    clocking: str | None
    connectivity: str | None
    latency: str | None
    subscriptions: str | None
    alert_message: AlertMessage | None


@dataclass(frozen=True)
class DomainStatus:
    summary: str | None
    clocking: str | None
    connectivity: str | None
    latency: str | None
    subscriptions: str | None


@dataclass(frozen=True)
class SignalPresence:
    id: str
    level_dbfs: float
    status: str


@dataclass(frozen=True)
class RxChannel:
    id: str
    index: int
    enabled: bool | None
    name: str | None
    subscribed_device: str | None
    subscribed_channel: str | None
    status: str | None
    status_message: str | None
    summary: str | None
    media_type: str | None
    encryption_scheme: str | None
    can_subscribe_self: bool | None
    signal_presence: SignalPresence | None


@dataclass(frozen=True)
class TxChannel:
    id: str
    index: int
    name: str | None
    media_type: str | None
    encryption_policy: str | None
    signal_presence: SignalPresence | None


@dataclass(frozen=True)
class DeviceParameter:
    typename: str
    id: str
    path: str
    key: str
    value: Any
    label: str
    settable: bool
    default_value: Any
    units: str | None
    apply_mode: str | None
    group: str | None
    rendering_hint: str | None
    options: tuple[Any, ...] | None
    minimum: Any
    maximum: Any
    precision: Any
    regex: str | None


@dataclass(frozen=True)
class DevicePort:
    typename: str
    id: str
    key: str
    parameters: tuple[DeviceParameter | None, ...] | None


@dataclass(frozen=True)
class Device:
    id: str
    name: str
    domain_id: str | None
    type: str | None
    enrolment_state: str | None
    identity: DeviceIdentity | None
    manufacturer: NamedEntity | None
    platform: Platform | None
    product: NamedEntity | None
    interfaces: tuple[NetworkInterface | None, ...] | None
    connection: DeviceConnection | None
    clock_preferences: ClockPreferences | None
    capabilities: Capabilities | None
    clocking_state: ClockingState | None
    status: DeviceStatus | None
    rx_channels: tuple[RxChannel, ...] | None
    tx_channels: tuple[TxChannel, ...] | None
    parameters: tuple[DeviceParameter, ...] | None
    inputs: tuple[DevicePort | None, ...] | None
    outputs: tuple[DevicePort | None, ...] | None


@dataclass(frozen=True)
class Domain:
    id: str
    name: str | None
    status: DomainStatus | None
    devices: tuple[Device | None, ...] | None


@dataclass(frozen=True)
class Inventory:
    domains: tuple[Domain | None, ...] | None
    unenrolled_devices: tuple[Device, ...] | None


T = TypeVar("T")


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ModelDecodeError(f"{path} must be an object")
    return value


def _required(mapping: Mapping[str, Any], key: str, path: str) -> Any:
    if key not in mapping or mapping[key] is None:
        raise ModelDecodeError(f"{path}.{key} is required")
    return mapping[key]


def _required_str(mapping: Mapping[str, Any], key: str, path: str) -> str:
    value = _required(mapping, key, path)
    if not isinstance(value, str):
        raise ModelDecodeError(f"{path}.{key} must be a string")
    return value


def _required_int(mapping: Mapping[str, Any], key: str, path: str) -> int:
    value = _required(mapping, key, path)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ModelDecodeError(f"{path}.{key} must be an integer")
    return value


def _required_bool(mapping: Mapping[str, Any], key: str, path: str) -> bool:
    value = _required(mapping, key, path)
    if not isinstance(value, bool):
        raise ModelDecodeError(f"{path}.{key} must be a boolean")
    return value


def _optional_str(mapping: Mapping[str, Any], key: str, path: str) -> str | None:
    value = mapping.get(key)
    if value is not None and not isinstance(value, str):
        raise ModelDecodeError(f"{path}.{key} must be a string or null")
    return value


def _optional_int(mapping: Mapping[str, Any], key: str, path: str) -> int | None:
    value = mapping.get(key)
    if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
        raise ModelDecodeError(f"{path}.{key} must be an integer or null")
    return value


def _optional_bool(mapping: Mapping[str, Any], key: str, path: str) -> bool | None:
    value = mapping.get(key)
    if value is not None and not isinstance(value, bool):
        raise ModelDecodeError(f"{path}.{key} must be a boolean or null")
    return value


def _optional_model(
    mapping: Mapping[str, Any],
    key: str,
    path: str,
    parser: Callable[[Mapping[str, Any], str], T],
) -> T | None:
    value = mapping.get(key)
    if value is None:
        return None
    item_path = f"{path}.{key}"
    return parser(_mapping(value, item_path), item_path)


@overload
def _optional_list(
    mapping: Mapping[str, Any],
    key: str,
    path: str,
    parser: Callable[[Mapping[str, Any], str], T],
    *,
    allow_null_items: Literal[False] = False,
) -> tuple[T, ...] | None: ...


@overload
def _optional_list(
    mapping: Mapping[str, Any],
    key: str,
    path: str,
    parser: Callable[[Mapping[str, Any], str], T],
    *,
    allow_null_items: Literal[True],
) -> tuple[T | None, ...] | None: ...


def _optional_list(
    mapping: Mapping[str, Any],
    key: str,
    path: str,
    parser: Callable[[Mapping[str, Any], str], T],
    *,
    allow_null_items: bool = False,
) -> tuple[T | None, ...] | None:
    value = mapping.get(key)
    if value is None:
        return None
    if not isinstance(value, list):
        raise ModelDecodeError(f"{path}.{key} must be a list or null")
    items: list[T | None] = []
    for index, item in enumerate(value):
        item_path = f"{path}.{key}[{index}]"
        if item is None and allow_null_items:
            items.append(None)
        else:
            items.append(parser(_mapping(item, item_path), item_path))
    return tuple(items)


def _parse_identity(value: Mapping[str, Any], path: str) -> DeviceIdentity:
    return DeviceIdentity(
        id=_required_str(value, "id", path),
        instance_id=_required_str(value, "instanceId", path),
        default_name=_optional_str(value, "defaultName", path),
        actual_name=_optional_str(value, "actualName", path),
        product_model_id=_optional_str(value, "productModelId", path),
        product_model_name=_optional_str(value, "productModelName", path),
        product_version=_optional_str(value, "productVersion", path),
        product_software_version=_optional_str(value, "productSoftwareVersion", path),
        dante_version=_optional_str(value, "danteVersion", path),
        dante_hardware_version=_optional_str(value, "danteHardwareVersion", path),
    )


def _parse_named_entity(value: Mapping[str, Any], path: str) -> NamedEntity:
    return NamedEntity(id=_required_str(value, "id", path), name=_required_str(value, "name", path))


def _parse_platform(value: Mapping[str, Any], path: str) -> Platform:
    return Platform(
        id=_required_str(value, "id", path),
        name=_required_str(value, "name", path),
        platform_id=_optional_str(value, "platformId", path),
    )


def _parse_interface(value: Mapping[str, Any], path: str) -> NetworkInterface:
    return NetworkInterface(
        id=_required_str(value, "id", path),
        mac_address=_optional_str(value, "macAddress", path),
        address=_optional_str(value, "address", path),
        netmask=_optional_int(value, "netmask", path),
        subnet=_optional_str(value, "subnet", path),
    )


def _parse_connection(value: Mapping[str, Any], path: str) -> DeviceConnection:
    return DeviceConnection(
        id=_required_str(value, "id", path),
        state=_optional_str(value, "state", path),
        last_changed=_optional_str(value, "lastChanged", path),
    )


def _parse_clock_preferences(value: Mapping[str, Any], path: str) -> ClockPreferences:
    return ClockPreferences(
        id=_required_str(value, "id", path),
        external_word_clock=_optional_bool(value, "externalWordClock", path),
        leader=_optional_bool(value, "leader", path),
        unicast_clocking=_optional_bool(value, "unicastClocking", path),
        v1_unicast_delay_requests=_optional_bool(value, "v1UnicastDelayRequests", path),
    )


def _parse_capabilities(value: Mapping[str, Any], path: str) -> Capabilities:
    return Capabilities(
        id=_required_str(value, "id", path),
        can_write_preferred_master=_optional_bool(value, "CAN_WRITE_PREFERRED_MASTER", path),
        can_write_external_word_clock=_optional_bool(value, "CAN_WRITE_EXT_WORD_CLOCK", path),
        can_write_follower_only=_optional_bool(value, "CAN_WRITE_SLAVE_ONLY", path),
        can_write_unicast_delay_requests=_optional_bool(value, "CAN_WRITE_UNICAST_DELAY_REQUESTS", path),
        can_unicast_clocking=_optional_bool(value, "CAN_UNICAST_CLOCKING", path),
        ddm_v1_1_clock_messages_supported=_optional_bool(value, "DDM_V_1_1_CLOCK_MESSAGES_SUPPORTED", path),
        can_encrypt_media=_optional_bool(value, "CAN_ENCRYPT_MEDIA", path),
        can_reset=_optional_bool(value, "CAN_RESET", path),
        rtp_audio_supported=_optional_bool(value, "RTP_AUDIO_SUPPORTED", path),
        rtp_audio_support_suppressed=_optional_bool(value, "RTP_AUDIO_SUPPORT_SUPPRESSED", path),
        media_types=_optional_int(value, "mediaTypes", path),
    )


def _parse_clocking_state(value: Mapping[str, Any], path: str) -> ClockingState:
    return ClockingState(
        id=_required_str(value, "id", path),
        locked=_optional_str(value, "locked", path),
        grand_leader=_optional_bool(value, "grandLeader", path),
        follower_without_leader=_optional_bool(value, "followerWithoutLeader", path),
        multicast_leader=_optional_bool(value, "multicastLeader", path),
        unicast_leader=_optional_bool(value, "unicastLeader", path),
        unicast_follower=_optional_bool(value, "unicastFollower", path),
        mute_status=_optional_str(value, "muteStatus", path),
        frequency_offset=_optional_int(value, "frequencyOffset", path),
    )


def _parse_alert_message(value: Mapping[str, Any], path: str) -> AlertMessage:
    return AlertMessage(
        id=_required_str(value, "id", path),
        connectivity=_optional_str(value, "connectivity", path),
        clocking=_optional_str(value, "clocking", path),
        latency=_optional_str(value, "latency", path),
        subscriptions=_optional_str(value, "subscriptions", path),
    )


def _parse_device_status(value: Mapping[str, Any], path: str) -> DeviceStatus:
    return DeviceStatus(
        id=_required_str(value, "id", path),
        summary=_optional_str(value, "summary", path),
        clocking=_optional_str(value, "clocking", path),
        connectivity=_optional_str(value, "connectivity", path),
        latency=_optional_str(value, "latency", path),
        subscriptions=_optional_str(value, "subscriptions", path),
        alert_message=_optional_model(value, "alertMessage", path, _parse_alert_message),
    )


def _parse_domain_status(value: Mapping[str, Any], path: str) -> DomainStatus:
    return DomainStatus(
        summary=_optional_str(value, "summary", path),
        clocking=_optional_str(value, "clocking", path),
        connectivity=_optional_str(value, "connectivity", path),
        latency=_optional_str(value, "latency", path),
        subscriptions=_optional_str(value, "subscriptions", path),
    )


def _parse_signal_presence(value: Mapping[str, Any], path: str) -> SignalPresence:
    level = _required(value, "leveldBFS", path)
    if isinstance(level, bool) or not isinstance(level, (int, float)):
        raise ModelDecodeError(f"{path}.leveldBFS must be a number")
    return SignalPresence(
        id=_required_str(value, "id", path),
        level_dbfs=float(level),
        status=_required_str(value, "status", path),
    )


def _parse_rx_channel(value: Mapping[str, Any], path: str) -> RxChannel:
    return RxChannel(
        id=_required_str(value, "id", path),
        index=_required_int(value, "index", path),
        enabled=_optional_bool(value, "enabled", path),
        name=_optional_str(value, "name", path),
        subscribed_device=_optional_str(value, "subscribedDevice", path),
        subscribed_channel=_optional_str(value, "subscribedChannel", path),
        status=_optional_str(value, "status", path),
        status_message=_optional_str(value, "statusMessage", path),
        summary=_optional_str(value, "summary", path),
        media_type=_optional_str(value, "mediaType", path),
        encryption_scheme=_optional_str(value, "encryptionScheme", path),
        can_subscribe_self=_optional_bool(value, "canSubscribeSelf", path),
        signal_presence=_optional_model(value, "signalPresence", path, _parse_signal_presence),
    )


def _parse_tx_channel(value: Mapping[str, Any], path: str) -> TxChannel:
    return TxChannel(
        id=_required_str(value, "id", path),
        index=_required_int(value, "index", path),
        name=_optional_str(value, "name", path),
        media_type=_optional_str(value, "mediaType", path),
        encryption_policy=_optional_str(value, "encryptionPolicy", path),
        signal_presence=_optional_model(value, "signalPresence", path, _parse_signal_presence),
    )


def _optional_values(value: Mapping[str, Any], key: str, path: str) -> tuple[Any, ...] | None:
    options = value.get(key)
    if options is None:
        return None
    if not isinstance(options, list):
        raise ModelDecodeError(f"{path}.{key} must be a list or null")
    return tuple(options)


def _parse_parameter(value: Mapping[str, Any], path: str) -> DeviceParameter:
    return DeviceParameter(
        typename=_required_str(value, "__typename", path),
        id=_required_str(value, "id", path),
        path=_required_str(value, "path", path),
        key=_required_str(value, "key", path),
        value=_required(value, "value", path),
        label=_required_str(value, "label", path),
        settable=_required_bool(value, "settable", path),
        default_value=value.get("defaultValue"),
        units=_optional_str(value, "units", path),
        apply_mode=_optional_str(value, "applyMode", path),
        group=_optional_str(value, "group", path),
        rendering_hint=_optional_str(value, "renderingHint", path),
        options=_optional_values(value, "options", path),
        minimum=value.get("min"),
        maximum=value.get("max"),
        precision=value.get("precision"),
        regex=_optional_str(value, "regex", path),
    )


def _parse_port(value: Mapping[str, Any], path: str) -> DevicePort:
    return DevicePort(
        typename=_required_str(value, "__typename", path),
        id=_required_str(value, "id", path),
        key=_required_str(value, "key", path),
        parameters=_optional_list(value, "parameters", path, _parse_parameter, allow_null_items=True),
    )


def parse_device(value: Mapping[str, Any], path: str) -> Device:
    return Device(
        id=_required_str(value, "id", path),
        name=_required_str(value, "name", path),
        domain_id=_optional_str(value, "domainId", path),
        type=_optional_str(value, "type", path),
        enrolment_state=_optional_str(value, "enrolmentState", path),
        identity=_optional_model(value, "identity", path, _parse_identity),
        manufacturer=_optional_model(value, "manufacturer", path, _parse_named_entity),
        platform=_optional_model(value, "platform", path, _parse_platform),
        product=_optional_model(value, "product", path, _parse_named_entity),
        interfaces=_optional_list(value, "interfaces", path, _parse_interface, allow_null_items=True),
        connection=_optional_model(value, "connection", path, _parse_connection),
        clock_preferences=_optional_model(value, "clockPreferences", path, _parse_clock_preferences),
        capabilities=_optional_model(value, "capabilities", path, _parse_capabilities),
        clocking_state=_optional_model(value, "clockingState", path, _parse_clocking_state),
        status=_optional_model(value, "status", path, _parse_device_status),
        rx_channels=_optional_list(value, "rxChannels", path, _parse_rx_channel),
        tx_channels=_optional_list(value, "txChannels", path, _parse_tx_channel),
        parameters=_optional_list(value, "parameters", path, _parse_parameter),
        inputs=_optional_list(value, "inputs", path, _parse_port, allow_null_items=True),
        outputs=_optional_list(value, "outputs", path, _parse_port, allow_null_items=True),
    )


def _parse_domain(value: Mapping[str, Any], path: str) -> Domain:
    return Domain(
        id=_required_str(value, "id", path),
        name=_optional_str(value, "name", path),
        status=_optional_model(value, "status", path, _parse_domain_status),
        devices=_optional_list(value, "devices", path, parse_device, allow_null_items=True),
    )


def parse_inventory(value: Mapping[str, Any], *, allow_missing_roots: bool = False) -> Inventory:
    if not allow_missing_roots and ("domains" not in value or "unenrolledDevices" not in value):
        raise ModelDecodeError("inventory data omitted a requested root")
    if not allow_missing_roots and (value["domains"] is None or value["unenrolledDevices"] is None):
        raise ModelDecodeError("inventory data roots must be non-null lists")
    domains = _optional_list(
        value,
        "domains",
        "data",
        _parse_domain,
        allow_null_items=True,
    )
    unenrolled = _optional_list(value, "unenrolledDevices", "data", parse_device)
    return Inventory(domains=domains, unenrolled_devices=unenrolled)


__all__ = [
    "AlertMessage",
    "Capabilities",
    "ClockPreferences",
    "ClockingState",
    "Device",
    "DeviceConnection",
    "DeviceIdentity",
    "DeviceParameter",
    "DevicePort",
    "DeviceStatus",
    "Domain",
    "DomainStatus",
    "GraphQLIssue",
    "Inventory",
    "ModelDecodeError",
    "NamedEntity",
    "NetworkInterface",
    "Platform",
    "RxChannel",
    "SignalPresence",
    "TxChannel",
    "parse_device",
    "parse_inventory",
]
