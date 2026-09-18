from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from enum import Enum
from functools import partial
from typing import Any, Awaitable, Callable

from netaudio.cli_support.execution import readback_after_notification
from netaudio.commands.config.readback import MUTATION_ERRORS
from netaudio.dante.network_configuration import NetworkConfigurationUnverified, validate_interface_configuration
from netaudio.dante.performance_configuration import requested_performance_properties
from netaudio.dante.flow_lifecycle import inspect_transmit_flows, plan_create_transmit_flow
from netaudio.dante.transmit_flow import TransmitFlowSpecification, compare_transmit_flows
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyChangedButUnverifiedError,
    SampleRateTopologyMutationOutcomeUnknownError,
)
from netaudio.monitoring import MonitoringEventKind


@dataclass
class MatchedPresetDevice:
    config: dict[str, Any]
    device: Any
    device_name: str
    server_name: str


class PresetActionState(str, Enum):
    CHANGE = "change"
    UNCHANGED = "unchanged"
    UNSUPPORTED = "unsupported"
    UNAVAILABLE = "unavailable"
    AMBIGUOUS = "ambiguous"


@dataclass
class PresetAction:
    kind: str
    payload: Any
    state: PresetActionState
    current: Any = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "requested": self.payload,
            "current": self.current,
            "state": self.state.value,
            "reason": self.reason,
        }


@dataclass
class PresetDeviceActions:
    actions: list[PresetAction]
    config: dict[str, Any]
    device: Any
    device_name: str
    server_name: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "device_name": self.device_name,
            "server_name": self.server_name,
            "actions": [action.to_dict() for action in self.actions],
        }


@dataclass
class PresetLoadPlan:
    device_actions: list[PresetDeviceActions]

    def to_dict(self) -> dict[str, Any]:
        return {"devices": [entry.to_dict() for entry in self.device_actions]}


@dataclass
class PresetOperationResult:
    device_name: str
    kind: str
    state: str
    requested: Any = None
    effective: Any = None
    request_acknowledgement: Any = None
    device_confirmation: bool | None = None
    effective_state_confirmation: bool | None = None
    persistence_request_acknowledgement: Any = None
    persistence_confirmation: bool | None = None
    verification_observations: list[dict[str, Any]] = field(default_factory=list)
    message: str = ""


@dataclass
class PresetLoadReport:
    failures: int = 0
    unverified: int = 0
    needs_reboot: list[str] = field(default_factory=list)
    results: list[tuple[str, str]] = field(default_factory=list)
    operations: list[PresetOperationResult] = field(default_factory=list)

    def record(self, device_name: str, text: str, *, failed: bool = False, verified: bool = True) -> None:
        if failed:
            self.failures += 1
        if not verified:
            self.unverified += 1
        self.results.append((device_name, text))

    def operation(
        self,
        device_name: str,
        kind: str,
        state: str,
        message: str,
        *,
        requested: Any = None,
        effective: Any = None,
        acknowledgement: Any = None,
        device_confirmation: bool | None = None,
        effective_state_confirmation: bool | None = None,
        persistence_request_acknowledgement: Any = None,
        persistence_confirmation: bool | None = None,
        verification_observations: list[dict[str, Any]] | None = None,
        failed: bool = False,
        verified: bool = True,
    ) -> None:
        self.operations.append(
            PresetOperationResult(
                device_name=device_name,
                kind=kind,
                state=state,
                requested=requested,
                effective=effective,
                request_acknowledgement=acknowledgement,
                device_confirmation=device_confirmation,
                effective_state_confirmation=effective_state_confirmation,
                persistence_request_acknowledgement=persistence_request_acknowledgement,
                persistence_confirmation=persistence_confirmation,
                verification_observations=verification_observations or [],
                message=message,
            )
        )
        self.record(device_name, message, failed=failed, verified=verified)


@dataclass
class PresetLoadContext:
    application: Any
    confirm_destructive: bool
    report: PresetLoadReport


ActionHandler = Callable[[PresetLoadContext, PresetDeviceActions, PresetAction], Awaitable[None]]


class PresetValidationError(ValueError):
    def __init__(self, lines: list[str]):
        self.lines = lines
        super().__init__("\n".join(lines))


READBACK_ERRORS = (*MUTATION_ERRORS, AttributeError, KeyError)


def _change_or_unchanged(
    kind: str,
    requested: Any,
    current: Any,
    *,
    matches: bool | None = None,
) -> PresetAction:
    if matches is None:
        matches = requested == current
    return PresetAction(
        kind=kind,
        payload=requested,
        current=current,
        state=PresetActionState.UNCHANGED if matches else PresetActionState.CHANGE,
        reason="fresh readback matches requested value" if matches else "fresh readback differs",
    )


def _unavailable(kind: str, requested: Any, reason: str, *, current: Any = None) -> PresetAction:
    return PresetAction(
        kind=kind,
        payload=requested,
        current=current,
        state=PresetActionState.UNAVAILABLE,
        reason=reason,
    )


def _unsupported(kind: str, requested: Any, reason: str, *, current: Any = None) -> PresetAction:
    return PresetAction(
        kind=kind,
        payload=requested,
        current=current,
        state=PresetActionState.UNSUPPORTED,
        reason=reason,
    )


def _ambiguous(kind: str, requested: Any, reason: str, *, current: Any = None) -> PresetAction:
    return PresetAction(
        kind=kind,
        payload=requested,
        current=current,
        state=PresetActionState.AMBIGUOUS,
        reason=reason,
    )


def _validate_config(device_name: str, config: dict) -> None:
    sample_rate = config.get("sample_rate")
    if sample_rate is not None and (
        isinstance(sample_rate, bool) or not isinstance(sample_rate, int) or not 1 <= sample_rate <= 0xFFFFFFFF
    ):
        raise ValueError(f"{device_name}: sample rate must be an integer from 1 through 4294967295")
    encoding = config.get("encoding")
    if encoding is not None and (
        isinstance(encoding, bool) or not isinstance(encoding, int) or not 1 <= encoding <= 0xFFFFFFFF
    ):
        raise ValueError(f"{device_name}: encoding must be an integer from 1 through 4294967295")
    latency = config.get("latency")
    if latency is not None and (
        isinstance(latency, bool) or not isinstance(latency, (int, float)) or not math.isfinite(latency) or latency < 0
    ):
        raise ValueError(f"{device_name}: latency must be a finite, nonnegative number")
    for interface in _interface_entries(config):
        mode = "dhcp" if interface["mode"] in ("dynamic", "dhcp") else interface["mode"]
        try:
            validate_interface_configuration(mode, interface if mode == "static" else None)
        except (TypeError, ValueError) as exception:
            raise ValueError(f"{device_name}: interface {interface['identity']}: {exception}") from exception


def _interface_entries(config: dict) -> list[dict[str, Any]]:
    if "interfaces" in config:
        return config["interfaces"]
    if "interface_mode" not in config:
        return []
    return [
        {
            "identity": "primary",
            "mode": config["interface_mode"],
            **{key: config[key] for key in ("ip_address", "netmask", "gateway", "dns_server") if key in config},
        }
    ]


def _interface_payload(interface: dict[str, Any]) -> dict[str, Any]:
    mode = interface["mode"]
    identity = interface["identity"]
    if mode in ("dynamic", "dhcp"):
        return {"identity": identity, "mode": "dhcp", "configuration": None}
    return {
        "identity": identity,
        "mode": "static",
        "configuration": {
            "dns_server": interface["dns_server"],
            "gateway": interface["gateway"],
            "ip_address": interface["ip_address"],
            "netmask": interface["netmask"],
        },
    }


def _expected_interface_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if payload["mode"] == "dhcp":
        return {"mode": "dynamic"}
    return {"mode": "static", **payload["configuration"]}


async def _plan_device_name(device, requested: str) -> PresetAction:
    try:
        current = await device.fetch_device_name()
    except READBACK_ERRORS as exception:
        return _unavailable("device_name", requested, f"fresh device-name readback failed: {exception}")
    if not isinstance(current, str) or not current:
        return _unavailable("device_name", requested, "fresh device-name readback was unavailable")
    return _change_or_unchanged("device_name", requested, current)


async def _plan_redundancy(application, device, requested: str) -> PresetAction:
    try:
        status = await application.probe_dante_redundancy(device, timeout=2.0)
    except READBACK_ERRORS as exception:
        return _unavailable("redundancy", requested, f"fresh redundancy readback failed: {exception}")
    if isinstance(status, dict) and status.get("advertised_support") is False:
        return _unsupported("redundancy", requested, "device explicitly reports redundancy configuration unsupported")
    if not isinstance(status, dict) or status.get("advertised_support") is not True:
        return _unavailable(
            "redundancy",
            requested,
            "redundancy capability support is unknown (capability_unknown)",
        )
    if status.get("configured_mode") is None:
        return _unavailable("redundancy", requested, "fresh configured redundancy was unavailable")
    current = status["configured_mode"]
    available_modes = status.get("available_modes")
    if not isinstance(available_modes, list) or status.get("available_modes_fresh") is not True:
        return _unavailable(
            "redundancy",
            requested,
            "redundancy capability values were unavailable",
            current=current,
        )
    supported = [
        choice.get("mode") for choice in available_modes if isinstance(choice, dict) and choice.get("mode") is not None
    ]
    if requested not in supported:
        return _unsupported(
            "redundancy",
            requested,
            f"device reports supported redundancy values {supported}",
            current=current,
        )
    from netaudio.dante.operation_availability import operation_availability

    availability = operation_availability(device, "redundancy")
    if not availability.writable and requested != current:
        state_factory = _unsupported if availability.supported is False else _unavailable
        return state_factory(
            "redundancy",
            requested,
            f"redundancy capability is not writable: {', '.join(availability.reasons)}",
            current=current,
        )
    return _change_or_unchanged("redundancy", requested, current)


async def _plan_status_setting(
    application,
    device,
    *,
    kind: str,
    requested: Any,
    probe_name: str,
) -> PresetAction:
    try:
        status = await getattr(application, probe_name)(device, timeout=2.0)
    except READBACK_ERRORS as exception:
        return _unavailable(kind, requested, f"fresh {kind.replace('_', ' ')} readback failed: {exception}")
    if not isinstance(status, dict) or status.get("current_value") is None:
        return _unavailable(kind, requested, f"fresh {kind.replace('_', ' ')} value was unavailable")
    current = status["current_value"]
    available = status.get("available_values")
    if not isinstance(available, list):
        return _unavailable(
            kind,
            requested,
            f"{kind.replace('_', ' ')} capability values were unavailable",
            current=current,
        )
    if requested not in available:
        return _unsupported(
            kind,
            requested,
            f"device reports supported {kind.replace('_', ' ')} values {available}",
            current=current,
        )
    update_mode = status.get("update_mode")
    if update_mode not in (1, 2):
        return _unsupported(
            kind,
            requested,
            f"device reports non-writable update mode {update_mode!r}",
            current=current,
        )
    fields = {
        "sample_rate": (
            "sample_rate",
            "requested_sample_rate",
            "sample_rate_update_mode",
            "supported_sample_rates",
        ),
        "encoding": (
            "encoding",
            "requested_encoding",
            "encoding_update_mode",
            "supported_encodings",
        ),
        "sample_rate_pullup": (
            "sample_rate_pullup_raw_value",
            "requested_sample_rate_pullup_raw_value",
            "sample_rate_pullup_update_mode",
            "supported_sample_rate_pullup_raw_values",
        ),
    }[kind]
    setattr(device, fields[0], current)
    setattr(device, fields[1], status.get("requested_value"))
    setattr(device, fields[2], update_mode)
    setattr(device, fields[3], available)
    from netaudio.dante.operation_availability import operation_availability

    availability = operation_availability(device, kind, requested)
    if not availability.writable and requested != current:
        state_factory = _unsupported if availability.supported is False else _unavailable
        return state_factory(
            kind,
            requested,
            f"{kind.replace('_', ' ')} capability is not writable: {', '.join(availability.reasons)}",
            current=current,
        )
    return _change_or_unchanged(kind, requested, current)


async def _plan_latency(application, device, requested: float) -> PresetAction:
    try:
        settings = await application.get_device_settings(device)
    except READBACK_ERRORS as exception:
        return _unavailable("latency", requested, f"fresh latency readback failed: {exception}")
    active = settings.get("active_latency_ns") if isinstance(settings, dict) else None
    if isinstance(active, bool) or not isinstance(active, (int, float)):
        return _unavailable("latency", requested, "fresh active latency was unavailable")
    current = active / 1_000_000
    return _change_or_unchanged(
        "latency",
        requested,
        current,
        matches=math.isclose(float(requested), float(current), rel_tol=0, abs_tol=1e-9),
    )


async def _plan_preferred_leader(application, device, requested: bool) -> PresetAction:
    try:
        current = await application.probe_preferred_leader_state(device, timeout=2.0)
    except READBACK_ERRORS as exception:
        return _unavailable(
            "preferred_leader",
            requested,
            f"fresh preferred-leader readback failed: {exception}",
        )
    if not isinstance(current, bool):
        return _unavailable("preferred_leader", requested, "fresh preferred-leader state was unavailable")
    return _change_or_unchanged("preferred_leader", requested, current)


async def _plan_performance_setting(application, device, kind: str, requested: Any) -> PresetAction:
    try:
        expected = requested_performance_properties(device, kind, requested)
        current = await application.get_performance_settings(device, expected)
    except READBACK_ERRORS as exception:
        return _unavailable(kind, requested, f"fresh performance readback failed: {exception}")
    missing = set(expected) - set(current)
    if missing:
        labels = ", ".join(f"0x{property_id:04x}" for property_id in sorted(missing))
        return _unavailable(kind, requested, f"fresh readback omitted properties {labels}", current=current)
    return _change_or_unchanged(kind, requested, current, matches=current == expected)


async def _plan_clock_configuration(application, device, changes: dict) -> PresetAction:
    try:
        preview = await application.preview_clock_configuration(device, changes)
    except READBACK_ERRORS as exception:
        return _unavailable("clock_configuration", changes, str(exception))
    return _change_or_unchanged("clock_configuration", changes, preview["before"], matches=not preview["changes"])


async def _plan_channel_names(application, device, config: dict, channel_type: str) -> PresetAction:
    kind = "transmitter_channel_names" if channel_type == "tx" else "receiver_channel_names"
    requested = config[kind]
    try:
        protocol_id = await application.resolve_channel_name_protocol_identifier(device, channel_type)
        await (device.get_tx_channels() if channel_type == "tx" else device.get_rx_channels())
    except READBACK_ERRORS as exception:
        return _unavailable(kind, requested, f"fresh channel readback failed: {exception}")
    if protocol_id is None:
        return _unsupported(kind, requested, "no supported channel-name protocol was reported")
    channels = device.tx_channels if channel_type == "tx" else device.rx_channels
    if not isinstance(channels, dict):
        return _unavailable(kind, requested, "fresh channel inventory was unavailable")
    by_number = {channel.number: channel for channel in channels.values()}
    missing = sorted(set(requested) - set(by_number))
    current = {
        number: by_number[number].friendly_name or by_number[number].name for number in requested if number in by_number
    }
    if missing:
        return _unavailable(
            kind,
            requested,
            f"fresh inventory did not report channel(s) {', '.join(map(str, missing))}",
            current=current,
        )
    return _change_or_unchanged(kind, requested, current)


def _fresh_flow_specifications(inventory: dict) -> list[TransmitFlowSpecification]:
    result = []
    for raw in inventory.get("flows", []):
        result.append(TransmitFlowSpecification.from_dict(raw))
    return result


async def _plan_transmit_flows(application, device, config: dict) -> list[PresetAction]:
    raw_requested = config.get("transmit_flows", [])
    desired_flows = [TransmitFlowSpecification.from_dict(raw) for raw in raw_requested]
    if not desired_flows:
        return []
    try:
        await device.get_tx_channels()
        sample_rate_status = await application.probe_sample_rate_status(device, timeout=2.0)
        encoding_status = await application.probe_encoding_status(device, timeout=2.0)
        device.sample_rate = sample_rate_status["current_value"]
        device.encoding = encoding_status["current_value"]
        inventory = await inspect_transmit_flows(device)
        current_flows = _fresh_flow_specifications(inventory)
    except READBACK_ERRORS as exception:
        return [
            _unavailable(
                "transmit_flow",
                desired.to_dict(),
                f"fresh transmit-flow planning readback failed: {exception}",
            )
            for desired in desired_flows
        ]

    current_by_id = {
        flow.identity.global_flow_id: flow for flow in current_flows if flow.identity.global_flow_id is not None
    }
    actions = []
    for desired in desired_flows:
        flow_id = desired.identity.global_flow_id
        if flow_id is not None and flow_id in current_by_id:
            current = current_by_id[flow_id]
            comparison = compare_transmit_flows(desired, current)
            if comparison.matches:
                actions.append(
                    _change_or_unchanged(
                        "transmit_flow",
                        desired.to_dict(),
                        current.to_dict(),
                    )
                )
            else:
                reason = (
                    "fresh inventory does not expose all requested durable flow fields; destructive replacement is refused"
                    if comparison.unavailable_fields and not comparison.differences
                    else "the identified flow has different durable state; destructive replacement is refused"
                )
                actions.append(
                    _ambiguous(
                        "transmit_flow",
                        desired.to_dict(),
                        reason,
                        current=current.to_dict(),
                    )
                )
            continue
        if flow_id is None:
            matches = [flow for flow in current_flows if compare_transmit_flows(desired, flow).matches]
            if len(matches) == 1:
                actions.append(
                    _change_or_unchanged(
                        "transmit_flow",
                        desired.to_dict(),
                        matches[0].to_dict(),
                        matches=True,
                    )
                )
                continue
            if len(matches) > 1:
                actions.append(
                    _ambiguous(
                        "transmit_flow",
                        desired.to_dict(),
                        "multiple existing flows match the identity-free request",
                        current=[flow.to_dict() for flow in matches],
                    )
                )
                continue
        plan = plan_create_transmit_flow(device, desired)
        if plan.supported:
            actions.append(
                PresetAction(
                    kind="transmit_flow",
                    payload=desired.to_dict(),
                    current=None,
                    state=PresetActionState.CHANGE,
                    reason="fresh inventory contains no matching flow",
                )
            )
        else:
            actions.append(
                _unsupported(
                    "transmit_flow",
                    desired.to_dict(),
                    "; ".join(plan.reasons),
                )
            )
    return actions


async def _fresh_receiver_state(device, requested_channels: set[int]) -> tuple[dict[int, Any], list[int]]:
    from netaudio.commands.subscription import _index_fresh_subscriptions, _subscription_signature

    await device.get_rx_channels()
    channels = getattr(device, "rx_channels", None)
    if not isinstance(channels, dict):
        raise RuntimeError("fresh receiver channel inventory was unavailable")
    by_number = {channel.number: channel for channel in channels.values()}
    missing = sorted(requested_channels - set(by_number))
    _index_fresh_subscriptions(device)
    current = {number: _subscription_signature(device, number) for number in requested_channels if number in by_number}
    return current, missing


async def _plan_receiver_subscriptions(device, device_name: str, config: dict) -> list[PresetAction]:
    desired_sources: dict[int, tuple[str, str] | None] = {}
    external: list[tuple[int, dict]] = []
    for receiver_channel_number, subscription in config["rx_subscriptions"].items():
        if subscription is None:
            desired_sources[receiver_channel_number] = None
            continue
        if subscription.get("kind", "native_dante") == "external_rtp":
            external.append((receiver_channel_number, subscription))
            continue
        transmitter_device_name = subscription["tx_device"]
        if transmitter_device_name == ".":
            transmitter_device_name = device_name
        desired_sources[receiver_channel_number] = (subscription["tx_channel"], transmitter_device_name)

    requested_channels = set(desired_sources) | {number for number, _ in external}
    try:
        current, missing = await _fresh_receiver_state(device, requested_channels)
    except READBACK_ERRORS as exception:
        reason = f"fresh receiver subscription readback failed: {exception}"
        actions = []
        if desired_sources:
            actions.append(_unavailable("receiver_subscriptions", desired_sources, reason))
        if external:
            actions.append(_unavailable("external_receiver_subscriptions", external, reason))
        return actions

    actions = []
    if desired_sources:
        native_current = {number: current[number] for number in desired_sources if number in current}
        native_missing = sorted(set(desired_sources) & set(missing))
        if native_missing:
            actions.append(
                _unavailable(
                    "receiver_subscriptions",
                    desired_sources,
                    f"fresh inventory did not report receiver channel(s) {', '.join(map(str, native_missing))}",
                    current=native_current,
                )
            )
        else:
            actions.append(_change_or_unchanged("receiver_subscriptions", desired_sources, native_current))
    if external:
        external_missing = sorted({number for number, _ in external} & set(missing))
        if external_missing:
            actions.append(
                _unavailable(
                    "external_receiver_subscriptions",
                    external,
                    f"fresh inventory did not report receiver channel(s) {', '.join(map(str, external_missing))}",
                    current={number: current.get(number) for number, _ in external},
                )
            )
        else:
            from netaudio.dante import flows

            try:
                receiver_inventory = await flows.query_preferred_receiver_flow_inventory(device)
            except (*READBACK_ERRORS, AttributeError):
                receiver_inventory = None
            if receiver_inventory is None:
                actions.append(
                    _unavailable(
                        "external_receiver_subscriptions",
                        external,
                        "complete fresh receiver-flow inventory was unavailable",
                    )
                )
            else:
                effective = flows.effective_external_subscription_index(receiver_inventory)
                requested = {
                    number: _preset_external_identity(number, subscription) for number, subscription in external
                }
                observed = {number: effective.get(number, []) for number, _ in external}
                matches = all(
                    any(
                        _preset_external_identity_projection(candidate)
                        == _preset_external_identity_projection(expected)
                        for candidate in observed[number]
                    )
                    for number, expected in requested.items()
                )
                actions.append(
                    _change_or_unchanged(
                        "external_receiver_subscriptions",
                        external,
                        observed,
                        matches=matches,
                    )
                )
    return actions


def _preset_external_identity(receiver_channel: int, subscription: dict) -> dict:
    identity = subscription["flow_identity"]
    return {
        "receiver_channel": receiver_channel,
        "flow_slot": subscription["flow_slot"],
        "source_ipv4": identity["source_ipv4"],
        "session_id": identity["session_id"],
        "interface_endpoints": subscription.get("interface_endpoints"),
    }


def _preset_external_identity_projection(identity: dict) -> tuple:
    endpoints = identity.get("interface_endpoints")
    endpoint_projection = None
    if endpoints is not None:
        endpoint_projection = tuple(
            (endpoint.get("ipv4_address"), endpoint.get("udp_port"))
            for endpoint in endpoints
            if isinstance(endpoint, dict)
        )
    return (
        identity.get("receiver_channel"),
        identity.get("flow_slot"),
        identity.get("source_ipv4"),
        identity.get("session_id"),
        endpoint_projection,
    )


async def _plan_codec_gain(application, device, gains: list[dict[str, Any]]) -> list[PresetAction]:
    if not gains:
        return []
    try:
        device_type, levels = await application.probe_gain_adapter(device, timeout=2.0)
    except READBACK_ERRORS as exception:
        return [_unavailable("codec_gain", gain, f"fresh codec/gain readback failed: {exception}") for gain in gains]
    if device_type not in {"input", "output"} or not isinstance(levels, list):
        return [_unavailable("codec_gain", gain, "fresh codec/gain adapter state was unavailable") for gain in gains]
    device.gain_adapter = {"device_type": device_type, "channel_levels": list(levels)}
    device.gain_device_type = device_type
    device.gain_levels = list(levels)
    device.supported_gain_levels = [1, 2, 3, 4, 5]
    from netaudio.dante.operation_availability import operation_availability

    availability = operation_availability(device, "codec_control")
    actions = []
    for gain in gains:
        channel = gain["channel"]
        current = {
            "channel": channel,
            "device_type": device_type,
            "level": levels[channel - 1] if 1 <= channel <= len(levels) else None,
        }
        if gain["device_type"] != device_type:
            actions.append(
                _unsupported(
                    "codec_gain",
                    gain,
                    f"device reports a {device_type} gain adapter",
                    current=current,
                )
            )
        elif not 1 <= channel <= len(levels):
            actions.append(
                _unavailable(
                    "codec_gain",
                    gain,
                    f"fresh gain readback did not report channel {channel}",
                    current=current,
                )
            )
        elif gain["level"] not in {1, 2, 3, 4, 5}:
            actions.append(
                _unsupported(
                    "codec_gain",
                    gain,
                    "requested gain level is outside the established adapter values",
                    current=current,
                )
            )
        elif current["level"] != gain["level"] and not availability.writable:
            state_factory = _unsupported if availability.supported is False else _unavailable
            actions.append(
                state_factory(
                    "codec_gain",
                    gain,
                    f"codec/gain capability is not writable: {', '.join(availability.reasons)}",
                    current=current,
                )
            )
        else:
            actions.append(
                _change_or_unchanged(
                    "codec_gain",
                    gain,
                    current,
                    matches=current["level"] == gain["level"],
                )
            )
    return actions


async def _plan_interfaces(application, device, config: dict) -> list[PresetAction]:
    entries = _interface_entries(config)
    if not entries:
        return []
    requested = [_interface_payload(interface) for interface in entries]
    try:
        interfaces = await application.probe_interface_status(device, timeout=2.0)
    except READBACK_ERRORS as exception:
        return [
            _unavailable("interface", payload, f"fresh interface readback failed: {exception}") for payload in requested
        ]
    actions = []
    from netaudio.dante.network_configuration import interface_configuration
    from netaudio.dante.operation_availability import operation_availability

    availability = operation_availability(device, "static_ipv4")
    for payload in requested:
        identity = payload["identity"]
        if identity not in {"primary", "secondary"}:
            actions.append(
                _unsupported(
                    "interface",
                    payload,
                    "only explicitly identified primary and secondary Dante interfaces have supported writers",
                )
            )
            continue
        try:
            if identity == "primary" and len(interfaces) == 1 and interfaces[0].get("interface") is None:
                selected = interfaces[0]
            else:
                selected = interface_configuration(interfaces, identity)
        except (LookupError, TypeError, ValueError, RuntimeError) as exception:
            actions.append(
                _unavailable("interface", payload, f"fresh {identity} interface was unavailable: {exception}")
            )
            continue
        expected = _expected_interface_payload(payload)
        reported = selected.get("configured") if isinstance(selected.get("configured"), dict) else selected
        current = {field: reported.get(field) for field in expected}
        if any(value is None for value in current.values()):
            actions.append(
                _unavailable(
                    "interface",
                    payload,
                    f"fresh {identity} configured-state readback was incomplete",
                    current=current,
                )
            )
        elif not availability.writable:
            state_factory = _unsupported if availability.supported is False else _unavailable
            actions.append(
                state_factory(
                    "interface",
                    payload,
                    f"interface capability is not writable: {', '.join(availability.reasons)}",
                    current=current,
                )
            )
        else:
            actions.append(_change_or_unchanged("interface", payload, current, matches=current == expected))
    return actions


def _preserved_actions(config: dict) -> list[PresetAction]:
    labels = {
        "external_word_clock": "external word-clock selection has no verified writer on this transport",
        "ha_bridge": "HA bridge application is unavailable without an explicitly advertised and understood capability",
        "unknown_fields": "unknown preset extensions are preserved but cannot be applied",
    }
    actions = [
        _unsupported(field_name, config[field_name], reason)
        for field_name, reason in labels.items()
        if field_name in config
    ]
    known = {
        "name",
        "device_name",
        "device_identity",
        "preferred_leader",
        "external_word_clock",
        "sample_rate",
        "encoding",
        "latency",
        "receive_flow_performance",
        "transmit_flow_performance",
        "unicast_performance",
        "receive_flow_default_slots",
        "sample_rate_pullup",
        "clock_source_code",
        "clock_subdomain",
        "global_unicast_delay_requests",
        "aggregate_ptpv1_unicast_delay_requests",
        "redundancy_mode",
        "interfaces",
        "interface_mode",
        "ip_address",
        "netmask",
        "gateway",
        "dns_server",
        "transmitter_channel_names",
        "receiver_channel_names",
        "transmit_flows",
        "rx_subscriptions",
        "codec_gain",
        "ha_bridge",
        "unknown_fields",
    }
    actions.extend(
        _unsupported(
            f"unknown:{field_name}",
            value,
            "unrecognized preset category is preserved without a mutation",
        )
        for field_name, value in config.items()
        if field_name not in known
    )
    return actions


async def _plan_device_actions(application, matched: MatchedPresetDevice) -> PresetDeviceActions:
    config = matched.config
    _validate_config(matched.device_name, config)
    actions = []
    if "device_name" in config:
        actions.append(await _plan_device_name(matched.device, config["device_name"]))
    if "redundancy_mode" in config:
        actions.append(await _plan_redundancy(application, matched.device, config["redundancy_mode"]))
    if "sample_rate" in config:
        actions.append(
            await _plan_status_setting(
                application,
                matched.device,
                kind="sample_rate",
                requested=config["sample_rate"],
                probe_name="probe_sample_rate_status",
            )
        )
    if "encoding" in config:
        actions.append(
            await _plan_status_setting(
                application,
                matched.device,
                kind="encoding",
                requested=config["encoding"],
                probe_name="probe_encoding_status",
            )
        )
    if "latency" in config:
        actions.append(await _plan_latency(application, matched.device, config["latency"]))
    for performance_kind in (
        "receive_flow_performance",
        "transmit_flow_performance",
        "unicast_performance",
        "receive_flow_default_slots",
    ):
        if performance_kind in config:
            actions.append(
                await _plan_performance_setting(
                    application,
                    matched.device,
                    performance_kind,
                    config[performance_kind],
                )
            )
    if "preferred_leader" in config and getattr(matched.device, "requires_managed_control", False):
        actions.append(await _plan_preferred_leader(application, matched.device, config["preferred_leader"]))
    if "sample_rate_pullup" in config:
        actions.append(
            await _plan_status_setting(
                application,
                matched.device,
                kind="sample_rate_pullup",
                requested=config["sample_rate_pullup"],
                probe_name="probe_sample_rate_pullup_status",
            )
        )
    clock_changes = {
        target: config[source]
        for source, target in (
            ("clock_source_code", "clock_source"),
            ("clock_subdomain", "subdomain"),
            ("global_unicast_delay_requests", "global_unicast_delay_requests"),
            ("aggregate_ptpv1_unicast_delay_requests", "aggregate_ptpv1_unicast_delay_requests"),
        )
        if source in config
    }
    if "preferred_leader" in config and not getattr(matched.device, "requires_managed_control", False):
        clock_changes["preferred_leader"] = config["preferred_leader"]
    if clock_changes:
        actions.append(await _plan_clock_configuration(application, matched.device, clock_changes))
    if "transmitter_channel_names" in config:
        actions.append(await _plan_channel_names(application, matched.device, config, "tx"))
    if "receiver_channel_names" in config:
        actions.append(await _plan_channel_names(application, matched.device, config, "rx"))
    actions.extend(await _plan_transmit_flows(application, matched.device, config))
    if "rx_subscriptions" in config:
        actions.extend(await _plan_receiver_subscriptions(matched.device, matched.device_name, config))
    actions.extend(await _plan_codec_gain(application, matched.device, config.get("codec_gain", [])))
    actions.extend(await _plan_interfaces(application, matched.device, config))
    actions.extend(_preserved_actions(config))
    return PresetDeviceActions(
        actions=actions,
        config=config,
        device=matched.device,
        device_name=matched.device_name,
        server_name=matched.server_name,
    )


async def build_preset_plan(application, matched_devices: list[MatchedPresetDevice]) -> PresetLoadPlan:
    preflight_errors = []
    device_actions = []
    for matched in matched_devices:
        try:
            device_actions.append(await _plan_device_actions(application, matched))
        except (*MUTATION_ERRORS, LookupError, TypeError, AttributeError, KeyError) as exception:
            preflight_errors.append(f"{matched.device_name}: {exception}")
    if preflight_errors:
        raise PresetValidationError(
            [
                "Error: preset load was refused before sending any changes:",
                *(f"  - {error}" for error in preflight_errors),
            ]
        )
    return PresetLoadPlan(device_actions=device_actions)


async def _read_sample_rate(application, device):
    settings = await application.get_device_settings(device)
    if not isinstance(settings, dict) or settings.get("sample_rate") is None:
        raise RuntimeError("sample-rate readback was unavailable")
    return settings["sample_rate"]


async def _read_encoding(application, device):
    from netaudio.commands.config.readback import _read_encoding_status

    return await _read_encoding_status(application, device)


async def _read_latency(application, device):
    settings = await application.get_device_settings(device)
    if not isinstance(settings, dict) or settings.get("active_latency_ns") is None:
        raise RuntimeError("active latency readback was unavailable")
    return settings["active_latency_ns"]


async def _read_audio_setting(action, application, device):
    if action == "sample_rate":
        return await _read_sample_rate(application, device)
    if action == "encoding":
        return await _read_encoding(application, device)
    if action == "latency":
        return await _read_latency(application, device)
    raise ValueError(f"unsupported audio setting: {action}")


async def _read_preferred_leader(application, device):
    state = await application.probe_preferred_leader_state(device, timeout=1.0)
    if state is None:
        raise RuntimeError("preferred-leader readback was unavailable")
    return state


def _expected_interface_config(action: PresetAction) -> dict:
    mode = action.payload["mode"]
    if mode == "dhcp":
        return {"mode": "dynamic"}
    return {"mode": "static", **action.payload["configuration"]}


async def _read_interface_config(application, device, interface: str, expected: dict):
    from netaudio.dante.network_configuration import interface_configuration

    interfaces = await application.probe_interface_status(device, timeout=1.0)
    if not interfaces:
        raise RuntimeError("interface readback was unavailable")
    if interface == "primary" and len(interfaces) == 1 and interfaces[0].get("interface") is None:
        selected = interfaces[0]
    else:
        selected = interface_configuration(interfaces, interface)
    reported = selected.get("configured") or selected
    return {field_name: reported.get(field_name) for field_name in expected}


async def _apply_sample_rate(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    report = context.report
    try:
        result = await context.application.set_sample_rate(
            entry.device, action.payload, confirm_destructive=context.confirm_destructive
        )
    except SampleRateTopologyChangedButUnverifiedError as exception:
        report.record(entry.device_name, f"sample rate: CHANGED BUT UNVERIFIED ({exception})", failed=True)
        return
    except SampleRateTopologyMutationOutcomeUnknownError as exception:
        report.record(entry.device_name, f"sample rate: MUTATION OUTCOME UNKNOWN ({exception})", failed=True)
        return
    except MUTATION_ERRORS as exception:
        report.record(entry.device_name, f"sample rate: REFUSED ({exception})", failed=True)
        return
    if result.changed:
        report.record(entry.device_name, f"sample rate {result.observed_sample_rate_hertz} Hz and topology (verified)")
    else:
        report.record(
            entry.device_name,
            f"sample rate already {result.observed_sample_rate_hertz} Hz (verified; no write sent)",
        )


async def _apply_receiver_subscriptions(
    context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction
) -> None:
    from netaudio.commands.subscription import reconcile_receiver_subscriptions

    report = context.report
    try:
        result = await reconcile_receiver_subscriptions(context.application, entry.device, action.payload)
    except MUTATION_ERRORS as exception:
        report.record(entry.device_name, f"receiver subscriptions: FAILED ({exception})", failed=True)
        return
    if result.unchanged and not result.verified and not result.failures:
        report.record(entry.device_name, f"receiver subscriptions already match ({len(result.unchanged)} channels)")
    for receiver_channel_number, desired_source in sorted(result.verified.items()):
        if desired_source is None:
            description = f"receiver channel {receiver_channel_number} unsubscribed"
        else:
            description = f"receiver channel {receiver_channel_number} <- {desired_source[0]}@{desired_source[1]}"
        report.record(entry.device_name, f"{description} (verified)")
    for receiver_channel_number, detail in sorted(result.failures.items()):
        report.record(entry.device_name, f"receiver channel {receiver_channel_number}: FAILED ({detail})", failed=True)


async def _apply_transmitter_channel_names(
    context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction
) -> None:
    from netaudio.dante.transmitter_channel_name_reconciliation import reconcile_transmitter_channel_names

    report = context.report
    try:
        result = await reconcile_transmitter_channel_names(context.application, entry.device, action.payload)
    except MUTATION_ERRORS as exception:
        report.record(entry.device_name, f"transmitter channel names: FAILED ({exception})", failed=True)
        return
    if result.unchanged and not result.verified and not result.failures:
        report.record(entry.device_name, f"transmitter channel names already match ({len(result.unchanged)} channels)")
    for transmitter_channel_number, channel_name in sorted(result.verified.items()):
        report.record(entry.device_name, f"transmitter channel {transmitter_channel_number}: {channel_name} (verified)")
    for transmitter_channel_number, detail in sorted(result.failures.items()):
        report.record(
            entry.device_name, f"transmitter channel {transmitter_channel_number}: FAILED ({detail})", failed=True
        )


async def _read_channel_names(device, channel_type: str, channel_numbers: tuple[int, ...]) -> dict[int, str]:
    await (device.get_rx_channels() if channel_type == "rx" else device.get_tx_channels())
    channels = device.rx_channels if channel_type == "rx" else device.tx_channels
    by_number = {channel.number: channel for channel in channels.values()}
    return {
        number: by_number[number].friendly_name or by_number[number].name
        for number in channel_numbers
        if number in by_number
    }


async def _apply_receiver_channel_names(
    context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction
) -> None:
    desired = action.payload
    current = await _read_channel_names(entry.device, "rx", tuple(desired))
    failures = 0
    changed = 0
    for number, name in desired.items():
        if current.get(number) == name:
            continue
        try:
            await context.application.set_channel_name(entry.device, "rx", number, name)
            readback = await readback_after_notification(
                lambda number=number: _read_channel_names(entry.device, "rx", (number,)),
                {number: name},
            )
        except MUTATION_ERRORS as exception:
            context.report.record(entry.device_name, f"receiver channel {number}: FAILED ({exception})", failed=True)
            failures += 1
            continue
        if readback.matched:
            context.report.record(entry.device_name, f"receiver channel {number}: {name} (verified)")
            changed += 1
        else:
            context.report.record(
                entry.device_name,
                f"receiver channel {number}: FAILED (fresh readback reports {readback.observed!r})",
                failed=True,
            )
            failures += 1
    if not changed and not failures:
        context.report.record(entry.device_name, f"receiver channel names already match ({len(desired)} channels)")


async def _apply_device_name(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    desired = action.payload
    try:
        acknowledgement = await context.application.set_device_name(entry.device, desired)
        observed = await entry.device.fetch_device_name()
    except MUTATION_ERRORS as exception:
        context.report.operation(
            entry.device_name,
            action.kind,
            "failed",
            f"device name: FAILED ({exception})",
            requested=desired,
            failed=True,
        )
        return
    if observed == desired:
        entry.device.name = observed
        context.report.operation(
            entry.device_name,
            action.kind,
            "confirmed",
            f"device name {desired} (verified)",
            requested=desired,
            effective=observed,
            acknowledgement=acknowledgement,
        )
    else:
        context.report.operation(
            entry.device_name,
            action.kind,
            "failed",
            f"device name {desired}: FAILED (fresh readback reports {observed!r})",
            requested=desired,
            effective=observed,
            acknowledgement=acknowledgement,
            failed=True,
        )


async def _apply_redundancy(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    try:
        observed = await context.application.set_dante_redundancy(entry.device, action.payload)
    except MUTATION_ERRORS as exception:
        evidence = exception.evidence if isinstance(exception, NetworkConfigurationUnverified) else None
        readback = evidence.get("effective_readback") if isinstance(evidence, dict) else None
        context.report.operation(
            entry.device_name,
            action.kind,
            "failed",
            f"redundancy {action.payload}: FAILED ({exception})",
            requested=action.payload,
            effective=(readback.get("configured_mode") if isinstance(readback, dict) else None),
            acknowledgement=(evidence.get("request_acknowledgement") if isinstance(evidence, dict) else None),
            device_confirmation=(evidence.get("device_side_confirmation") if isinstance(evidence, dict) else None),
            effective_state_confirmation=(
                evidence.get("effective_state_confirmation") if isinstance(evidence, dict) else None
            ),
            persistence_confirmation=(evidence.get("persistence_confirmation") if isinstance(evidence, dict) else None),
            failed=True,
        )
        return
    readback = observed.get("effective_readback") if isinstance(observed, dict) else None
    effective = readback.get("configured_mode") if isinstance(readback, dict) else None
    matched = effective == action.payload
    context.report.operation(
        entry.device_name,
        action.kind,
        "confirmed" if matched else "failed",
        f"redundancy {action.payload} "
        f"({'fresh configured-state readback matched; persistence not checked' if matched else f'FAILED; device reports {effective!r}'})",
        requested=action.payload,
        effective=effective,
        acknowledgement=(observed.get("request_acknowledgement") if isinstance(observed, dict) else None),
        persistence_confirmation=(observed.get("persistence_confirmation") if isinstance(observed, dict) else None),
        failed=not matched,
    )


async def _apply_sample_rate_pullup(
    context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction
) -> None:
    try:
        observed = await context.application.set_sample_rate_pullup(entry.device, action.payload)
    except MUTATION_ERRORS as exception:
        context.report.record(entry.device_name, f"sample rate pull-up: FAILED ({exception})", failed=True)
        return
    effective = observed.get("current_value") if isinstance(observed, dict) else None
    if effective is None:
        effective = getattr(entry.device, "sample_rate_pullup_raw_value", None)
    if effective == action.payload:
        context.report.record(entry.device_name, f"sample rate pull-up {action.payload} (verified)")
    else:
        context.report.record(
            entry.device_name,
            f"sample rate pull-up {action.payload}: FAILED (device reports {effective!r})",
            failed=True,
        )


async def _apply_clock_configuration(
    context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction
) -> None:
    try:
        result = await context.application.set_clock_configuration(entry.device, action.payload)
    except MUTATION_ERRORS as exception:
        context.report.record(entry.device_name, f"clock settings: FAILED ({exception})", failed=True)
        return
    if result["effective_state_confirmed"]:
        context.report.record(entry.device_name, "clock settings verified; persistence unknown")
    else:
        context.report.record(entry.device_name, "clock settings sent but not confirmed", failed=True)


async def _apply_codec_gain(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    requested = action.payload
    try:
        observed = await context.application.set_gain_level(
            entry.device,
            requested["channel"],
            requested["level"],
            requested["device_type"],
        )
    except MUTATION_ERRORS as exception:
        context.report.record(
            entry.device_name, f"codec gain channel {requested['channel']}: FAILED ({exception})", failed=True
        )
        return
    levels = observed[1] if isinstance(observed, tuple) and len(observed) == 2 else None
    effective = levels[requested["channel"] - 1] if levels and len(levels) >= requested["channel"] else None
    if effective == requested["level"]:
        context.report.record(
            entry.device_name,
            f"codec gain channel {requested['channel']} level {requested['level']} (verified)",
        )
    else:
        context.report.record(
            entry.device_name,
            f"codec gain channel {requested['channel']}: FAILED (device reports {effective!r})",
            failed=True,
        )


async def _apply_transmit_flow(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    try:
        result = await context.application.create_transmit_flow(entry.device, action.payload)
    except MUTATION_ERRORS as exception:
        context.report.operation(
            entry.device_name,
            action.kind,
            "failed",
            f"transmit flow: FAILED ({exception})",
            requested=action.payload,
            failed=True,
        )
        return
    serialized = result.to_dict() if hasattr(result, "to_dict") else result
    state = serialized.get("state") if isinstance(serialized, dict) else None
    message = (
        serialized.get("message", "transmit-flow result unavailable") if isinstance(serialized, dict) else str(result)
    )
    acknowledgement = serialized.get("request_acknowledgement") if isinstance(serialized, dict) else None
    effective = serialized.get("effective") if isinstance(serialized, dict) else None
    confirmed = state == "confirmed"
    context.report.operation(
        entry.device_name,
        action.kind,
        state or "failed",
        f"transmit flow: {message}",
        requested=action.payload,
        effective=effective,
        acknowledgement=acknowledgement,
        device_confirmation=serialized.get("device_confirmation") if isinstance(serialized, dict) else None,
        effective_state_confirmation=(
            serialized.get("effective_state_confirmation") if isinstance(serialized, dict) else None
        ),
        persistence_confirmation=(serialized.get("persistence_confirmation") if isinstance(serialized, dict) else None),
        verification_observations=(
            serialized.get("verification_observations", []) if isinstance(serialized, dict) else []
        ),
        failed=not confirmed,
        verified=confirmed,
    )


async def _apply_external_receiver_subscriptions(
    context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction
) -> None:
    groups: dict[tuple[str, int, bool], list[tuple[int, int]]] = {}
    for receiver_channel, subscription in action.payload:
        identity = subscription["flow_identity"]
        key = (
            identity["source_ipv4"],
            identity["session_id"],
            bool(subscription.get("receiver_supports_multiple_interfaces", False)),
        )
        groups.setdefault(key, []).append((receiver_channel, subscription["flow_slot"]))
    for (source, session_id, multiple_interfaces), mappings in groups.items():
        inventory = getattr(context.application, "external_flows", None)
        flow = inventory.get(source, session_id) if inventory is not None else None
        if flow is None:
            context.report.operation(
                entry.device_name,
                action.kind,
                "skipped",
                f"external RTP {source}/{session_id}: skipped; matching current SAP/SDP advertisement unavailable",
                requested=dict(mappings),
            )
            continue
        mappings.sort()
        try:
            result = await context.application.subscribe_external_rtp(
                entry.device,
                flow,
                [item[0] for item in mappings],
                [item[1] for item in mappings],
                receiver_supports_multiple_interfaces=multiple_interfaces,
            )
        except MUTATION_ERRORS as exception:
            context.report.operation(
                entry.device_name,
                action.kind,
                "failed",
                f"external RTP {source}/{session_id}: FAILED ({exception})",
                requested=dict(mappings),
                failed=True,
            )
            continue
        acknowledged = bool(result.get("request_acknowledged"))
        arc_effective_state = result.get("arc_effective_state_confirmed")
        confirmed = arc_effective_state is True
        contradicted = arc_effective_state is False
        context.report.operation(
            entry.device_name,
            action.kind,
            "confirmed" if confirmed else "failed" if contradicted or not acknowledged else "acknowledged",
            (
                f"external RTP {source}/{session_id}: ARC effective state confirmed"
                if confirmed
                else f"external RTP {source}/{session_id}: FAILED; complete fresh ARC readback contradicts the request"
                if contradicted
                else f"external RTP {source}/{session_id}: request acknowledged; ARC readback unconfirmed"
                if acknowledged
                else f"external RTP {source}/{session_id}: request rejected"
            ),
            requested=dict(mappings),
            acknowledgement=result,
            failed=contradicted or not acknowledged,
            verified=confirmed,
        )


async def _apply_skipped(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    if action.state is PresetActionState.UNCHANGED and action.kind == "sample_rate":
        message = f"sample rate already {action.payload} Hz (verified; no write sent)"
    elif action.state is PresetActionState.UNCHANGED and action.kind == "encoding":
        message = f"encoding already {action.payload}-bit (verified; no write sent)"
    elif action.state is PresetActionState.UNCHANGED and action.kind == "latency":
        message = f"latency already {action.payload:g} ms (verified; no write sent)"
    elif action.state is PresetActionState.UNCHANGED and action.kind == "receiver_subscriptions":
        message = f"receiver subscriptions already match ({len(action.payload)} channels; no write sent)"
    else:
        message = f"{action.kind.replace('_', ' ')}: {action.state.value} ({action.reason or 'no mutation scheduled'})"
    context.report.operation(
        entry.device_name,
        action.kind,
        action.state.value,
        message,
        requested=action.payload,
        effective=action.current,
        verified=action.state not in {PresetActionState.UNAVAILABLE, PresetActionState.AMBIGUOUS},
    )


def _structured_state_from_message(message: str) -> str:
    lowered = message.casefold()
    if "failed" in lowered or "refused" in lowered or "outcome unknown" in lowered:
        return "failed"
    if "already" in lowered:
        return "unchanged"
    if "not verified" in lowered or "unconfirmed" in lowered:
        return "acknowledged"
    if "verified" in lowered:
        return "confirmed"
    return "applied"


async def _send_request(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> bool:
    application = context.application
    try:
        if action.kind == "preferred_leader":
            await application.set_preferred_leader(entry.device, action.payload)
        elif action.kind == "encoding":
            await application.set_encoding(entry.device, action.payload)
        elif action.kind == "latency":
            await application.set_latency(entry.device, action.payload)
        else:
            await application.set_interface(
                entry.device,
                action.payload["mode"],
                action.payload["configuration"],
                interface=action.payload["identity"],
            )
    except MUTATION_ERRORS as exception:
        action_label = action.kind.replace("_", " ")
        context.report.record(entry.device_name, f"{action_label}: FAILED to send request: {exception}", failed=True)
        return False
    return True


async def _apply_audio_setting(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    if not await _send_request(context, entry, action):
        return
    if action.kind == "encoding":
        expected = entry.config["encoding"]
        success = f"encoding {expected}-bit"
    else:
        expected = int(round(entry.config["latency"] * 1_000_000))
        success = f"latency {entry.config['latency']:g} ms"
    result = await readback_after_notification(
        partial(_read_audio_setting, action.kind, context.application, entry.device), expected
    )
    if result.matched:
        context.report.record(entry.device_name, f"{success} (verified)")
        return
    if result.observed_available:
        detail = f"device reports {result.observed!r}"
    else:
        detail = f"fresh readback was unavailable: {result.error}"
    context.report.record(entry.device_name, f"{success}: FAILED ({detail})", failed=True)


async def _apply_preferred_leader(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    if not await _send_request(context, entry, action):
        return
    expected = entry.config["preferred_leader"]
    enabled = "on" if expected else "off"
    result = await readback_after_notification(
        lambda device=entry.device: _read_preferred_leader(context.application, device), expected
    )
    if result.matched:
        context.report.record(entry.device_name, f"preferred leader {enabled} (verified)")
    elif result.observed_available:
        context.report.record(
            entry.device_name, f"preferred leader {enabled}: FAILED (device reports {result.observed!r})", failed=True
        )
    else:
        detail = f": {result.error}" if result.error is not None else ""
        context.report.record(
            entry.device_name,
            f"preferred leader {enabled} requested; not verified (fresh readback unavailable{detail})",
            verified=False,
        )


async def _apply_performance_setting(
    context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction
) -> None:
    method = getattr(context.application, f"set_{action.kind}")
    try:
        if action.kind == "receive_flow_default_slots":
            result = await method(entry.device, action.payload)
        else:
            result = await method(
                entry.device,
                action.payload["latency_microseconds"],
                action.payload["frames_per_packet"],
            )
    except MUTATION_ERRORS as exception:
        context.report.operation(
            entry.device_name,
            action.kind,
            "failed",
            f"{action.kind.replace('_', ' ')}: FAILED ({exception})",
            requested=action.payload,
            failed=True,
        )
        return
    payload = result.to_dict()
    confirmed = result.state == "confirmed" and result.effective_state_confirmation is True
    failed = result.state in {"rejected", "contradicted"}
    context.report.operation(
        entry.device_name,
        action.kind,
        result.state,
        result.message,
        requested=payload["requested_properties"],
        effective=payload["effective_properties"],
        acknowledgement=payload["request_acknowledgement"],
        device_confirmation=payload["device_confirmation"],
        effective_state_confirmation=payload["effective_state_confirmation"],
        persistence_request_acknowledgement=payload["persistence_request_acknowledgement"],
        persistence_confirmation=payload["persistence_confirmation"],
        verification_observations=payload["verification_observations"],
        failed=failed,
        verified=confirmed,
    )


async def _apply_interface(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    if not await _send_request(context, entry, action):
        return
    mode = action.payload["mode"]
    identity = action.payload["identity"]
    mode_label = "dynamic" if mode == "dhcp" else mode
    interface_label = "interface" if identity == "primary" else f"{identity} interface"
    expected = _expected_interface_config(action)
    result = await readback_after_notification(
        lambda device=entry.device, expected=expected: _read_interface_config(
            context.application, device, identity, expected
        ),
        expected,
    )
    if entry.device.interface_reboot_required:
        context.report.needs_reboot.append(entry.device_name)
    if result.matched:
        context.report.record(entry.device_name, f"{interface_label} {mode_label} (verified)")
    elif result.observed_available:
        context.report.record(
            entry.device_name,
            f"{interface_label} {mode_label} requested; not verified "
            f"(device currently reports {result.observed!r}; reboot may be pending)",
            verified=False,
        )
    else:
        detail = f": {result.error}" if result.error is not None else ""
        context.report.record(
            entry.device_name,
            f"{interface_label} {mode_label} requested; not verified "
            f"(fresh readback unavailable{detail}; reboot may be pending)",
            verified=False,
        )


ACTION_HANDLERS: dict[str, ActionHandler] = {
    "clock_configuration": _apply_clock_configuration,
    "codec_gain": _apply_codec_gain,
    "device_name": _apply_device_name,
    "encoding": _apply_audio_setting,
    "external_receiver_subscriptions": _apply_external_receiver_subscriptions,
    "interface": _apply_interface,
    "latency": _apply_audio_setting,
    "preferred_leader": _apply_preferred_leader,
    "receive_flow_default_slots": _apply_performance_setting,
    "receive_flow_performance": _apply_performance_setting,
    "receiver_channel_names": _apply_receiver_channel_names,
    "receiver_subscriptions": _apply_receiver_subscriptions,
    "redundancy": _apply_redundancy,
    "sample_rate": _apply_sample_rate,
    "sample_rate_pullup": _apply_sample_rate_pullup,
    "transmit_flow": _apply_transmit_flow,
    "transmit_flow_performance": _apply_performance_setting,
    "transmitter_channel_names": _apply_transmitter_channel_names,
    "unicast_performance": _apply_performance_setting,
}


async def _apply_plan(
    context: PresetLoadContext,
    plan: PresetLoadPlan,
    *,
    stop_on_failure: bool = False,
    store_current_configuration: bool = False,
    preset_run_id: str | None = None,
    summary_states: list[str] | None = None,
) -> None:
    processed_action_count = 0
    planned_action_count = sum(len(entry.actions) for entry in plan.device_actions)
    for entry in plan.device_actions:
        if not entry.actions:
            context.report.record(entry.device_name, "no supported changes")
            continue
        entry_failures = context.report.failures
        entry_unverified = context.report.unverified
        changed = False
        for action in entry.actions:
            operation_count = len(context.report.operations)
            result_count = len(context.report.results)
            recorder = None
            handle = None
            if action.state is not PresetActionState.CHANGE:
                await _apply_skipped(context, entry, action)
            else:
                changed = True
                recorder = getattr(context.application, "operation_recorder", None)
                if recorder is not None:
                    handle = await recorder.begin_operation(
                        entry.device,
                        action.kind,
                        action.payload,
                        parent_preset_run_id=preset_run_id,
                    )
                try:
                    if recorder is None:
                        await ACTION_HANDLERS[action.kind](context, entry, action)
                    else:
                        with recorder.suppress_automatic_operations():
                            await ACTION_HANDLERS[action.kind](context, entry, action)
                except Exception as exception:
                    if recorder is not None and handle is not None:
                        await recorder.fail_operation(entry.device, handle, exception)
                    raise
            if len(context.report.operations) == operation_count:
                for _, message in context.report.results[result_count:]:
                    context.report.operations.append(
                        PresetOperationResult(
                            device_name=entry.device_name,
                            kind=action.kind,
                            state=_structured_state_from_message(message),
                            requested=action.payload,
                            message=message,
                        )
                    )
            action_results = context.report.operations[operation_count:]
            action_summary_state = (
                _preset_summary_state(action_results)
                if action.state is PresetActionState.CHANGE
                else "unchanged"
                if action.state is PresetActionState.UNCHANGED
                else "unavailable"
            )
            processed_action_count += 1
            if summary_states is not None:
                summary_states.append(action_summary_state)
            if action.state is PresetActionState.CHANGE and recorder is not None and handle is not None:
                if len(action_results) == 1:
                    operation_result = action_results[0]
                    audit_result = {
                        "state": operation_result.state,
                        "requested_values": operation_result.requested,
                        "effective_values": operation_result.effective,
                        "request_acknowledgement": operation_result.request_acknowledgement,
                        "device_confirmation": operation_result.device_confirmation,
                        "effective_state_confirmation": operation_result.effective_state_confirmation,
                        "persistence_request_acknowledgement": (operation_result.persistence_request_acknowledgement),
                        "persistence_confirmation": operation_result.persistence_confirmation,
                        "verification_observations": operation_result.verification_observations,
                        "message": operation_result.message,
                    }
                else:
                    state = action_summary_state
                    audit_result = {
                        "state": state,
                        "requested_values": action.payload,
                        "effective_values": [result.effective for result in action_results],
                        "effective_state_confirmation": True
                        if state == "confirmed"
                        else False
                        if state == "inconsistent"
                        else None,
                        "verification_observations": [
                            {"kind": result.kind, "state": result.state, "message": result.message}
                            for result in action_results
                        ],
                        "message": "; ".join(result.message for result in action_results),
                    }
                await recorder.complete_operation(entry.device, handle, audit_result)
            if stop_on_failure and (context.report.failures or context.report.unverified):
                context.report.record(
                    entry.device_name,
                    "Stopped after an unsuccessful or unverified change; remaining settings were not sent.",
                )
                if summary_states is not None:
                    summary_states.extend("skipped" for _ in range(planned_action_count - processed_action_count))
                return
        if (
            store_current_configuration
            and changed
            and context.report.failures == entry_failures
            and context.report.unverified == entry_unverified
        ):
            recorder = getattr(context.application, "operation_recorder", None)
            handle = None
            if recorder is not None:
                handle = await recorder.begin_operation(
                    entry.device,
                    "store_current_configuration",
                    {},
                    parent_preset_run_id=preset_run_id,
                )
            try:
                if recorder is None:
                    result = await context.application.store_current_configuration(entry.device)
                else:
                    with recorder.suppress_automatic_operations():
                        result = await context.application.store_current_configuration(entry.device)
            except MUTATION_ERRORS as exception:
                context.report.operation(
                    entry.device_name,
                    "store_current_configuration",
                    "failed",
                    f"configuration storage: FAILED ({exception})",
                    failed=True,
                )
                if recorder is not None and handle is not None:
                    await recorder.fail_operation(entry.device, handle, exception)
                if summary_states is not None:
                    summary_states.append("failed")
            else:
                payload = result.to_dict()
                acknowledged = bool(payload.get("persistence_request_acknowledgement", {}).get("accepted"))
                context.report.operation(
                    entry.device_name,
                    "store_current_configuration",
                    result.state,
                    result.message,
                    persistence_request_acknowledgement=payload["persistence_request_acknowledgement"],
                    persistence_confirmation=payload["persistence_confirmation"],
                    failed=not acknowledged,
                    verified=acknowledged,
                )
                if recorder is not None and handle is not None:
                    await recorder.complete_operation(entry.device, handle, result)
                if summary_states is not None:
                    summary_states.append(_preset_summary_state(context.report.operations[-1:]))


def _preset_summary_state(results: list[PresetOperationResult]) -> str:
    states = [result.state for result in results]
    if "failed" in states:
        return "failed"
    if any(value in {"contradicted", "inconsistent"} for value in states):
        return "inconsistent"
    if "rejected" in states:
        return "rejected"
    if any(value in {"unsupported", "unavailable", "ambiguous"} for value in states):
        return "unavailable"
    if states and all(value in {"confirmed", "deleted"} for value in states):
        return "confirmed"
    if states and all(value == "unchanged" for value in states):
        return "unchanged"
    if states and all(value == "skipped" for value in states):
        return "skipped"
    return "partial"


def _preset_summary(states: list[str]) -> dict[str, int]:
    summary = {
        "confirmed": 0,
        "partial": 0,
        "inconsistent": 0,
        "rejected": 0,
        "unavailable": 0,
        "failed": 0,
        "unchanged": 0,
        "skipped": 0,
    }
    for state in states:
        summary[state] += 1
    return summary


async def apply_preset_plan(
    application,
    plan: PresetLoadPlan,
    *,
    confirm_destructive: bool = False,
    report: PresetLoadReport | None = None,
    stop_on_failure: bool = False,
    store_current_configuration: bool = False,
) -> PresetLoadReport:
    context = PresetLoadContext(
        application=application,
        confirm_destructive=confirm_destructive,
        report=report if report is not None else PresetLoadReport(),
    )
    recorder = getattr(application, "operation_recorder", None)
    preset_handle = None
    summary_states: list[str] = []
    scope = None
    if recorder is not None:
        run_id = str(uuid.uuid4())
        scope = {
            "device_identity": f"preset-run:{run_id}",
            "name": "Preset",
            "server_name": "",
        }
        preset_handle = await recorder.begin_operation(
            scope,
            "apply_preset",
            {
                "devices": [entry.server_name for entry in plan.device_actions],
                "action_count": sum(len(entry.actions) for entry in plan.device_actions),
                "store_current_configuration": store_current_configuration,
            },
            transport="preset",
            operation_id=run_id,
            kind=MonitoringEventKind.PRESET_RUN,
        )
    try:
        await _apply_plan(
            context,
            plan,
            stop_on_failure=stop_on_failure,
            store_current_configuration=store_current_configuration,
            preset_run_id=preset_handle.operation_id if preset_handle is not None else None,
            summary_states=summary_states if preset_handle is not None else None,
        )
    except Exception as exception:
        if recorder is not None and preset_handle is not None:
            assert scope is not None
            await recorder.fail_operation(scope, preset_handle, exception)
        raise
    if recorder is not None and preset_handle is not None:
        assert scope is not None
        summary = _preset_summary(summary_states)
        if summary["failed"]:
            state = "failed"
        elif summary["inconsistent"]:
            state = "inconsistent"
        elif summary["rejected"]:
            state = "rejected"
        elif summary["partial"] or summary["unavailable"]:
            state = "partial"
        else:
            state = "confirmed"
        await recorder.complete_operation(
            scope,
            preset_handle,
            {
                "state": state,
                "effective_values": summary,
                "effective_state_confirmation": True
                if state == "confirmed"
                else False
                if state == "inconsistent"
                else None,
                "message": "preset application completed",
                "verification_observations": [{"summary": summary}],
            },
        )
    return context.report
