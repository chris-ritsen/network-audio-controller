from __future__ import annotations

import ctypes
import ipaddress
import json
import logging
import os
import sys
import threading
from collections.abc import Mapping
from pathlib import Path

from . import _requests, _types

from ._abi import (
    ABI_VERSION,
    PORT_ARC as _PORT_ARC,
    STATUS_INVALID_SEQUENCE,
    STATUS_IO_ERROR as _STATUS_IO_ERROR,
    STATUS_OK,
)
from ._abi import STATUS_BUFFER_TOO_SMALL as _STATUS_BUFFER_TOO_SMALL
from ._abi import STATUS_TIMEOUT as STATUS_TIMEOUT
from ._abi import configure as _configure

logger = logging.getLogger("netaudio")

_REPO_ROOT = Path(__file__).resolve().parents[5]


def _library_names():
    if sys.platform == "darwin":
        return ("libnetaudio_core.dylib", "libnetaudio_core.so")
    if sys.platform == "win32":
        return ("netaudio_core.dll",)
    return ("libnetaudio_core.so",)


class NetaudioCoreError(RuntimeError):
    def __init__(self, status: int, context: str = ""):
        self.status = status
        self.context = context
        self.detail = last_error_message()
        library = require()
        self.category = library.netaudio_status_category(status).decode("ascii")
        message = library.netaudio_status_description(status).decode("utf-8")
        if self.detail and self.detail != message:
            message = f"{message} ({self.detail})"
        super().__init__(f"{context}: {message}" if context else message)


class NetaudioCoreJsonError(ValueError):
    def __init__(self, category: str, context: str):
        self.category = category
        self.context = context
        operation = "decoding" if category == "json_decoding" else "encoding"
        super().__init__(f"{context}: JSON {operation} failed")


def _decode_json_output(data: bytes, context: str):
    try:
        return json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exception:
        raise NetaudioCoreJsonError("json_decoding", context) from exception


def _encode_command_spec(spec: dict | str) -> bytes:
    try:
        return json.dumps(spec, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exception:
        raise NetaudioCoreJsonError("json_encoding", "command specification") from exception


class NetaudioCoreLibraryMissing(RuntimeError):
    pass


def _candidate_paths():
    override = os.environ.get("NETAUDIO_CORE_LIB")
    if override:
        yield Path(override)
        return

    for library_name in _library_names():
        yield Path(__file__).resolve().parent / library_name
        for profile in ("release", "debug"):
            yield _REPO_ROOT / "packages" / "netaudio-core" / "target" / profile / library_name


_library = None
_load_attempted = False
_load_failures: list[tuple[Path, str]] = []
_library_lock = threading.Lock()


def _abi_compatibility_failure(lib) -> str | None:
    try:
        version_function = lib.netaudio_abi_version
    except AttributeError:
        return "predates ABI versioning"
    version_function.argtypes = []
    version_function.restype = ctypes.c_uint32
    found = version_function()
    if found != ABI_VERSION:
        return f"ABI {found}, expected {ABI_VERSION}"
    return None


def _load():
    global _library, _load_attempted, _load_failures

    with _library_lock:
        if _load_attempted:
            return _library

        _load_failures = []

        for path in _candidate_paths():
            if not path:
                continue

            if not path.exists():
                _load_failures.append((path, "not found"))
                continue

            try:
                lib = ctypes.CDLL(str(path))
            except OSError as exception:
                _load_failures.append((path, str(exception)))
                continue

            compatibility_failure = _abi_compatibility_failure(lib)

            if compatibility_failure is not None:
                logger.warning(f"netaudio-core at {path} {compatibility_failure}, skipping")
                _load_failures.append((path, compatibility_failure))
                continue

            try:
                _library = _configure(lib)
            except AttributeError as exception:
                _load_failures.append((path, str(exception)))
                continue

            _load_attempted = True
            return _library

        _load_attempted = True
        return None


def available() -> bool:
    return _load() is not None


def require():
    library = _load()
    if library is not None:
        return library
    lines = [
        f"netaudio-core library not found (expected ABI {ABI_VERSION}).",
        "Build it with `make core`.",
        "Searched:",
    ]
    if _load_failures:
        lines.extend(f"  {path}: {reason}" for path, reason in _load_failures)
    else:
        lines.extend(f"  {path}" for path in _candidate_paths())
    raise NetaudioCoreLibraryMissing("\n".join(lines))


def _call_buffer(function, *leading_args, capacity=8192):
    require()
    out = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = function(*leading_args, out, capacity, ctypes.byref(length))
    if status == _STATUS_BUFFER_TOO_SMALL and length.value > capacity:
        out = (ctypes.c_uint8 * length.value)()
        capacity = length.value
        status = function(*leading_args, out, capacity, ctypes.byref(length))
    return status, bytes(out[: length.value])


def last_error_message() -> str:
    lib = _library
    if lib is None:
        return ""
    capacity = 1024
    out = (ctypes.c_uint8 * capacity)()
    length = ctypes.c_size_t(0)
    status = lib.netaudio_last_error_message(out, capacity, ctypes.byref(length))
    if status == _STATUS_BUFFER_TOO_SMALL and length.value > capacity:
        capacity = length.value
        out = (ctypes.c_uint8 * capacity)()
        status = lib.netaudio_last_error_message(out, capacity, ctypes.byref(length))
    if status != STATUS_OK:
        return ""
    return bytes(out[: length.value]).decode("utf-8", errors="replace")


def next_message_id() -> int:
    return require().netaudio_next_message_id()


def next_publication_id(previous: int) -> int:
    if isinstance(previous, bool) or not isinstance(previous, int) or not 0 <= previous <= 65535:
        raise ValueError("previous publication ID must be an unsigned 16-bit integer")

    return require().netaudio_next_publication_id(previous)


def next_panel_sequence() -> int:
    return require().netaudio_next_panel_sequence()


def next_dapi_wrapper_id(previous: int) -> int:
    if isinstance(previous, bool) or not isinstance(previous, int) or not 0 <= previous <= 65535:
        raise ValueError("previous wrapper ID must be an unsigned 16-bit integer")

    return require().netaudio_dapi_next_wrapper_id(previous)


def _call_json(function, value, context):
    status, data = _call_buffer(function, _encode_command_spec(value))

    if status != STATUS_OK:
        raise NetaudioCoreError(status, context)

    return _decode_json_output(data, context)


def performance_capabilities(facts: _requests.PerformanceFacts) -> _types.PerformanceCapabilities:
    return _call_json(require().netaudio_performance_capabilities, facts, "performance capabilities")


def panel_profile(facts: _requests.PanelProfileRequest) -> _types.PanelProfile:
    return _call_json(require().netaudio_panel_profile, facts, "panel profile")


def panel_presentation(request: _requests.PanelPresentationRequest) -> _types.PanelPresentation:
    return _call_json(require().netaudio_panel_presentation, request, "panel presentation")


def plan_panel(request: _requests.PanelPlanRequest) -> _types.PanelPlan:
    return _call_json(require().netaudio_plan_panel, request, "panel plan")


def panel_readback_matches(request: _requests.PanelReadbackRequest) -> bool:
    return _call_json(require().netaudio_panel_readback_matches, request, "panel readback")


def performance_completion(request: _requests.PerformanceCompletionRequest) -> _types.PerformanceCompletion:
    return _call_json(require().netaudio_performance_completion, request, "performance completion")


def operation_availability(facts: _requests.AvailabilityRequest) -> _types.Availability:
    return _call_json(require().netaudio_operation_availability, facts, "operation availability")


def connection_health_update(request: _requests.ConnectionHealthUpdateRequest) -> _types.ConnectionHealthUpdate | None:
    return _call_json(require().netaudio_connection_health_update, request, "connection-health update")


def sample_rate_status_evidence(status: _requests.SampleRateStatus) -> _types.SampleRateStatus:
    return _call_json(
        require().netaudio_sample_rate_evidence, {"kind": "status", "status": status}, "sample-rate status"
    )


def sample_rate_capacity(
    capacities: list[_requests.ChannelCapacity] | None, sample_rate_hertz: int
) -> _types.ChannelCapacity | None:
    return _call_json(
        require().netaudio_sample_rate_evidence,
        {"kind": "capacity", "capacities": capacities, "sample_rate_hertz": sample_rate_hertz},
        "sample-rate capacity",
    )


def verify_sample_rate_receiver_inventory(channel_numbers: list[int], receive_channel_count: int) -> None:
    _call_json(
        require().netaudio_sample_rate_evidence,
        {
            "kind": "receiver_inventory",
            "channel_numbers": channel_numbers,
            "receive_channel_count": receive_channel_count,
        },
        "sample-rate receiver inventory",
    )


def latency_configuration(settings: Mapping[str, _requests.JsonValue]) -> _types.LatencyConfiguration:
    return _call_json(require().netaudio_latency_configuration, dict(settings), "latency configuration")


def latency_control(
    requested_milliseconds: float,
    settings: _types.LatencyState | dict[str, _requests.JsonValue] | None = None,
    acknowledged: bool | None = None,
) -> _types.LatencyCompletion:
    return _call_json(
        require().netaudio_latency_control,
        {"requested_milliseconds": requested_milliseconds, "settings": settings, "acknowledged": acknowledged},
        "latency control",
    )


def settings_capabilities(facts: _requests.SettingsCapabilityFacts) -> _types.SettingsCapabilities:
    return _call_json(require().netaudio_settings_capabilities, facts, "settings capabilities")


def clock_sources(facts: _requests.ClockSourceFacts) -> _types.ClockSources:
    return _call_json(require().netaudio_clock_sources, facts, "clock sources")


def performance_snapshot(facts: _requests.PerformanceSnapshotFacts) -> _types.PerformanceSnapshot:
    return _call_json(require().netaudio_performance_snapshot, facts, "performance snapshot")


def plan_performance_command(spec: dict) -> list[dict]:
    return _call_json(require().netaudio_plan_performance_command, spec, "performance command plan")


def build_managed_command(
    specification: dict, *, host_mac: bytes | None = None, message_id: int
) -> _types.ManagedCommand:
    request: _requests.ManagedCommandRequest = {
        "specification": specification,
        "host_mac": host_mac.hex() if host_mac is not None else None,
        "message_id": message_id,
    }
    return _call_json(require().netaudio_build_managed_command, request, "managed command plan")


def audio_capability_readback(status: dict | None, requested_value: int) -> _types.AudioReadbackResult:
    request: _requests.AudioCapabilityReadback = {"status": status, "requested_value": requested_value}

    return _call_json(
        require().netaudio_audio_capability_readback,
        request,
        "audio capability readback",
    )


def flow_format_readback(status: dict | None, requested_value: int) -> _types.AudioReadbackResult:
    request: _requests.AudioCapabilityReadback = {"status": status, "requested_value": requested_value}

    return _call_json(require().netaudio_flow_format_readback, request, "flow format readback")


def audio_capability_control(update_mode, available_values, requested_value=None, host_disabled=None) -> list[str]:
    return _call_json(
        require().netaudio_audio_capability_control,
        {
            "update_mode": update_mode,
            "available_values": available_values,
            "requested_value": requested_value,
            "host_disabled": host_disabled,
        },
        "audio capability control",
    )


def redundancy_control(
    state: dict | None, mode: str | None = None, *, readback: dict | None = None
) -> _types.RedundancyControl:
    return _call_json(
        require().netaudio_redundancy_control,
        {"state": state, "mode": mode, "readback": readback},
        "redundancy control",
    )


def interface_configuration(request: _requests.InterfaceConfigurationRequest) -> _types.InterfaceConfiguration:
    return _call_json(require().netaudio_interface_configuration, request, "interface configuration")


def analog_access(facts: _requests.AnalogAccess) -> str | None:
    return _call_json(require().netaudio_analog_access, facts, "analog access")


def analog_level_control(
    adapter: _requests.GainStatus | None, channel: int, level: int, direction: str | None = None
) -> _types.AnalogLevelPlan:
    return _call_json(
        require().netaudio_analog_level_control,
        {"adapter": adapter, "channel": channel, "level": level, "direction": direction},
        "analog level control",
    )


def verify_interface_configuration(request: _requests.InterfaceReadbackRequest) -> None:
    _call_json(require().netaudio_verify_interface_configuration, request, "interface configuration readback")


def metering_scale() -> _types.MeteringScales:
    status, data = _call_buffer(require().netaudio_metering_scale)

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "metering scale")

    return _decode_json_output(data, "metering scale")


def gain_metadata() -> _types.GainMetadata:
    status, data = _call_buffer(require().netaudio_gain_metadata)

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "gain metadata")

    return _decode_json_output(data, "gain metadata")


def network_control_state(facts: _requests.NetworkControlFacts) -> _types.NetworkControlState:
    return _call_json(require().netaudio_network_control_state, facts, "network control state")


def interface_redundancy_status(
    observation: _requests.InterfaceRedundancyObservation,
) -> _types.InterfaceRedundancyResult | None:
    return _call_json(require().netaudio_interface_redundancy_status, observation, "interface redundancy status")


def external_subscription_readback(request: _requests.ExternalReadbackRequest) -> _types.ExternalSubscriptionReadback:
    return _call_json(
        require().netaudio_external_subscription_readback,
        request,
        "external subscription readback",
    )


def build_response(spec: dict) -> bytes:
    status, data = _call_buffer(require().netaudio_build_response, _encode_command_spec(spec))

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "build response")

    return data


def build_command(spec: dict) -> bytes:
    lib = require()
    status, data = _call_buffer(lib.netaudio_build_command, _encode_command_spec(spec))
    if status == STATUS_INVALID_SEQUENCE and "message_id" not in spec:
        spec = {**spec, "message_id": next_message_id()}
        status, data = _call_buffer(lib.netaudio_build_command, _encode_command_spec(spec))
    if status != STATUS_OK:
        raise NetaudioCoreError(status, f"build_command {spec.get('command')}")
    return data


def plan_transmit_flow_delete(spec: _requests.FlowDeleteRequest) -> _types.FlowCommandPlan:
    return _call_json(require().netaudio_plan_transmit_flow_delete, spec, "plan transmit flow deletion")


def verify_sample_rate_topology(
    before: _requests.TopologySnapshot,
    after: _requests.TopologySnapshot,
    target_capacity: _requests.ChannelCapacity | None,
    target_sample_rate_hertz: int,
) -> None:
    status, _ = _call_buffer(
        require().netaudio_verify_sample_rate_topology,
        _encode_command_spec(
            {
                "before": before,
                "after": after,
                "target_capacity": target_capacity,
                "target_sample_rate_hertz": target_sample_rate_hertz,
            }
        ),
    )

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "sample-rate topology readback")


def sample_rate_topology_impact(
    snapshot: _requests.TopologySnapshot, target_capacity: _requests.ChannelCapacity | None
) -> _types.TopologyImpact:
    return _call_json(
        require().netaudio_sample_rate_topology_impact,
        {"snapshot": snapshot, "target_capacity": target_capacity},
        "sample-rate topology impact",
    )


def transmit_flow_topology(record: dict, *, protocol_id: int) -> _types.FlowTopology:
    request: _requests.FlowReadbackRequest = {"record": record, "protocol_id": protocol_id}
    return _call_json(require().netaudio_transmit_flow_topology, request, "transmitter flow topology")


def transmit_flow_specification(record: dict, *, protocol_id: int) -> _types.ObservedTransmitFlowSpecification:
    request: _requests.FlowReadbackRequest = {"record": record, "protocol_id": protocol_id}
    return _call_json(require().netaudio_transmit_flow_specification, request, "transmitter flow specification")


def normalize_transmit_flow_specification(
    specification: _requests.TransmitFlowSpecification,
) -> _requests.TransmitFlowSpecification:
    return _call_json(
        require().netaudio_normalize_transmit_flow_specification, specification, "transmitter flow specification"
    )


def plan_transmit_flow_create(spec: _requests.FlowCreateRequest) -> _types.FlowCommandPlan:
    return _call_json(require().netaudio_plan_transmit_flow_create, spec, "plan transmit flow creation")


def compare_transmit_flows(
    requested: _requests.TransmitFlowSpecification | _types.ObservedTransmitFlowSpecification,
    effective: _requests.TransmitFlowSpecification | _types.ObservedTransmitFlowSpecification,
) -> _types.FlowComparison:
    return _call_json(
        require().netaudio_compare_transmit_flows,
        {"requested": requested, "effective": effective},
        "transmit flow readback",
    )


def flow_creation_candidate(request: _requests.FlowCandidateRequest) -> _types.FlowCandidate:
    return _call_json(require().netaudio_flow_creation_candidate, request, "flow creation readback")


def flow_inventory_complete(inventory: dict | None) -> bool:
    return _call_json(require().netaudio_flow_inventory_complete, inventory, "flow inventory completeness")


def flow_create_preflight(request: _requests.FlowCreatePreflightRequest) -> _types.FlowCreatePreflight:
    return _call_json(require().netaudio_flow_create_preflight, request, "flow creation preflight")


def flow_delete_preflight(request: _requests.FlowDeletePreflightRequest) -> _types.FlowDeletePreflight:
    return _call_json(require().netaudio_flow_delete_preflight, request, "flow deletion preflight")


def flow_topology_change(request: _requests.FlowTopologyChangeRequest) -> _types.FlowTopologyChange | None:
    return _call_json(require().netaudio_flow_topology_change, request, "flow topology change")


def flow_verification(request: _requests.FlowVerificationRequest) -> _types.FlowVerification:
    return _call_json(require().netaudio_flow_verification, request, "flow verification")


def canonical_device_mac(value: str | None) -> str | None:
    return device_identity({"kind": "mac", "value": value if isinstance(value, str) else None})


def device_identity(request: _requests.DeviceIdentityRequest) -> str | None:
    return _call_json(require().netaudio_device_identity, request, "device identity")


def validate_configuration(values: Mapping[str, _requests.JsonValue]) -> None:
    request: _requests.ConfigurationRequest = {"kind": "validate", "values": dict(values)}
    _call_json(require().netaudio_configuration, request, "configuration values")


def capture_panel_configuration(fresh_values: dict[str, _requests.JsonValue]) -> dict[str, _types.JsonValue]:
    request: _requests.ConfigurationRequest = {"kind": "capture_panel", "fresh_values": fresh_values}
    return _call_json(require().netaudio_configuration, request, "panel configuration")


def channel_audio_publication(spec: _requests.ChannelAudioConfiguration) -> _types.ChannelAudioPublication | None:
    return _call_json(require().netaudio_channel_audio_publication, spec, "channel audio publication")


def virtual_device_advertisements(spec: _requests.VirtualDeviceAdvertisement) -> list[_types.ServiceAdvertisement]:
    return _call_json(require().netaudio_virtual_device_advertisements, spec, "virtual device advertisements")


def build_publication(spec: dict) -> bytes:
    status, data = _call_buffer(require().netaudio_build_publication, _encode_command_spec(spec))

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "build publication")

    return data


def parse_response(kind: str, data: bytes):
    lib = require()
    in_buffer = (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else (ctypes.c_uint8 * 0)()
    status, out = _call_buffer(lib.netaudio_parse_response, kind.encode("utf-8"), in_buffer, len(data))
    if status != STATUS_OK:
        raise NetaudioCoreError(status, f"parse_response {kind}")
    return _decode_json_output(out, f"parse {kind}")


def parse_diagnostic_audio(data: bytes) -> _types.DiagnosticAudioCapabilities | None:
    return parse_response("diagnostic_audio_capabilities", data)


def parse_connection_health(data: bytes) -> _requests.HeartbeatConnectionHealthRecords:
    return parse_response("heartbeat_connection_health", data)


def parse_sap(data: bytes) -> _types.SapAnnouncement:
    return parse_response("sap", data)


def controller_api_routes(data: bytes) -> _types.ControllerApiRoutes:
    return parse_response("controller_api_routes", data)


def controller_endpoints(data: bytes) -> _types.ControllerEndpoints:
    return parse_response("controller_endpoints", data)


def controller_login(data: bytes) -> _types.ControllerLoginResult:
    return parse_response("controller_login", data)


def sap_transition(request: _requests.SapTransitionRequest) -> _types.SapTransitionResult:
    return _call_json(require().netaudio_sap_transition, request, "SAP announcement transition")


def parse_sdp(text: str) -> _types.SdpDocument:
    return parse_response("sdp", text.encode("utf-8"))


def parse_page(kind: str, data: bytes, starting_channel: int):
    lib = require()
    in_buffer = (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else (ctypes.c_uint8 * 0)()
    status, out = _call_buffer(lib.netaudio_parse_page, kind.encode("utf-8"), in_buffer, len(data), starting_channel)
    if status != STATUS_OK:
        raise NetaudioCoreError(status, f"parse_page {kind}")
    return _decode_json_output(out, f"parse {kind}")


def lock_key_length() -> int:
    return require().netaudio_lock_key_length()


def validate_lock_pin(pin: str) -> None:
    status = require().netaudio_validate_lock_pin(_encode_command_spec(pin))

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "validate_lock_pin")


def lock_token(pin: str, nonce: bytes, key: bytes) -> bytes:
    validate_lock_pin(pin)
    lib = require()
    nonce_buffer = (ctypes.c_uint8 * len(nonce)).from_buffer_copy(nonce)
    key_buffer = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
    status, token = _call_buffer(
        lib.netaudio_lock_token,
        pin.encode("ascii"),
        nonce_buffer,
        len(nonce),
        key_buffer,
        len(key),
        capacity=256,
    )
    if status != STATUS_OK:
        raise NetaudioCoreError(status, "lock_token")
    return token


def host_mac() -> bytes | None:
    lib = _load()
    if lib is None:
        return None
    out = (ctypes.c_uint8 * 6)()
    if lib.netaudio_host_mac(out) == STATUS_OK:
        return bytes(out)
    return None


def host_mac_for_ipv4(local_ip: str) -> bytes | None:
    address = str(ipaddress.IPv4Address(local_ip))
    lib = _load()
    if lib is None:
        return None
    out = (ctypes.c_uint8 * 6)()
    if lib.netaudio_host_mac_for_ipv4(address.encode("ascii"), out) == STATUS_OK:
        return bytes(out)
    return None


def _checked_dapi_frame(function, *arguments) -> bytes:
    status, frame = _call_buffer(function, *arguments)
    if status != STATUS_OK:
        raise NetaudioCoreError(status, "build DAPI frame")
    return frame


def build_dapi_session_open() -> bytes:
    return _checked_dapi_frame(require().netaudio_dapi_build_session_open)


def correlate_managed_arc(request: _requests.ManagedArcCorrelationRequest) -> _types.ManagedArcResponse | None:
    return _call_json(require().netaudio_dapi_correlate_arc_response, request, "managed ARC response")


def advance_managed_settings(request: _requests.ManagedSettingsRequest) -> _types.ManagedSettingsResult:
    return _call_json(require().netaudio_dapi_advance_settings_exchange, request, "managed settings exchange")


def advance_managed_session(request: _requests.ManagedSessionRequest) -> _types.ManagedSessionStep:
    return _call_json(require().netaudio_dapi_advance_session, request, "managed session")


def plan_external_subscription(
    request: _requests.ExternalSubscriptionPlanRequest,
) -> _requests.ExternalSubscriptionCommand:
    return _call_json(require().netaudio_plan_external_subscription, request, "external subscription plan")


def validate_managed_credential(request: _requests.ManagedCredential) -> None:
    _call_json(require().netaudio_dapi_validate_credential, request, "managed credential")


def clock_control_availability(status: Mapping[str, _types.JsonValue]) -> _types.ClockControlAvailability:
    return _call_json(require().netaudio_clock_control_availability, dict(status), "clock control availability")


def build_dapi_authentication(credential: str) -> bytes:
    encoded = credential.encode("ascii")
    return _checked_dapi_frame(
        require().netaudio_dapi_build_authentication,
        _as_buffer(encoded),
        len(encoded),
    )


def build_dapi_domain_initialization(
    domain_id: bytes,
    first_message_id: int,
    notification_port: int,
    local_ipv4: bytes,
) -> bytes:
    if len(local_ipv4) != 4:
        raise ValueError("local IPv4 address must be exactly 4 bytes")
    return _checked_dapi_frame(
        require().netaudio_dapi_build_domain_initialization,
        _as_buffer(domain_id),
        len(domain_id),
        first_message_id,
        notification_port,
        _as_buffer(local_ipv4),
    )


def build_dapi_identify(target_selector: int, wrapper_id: int, message_id: int, mac: bytes) -> bytes:
    if len(mac) != 6:
        raise ValueError("host MAC must be exactly 6 bytes")
    return _checked_dapi_frame(
        require().netaudio_dapi_build_identify,
        target_selector,
        wrapper_id,
        message_id,
        _as_buffer(mac),
    )


def build_dapi_arc_request(target_selector: int, wrapper_id: int, arc_packet: bytes) -> bytes:
    return _checked_dapi_frame(
        require().netaudio_dapi_build_arc_request,
        target_selector,
        wrapper_id,
        _as_buffer(arc_packet),
        len(arc_packet),
    )


def build_dapi_settings_request(target_selector: int, wrapper_id: int, settings_packet: bytes) -> bytes:
    return _checked_dapi_frame(
        require().netaudio_dapi_build_settings_request,
        target_selector,
        wrapper_id,
        _as_buffer(settings_packet),
        len(settings_packet),
    )


def build_dapi_service_acknowledgement(announcement_frame: bytes) -> bytes:
    return _checked_dapi_frame(
        require().netaudio_dapi_build_service_acknowledgement,
        _as_buffer(announcement_frame),
        len(announcement_frame),
    )


def _as_buffer(data: bytes):
    return (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else (ctypes.c_uint8 * 0)()


def command_acknowledgement(response: bytes | None) -> _types.CommandReceipt | None:
    if response is None:
        return None

    return parse_response("command_receipt", response)


def flow_authoring_capabilities(capability_word: int) -> _types.FlowAuthoringCapabilities:
    if isinstance(capability_word, bool) or not isinstance(capability_word, int) or not 0 <= capability_word <= 65535:
        raise ValueError("capability word must be an unsigned 16-bit integer")

    status, data = _call_buffer(require().netaudio_flow_authoring_capabilities, capability_word)

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "flow authoring capabilities")

    return _decode_json_output(data, "flow authoring capabilities")


def transmit_flow_inventory_protocols(advertised: int, observed: int) -> list[int]:
    for value in (advertised, observed):
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 65535:
            raise ValueError("protocol identifier must fit an unsigned 16-bit integer")

    status, data = _call_buffer(require().netaudio_transmit_flow_inventory_protocols, advertised, observed)

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "transmitter inventory protocols")

    return _decode_json_output(data, "transmitter inventory protocols")


def arc_protocol(version: str | None, *, managed: bool = False) -> _types.ArcProtocol | None:
    if version is not None and (not isinstance(version, str) or "\0" in version):
        raise ValueError("ARC version must be a string without null bytes")

    status, data = _call_buffer(
        require().netaudio_arc_protocol, version.encode("utf-8") if version is not None else None, managed
    )

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "ARC protocol")

    return _decode_json_output(data, "ARC protocol")


class ConmonExportCollector:
    def __init__(self, configuration: _requests.ExportConfiguration):
        self._native_lock = threading.RLock()
        self._handle = ctypes.c_void_p()
        self._lib = require()
        status = self._lib.netaudio_export_new(_encode_command_spec(dict(configuration)), ctypes.byref(self._handle))

        if status != STATUS_OK:
            raise NetaudioCoreError(status, "ConMon export")

    def close(self):
        with self._native_lock:
            if self._handle:
                self._lib.netaudio_export_free(self._handle)
                self._handle = ctypes.c_void_p()

    def __enter__(self):
        return self

    def __exit__(self, *_exception_information):
        self.close()

    def __del__(self):
        self.close()

    def accept(self, fragment: _requests.ConmonExportFragment) -> _types.ExportProgress:
        with self._native_lock:
            if not self._handle:
                raise RuntimeError("ConMon export collector is closed")

            status, data = _call_buffer(
                self._lib.netaudio_export_accept, self._handle, _encode_command_spec(dict(fragment))
            )

            if status != STATUS_OK:
                raise NetaudioCoreError(status, "ConMon export")

            return _decode_json_output(data, "ConMon export")


class _Inventory:
    def __init__(self, kind: str, protocol_id: int, maximum_pages: int = 256):
        self._native_lock = threading.RLock()
        self._handle = ctypes.c_void_p()
        self._lib = require()

        if not isinstance(protocol_id, int) or isinstance(protocol_id, bool) or not 0 <= protocol_id <= 65535:
            raise ValueError("protocol_id must fit an unsigned 16-bit integer")

        if (
            not isinstance(maximum_pages, int)
            or isinstance(maximum_pages, bool)
            or not 0 <= maximum_pages <= ctypes.c_size_t(-1).value
        ):
            raise ValueError("maximum_pages must fit an unsigned size_t integer")

        status = self._lib.netaudio_inventory_new(
            kind.encode("utf-8"), protocol_id, maximum_pages, ctypes.byref(self._handle)
        )

        if status != STATUS_OK:
            raise NetaudioCoreError(status, "inventory")

    def close(self):
        with self._native_lock:
            if self._handle:
                self._lib.netaudio_inventory_free(self._handle)
                self._handle = ctypes.c_void_p()

    def __enter__(self):
        return self

    def __exit__(self, *_exception_information):
        self.close()

    def __del__(self):
        self.close()

    def _require_open(self):
        if not self._handle:
            raise RuntimeError("inventory is closed")

    def accept(self, response: bytes):
        with self._native_lock:
            self._require_open()
            data = (ctypes.c_uint8 * len(response)).from_buffer_copy(response)
            status = self._lib.netaudio_inventory_accept(self._handle, data, len(response))

            if status != STATUS_OK:
                raise NetaudioCoreError(status, "inventory")

    def state(self) -> _types.InventoryState:
        with self._native_lock:
            self._require_open()
            status, data = _call_buffer(self._lib.netaudio_inventory_state, self._handle)

            if status != STATUS_OK:
                raise NetaudioCoreError(status, "inventory")

            return _decode_json_output(data, "inventory")


class ChannelInventory(_Inventory):
    def __init__(self, channel_type: str, protocol_id: int, maximum_pages: int = 256):
        super().__init__(f"{channel_type}_channels", protocol_id, maximum_pages)


class ReceiverFlowInventory(_Inventory):
    def __init__(self, protocol_id: int, maximum_pages: int = 256):
        super().__init__("rx_flows", protocol_id, maximum_pages)


class TransmitFlowInventory(_Inventory):
    def __init__(self, protocol_id: int, maximum_pages: int = 256):
        super().__init__("tx_flows", protocol_id, maximum_pages)


class CoreClient:
    def __init__(
        self,
        device_ip: str,
        arc_port: int = _PORT_ARC,
        timeout_ms: int = 1000,
        attempts: int = 3,
        *,
        local_ip: str | None = None,
    ):
        self._native_lock = threading.RLock()
        self._handle = ctypes.c_void_p()
        self._lib = None
        if local_ip is not None:
            if not isinstance(local_ip, str):
                raise ValueError("local_ip must be an IPv4 address string")
            local_ip = str(ipaddress.IPv4Address(local_ip))
        library = require()
        self._lib = library
        self._device_ip = device_ip
        with self._native_lock:
            status = library.netaudio_client_new(
                device_ip.encode("ascii"),
                local_ip.encode("ascii") if local_ip is not None else None,
                arc_port,
                timeout_ms,
                attempts,
                ctypes.byref(self._handle),
            )
        if status != STATUS_OK:
            raise NetaudioCoreError(status, f"client_new {device_ip}")

    def close(self):
        with self._native_lock:
            if self._handle and self._lib is not None:
                self._lib.netaudio_client_free(self._handle)
                self._handle = ctypes.c_void_p()

    def __enter__(self):
        return self

    def __exit__(self, *_exception_information):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            logger.exception("Failed to close native core client")

    def _require_library(self):
        if self._lib is None:
            raise NetaudioCoreError(_STATUS_IO_ERROR, "netaudio-core library not loaded")
        return self._lib

    @property
    def device_ip(self) -> str:
        return self._device_ip

    def clear_wire_captures(self) -> None:
        library = self._require_library()

        with self._native_lock:
            status = library.netaudio_client_clear_wire_captures(self._handle)

            if status != STATUS_OK:
                raise NetaudioCoreError(status, "clear_wire_captures")

    def get_wire_captures(self) -> list[dict]:
        library = self._require_library()

        with self._native_lock:
            status, data = _call_buffer(library.netaudio_client_get_wire_captures_json, self._handle, capacity=262144)

            if status != STATUS_OK:
                raise NetaudioCoreError(status, "get_wire_captures")

        return _decode_json_output(data, "wire captures")

    def set_host_mac(self, mac: bytes):
        if len(mac) != 6:
            raise ValueError("mac must be exactly 6 bytes")
        buffer = (ctypes.c_uint8 * 6).from_buffer_copy(mac)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_set_host_mac(self._handle, buffer)
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "set_host_mac")

    def request(
        self, packet: bytes, target_port: int, expect_response: bool = True, repeat: int = 1, interval_ms: int = 0
    ):
        out = (ctypes.c_uint8 * 65536)()
        length = ctypes.c_size_t(0)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_request(
                self._handle,
                _as_buffer(packet),
                len(packet),
                target_port,
                expect_response,
                repeat,
                interval_ms,
                out,
                65536,
                ctypes.byref(length),
            )
            data = bytes(out[: length.value])
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "client_request")
        response = data if expect_response else None
        return response

    def execute(self, spec: dict):
        out = (ctypes.c_uint8 * 65536)()
        length = ctypes.c_size_t(0)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_execute(
                self._handle, _encode_command_spec(spec), out, 65536, ctypes.byref(length)
            )
            data = bytes(out[: length.value])
        if status != STATUS_OK:
            raise NetaudioCoreError(status, f"execute {spec.get('command')}")
        return data if data else None

    def _json_getter(self, name, *arguments):
        out = (ctypes.c_uint8 * 262144)()
        length = ctypes.c_size_t(0)
        library = self._require_library()
        with self._native_lock:
            status = getattr(library, name)(self._handle, *arguments, out, 262144, ctypes.byref(length))
            data = bytes(out[: length.value])
        if status != STATUS_OK:
            raise NetaudioCoreError(status, name)
        return _decode_json_output(data, name)

    def get_rx_channels(self):
        return self._json_getter("netaudio_client_get_rx_channels_json")

    def get_rx_inventory(self, rx_count: int):
        out = (ctypes.c_uint8 * 262144)()
        length = ctypes.c_size_t(0)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_get_rx_inventory_json(
                self._handle,
                rx_count,
                out,
                262144,
                ctypes.byref(length),
            )
            data = bytes(out[: length.value])
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "netaudio_client_get_rx_inventory_json")
        return _decode_json_output(data, "receiver inventory")

    def get_tx_channels(self):
        return self._json_getter("netaudio_client_get_tx_channels_json")

    def get_device_name(self):
        return self._json_getter("netaudio_client_get_device_name_json")

    def get_device_info(self):
        return self._json_getter("netaudio_client_get_device_info_json")

    def get_device_settings(self):
        return self._json_getter("netaudio_client_get_device_settings_json")

    def get_property_directory(self):
        return self._json_getter("netaudio_client_get_property_directory_json")

    def get_channel_audio_metadata(self, tx_count: int, rx_count: int):
        for count in (tx_count, rx_count):
            if type(count) is not int or not 0 <= count <= 65535:
                raise ValueError("channel count must be an unsigned 16-bit integer")

        return self._json_getter("netaudio_client_get_channel_audio_metadata_json", tx_count, rx_count)

    def get_channel_count(self):
        tx = ctypes.c_uint16(0)
        rx = ctypes.c_uint16(0)
        transmit_flow_authoring_capability_word = ctypes.c_uint16(0)
        locked = ctypes.c_int32(-2)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_get_channel_count(
                self._handle,
                ctypes.byref(tx),
                ctypes.byref(rx),
                ctypes.byref(transmit_flow_authoring_capability_word),
                ctypes.byref(locked),
            )
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "get_channel_count")
        lock_state = None if locked.value < 0 else bool(locked.value)
        return tx.value, rx.value, lock_state, transmit_flow_authoring_capability_word.value

    def get_aes67_configured(self):
        state = ctypes.c_int32(-2)
        library = self._require_library()
        with self._native_lock:
            status = library.netaudio_client_get_aes67_configured(self._handle, ctypes.byref(state))
        if status != STATUS_OK:
            raise NetaudioCoreError(status, "get_aes67_configured")
        return None if state.value < 0 else bool(state.value)

    def lock(self, pin: str, key: bytes):
        return self._lock_op("netaudio_client_lock", pin, key)

    def unlock(self, pin: str, key: bytes):
        return self._lock_op("netaudio_client_unlock", pin, key)

    def _lock_op(self, name, pin, key):
        validate_lock_pin(pin)
        key_buffer = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
        out = (ctypes.c_uint8 * 4096)()
        length = ctypes.c_size_t(0)
        library = self._require_library()
        with self._native_lock:
            status = getattr(library, name)(
                self._handle,
                pin.encode("ascii"),
                key_buffer,
                len(key),
                out,
                4096,
                ctypes.byref(length),
            )
            data = bytes(out[: length.value])
        if status != STATUS_OK:
            raise NetaudioCoreError(status, name)
        return _decode_json_output(data, "Rust API response")


def normalize_clock_subdomain(value) -> bytes:
    if isinstance(value, (bytes, bytearray)):
        value = list(value)

    return bytes(_call_json(require().netaudio_normalize_clock_subdomain, value, "clock subdomain"))


def clock_subdomain_presentation(value) -> _types.ClockSubdomainPresentation:
    if isinstance(value, (bytes, bytearray)):
        value = list(value)

    return _call_json(require().netaudio_clock_subdomain_presentation, value, "clock subdomain presentation")


def managed_subscription_status(request: _requests.ManagedStatusRequest) -> _types.ManagedSubscriptionStatus:
    return _call_json(require().netaudio_managed_subscription_status, request, "managed subscription status")


def clock_record_revision(facts: dict) -> int:
    return _call_json(require().netaudio_clock_record_revision, facts, "clock record revision")


def flow_inventory_protocol(facts: _requests.FlowInventoryProtocolFacts) -> int | None:
    return _call_json(require().netaudio_flow_inventory_protocol, facts, "flow inventory protocol")


def plan_subscription_commands(spec: dict) -> list[dict]:
    return _call_json(require().netaudio_plan_subscription_commands, spec, "subscription page plan")


def subscription_readback(spec: _requests.SubscriptionReadbackRequest) -> _types.SubscriptionReadback:
    return _call_json(require().netaudio_subscription_readback, spec, "subscription readback")


def plan_subscription_reconciliation(spec: _requests.SubscriptionReconciliationRequest) -> _types.SubscriptionPlan:
    return _call_json(require().netaudio_plan_subscription_reconciliation, spec, "subscription reconciliation")


def receiver_self_connection_capabilities(spec: _requests.ReceiverCapabilityRequest) -> _types.ReceiverCapabilities:
    return _call_json(require().netaudio_receiver_self_connection_capabilities, spec, "receiver capabilities")


def plan_clock_configuration(spec: dict) -> _types.ClockPlan:
    spec = dict(spec)

    for field in ("status", "changes"):
        if isinstance(spec.get(field), dict):
            spec[field] = {
                key: list(value) if isinstance(value, (bytes, bytearray)) else value
                for key, value in spec[field].items()
            }

    return _call_json(require().netaudio_plan_clock_configuration, spec, "clock configuration plan")


def clock_configuration_matches(observed: dict, requested: dict) -> bool:
    observed = {key: list(value) if isinstance(value, bytes) else value for key, value in observed.items()}

    return _call_json(
        require().netaudio_clock_configuration_matches,
        {"status": observed, "requested": requested},
        "clock configuration readback",
    )


def subscription_status(code: int, receiver_status_code: int | None = None) -> _types.SubscriptionStatus:
    values = (code,) if receiver_status_code is None else (code, receiver_status_code)
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 0xFFFF:
            raise ValueError("subscription and receiver status values must be unsigned 16-bit integers")
    status, data = _call_buffer(
        require().netaudio_subscription_status, code, receiver_status_code or 0, receiver_status_code is not None
    )
    if status != STATUS_OK:
        raise NetaudioCoreError(status, "subscription_status")
    return _decode_json_output(data, "Rust API response")


def subscription_classification_for_identifier(identifier: str | None) -> _types.SubscriptionClassification:
    if identifier is None:
        identifier = ""

    if not isinstance(identifier, str) or "\0" in identifier:
        raise ValueError("subscription status identifier must be a string without null bytes")

    status, data = _call_buffer(
        require().netaudio_subscription_classification_for_identifier, identifier.encode("utf-8")
    )

    if status != STATUS_OK:
        raise NetaudioCoreError(status, "subscription_classification_for_identifier")

    return _decode_json_output(data, "Rust API response")
