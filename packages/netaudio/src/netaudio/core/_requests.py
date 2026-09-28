# Generated from Rust schemas by scripts/generate_core_types.py. Do not edit.
import typing as _typing
import typing_extensions as _extensions


JsonValue = _typing.Union[None, bool, int, float, str, list["JsonValue"], dict[str, "JsonValue"]]


class AnalogAccess(_extensions.TypedDict):
    address_available: bool
    locked: _extensions.NotRequired[_typing.Union[bool, None]]
    managed: bool
    online: _extensions.NotRequired[_typing.Union[bool, None]]
    supported: _extensions.NotRequired[_typing.Union[bool, None]]
    write: bool


class GainStatus(_extensions.TypedDict):
    channel_levels: list[int]
    device_type: str
    supported_levels: list[int]


class AnalogLevelRequest(_extensions.TypedDict):
    adapter: _extensions.NotRequired[_typing.Union[GainStatus, None]]
    channel: JsonValue
    direction: _extensions.NotRequired[_typing.Union[str, None]]
    level: JsonValue


class AudioCapabilityControl(_extensions.TypedDict):
    available_values: _extensions.NotRequired[_typing.Union[list[int], None]]
    host_disabled: _extensions.NotRequired[_typing.Union[bool, None]]
    requested_value: _extensions.NotRequired[_typing.Union[int, None]]
    update_mode: _extensions.NotRequired[_typing.Union[int, None]]


class AudioCapabilityReadback(_extensions.TypedDict):
    requested_value: int
    status: _extensions.NotRequired[_typing.Union[dict[str, JsonValue], None]]


Authority = _typing.Literal["direct", "managed", "observed"]


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


class ChannelAudioConfiguration(_extensions.TypedDict):
    encoding: int
    sample_rate: int
    supported_encodings: list[int]


class ChannelCapacity(_extensions.TypedDict):
    receive_channel_count: int
    sample_rate_hertz: int
    transmit_channel_count: int


class ChannelSlot(_extensions.TypedDict):
    slot: int
    transmitter_channel: int


class ClockPortControl(_extensions.TypedDict):
    announce_interval: _extensions.NotRequired[_typing.Union[int, None]]
    delay_mechanism: _extensions.NotRequired[_typing.Union[int, None]]
    delay_request_interval: _extensions.NotRequired[_typing.Union[int, None]]
    follower_only: _extensions.NotRequired[_typing.Union[bool, None]]
    peer_delay_interval: _extensions.NotRequired[_typing.Union[int, None]]
    port_id: int
    sync_interval: _extensions.NotRequired[_typing.Union[int, None]]
    ttl: _extensions.NotRequired[_typing.Union[int, None]]


class ClockControl(_extensions.TypedDict):
    advanced: _extensions.NotRequired[bool]
    aggregate_ptpv1_unicast_delay_requests: _extensions.NotRequired[_typing.Union[bool, None]]
    aggregate_ptpv2_unicast_delay_requests: _extensions.NotRequired[_typing.Union[bool, None]]
    clock_capabilities: _extensions.NotRequired[_typing.Union[int, None]]
    clock_source: _extensions.NotRequired[_typing.Union[int, None]]
    control_profile: _extensions.NotRequired[int]
    extension_flags: _extensions.NotRequired[_typing.Union[int, None]]
    follower_only: _extensions.NotRequired[_typing.Union[bool, None]]
    global_unicast_delay_requests: _extensions.NotRequired[_typing.Union[bool, None]]
    multicast_dscp: _extensions.NotRequired[_typing.Union[int, None]]
    ports: _extensions.NotRequired[list[ClockPortControl]]
    preferred_leader: _extensions.NotRequired[_typing.Union[bool, None]]
    preferred_protocol: _extensions.NotRequired[_typing.Union[int, None]]
    priority_mapping: _extensions.NotRequired[_typing.Union[int, None]]
    ptpv1_enabled: _extensions.NotRequired[_typing.Union[bool, None]]
    ptpv2_clock_class: _extensions.NotRequired[_typing.Union[int, None]]
    ptpv2_domain: _extensions.NotRequired[_typing.Union[int, None]]
    ptpv2_enabled: _extensions.NotRequired[_typing.Union[bool, None]]
    ptpv2_priority1: _extensions.NotRequired[_typing.Union[int, None]]
    ptpv2_priority2: _extensions.NotRequired[_typing.Union[int, None]]
    status_revision: _extensions.NotRequired[_typing.Union[int, None]]
    subdomain: _extensions.NotRequired[_typing.Union[list[int], None]]
    supported_clock_sources: _extensions.NotRequired[list[int]]


class Observation(_extensions.TypedDict):
    clock_state_evidence: _extensions.NotRequired[JsonValue]
    display_epoch: int
    epoch: int
    evidence: JsonValue
    latency_microseconds: _extensions.NotRequired[_typing.Union[int, None]]
    observed_at: str
    observed_monotonic: float
    raw: int
    raw_record: list[int]
    record_type: int
    sample_rate_hertz: _extensions.NotRequired[_typing.Union[int, None]]
    sequence: _extensions.NotRequired[_typing.Union[int, None]]
    sequence_gap: int
    source: str
    timestamp_provenance: str
    value: _extensions.NotRequired[_typing.Union[float, None]]
    value_unit: str


class Series(_extensions.TypedDict):
    baseline: _extensions.NotRequired[_typing.Union[int, None]]
    current: _extensions.NotRequired[_typing.Union[Observation, None]]
    delta: _extensions.NotRequired[_typing.Union[int, None]]
    display_epoch: int
    fresh: bool
    histogram: JsonValue
    history: list[Observation]
    increase_since_baseline: _extensions.NotRequired[_typing.Union[int, None]]
    statistics: JsonValue


class Diagnostic(_extensions.TypedDict):
    evidence: JsonValue
    kind: str


class ClockVariation(_extensions.TypedDict):
    active: bool
    deviation_ppb: _extensions.NotRequired[_typing.Union[float, None]]
    last_epoch: _extensions.NotRequired[_typing.Union[int, None]]
    last_observed_monotonic: _extensions.NotRequired[_typing.Union[float, None]]
    observable: bool
    recovered: bool
    recovery_since: _extensions.NotRequired[_typing.Union[float, None]]
    threshold_ppb: int
    window_size: int


class ClockObservations(_extensions.TypedDict):
    conmon: Series
    diagnostics: list[Diagnostic]
    heartbeat: Series
    variation: _extensions.NotRequired[dict[str, ClockVariation]]
    warning_enabled: _extensions.NotRequired[bool]


class ClockObservationRequest(_extensions.TypedDict):
    conmon_status: _extensions.NotRequired[JsonValue]
    freshness_seconds: float
    history_limit: int
    observed_at: str
    observed_monotonic: float
    packet: _extensions.NotRequired[_typing.Union[list[int], None]]
    previous: _extensions.NotRequired[_typing.Union[ClockObservations, None]]
    reset: _extensions.NotRequired[bool]
    warning_enabled: _extensions.NotRequired[_typing.Union[bool, None]]


class ClockPlanRequest(_extensions.TypedDict):
    changes: dict[str, JsonValue]
    control_profile: _extensions.NotRequired[_typing.Union[int, None]]
    status: dict[str, JsonValue]
    supported_clock_sources: _extensions.NotRequired[list[int]]


class ClockProfile(_extensions.TypedDict):
    control_profile: _extensions.NotRequired[_typing.Union[int, None]]


class ClockReadbackRequest(_extensions.TypedDict):
    requested: dict[str, JsonValue]
    status: dict[str, JsonValue]


class ClockSourceFacts(_extensions.TypedDict):
    current: _extensions.NotRequired[JsonValue]
    supported: list[JsonValue]


class ConfigurationRequestValidate(_extensions.TypedDict):
    kind: _typing.Literal["validate"]
    values: dict[str, JsonValue]


class ConfigurationRequestCapturePanel(_extensions.TypedDict):
    fresh_values: dict[str, JsonValue]
    kind: _typing.Literal["capture_panel"]


ConfigurationRequest = _typing.Union[ConfigurationRequestValidate, ConfigurationRequestCapturePanel]


class ConmonExportFragment(_extensions.TypedDict):
    data_hexadecimal: str
    echoed_tag_hexadecimal: str
    envelope_sequence_identifier: int
    fragment_identifier: int
    fragment_size: int
    has_more_fragments: bool
    header_size: int
    record_protocol_identifier: int
    selector_value: int
    total_encoded_size: int


class ReceiverPath(_extensions.TypedDict):
    attribution_epoch: int
    attribution_reason: _extensions.NotRequired[_typing.Union[str, None]]
    attribution_status: str
    audio_receiver_flow_id: _extensions.NotRequired[_typing.Union[int, None]]
    evidence: JsonValue
    global_flow_id: _extensions.NotRequired[_typing.Union[int, None]]
    late_packets: Series
    latency: Series
    media_type: _extensions.NotRequired[_typing.Union[str, None]]
    network_interface_index: _extensions.NotRequired[_typing.Union[int, None]]
    telemetry_index: int


class ConnectionHealthUpdate(_extensions.TypedDict):
    complete: bool
    device_extended_unique_identifier: str
    diagnostics: list[Diagnostic]
    fresh: bool
    paths: list[ReceiverPath]
    retention_limit: int


class HeartbeatLatePacketEntry(_extensions.TypedDict):
    late_packet_count: int
    telemetry_index: int


class HeartbeatLatePacketRecord(_extensions.TypedDict):
    entries: list[HeartbeatLatePacketEntry]
    raw_record: list[int]
    sequence: int


class HeartbeatFlowLatencyEntry(_extensions.TypedDict):
    latency_sample_count: int
    telemetry_index: int


class HeartbeatFlowLatencyRecord(_extensions.TypedDict):
    entries: list[HeartbeatFlowLatencyEntry]
    raw_record: list[int]
    sample_rate_hertz: int
    sequence: int


class HeartbeatConnectionHealthRecords(_extensions.TypedDict):
    device_extended_unique_identifier: str
    diagnostics: list[Diagnostic]
    late_packet_records: list[HeartbeatLatePacketRecord]
    latency_records: list[HeartbeatFlowLatencyRecord]


class Topology(_extensions.TypedDict):
    capacity: _extensions.NotRequired[JsonValue]
    complete: _extensions.NotRequired[bool]
    flows: _extensions.NotRequired[list[JsonValue]]
    inventory_family: _extensions.NotRequired[_typing.Union[str, None]]


class ConnectionHealthUpdateRequest(_extensions.TypedDict):
    freshness_seconds: float
    history_limit: int
    observed_at: str
    observed_monotonic: float
    previous: _extensions.NotRequired[_typing.Union[ConnectionHealthUpdate, None]]
    records: HeartbeatConnectionHealthRecords
    refresh_only: _extensions.NotRequired[bool]
    reset: _extensions.NotRequired[bool]
    topology: Topology


class Destination(_extensions.TypedDict):
    address: str
    interface: _extensions.NotRequired[_typing.Union[str, None]]
    port: int


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


class Endpoint(_extensions.TypedDict):
    ipv4_address: str
    udp_port: int


class Evidence(_extensions.TypedDict):
    direct: _extensions.NotRequired[_typing.Union[bool, None]]
    managed: _extensions.NotRequired[_typing.Union[bool, None]]
    managed_fresh: bool


class ExpectedSubscription(_extensions.TypedDict):
    number: int
    source: _extensions.NotRequired[_typing.Union[list[str], None]]


ExportKind = _typing.Literal["diagnostic_logs", "capability_partition"]


class ExportConfiguration(_extensions.TypedDict):
    kind: ExportKind
    maximum_encoded_size: int


class ExternalRtpDestinationSpec(_extensions.TypedDict):
    address: str
    port: _extensions.NotRequired[int]


class ExternalSubscriptionCommandSubscribeExternalRtp(_extensions.TypedDict):
    advertised_flow_slot_count: int
    advertisement_supports_multiple_interfaces: _extensions.NotRequired[bool]
    clock_offset: _extensions.NotRequired[_typing.Union[int, None]]
    command: _typing.Literal["subscribe_external_rtp"]
    device_protocol: int
    flow_slot_assignments: list[int]
    message_id: _extensions.NotRequired[int]
    primary_destination: ExternalRtpDestinationSpec
    receiver_channel_ids: list[int]
    receiver_supports_multiple_interfaces: _extensions.NotRequired[bool]
    secondary_destination: _extensions.NotRequired[_typing.Union[ExternalRtpDestinationSpec, None]]
    session_id: int
    source_address: str


ExternalSubscriptionCommand = _typing.Union[ExternalSubscriptionCommandSubscribeExternalRtp]


class ExternalReadbackRequestCommand(_extensions.TypedDict):
    inventory: _extensions.NotRequired[JsonValue]
    kind: _typing.Literal["command"]
    specification: ExternalSubscriptionCommand


class Identity(_extensions.TypedDict):
    flow_slot: int
    interface_endpoints: list[Endpoint]
    receiver_channel: int
    session_id: int
    source_ipv4: str


class ExternalReadbackRequestIdentities(_extensions.TypedDict):
    identities: list[Identity]
    inventory: _extensions.NotRequired[JsonValue]
    kind: _typing.Literal["identities"]


ExternalReadbackRequest = _typing.Union[ExternalReadbackRequestCommand, ExternalReadbackRequestIdentities]


class ExternalReceiverFacts(_extensions.TypedDict):
    aes67_enabled: _extensions.NotRequired[_typing.Union[bool, None]]
    aes67_supported: _extensions.NotRequired[_typing.Union[bool, None]]
    encoding: _extensions.NotRequired[_typing.Union[int, None]]
    locked: _extensions.NotRequired[_typing.Union[bool, None]]
    redundancy_supported: _extensions.NotRequired[_typing.Union[bool, None]]
    sample_rate: _extensions.NotRequired[_typing.Union[int, None]]


class ExternalSubscriptionPlanRequest(_extensions.TypedDict):
    advertised_flow_slot_count: int
    clock_offset: _extensions.NotRequired[_typing.Union[int, None]]
    device_protocol: int
    flow_slot_assignments: list[int]
    message_id: _extensions.NotRequired[int]
    primary_destination: ExternalRtpDestinationSpec
    receiver: ExternalReceiverFacts
    receiver_channel_ids: list[int]
    receiver_supports_multiple_interfaces: bool
    secondary_address: _extensions.NotRequired[_typing.Union[str, None]]
    secondary_port: _extensions.NotRequired[_typing.Union[int, None]]
    session_id: int
    source_address: str
    source_direction: _extensions.NotRequired[_typing.Union[str, None]]
    source_encoding: _extensions.NotRequired[_typing.Union[str, None]]
    source_sample_rate: _extensions.NotRequired[_typing.Union[int, None]]


class FlowInventoryEvidence(_extensions.TypedDict):
    flows: list[dict[str, JsonValue]]
    page_disposition: _extensions.NotRequired[_typing.Union[str, None]]
    reported_flow_count: _extensions.NotRequired[_typing.Union[int, None]]


FlowType = _typing.Literal["unicast", "multicast"]


class FlowIdentity(_extensions.TypedDict):
    global_flow_id: _extensions.NotRequired[_typing.Union[int, None]]
    media_local_flow_id: _extensions.NotRequired[_typing.Union[int, None]]
    media_type_code: _extensions.NotRequired[_typing.Union[int, None]]


MediaMode = _typing.Literal["unknown", "native_dante", "rtp_aes67"]


class ProtocolRequirements(_extensions.TypedDict):
    cohort: _extensions.NotRequired[_typing.Union[str, None]]
    protocol_id: _extensions.NotRequired[_typing.Union[int, None]]
    protocol_version: _extensions.NotRequired[_typing.Union[str, None]]
    required_capabilities: _extensions.NotRequired[list[str]]


RedundancyConstraint = _typing.Literal["device_default", "none", "optional", "required"]


class TransmitFlowSpecification(_extensions.TypedDict):
    channel_slots: list[ChannelSlot]
    encoding_bits: _extensions.NotRequired[_typing.Union[int, None]]
    flow_type: FlowType
    frames_per_packet: _extensions.NotRequired[_typing.Union[int, None]]
    identity: _extensions.NotRequired[FlowIdentity]
    media_mode: MediaMode
    name: _extensions.NotRequired[_typing.Union[str, None]]
    primary_destination: _extensions.NotRequired[_typing.Union[Destination, None]]
    protocol: _extensions.NotRequired[ProtocolRequirements]
    raw_fields: _extensions.NotRequired[dict[str, JsonValue]]
    redundancy: _extensions.NotRequired[RedundancyConstraint]
    sample_rate_hz: _extensions.NotRequired[_typing.Union[int, None]]
    schema_version: _extensions.NotRequired[int]
    secondary_destination: _extensions.NotRequired[_typing.Union[Destination, None]]


class FlowCandidateRequest(_extensions.TypedDict):
    after: FlowInventoryEvidence
    before: FlowInventoryEvidence
    correlated_flow_id: _extensions.NotRequired[_typing.Union[int, None]]
    protocol_id: int
    requested: TransmitFlowSpecification


class FlowDifference(_extensions.TypedDict):
    effective: JsonValue
    field: str
    requested: JsonValue


class FlowComparison(_extensions.TypedDict):
    differences: list[FlowDifference]
    matches: bool
    unavailable_fields: list[str]


class FlowCreatePreflightRequest(_extensions.TypedDict):
    inventory: JsonValue
    requested_flow_id: _extensions.NotRequired[_typing.Union[int, None]]


class FlowDeviceFacts(_extensions.TypedDict):
    advertised_protocol: _extensions.NotRequired[_typing.Union[int, None]]
    capabilities: dict[str, JsonValue]
    capability_word: _extensions.NotRequired[JsonValue]
    channel_capacity: _extensions.NotRequired[JsonValue]
    channels: _extensions.NotRequired[_typing.Union[list[int], None]]
    encoding: _extensions.NotRequired[_typing.Union[int, None]]
    locked: _extensions.NotRequired[_typing.Union[bool, None]]
    managed: bool
    protocol_version: _extensions.NotRequired[_typing.Union[str, None]]
    sample_rate: _extensions.NotRequired[_typing.Union[int, None]]


class FlowCreateRequest(_extensions.TypedDict):
    device: FlowDeviceFacts
    protocol_id: _extensions.NotRequired[_typing.Union[int, None]]
    specification: TransmitFlowSpecification


class FlowDeletePreflightRequest(_extensions.TypedDict):
    capability_word: _extensions.NotRequired[_typing.Union[int, None]]
    flow_id: int
    inventory: JsonValue
    protocol_id: int


class FlowDeleteRequest(_extensions.TypedDict):
    device: FlowDeviceFacts
    flow_id: int
    protocol_id: _extensions.NotRequired[_typing.Union[int, None]]


class FlowInventoryProtocolFacts(_extensions.TypedDict):
    managed: bool
    observed: _extensions.NotRequired[_typing.Union[int, None]]
    version: _extensions.NotRequired[_typing.Union[str, None]]


FlowMutation = _typing.Literal["create", "delete"]


class FlowReadbackRequest(_extensions.TypedDict):
    protocol_id: int
    record: JsonValue


class FlowTopology(_extensions.TypedDict):
    channel_count: int
    channel_members: list[int]
    encoding: int
    flow_number: int
    flow_type: FlowType
    frames_per_packet: _extensions.NotRequired[_typing.Union[int, None]]
    may_retire_after_sample_rate_change: bool
    sample_rate_hertz: int


class FlowTopologyChangeRequest(_extensions.TypedDict):
    after: FlowInventoryEvidence
    before: FlowInventoryEvidence
    protocol_id: int
    target_flow_id: _extensions.NotRequired[_typing.Union[int, None]]


class FlowVerificationRequest(_extensions.TypedDict):
    authoring_refreshed: bool
    comparison: _extensions.NotRequired[_typing.Union[FlowComparison, None]]
    operation: FlowMutation
    record_present: bool


class InterfaceConfigurationRequestDhcp(_extensions.TypedDict):
    mode: _typing.Literal["dhcp"]


class InterfaceConfigurationRequestStatic(_extensions.TypedDict):
    dns_server: _extensions.NotRequired[_typing.Union[str, None]]
    gateway: _extensions.NotRequired[_typing.Union[str, None]]
    ip_address: str
    mode: _typing.Literal["static"]
    netmask: str


InterfaceConfigurationRequest = _typing.Union[InterfaceConfigurationRequestDhcp, InterfaceConfigurationRequestStatic]


NetworkInterface = _typing.Literal["primary", "secondary"]


class InterfaceReadbackRequest(_extensions.TypedDict):
    after: list[JsonValue]
    after_redundancy: _extensions.NotRequired[JsonValue]
    before: list[JsonValue]
    before_redundancy: _extensions.NotRequired[JsonValue]
    configuration: InterfaceConfigurationRequest
    interface: NetworkInterface


class InterfaceRedundancyObservation(_extensions.TypedDict):
    flags: _extensions.NotRequired[_typing.Union[int, None]]
    observed_at_unix: float
    previous: _extensions.NotRequired[_typing.Union[dict[str, JsonValue], None]]
    raw_record_hexadecimal: _extensions.NotRequired[_typing.Union[str, None]]
    record_protocol_identifier: _extensions.NotRequired[_typing.Union[int, None]]


class LatencyControl(_extensions.TypedDict):
    acknowledged: _extensions.NotRequired[_typing.Union[bool, None]]
    requested_milliseconds: float
    settings: _extensions.NotRequired[_typing.Union[dict[str, JsonValue], None]]


class ManagedArcCorrelationRequest(_extensions.TypedDict):
    request_packet: list[int]
    response_frame: list[int]
    wrapper_id: int


class ManagedCommandRequest(_extensions.TypedDict):
    host_mac: _extensions.NotRequired[_typing.Union[str, None]]
    message_id: int
    specification: JsonValue


class ManagedCredentialApiKey(_extensions.TypedDict):
    kind: _typing.Literal["api_key"]
    value: str


class ManagedCredentialControllerToken(_extensions.TypedDict):
    kind: _typing.Literal["controller_token"]
    value: str


ManagedCredential = _typing.Union[ManagedCredentialApiKey, ManagedCredentialControllerToken]


class ManagedOperationMonitorSignals(_extensions.TypedDict):
    kind: _typing.Literal["monitor_signals"]


class ManagedOperationIdentify(_extensions.TypedDict):
    device_id: str
    host_mac: list[int]
    kind: _typing.Literal["identify"]


class ManagedOperationArc(_extensions.TypedDict):
    device_id: str
    kind: _typing.Literal["arc"]
    packet: list[int]


class ManagedOperationSettings(_extensions.TypedDict):
    device_id: str
    kind: _typing.Literal["settings"]
    packet: list[int]
    response_opcode: int


class ManagedOperationReboot(_extensions.TypedDict):
    device_id: str
    host_mac: list[int]
    kind: _typing.Literal["reboot"]


ManagedOperation = _typing.Union[
    ManagedOperationMonitorSignals,
    ManagedOperationIdentify,
    ManagedOperationArc,
    ManagedOperationSettings,
    ManagedOperationReboot,
]


class SettingsExchange(_extensions.TypedDict):
    acknowledged: bool
    device_id: str
    packet_hex: _typing.Union[str, None]
    response_opcode: _typing.Union[int, None]
    wrapper_id: int


class ManagedSessionState(_extensions.TypedDict):
    domain_id: _typing.Union[str, None]
    expected_domain_id: _typing.Union[str, None]
    frame: list[int]
    local_ipv4: str
    notification_port: int
    operation: _typing.Union[ManagedOperation, None]
    sent: bool
    settings: _typing.Union[SettingsExchange, None]
    target: str
    targets: dict[str, int]
    wrapper_id: int


class ManagedSessionRequestBegin(_extensions.TypedDict):
    action: _typing.Literal["begin"]
    credential: str
    expected_domain_id: _extensions.NotRequired[_typing.Union[str, None]]
    local_ipv4: str
    notification_port: int
    operation: ManagedOperation
    state: _extensions.NotRequired[_typing.Union[ManagedSessionState, None]]


class ManagedSessionRequestReceive(_extensions.TypedDict):
    action: _typing.Literal["receive"]
    data: list[int]
    state: ManagedSessionState


ManagedSessionRequest = _typing.Union[ManagedSessionRequestBegin, ManagedSessionRequestReceive]


class ManagedSettingsRequest(_extensions.TypedDict):
    device_id: str
    frame: list[int]
    response_opcode: _extensions.NotRequired[_typing.Union[int, None]]
    state: _extensions.NotRequired[_typing.Union[SettingsExchange, None]]
    wrapper_id: int


class ManagedStatusRequest(_extensions.TypedDict):
    status: _extensions.NotRequired[JsonValue]
    status_message: _extensions.NotRequired[JsonValue]
    summary: _extensions.NotRequired[JsonValue]


class NetworkControlFacts(_extensions.TypedDict):
    address_available: bool
    entry: _extensions.NotRequired[JsonValue]
    interfaces: _extensions.NotRequired[JsonValue]
    managed: bool
    redundancy_supported: _extensions.NotRequired[_typing.Union[bool, None]]
    transports: _extensions.NotRequired[JsonValue]
    writable: bool


class ObservedSubscription(_extensions.TypedDict):
    managed_status: _extensions.NotRequired[_typing.Union[str, None]]
    number: _extensions.NotRequired[_typing.Union[int, None]]
    receiver_status_code: _extensions.NotRequired[_typing.Union[int, None]]
    status_code: _extensions.NotRequired[_typing.Union[int, None]]
    tx_channel: _extensions.NotRequired[_typing.Union[str, None]]
    tx_device: _extensions.NotRequired[_typing.Union[str, None]]


class PanelProfileRequest(_extensions.TypedDict):
    address_available: bool
    locked: _extensions.NotRequired[_typing.Union[bool, None]]
    managed: bool
    online: _extensions.NotRequired[_typing.Union[bool, None]]
    plugins: list[str]
    read_allowed: bool
    video_transmission_supported: bool
    virtual_panel_supported: bool
    write_allowed: bool


class PanelPlanRequest(_extensions.TypedDict):
    category: str
    confirm_clear: bool
    fresh_values: dict[str, JsonValue]
    profile: PanelProfileRequest
    requested: JsonValue


class PanelPresentationRequest(_extensions.TypedDict):
    fresh_values: dict[str, JsonValue]
    profile: PanelProfileRequest
    values: dict[str, JsonValue]


class PanelReadbackRequest(_extensions.TypedDict):
    expected: JsonValue
    observed: JsonValue


PerformanceCompletionKind = _typing.Literal["configuration", "storage"]


class PerformanceCompletionRequest(_extensions.TypedDict):
    acknowledgement: _extensions.NotRequired[_typing.Union[list[int], None]]
    kind: PerformanceCompletionKind
    readback: _extensions.NotRequired[_typing.Union[list[int], None]]
    requested: _extensions.NotRequired[dict[str, int]]


class PerformanceFacts(_extensions.TypedDict):
    managed: bool
    platform_software_version: _extensions.NotRequired[_typing.Union[str, None]]
    property_ids: _extensions.NotRequired[_typing.Union[list[JsonValue], None]]
    protocol_id: _extensions.NotRequired[_typing.Union[int, None]]


class PerformanceSnapshotFacts(_extensions.TypedDict):
    property_ids: list[JsonValue]
    values: dict[str, JsonValue]


class Receiver(_extensions.TypedDict):
    media_type_code: _extensions.NotRequired[_typing.Union[int, None]]
    number: int


class ReceiverCapabilityRequest(_extensions.TypedDict):
    authority: Authority
    channels: list[Evidence]


class ReceiverSubscription(_extensions.TypedDict):
    receiver_channel_name: str
    receiver_channel_number: int
    transmitter_channel_name: str
    transmitter_device_name: str


class SampleRateStatus(_extensions.TypedDict):
    available_values: list[int]
    current_value: int


class SapSessionAnnouncement(_extensions.TypedDict):
    message_hash: int
    raw_sdp: str


class SapTransitionRequest(_extensions.TypedDict):
    current: SapSessionAnnouncement
    delete: bool
    previous: _extensions.NotRequired[_typing.Union[SapSessionAnnouncement, None]]


class SettingsCapabilityFacts(_extensions.TypedDict):
    multicast_prefix: _extensions.NotRequired[_typing.Union[str, None]]
    properties: _extensions.NotRequired[_typing.Union[list[JsonValue], None]]


class SubscriptionReadbackRequest(_extensions.TypedDict):
    channels: list[int]
    expected: list[ExpectedSubscription]
    subscriptions: list[ObservedSubscription]


class SubscriptionReconciliationRequest(_extensions.TypedDict):
    channels: list[Receiver]
    expected: list[ExpectedSubscription]
    managed: bool
    protocol_id: int
    subscriptions: list[ObservedSubscription]


class TopologySnapshot(_extensions.TypedDict):
    capacity: ChannelCapacity
    flow_protocol_identifier: int
    receiver_subscriptions: list[ReceiverSubscription]
    transmitter_flows: list[FlowTopology]


class TopologyImpactRequest(_extensions.TypedDict):
    snapshot: TopologySnapshot
    target_capacity: _extensions.NotRequired[_typing.Union[ChannelCapacity, None]]


class TopologyReadbackRequest(_extensions.TypedDict):
    after: TopologySnapshot
    before: TopologySnapshot
    target_capacity: _extensions.NotRequired[_typing.Union[ChannelCapacity, None]]
    target_sample_rate_hertz: int


class VirtualDeviceAdvertisement(_extensions.TypedDict):
    address: str
    arc_port: int
    configured_latency_ns: int
    encoding: int
    manufacturer: str
    model: str
    name: str
    sample_rate: int
    supported_encodings: list[int]
    tx_channels: list[str]
