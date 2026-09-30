# Generated from Rust schemas by scripts/generate_core_types.py. Do not edit.
import typing as _typing
import typing_extensions as _extensions


JsonValue = _typing.Union[None, bool, int, float, str, list["JsonValue"], dict[str, "JsonValue"]]


Action = _typing.Literal["clear", "set"]


class AnalogLevelPlanUnsupported(_extensions.TypedDict):
    action: _typing.Literal["unsupported"]
    reason: str


class AnalogLevelPlanAmbiguous(_extensions.TypedDict):
    action: _typing.Literal["ambiguous"]
    reason: str


class AnalogLevelPlanUnchanged(_extensions.TypedDict):
    action: _typing.Literal["unchanged"]
    before: int
    direction: str
    reason: None


class AnalogLevelPlanChange(_extensions.TypedDict):
    action: _typing.Literal["change"]
    before: int
    direction: str
    reason: None


AnalogLevelPlan = _typing.Union[
    AnalogLevelPlanUnsupported, AnalogLevelPlanAmbiguous, AnalogLevelPlanUnchanged, AnalogLevelPlanChange
]


class ArcProtocol(_extensions.TypedDict):
    channel_name_probe_protocol_id: int
    flow_query_protocol_ids: list[int]
    modern_channel_inventory: bool
    protocol_id: int
    subscription_batch_limit: int
    subscription_page: bool


AudioReadbackState = _typing.Literal["confirmed", "unverified", "unavailable"]


class AudioReadbackResult(_extensions.TypedDict):
    current_value: _typing.Union[int, None]
    effective_state_confirmed: bool
    requested_value: int
    state: AudioReadbackState


class Availability(_extensions.TypedDict):
    read_only: _typing.Union[bool, None]
    readable: bool
    reasons: list[str]
    supported: _typing.Union[bool, None]
    writable: bool
    write_permitted: bool


class ExpectedSubscription(_extensions.TypedDict):
    number: int
    source: _typing.Union[list[str], None]


class Batch(_extensions.TypedDict):
    action: Action
    expected: list[ExpectedSubscription]


class Capability(_extensions.TypedDict):
    conflict: bool
    supported: _typing.Union[bool, None]


class ChannelAudioPublication(_extensions.TypedDict):
    channel_metadata: list[int]
    pcm_property: str


class ChannelCapacity(_extensions.TypedDict):
    receive_channel_count: int
    sample_rate_hertz: int
    transmit_channel_count: int


class ChannelReadback(_extensions.TypedDict):
    connection_state: str
    matched: bool
    number: int
    settled: bool
    source: _typing.Union[list[str], None]


class ChannelSlot(_extensions.TypedDict):
    slot: int
    transmitter_channel: int


class ClockBasePort(_extensions.TypedDict):
    role: _typing.Union[str, None]
    state: _typing.Union[str, None]
    state_code: int
    unknown: int


class ClockControlAvailability(_extensions.TypedDict):
    aggregate_ptpv1_unicast_delay_requests: bool
    clock_source: bool
    follower_only: bool
    global_unicast_delay_requests: bool
    multicast_dscp: bool
    ports: bool
    preferred_leader: bool
    preferred_protocol: bool
    priority_mapping: bool
    ptpv2_clock_class: bool
    ptpv2_domain: bool
    ptpv2_priority1: bool
    ptpv2_priority2: bool
    subdomain: bool


class ClockExtensionDescriptor(_extensions.TypedDict):
    base_vector_metadata: int
    base_vector_offset: int
    extent: int
    global_block_length: _typing.Union[int, None]
    global_block_offset: _typing.Union[int, None]
    unresolved_metadata: int


class ClockGlobalBlock(_extensions.TypedDict):
    extended_port_count: int
    extended_port_offset: int
    extended_port_stride: int
    identity_validity: int
    length: int
    offset: int
    raw_block: list[int]


class Observation(_extensions.TypedDict):
    clock_state_evidence: JsonValue
    display_epoch: int
    epoch: int
    evidence: JsonValue
    latency_microseconds: _typing.Union[int, None]
    observed_at: str
    observed_monotonic: float
    raw: int
    raw_record: list[int]
    record_type: int
    sample_rate_hertz: _typing.Union[int, None]
    sequence: _typing.Union[int, None]
    sequence_gap: int
    source: str
    timestamp_provenance: str
    value: _typing.Union[float, None]
    value_unit: str


class Series(_extensions.TypedDict):
    baseline: _typing.Union[int, None]
    current: _typing.Union[Observation, None]
    delta: _typing.Union[int, None]
    display_epoch: int
    fresh: bool
    histogram: JsonValue
    history: list[Observation]
    increase_since_baseline: _typing.Union[int, None]
    statistics: JsonValue


class Diagnostic(_extensions.TypedDict):
    evidence: JsonValue
    kind: str


class ClockVariation(_extensions.TypedDict):
    active: bool
    deviation_ppb: _typing.Union[float, None]
    last_epoch: _typing.Union[int, None]
    last_observed_monotonic: _typing.Union[float, None]
    observable: bool
    recovered: bool
    recovery_since: _typing.Union[float, None]
    threshold_ppb: int
    window_size: int


class ClockObservations(_extensions.TypedDict):
    conmon: Series
    diagnostics: list[Diagnostic]
    heartbeat: Series
    variation: dict[str, ClockVariation]
    warning_enabled: bool


class ClockPlan(_extensions.TypedDict):
    before: dict[str, JsonValue]
    changes: dict[str, JsonValue]
    control: dict[str, JsonValue]
    requested: dict[str, JsonValue]


class ClockSourceChoice(_extensions.TypedDict):
    code: int
    label: str


class ClockSources(_extensions.TypedDict):
    choices: list[ClockSourceChoice]
    current: _typing.Union[str, None]


class ClockSubdomainPresentation(_extensions.TypedDict):
    label: str
    text: _typing.Union[str, None]


class ClockVector(_extensions.TypedDict):
    count: int
    first_record: int
    offset: int
    stride: int
    unknown: int


class CodecFormat(_extensions.TypedDict):
    codec_type: int
    level: int
    profile: int


class MulticastFlowCreation2809(_extensions.TypedDict):
    channels: list[int]
    global_flow_id: int
    media_local_flow_id: int
    media_type_code: int


class CommandReceipt(_extensions.TypedDict):
    accepted: _extensions.NotRequired[bool]
    allocation: _extensions.NotRequired[_typing.Union[MulticastFlowCreation2809, None]]
    parseable: bool
    raw_response_hexadecimal: str
    received: bool
    result_code: _extensions.NotRequired[int]


class ReceiverPath(_extensions.TypedDict):
    attribution_epoch: int
    attribution_reason: _typing.Union[str, None]
    attribution_status: str
    audio_receiver_flow_id: _typing.Union[int, None]
    evidence: JsonValue
    global_flow_id: _typing.Union[int, None]
    late_packets: Series
    latency: Series
    media_type: _typing.Union[str, None]
    network_interface_index: _typing.Union[int, None]
    telemetry_index: int


class ConnectionHealthUpdate(_extensions.TypedDict):
    complete: bool
    device_extended_unique_identifier: str
    diagnostics: list[Diagnostic]
    fresh: bool
    paths: list[ReceiverPath]
    retention_limit: int


class ControllerApiRoutes(_extensions.TypedDict):
    endpoints: str
    login: str


class ControllerEndpoints(_extensions.TypedDict):
    device_port: int
    graphql_url: str
    service_port: int


class ControllerLogin(_extensions.TypedDict):
    auth_token: str
    endpoints: ControllerEndpoints


ControllerLoginResult = _typing.Union[ControllerLogin, None]


DanteRedundancyMode = _typing.Literal["switched", "redundant", "split_redundant"]


class Destination(_extensions.TypedDict):
    address: str
    interface: _typing.Union[str, None]
    port: int


class DiagnosticAudioCapabilities(_extensions.TypedDict):
    channel_capacities: list[ChannelCapacity]
    current_sample_rate_hertz: _typing.Union[int, None]
    default_sample_rate_hertz: _typing.Union[int, None]
    license_signature_length_bytes: _typing.Union[int, None]
    licensed_receive_channel_count: _typing.Union[int, None]
    licensed_redundancy_enabled: _typing.Union[bool, None]
    licensed_transmit_channel_count: _typing.Union[int, None]


class Endpoint(_extensions.TypedDict):
    ipv4_address: str
    udp_port: int


ExportKind = _typing.Literal["diagnostic_logs", "capability_partition"]


class ExportResult(_extensions.TypedDict):
    echoed_tag_hexadecimal: str
    encoded_payload_hexadecimal: str
    fragment_count: int
    kind: ExportKind
    record_protocol_identifier: int
    selector_value: int


class ExportProgress(_extensions.TypedDict):
    matched: bool
    result: _typing.Union[ExportResult, None]


class ExtendedClockPort(_extensions.TypedDict):
    announce_interval: _typing.Union[int, None]
    announce_interval_raw: _typing.Union[int, None]
    delay_mechanism: _typing.Union[int, None]
    delay_request_interval: _typing.Union[int, None]
    delay_request_interval_raw: _typing.Union[int, None]
    follower_only: _typing.Union[bool, None]
    network_interface_index: _typing.Union[int, None]
    peer_delay_interval: _typing.Union[int, None]
    peer_delay_interval_raw: _typing.Union[int, None]
    port_id: _typing.Union[int, None]
    raw_record: list[int]
    record_index: int
    sync_interval: _typing.Union[int, None]
    sync_interval_raw: _typing.Union[int, None]
    ttl: _typing.Union[int, None]
    validity: int


class Identity(_extensions.TypedDict):
    flow_slot: int
    interface_endpoints: list[Endpoint]
    receiver_channel: int
    session_id: int
    source_ipv4: str


class ExternalSubscriptionReadback(_extensions.TypedDict):
    arc_effective_state_confirmed: _typing.Union[bool, None]
    observed_effective_identities: list[Identity]
    requested_effective_identities: list[Identity]
    sdp_correlation_confirmed: _typing.Union[bool, None]


class FlowAuthoringProfile(_extensions.TypedDict):
    family: str
    identifier_max: int
    identity_field: str
    media_modes: list[str]
    opcode: int
    supports_flow_options: bool


class FlowAuthoringCapabilities(_extensions.TypedDict):
    receiver_flow_query_family: str
    transmit_flow_authoring: FlowAuthoringProfile


class FlowDifference(_extensions.TypedDict):
    effective: JsonValue
    field: str
    requested: JsonValue


class FlowComparison(_extensions.TypedDict):
    differences: list[FlowDifference]
    matches: bool
    unavailable_fields: list[str]


class FlowCandidate(_extensions.TypedDict):
    comparison: _typing.Union[FlowComparison, None]
    flow_id: _typing.Union[int, None]
    record: _typing.Union[dict[str, JsonValue], None]


class FlowCommandPlan(_extensions.TypedDict):
    authoring_family: _typing.Union[str, None]
    command: _typing.Union[dict[str, JsonValue], None]
    reasons: list[str]
    serializer_cohort: _typing.Union[str, None]
    wire_authored_fields: list[str]


FlowPreflightOutcome = _typing.Literal["ready", "unavailable", "rejected"]


class FlowCreatePreflight(_extensions.TypedDict):
    reason: _typing.Union[str, None]
    state: FlowPreflightOutcome


FlowType = _typing.Literal["unicast", "multicast"]


class FlowIdentity(_extensions.TypedDict):
    global_flow_id: _typing.Union[int, None]
    media_local_flow_id: _typing.Union[int, None]
    media_type_code: _typing.Union[int, None]


MediaMode = _typing.Literal["unknown", "native_dante", "rtp_aes67"]


class ProtocolRequirements(_extensions.TypedDict):
    cohort: _typing.Union[str, None]
    protocol_id: _typing.Union[int, None]
    protocol_version: _typing.Union[str, None]
    required_capabilities: list[str]


RedundancyConstraint = _typing.Literal["device_default", "none", "optional", "required"]


class ObservedTransmitFlowSpecification(_extensions.TypedDict):
    channel_slots: list[ChannelSlot]
    encoding_bits: _typing.Union[int, None]
    flow_type: FlowType
    frames_per_packet: _typing.Union[int, None]
    identity: FlowIdentity
    media_mode: MediaMode
    name: _typing.Union[str, None]
    observed_fields: list[str]
    primary_destination: _typing.Union[Destination, None]
    protocol: ProtocolRequirements
    raw_fields: dict[str, JsonValue]
    redundancy: RedundancyConstraint
    sample_rate_hz: _typing.Union[int, None]
    schema_version: int
    secondary_destination: _typing.Union[Destination, None]


class FlowDeletePreflight(_extensions.TypedDict):
    command: _typing.Union[dict[str, JsonValue], None]
    reason: _typing.Union[str, None]
    specification: _typing.Union[ObservedTransmitFlowSpecification, None]
    state: FlowPreflightOutcome


class FlowMembershipLoss(_extensions.TypedDict):
    flow_number: int
    flow_type: FlowType
    removed_channel_members: list[int]
    retained_channel_members: list[int]


class FlowTopology(_extensions.TypedDict):
    channel_count: int
    channel_members: list[int]
    encoding: int
    flow_number: int
    flow_type: FlowType
    frames_per_packet: _typing.Union[int, None]
    may_retire_after_sample_rate_change: bool
    sample_rate_hertz: int


class FlowTopologyChange(_extensions.TypedDict):
    after: list[dict[str, JsonValue]]
    before: list[dict[str, JsonValue]]


FlowVerificationOutcome = _typing.Literal["confirmed", "contradiction", "partially_observed", "not_yet_visible"]


class FlowVerification(_extensions.TypedDict):
    authoring_refresh_missing: bool
    outcome: FlowVerificationOutcome
    unavailable_fields: list[str]


class GainLevel(_extensions.TypedDict):
    label: str
    value: int


class GainDirection(_extensions.TypedDict):
    channel_type: str
    levels: list[GainLevel]


class GainMetadata(_extensions.TypedDict):
    input: GainDirection
    output: GainDirection


class InterfaceConfigurationDynamic(_extensions.TypedDict):
    mode: _typing.Literal["dynamic"]


class InterfaceConfigurationStatic(_extensions.TypedDict):
    dns_server: str
    gateway: str
    ip_address: str
    mode: _typing.Literal["static"]
    netmask: str


InterfaceConfiguration = _typing.Union[InterfaceConfigurationDynamic, InterfaceConfigurationStatic]


class InterfaceModeEvidenceKnown(_extensions.TypedDict):
    flag_mask: int
    flag_set: bool
    mode: _typing.Union[DanteRedundancyMode, None]
    raw_flags: int
    status: _typing.Literal["known"]


class InterfaceModeEvidenceUnknownRaw(_extensions.TypedDict):
    known_mask: int
    mode: _typing.Union[DanteRedundancyMode, None]
    raw_flags: int
    status: _typing.Literal["unknown_raw"]


InterfaceModeEvidence = _typing.Union[InterfaceModeEvidenceKnown, InterfaceModeEvidenceUnknownRaw]


class InterfaceRedundancySource(_extensions.TypedDict):
    cohort: str
    kind: str
    observed_at_unix: float
    opcode: int
    record_protocol_identifier: _typing.Union[int, None]


class InterfaceRedundancyState(_extensions.TypedDict):
    available_modes: JsonValue
    available_modes_fresh: bool
    available_modes_source: _typing.Union[str, None]
    configured: _typing.Union[DanteRedundancyMode, None]
    configured_mode_evidence: InterfaceModeEvidence
    current: _typing.Union[DanteRedundancyMode, None]
    current_mode_evidence: InterfaceModeEvidence
    raw_record_hexadecimal: _typing.Union[str, None]
    reboot_required: bool
    state_fresh: bool
    state_source: InterfaceRedundancySource
    supported: list[JsonValue]


InterfaceRedundancyResult = _typing.Union[InterfaceRedundancyState, dict[str, JsonValue]]


InventoryCompleteness = _typing.Literal["unknown", "partial", "complete"]


class InventoryStateVariant0(_extensions.TypedDict):
    inventory: None
    next_command: dict[str, JsonValue]
    partial_inventory: dict[str, JsonValue]


class InventoryStateVariant1(_extensions.TypedDict):
    inventory: None
    next_command: dict[str, JsonValue]


class InventoryStateVariant2(_extensions.TypedDict):
    inventory: dict[str, JsonValue]
    next_command: None


InventoryState = _typing.Union[InventoryStateVariant0, InventoryStateVariant1, InventoryStateVariant2]


LatencyOutcome = _typing.Literal["rejected", "unavailable", "confirmed", "unverified"]


class LatencyCompletion(_extensions.TypedDict):
    configured_latency_ms: _typing.Union[float, None]
    configured_latency_ns: _typing.Union[int, None]
    effective_state_confirmed: bool
    requested_latency_ns: int
    state: LatencyOutcome


class LatencyControls(_extensions.TypedDict):
    active_latency: _extensions.NotRequired[_typing.Union[float, None]]
    configured_latency: _extensions.NotRequired[_typing.Union[float, None]]
    default_latency: _extensions.NotRequired[_typing.Union[float, None]]
    latency: _extensions.NotRequired[_typing.Union[float, None]]
    max_latency: _extensions.NotRequired[_typing.Union[float, None]]
    min_latency: _extensions.NotRequired[_typing.Union[float, None]]


LatencyOptionsSource = _typing.Literal[
    "device_reports_no_usable_range", "controller_fixed_set_filtered_by_reported_range"
]


class LatencyState(_extensions.TypedDict):
    active_latency_is_standard_choice: _extensions.NotRequired[_typing.Union[bool, None]]
    active_latency_ms: _extensions.NotRequired[_typing.Union[float, None]]
    active_latency_ns: _extensions.NotRequired[_typing.Union[int, None]]
    active_latency_within_reported_range: _extensions.NotRequired[_typing.Union[bool, None]]
    configured_latency_is_standard_choice: _extensions.NotRequired[_typing.Union[bool, None]]
    configured_latency_ms: _extensions.NotRequired[_typing.Union[float, None]]
    configured_latency_ns: _extensions.NotRequired[_typing.Union[int, None]]
    configured_latency_within_reported_range: _extensions.NotRequired[_typing.Union[bool, None]]
    default_latency_ms: _extensions.NotRequired[_typing.Union[float, None]]
    default_latency_ns: _extensions.NotRequired[_typing.Union[int, None]]
    latency_options_ms: _extensions.NotRequired[_typing.Union[list[float], None]]
    latency_options_ns: _extensions.NotRequired[_typing.Union[list[int], None]]
    latency_options_source: _extensions.NotRequired[_typing.Union[LatencyOptionsSource, None]]
    max_latency_ms: _extensions.NotRequired[_typing.Union[float, None]]
    max_latency_ns: _extensions.NotRequired[_typing.Union[int, None]]
    min_latency_ms: _extensions.NotRequired[_typing.Union[float, None]]
    min_latency_ns: _extensions.NotRequired[_typing.Union[int, None]]


class LatencyConfiguration(_extensions.TypedDict):
    controls: LatencyControls
    state: LatencyState


class ManagedArcResponse(_extensions.TypedDict):
    alignment_bytes_hex: str
    opcode: int
    packet_hex: str
    protocol_id: int
    result_code: int
    transaction_id: int
    wrapper_id: int


Target = _typing.Literal["arc", "control", "settings"]


class ManagedCommand(_extensions.TypedDict):
    packet: list[int]
    response_opcode: _typing.Union[int, None]
    transport: Target


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


class SignalPresenceRecord(_extensions.TypedDict):
    extension_length: int
    level_vector_offset: int
    padding_length: int
    payload_length: int
    record_length: int
    rx_count: int
    rx_first_channel_index: int
    rx_levels: list[int]
    sequence: int
    tx_count: int
    tx_first_channel_index: int
    tx_levels: list[int]


class SignalPresencePublication(_extensions.TypedDict):
    device_id: str
    records: list[SignalPresenceRecord]


class ManagedSessionStep(_extensions.TypedDict):
    complete: bool
    outgoing: list[list[int]]
    packet_hex: _typing.Union[str, None]
    receive_bytes: int
    signal_presence: _typing.Union[SignalPresencePublication, None]
    state: ManagedSessionState


class ManagedSettingsResult(_extensions.TypedDict):
    complete: bool
    state: SettingsExchange


SubscriptionTransport = _typing.Literal["unicast", "multicast"]


class ManagedSubscriptionStatus(_extensions.TypedDict):
    detail: _typing.Union[str, None]
    label: _typing.Union[str, None]
    settled: bool
    severity: str
    state: str
    status: _typing.Union[str, None]
    transport: _typing.Union[SubscriptionTransport, None]


class MeteringValue(_extensions.TypedDict):
    dbfs: _typing.Union[float, None]
    display_state: str
    raw: int
    source: str
    state: str


class MeteringScales(_extensions.TypedDict):
    detailed: list[MeteringValue]
    signal_presence: list[MeteringValue]


class NetworkControlState(_extensions.TypedDict):
    configuration_modes: list[str]
    inventory_completeness: InventoryCompleteness
    reported_count: _typing.Union[int, None]
    transport_available: bool


class PacketPerformance(_extensions.TypedDict):
    frames_per_packet: int
    latency_microseconds: int


class PanelBandwidth(_extensions.TypedDict):
    disable: JsonValue
    enable: JsonValue
    maximum: int
    minimum: int


class PanelField(_extensions.TypedDict):
    key: str
    label: str


class PanelVariant(_extensions.TypedDict):
    fields: dict[str, PanelField]
    requested: JsonValue


class PanelEditor(_extensions.TypedDict):
    bandwidth: _typing.Union[PanelBandwidth, None]
    custom_name_limit: _typing.Union[int, None]
    custom_name_source: _typing.Union[int, None]
    details: dict[str, str]
    initial: JsonValue
    initial_fields: dict[str, PanelField]
    reason: _typing.Union[str, None]
    variants: list[PanelVariant]


PanelFamily = _typing.Literal["bluetooth", "dante_av"]


PanelPlanAction = _typing.Literal["unavailable", "unsupported", "unchanged", "change"]


class PanelRequestBluetoothQuery(_extensions.TypedDict):
    operation: _typing.Literal["bluetooth_query"]
    selector: int


class PanelRequestBluetoothIdentification(_extensions.TypedDict):
    custom_name: str
    name_source: int
    operation: _typing.Literal["bluetooth_identification"]


class PanelRequestBluetoothDiscovery(_extensions.TypedDict):
    discoverable: bool
    operation: _typing.Literal["bluetooth_discovery"]


class PanelRequestBluetoothClearPairing(_extensions.TypedDict):
    confirmed: bool
    operation: _typing.Literal["bluetooth_clear_pairing"]


class PanelRequestVideoQuery(_extensions.TypedDict):
    operation: _typing.Literal["video_query"]
    selector: int


class VideoFormat(_extensions.TypedDict):
    bit_depth: int
    color_space: int
    resolution: int


class SelectionMode(_extensions.TypedDict):
    manual_bit_depth: bool
    manual_color_space: bool
    manual_resolution: bool


class PanelRequestVideoFormat(_extensions.TypedDict):
    format: VideoFormat
    operation: _typing.Literal["video_format"]
    selection: SelectionMode


class PanelRequestCodecFormat(_extensions.TypedDict):
    format: CodecFormat
    operation: _typing.Literal["codec_format"]


class SerialSettings(_extensions.TypedDict):
    baud_rate: int
    data_bits: int
    hardware_flow_control: int
    parity: int
    software_flow_control: int
    stop_bits: int


class PanelRequestSerial(_extensions.TypedDict):
    operation: _typing.Literal["serial"]
    settings: SerialSettings


class PanelRequestBandwidth(_extensions.TypedDict):
    enabled: bool
    operation: _typing.Literal["bandwidth"]
    target: int


class PanelRequestHdcp(_extensions.TypedDict):
    mode: int
    operation: _typing.Literal["hdcp"]


class PanelRequestVideoViscaQuery(_extensions.TypedDict):
    operation: _typing.Literal["video_visca_query"]


class PanelRequestVideoViscaFormat(_extensions.TypedDict):
    format: VideoFormat
    operation: _typing.Literal["video_visca_format"]
    selection: SelectionMode


PanelRequest = _typing.Union[
    PanelRequestBluetoothQuery,
    PanelRequestBluetoothIdentification,
    PanelRequestBluetoothDiscovery,
    PanelRequestBluetoothClearPairing,
    PanelRequestVideoQuery,
    PanelRequestVideoFormat,
    PanelRequestCodecFormat,
    PanelRequestSerial,
    PanelRequestBandwidth,
    PanelRequestHdcp,
    PanelRequestVideoViscaQuery,
    PanelRequestVideoViscaFormat,
]


class PanelPlan(_extensions.TypedDict):
    action: PanelPlanAction
    before: JsonValue
    category: str
    expected: JsonValue
    reason: _typing.Union[str, None]
    requested: JsonValue
    requests: list[PanelRequest]


class PanelPresentation(_extensions.TypedDict):
    editors: dict[str, PanelEditor]
    summary: dict[str, str]


class PanelQuery(_extensions.TypedDict):
    category: str
    prerequisites: list[str]
    request: PanelRequest


PanelSelection = _typing.Literal["advertised", "platform_default"]


class PanelProfile(_extensions.TypedDict):
    family: _typing.Union[PanelFamily, None]
    queries: list[PanelQuery]
    read_unavailable_reason: _typing.Union[str, None]
    selection: _typing.Union[PanelSelection, None]
    unrecognized_panels: list[_typing.Union[str, None]]
    write_unavailable_reason: _typing.Union[str, None]


class PerformanceAvailability(_extensions.TypedDict):
    readable: bool
    reasons: list[str]
    supported: bool
    writable: bool


class PerformanceCapabilities(_extensions.TypedDict):
    operations: dict[str, PerformanceAvailability]
    platform_software_version: _typing.Union[list[int], None]
    supported_property_ids: _typing.Union[list[int], None]


PerformanceReadbackOutcome = _typing.Literal["no_response", "unparseable", "matched", "mismatch", "incomplete"]


PerformanceCompletionState = _typing.Literal[
    "rejected", "confirmed", "contradicted", "request_acknowledged", "unverified"
]


class PerformanceCompletion(_extensions.TypedDict):
    effective_properties: dict[str, int]
    effective_state_confirmation: _typing.Union[bool, None]
    message: str
    mismatched_properties: dict[str, int]
    missing_property_ids: list[int]
    observed_properties: dict[str, int]
    persistence_confirmation: _typing.Union[bool, None]
    readback_outcome: _typing.Union[PerformanceReadbackOutcome, None]
    state: PerformanceCompletionState


class PerformanceSnapshot(_extensions.TypedDict):
    receive_flow_default_slots: _extensions.NotRequired[_typing.Union[int, None]]
    receive_flow_performance: _extensions.NotRequired[_typing.Union[PacketPerformance, None]]
    transmit_flow_performance: _extensions.NotRequired[_typing.Union[PacketPerformance, None]]
    unicast_performance: _extensions.NotRequired[_typing.Union[PacketPerformance, None]]


class PtpClockPortRecord(_extensions.TypedDict):
    interface_flags: _typing.Union[int, None]
    interface_record: _typing.Union[list[int], None]
    link_down: _typing.Union[bool, None]
    network_interface_index: _typing.Union[int, None]
    ptp_version: int
    record_flags: int
    record_format_code: int
    record_number: int
    reserved_byte: int
    role: _typing.Union[str, None]
    state: _typing.Union[str, None]
    state_code: int
    status_flags: int
    transport_path: _typing.Union[str, None]
    transport_path_code: int
    unicast_delay_requests: _typing.Union[bool, None]
    unknown_word: int
    user_disabled: _typing.Union[bool, None]


class PtpClockStatus(_extensions.TypedDict):
    aggregate_ptpv1_unicast_delay_requests: _typing.Union[bool, None]
    base_ports: list[ClockBasePort]
    clock_capabilities: _typing.Union[int, None]
    clock_frequency_offset_parts_per_billion: int
    clock_port_records: _typing.Union[list[PtpClockPortRecord], None]
    clock_port_state_code: _typing.Union[int, None]
    clock_role: _typing.Union[str, None]
    clock_source: _typing.Union[str, None]
    clock_source_code: int
    clock_state: _typing.Union[str, None]
    clock_state_code: int
    clock_subdomain: _typing.Union[list[int], None]
    congestion_delay_microseconds: int
    descriptor_bytes: list[int]
    domain_raw: _typing.Union[int, None]
    extended_capabilities: _typing.Union[int, None]
    extended_port_descriptor: _typing.Union[list[int], None]
    extended_ports: list[ExtendedClockPort]
    extended_ptpv2_domain: _typing.Union[int, None]
    extended_validity: _typing.Union[int, None]
    extended_value_validity_word: _typing.Union[int, None]
    extended_value_word: _typing.Union[int, None]
    extension_descriptor: _typing.Union[ClockExtensionDescriptor, None]
    extension_flags: _typing.Union[int, None]
    extension_offset: _typing.Union[int, None]
    extension_unknown_byte: _typing.Union[int, None]
    extension_unknown_word: _typing.Union[int, None]
    follower_only: _typing.Union[bool, None]
    global_block: _typing.Union[ClockGlobalBlock, None]
    global_unicast_delay_requests: _typing.Union[bool, None]
    maximum_drift_parts_per_billion: _typing.Union[int, None]
    multicast_dscp: _typing.Union[int, None]
    mute_flags: _typing.Union[int, None]
    mute_reasons: list[str]
    mute_state: _typing.Union[str, None]
    port_vector: _typing.Union[ClockVector, None]
    preferred_leader: _typing.Union[bool, None]
    preferred_leader_locked: _typing.Union[bool, None]
    preferred_protocol: _typing.Union[int, None]
    priority_mapping: _typing.Union[int, None]
    ptpv1_device_uuid: _typing.Union[list[int], None]
    ptpv1_grandmaster_uuid: _typing.Union[list[int], None]
    ptpv1_master_uuid: _typing.Union[list[int], None]
    ptpv2_clock_class: _typing.Union[int, None]
    ptpv2_device_identity: _typing.Union[str, None]
    ptpv2_domain: _typing.Union[int, None]
    ptpv2_grandmaster_identity: _typing.Union[str, None]
    ptpv2_master_identity: _typing.Union[str, None]
    ptpv2_priority1: _typing.Union[int, None]
    ptpv2_priority2: _typing.Union[int, None]
    raw_record: list[int]
    record_revision: int
    servo_state: _typing.Union[str, None]
    servo_state_code: int
    status_supported: bool
    stratum: int
    synchronization: str
    uuid_reserved: list[int]
    word_clock_state: _typing.Union[str, None]
    word_clock_state_code: _typing.Union[int, None]


class ReceiverCapabilities(_extensions.TypedDict):
    channels: list[Capability]
    support: str


class ReceiverSubscription(_extensions.TypedDict):
    receiver_channel_name: str
    receiver_channel_number: int
    transmitter_channel_name: str
    transmitter_device_name: str


RedundancyConfirmation = _typing.Literal["configuration_unconfirmed", "network_changed", "verified"]


class RedundancyControl(_extensions.TypedDict):
    configuration_matched: bool
    readback: _typing.Union[RedundancyConfirmation, None]
    reasons: list[str]
    serializer_cohort: _typing.Union[str, None]
    switch_configuration_choice: _typing.Union[int, None]


class SampleRateStatus(_extensions.TypedDict):
    available_values: list[int]
    current_value: int


class SapAnnouncement(_extensions.TypedDict):
    authentication_data_hexadecimal: str
    authentication_length_words: int
    content_type: str
    delete: bool
    message_hash: int
    origin_address: str
    raw_sdp: str
    reserved: bool
    version: int


SapTransition = _typing.Literal["added", "refreshed", "replaced", "deleted"]


SapTransitionResult = _typing.Union[SapTransition, None]


class SdpConnection(_extensions.TypedDict):
    address: str
    address_count: _typing.Union[int, None]
    address_type: str
    network_type: str
    raw_value: str
    time_to_live: _typing.Union[int, None]


class SdpMediaDescription(_extensions.TypedDict):
    attributes: list[str]
    connections: list[SdpConnection]
    information: _typing.Union[str, None]
    media_type: str
    payload_types: list[int]
    port: _typing.Union[int, None]
    port_count: _typing.Union[int, None]
    protocol: str
    raw_value: str


class SdpRoutableAudio(_extensions.TypedDict):
    channel_count: int
    clock_offset: _typing.Union[int, None]
    dante_origin: bool
    destination_port: int
    direction: _typing.Union[str, None]
    encoding: str
    media_clock: _typing.Union[str, None]
    media_title: _typing.Union[str, None]
    packet_time_microseconds: _typing.Union[int, None]
    payload_type: int
    primary_destination_address: str
    ptp_domain_token: _typing.Union[str, None]
    ptp_reference: _typing.Union[str, None]
    sample_rate: int
    secondary_destination_address: _typing.Union[str, None]


class SdpRtpMap(_extensions.TypedDict):
    channels: int
    encoding: str
    payload_type: int
    raw_value: str
    sample_rate: int


class SdpDocument(_extensions.TypedDict):
    media_descriptions: list[SdpMediaDescription]
    origin_address: str
    origin_address_type: str
    origin_network_type: str
    origin_username: str
    raw_sdp: str
    routability_errors: list[str]
    routable: bool
    routable_audio: _typing.Union[SdpRoutableAudio, None]
    rtp_maps: list[SdpRtpMap]
    session_attributes: list[str]
    session_connections: list[SdpConnection]
    session_id: int
    session_information: _typing.Union[str, None]
    session_name: str
    session_version: int
    start_time: int
    stop_time: int
    unknown_lines: list[str]
    version: int


class ServiceAdvertisement(_extensions.TypedDict):
    address: list[int]
    name: str
    port: int
    properties: dict[str, str]
    server: str
    service_type: str


class SettingsCapabilities(_extensions.TypedDict):
    aes67_multicast_prefix: bool


class SubscriptionClassification(_extensions.TypedDict):
    detail: _typing.Union[str, None]
    label: _typing.Union[str, None]
    settled: bool
    severity: str
    state: str
    transport: _typing.Union[SubscriptionTransport, None]


class SubscriptionPlan(_extensions.TypedDict):
    batches: list[Batch]
    unchanged: list[ExpectedSubscription]


class SubscriptionReadback(_extensions.TypedDict):
    channels: list[ChannelReadback]
    matched: bool
    settled: bool


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


class UncharacterizedFlow(_extensions.TypedDict):
    channel_count: int
    flow_number: int
    flow_type: FlowType
    reason: str


class TopologyImpact(_extensions.TypedDict):
    destructive_transmitter_membership_loss: list[FlowMembershipLoss]
    requires_destructive_confirmation: bool
    reversible_receiver_clipping: list[ReceiverSubscription]
    uncharacterized_transmitter_flows: list[UncharacterizedFlow]
