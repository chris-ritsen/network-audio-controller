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


class ClockControlAvailability(_extensions.TypedDict):
    aggregate_ptpv1_unicast_delay_requests: bool
    clock_source: bool
    global_unicast_delay_requests: bool
    preferred_leader: bool
    subdomain: bool


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


class LateSample(_extensions.TypedDict):
    late_packet_count: int
    late_packet_delta: _typing.Union[int, None]
    preserve_history: bool
    receiver_flow_index: int


class LateUpdate(_extensions.TypedDict):
    samples: list[LateSample]
    sequence: int


class LatencySample(_extensions.TypedDict):
    latency_nanoseconds: int
    latency_sample_count: int
    receiver_flow_index: int
    sample_rate_hertz: int


class LatencyUpdate(_extensions.TypedDict):
    preserve_history: bool
    samples: list[LatencySample]
    sequence: int


class ConnectionHealthUpdate(_extensions.TypedDict):
    device_extended_unique_identifier: str
    late_packets: _typing.Union[LateUpdate, None]
    latency: _typing.Union[LatencyUpdate, None]


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


ReceiverFlowInventoryFamily = _typing.Literal["legacy", "modern"]


class FlowAuthoringProfile(_extensions.TypedDict):
    identifier_max: int
    identity_field: str
    media_modes: list[str]
    protocol_id: int
    supports_flow_options: bool


class FlowAuthoringCapabilities(_extensions.TypedDict):
    receiver_flow_inventory_family: ReceiverFlowInventoryFamily
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


class InventoryStateVariant1(_extensions.TypedDict):
    inventory: dict[str, JsonValue]
    next_command: None


InventoryState = _typing.Union[InventoryStateVariant0, InventoryStateVariant1]


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


class PanelProfile(_extensions.TypedDict):
    family: _typing.Union[PanelFamily, None]
    queries: list[PanelQuery]
    read_unavailable_reason: _typing.Union[str, None]
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
