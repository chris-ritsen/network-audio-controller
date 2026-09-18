use std::collections::HashSet;
use std::fmt::Write;

use serde::Serialize;

use crate::bytes::{read_u16, read_u32, read_u64, string_at_pointer, u16_at};
use crate::commands::{
    FLOW_TYPE_MULTICAST, OPCODE_CREATE_TX_FLOW, OPCODE_CREATE_TX_FLOW_2809, OPCODE_DELETE_TX_FLOW,
    OPCODE_DELETE_TX_FLOW_2809, OPCODE_DEVICE_INFO, OPCODE_DEVICE_NAME, OPCODE_DEVICE_SETTINGS,
    OPCODE_DEVICE_SETTINGS_SET, OPCODE_PROPERTY_DIRECTORY,
    OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809, OPCODE_QUERY_RECEIVER_FLOWS,
    OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809, OPCODE_QUERY_RECEIVER_PORT_RANGES,
    OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809, OPCODE_QUERY_TRANSMIT_CHANNEL_CAPABILITIES,
    OPCODE_QUERY_TX_FLOWS, OPCODE_QUERY_TX_FLOWS_2809,
    OPCODE_RECONCILE_TRANSMITTER_CHANNEL_NAMES_2809, OPCODE_RX_CHANNEL_NAME_SET,
    OPCODE_SET_RECEIVER_CHANNEL_NAME_2809, OPCODE_STORE_CURRENT_CONFIGURATION,
    OPCODE_SUBSCRIPTION_ADD, OPCODE_SUBSCRIPTION_REMOVE, OPCODE_TX_CHANNEL_NAME_SET, PROTOCOL_CMC,
    PROTOCOL_DANTE_FLOW, PROTOCOL_DANTE_FLOW_2801,
};
use crate::protocol::{
    common_arc_protocol_opcodes, conmon_opcode, device_settings_arc_protocol_opcodes,
    is_common_arc_protocol, modern_arc_protocol_opcodes, response_envelope,
    validate_conmon_envelope, validate_response_envelope, OPCODE_CHANNEL_COUNT,
    OPCODE_DEVICE_NAME_SET, OPCODE_RX_CHANNELS, OPCODE_TX_CHANNEL_INFO, OPCODE_TX_CHANNEL_NAMES,
    PROTOCOL_ARC_2809, PROTOCOL_ARC_280F,
};

pub use crate::protocol::{RESPONSE_HEADER_SIZE, RESULT_CODE_SUCCESS};

#[cfg(test)]
const CONMON_OPCODE_PANEL_STATUS: u16 = 0x100E;

const METERING_V2_HEADER_SIZE: usize = 27;
const METERING_FAMILY_OFFSET: usize = 24;
const METERING_V2_TX_COUNT_OFFSET: usize = 25;
const METERING_V2_RX_COUNT_OFFSET: usize = 26;
const METERING_V2_LEVELS_OFFSET: usize = 27;
const METERING_V3_HEADER_SIZE: usize = 30;
const METERING_V3_TX_COUNT_OFFSET: usize = 26;
const METERING_V3_RX_COUNT_OFFSET: usize = 28;
const METERING_V3_LEVELS_OFFSET: usize = 30;

const FLOW_RECORD_FIXED_SIZE: usize = 16;
const FLOW_RECORD_FLOW_TYPE: usize = 2;
const FLOW_RECORD_SAMPLE_RATE: usize = 4;
const FLOW_RECORD_ENCODING: usize = 8;
const FLOW_RECORD_FRAMES_PER_PACKET: usize = 12;
const FLOW_RECORD_CHANNEL_COUNT: usize = 14;
const FLOW_TYPE_UNICAST: u16 = 0x0011;
const MODERN_ARC_POINTER_TABLE_OFFSET: usize = 18;
const TRANSMITTER_FLOW_STATUS_RECORD_FLOW_NUMBER: usize = 2;
const TRANSMITTER_FLOW_STATUS_RECORD_MEDIA_TYPE: usize = 6;
const TRANSMITTER_FLOW_STATUS_RECORD_MEDIA_LOCAL_ID: usize = 8;
const TRANSMITTER_FLOW_STATUS_RECORD_FLOW_TYPE: usize = 14;
const TRANSMITTER_FLOW_STATUS_RECORD_NAME_POINTER: usize = 20;
const TRANSMITTER_FLOW_STATUS_RECORD_FORMAT_POINTER: usize = 22;
const TRANSMITTER_FLOW_STATUS_SUBSCRIBER_SEGMENT_INDEX: usize = 1;
const TRANSMITTER_FLOW_STATUS_SUBSCRIBER_DEVICE_POINTER: usize = 4;
const TRANSMITTER_FLOW_STATUS_SUBSCRIBER_FLOW_POINTER: usize = 6;
const TRANSMITTER_FLOW_STATUS_ENDPOINT_SEGMENT_INDEX: usize = 2;
const TRANSMITTER_FLOW_STATUS_ENDPOINT_POINTER: usize = 4;
const TRANSMITTER_FLOW_STATUS_SLOT_COUNT: usize = 2;
const TRANSMITTER_FLOW_STATUS_SLOT_IDS: usize = 4;
const TRANSMITTER_FLOW_STATUS_SLOT_TRAILING_FIELD_SIZE: usize = 2;
const TRANSMITTER_FLOW_STATUS_ENDPOINT_SIZE: usize = 8;
const MEDIA_TYPE_AUDIO: u16 = 3;
const MEDIA_TYPE_VIDEO: u16 = 4;
const MEDIA_TYPE_ANCILLARY: u16 = 5;
const CHANNEL_STATUS_RECORD_CHANNEL_NUMBER: usize = 2;
const CHANNEL_STATUS_RECORD_MEDIA_TYPE: usize = 6;
const CHANNEL_STATUS_RECORD_MEDIA_LOCAL_ID: usize = 8;
const CHANNEL_STATUS_RECORD_NAME_POINTER: usize = 20;
const CHANNEL_STATUS_RECORD_FORMAT_POINTER: usize = 22;
const CHANNEL_STATUS_RECORD_FRIENDLY_NAME_POINTER: usize = 30;
const CHANNEL_STATUS_FORMAT_SIZE: usize = 16;
const RECEIVER_FLOW_STATUS_RECORD_FLOW_NUMBER: usize = 2;
const RECEIVER_FLOW_STATUS_RECORD_MEDIA_TYPE: usize = 6;
const RECEIVER_FLOW_STATUS_RECORD_MEDIA_LOCAL_ID: usize = 8;
const RECEIVER_FLOW_STATUS_RECORD_FLOW_TYPE: usize = 14;
const RECEIVER_FLOW_STATUS_RECORD_NAME_POINTER: usize = 20;
const RECEIVER_FLOW_STATUS_RECORD_FORMAT_POINTER: usize = 22;
const RECEIVER_FLOW_STATUS_RECORD_LATENCY: usize = 24;
const RECEIVER_FLOW_STATUS_MAPPING_SIZE: usize = 8;
const RECEIVER_FLOW_STATUS_ENDPOINT_SIZE: usize = 8;

pub const DEVICE_SETTINGS_INFO_SAMPLE_RATE: u16 = 0x8020;
pub const DEVICE_SETTINGS_INFO_AES67_CONFIGURED: u16 = 0x0063;
pub const DEVICE_SETTINGS_INFO_AES67_MULTICAST_PREFIX: u16 = 0x8060;
pub const DEVICE_SETTINGS_INFO_DEFAULT_LATENCY_NS: u16 = 0x8204;
pub const DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS: u16 = 0x8205;
pub const DEVICE_SETTINGS_INFO_LATENCY_NS: u16 = DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS;
pub const DEVICE_SETTINGS_INFO_ACTIVE_LATENCY_NS: u16 = 0x8301;
pub const DEVICE_SETTINGS_INFO_MAX_LATENCY_NS: u16 = 0x8302;
pub const DEVICE_SETTINGS_INFO_MIN_LATENCY_NS: u16 = 0x8306;

const CONMON_DANTE_MODEL_BODY_OFFSET: usize = 0x18;
const CONMON_DANTE_MODEL_PRIMARY_CAPABILITIES_OFFSET: usize = CONMON_DANTE_MODEL_BODY_OFFSET + 0x1C;
const CONMON_DANTE_MODEL_READ_ONLY_CAPABILITIES_OFFSET: usize =
    CONMON_DANTE_MODEL_BODY_OFFSET + 0x3C;
const CONMON_DANTE_MODEL_MONITORING_CAPABILITIES_OFFSET: usize =
    CONMON_DANTE_MODEL_BODY_OFFSET + 0xC0;
const CONMON_DANTE_MODEL_SECONDARY_CAPABILITIES_OFFSET: usize =
    CONMON_DANTE_MODEL_BODY_OFFSET + 0xC4;
const CONMON_DANTE_MODEL_DOMAIN_CAPABILITY_VALUES_OFFSET: usize =
    CONMON_DANTE_MODEL_BODY_OFFSET + 0xC8;
const CONMON_DANTE_MODEL_DOMAIN_CAPABILITY_VALIDITY_OFFSET: usize =
    CONMON_DANTE_MODEL_BODY_OFFSET + 0xCC;
const DANTE_MODEL_IDENTIFY_CAPABILITY_MASK: u32 = 0x0000_0001;
const DANTE_MODEL_SAMPLE_RATE_CAPABILITY_MASK: u32 = 0x0000_0008;
const DANTE_MODEL_ENCODING_CAPABILITY_MASK: u32 = 0x0000_0010;
const DANTE_MODEL_SAMPLE_RATE_PULLUP_CAPABILITY_MASK: u32 = 0x0000_0200;
const DANTE_MODEL_SWITCH_REDUNDANCY_CAPABILITY_MASK: u32 = 0x0000_2000;
const DANTE_MODEL_STATIC_IPV4_CAPABILITY_MASK: u32 = 0x0000_4000;
const DANTE_MODEL_AES67_CAPABILITY_MASK: u32 = 0x0400_0000;
const DANTE_MODEL_DETAILED_METERING_CAPABILITY_MASK: u32 = 0x0000_8000;
const DANTE_MODEL_LOCKING_CAPABILITY_MASK: u32 = 0x0800_0000;
const DANTE_MODEL_EXTERNAL_WORD_CLOCK_READ_ONLY_MASK: u32 = 0x0000_0080;
const DANTE_MODEL_SWITCH_REDUNDANCY_READ_ONLY_MASK: u32 = 0x0000_2000;
const DANTE_MODEL_STATIC_IPV4_READ_ONLY_MASK: u32 = 0x0000_4000;
const DANTE_MODEL_GENERIC_CODEC_CAPABILITY_MASK: u32 = 0x0000_0008;
const MONITORING_INTERFACE_STATISTICS_MASK: u32 = 0x01;
const MONITORING_CLOCK_MASK: u32 = 0x02;
const MONITORING_PER_CHANNEL_SIGNAL_PRESENCE_MASK: u32 = 0x04;
const MONITORING_RX_FLOW_MAXIMUM_LATENCY_MASK: u32 = 0x08;
const MONITORING_RX_FLOW_LATE_PACKET_MASK: u32 = 0x10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeviceInfo {
    pub model_name: String,
    pub display_name: String,
    pub model_code: String,
    pub port: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeviceSettings {
    pub sample_rate: Option<u32>,
    pub configured_latency_ns: Option<u32>,
    pub active_latency_ns: Option<u32>,
    pub default_latency_ns: Option<u32>,
    pub min_latency_ns: Option<u32>,
    pub max_latency_ns: Option<u32>,
    pub aes67_multicast_prefix: Option<String>,
    pub inline_values: Vec<DeviceSettingsInlineValue>,
    pub referenced_values: Vec<DeviceSettingsReferencedValue>,
    pub unavailable_property_ids: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeviceSettingsInlineValue {
    pub info_code: u16,
    pub value: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeviceSettingsReferencedValue {
    pub info_code: u16,
    pub pointer: u16,
    pub value_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PropertyDirectoryEntry {
    pub property_id: u16,
    pub flags: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PropertyDirectory {
    pub properties: Vec<PropertyDirectoryEntry>,
    pub aes67_configured_property_advertised: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManufacturerVersions {
    pub record_protocol_version: u16,
    pub manufacturer_identifier: Option<String>,
    pub manufacturer_identifier_hexadecimal: String,
    pub product_identifier: Option<String>,
    pub product_identifier_hexadecimal: String,
    pub serial_number_identifier: Option<String>,
    pub serial_number_identifier_hexadecimal: String,
    pub manufacturer_software_version: Option<String>,
    pub manufacturer_software_version_components: Option<Vec<u32>>,
    pub manufacturer_firmware_version: Option<String>,
    pub manufacturer_firmware_version_components: Option<Vec<u32>>,
    pub manufacturer_capabilities: Option<u32>,
    pub manufacturer: Option<String>,
    pub product_name: Option<String>,
    pub product_version: Option<String>,
    pub product_version_components: Option<Vec<u32>>,
    pub friendly_product_version: Option<String>,
    pub display_product_version: Option<String>,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlatformVersions {
    pub record_protocol_version: u16,
    pub platform_software_version: Option<String>,
    pub platform_software_version_components: Option<Vec<u32>>,
    pub platform_hardware_version: Option<String>,
    pub platform_hardware_version_components: Option<Vec<u32>>,
    pub platform_api_version: Option<String>,
    pub platform_api_version_components: Option<Vec<u32>>,
    pub platform_model_identifier: Option<String>,
    pub platform_model_identifier_hexadecimal: String,
    pub primary_capabilities: Option<u32>,
    pub preferred_link_speed: Option<u32>,
    pub device_status_flags: Option<u32>,
    pub rom_boot_version: Option<String>,
    pub rom_boot_version_components: Option<Vec<u32>>,
    pub supported_clock_protocol_flags: u32,
    pub read_only_capabilities: Option<u32>,
    pub platform_model_name: Option<String>,
    pub monitoring_capabilities: u32,
    pub secondary_capabilities: u32,
    pub domain_capability_values: u32,
    pub domain_capability_validity: u32,
    pub effective_domain_capabilities: u32,
    pub plugin_identifiers: Vec<Option<String>>,
    pub plugin_records_hexadecimal: Vec<String>,
    pub raw_record_hexadecimal: String,
    pub identify_supported: bool,
    pub sample_rate_configuration_supported: bool,
    pub encoding_configuration_supported: bool,
    pub sample_rate_pullup_configuration_supported: bool,
    pub switch_redundancy_supported: Option<bool>,
    pub static_ipv4_configuration_supported: bool,
    pub detailed_metering_supported: bool,
    pub aes67_configuration_supported: bool,
    pub device_locking_supported: bool,
    pub external_word_clock_read_only: bool,
    pub switch_redundancy_read_only: Option<bool>,
    pub static_ipv4_configuration_read_only: bool,
    pub generic_codec_control_supported: bool,
    pub virtual_panel_supported: bool,
    pub video_transmission_supported: bool,
    pub video_reception_supported: bool,
    pub interface_statistics_supported: bool,
    pub clock_monitoring_supported: bool,
    pub per_channel_signal_presence_supported: bool,
    pub rx_flow_maximum_latency_monitoring_supported: bool,
    pub rx_flow_late_packet_monitoring_supported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CmcRegistrationResponse {
    pub sequence: u16,
    pub status: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MeteringFrame {
    pub message_version: u8,
    pub sequence: u16,
    pub source_eui64: String,
    pub tx_count: u16,
    pub rx_count: u16,
    pub tx_levels: Vec<u8>,
    pub rx_levels: Vec<u8>,
    pub trailing_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TxFlow {
    pub flow_number: u16,
    pub flow_type: String,
    pub sample_rate: u32,
    pub encoding: u16,
    pub frames_per_packet: u16,
    pub channel_count: u16,
    pub channels: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TxFlowPage {
    pub max_flow_slots: u8,
    pub flows: Vec<TxFlow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransmitterFlowStatus {
    pub record_pointer: u16,
    pub record_length_bytes: u16,
    pub global_flow_id: u16,
    pub media_type_code: u16,
    pub media_local_flow_id: u16,
    pub flow_name_pointer: u16,
    pub flow_name: String,
    pub flow_type_code: u16,
    pub flow_type: Option<String>,
    pub format_pointer: u16,
    pub format_descriptor_hexadecimal: String,
    pub sample_rate: Option<u32>,
    pub encoding: Option<u32>,
    pub channel_slot_segment_header: Option<u16>,
    pub channel_slot_count: Option<u16>,
    pub transmitter_channel_ids_by_slot: Vec<u16>,
    pub populated_transmitter_channel_ids: Vec<u16>,
    pub populated_slot_count: u16,
    pub endpoint_descriptor_pointer: u16,
    pub endpoint_descriptor_hexadecimal: String,
    pub destination_user_datagram_port: Option<u16>,
    pub destination_internet_protocol_version_four_address: Option<String>,
    pub subscriber_device_name_pointer: u16,
    pub subscriber_device_name: Option<String>,
    pub subscriber_flow_name_pointer: u16,
    pub subscriber_flow_name: Option<String>,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransmitterFlowStatusPage {
    pub maximum_flow_slots: u8,
    pub reported_flow_count: u8,
    pub flows: Vec<TransmitterFlowStatus>,
    pub raw_body_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModernArcTransmitterChannelStatus {
    pub record_pointer: u16,
    pub record_length_bytes: u16,
    pub record_type_code: u16,
    pub channel_number: u16,
    pub media_type_code: u16,
    pub media_type: String,
    pub media_local_channel_id: u16,
    pub channel_name_pointer: u16,
    pub channel_name: String,
    pub format_pointer: u16,
    pub format_descriptor_hexadecimal: String,
    pub sample_rate: Option<u32>,
    pub encoding: Option<u16>,
    pub friendly_channel_name_pointer: u16,
    pub friendly_channel_name: String,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModernArcPageDisposition {
    Complete,
    MorePages,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModernArcTransmitterChannelStatusPage {
    pub protocol_id: u16,
    pub transaction_id: u16,
    pub opcode: u16,
    pub result_code: u16,
    pub page_disposition: ModernArcPageDisposition,
    pub page_capacity: u8,
    pub reported_record_count: u8,
    pub records: Vec<ModernArcTransmitterChannelStatus>,
    pub raw_body_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransmitterChannelNameReconciliationRecord2809 {
    pub channel_number: u16,
    pub record_type_code: u16,
    pub name_pointer: u16,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransmitterChannelNameReconciliation2809 {
    pub declared_channel_count: u8,
    pub reported_record_count: u8,
    pub records: Vec<TransmitterChannelNameReconciliationRecord2809>,
    pub raw_body_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModernArcReceiverChannelStatus {
    pub record_pointer: u16,
    pub record_length_bytes: u16,
    pub record_type_code: u16,
    pub channel_number: u16,
    pub media_type_code: u16,
    pub media_type: String,
    pub media_local_channel_id: u16,
    pub local_channel_name_pointer: u16,
    pub local_channel_name: String,
    pub format_pointer: u16,
    pub format_descriptor_hexadecimal: String,
    pub sample_rate: Option<u32>,
    pub encoding: Option<u16>,
    pub friendly_channel_name_pointer: u16,
    pub friendly_channel_name: String,
    pub source_channel_name_pointer: u16,
    pub source_channel_name: Option<String>,
    pub source_device_name_pointer: u16,
    pub source_device_name: Option<String>,
    pub subscription_status_code: u16,
    pub receiver_status_code: u16,
    pub receiver_capability_flags: u32,
    pub can_subscribe_self: bool,
    pub can_rename: bool,
    pub status_flags: Option<u16>,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModernArcReceiverChannelStatusPage {
    pub protocol_id: u16,
    pub transaction_id: u16,
    pub opcode: u16,
    pub result_code: u16,
    pub page_disposition: ModernArcPageDisposition,
    pub page_capacity: u8,
    pub reported_record_count: u8,
    pub records: Vec<ModernArcReceiverChannelStatus>,
    pub raw_body_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModernArcReceiverFlowStatus {
    pub record_pointer: u16,
    pub record_length_bytes: u16,
    pub record_type_code: u16,
    pub global_flow_id: u16,
    pub media_type_code: u16,
    pub media_local_flow_id: u16,
    pub flow_type_code: u16,
    pub flow_name_pointer: u16,
    pub flow_name: String,
    pub format_pointer: u16,
    pub format_descriptor_hexadecimal: String,
    pub sample_rate: Option<u32>,
    pub encoding: Option<u32>,
    pub latency_nanoseconds: Option<u32>,
    pub local_receiver_channel_count: u16,
    pub receiver_mapping_descriptor_pointer: u16,
    pub receiver_mapping_descriptor_hexadecimal: String,
    pub status_flags: u16,
    pub status_code: u16,
    pub endpoint_descriptor_hexadecimal: String,
    pub destination_user_datagram_port: Option<u16>,
    pub destination_internet_protocol_version_four_address: Option<String>,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModernArcReceiverFlowStatusPage {
    pub protocol_id: u16,
    pub transaction_id: u16,
    pub opcode: u16,
    pub result_code: u16,
    pub page_disposition: ModernArcPageDisposition,
    pub maximum_flow_slots: u8,
    pub reported_flow_count: u8,
    pub flows: Vec<ModernArcReceiverFlowStatus>,
    pub raw_body_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverFlow {
    pub flow_number: u16,
    pub flags: u16,
    pub flow_type: Option<String>,
    pub sample_rate: u32,
    pub encoding: u32,
    pub interface_count: u16,
    pub flow_channel_slot_count: u16,
    pub receiver_bitmap_word_count: u16,
    pub interface_endpoints: Vec<ReceiverFlowInterfaceEndpoint>,
    pub receiver_bitmaps_hexadecimal: Vec<String>,
    pub receiver_channel_numbers_by_flow_channel: Vec<Vec<u16>>,
    pub subscription_status_code: u16,
    pub interface_state_bitmap: u16,
    pub status_flags: u16,
    pub status_unknown: u16,
    pub latency_nanoseconds: u32,
    pub transport: u16,
    pub external_identity_pointer: u16,
    pub external_identity: Option<ExternalRtpFlowIdentity>,
    pub effective_subscription_identities: Vec<ReceiverFlowSubscriptionIdentity>,
    pub status_descriptor_hexadecimal: String,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverFlowInterfaceEndpoint {
    pub pointer: u16,
    pub descriptor_length_bytes: u8,
    pub kind: u8,
    pub udp_port: u16,
    pub ipv4_address: Option<String>,
    pub raw_descriptor_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExternalRtpFlowIdentity {
    pub pointer: u16,
    pub length_words: u8,
    pub reserved: u8,
    pub presence_mask: u16,
    pub source_ipv4: Option<String>,
    pub session_id: Option<u64>,
    pub unknown_optional_field_raw: u64,
    pub clock_offset: Option<u32>,
    pub raw_descriptor_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverFlowSubscriptionIdentity {
    pub receiver_channel: u16,
    pub flow_slot: u16,
    pub source_ipv4: String,
    pub session_id: u64,
    pub interface_endpoints: Vec<ReceiverFlowInterfaceEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverFlowPage {
    pub result_code: u16,
    pub page_disposition: ModernArcPageDisposition,
    pub maximum_flow_slots: u8,
    pub reported_flow_count: u8,
    pub flows: Vec<ReceiverFlow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverPortRanges {
    pub first_port_range_start: u16,
    pub first_port_range_end: u16,
    pub second_port_range_start: u16,
    pub second_port_range_end: u16,
    pub second_port_range_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransmitChannelCapabilities {
    pub record_count: u8,
    pub ranges: Vec<TransmitChannelCapabilityRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransmitChannelCapabilityRange {
    pub first_transmit_channel: u16,
    pub last_transmit_channel: u16,
    pub unknown_value: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DanteBrooklynControlProtocolFlowSetupRequest {
    pub transaction_identifier_hex: String,
    pub receiver_device_name_pointer: u32,
    pub sample_rate: u32,
    pub encoding: u32,
    pub transport_descriptor_pointer: u16,
    pub transport_descriptor_count: u16,
    pub address_value_pointer: u32,
    pub flow_span_value: u16,
    pub receiver_channel_name_pointer: u16,
    pub receiver_device_name: String,
    pub receiver_channel_name: String,
    pub address_at_pointer: String,
    pub transport_descriptor_hex: String,
    pub receiver_address: String,
    pub raw_payload_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DanteBrooklynControlProtocolFlowSetupResponse {
    pub transaction_identifier_hex: String,
    pub field_at_offset_8_hex: String,
    pub flow_identifier: u32,
    pub field_at_offset_16_hex: String,
    pub field_at_offset_20_hex: String,
    pub raw_payload_hex: String,
}

fn ipv4_at(data: &[u8], offset: usize) -> Option<String> {
    let octets: [u8; 4] = data.get(offset..offset + 4)?.try_into().ok()?;
    Some(std::net::Ipv4Addr::from(octets).to_string())
}

fn bytes_to_hex(data: &[u8]) -> String {
    let mut encoded = String::with_capacity(data.len() * 2);
    for value in data {
        write!(encoded, "{value:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

mod channel_status;
mod clock;
mod conmon;
mod conmon_common;
mod conmon_detail;
mod device;
mod flow_setup;
mod flows;
mod gain;
mod network;
mod pointer_table;

pub use channel_status::*;
pub use clock::*;
pub use conmon::*;
pub use conmon_common::*;
pub use conmon_detail::*;
pub use device::*;
pub use flow_setup::*;
pub use flows::*;
pub use gain::*;
pub use network::*;
use pointer_table::parse_pointer_table_page;

#[cfg(test)]
mod tests;
