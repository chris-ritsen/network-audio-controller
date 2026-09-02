use std::collections::HashSet;
use std::fmt::Write;

use serde::Serialize;

use crate::bytes::{read_u16, read_u32, string_at_pointer, u16_at};
use crate::commands::{
    FLOW_TYPE_MULTICAST, OPCODE_CREATE_TX_FLOW, OPCODE_CREATE_TX_FLOW_2809, OPCODE_DELETE_TX_FLOW,
    OPCODE_DELETE_TX_FLOW_2809, OPCODE_DEVICE_INFO, OPCODE_DEVICE_NAME, OPCODE_DEVICE_SETTINGS,
    OPCODE_DEVICE_SETTINGS_SET, OPCODE_PROPERTY_DIRECTORY,
    OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809, OPCODE_QUERY_RECEIVER_FLOWS,
    OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809, OPCODE_QUERY_RECEIVER_PORT_RANGES,
    OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809, OPCODE_QUERY_TRANSMIT_CHANNEL_CAPABILITIES,
    OPCODE_QUERY_TX_FLOWS, OPCODE_QUERY_TX_FLOWS_2809,
    OPCODE_RECONCILE_TRANSMITTER_CHANNEL_NAMES_2809, OPCODE_RX_CHANNEL_NAME_SET,
    OPCODE_SET_RECEIVER_CHANNEL_NAME_2809, OPCODE_SUBSCRIPTION_ADD, OPCODE_SUBSCRIPTION_REMOVE,
    OPCODE_TX_CHANNEL_NAME_SET, PROTOCOL_CMC, PROTOCOL_DANTE_FLOW, PROTOCOL_DANTE_FLOW_2801,
};
use crate::protocol::{
    common_arc_protocol_opcodes, conmon_opcode, device_settings_arc_protocol_opcodes,
    is_common_arc_protocol, modern_arc_protocol_opcodes, response_envelope,
    validate_conmon_envelope, validate_response_envelope, OPCODE_CHANNEL_COUNT,
    OPCODE_DEVICE_NAME_SET, OPCODE_RX_CHANNELS, OPCODE_TX_CHANNEL_INFO, OPCODE_TX_CHANNEL_NAMES,
    PROTOCOL_ARC_2809,
};

pub use crate::protocol::{RESPONSE_HEADER_SIZE, RESULT_CODE_SUCCESS};

const CONMON_OPCODE_BLUETOOTH_STATUS: u16 = 0x100E;

const METERING_V2_HEADER_SIZE: usize = 28;
const METERING_FAMILY_OFFSET: usize = 24;
const METERING_V2_TX_COUNT_OFFSET: usize = 25;
const METERING_V2_RX_COUNT_OFFSET: usize = 26;
const METERING_V2_SUFFIX_OFFSET: usize = 27;
const METERING_V2_LEVELS_OFFSET: usize = 28;
const METERING_V3_HEADER_SIZE: usize = 30;
const METERING_V3_RESERVED_OFFSET: usize = 25;
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
const TRANSMITTER_FLOW_STATUS_FORMAT_SIZE: usize = 8;
const TRANSMITTER_FLOW_STATUS_ENDPOINT_SIZE: usize = 8;
const MEDIA_TYPE_AUDIO: u16 = 3;
const CHANNEL_STATUS_RECORD_CHANNEL_NUMBER: usize = 2;
const CHANNEL_STATUS_RECORD_MEDIA_TYPE: usize = 6;
const CHANNEL_STATUS_RECORD_MEDIA_LOCAL_ID: usize = 8;
const CHANNEL_STATUS_RECORD_NAME_POINTER: usize = 20;
const CHANNEL_STATUS_RECORD_FORMAT_POINTER: usize = 22;
const CHANNEL_STATUS_RECORD_FRIENDLY_NAME_POINTER: usize = 30;
const CHANNEL_STATUS_FORMAT_SIZE: usize = 16;
const TRANSMITTER_CHANNEL_STATUS_RECORD_SIZE: usize = 40;
const RECEIVER_CHANNEL_STATUS_RECORD_SIZE: usize = 56;
const RECEIVER_CHANNEL_STATUS_RECORD_SOURCE_CHANNEL_POINTER: usize = 44;
const RECEIVER_CHANNEL_STATUS_RECORD_SOURCE_DEVICE_POINTER: usize = 46;
const RECEIVER_CHANNEL_STATUS_RECORD_SUBSCRIPTION_STATUS: usize = 48;
const RECEIVER_CHANNEL_STATUS_RECORD_RECEIVER_STATUS: usize = 50;
const RECEIVER_CHANNEL_STATUS_RECORD_STATUS_FLAGS: usize = 52;
const RECEIVER_FLOW_STATUS_RECORD_SIZE: usize = 84;
const RECEIVER_FLOW_STATUS_RECORD_FLOW_NUMBER: usize = 2;
const RECEIVER_FLOW_STATUS_RECORD_CHANNEL_COUNT: usize = 8;
const RECEIVER_FLOW_STATUS_RECORD_FLOW_TYPE: usize = 14;
const RECEIVER_FLOW_STATUS_RECORD_NAME_POINTER: usize = 20;
const RECEIVER_FLOW_STATUS_RECORD_FORMAT_POINTER: usize = 22;
const RECEIVER_FLOW_STATUS_RECORD_LATENCY: usize = 24;
const RECEIVER_FLOW_STATUS_RECORD_LOCAL_RECEIVER_COUNT: usize = 52;
const RECEIVER_FLOW_STATUS_RECORD_MAPPING_POINTER: usize = 54;
const RECEIVER_FLOW_STATUS_RECORD_STATUS_FLAGS: usize = 60;
const RECEIVER_FLOW_STATUS_RECORD_STATUS_CODE: usize = 62;
const RECEIVER_FLOW_STATUS_RECORD_ENDPOINT: usize = 68;
const RECEIVER_FLOW_STATUS_FORMAT_SIZE: usize = 8;
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

const CONMON_MANUFACTURER_OFFSET: usize = 0x4C;
const CONMON_MANUFACTURER_END: usize = 0xCC;
const CONMON_UNMAPPED_FIELD_BEFORE_MANUFACTURER_OFFSET: usize = 0x4A;
const CONMON_PRODUCT_NAME_OFFSET: usize = 0xCC;
const CONMON_PRODUCT_NAME_END: usize = 0x14C;
const CONMON_PRODUCT_VERSION_OFFSET: usize = 0x14C;
const CONMON_PRODUCT_VERSION_END: usize = 0x150;
const CONMON_BOARD_CODENAME_OFFSET: usize = 0x2C;
const CONMON_BOARD_CODENAME_END: usize = 0x58;
const CONMON_BOARD_NAME_OFFSET: usize = 0x58;
const CONMON_BOARD_NAME_END: usize = 0x98;

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
    pub latency_ns: Option<u32>,
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
    pub aes67_supported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MakeModel {
    pub manufacturer: String,
    pub manufacturer_field_hexadecimal: String,
    pub unmapped_field_at_byte_offset_74: u16,
    pub product_name: String,
    pub product_version: String,
    pub product_version_components: [u8; 4],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DanteModel {
    pub board_codename: String,
    pub board_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BluetoothStatus {
    pub connected: bool,
    pub device_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CmcRegistrationResponse {
    pub sequence: u16,
    pub status: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MeteringFrame {
    pub sequence: u16,
    pub source_eui64: String,
    pub tx_count: u16,
    pub rx_count: u16,
    pub tx_levels: Vec<u8>,
    pub rx_levels: Vec<u8>,
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
    /// Compatibility alias for `global_flow_id`.
    pub flow_number: u16,
    pub media_type: u16,
    pub media_local_flow_id: u16,
    pub flow_name_pointer: u16,
    pub flow_name: String,
    pub flow_type_code: u16,
    pub flow_type: Option<String>,
    pub format_pointer: u16,
    pub sample_rate: u32,
    pub encoding: u32,
    /// Deprecated compatibility count derived from populated channel slots.
    pub channel_count: u16,
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
pub struct TransmitterChannelStatus2809 {
    pub record_pointer: u16,
    pub record_type_code: u16,
    pub channel_number: u16,
    pub media_type: u16,
    pub media_local_channel_id: u16,
    pub channel_name_pointer: u16,
    pub channel_name: String,
    pub format_pointer: u16,
    pub format_descriptor_hexadecimal: String,
    pub sample_rate: u32,
    pub encoding: u16,
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
pub struct TransmitterChannelStatusPage2809 {
    pub protocol_id: u16,
    pub transaction_id: u16,
    pub opcode: u16,
    pub result_code: u16,
    pub page_disposition: ModernArcPageDisposition,
    pub page_capacity: u8,
    /// Deprecated compatibility alias for `page_capacity`.
    pub maximum_transmitter_channels: u8,
    pub reported_record_count: u8,
    pub records: Vec<TransmitterChannelStatus2809>,
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
pub struct ReceiverChannelStatus2809 {
    pub record_pointer: u16,
    pub record_type_code: u16,
    pub channel_number: u16,
    pub media_type: u16,
    pub media_local_channel_id: u16,
    pub local_channel_name_pointer: u16,
    pub local_channel_name: String,
    pub format_pointer: u16,
    pub format_descriptor_hexadecimal: String,
    pub sample_rate: u32,
    pub encoding: u16,
    pub friendly_channel_name_pointer: u16,
    pub friendly_channel_name: String,
    pub source_channel_name_pointer: u16,
    pub source_channel_name: Option<String>,
    pub source_device_name_pointer: u16,
    pub source_device_name: Option<String>,
    pub subscription_status_code: u16,
    pub receiver_status_code: u16,
    pub status_flags: u16,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverChannelStatusPage2809 {
    pub protocol_id: u16,
    pub transaction_id: u16,
    pub opcode: u16,
    pub result_code: u16,
    pub page_disposition: ModernArcPageDisposition,
    pub page_capacity: u8,
    /// Deprecated compatibility alias for `page_capacity`.
    pub maximum_receiver_channels: u8,
    pub reported_record_count: u8,
    pub records: Vec<ReceiverChannelStatus2809>,
    pub raw_body_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverFlowStatus2809 {
    pub record_pointer: u16,
    pub record_type_code: u16,
    pub flow_number: u16,
    pub channel_count: u16,
    pub flow_type_code: u16,
    pub flow_name_pointer: u16,
    pub flow_name: String,
    pub format_pointer: u16,
    pub sample_rate: u32,
    pub encoding: u32,
    pub latency_nanoseconds: u32,
    pub local_receiver_channel_count: u16,
    pub receiver_mapping_descriptor_pointer: u16,
    pub receiver_mapping_descriptor_hexadecimal: String,
    pub status_flags_at_record_offset_60: u16,
    pub status_code_at_record_offset_62: u16,
    pub endpoint_descriptor_hexadecimal: String,
    pub destination_user_datagram_port: Option<u16>,
    pub destination_internet_protocol_version_four_address: Option<String>,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverFlowStatusPage2809 {
    pub maximum_flow_slots: u8,
    pub reported_flow_count: u8,
    pub flows: Vec<ReceiverFlowStatus2809>,
    pub raw_body_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverFlow {
    pub flow_number: u16,
    pub flow_state_code: u16,
    pub flow_type: Option<String>,
    pub sample_rate: u32,
    pub encoding: u16,
    pub frames_per_packet: u16,
    pub channel_count: u16,
    pub endpoint_descriptor_size: u16,
    pub endpoint_descriptor_hexadecimal: String,
    pub destination_user_datagram_port: Option<u16>,
    pub destination_internet_protocol_version_four_address: String,
    pub channel_descriptors_hexadecimal: Vec<String>,
    pub receiver_channel_numbers_by_flow_channel: Vec<Vec<u16>>,
    pub subscription_status_code: u16,
    pub status_field_at_byte_offset_two: u16,
    pub status_field_at_byte_offset_four: u16,
    pub status_field_at_byte_offset_six: u16,
    pub latency_nanoseconds: u32,
    pub status_field_at_byte_offset_twelve: u32,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverFlowPage {
    pub maximum_flow_slots: u8,
    pub flows: Vec<ReceiverFlow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiverPortRanges {
    pub first_port_range_start: u16,
    pub first_port_range_end: u16,
    pub second_port_range_start: u16,
    pub second_port_range_end: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransmitChannelCapabilities {
    pub format_identifier: u16,
    pub starting_channel_identifier: u16,
    pub channel_count: u16,
    pub capability_flags: u16,
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
mod conmon;
mod conmon_common;
mod conmon_detail;
mod device;
mod flow_setup;
mod flows;
mod pointer_table;

pub use channel_status::*;
pub use conmon::*;
pub use conmon_common::*;
pub use conmon_detail::*;
pub use device::*;
pub use flow_setup::*;
pub use flows::*;
use pointer_table::parse_pointer_table_page;

#[cfg(test)]
mod tests;
