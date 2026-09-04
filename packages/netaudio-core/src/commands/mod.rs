use std::collections::{HashMap, HashSet};

use crate::protocol::{
    build_control_packet, build_control_packet_for_protocol, validate_dante_channel_name,
    validate_dante_channel_reference, validate_dante_name, ConmonHeader, NetaudioError,
    OPCODE_CHANNEL_COUNT, OPCODE_DEVICE_NAME_SET, OPCODE_RX_CHANNELS, OPCODE_TX_CHANNEL_INFO,
    OPCODE_TX_CHANNEL_NAMES, PROTOCOL_ARC_2809,
};

pub const PROTOCOL_SETTINGS: u16 = 0xFFFF;
pub const PROTOCOL_CMC: u16 = 0x1200;
pub const PROTOCOL_DANTE_FLOW: u16 = 0x2729;
pub const PROTOCOL_DANTE_FLOW_2801: u16 = 0x2801;

pub const OPCODE_DEVICE_NAME: u16 = 0x1002;
pub const OPCODE_DEVICE_INFO: u16 = 0x1003;
pub const OPCODE_DEVICE_SETTINGS: u16 = 0x1100;
pub const OPCODE_DEVICE_SETTINGS_SET: u16 = 0x1101;
pub const OPCODE_PROPERTY_DIRECTORY: u16 = 0x1102;
pub const OPCODE_TX_CHANNEL_NAME_SET: u16 = 0x2013;
pub const OPCODE_RX_CHANNEL_NAME_SET: u16 = 0x3001;
pub const OPCODE_SUBSCRIPTION_ADD: u16 = 0x3010;
pub const OPCODE_SUBSCRIPTION_REMOVE: u16 = 0x3014;
pub const OPCODE_QUERY_TX_FLOWS: u16 = 0x2200;
pub const OPCODE_QUERY_TRANSMIT_CHANNEL_CAPABILITIES: u16 = 0x2032;
pub const OPCODE_QUERY_RECEIVER_FLOWS: u16 = 0x3200;
pub const OPCODE_QUERY_RECEIVER_PORT_RANGES: u16 = 0x3300;
pub const OPCODE_CREATE_TX_FLOW: u16 = 0x2201;
pub const OPCODE_DELETE_TX_FLOW: u16 = 0x2202;
pub const OPCODE_QUERY_TX_FLOWS_2809: u16 = 0x2600;
pub const OPCODE_CREATE_TX_FLOW_2809: u16 = 0x2601;
pub const OPCODE_DELETE_TX_FLOW_2809: u16 = 0x2602;
pub const OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809: u16 = 0x2400;
pub const OPCODE_RECONCILE_TRANSMITTER_CHANNEL_NAMES_2809: u16 = 0x2438;
pub const OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809: u16 = 0x3400;
pub const OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809: u16 = 0x3600;
pub const OPCODE_MODERN_ARC_SUBSCRIPTION: u16 = 0x3410;
pub const OPCODE_SET_RECEIVER_CHANNEL_NAME_2809: u16 = 0x3401;

pub const FLOW_TYPE_MULTICAST: u16 = 0x0002;

pub const MAGIC_VENDOR: &[u8] = b"Audinate";

const VENDOR_SEPARATOR: u8 = 0x07;

const SETTINGS_SUFFIX_SYSTEM_CONFIG: u8 = 0x3a;
const SETTINGS_SUFFIX_IDENTITY: u8 = 0x31;
const SETTINGS_SUFFIX_AES67_WRITE: u8 = 0x34;
const SETTINGS_SUFFIX_AUDIO_CONFIG: u8 = 0x27;
const SETTINGS_SUFFIX_CLEAR_CONFIGURATION: u8 = 0x3e;
const SETTINGS_SUFFIX_DIAGNOSTIC_EXPORT: u8 = 0x24;

const CONMON_EXPORT_MESSAGE_TYPE: u16 = 0xFF04;
const DIAGNOSTIC_LOG_EXPORT_TAG: [u8; 4] = *b"LOGS";
const DIAGNOSTIC_LOG_EXPORT_SELECTOR: u16 = 1;
const CAPABILITY_PARTITION_EXPORT_TAG: [u8; 4] = *b"CAP1";
const CAPABILITY_PARTITION_EXPORT_SELECTOR: u16 = 2;

const CLEAR_CONFIGURATION_MESSAGE_TYPE: u16 = 0x0077;
const CLEAR_CONFIGURATION_REQUEST_VALUE: u32 = 100;
const CLEAR_CONFIGURATION_ACTION_ALL: u32 = 1;
const CLEAR_CONFIGURATION_ACTION_PRESERVE_INTERNET_PROTOCOL: u32 = 2;
const SYSTEM_RESET_MESSAGE_TYPE: u16 = 0x0090;
const SYSTEM_RESET_REQUEST_VALUE: u32 = 100;
const SYSTEM_RESET_PRESENT: u16 = 1;
const SYSTEM_RESET_MODE_REBOOT: u16 = 0;
const SYSTEM_RESET_MODE_FACTORY: u16 = 1;

const GAIN_MESSAGE_TYPE: u16 = 0x100A;
const GAIN_INPUT_DIRECTION: u16 = 0x0102;
const GAIN_OUTPUT_DIRECTION: u16 = 0x0201;

const AUDIO_CONFIG_PSEUDO_MAC: [u8; 6] = [b'R', b'T', 0, 0, 0, 0];

const LATENCY_SET_PREAMBLE: [u8; 22] = [
    0x05, 0x04, 0x82, 0x05, 0x00, 0x20, 0x02, 0x11, 0x00, 0x04, 0x83, 0x01, 0x00, 0x24, 0x03, 0x10,
    0x00, 0x04, 0x83, 0x02, 0x83, 0x06,
];

const LATENCY_CONFIG_QUERY_INFO_CODES: [u8; 48] = [
    0x00, 0x17, 0x02, 0x01, 0x82, 0x04, 0x82, 0x05, 0x02, 0x10, 0x02, 0x11, 0x82, 0x18, 0x82, 0x19,
    0x83, 0x01, 0x83, 0x02, 0x83, 0x06, 0x03, 0x10, 0x03, 0x11, 0x03, 0x03, 0x80, 0x21, 0x00, 0xF0,
    0x80, 0x60, 0x00, 0x22, 0x00, 0x63, 0x00, 0x64, 0x00, 0x65, 0x02, 0x22, 0x02, 0x12, 0x83, 0x21,
];

pub const MIN_GAIN_LEVEL: u8 = 1;
pub const MAX_GAIN_LEVEL: u8 = 5;
pub const MAX_LATENCY_MILLISECONDS: f64 = u32::MAX as f64 / 1_000_000.0;

pub fn build_cmc_register(message_id: u16, host_mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    let mut body = Vec::with_capacity(14);
    body.extend_from_slice(&0x1001u16.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&host_mac);
    body.extend_from_slice(&0u16.to_be_bytes());
    ConmonHeader {
        message_id,
        protocol_id: PROTOCOL_CMC,
    }
    .packet(&body)
}

fn settings_packet(
    message_id: u16,
    mac: [u8; 6],
    suffix: u8,
    tail: &[u8],
) -> Result<Vec<u8>, NetaudioError> {
    let mut body = Vec::with_capacity(20 + tail.len());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&mac);
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(MAGIC_VENDOR);
    body.push(VENDOR_SEPARATOR);
    body.push(suffix);
    body.extend_from_slice(tail);
    ConmonHeader {
        message_id,
        protocol_id: PROTOCOL_SETTINGS,
    }
    .packet(&body)
}

fn channel_query_payload(starting_channel: u16) -> [u8; 8] {
    let mut payload = [0u8; 8];
    payload[3] = 0x01;
    payload[4..6].copy_from_slice(&starting_channel.to_be_bytes());
    payload
}

fn channel_range_query_payload(starting_channel: u16, ending_channel: u16) -> [u8; 8] {
    let mut payload = channel_query_payload(starting_channel);
    payload[6..8].copy_from_slice(&ending_channel.to_be_bytes());
    payload
}

fn arc_packet_with_reserved_word(
    protocol_id: u16,
    opcode: u16,
    body: &[u8],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut payload = Vec::with_capacity(2 + body.len());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(body);
    build_control_packet_for_protocol(protocol_id, opcode, &payload, message_id)
}

mod device;
mod flows;
mod metering;
mod settings;
mod subscriptions;

pub use device::*;
pub use flows::*;
pub use metering::*;
pub use settings::*;
pub use subscriptions::*;

#[cfg(test)]
mod tests;
