use serde::Serialize;

use crate::bytes::{null_terminated_lossy, read_u16, read_u32, u16_at};
use crate::commands::FLOW_TYPE_MULTICAST;

pub use crate::parser::RESPONSE_HEADER_SIZE;

pub const RESULT_CODE_SUCCESS: u16 = 0x0001;
const RESULT_CODE_OFFSET: usize = 8;

const FLOW_RECORD_MINIMUM_SIZE: usize = 20;
const FLOW_RECORD_FLOW_TYPE: usize = 2;
const FLOW_RECORD_SAMPLE_RATE: usize = 4;
const FLOW_RECORD_ENCODING: usize = 8;
const FLOW_RECORD_FRAMES_PER_PACKET: usize = 10;
const FLOW_RECORD_CHANNEL_COUNT: usize = 22;
const FLOW_RECORD_CHANNELS: usize = 24;

pub const DEVICE_SETTINGS_INFO_SAMPLE_RATE: u16 = 0x8020;
pub const DEVICE_SETTINGS_INFO_LATENCY: u16 = 0x8204;
pub const DEVICE_SETTINGS_INFO_MIN_LATENCY: u16 = 0x8205;
pub const DEVICE_SETTINGS_INFO_MAX_LATENCY: u16 = 0x8302;

const CONMON_MANUFACTURER_OFFSET: usize = 0x4C;
const CONMON_MANUFACTURER_END: usize = 0xCC;
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
    pub latency_us: Option<u32>,
    pub min_latency_us: Option<u32>,
    pub max_latency_us: Option<u32>,
    pub info_codes: Vec<(u16, u32)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MakeModel {
    pub manufacturer: String,
    pub product_name: String,
    pub product_version: String,
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
pub struct TxFlow {
    pub flow_number: u16,
    pub flow_type: String,
    pub sample_rate: u32,
    pub encoding: u16,
    pub frames_per_packet: u16,
    pub channel_count: u16,
    pub channels: Vec<u16>,
}

impl BluetoothStatus {
    fn disconnected() -> BluetoothStatus {
        BluetoothStatus {
            connected: false,
            device_name: None,
        }
    }
}

pub fn parse_device_name(response: &[u8]) -> Option<String> {
    if response.len() <= RESPONSE_HEADER_SIZE {
        return None;
    }
    let body = &response[RESPONSE_HEADER_SIZE..response.len() - 1];
    std::str::from_utf8(body).ok().map(str::to_owned)
}

pub fn parse_device_info(response: &[u8]) -> Option<DeviceInfo> {
    if response.len() < RESPONSE_HEADER_SIZE {
        return None;
    }
    let body = &response[RESPONSE_HEADER_SIZE..];
    if body.len() < 18 {
        return None;
    }

    let string_at_pointer = |pointer: u16| -> String {
        match (pointer as usize).checked_sub(RESPONSE_HEADER_SIZE) {
            Some(offset) => null_terminated_lossy(body, offset),
            None => String::new(),
        }
    };

    let code_pointer = read_u16(body, 6)?;
    let port_pointer = read_u16(body, 8)?;
    let model_pointer = read_u16(body, 12)?;
    let display_pointer = read_u16(body, 14)?;

    Some(DeviceInfo {
        model_name: string_at_pointer(model_pointer),
        display_name: string_at_pointer(display_pointer),
        model_code: string_at_pointer(code_pointer),
        port: string_at_pointer(port_pointer),
    })
}

pub fn parse_device_settings(response: &[u8]) -> Option<DeviceSettings> {
    if response.len() < RESPONSE_HEADER_SIZE {
        return None;
    }
    let body = &response[RESPONSE_HEADER_SIZE..];
    if body.len() < 2 {
        return None;
    }

    let record_count = body[1];
    let mut settings = DeviceSettings {
        sample_rate: None,
        latency_us: None,
        min_latency_us: None,
        max_latency_us: None,
        info_codes: Vec::new(),
    };
    let mut offset = 2;

    for _ in 0..record_count {
        if offset + 4 > body.len() {
            break;
        }
        let info_code = u16_at(body, offset);
        let value_pointer = u16_at(body, offset + 2);
        offset += 4;

        let Some(value_offset) = (value_pointer as usize).checked_sub(RESPONSE_HEADER_SIZE) else {
            continue;
        };
        let Some(value) = read_u32(body, value_offset) else {
            continue;
        };

        settings.info_codes.push((info_code, value));
        match info_code {
            DEVICE_SETTINGS_INFO_SAMPLE_RATE => settings.sample_rate = Some(value),
            DEVICE_SETTINGS_INFO_LATENCY => settings.latency_us = Some(value),
            DEVICE_SETTINGS_INFO_MIN_LATENCY => settings.min_latency_us = Some(value),
            DEVICE_SETTINGS_INFO_MAX_LATENCY => settings.max_latency_us = Some(value),
            _ => {}
        }
    }

    Some(settings)
}

pub fn parse_aes67_configured(response: &[u8]) -> Option<bool> {
    if response.len() <= 0x53 {
        return None;
    }
    match response[0x53] {
        0x03 => Some(true),
        0x01 => Some(false),
        _ => None,
    }
}

fn conmon_string(data: &[u8], start: usize, end: usize) -> String {
    if data.len() < end {
        return String::new();
    }
    let raw = &data[start..end];
    let null_position = raw.iter().position(|&byte| byte == 0).unwrap_or(raw.len());
    let raw = &raw[..null_position];
    let first_printable = raw.iter().position(|&byte| byte >= 0x20).unwrap_or(raw.len());
    let printable = &raw[first_printable..];
    if printable.is_empty() {
        return String::new();
    }
    let text = String::from_utf8_lossy(printable).trim().to_owned();
    if text.chars().all(|character| !character.is_control() || character == ' ') {
        text
    } else {
        String::new()
    }
}

pub fn parse_make_model(data: &[u8]) -> MakeModel {
    let manufacturer = conmon_string(data, CONMON_MANUFACTURER_OFFSET, CONMON_MANUFACTURER_END);
    let product_name = conmon_string(data, CONMON_PRODUCT_NAME_OFFSET, CONMON_PRODUCT_NAME_END);
    let mut product_version = String::new();
    if data.len() >= CONMON_PRODUCT_VERSION_END {
        let version = &data[CONMON_PRODUCT_VERSION_OFFSET..CONMON_PRODUCT_VERSION_END];
        if version.iter().any(|&byte| byte != 0) {
            let (major, minor, build) = (version[0], version[1], version[3]);
            product_version = format!("{major}.{minor}.{build}");
        }
    }
    MakeModel {
        manufacturer,
        product_name,
        product_version,
    }
}

pub fn parse_dante_model(data: &[u8]) -> DanteModel {
    DanteModel {
        board_codename: conmon_string(data, CONMON_BOARD_CODENAME_OFFSET, CONMON_BOARD_CODENAME_END),
        board_name: conmon_string(data, CONMON_BOARD_NAME_OFFSET, CONMON_BOARD_NAME_END),
    }
}

pub fn parse_result_code(response: &[u8]) -> Option<u16> {
    read_u16(response, RESULT_CODE_OFFSET)
}

pub fn parse_tx_flows(response: &[u8]) -> Option<Vec<TxFlow>> {
    if response.len() < RESPONSE_HEADER_SIZE + 2 {
        return None;
    }
    if parse_result_code(response)? != RESULT_CODE_SUCCESS {
        return None;
    }
    let body = &response[RESPONSE_HEADER_SIZE..];
    let active_count = body[1] as usize;

    let mut flows = Vec::new();
    let mut offset = 2;
    for _ in 0..active_count {
        let Some(flow_number) = read_u16(body, offset) else {
            break;
        };
        let Some(record_pointer) = read_u16(body, offset + 2) else {
            break;
        };
        offset += 4;

        let Some(record_offset) = (record_pointer as usize).checked_sub(RESPONSE_HEADER_SIZE) else {
            continue;
        };
        if record_offset >= body.len() {
            continue;
        }
        if let Some(flow) = parse_flow_record(body, record_offset, flow_number) {
            flows.push(flow);
        }
    }
    Some(flows)
}

fn parse_flow_record(body: &[u8], offset: usize, flow_number: u16) -> Option<TxFlow> {
    if offset + FLOW_RECORD_MINIMUM_SIZE > body.len() {
        return None;
    }
    let flow_type_code = u16_at(body, offset + FLOW_RECORD_FLOW_TYPE);
    let sample_rate = read_u32(body, offset + FLOW_RECORD_SAMPLE_RATE)?;
    let encoding = u16_at(body, offset + FLOW_RECORD_ENCODING);
    let frames_per_packet = u16_at(body, offset + FLOW_RECORD_FRAMES_PER_PACKET);

    let (flow_type, channel_count, channels) = if flow_type_code == FLOW_TYPE_MULTICAST {
        let (channel_count, channels) = flow_channel_list(body, offset);
        ("multicast".to_owned(), channel_count, channels)
    } else {
        (format!("0x{flow_type_code:04X}"), 0, Vec::new())
    };

    Some(TxFlow {
        flow_number,
        flow_type,
        sample_rate,
        encoding,
        frames_per_packet,
        channel_count,
        channels,
    })
}

fn flow_channel_list(body: &[u8], record_offset: usize) -> (u16, Vec<u16>) {
    let Some(channel_count) = read_u16(body, record_offset + FLOW_RECORD_CHANNEL_COUNT) else {
        return (0, Vec::new());
    };
    let mut channels = Vec::new();
    let mut channel_offset = record_offset + FLOW_RECORD_CHANNELS;
    for _ in 0..channel_count {
        let Some(channel_number) = read_u16(body, channel_offset) else {
            break;
        };
        channels.push(channel_number);
        channel_offset += 2;
    }
    (channel_count, channels)
}

pub fn parse_bluetooth_status(response: &[u8]) -> Option<BluetoothStatus> {
    if response.len() < 50 {
        return None;
    }
    if response[36] != 0x12 || response[38] != 0x0a {
        return None;
    }

    let field1_len = response[39] as usize;
    let mut position = 40 + field1_len;

    if position < response.len() && response[position] == 0x18 {
        position += 1;
        while position < response.len() && response[position] & 0x80 != 0 {
            position += 1;
        }
        position += 1;
    }

    if position >= response.len() || response[position] != 0x22 {
        return None;
    }

    position += 1;
    if position >= response.len() {
        return None;
    }

    let field4_len = response[position] as usize;
    position += 1;
    let field4_end = position + field4_len;
    if field4_end > response.len() {
        return None;
    }

    Some(find_bluetooth_name(&response[position..field4_end]))
}

fn find_bluetooth_name(data: &[u8]) -> BluetoothStatus {
    if data.len() < 5 || data[0] != 0x0a || data[2] != 0x12 {
        return BluetoothStatus::disconnected();
    }

    let Some(marker) = data.windows(3).position(|window| window == [0x08, 0x01, 0x12]) else {
        return BluetoothStatus::disconnected();
    };

    let name_length_position = marker + 3;
    let Some(&name_length) = data.get(name_length_position) else {
        return BluetoothStatus::disconnected();
    };
    let name_start = name_length_position + 1;
    let Some(name_bytes) = data.get(name_start..name_start + name_length as usize) else {
        return BluetoothStatus::disconnected();
    };

    match std::str::from_utf8(name_bytes) {
        Ok(name) => BluetoothStatus {
            connected: true,
            device_name: Some(name.to_owned()),
        },
        Err(_) => BluetoothStatus::disconnected(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_name_strips_header_and_terminator() {
        let mut response = vec![0x27, 0xFF, 0x00, 0x16, 0x9e, 0x7f, 0x10, 0x02, 0x00, 0x01];
        response.extend_from_slice(b"avio-aes3-1\x00");
        assert_eq!(parse_device_name(&response).as_deref(), Some("avio-aes3-1"));
    }

    #[test]
    fn device_settings_decodes_sample_rate_and_latency() {
        let mut body = vec![0x00, 0x02];
        body.extend_from_slice(&DEVICE_SETTINGS_INFO_SAMPLE_RATE.to_be_bytes());
        body.extend_from_slice(&20u16.to_be_bytes());
        body.extend_from_slice(&DEVICE_SETTINGS_INFO_LATENCY.to_be_bytes());
        body.extend_from_slice(&24u16.to_be_bytes());
        body.extend_from_slice(&48000u32.to_be_bytes());
        body.extend_from_slice(&1_000_000u32.to_be_bytes());
        let mut response = vec![0u8; 10];
        response.extend_from_slice(&body);

        let settings = parse_device_settings(&response).unwrap();
        assert_eq!(settings.sample_rate, Some(48000));
        assert_eq!(settings.latency_us, Some(1_000_000));
    }

    fn flow_query_response() -> Vec<u8> {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE];
        response[8..10].copy_from_slice(&RESULT_CODE_SUCCESS.to_be_bytes());
        response.extend_from_slice(&[0x00, 0x01]);
        response.extend_from_slice(&1u16.to_be_bytes());
        response.extend_from_slice(&16u16.to_be_bytes());

        let mut record = vec![0u8; 28];
        record[2..4].copy_from_slice(&FLOW_TYPE_MULTICAST.to_be_bytes());
        record[4..8].copy_from_slice(&48000u32.to_be_bytes());
        record[8..10].copy_from_slice(&16u16.to_be_bytes());
        record[10..12].copy_from_slice(&16u16.to_be_bytes());
        record[22..24].copy_from_slice(&2u16.to_be_bytes());
        record[24..26].copy_from_slice(&1u16.to_be_bytes());
        record[26..28].copy_from_slice(&2u16.to_be_bytes());
        response.extend_from_slice(&record);
        response
    }

    #[test]
    fn tx_flows_parser_decodes_multicast_record() {
        let flows = parse_tx_flows(&flow_query_response()).unwrap();
        assert_eq!(flows.len(), 1);
        let flow = &flows[0];
        assert_eq!(flow.flow_number, 1);
        assert_eq!(flow.flow_type, "multicast");
        assert_eq!(flow.sample_rate, 48000);
        assert_eq!(flow.encoding, 16);
        assert_eq!(flow.frames_per_packet, 16);
        assert_eq!(flow.channel_count, 2);
        assert_eq!(flow.channels, vec![1, 2]);
    }

    #[test]
    fn tx_flows_parser_rejects_failure_result_code() {
        let mut response = flow_query_response();
        response[8..10].copy_from_slice(&0x0600u16.to_be_bytes());
        assert_eq!(parse_tx_flows(&response), None);
    }

    #[test]
    fn result_code_reads_header_field() {
        assert_eq!(parse_result_code(&flow_query_response()), Some(RESULT_CODE_SUCCESS));
        assert_eq!(parse_result_code(&[0u8; 4]), None);
    }

    #[test]
    fn aes67_byte_maps_to_bool() {
        let mut response = vec![0u8; 0x54];
        response[0x53] = 0x03;
        assert_eq!(parse_aes67_configured(&response), Some(true));
        response[0x53] = 0x01;
        assert_eq!(parse_aes67_configured(&response), Some(false));
        response[0x53] = 0x00;
        assert_eq!(parse_aes67_configured(&response), None);
    }
}
