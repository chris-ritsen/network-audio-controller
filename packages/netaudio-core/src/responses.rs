use std::collections::HashSet;

use serde::Serialize;

use crate::bytes::{read_u16, read_u32, string_at_pointer, u16_at};
use crate::commands::{
    FLOW_TYPE_MULTICAST, OPCODE_CREATE_TX_FLOW, OPCODE_CREATE_TX_FLOW_2809, OPCODE_DELETE_TX_FLOW,
    OPCODE_DELETE_TX_FLOW_2809, OPCODE_DEVICE_INFO, OPCODE_DEVICE_NAME, OPCODE_DEVICE_SETTINGS,
    OPCODE_DEVICE_SETTINGS_SET, OPCODE_QUERY_TX_FLOWS, OPCODE_QUERY_TX_FLOWS_2809,
    OPCODE_RX_CHANNEL_NAME_SET, OPCODE_SUBSCRIPTION_ADD, OPCODE_SUBSCRIPTION_REMOVE,
    OPCODE_TX_CHANNEL_NAME_SET, PROTOCOL_AES67_CONFIG, PROTOCOL_DANTE_FLOW,
    PROTOCOL_DANTE_FLOW_2801,
};
use crate::protocol::{
    common_arc_protocol_opcodes, conmon_opcode, device_settings_arc_protocol_opcodes,
    is_common_arc_protocol, response_envelope, validate_conmon_envelope,
    validate_response_envelope, OPCODE_CHANNEL_COUNT, OPCODE_DEVICE_NAME_SET, OPCODE_RX_CHANNELS,
    OPCODE_TX_CHANNEL_INFO, OPCODE_TX_CHANNEL_NAMES,
};

pub use crate::protocol::{RESPONSE_HEADER_SIZE, RESULT_CODE_SUCCESS};

const CONMON_OPCODE_BLUETOOTH_STATUS: u16 = 0x100E;

const FLOW_RECORD_FIXED_SIZE: usize = 16;
const FLOW_RECORD_FLOW_TYPE: usize = 2;
const FLOW_RECORD_SAMPLE_RATE: usize = 4;
const FLOW_RECORD_ENCODING: usize = 8;
const FLOW_RECORD_FRAMES_PER_PACKET: usize = 12;
const FLOW_RECORD_CHANNEL_COUNT: usize = 14;
const FLOW_TYPE_UNICAST: u16 = 0x0011;

pub const DEVICE_SETTINGS_INFO_SAMPLE_RATE: u16 = 0x8020;
pub const DEVICE_SETTINGS_INFO_AES67_CONFIGURED: u16 = 0x0063;
pub const DEVICE_SETTINGS_INFO_DEFAULT_LATENCY_NS: u16 = 0x8204;
pub const DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS: u16 = 0x8205;
pub const DEVICE_SETTINGS_INFO_LATENCY_NS: u16 = DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS;
pub const DEVICE_SETTINGS_INFO_ACTIVE_LATENCY_NS: u16 = 0x8301;
pub const DEVICE_SETTINGS_INFO_MAX_LATENCY_NS: u16 = 0x8302;
pub const DEVICE_SETTINGS_INFO_MIN_LATENCY_NS: u16 = 0x8306;

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
    pub latency_ns: Option<u32>,
    pub configured_latency_ns: Option<u32>,
    pub active_latency_ns: Option<u32>,
    pub default_latency_ns: Option<u32>,
    pub min_latency_ns: Option<u32>,
    pub max_latency_ns: Option<u32>,
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
    let body = validate_response_envelope(
        response,
        &common_arc_protocol_opcodes(OPCODE_DEVICE_NAME),
        &[RESULT_CODE_SUCCESS],
    )?
    .body;
    let (&terminator, name) = body.split_last()?;
    if terminator != 0 || name.contains(&0) {
        return None;
    }
    std::str::from_utf8(name).ok().map(str::to_owned)
}

pub fn parse_device_info(response: &[u8]) -> Option<DeviceInfo> {
    let body = validate_response_envelope(
        response,
        &common_arc_protocol_opcodes(OPCODE_DEVICE_INFO),
        &[RESULT_CODE_SUCCESS],
    )?
    .body;
    if body.len() < 18 {
        return None;
    }

    let minimum_string_pointer = RESPONSE_HEADER_SIZE + 18;
    let required_string = |pointer: u16| -> Option<String> {
        (usize::from(pointer) >= minimum_string_pointer)
            .then(|| string_at_pointer(response, pointer))?
    };

    let code_pointer = read_u16(body, 6)?;
    let port_pointer = read_u16(body, 8)?;
    let model_pointer = read_u16(body, 12)?;
    let display_pointer = read_u16(body, 14)?;

    let (model_name, display_name) = if (model_pointer == 0 && display_pointer == 0)
        || usize::from(model_pointer) < minimum_string_pointer
    {
        (String::new(), String::new())
    } else {
        (
            required_string(model_pointer)?,
            required_string(display_pointer)?,
        )
    };

    Some(DeviceInfo {
        model_name,
        display_name,
        model_code: required_string(code_pointer)?,
        port: required_string(port_pointer)?,
    })
}

pub fn parse_device_settings(response: &[u8]) -> Option<DeviceSettings> {
    let body = validate_response_envelope(
        response,
        &device_settings_arc_protocol_opcodes(OPCODE_DEVICE_SETTINGS),
        &[RESULT_CODE_SUCCESS],
    )?
    .body;

    let record_count = usize::from(*body.get(1)?);
    let record_bytes = record_count.checked_mul(4)?;
    let values_offset = 2usize.checked_add(record_bytes)?;
    body.get(..values_offset)?;
    let mut settings = DeviceSettings {
        sample_rate: None,
        latency_ns: None,
        configured_latency_ns: None,
        active_latency_ns: None,
        default_latency_ns: None,
        min_latency_ns: None,
        max_latency_ns: None,
        info_codes: Vec::new(),
    };
    let mut info_codes = HashSet::with_capacity(record_count);

    for index in 0..record_count {
        let offset = 2 + index * 4;
        let info_code = u16_at(body, offset);
        let value_pointer = u16_at(body, offset + 2);
        if info_code == 0 {
            continue;
        }
        if !info_codes.insert(info_code) {
            return None;
        }

        if info_code & 0x8000 == 0 {
            continue;
        }
        let value_pointer = usize::from(value_pointer);
        let minimum_value_pointer = RESPONSE_HEADER_SIZE.checked_add(values_offset)?;
        if value_pointer < minimum_value_pointer {
            return None;
        }
        let value_offset = value_pointer.checked_sub(RESPONSE_HEADER_SIZE)?;
        let value = read_u32(body, value_offset)?;

        settings.info_codes.push((info_code, value));
        match info_code {
            DEVICE_SETTINGS_INFO_SAMPLE_RATE => settings.sample_rate = Some(value),
            DEVICE_SETTINGS_INFO_DEFAULT_LATENCY_NS => settings.default_latency_ns = Some(value),
            DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS => {
                settings.configured_latency_ns = Some(value)
            }
            DEVICE_SETTINGS_INFO_ACTIVE_LATENCY_NS => settings.active_latency_ns = Some(value),
            DEVICE_SETTINGS_INFO_MIN_LATENCY_NS => settings.min_latency_ns = Some(value),
            DEVICE_SETTINGS_INFO_MAX_LATENCY_NS => settings.max_latency_ns = Some(value),
            _ => {}
        }
    }

    settings.latency_ns = settings
        .active_latency_ns
        .or(settings.configured_latency_ns);

    Some(settings)
}

pub fn parse_aes67_configured(response: &[u8]) -> Option<Option<bool>> {
    let body = validate_response_envelope(
        response,
        &device_settings_arc_protocol_opcodes(OPCODE_DEVICE_SETTINGS),
        &[RESULT_CODE_SUCCESS],
    )?
    .body;
    let record_count = usize::from(*body.get(1)?);
    let record_bytes = record_count.checked_mul(4)?;
    let records_end = 2usize.checked_add(record_bytes)?;
    body.get(..records_end)?;
    let mut configured = None;
    for record_index in 0..record_count {
        let record_offset = 2 + record_index * 4;
        let info_code = u16_at(body, record_offset);
        let inline_value = u16_at(body, record_offset + 2);
        if info_code == 0 {
            continue;
        }
        if info_code != DEVICE_SETTINGS_INFO_AES67_CONFIGURED {
            continue;
        }
        if configured.is_some() {
            return None;
        }
        configured = match inline_value {
            0x0003 => Some(true),
            0x0001 => Some(false),
            _ => return None,
        };
    }
    Some(configured)
}

fn conmon_string(data: &[u8], start: usize, end: usize) -> Option<String> {
    let raw = data.get(start..end)?;
    let null_position = raw.iter().position(|&byte| byte == 0).unwrap_or(raw.len());
    let raw = &raw[..null_position];
    let text = std::str::from_utf8(raw).ok()?.trim().to_owned();
    if text
        .chars()
        .all(|character| !character.is_control() || character == ' ')
    {
        Some(text)
    } else {
        None
    }
}

pub fn parse_make_model(data: &[u8]) -> Option<MakeModel> {
    validate_conmon_envelope(data, CONMON_OPCODE_MAKE_MODEL_RESPONSE)?;
    let manufacturer = conmon_string(data, CONMON_MANUFACTURER_OFFSET, CONMON_MANUFACTURER_END)?;
    let product_name = conmon_string(data, CONMON_PRODUCT_NAME_OFFSET, CONMON_PRODUCT_NAME_END)?;
    let version = data.get(CONMON_PRODUCT_VERSION_OFFSET..CONMON_PRODUCT_VERSION_END)?;
    let mut product_version = String::new();
    if version.iter().any(|&byte| byte != 0) {
        let (major, minor, build) = (version[0], version[1], version[3]);
        product_version = format!("{major}.{minor}.{build}");
    }
    Some(MakeModel {
        manufacturer,
        product_name,
        product_version,
    })
}

pub fn parse_dante_model(data: &[u8]) -> Option<DanteModel> {
    validate_conmon_envelope(data, CONMON_OPCODE_DANTE_MODEL_RESPONSE)?;
    Some(DanteModel {
        board_codename: conmon_string(
            data,
            CONMON_BOARD_CODENAME_OFFSET,
            CONMON_BOARD_CODENAME_END,
        )?,
        board_name: conmon_string(data, CONMON_BOARD_NAME_OFFSET, CONMON_BOARD_NAME_END)?,
    })
}

pub fn parse_result_code(response: &[u8]) -> Option<u16> {
    let envelope = response_envelope(response)?;
    let common_opcode = matches!(
        envelope.opcode,
        OPCODE_CHANNEL_COUNT
            | OPCODE_DEVICE_NAME_SET
            | OPCODE_DEVICE_NAME
            | OPCODE_DEVICE_INFO
            | OPCODE_DEVICE_SETTINGS
            | OPCODE_DEVICE_SETTINGS_SET
            | OPCODE_TX_CHANNEL_INFO
            | OPCODE_TX_CHANNEL_NAMES
            | OPCODE_TX_CHANNEL_NAME_SET
            | OPCODE_RX_CHANNELS
            | OPCODE_RX_CHANNEL_NAME_SET
            | OPCODE_SUBSCRIPTION_ADD
            | OPCODE_SUBSCRIPTION_REMOVE
    );
    let flow_opcode = match envelope.protocol_id {
        PROTOCOL_DANTE_FLOW | PROTOCOL_DANTE_FLOW_2801 => matches!(
            envelope.opcode,
            OPCODE_QUERY_TX_FLOWS | OPCODE_CREATE_TX_FLOW | OPCODE_DELETE_TX_FLOW
        ),
        PROTOCOL_AES67_CONFIG => matches!(
            envelope.opcode,
            OPCODE_QUERY_TX_FLOWS_2809 | OPCODE_CREATE_TX_FLOW_2809 | OPCODE_DELETE_TX_FLOW_2809
        ),
        _ => false,
    };
    let valid = (is_common_arc_protocol(envelope.protocol_id) && common_opcode) || flow_opcode;
    valid.then_some(envelope.result_code)
}

pub fn parse_tx_flows(response: &[u8]) -> Option<Vec<TxFlow>> {
    let envelope = validate_response_envelope(
        response,
        &[
            (PROTOCOL_DANTE_FLOW, OPCODE_QUERY_TX_FLOWS),
            (PROTOCOL_DANTE_FLOW_2801, OPCODE_QUERY_TX_FLOWS),
        ],
        &[RESULT_CODE_SUCCESS, crate::protocol::RESULT_CODE_MORE_PAGES],
    )?;
    let body = envelope.body;
    let maximum_records = usize::from(*body.first()?);
    let active_count = usize::from(*body.get(1)?);
    if !(1..=32).contains(&maximum_records) || active_count > maximum_records {
        return None;
    }
    let pointer_table_size = maximum_records.checked_mul(2)?;
    let records_start = 2usize.checked_add(pointer_table_size)?;
    body.get(..records_start)?;

    let mut record_offsets = Vec::with_capacity(active_count);
    for index in 0..maximum_records {
        let record_pointer = read_u16(body, 2 + index * 2)?;
        if record_pointer == 0 {
            continue;
        }
        let record_offset = usize::from(record_pointer).checked_sub(RESPONSE_HEADER_SIZE)?;
        if record_offset < records_start
            || record_offsets
                .last()
                .is_some_and(|previous_offset| record_offset <= *previous_offset)
        {
            return None;
        }
        record_offsets.push(record_offset);
    }
    if record_offsets.len() != active_count {
        return None;
    }

    let mut flows = Vec::with_capacity(active_count);
    let mut flow_numbers = HashSet::with_capacity(active_count);
    for (index, record_offset) in record_offsets.iter().copied().enumerate() {
        let record_end = record_offsets.get(index + 1).copied().unwrap_or(body.len());
        let flow = parse_flow_record(body, record_offset, record_end)?;
        if !(1..=32).contains(&flow.flow_number) || !flow_numbers.insert(flow.flow_number) {
            return None;
        }
        flows.push(flow);
    }
    Some(flows)
}

fn parse_flow_record(body: &[u8], offset: usize, record_end: usize) -> Option<TxFlow> {
    let fixed_end = offset.checked_add(FLOW_RECORD_FIXED_SIZE)?;
    if fixed_end > record_end {
        return None;
    }
    body.get(offset..record_end)?;
    let flow_number = read_u16(body, offset)?;
    let flow_type_code = u16_at(body, offset + FLOW_RECORD_FLOW_TYPE);
    let sample_rate = read_u32(body, offset + FLOW_RECORD_SAMPLE_RATE)?;
    let encoding = u16::try_from(read_u32(body, offset + FLOW_RECORD_ENCODING)?).ok()?;
    let frames_per_packet = u16_at(body, offset + FLOW_RECORD_FRAMES_PER_PACKET);
    let channel_count = read_u16(body, offset + FLOW_RECORD_CHANNEL_COUNT)?;
    if channel_count == 0 {
        return None;
    }

    let (flow_type, channels) = match flow_type_code {
        FLOW_TYPE_MULTICAST => (
            "multicast".to_owned(),
            flow_channel_list(body, offset, record_end, frames_per_packet, channel_count)?,
        ),
        FLOW_TYPE_UNICAST => ("unicast".to_owned(), Vec::new()),
        _ => return None,
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

fn flow_channel_list(
    body: &[u8],
    record_offset: usize,
    record_end: usize,
    frames_per_packet: u16,
    channel_count: u16,
) -> Option<Vec<u16>> {
    let channel_bytes = usize::from(channel_count).checked_mul(2)?;
    let variable_prefix_bytes = usize::from(frames_per_packet).checked_mul(2)?;
    let channels_start = record_offset
        .checked_add(FLOW_RECORD_FIXED_SIZE)?
        .checked_add(variable_prefix_bytes)?;
    let channels_end = channels_start.checked_add(channel_bytes)?;
    if channels_end > record_end {
        return None;
    }
    body.get(channels_start..channels_end)?;
    let mut channels = Vec::with_capacity(usize::from(channel_count));
    let mut seen = HashSet::with_capacity(usize::from(channel_count));
    let mut channel_offset = channels_start;
    for _ in 0..channel_count {
        let channel_number = read_u16(body, channel_offset)?;
        if channel_number == 0 || !seen.insert(channel_number) {
            return None;
        }
        channels.push(channel_number);
        channel_offset += 2;
    }
    Some(channels)
}

pub fn parse_bluetooth_status(response: &[u8]) -> Option<BluetoothStatus> {
    validate_conmon_envelope(response, CONMON_OPCODE_BLUETOOTH_STATUS)?;
    if response.len() < 50 {
        return None;
    }
    if response[36] != 0x12 || response[38] != 0x0a {
        return None;
    }

    let field1_len = usize::from(response[39]);
    let mut position = 40usize.checked_add(field1_len)?;

    if position < response.len() && response[position] == 0x18 {
        position = position.checked_add(1)?;
        while position < response.len() && response[position] & 0x80 != 0 {
            position = position.checked_add(1)?;
        }
        position = position.checked_add(1)?;
    }

    if position >= response.len() || response[position] != 0x22 {
        return None;
    }

    position = position.checked_add(1)?;
    if position >= response.len() {
        return None;
    }

    let field4_len = usize::from(response[position]);
    position = position.checked_add(1)?;
    let field4_end = position.checked_add(field4_len)?;
    if field4_end != response.len() {
        return None;
    }

    parse_bluetooth_payload(&response[position..field4_end])
}

fn length_delimited_payload(data: &[u8], tag: u8) -> Option<&[u8]> {
    if data.first().copied()? != tag {
        return None;
    }
    let length = usize::from(*data.get(1)?);
    (length.checked_add(2)? == data.len()).then_some(&data[2..])
}

fn parse_bluetooth_payload(data: &[u8]) -> Option<BluetoothStatus> {
    let level_one = length_delimited_payload(data, 0x0A)?;
    let level_two = length_delimited_payload(level_one, 0x12)?;
    let state = length_delimited_payload(level_two, 0x0A)?;
    if state.get(0..2)? != [0x08, 0x02] {
        if state.get(0..2)? != [0x08, 0x01] {
            return None;
        }
        let name_payload = length_delimited_payload(state.get(2..)?, 0x12)?;
        if name_payload.is_empty() {
            return None;
        }
        let name = std::str::from_utf8(name_payload).ok()?;
        return Some(BluetoothStatus {
            connected: true,
            device_name: Some(name.to_owned()),
        });
    }

    (state.len() == 2).then(BluetoothStatus::disconnected)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConmonOpcode {
    pub opcode: Option<u16>,
}

pub fn parse_conmon_opcode(data: &[u8]) -> Option<ConmonOpcode> {
    Some(ConmonOpcode {
        opcode: Some(conmon_opcode(data)?),
    })
}

pub const CONMON_OPCODE_INTERFACE_STATUS: u16 = 0x0011;
pub const CONMON_OPCODE_MAKE_MODEL_RESPONSE: u16 = 0x00C0;
pub const CONMON_OPCODE_DANTE_MODEL_RESPONSE: u16 = 0x0060;
pub const CONMON_OPCODE_SAMPLE_RATE_STATUS: u16 = 0x0080;
pub const CONMON_OPCODE_ENCODING_STATUS: u16 = 0x0082;
pub const CONMON_OPCODE_GAIN_STATUS: u16 = 0x100B;
pub const CONMON_OPCODE_AES67_CURRENT_NEW: u16 = 0x1007;
pub const CONMON_OPCODE_PTP_CLOCK_STATUS: u16 = 0x0020;

const CONMON_SUPPORTED_SAMPLE_RATE_COUNT_OFFSET: usize = 0x22;
const CONMON_CURRENT_SAMPLE_RATE_OFFSET: usize = 0x24;
const CONMON_SUPPORTED_SAMPLE_RATES_OFFSET: usize = 0x30;
const CONMON_SUPPORTED_ENCODING_COUNT_OFFSET: usize = 0x22;
const CONMON_CURRENT_ENCODING_OFFSET: usize = 0x24;
const CONMON_SUPPORTED_ENCODINGS_OFFSET: usize = 0x30;
const CONMON_GAIN_DIRECTION_OFFSET: usize = 0x28;
const CONMON_GAIN_CHANNEL_COUNT_OFFSET: usize = 0x2A;
const CONMON_GAIN_LEVELS_OFFSET: usize = 0x30;
const CONMON_GAIN_INPUT_DIRECTION: u16 = 0x0102;
const CONMON_GAIN_OUTPUT_DIRECTION: u16 = 0x0201;
const CONMON_PREFERRED_LEADER_OFFSET: usize = 0x26;
const CONMON_PTP_V1_ROLE_OFFSET: usize = 0x48;
const CONMON_AES67_CURRENT_NEW_OFFSET: usize = 0x21;
const CONMON_INTERFACE_COUNT_OFFSET: usize = 0x20;
const CONMON_INTERFACE_LINK_SPEED_OFFSET: usize = 0x24;
const CONMON_INTERFACE_RECORDS_OFFSET: usize = 0x28;
const CONMON_INTERFACE_RECORD_SIZE: usize = 20;
const CONMON_INTERFACE_CONFIGURED_RECORD_SIZE: usize = 24;
const CONMON_INTERFACE_CONFIGURED_RECORD_STRIDE: usize = 28;
const CONMON_INTERFACE_REBOOT_FLAG_OFFSET: usize = 0x48;
const CONMON_INTERFACE_PENDING_STATIC_OFFSET: usize = 0x4C;
const CONMON_INTERFACE_MINIMUM_SIZE: usize = 0x40;

const INTERFACE_MODE_DYNAMIC: u16 = 0x0001;
const INTERFACE_MODE_STATIC: u16 = 0x0003;
const INTERFACE_REBOOT_PENDING_DYNAMIC: u16 = 0x0004;
const INTERFACE_REBOOT_PENDING_STATIC: u16 = 0x0006;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SampleRateStatus {
    pub current_sample_rate: u32,
    pub supported_sample_rates: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EncodingStatus {
    pub current_encoding: u32,
    pub supported_encodings: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GainStatus {
    pub device_type: String,
    pub channel_levels: Vec<u32>,
}

fn parse_supported_u32_values(
    data: &[u8],
    expected_opcode: u16,
    supported_value_count_offset: usize,
    current_value_offset: usize,
    supported_values_offset: usize,
) -> Option<(u32, Vec<u32>)> {
    validate_conmon_envelope(data, expected_opcode)?;
    let supported_value_count = usize::from(read_u16(data, supported_value_count_offset)?);
    let current_value = read_u32(data, current_value_offset)?;
    let supported_values_byte_length = supported_value_count.checked_mul(4)?;
    let supported_values_end = supported_values_offset.checked_add(supported_values_byte_length)?;
    data.get(supported_values_offset..supported_values_end)?;

    let mut supported_values = Vec::with_capacity(supported_value_count);
    for supported_value_index in 0..supported_value_count {
        let supported_value_offset =
            supported_values_offset.checked_add(supported_value_index.checked_mul(4)?)?;
        supported_values.push(read_u32(data, supported_value_offset)?);
    }

    Some((current_value, supported_values))
}

pub fn parse_sample_rate_status(data: &[u8]) -> Option<SampleRateStatus> {
    let (current_sample_rate, supported_sample_rates) = parse_supported_u32_values(
        data,
        CONMON_OPCODE_SAMPLE_RATE_STATUS,
        CONMON_SUPPORTED_SAMPLE_RATE_COUNT_OFFSET,
        CONMON_CURRENT_SAMPLE_RATE_OFFSET,
        CONMON_SUPPORTED_SAMPLE_RATES_OFFSET,
    )?;

    Some(SampleRateStatus {
        current_sample_rate,
        supported_sample_rates,
    })
}

pub fn parse_encoding_status(data: &[u8]) -> Option<EncodingStatus> {
    let (current_encoding, supported_encodings) = parse_supported_u32_values(
        data,
        CONMON_OPCODE_ENCODING_STATUS,
        CONMON_SUPPORTED_ENCODING_COUNT_OFFSET,
        CONMON_CURRENT_ENCODING_OFFSET,
        CONMON_SUPPORTED_ENCODINGS_OFFSET,
    )?;

    Some(EncodingStatus {
        current_encoding,
        supported_encodings,
    })
}

pub fn parse_gain_status(data: &[u8]) -> Option<GainStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_GAIN_STATUS)?;
    if data.get(25).copied()? != 0x27
        || data.get(28..40)?
            != [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x10,
            ]
        || data.get(44..48)? != [0x00, 0x04, 0x00, 0x18]
    {
        return None;
    }

    let device_type = match read_u16(data, CONMON_GAIN_DIRECTION_OFFSET)? {
        CONMON_GAIN_INPUT_DIRECTION => "input",
        CONMON_GAIN_OUTPUT_DIRECTION => "output",
        _ => return None,
    };
    let channel_count = usize::from(read_u16(data, CONMON_GAIN_CHANNEL_COUNT_OFFSET)?);
    if channel_count == 0 {
        return None;
    }
    let levels_byte_length = channel_count.checked_mul(4)?;
    let levels_end = CONMON_GAIN_LEVELS_OFFSET.checked_add(levels_byte_length)?;
    data.get(CONMON_GAIN_LEVELS_OFFSET..levels_end)?;

    let mut channel_levels = Vec::with_capacity(channel_count);
    for channel_index in 0..channel_count {
        let level_offset = CONMON_GAIN_LEVELS_OFFSET.checked_add(channel_index.checked_mul(4)?)?;
        channel_levels.push(read_u32(data, level_offset)?);
    }

    Some(GainStatus {
        device_type: device_type.to_owned(),
        channel_levels,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PtpClockStatus {
    pub preferred_leader: bool,
    pub ptp_v1_role: Option<String>,
}

pub fn parse_ptp_clock_status(data: &[u8]) -> Option<PtpClockStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_PTP_CLOCK_STATUS)?;
    if data.len() <= CONMON_PREFERRED_LEADER_OFFSET {
        return None;
    }
    let preferred_leader = match data[CONMON_PREFERRED_LEADER_OFFSET] {
        0x00 => false,
        0x01 => true,
        _ => return None,
    };
    let ptp_v1_role = if data.len() >= CONMON_PTP_V1_ROLE_OFFSET + 2 {
        match u16::from_be_bytes([
            data[CONMON_PTP_V1_ROLE_OFFSET],
            data[CONMON_PTP_V1_ROLE_OFFSET + 1],
        ]) {
            0x0006 => Some("Leader".to_string()),
            0x0009 => Some("Follower".to_string()),
            _ => None,
        }
    } else {
        None
    };
    Some(PtpClockStatus {
        preferred_leader,
        ptp_v1_role,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Aes67Status {
    pub aes67_current: Option<bool>,
    pub aes67_configured: Option<bool>,
}

pub fn parse_aes67_status(data: &[u8]) -> Option<Aes67Status> {
    validate_conmon_envelope(data, CONMON_OPCODE_AES67_CURRENT_NEW)?;
    if data.len() <= CONMON_AES67_CURRENT_NEW_OFFSET {
        return None;
    }
    let (current, configured) = match data[CONMON_AES67_CURRENT_NEW_OFFSET] {
        0x00 => (Some(false), Some(false)),
        0x01 => (Some(true), Some(false)),
        0x02 => (Some(false), Some(true)),
        0x03 => (Some(true), Some(true)),
        _ => return None,
    };
    Some(Aes67Status {
        aes67_current: current,
        aes67_configured: configured,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceStatus {
    pub link_speed_mbps: u32,
    pub interfaces: Vec<InterfaceStatusEntry>,
    pub reboot_required: bool,
    pub pending_config: Option<PendingInterfaceConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceStatusEntry {
    pub mode: String,
    pub mac_address: String,
    pub ip_address: String,
    pub netmask: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_server: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingInterfaceConfig {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub netmask: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_server: Option<String>,
}

pub fn parse_interface_status(data: &[u8]) -> Option<InterfaceStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_INTERFACE_STATUS)?;
    if data.len() < CONMON_INTERFACE_MINIMUM_SIZE {
        return None;
    }

    let interface_count = read_u16(data, CONMON_INTERFACE_COUNT_OFFSET)?;
    if interface_count > 8 {
        return None;
    }
    let link_speed_mbps = read_u32(data, CONMON_INTERFACE_LINK_SPEED_OFFSET)?;
    let mut interfaces = Vec::with_capacity(usize::from(interface_count));
    let mut mac_addresses = HashSet::with_capacity(usize::from(interface_count));
    let mut offset = CONMON_INTERFACE_RECORDS_OFFSET;

    for _ in 0..interface_count {
        data.get(offset..offset.checked_add(CONMON_INTERFACE_RECORD_SIZE)?)?;

        let mode_value = read_u16(data, offset)?;
        let (mode, configured) = match mode_value {
            INTERFACE_MODE_DYNAMIC => ("dynamic".to_owned(), true),
            INTERFACE_MODE_STATIC => ("static".to_owned(), true),
            value => (format!("unknown(0x{value:04X})"), false),
        };
        let (record_size, record_stride) = if configured {
            (
                CONMON_INTERFACE_CONFIGURED_RECORD_SIZE,
                CONMON_INTERFACE_CONFIGURED_RECORD_STRIDE,
            )
        } else {
            (CONMON_INTERFACE_RECORD_SIZE, CONMON_INTERFACE_RECORD_SIZE)
        };
        data.get(offset..offset.checked_add(record_size)?)?;

        let mac = &data[offset + 2..offset + 8];
        let mac_address = format!(
            "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
            mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
        );
        if !mac_addresses.insert(mac_address.clone()) {
            return None;
        }
        let ip_address = ipv4_at(data, offset + 8)?;
        let netmask = ipv4_at(data, offset + 12)?;
        let (gateway, dns_server) = match mode_value {
            INTERFACE_MODE_DYNAMIC => (
                Some(ipv4_at(data, offset + 16)?),
                Some(ipv4_at(data, offset + 20)?),
            ),
            INTERFACE_MODE_STATIC => (
                Some(ipv4_at(data, offset + 20)?),
                Some(ipv4_at(data, offset + 16)?),
            ),
            _ => (None, None),
        };

        interfaces.push(InterfaceStatusEntry {
            mode,
            mac_address,
            ip_address,
            netmask,
            gateway,
            dns_server,
        });
        offset = offset.checked_add(record_stride)?;
    }

    let reboot_flag = if interface_count == 1 {
        read_u16(data, CONMON_INTERFACE_REBOOT_FLAG_OFFSET)?
    } else {
        0
    };
    let pending_config = match reboot_flag {
        INTERFACE_REBOOT_PENDING_DYNAMIC => Some(PendingInterfaceConfig {
            mode: "dynamic".to_owned(),
            ip_address: None,
            netmask: None,
            gateway: None,
            dns_server: None,
        }),
        INTERFACE_REBOOT_PENDING_STATIC
            if data.len() >= CONMON_INTERFACE_PENDING_STATIC_OFFSET + 16 =>
        {
            Some(PendingInterfaceConfig {
                mode: "static".to_owned(),
                ip_address: Some(ipv4_at(data, CONMON_INTERFACE_PENDING_STATIC_OFFSET)?),
                netmask: Some(ipv4_at(data, CONMON_INTERFACE_PENDING_STATIC_OFFSET + 4)?),
                dns_server: Some(ipv4_at(data, CONMON_INTERFACE_PENDING_STATIC_OFFSET + 8)?),
                gateway: Some(ipv4_at(data, CONMON_INTERFACE_PENDING_STATIC_OFFSET + 12)?),
            })
        }
        0 => None,
        _ => return None,
    };

    Some(InterfaceStatus {
        link_speed_mbps,
        interfaces,
        reboot_required: reboot_flag != 0,
        pending_config,
    })
}

fn ipv4_at(data: &[u8], offset: usize) -> Option<String> {
    let octets: [u8; 4] = data.get(offset..offset + 4)?.try_into().ok()?;
    Some(std::net::Ipv4Addr::from(octets).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PROTOCOL_ID;

    fn stamp_arc_response(response: &mut [u8], protocol: u16, opcode: u16, result: u16) {
        let length = response.len() as u16;
        response[0..2].copy_from_slice(&protocol.to_be_bytes());
        response[2..4].copy_from_slice(&length.to_be_bytes());
        response[6..8].copy_from_slice(&opcode.to_be_bytes());
        response[8..10].copy_from_slice(&result.to_be_bytes());
    }

    fn stamp_conmon_response(response: &mut [u8], opcode: u16) {
        let length = response.len() as u16;
        response[0..2].copy_from_slice(&0xFFFFu16.to_be_bytes());
        response[2..4].copy_from_slice(&length.to_be_bytes());
        response[16..24].copy_from_slice(b"Audinate");
        response[24] = 0x07;
        response[26..28].copy_from_slice(&opcode.to_be_bytes());
    }

    #[test]
    fn device_name_strips_header_and_terminator() {
        let mut response = vec![0x27, 0xFF, 0x00, 0x16, 0x9e, 0x7f, 0x10, 0x02, 0x00, 0x01];
        response.extend_from_slice(b"avio-aes3-1\x00");
        assert_eq!(parse_device_name(&response).as_deref(), Some("avio-aes3-1"));
    }

    fn device_settings_response(values: &[(u16, u32)]) -> Vec<u8> {
        let mut body = vec![0x00, values.len() as u8];
        let first_value_offset = RESPONSE_HEADER_SIZE + 2 + values.len() * 4;
        for (index, (info_code, _)) in values.iter().enumerate() {
            body.extend_from_slice(&info_code.to_be_bytes());
            body.extend_from_slice(&((first_value_offset + index * 4) as u16).to_be_bytes());
        }
        for (_, value) in values.iter().copied() {
            body.extend_from_slice(&value.to_be_bytes());
        }
        let mut response = vec![0u8; 10];
        response.extend_from_slice(&body);
        stamp_arc_response(
            &mut response,
            PROTOCOL_ID,
            OPCODE_DEVICE_SETTINGS,
            RESULT_CODE_SUCCESS,
        );
        response
    }

    #[test]
    fn device_settings_decodes_distinct_latency_fields_as_nanoseconds() {
        let response = device_settings_response(&[
            (DEVICE_SETTINGS_INFO_SAMPLE_RATE, 48_000u32),
            (DEVICE_SETTINGS_INFO_DEFAULT_LATENCY_NS, 1_000_000u32),
            (DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS, 150_000u32),
            (DEVICE_SETTINGS_INFO_ACTIVE_LATENCY_NS, 1_000_000u32),
            (DEVICE_SETTINGS_INFO_MAX_LATENCY_NS, 21_333_334u32),
            (DEVICE_SETTINGS_INFO_MIN_LATENCY_NS, 150_000u32),
        ]);

        let settings = parse_device_settings(&response).unwrap();
        assert_eq!(settings.sample_rate, Some(48_000));
        assert_eq!(settings.default_latency_ns, Some(1_000_000));
        assert_eq!(settings.configured_latency_ns, Some(150_000));
        assert_eq!(settings.active_latency_ns, Some(1_000_000));
        assert_eq!(settings.latency_ns, Some(1_000_000));
        assert_eq!(settings.max_latency_ns, Some(21_333_334));
        assert_eq!(settings.min_latency_ns, Some(150_000));
    }

    #[test]
    fn device_settings_uses_configured_latency_when_active_is_absent() {
        let response =
            device_settings_response(&[(DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS, 250_000)]);
        let settings = parse_device_settings(&response).unwrap();
        assert_eq!(settings.configured_latency_ns, Some(250_000));
        assert_eq!(settings.active_latency_ns, None);
        assert_eq!(settings.latency_ns, Some(250_000));
    }

    fn captured_selective_device_settings_packet_87509() -> Vec<u8> {
        vec![
            0x28, 0x01, 0x00, 0x94, 0x14, 0x21, 0x11, 0x00, 0x00, 0x01, 0x17, 0x17, 0x02, 0x01,
            0x00, 0x01, 0x82, 0x04, 0x00, 0x68, 0x82, 0x05, 0x00, 0x6C, 0x02, 0x10, 0x00, 0x10,
            0x02, 0x11, 0x00, 0x10, 0x00, 0x00, 0x82, 0x18, 0x00, 0x00, 0x82, 0x19, 0x83, 0x01,
            0x00, 0x70, 0x83, 0x02, 0x00, 0x74, 0x83, 0x06, 0x00, 0x78, 0x03, 0x10, 0x00, 0x10,
            0x03, 0x11, 0x00, 0x02, 0x03, 0x03, 0x00, 0x04, 0x80, 0x21, 0x00, 0x7C, 0x00, 0xF0,
            0x00, 0x00, 0x80, 0x60, 0x00, 0x8C, 0x00, 0x22, 0x00, 0x01, 0x00, 0x63, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x65, 0x02, 0x22, 0x13, 0x8C, 0x02, 0x12,
            0x00, 0x30, 0x83, 0x21, 0x00, 0x90, 0x00, 0x0F, 0x42, 0x40, 0x00, 0x0F, 0x42, 0x40,
            0x00, 0x0F, 0x42, 0x40, 0x14, 0x58, 0x55, 0x56, 0x00, 0x03, 0xD0, 0x90, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xEF, 0x45, 0x00, 0x00, 0x00, 0x1E, 0x84, 0x80,
        ]
    }

    #[test]
    fn device_settings_accepts_captured_2801_response() {
        let settings =
            parse_device_settings(&captured_selective_device_settings_packet_87509()).unwrap();
        assert_eq!(settings.default_latency_ns, Some(1_000_000));
        assert_eq!(settings.configured_latency_ns, Some(1_000_000));
        assert_eq!(settings.active_latency_ns, Some(1_000_000));
        assert_eq!(settings.latency_ns, Some(1_000_000));
        assert_eq!(settings.min_latency_ns, Some(250_000));
        assert_eq!(settings.max_latency_ns, Some(341_333_334));
    }

    fn captured_selective_device_settings_packet_9084571() -> Vec<u8> {
        vec![
            0x28, 0x09, 0x00, 0x94, 0x00, 0x10, 0x11, 0x00, 0x00, 0x01, 0x17, 0x17, 0x02, 0x01,
            0x00, 0x01, 0x82, 0x04, 0x00, 0x68, 0x82, 0x05, 0x00, 0x6C, 0x02, 0x10, 0x00, 0x10,
            0x02, 0x11, 0x00, 0x10, 0x00, 0x00, 0x82, 0x18, 0x00, 0x00, 0x82, 0x19, 0x83, 0x01,
            0x00, 0x70, 0x83, 0x02, 0x00, 0x74, 0x83, 0x06, 0x00, 0x78, 0x03, 0x10, 0x00, 0x10,
            0x03, 0x11, 0x00, 0x10, 0x03, 0x03, 0x00, 0x02, 0x80, 0x21, 0x00, 0x7C, 0x00, 0x00,
            0x00, 0xF0, 0x80, 0x60, 0x00, 0x8C, 0x00, 0x22, 0x00, 0x01, 0x00, 0x63, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x65, 0x02, 0x22, 0x13, 0x8C, 0x02, 0x12,
            0x00, 0x30, 0x83, 0x21, 0x00, 0x90, 0x00, 0x0F, 0x42, 0x40, 0x00, 0x0F, 0x42, 0x40,
            0x00, 0x0F, 0x42, 0x40, 0x00, 0xA7, 0x87, 0x5F, 0x00, 0x0F, 0x42, 0x40, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xEF, 0x45, 0x00, 0x00, 0x00, 0x1E, 0x84, 0x80,
        ]
    }

    #[test]
    fn device_settings_accepts_captured_repeated_zero_placeholders() {
        let settings =
            parse_device_settings(&captured_selective_device_settings_packet_9084571()).unwrap();
        assert_eq!(settings.default_latency_ns, Some(1_000_000));
        assert_eq!(settings.configured_latency_ns, Some(1_000_000));
        assert_eq!(settings.active_latency_ns, Some(1_000_000));
        assert_eq!(settings.latency_ns, Some(1_000_000));
        assert_eq!(settings.min_latency_ns, Some(1_000_000));
        assert_eq!(settings.max_latency_ns, Some(10_979_167));
        assert!(settings
            .info_codes
            .iter()
            .all(|(info_code, _)| *info_code != 0));
    }

    #[test]
    fn device_settings_rejects_duplicate_non_placeholder_info_codes() {
        let mut response = captured_selective_device_settings_packet_9084571();
        response[32..34].copy_from_slice(&DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS.to_be_bytes());
        assert_eq!(parse_device_settings(&response), None);
    }

    fn flow_query_response() -> Vec<u8> {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE];
        response.extend_from_slice(&[0x10, 0x02]);
        response.extend_from_slice(&44u16.to_be_bytes());
        response.extend_from_slice(&112u16.to_be_bytes());
        response.extend(std::iter::repeat_n(0, 28));

        let mut unicast_record = vec![0u8; 68];
        unicast_record[0..2].copy_from_slice(&1u16.to_be_bytes());
        unicast_record[2..4].copy_from_slice(&FLOW_TYPE_UNICAST.to_be_bytes());
        unicast_record[4..8].copy_from_slice(&48_000u32.to_be_bytes());
        unicast_record[8..12].copy_from_slice(&24u32.to_be_bytes());
        unicast_record[12..14].copy_from_slice(&1u16.to_be_bytes());
        unicast_record[14..16].copy_from_slice(&1u16.to_be_bytes());
        response.extend_from_slice(&unicast_record);

        let mut multicast_record = vec![0u8; 64];
        multicast_record[0..2].copy_from_slice(&17u16.to_be_bytes());
        multicast_record[2..4].copy_from_slice(&FLOW_TYPE_MULTICAST.to_be_bytes());
        multicast_record[4..8].copy_from_slice(&48_000u32.to_be_bytes());
        multicast_record[8..12].copy_from_slice(&24u32.to_be_bytes());
        multicast_record[12..14].copy_from_slice(&2u16.to_be_bytes());
        multicast_record[14..16].copy_from_slice(&2u16.to_be_bytes());
        multicast_record[20..22].copy_from_slice(&1u16.to_be_bytes());
        multicast_record[22..24].copy_from_slice(&2u16.to_be_bytes());
        response.extend_from_slice(&multicast_record);
        stamp_arc_response(
            &mut response,
            PROTOCOL_DANTE_FLOW,
            OPCODE_QUERY_TX_FLOWS,
            RESULT_CODE_SUCCESS,
        );
        response
    }

    #[test]
    fn tx_flows_parser_decodes_multicast_record() {
        let flows = parse_tx_flows(&flow_query_response()).unwrap();
        assert_eq!(flows.len(), 2);
        assert_eq!(flows[0].flow_number, 1);
        assert_eq!(flows[0].flow_type, "unicast");
        assert_eq!(flows[0].channel_count, 1);
        assert!(flows[0].channels.is_empty());
        let flow = &flows[1];
        assert_eq!(flow.flow_number, 17);
        assert_eq!(flow.flow_type, "multicast");
        assert_eq!(flow.sample_rate, 48_000);
        assert_eq!(flow.encoding, 24);
        assert_eq!(flow.frames_per_packet, 2);
        assert_eq!(flow.channel_count, 2);
        assert_eq!(flow.channels, vec![1, 2]);
    }

    #[test]
    fn tx_flows_parser_uses_variable_channel_offset_in_short_records() {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE];
        response.extend_from_slice(&[0x10, 0x01]);
        response.extend_from_slice(&44u16.to_be_bytes());
        response.extend(std::iter::repeat_n(0, 30));

        let mut record = vec![0u8; 55];
        record[0..2].copy_from_slice(&17u16.to_be_bytes());
        record[2..4].copy_from_slice(&FLOW_TYPE_MULTICAST.to_be_bytes());
        record[4..8].copy_from_slice(&48_000u32.to_be_bytes());
        record[8..12].copy_from_slice(&24u32.to_be_bytes());
        record[12..14].copy_from_slice(&1u16.to_be_bytes());
        record[14..16].copy_from_slice(&2u16.to_be_bytes());
        record[18..20].copy_from_slice(&7u16.to_be_bytes());
        record[20..22].copy_from_slice(&8u16.to_be_bytes());
        response.extend_from_slice(&record);
        stamp_arc_response(
            &mut response,
            PROTOCOL_DANTE_FLOW_2801,
            OPCODE_QUERY_TX_FLOWS,
            RESULT_CODE_SUCCESS,
        );

        let flows = parse_tx_flows(&response).unwrap();
        assert_eq!(flows.len(), 1);
        assert_eq!(flows[0].channels, vec![7, 8]);
    }

    #[test]
    fn tx_flows_parser_accepts_paginated_and_alternate_legacy_protocol_responses() {
        let mut response = flow_query_response();
        stamp_arc_response(
            &mut response,
            PROTOCOL_DANTE_FLOW_2801,
            OPCODE_QUERY_TX_FLOWS,
            crate::protocol::RESULT_CODE_MORE_PAGES,
        );
        assert_eq!(parse_tx_flows(&response).unwrap().len(), 2);

        stamp_arc_response(
            &mut response,
            PROTOCOL_AES67_CONFIG,
            OPCODE_QUERY_TX_FLOWS_2809,
            RESULT_CODE_SUCCESS,
        );
        assert_eq!(parse_tx_flows(&response), None);
    }

    #[test]
    fn tx_flows_parser_rejects_failure_result_code() {
        let mut response = flow_query_response();
        response[8..10].copy_from_slice(&0x0600u16.to_be_bytes());
        assert_eq!(parse_tx_flows(&response), None);
    }

    #[test]
    fn result_code_reads_header_field() {
        assert_eq!(
            parse_result_code(&flow_query_response()),
            Some(RESULT_CODE_SUCCESS)
        );
        assert_eq!(parse_result_code(&[0u8; 4]), None);
    }

    fn aes67_settings_response(records: &[(u16, u16)]) -> Vec<u8> {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE];
        response.extend_from_slice(&[0, records.len() as u8]);
        for (info_code, inline_value) in records {
            response.extend_from_slice(&info_code.to_be_bytes());
            response.extend_from_slice(&inline_value.to_be_bytes());
        }
        stamp_arc_response(
            &mut response,
            PROTOCOL_AES67_CONFIG,
            OPCODE_DEVICE_SETTINGS,
            RESULT_CODE_SUCCESS,
        );
        response
    }

    #[test]
    fn aes67_configured_uses_property_identity_not_record_offset() {
        let enabled = aes67_settings_response(&[
            (0x0211, 0x0004),
            (DEVICE_SETTINGS_INFO_AES67_CONFIGURED, 0x0003),
            (0x0310, 0x0004),
        ]);
        assert_eq!(parse_aes67_configured(&enabled), Some(Some(true)));

        let disabled = aes67_settings_response(&[
            (DEVICE_SETTINGS_INFO_AES67_CONFIGURED, 0x0001),
            (0x0211, 0x0004),
        ]);
        assert_eq!(parse_aes67_configured(&disabled), Some(Some(false)));

        let unsupported = aes67_settings_response(&[(0x0000, 0x0063), (0x0211, 0x0004)]);
        assert_eq!(parse_aes67_configured(&unsupported), Some(None));

        let duplicate = aes67_settings_response(&[
            (DEVICE_SETTINGS_INFO_AES67_CONFIGURED, 0x0001),
            (DEVICE_SETTINGS_INFO_AES67_CONFIGURED, 0x0003),
        ]);
        assert_eq!(parse_aes67_configured(&duplicate), None);
    }

    #[test]
    fn aes67_configured_accepts_captured_2801_device_settings_response() {
        assert_eq!(
            parse_aes67_configured(&captured_selective_device_settings_packet_87509()),
            Some(Some(false))
        );
    }

    #[test]
    fn conmon_opcode_extracts_after_magic() {
        let mut data = vec![0u8; 0x20];
        stamp_conmon_response(&mut data, CONMON_OPCODE_PTP_CLOCK_STATUS);
        assert_eq!(
            parse_conmon_opcode(&data).unwrap().opcode,
            Some(CONMON_OPCODE_PTP_CLOCK_STATUS)
        );
        assert_eq!(parse_conmon_opcode(&[0u8; 0x20]), None);
        assert_eq!(parse_conmon_opcode(&[0u8; 4]), None);
    }

    fn captured_sample_rate_status_packet_28101() -> Vec<u8> {
        vec![
            0xFF, 0xFF, 0x00, 0x48, 0x16, 0x31, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0x08, 0x12, 0x58,
            0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x24, 0x00, 0x80,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x06, 0x00, 0x00, 0xAC, 0x44, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0xAC, 0x44, 0x00, 0x00, 0xBB, 0x80,
            0x00, 0x01, 0x58, 0x88, 0x00, 0x01, 0x77, 0x00, 0x00, 0x02, 0xB1, 0x10, 0x00, 0x02,
            0xEE, 0x00,
        ]
    }

    #[test]
    fn sample_rate_status_parses_captured_packet_28101() {
        let parsed = parse_sample_rate_status(&captured_sample_rate_status_packet_28101()).unwrap();
        assert_eq!(parsed.current_sample_rate, 44_100);
        assert_eq!(
            parsed.supported_sample_rates,
            vec![44_100, 48_000, 88_200, 96_000, 176_400, 192_000]
        );
    }

    #[test]
    fn sample_rate_status_parses_captured_packet_4170820() {
        let data = [
            0xFF, 0xFF, 0x00, 0x34, 0x06, 0x1A, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0x08, 0x12, 0x58,
            0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x24, 0x00, 0x80,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0xBB, 0x80, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0xBB, 0x80,
        ];
        let parsed = parse_sample_rate_status(&data).unwrap();
        assert_eq!(parsed.current_sample_rate, 48_000);
        assert_eq!(parsed.supported_sample_rates, vec![48_000]);
    }

    #[test]
    fn sample_rate_status_parses_captured_packet_9695783() {
        let data = [
            0xFF, 0xFF, 0x00, 0x40, 0xFD, 0x2A, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x53,
            0xEF, 0x37, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x38, 0x00, 0x80,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x04, 0x00, 0x00, 0xBB, 0x80, 0x00, 0x00,
            0xBB, 0x80, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0xAC, 0x44, 0x00, 0x00, 0xBB, 0x80,
            0x00, 0x01, 0x58, 0x88, 0x00, 0x01, 0x77, 0x00,
        ];
        let parsed = parse_sample_rate_status(&data).unwrap();
        assert_eq!(parsed.current_sample_rate, 48_000);
        assert_eq!(
            parsed.supported_sample_rates,
            vec![44_100, 48_000, 88_200, 96_000]
        );
    }

    #[test]
    fn sample_rate_status_rejects_count_exceeding_packet() {
        let mut data = captured_sample_rate_status_packet_28101();
        data[CONMON_SUPPORTED_SAMPLE_RATE_COUNT_OFFSET
            ..CONMON_SUPPORTED_SAMPLE_RATE_COUNT_OFFSET + 2]
            .copy_from_slice(&7u16.to_be_bytes());
        assert_eq!(parse_sample_rate_status(&data), None);
    }

    #[test]
    fn sample_rate_status_preserves_uninterpreted_trailing_bytes() {
        let mut data = captured_sample_rate_status_packet_28101();
        data.extend_from_slice(&[0x12, 0x34]);
        let packet_length = u16::try_from(data.len()).unwrap();
        data[2..4].copy_from_slice(&packet_length.to_be_bytes());
        assert!(parse_sample_rate_status(&data).is_some());
    }

    fn captured_encoding_status_packet_204720() -> Vec<u8> {
        vec![
            0xFF, 0xFF, 0x00, 0x3C, 0x21, 0x02, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0x10, 0x73, 0x32,
            0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x24, 0x00, 0x82,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x03, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x10,
            0x00, 0x00, 0x00, 0x20,
        ]
    }

    #[test]
    fn encoding_status_parses_captured_packet_204720() {
        let parsed = parse_encoding_status(&captured_encoding_status_packet_204720()).unwrap();
        assert_eq!(parsed.current_encoding, 24);
        assert_eq!(parsed.supported_encodings, vec![24, 16, 32]);
    }

    #[test]
    fn encoding_status_parses_captured_packet_645566() {
        let data = [
            0xFF, 0xFF, 0x00, 0x34, 0x79, 0xB2, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x50,
            0xCA, 0xC5, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x38, 0x00, 0x82,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00,
            0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18,
        ];
        let parsed = parse_encoding_status(&data).unwrap();
        assert_eq!(parsed.current_encoding, 24);
        assert_eq!(parsed.supported_encodings, vec![24]);
    }

    #[test]
    fn encoding_status_rejects_invalid_envelope_and_oversized_count() {
        let mut wrong_protocol = captured_encoding_status_packet_204720();
        wrong_protocol[0..2].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
        assert_eq!(parse_encoding_status(&wrong_protocol), None);

        let mut wrong_opcode = captured_encoding_status_packet_204720();
        wrong_opcode[26..28].copy_from_slice(&CONMON_OPCODE_SAMPLE_RATE_STATUS.to_be_bytes());
        assert_eq!(parse_encoding_status(&wrong_opcode), None);

        let mut wrong_declared_length = captured_encoding_status_packet_204720();
        wrong_declared_length[2..4].copy_from_slice(&59u16.to_be_bytes());
        assert_eq!(parse_encoding_status(&wrong_declared_length), None);

        let mut oversized_count = captured_encoding_status_packet_204720();
        oversized_count
            [CONMON_SUPPORTED_ENCODING_COUNT_OFFSET..CONMON_SUPPORTED_ENCODING_COUNT_OFFSET + 2]
            .copy_from_slice(&4u16.to_be_bytes());
        assert_eq!(parse_encoding_status(&oversized_count), None);
    }

    fn captured_input_gain_status_packet_1528() -> Vec<u8> {
        vec![
            0xFF, 0xFF, 0x00, 0x38, 0x06, 0x11, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x50,
            0x69, 0x2E, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x27, 0x10, 0x0B,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x10, 0x01, 0x02,
            0x00, 0x02, 0x00, 0x04, 0x00, 0x18, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01,
        ]
    }

    #[test]
    fn gain_status_parses_captured_input_packet_1528() {
        assert_eq!(
            parse_gain_status(&captured_input_gain_status_packet_1528()),
            Some(GainStatus {
                device_type: "input".to_owned(),
                channel_levels: vec![5, 1],
            })
        );
    }

    #[test]
    fn gain_status_parses_captured_output_packet_1585() {
        let data = [
            0xFF, 0xFF, 0x00, 0x38, 0x08, 0x10, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x50,
            0x7B, 0x8D, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x27, 0x10, 0x0B,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x10, 0x02, 0x01,
            0x00, 0x02, 0x00, 0x04, 0x00, 0x18, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04,
        ];
        assert_eq!(
            parse_gain_status(&data),
            Some(GainStatus {
                device_type: "output".to_owned(),
                channel_levels: vec![4, 4],
            })
        );
    }

    #[test]
    fn gain_status_rejects_unknown_direction_and_inconsistent_channel_count() {
        let mut unknown_direction = captured_input_gain_status_packet_1528();
        unknown_direction[CONMON_GAIN_DIRECTION_OFFSET..CONMON_GAIN_DIRECTION_OFFSET + 2]
            .copy_from_slice(&0x0101u16.to_be_bytes());
        assert_eq!(parse_gain_status(&unknown_direction), None);

        let mut oversized_count = captured_input_gain_status_packet_1528();
        oversized_count[CONMON_GAIN_CHANNEL_COUNT_OFFSET..CONMON_GAIN_CHANNEL_COUNT_OFFSET + 2]
            .copy_from_slice(&3u16.to_be_bytes());
        assert_eq!(parse_gain_status(&oversized_count), None);
    }

    #[test]
    fn ptp_clock_status_parses_preferred_and_role() {
        let mut data = vec![0u8; 0x4A];
        stamp_conmon_response(&mut data, CONMON_OPCODE_PTP_CLOCK_STATUS);
        data[0x26] = 0x01;
        data[0x48..0x4A].copy_from_slice(&0x0006u16.to_be_bytes());
        let parsed = parse_ptp_clock_status(&data).unwrap();
        assert!(parsed.preferred_leader);
        assert_eq!(parsed.ptp_v1_role.as_deref(), Some("Leader"));

        data[0x26] = 0x00;
        data[0x48..0x4A].copy_from_slice(&0x0009u16.to_be_bytes());
        let parsed = parse_ptp_clock_status(&data).unwrap();
        assert!(!parsed.preferred_leader);
        assert_eq!(parsed.ptp_v1_role.as_deref(), Some("Follower"));
    }

    #[test]
    fn aes67_status_maps_state_byte() {
        let mut data = vec![0u8; 0x22];
        stamp_conmon_response(&mut data, CONMON_OPCODE_AES67_CURRENT_NEW);
        data[0x21] = 0x03;
        let parsed = parse_aes67_status(&data).unwrap();
        assert_eq!(parsed.aes67_current, Some(true));
        assert_eq!(parsed.aes67_configured, Some(true));
    }

    fn write_interface_record(
        data: &mut [u8],
        offset: usize,
        mode: u16,
        mac: [u8; 6],
        addresses: [[u8; 4]; 4],
    ) {
        let [ip_address, netmask, first_extra_address, second_extra_address] = addresses;
        data[offset..offset + 2].copy_from_slice(&mode.to_be_bytes());
        data[offset + 2..offset + 8].copy_from_slice(&mac);
        data[offset + 8..offset + 12].copy_from_slice(&ip_address);
        data[offset + 12..offset + 16].copy_from_slice(&netmask);
        data[offset + 16..offset + 20].copy_from_slice(&first_extra_address);
        data[offset + 20..offset + 24].copy_from_slice(&second_extra_address);
    }

    #[test]
    fn interface_status_parses_pending_dhcp_and_reboot_state() {
        let mut data = vec![0u8; 0x4A];
        data[CONMON_INTERFACE_COUNT_OFFSET..CONMON_INTERFACE_COUNT_OFFSET + 2]
            .copy_from_slice(&1u16.to_be_bytes());
        data[CONMON_INTERFACE_LINK_SPEED_OFFSET..CONMON_INTERFACE_LINK_SPEED_OFFSET + 4]
            .copy_from_slice(&100u32.to_be_bytes());
        write_interface_record(
            &mut data,
            CONMON_INTERFACE_RECORDS_OFFSET,
            INTERFACE_MODE_DYNAMIC,
            [0x00, 0x1D, 0xC1, 0x12, 0x34, 0x56],
            [
                [192, 168, 10, 20],
                [255, 255, 255, 0],
                [192, 168, 10, 1],
                [192, 168, 10, 2],
            ],
        );
        data[CONMON_INTERFACE_REBOOT_FLAG_OFFSET..CONMON_INTERFACE_REBOOT_FLAG_OFFSET + 2]
            .copy_from_slice(&INTERFACE_REBOOT_PENDING_DYNAMIC.to_be_bytes());
        stamp_conmon_response(&mut data, CONMON_OPCODE_INTERFACE_STATUS);

        let parsed = parse_interface_status(&data).unwrap();
        assert_eq!(parsed.link_speed_mbps, 100);
        assert!(parsed.reboot_required);
        assert_eq!(parsed.interfaces.len(), 1);
        assert_eq!(parsed.interfaces[0].mode, "dynamic");
        assert_eq!(parsed.interfaces[0].mac_address, "00:1D:C1:12:34:56");
        assert_eq!(parsed.interfaces[0].ip_address, "192.168.10.20");
        assert_eq!(parsed.interfaces[0].netmask, "255.255.255.0");
        assert_eq!(
            parsed.interfaces[0].gateway.as_deref(),
            Some("192.168.10.1")
        );
        assert_eq!(
            parsed.interfaces[0].dns_server.as_deref(),
            Some("192.168.10.2")
        );

        let pending = parsed.pending_config.as_ref().unwrap();
        assert_eq!(pending.mode, "dynamic");
        assert_eq!(pending.ip_address, None);
        let json = serde_json::to_value(&parsed).unwrap();
        assert_eq!(json["reboot_required"], true);
        assert!(json["pending_config"].get("ip_address").is_none());
    }

    #[test]
    fn interface_status_parses_pending_static_configuration() {
        let mut data = vec![0u8; 0x5C];
        data[CONMON_INTERFACE_COUNT_OFFSET..CONMON_INTERFACE_COUNT_OFFSET + 2]
            .copy_from_slice(&1u16.to_be_bytes());
        data[CONMON_INTERFACE_LINK_SPEED_OFFSET..CONMON_INTERFACE_LINK_SPEED_OFFSET + 4]
            .copy_from_slice(&1_000u32.to_be_bytes());
        write_interface_record(
            &mut data,
            CONMON_INTERFACE_RECORDS_OFFSET,
            INTERFACE_MODE_STATIC,
            [0x00, 0x1D, 0xC1, 0x65, 0x43, 0x21],
            [
                [10, 0, 0, 20],
                [255, 255, 255, 0],
                [10, 0, 0, 53],
                [10, 0, 0, 1],
            ],
        );
        data[CONMON_INTERFACE_REBOOT_FLAG_OFFSET..CONMON_INTERFACE_REBOOT_FLAG_OFFSET + 2]
            .copy_from_slice(&INTERFACE_REBOOT_PENDING_STATIC.to_be_bytes());
        data[CONMON_INTERFACE_PENDING_STATIC_OFFSET..CONMON_INTERFACE_PENDING_STATIC_OFFSET + 4]
            .copy_from_slice(&[172, 16, 1, 50]);
        data[CONMON_INTERFACE_PENDING_STATIC_OFFSET + 4
            ..CONMON_INTERFACE_PENDING_STATIC_OFFSET + 8]
            .copy_from_slice(&[255, 255, 0, 0]);
        data[CONMON_INTERFACE_PENDING_STATIC_OFFSET + 8
            ..CONMON_INTERFACE_PENDING_STATIC_OFFSET + 12]
            .copy_from_slice(&[172, 16, 1, 53]);
        data[CONMON_INTERFACE_PENDING_STATIC_OFFSET + 12
            ..CONMON_INTERFACE_PENDING_STATIC_OFFSET + 16]
            .copy_from_slice(&[172, 16, 1, 1]);
        stamp_conmon_response(&mut data, CONMON_OPCODE_INTERFACE_STATUS);

        let parsed = parse_interface_status(&data).unwrap();
        assert_eq!(parsed.link_speed_mbps, 1_000);
        assert!(parsed.reboot_required);
        assert_eq!(parsed.interfaces[0].mode, "static");
        assert_eq!(
            parsed.interfaces[0].dns_server.as_deref(),
            Some("10.0.0.53")
        );
        assert_eq!(parsed.interfaces[0].gateway.as_deref(), Some("10.0.0.1"));

        let pending = parsed.pending_config.unwrap();
        assert_eq!(pending.mode, "static");
        assert_eq!(pending.ip_address.as_deref(), Some("172.16.1.50"));
        assert_eq!(pending.netmask.as_deref(), Some("255.255.0.0"));
        assert_eq!(pending.dns_server.as_deref(), Some("172.16.1.53"));
        assert_eq!(pending.gateway.as_deref(), Some("172.16.1.1"));
    }

    #[test]
    fn interface_status_parses_multiple_interfaces() {
        let mut data = vec![0u8; 0x5C];
        data[CONMON_INTERFACE_COUNT_OFFSET..CONMON_INTERFACE_COUNT_OFFSET + 2]
            .copy_from_slice(&2u16.to_be_bytes());
        data[CONMON_INTERFACE_LINK_SPEED_OFFSET..CONMON_INTERFACE_LINK_SPEED_OFFSET + 4]
            .copy_from_slice(&10_000u32.to_be_bytes());
        write_interface_record(
            &mut data,
            CONMON_INTERFACE_RECORDS_OFFSET,
            INTERFACE_MODE_DYNAMIC,
            [0x00, 0x1D, 0xC1, 0x00, 0x00, 0x01],
            [
                [192, 168, 1, 10],
                [255, 255, 255, 0],
                [192, 168, 1, 1],
                [1, 1, 1, 1],
            ],
        );
        write_interface_record(
            &mut data,
            CONMON_INTERFACE_RECORDS_OFFSET + CONMON_INTERFACE_CONFIGURED_RECORD_STRIDE,
            INTERFACE_MODE_STATIC,
            [0x00, 0x1D, 0xC1, 0xAA, 0xBB, 0xCC],
            [
                [192, 168, 2, 20],
                [255, 255, 0, 0],
                [8, 8, 8, 8],
                [192, 168, 2, 1],
            ],
        );
        stamp_conmon_response(&mut data, CONMON_OPCODE_INTERFACE_STATUS);

        let parsed = parse_interface_status(&data).unwrap();
        assert_eq!(parsed.link_speed_mbps, 10_000);
        assert_eq!(parsed.interfaces.len(), 2);
        assert_eq!(parsed.interfaces[0].mode, "dynamic");
        assert_eq!(parsed.interfaces[1].mode, "static");
        assert_eq!(parsed.interfaces[1].mac_address, "00:1D:C1:AA:BB:CC");
        assert_eq!(parsed.interfaces[1].ip_address, "192.168.2.20");
        assert_eq!(parsed.interfaces[1].dns_server.as_deref(), Some("8.8.8.8"));
        assert_eq!(parsed.interfaces[1].gateway.as_deref(), Some("192.168.2.1"));
        assert!(!parsed.reboot_required);
        assert_eq!(parsed.pending_config, None);

        let json = serde_json::to_value(&parsed).unwrap();
        assert!(json.get("interfaces").is_some());
        assert_eq!(json["pending_config"], serde_json::Value::Null);
    }

    #[test]
    fn typed_parsers_reject_every_truncated_prefix_without_panicking() {
        let flow = flow_query_response();
        for length in 0..flow.len() {
            assert_eq!(parse_tx_flows(&flow[..length]), None);
            assert_eq!(parse_result_code(&flow[..length]), None);
        }

        let mut conmon = vec![0u8; 0x4A];
        stamp_conmon_response(&mut conmon, CONMON_OPCODE_PTP_CLOCK_STATUS);
        for length in 0..conmon.len() {
            assert_eq!(parse_ptp_clock_status(&conmon[..length]), None);
            assert_eq!(parse_conmon_opcode(&conmon[..length]), None);
        }

        let sample_rate_status = captured_sample_rate_status_packet_28101();
        for length in 0..sample_rate_status.len() {
            assert_eq!(
                parse_sample_rate_status(&sample_rate_status[..length]),
                None
            );
        }

        let encoding_status = captured_encoding_status_packet_204720();
        for length in 0..encoding_status.len() {
            assert_eq!(parse_encoding_status(&encoding_status[..length]), None);
        }

        let gain_status = captured_input_gain_status_packet_1528();
        for length in 0..gain_status.len() {
            assert_eq!(parse_gain_status(&gain_status[..length]), None);
        }

        let device_settings = captured_selective_device_settings_packet_9084571();
        for length in 0..device_settings.len() {
            assert_eq!(parse_device_settings(&device_settings[..length]), None);
        }
    }

    #[test]
    fn flow_parser_rejects_duplicate_records_and_truncated_channel_lists() {
        let mut duplicate = flow_query_response();
        duplicate[112..114].copy_from_slice(&1u16.to_be_bytes());
        assert_eq!(parse_tx_flows(&duplicate), None);

        let mut truncated = flow_query_response();
        truncated[126..128].copy_from_slice(&23u16.to_be_bytes());
        assert_eq!(parse_tx_flows(&truncated), None);

        let mut overlapping = flow_query_response();
        overlapping[14..16].copy_from_slice(&60u16.to_be_bytes());
        assert_eq!(parse_tx_flows(&overlapping), None);

        let mut invalid_max = flow_query_response();
        invalid_max[10] = 0;
        assert_eq!(parse_tx_flows(&invalid_max), None);

        let mut active_above_max = flow_query_response();
        active_above_max[10] = 1;
        active_above_max[11] = 2;
        assert_eq!(parse_tx_flows(&active_above_max), None);

        let mut flow_number_above_max = flow_query_response();
        flow_number_above_max[44..46].copy_from_slice(&33u16.to_be_bytes());
        assert_eq!(parse_tx_flows(&flow_number_above_max), None);

        let mut missing_pointer = flow_query_response();
        missing_pointer[14..16].copy_from_slice(&0u16.to_be_bytes());
        assert_eq!(parse_tx_flows(&missing_pointer), None);

        let mut oversized_encoding = flow_query_response();
        oversized_encoding[52..56].copy_from_slice(&65_536u32.to_be_bytes());
        assert_eq!(parse_tx_flows(&oversized_encoding), None);
    }

    #[test]
    fn every_typed_response_parser_rejects_truncation() {
        let mut device_info = vec![0u8; RESPONSE_HEADER_SIZE + 18];
        stamp_arc_response(
            &mut device_info,
            PROTOCOL_ID,
            OPCODE_DEVICE_INFO,
            RESULT_CODE_SUCCESS,
        );

        let aes67_config =
            aes67_settings_response(&[(DEVICE_SETTINGS_INFO_AES67_CONFIGURED, 0x0003)]);

        let mut bluetooth = vec![0u8; 62];
        stamp_conmon_response(&mut bluetooth, CONMON_OPCODE_BLUETOOTH_STATUS);
        bluetooth[36..40].copy_from_slice(&[0x12, 0x18, 0x0A, 0x0A]);
        bluetooth[50..54].copy_from_slice(&[0x18, 0x09, 0x22, 0x08]);
        bluetooth[54..62].copy_from_slice(&[0x0A, 0x06, 0x12, 0x04, 0x0A, 0x02, 0x08, 0x02]);

        let mut make_model = vec![0u8; CONMON_PRODUCT_VERSION_END];
        stamp_conmon_response(&mut make_model, CONMON_OPCODE_MAKE_MODEL_RESPONSE);
        let mut dante_model = vec![0u8; CONMON_BOARD_NAME_END];
        stamp_conmon_response(&mut dante_model, CONMON_OPCODE_DANTE_MODEL_RESPONSE);

        for length in 0..device_info.len() {
            assert_eq!(parse_device_info(&device_info[..length]), None);
        }
        for length in 0..aes67_config.len() {
            assert_eq!(parse_aes67_configured(&aes67_config[..length]), None);
        }
        for length in 0..bluetooth.len() {
            assert_eq!(parse_bluetooth_status(&bluetooth[..length]), None);
        }
        for length in 0..make_model.len() {
            assert_eq!(parse_make_model(&make_model[..length]), None);
        }
        for length in 0..dante_model.len() {
            assert_eq!(parse_dante_model(&dante_model[..length]), None);
        }
    }

    #[test]
    fn hostile_bytes_never_panic_or_decode_as_typed_responses() {
        for length in 0..256usize {
            let data: Vec<u8> = (0..length)
                .map(|index| ((index * 73 + length * 19) & 0xFF) as u8)
                .collect();
            let result = std::panic::catch_unwind(|| {
                let _ = parse_device_name(&data);
                let _ = parse_device_info(&data);
                let _ = parse_device_settings(&data);
                let _ = parse_aes67_configured(&data);
                let _ = parse_make_model(&data);
                let _ = parse_dante_model(&data);
                let _ = parse_result_code(&data);
                let _ = parse_tx_flows(&data);
                let _ = parse_bluetooth_status(&data);
                let _ = parse_conmon_opcode(&data);
                let _ = parse_ptp_clock_status(&data);
                let _ = parse_aes67_status(&data);
                let _ = parse_interface_status(&data);
                let _ = parse_sample_rate_status(&data);
                let _ = parse_encoding_status(&data);
                let _ = parse_gain_status(&data);
            });
            assert!(result.is_ok(), "length={length}");
            assert_eq!(parse_device_name(&data), None);
            assert_eq!(parse_tx_flows(&data), None);
            assert_eq!(parse_interface_status(&data), None);
        }
    }
}
