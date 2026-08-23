use super::conmon_common::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SampleRateStatus {
    pub current_sample_rate: u32,
    pub supported_sample_rates: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClearConfigurationStatus {
    pub record_protocol_identifier: u16,
    pub unmapped_first_word: u32,
    pub available_actions_mask: u32,
    pub action_result_code: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutingCapacityStatus {
    pub unmapped_prefix_word: u32,
    pub state_code: u16,
    pub routing_ready: Option<bool>,
    pub unmapped_word: u16,
    pub transmit_channel_count: u16,
    pub receive_channel_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SwitchConfigurationChoice {
    pub code: u16,
    pub unmapped_word: u16,
    pub label: String,
    pub raw_label_field_hexadecimal: String,
    pub unmapped_trailing_words: [u32; 4],
    pub raw_choice_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SwitchConfigurationStatus {
    pub record_protocol_identifier: u16,
    pub unmapped_prefix_word: u32,
    pub choice_count: u16,
    pub choice_table_pointer: u16,
    pub referenced_value_pointer: u16,
    pub referenced_value_size: u16,
    pub referenced_value_hexadecimal: String,
    pub mode_codes_at_record_offsets_20_and_22: [u16; 2],
    pub choices: Vec<SwitchConfigurationChoice>,
    pub unmapped_before_choice_table_hexadecimal: String,
    pub unmapped_after_choice_table_hexadecimal: String,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EncodingStatus {
    pub current_encoding: u32,
    pub supported_encodings: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleRatePullupMeaning {
    NoPullupOrPulldown,
    PositiveFourPointOneSixSixSevenPercent,
    PositiveOneTenthPercent,
    NegativeOneTenthPercent,
    NegativeFourPercent,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SampleRatePullupValue {
    pub raw_value: u32,
    pub meaning: SampleRatePullupMeaning,
    pub rate_multiplier_numerator: Option<u32>,
    pub rate_multiplier_denominator: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SampleRatePullupStatus {
    pub applied_value: SampleRatePullupValue,
    pub requested_value: SampleRatePullupValue,
    pub mode_code: u16,
    pub unmapped_word_at_body_offset_20: u32,
    pub supported_values: Vec<SampleRatePullupValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GainStatus {
    pub device_type: String,
    pub channel_levels: Vec<u32>,
}

pub fn parse_clear_configuration_status(data: &[u8]) -> Option<ClearConfigurationStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_CLEAR_CONFIGURATION_STATUS)?;
    if data.len() != CONMON_CLEAR_CONFIGURATION_PACKET_SIZE || data.get(25).copied()? != 0x24 {
        return None;
    }
    Some(ClearConfigurationStatus {
        record_protocol_identifier: read_u16(
            data,
            CONMON_CLEAR_CONFIGURATION_RECORD_IDENTIFIER_OFFSET,
        )?,
        unmapped_first_word: read_u32(data, CONMON_CLEAR_CONFIGURATION_FIRST_WORD_OFFSET)?,
        available_actions_mask: read_u32(
            data,
            CONMON_CLEAR_CONFIGURATION_AVAILABLE_ACTIONS_MASK_OFFSET,
        )?,
        action_result_code: read_u32(data, CONMON_CLEAR_CONFIGURATION_ACTION_RESULT_CODE_OFFSET)?,
    })
}

pub fn parse_routing_capacity_status(data: &[u8]) -> Option<RoutingCapacityStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_ROUTING_CAPACITY_STATUS)?;
    if data.len() != CONMON_ROUTING_CAPACITY_PACKET_SIZE {
        return None;
    }
    let state_code = read_u16(data, CONMON_ROUTING_CAPACITY_STATE_CODE_OFFSET)?;
    let routing_ready = match state_code {
        0x0101 => Some(true),
        0x0001 => Some(false),
        _ => None,
    };
    Some(RoutingCapacityStatus {
        unmapped_prefix_word: read_u32(data, CONMON_ROUTING_CAPACITY_UNMAPPED_PREFIX_WORD_OFFSET)?,
        state_code,
        routing_ready,
        unmapped_word: read_u16(data, CONMON_ROUTING_CAPACITY_UNMAPPED_WORD_OFFSET)?,
        transmit_channel_count: read_u16(
            data,
            CONMON_ROUTING_CAPACITY_TRANSMIT_CHANNEL_COUNT_OFFSET,
        )?,
        receive_channel_count: read_u16(
            data,
            CONMON_ROUTING_CAPACITY_RECEIVE_CHANNEL_COUNT_OFFSET,
        )?,
    })
}

const CONMON_SWITCH_CONFIGURATION_RECORD_OFFSET: usize = 24;
const CONMON_SWITCH_CONFIGURATION_FIXED_RECORD_SIZE: usize = 24;
const CONMON_SWITCH_CONFIGURATION_CHOICE_SIZE: usize = 148;
const CONMON_SWITCH_CONFIGURATION_CHOICE_LABEL_OFFSET: usize = 4;
const CONMON_SWITCH_CONFIGURATION_CHOICE_LABEL_SIZE: usize = 128;
const CONMON_SWITCH_CONFIGURATION_CHOICE_TRAILING_WORDS_OFFSET: usize = 132;

pub fn parse_switch_configuration_status(data: &[u8]) -> Option<SwitchConfigurationStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_SWITCH_CONFIGURATION_STATUS)?;
    let record = data.get(CONMON_SWITCH_CONFIGURATION_RECORD_OFFSET..)?;
    if record.len() < CONMON_SWITCH_CONFIGURATION_FIXED_RECORD_SIZE {
        return None;
    }

    let choice_count = read_u16(record, 8)?;
    let choice_table_pointer = read_u16(record, 10)?;
    let choice_table_offset = usize::from(choice_table_pointer);
    if choice_table_offset < CONMON_SWITCH_CONFIGURATION_FIXED_RECORD_SIZE {
        return None;
    }
    let choices_byte_length =
        usize::from(choice_count).checked_mul(CONMON_SWITCH_CONFIGURATION_CHOICE_SIZE)?;
    let choices_end = choice_table_offset.checked_add(choices_byte_length)?;
    if choices_end > record.len() {
        return None;
    }

    let referenced_value_pointer = read_u16(record, 12)?;
    let referenced_value_size = read_u16(record, 14)?;
    let referenced_value_offset = usize::from(referenced_value_pointer);
    let referenced_value_end =
        referenced_value_offset.checked_add(usize::from(referenced_value_size))?;
    let referenced_value = record.get(referenced_value_offset..referenced_value_end)?;

    let mut choices = Vec::with_capacity(usize::from(choice_count));
    for choice_index in 0..usize::from(choice_count) {
        let choice_offset = choice_table_offset
            .checked_add(choice_index.checked_mul(CONMON_SWITCH_CONFIGURATION_CHOICE_SIZE)?)?;
        let choice_end = choice_offset.checked_add(CONMON_SWITCH_CONFIGURATION_CHOICE_SIZE)?;
        let choice = record.get(choice_offset..choice_end)?;
        let label_field = choice.get(
            CONMON_SWITCH_CONFIGURATION_CHOICE_LABEL_OFFSET
                ..CONMON_SWITCH_CONFIGURATION_CHOICE_LABEL_OFFSET
                    + CONMON_SWITCH_CONFIGURATION_CHOICE_LABEL_SIZE,
        )?;
        let label_end = label_field.iter().position(|byte| *byte == 0)?;
        let label = std::str::from_utf8(&label_field[..label_end])
            .ok()?
            .to_owned();
        choices.push(SwitchConfigurationChoice {
            code: read_u16(choice, 0)?,
            unmapped_word: read_u16(choice, 2)?,
            label,
            raw_label_field_hexadecimal: bytes_to_hex(label_field),
            unmapped_trailing_words: [
                read_u32(
                    choice,
                    CONMON_SWITCH_CONFIGURATION_CHOICE_TRAILING_WORDS_OFFSET,
                )?,
                read_u32(
                    choice,
                    CONMON_SWITCH_CONFIGURATION_CHOICE_TRAILING_WORDS_OFFSET + 4,
                )?,
                read_u32(
                    choice,
                    CONMON_SWITCH_CONFIGURATION_CHOICE_TRAILING_WORDS_OFFSET + 8,
                )?,
                read_u32(
                    choice,
                    CONMON_SWITCH_CONFIGURATION_CHOICE_TRAILING_WORDS_OFFSET + 12,
                )?,
            ],
            raw_choice_hexadecimal: bytes_to_hex(choice),
        });
    }

    Some(SwitchConfigurationStatus {
        record_protocol_identifier: read_u16(record, 0)?,
        unmapped_prefix_word: read_u32(record, 4)?,
        choice_count,
        choice_table_pointer,
        referenced_value_pointer,
        referenced_value_size,
        referenced_value_hexadecimal: bytes_to_hex(referenced_value),
        mode_codes_at_record_offsets_20_and_22: [read_u16(record, 20)?, read_u16(record, 22)?],
        choices,
        unmapped_before_choice_table_hexadecimal: bytes_to_hex(
            record.get(CONMON_SWITCH_CONFIGURATION_FIXED_RECORD_SIZE..choice_table_offset)?,
        ),
        unmapped_after_choice_table_hexadecimal: bytes_to_hex(record.get(choices_end..)?),
        raw_record_hexadecimal: bytes_to_hex(record),
    })
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

const CONMON_0022_BODY_OFFSET: usize = 28;
const CONMON_0022_MINIMUM_SIZE: usize = 64;
const CONMON_0022_COUNT_OFFSET: usize = 32;
const CONMON_0022_CODES_OFFSET: usize = 36;
const CONMON_0024_BODY_OFFSET: usize = 28;
const CONMON_0024_MINIMUM_SIZE: usize = 48;
const CONMON_0026_MINIMUM_SIZE: usize = 76;
const CONMON_0026_NAME_POINTER_OFFSET: usize = 40;
const CONMON_0040_COUNT_OFFSET: usize = 60;
const CONMON_0040_POINTERS_OFFSET: usize = 62;
const CONMON_0040_RECORD_SIZE: usize = 24;
const CONMON_0086_BODY_OFFSET: usize = 28;
const CONMON_0086_MINIMUM_SIZE: usize = 40;
const CONMON_00E0_BODY_OFFSET: usize = 28;
const CONMON_00E0_MINIMUM_SIZE: usize = 52;
const CONMON_0102_PREFIX_OFFSET: usize = 28;
const CONMON_0102_COUNT_OFFSET: usize = 32;
const CONMON_0102_BYTES_OFFSET: usize = 34;
const CONMON_0106_BODY_OFFSET: usize = 28;
const CONMON_0106_MINIMUM_SIZE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unmapped0086Status {
    pub unmapped_word_at_body_offset_0: u32,
    pub unmapped_word_at_body_offset_4: u32,
    pub unmapped_word_at_body_offset_8: u32,
}

pub fn parse_unmapped_0086_status(data: &[u8]) -> Option<Unmapped0086Status> {
    validate_conmon_envelope(data, CONMON_OPCODE_UNMAPPED_0086_STATUS)?;
    if data.len() < CONMON_0086_MINIMUM_SIZE {
        return None;
    }
    Some(Unmapped0086Status {
        unmapped_word_at_body_offset_0: read_u32(data, CONMON_0086_BODY_OFFSET)?,
        unmapped_word_at_body_offset_4: read_u32(data, CONMON_0086_BODY_OFFSET + 4)?,
        unmapped_word_at_body_offset_8: read_u32(data, CONMON_0086_BODY_OFFSET + 8)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unmapped00E0Status {
    pub unmapped_word_at_body_offset_0: u32,
    pub unmapped_word_at_body_offset_4: u32,
    pub unmapped_word_at_body_offset_8: u32,
    pub unmapped_word_at_body_offset_12: u32,
    pub unmapped_word_at_body_offset_16: u32,
    pub unmapped_word_at_body_offset_20: u32,
}

pub fn parse_unmapped_00e0_status(data: &[u8]) -> Option<Unmapped00E0Status> {
    validate_conmon_envelope(data, CONMON_OPCODE_UNMAPPED_00E0_STATUS)?;
    if data.len() != CONMON_00E0_MINIMUM_SIZE {
        return None;
    }
    Some(Unmapped00E0Status {
        unmapped_word_at_body_offset_0: read_u32(data, CONMON_00E0_BODY_OFFSET)?,
        unmapped_word_at_body_offset_4: read_u32(data, CONMON_00E0_BODY_OFFSET + 4)?,
        unmapped_word_at_body_offset_8: read_u32(data, CONMON_00E0_BODY_OFFSET + 8)?,
        unmapped_word_at_body_offset_12: read_u32(data, CONMON_00E0_BODY_OFFSET + 12)?,
        unmapped_word_at_body_offset_16: read_u32(data, CONMON_00E0_BODY_OFFSET + 16)?,
        unmapped_word_at_body_offset_20: read_u32(data, CONMON_00E0_BODY_OFFSET + 20)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unmapped0106Status {
    pub unmapped_word_at_body_offset_0: u32,
}

pub fn parse_unmapped_0106_status(data: &[u8]) -> Option<Unmapped0106Status> {
    validate_conmon_envelope(data, CONMON_OPCODE_UNMAPPED_0106_STATUS)?;
    if data.len() != CONMON_0106_MINIMUM_SIZE {
        return None;
    }
    Some(Unmapped0106Status {
        unmapped_word_at_body_offset_0: read_u32(data, CONMON_0106_BODY_OFFSET)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unmapped0102Status {
    pub unmapped_prefix_word: u32,
    pub trailing_byte_count: u16,
    pub trailing_bytes: Vec<u8>,
}

pub fn parse_unmapped_0102_status(data: &[u8]) -> Option<Unmapped0102Status> {
    validate_conmon_envelope(data, CONMON_OPCODE_UNMAPPED_0102_STATUS)?;
    if data.len() < CONMON_0102_BYTES_OFFSET {
        return None;
    }
    let trailing_byte_count = read_u16(data, CONMON_0102_COUNT_OFFSET)?;
    let trailing_end = CONMON_0102_BYTES_OFFSET.checked_add(usize::from(trailing_byte_count))?;
    if data.len() != trailing_end {
        return None;
    }
    Some(Unmapped0102Status {
        unmapped_prefix_word: read_u32(data, CONMON_0102_PREFIX_OFFSET)?,
        trailing_byte_count,
        trailing_bytes: data[CONMON_0102_BYTES_OFFSET..trailing_end].to_vec(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unmapped0024Status {
    pub unmapped_word_at_body_offset_0: u32,
    pub unmapped_word_at_body_offset_4: u32,
    pub unmapped_word_at_body_offset_8: u32,
    pub unmapped_word_at_body_offset_12: u32,
    pub unmapped_word_at_body_offset_16: u32,
}

pub fn parse_unmapped_0024_status(data: &[u8]) -> Option<Unmapped0024Status> {
    validate_conmon_envelope(data, CONMON_OPCODE_UNMAPPED_0024_STATUS)?;
    if data.len() < CONMON_0024_MINIMUM_SIZE {
        return None;
    }
    Some(Unmapped0024Status {
        unmapped_word_at_body_offset_0: read_u32(data, CONMON_0024_BODY_OFFSET)?,
        unmapped_word_at_body_offset_4: read_u32(data, CONMON_0024_BODY_OFFSET + 4)?,
        unmapped_word_at_body_offset_8: read_u32(data, CONMON_0024_BODY_OFFSET + 8)?,
        unmapped_word_at_body_offset_12: read_u32(data, CONMON_0024_BODY_OFFSET + 12)?,
        unmapped_word_at_body_offset_16: read_u32(data, CONMON_0024_BODY_OFFSET + 16)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unmapped0022Status {
    pub unmapped_prefix_word: u32,
    pub record_count: u16,
    pub unmapped_word_at_body_offset_6: u16,
    pub unmapped_codes: Vec<u16>,
}

pub fn parse_unmapped_0022_status(data: &[u8]) -> Option<Unmapped0022Status> {
    validate_conmon_envelope(data, CONMON_OPCODE_UNMAPPED_0022_STATUS)?;
    if data.len() < CONMON_0022_MINIMUM_SIZE {
        return None;
    }
    let record_count = read_u16(data, CONMON_0022_COUNT_OFFSET)?;
    let codes_end =
        CONMON_0022_CODES_OFFSET.checked_add((record_count as usize).checked_mul(2)?)?;
    if codes_end > data.len() {
        return None;
    }
    let mut unmapped_codes = Vec::with_capacity(record_count as usize);
    for code_index in 0..record_count as usize {
        unmapped_codes.push(read_u16(
            data,
            CONMON_0022_CODES_OFFSET.checked_add(code_index.checked_mul(2)?)?,
        )?);
    }
    Some(Unmapped0022Status {
        unmapped_prefix_word: read_u32(data, CONMON_0022_BODY_OFFSET)?,
        record_count,
        unmapped_word_at_body_offset_6: read_u16(data, CONMON_0022_COUNT_OFFSET + 2)?,
        unmapped_codes,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unmapped0026Status {
    pub name_pointer: u16,
    pub device_name: String,
    pub trailing_bytes: Vec<u8>,
}

pub fn parse_unmapped_0026_status(data: &[u8]) -> Option<Unmapped0026Status> {
    validate_conmon_envelope(data, CONMON_OPCODE_UNMAPPED_0026_STATUS)?;
    if data.len() < CONMON_0026_MINIMUM_SIZE {
        return None;
    }
    let name_pointer = read_u16(data, CONMON_0026_NAME_POINTER_OFFSET)?;
    let name_offset = CONMON_CLOCK_RECORD_PAYLOAD_OFFSET.checked_add(usize::from(name_pointer))?;
    let name_bytes = data.get(name_offset..)?;
    let name_end = name_bytes.iter().position(|byte| *byte == 0)?;
    let device_name = std::str::from_utf8(&name_bytes[..name_end])
        .ok()?
        .to_owned();
    Some(Unmapped0026Status {
        name_pointer,
        device_name,
        trailing_bytes: name_bytes[name_end + 1..].to_vec(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unmapped0040Record {
    pub record_pointer: u16,
    pub record_size_bytes: usize,
    pub unmapped_prefix_words: [u32; 4],
    pub raw_link_status_word: u32,
    pub link_up: bool,
    pub link_speed_megabits_per_second: u32,
    pub unmapped_trailing_hexadecimal: String,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unmapped0040Status {
    pub record_count: u16,
    pub record_pointers: Vec<u16>,
    pub records: Vec<Unmapped0040Record>,
}

pub fn parse_unmapped_0040_status(data: &[u8]) -> Option<Unmapped0040Status> {
    validate_conmon_envelope(data, CONMON_OPCODE_UNMAPPED_0040_STATUS)?;
    if data.len() < CONMON_0040_POINTERS_OFFSET + 2 {
        return None;
    }
    let record_count = read_u16(data, CONMON_0040_COUNT_OFFSET)?;
    if record_count == 0 {
        return None;
    }
    let pointers_end =
        CONMON_0040_POINTERS_OFFSET.checked_add((record_count as usize).checked_mul(2)?)?;
    if pointers_end > data.len() {
        return None;
    }
    let mut record_pointers = Vec::with_capacity(record_count as usize);
    for record_index in 0..record_count as usize {
        let pointer = read_u16(
            data,
            CONMON_0040_POINTERS_OFFSET.checked_add(record_index.checked_mul(2)?)?,
        )?;
        let record_offset = CONMON_CLOCK_RECORD_PAYLOAD_OFFSET.checked_add(usize::from(pointer))?;
        if record_offset < pointers_end || record_offset > data.len() {
            return None;
        }
        if let Some(previous_pointer) = record_pointers.last() {
            if pointer <= *previous_pointer {
                return None;
            }
        }
        record_pointers.push(pointer);
    }
    let mut records = Vec::with_capacity(record_count as usize);
    for record_index in 0..record_count as usize {
        let pointer = record_pointers[record_index];
        let record_offset = CONMON_CLOCK_RECORD_PAYLOAD_OFFSET.checked_add(usize::from(pointer))?;
        let record_end = if let Some(next_pointer) = record_pointers.get(record_index + 1) {
            CONMON_CLOCK_RECORD_PAYLOAD_OFFSET.checked_add(usize::from(*next_pointer))?
        } else {
            data.len()
        };
        if record_end < record_offset.checked_add(CONMON_0040_RECORD_SIZE)?
            || record_end > data.len()
        {
            return None;
        }
        let record = data.get(record_offset..record_end)?;
        let raw_link_status_word = read_u32(record, 16)?;
        records.push(Unmapped0040Record {
            record_pointer: pointer,
            record_size_bytes: record.len(),
            unmapped_prefix_words: [
                read_u32(record, 0)?,
                read_u32(record, 4)?,
                read_u32(record, 8)?,
                read_u32(record, 12)?,
            ],
            raw_link_status_word,
            link_up: raw_link_status_word & 1 != 0,
            link_speed_megabits_per_second: read_u32(record, 20)?,
            unmapped_trailing_hexadecimal: bytes_to_hex(record.get(CONMON_0040_RECORD_SIZE..)?),
            raw_record_hexadecimal: bytes_to_hex(record),
        });
    }
    Some(Unmapped0040Status {
        record_count,
        record_pointers,
        records,
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

pub(super) fn sample_rate_pullup_value(raw_value: u32) -> SampleRatePullupValue {
    let (meaning, multiplier) = match raw_value {
        0 => (SampleRatePullupMeaning::NoPullupOrPulldown, Some((1, 1))),
        1 => (
            SampleRatePullupMeaning::PositiveFourPointOneSixSixSevenPercent,
            Some((25, 24)),
        ),
        2 => (
            SampleRatePullupMeaning::PositiveOneTenthPercent,
            Some((1001, 1000)),
        ),
        3 => (
            SampleRatePullupMeaning::NegativeOneTenthPercent,
            Some((999, 1000)),
        ),
        4 => (SampleRatePullupMeaning::NegativeFourPercent, Some((24, 25))),
        _ => (SampleRatePullupMeaning::Unknown, None),
    };
    let (rate_multiplier_numerator, rate_multiplier_denominator) = multiplier
        .map(|(numerator, denominator)| (Some(numerator), Some(denominator)))
        .unwrap_or((None, None));

    SampleRatePullupValue {
        raw_value,
        meaning,
        rate_multiplier_numerator,
        rate_multiplier_denominator,
    }
}

pub fn parse_sample_rate_pullup_status(data: &[u8]) -> Option<SampleRatePullupStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_SAMPLE_RATE_PULLUP_STATUS)?;
    let relative_vector_offset = usize::from(read_u16(
        data,
        CONMON_SAMPLE_RATE_PULLUP_VECTOR_OFFSET_FIELD,
    )?);
    let vector_offset =
        CONMON_SAMPLE_RATE_PULLUP_VECTOR_OFFSET_BASE.checked_add(relative_vector_offset)?;
    if vector_offset < CONMON_SAMPLE_RATE_PULLUP_FIXED_FIELDS_END {
        return None;
    }
    let vector_count = usize::from(read_u16(
        data,
        CONMON_SAMPLE_RATE_PULLUP_VECTOR_COUNT_FIELD,
    )?);
    if vector_count == 0 {
        return None;
    }
    let vector_byte_length = vector_count.checked_mul(4)?;
    let vector_end = vector_offset.checked_add(vector_byte_length)?;
    data.get(vector_offset..vector_end)?;

    let mut supported_values = Vec::with_capacity(vector_count);
    for vector_index in 0..vector_count {
        let value_offset = vector_offset.checked_add(vector_index.checked_mul(4)?)?;
        supported_values.push(sample_rate_pullup_value(read_u32(data, value_offset)?));
    }

    Some(SampleRatePullupStatus {
        applied_value: sample_rate_pullup_value(read_u32(
            data,
            CONMON_SAMPLE_RATE_PULLUP_APPLIED_VALUE_OFFSET,
        )?),
        requested_value: sample_rate_pullup_value(read_u32(
            data,
            CONMON_SAMPLE_RATE_PULLUP_REQUESTED_VALUE_OFFSET,
        )?),
        mode_code: read_u16(data, CONMON_SAMPLE_RATE_PULLUP_MODE_OFFSET)?,
        unmapped_word_at_body_offset_20: read_u32(
            data,
            CONMON_SAMPLE_RATE_PULLUP_UNMAPPED_WORD_OFFSET,
        )?,
        supported_values,
    })
}

pub fn parse_gain_status(data: &[u8]) -> Option<GainStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_GAIN_STATUS)?;
    if data.get(28..40)?
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
