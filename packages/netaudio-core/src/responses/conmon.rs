use super::conmon_common::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConfigurableU32Status {
    pub record_protocol_version: u16,
    pub current_value: u32,
    pub requested_value: u32,
    pub update_mode: u16,
    pub available_values: Vec<u32>,
    pub flags: Option<u32>,
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
    pub redundancy: crate::network::DanteRedundancyStatus,
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
pub struct CodecParameterStatus {
    pub parameter_type: u8,
    pub mode: u8,
    pub values: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodecStatus {
    pub record_protocol_version: u16,
    pub parameters: Vec<CodecParameterStatus>,
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

pub fn redundancy_mode_for_label(label: &str) -> Option<crate::network::DanteRedundancyMode> {
    match label {
        "Redundant" => Some(crate::network::DanteRedundancyMode::Redundant),
        "Split/Redundant" => Some(crate::network::DanteRedundancyMode::SplitRedundant),
        "Switched" => Some(crate::network::DanteRedundancyMode::Switched),
        _ => None,
    }
}

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

    let mode_for = |code| {
        choices
            .iter()
            .find(|choice| choice.code == code)
            .and_then(|choice| redundancy_mode_for_label(&choice.label))
    };
    let current = mode_for(read_u16(record, 20)?);
    let configured = mode_for(read_u16(record, 22)?);
    let supported: Vec<crate::network::DanteRedundancyMode> = choices
        .iter()
        .filter_map(|choice| redundancy_mode_for_label(&choice.label))
        .collect();
    let current = current.filter(|mode| supported.contains(mode));
    let configured = configured.filter(|mode| supported.contains(mode));
    Some(SwitchConfigurationStatus {
        redundancy: crate::network::DanteRedundancyStatus {
            current,
            configured,
            reboot_required: current.is_some() && configured.is_some() && current != configured,
            supported,
        },
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

const CONMON_BODY_OFFSET: usize = 0x18;
const CONFIGURABLE_VECTOR_POINTER_BODY_OFFSET: usize = 0x08;
const CONFIGURABLE_VECTOR_COUNT_BODY_OFFSET: usize = 0x0A;
const CONFIGURABLE_CURRENT_VALUE_BODY_OFFSET: usize = 0x0C;
const CONFIGURABLE_REQUESTED_VALUE_BODY_OFFSET: usize = 0x10;
const CONFIGURABLE_UPDATE_MODE_BODY_OFFSET: usize = 0x14;
const CONFIGURABLE_MINIMUM_VECTOR_BODY_OFFSET: usize = 0x18;
const CONFIGURABLE_UPDATE_MODE_VERSION: u16 = 0x0501;
const SAMPLE_RATE_PULLUP_FLAGS_VERSION: u16 = 0x070F;
const SAMPLE_RATE_PULLUP_FLAGS_BODY_OFFSET: usize = 0x1C;

fn parse_configurable_u32_status(
    data: &[u8],
    expected_opcode: u16,
    pre_0501_reboot_mode: bool,
    pullup_flags: bool,
) -> Option<ConfigurableU32Status> {
    validate_conmon_envelope(data, expected_opcode)?;
    let record_protocol_version = read_u16(data, CONMON_BODY_OFFSET)?;
    let vector_body_offset = usize::from(read_u16(
        data,
        CONMON_BODY_OFFSET + CONFIGURABLE_VECTOR_POINTER_BODY_OFFSET,
    )?);
    let vector_count = usize::from(read_u16(
        data,
        CONMON_BODY_OFFSET + CONFIGURABLE_VECTOR_COUNT_BODY_OFFSET,
    )?);
    let minimum_vector_body_offset =
        if pullup_flags && record_protocol_version >= SAMPLE_RATE_PULLUP_FLAGS_VERSION {
            SAMPLE_RATE_PULLUP_FLAGS_BODY_OFFSET + 4
        } else {
            CONFIGURABLE_MINIMUM_VECTOR_BODY_OFFSET
        };
    if vector_body_offset % 4 != 0
        || (vector_count != 0 && vector_body_offset < minimum_vector_body_offset)
    {
        return None;
    }
    let vector_offset = CONMON_BODY_OFFSET.checked_add(vector_body_offset)?;
    let vector_byte_length = vector_count.checked_mul(4)?;
    let vector_end = vector_offset.checked_add(vector_byte_length)?;
    data.get(vector_offset..vector_end)?;

    let mut available_values = Vec::with_capacity(vector_count);
    for vector_index in 0..vector_count {
        let value_offset = vector_offset.checked_add(vector_index.checked_mul(4)?)?;
        available_values.push(read_u32(data, value_offset)?);
    }

    let update_mode =
        if pre_0501_reboot_mode && record_protocol_version < CONFIGURABLE_UPDATE_MODE_VERSION {
            1
        } else {
            read_u16(
                data,
                CONMON_BODY_OFFSET + CONFIGURABLE_UPDATE_MODE_BODY_OFFSET,
            )?
        };
    let flags = if pullup_flags && record_protocol_version >= SAMPLE_RATE_PULLUP_FLAGS_VERSION {
        Some(read_u32(
            data,
            CONMON_BODY_OFFSET + SAMPLE_RATE_PULLUP_FLAGS_BODY_OFFSET,
        )?)
    } else {
        None
    };

    Some(ConfigurableU32Status {
        record_protocol_version,
        current_value: read_u32(
            data,
            CONMON_BODY_OFFSET + CONFIGURABLE_CURRENT_VALUE_BODY_OFFSET,
        )?,
        requested_value: read_u32(
            data,
            CONMON_BODY_OFFSET + CONFIGURABLE_REQUESTED_VALUE_BODY_OFFSET,
        )?,
        update_mode,
        available_values,
        flags,
    })
}

pub fn parse_sample_rate_status(data: &[u8]) -> Option<ConfigurableU32Status> {
    parse_configurable_u32_status(data, CONMON_OPCODE_SAMPLE_RATE_STATUS, true, false)
}

const INTERFACE_STATISTICS_BODY_OFFSET: usize = 0x18;
const INTERFACE_STATISTICS_GROUP_COUNT_BODY_OFFSET: usize = 0x08;
const INTERFACE_STATISTICS_GROUP_POINTERS_BODY_OFFSET: usize = 0x0A;
const INTERFACE_STATISTICS_HEADER_CAPABILITY_MASK_OFFSET: usize = 0x10;
const INTERFACE_STATISTICS_MINIMUM_RECORD_SIZE: usize = 24;
const INTERFACE_STATISTICS_CAPABILITIES_VERSION: u16 = 0x0713;
const INTERFACE_STATISTICS_LEGACY_CAPABILITY_MASK: u32 = 0x0000_0003;
const INTERFACE_STATISTICS_UTILIZATION_CAPABILITY: u32 = 0x0000_0001;
const INTERFACE_STATISTICS_ERRORS_CAPABILITY: u32 = 0x0000_0002;
const INTERFACE_STATISTICS_CLEAR_ERRORS_CAPABILITY: u32 = 0x0000_0004;
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
pub struct ClockUnicastStatus {
    pub record_revision: u16,
    pub raw_record: Vec<u8>,
    pub raw_words: [u32; 4],
}

pub fn parse_clock_unicast_status(data: &[u8]) -> Option<ClockUnicastStatus> {
    validate_conmon_envelope(data, 0x0024)?;
    let r = data.get(24..)?;
    Some(ClockUnicastStatus {
        record_revision: read_u16(r, 0)?,
        raw_record: r.to_vec(),
        raw_words: [
            read_u32(r, 8)?,
            read_u32(r, 12)?,
            read_u32(r, 16)?,
            read_u32(r, 20)?,
        ],
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClockMasterStatus {
    pub record_revision: u16,
    pub raw_record: Vec<u8>,
    pub record_count: u16,
    pub block_length: u16,
    pub status_codes: Vec<u16>,
    pub block_padding: Vec<u8>,
}

pub fn parse_clock_master_status(data: &[u8]) -> Option<ClockMasterStatus> {
    validate_conmon_envelope(data, 0x0022)?;
    let r = data.get(24..)?;
    let count = read_u16(r, 8)?;
    let length = read_u16(r, 10)?;
    let block_end = 8usize.checked_add(usize::from(length))?;
    let codes_end = 12usize.checked_add(usize::from(count).checked_mul(2)?)?;
    if codes_end > block_end {
        return None;
    }
    r.get(..block_end)?;
    let codes = (0..usize::from(count))
        .map(|i| read_u16(r, 12 + i * 2))
        .collect::<Option<Vec<_>>>()?;
    Some(ClockMasterStatus {
        record_revision: read_u16(r, 0)?,
        raw_record: r.to_vec(),
        record_count: count,
        block_length: length,
        status_codes: codes,
        block_padding: r[codes_end..block_end].to_vec(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClockIdentifierStatus {
    pub record_revision: u16,
    pub raw_record: Vec<u8>,
    pub region_size: u16,
    pub item_count: u16,
    pub maximum_name_width: u16,
    pub name_length: u16,
    pub name_pointer: u16,
    pub device_name: String,
    pub name_bytes: Vec<u8>,
    pub first_identifier: [u8; 6],
    pub second_identifier: [u8; 6],
}

pub fn parse_clock_identifier_status(data: &[u8]) -> Option<ClockIdentifierStatus> {
    validate_conmon_envelope(data, 0x0026)?;
    let r = data.get(24..)?;
    let region_size = read_u16(r, 8)?;
    let bounded = r.get(..usize::from(region_size))?;
    let width = read_u16(r, 12)?;
    let length = read_u16(r, 14)?;
    let name = read_u16(r, 16)?;
    let first = usize::from(read_u16(r, 20)?);
    let second = usize::from(read_u16(r, 24)?);
    let start = usize::from(name);
    let end = start.checked_add(usize::from(length))?;
    if length >= width || start < 26 || first < 26 || second < 26 || *bounded.get(end)? != 0 {
        return None;
    }
    let ranges = [
        start..end.checked_add(1)?,
        first..first.checked_add(6)?,
        second..second.checked_add(6)?,
    ];
    for (i, a) in ranges.iter().enumerate() {
        bounded.get(a.clone())?;
        if ranges[i + 1..]
            .iter()
            .any(|b| a.start < b.end && b.start < a.end)
        {
            return None;
        }
    }
    let name_bytes = bounded.get(start..end)?;
    if name_bytes.contains(&0) {
        return None;
    }
    Some(ClockIdentifierStatus {
        record_revision: read_u16(r, 0)?,
        raw_record: r.to_vec(),
        region_size,
        item_count: read_u16(r, 10)?,
        maximum_name_width: width,
        name_length: length,
        name_pointer: name,
        device_name: String::from_utf8_lossy(name_bytes).into_owned(),
        name_bytes: name_bytes.to_vec(),
        first_identifier: bounded[first..first + 6].try_into().ok()?,
        second_identifier: bounded[second..second + 6].try_into().ok()?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceStatisticsRecord {
    pub record_pointer: u16,
    pub record_size_bytes: usize,
    pub transmit_raw_bytes_per_second: u32,
    pub receive_raw_bytes_per_second: u32,
    pub transmit_bits_per_second: u64,
    pub receive_bits_per_second: u64,
    pub cumulative_transmit_errors: u32,
    pub cumulative_receive_errors: u32,
    pub discriminator_status_word: u32,
    pub speed_megabits_per_second: u32,
    pub extension_hexadecimal: String,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceStatisticsGroup {
    pub group_index: u16,
    pub group_pointer: u16,
    pub record_count: u16,
    pub record_pointers: Vec<u16>,
    pub selected_stats: Option<InterfaceStatisticsRecord>,
    pub raw_records: Vec<InterfaceStatisticsRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceStatisticsStatus {
    pub record_protocol_version: u16,
    pub header_record_pointer: u16,
    pub header_record_size_bytes: usize,
    pub header_record_hexadecimal: String,
    pub capability_mask: u32,
    pub utilization_supported: bool,
    pub errors_supported: bool,
    pub clear_errors_supported: bool,
    pub interface_group_count: u16,
    pub interface_group_pointers: Vec<u16>,
    pub interface_groups: Vec<InterfaceStatisticsGroup>,
    pub raw_body_hexadecimal: String,
}

fn interface_statistics_record(
    data: &[u8],
    record_pointer: u16,
    record_end_body_offset: usize,
) -> Option<InterfaceStatisticsRecord> {
    let record_body_offset = usize::from(record_pointer);
    let record_size_bytes = record_end_body_offset.checked_sub(record_body_offset)?;
    if record_size_bytes < INTERFACE_STATISTICS_MINIMUM_RECORD_SIZE {
        return None;
    }
    let record_offset = INTERFACE_STATISTICS_BODY_OFFSET.checked_add(record_body_offset)?;
    let record_end = INTERFACE_STATISTICS_BODY_OFFSET.checked_add(record_end_body_offset)?;
    let record = data.get(record_offset..record_end)?;
    let transmit_raw_bytes_per_second = read_u32(record, 0)?;
    let receive_raw_bytes_per_second = read_u32(record, 4)?;
    Some(InterfaceStatisticsRecord {
        record_pointer,
        record_size_bytes,
        transmit_raw_bytes_per_second,
        receive_raw_bytes_per_second,
        transmit_bits_per_second: u64::from(transmit_raw_bytes_per_second).checked_mul(8)?,
        receive_bits_per_second: u64::from(receive_raw_bytes_per_second).checked_mul(8)?,
        cumulative_transmit_errors: read_u32(record, 8)?,
        cumulative_receive_errors: read_u32(record, 12)?,
        discriminator_status_word: read_u32(record, 16)?,
        speed_megabits_per_second: read_u32(record, 20)?,
        extension_hexadecimal: bytes_to_hex(
            record.get(INTERFACE_STATISTICS_MINIMUM_RECORD_SIZE..)?,
        ),
        raw_record_hexadecimal: bytes_to_hex(record),
    })
}

pub fn parse_interface_statistics_status(data: &[u8]) -> Option<InterfaceStatisticsStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_INTERFACE_STATISTICS_STATUS)?;
    let body = data.get(INTERFACE_STATISTICS_BODY_OFFSET..)?;
    let record_protocol_version = read_u16(body, 0)?;
    let interface_group_count = read_u16(body, INTERFACE_STATISTICS_GROUP_COUNT_BODY_OFFSET)?;
    if interface_group_count == 0 {
        return None;
    }
    let group_pointers_end = INTERFACE_STATISTICS_GROUP_POINTERS_BODY_OFFSET
        .checked_add(usize::from(interface_group_count).checked_mul(2)?)?;
    let outer_structure_end = group_pointers_end.checked_add(2)?;
    if outer_structure_end > body.len() {
        return None;
    }
    let header_record_pointer = read_u16(body, group_pointers_end)?;
    let header_record_offset = usize::from(header_record_pointer);
    if header_record_offset < outer_structure_end || header_record_offset >= body.len() {
        return None;
    }
    let capability_mask = if record_protocol_version >= INTERFACE_STATISTICS_CAPABILITIES_VERSION {
        let capability_mask_end = header_record_offset
            .checked_add(INTERFACE_STATISTICS_HEADER_CAPABILITY_MASK_OFFSET)?
            .checked_add(4)?;
        if capability_mask_end > body.len() {
            return None;
        }
        read_u32(
            body,
            header_record_offset.checked_add(INTERFACE_STATISTICS_HEADER_CAPABILITY_MASK_OFFSET)?,
        )?
    } else {
        INTERFACE_STATISTICS_LEGACY_CAPABILITY_MASK
    };

    let mut interface_group_pointers = Vec::with_capacity(usize::from(interface_group_count));
    let mut occupied_group_pointers = HashSet::new();
    for group_index in 0..usize::from(interface_group_count) {
        let pointer_offset = INTERFACE_STATISTICS_GROUP_POINTERS_BODY_OFFSET
            .checked_add(group_index.checked_mul(2)?)?;
        let pointer = read_u16(body, pointer_offset)?;
        let group_body_offset = usize::from(pointer);
        if group_body_offset < outer_structure_end
            || group_body_offset.checked_add(2)? > body.len()
            || !occupied_group_pointers.insert(pointer)
        {
            return None;
        }
        interface_group_pointers.push(pointer);
    }

    let mut group_record_pointers = Vec::with_capacity(usize::from(interface_group_count));
    let mut group_ranges = Vec::with_capacity(usize::from(interface_group_count));
    let header_minimum_end = if record_protocol_version >= INTERFACE_STATISTICS_CAPABILITIES_VERSION
    {
        header_record_offset
            .checked_add(INTERFACE_STATISTICS_HEADER_CAPABILITY_MASK_OFFSET)?
            .checked_add(4)?
    } else {
        header_record_offset
    };
    for group_pointer in &interface_group_pointers {
        let group_body_offset = usize::from(*group_pointer);
        let record_count = read_u16(body, group_body_offset)?;
        let record_pointers_offset = group_body_offset.checked_add(2)?;
        let record_pointers_end =
            record_pointers_offset.checked_add(usize::from(record_count).checked_mul(2)?)?;
        if record_pointers_end > body.len() {
            return None;
        }
        group_ranges.push((group_body_offset, record_pointers_end));
        group_record_pointers.push((record_count, Vec::with_capacity(usize::from(record_count))));
    }
    for (index, (start, end)) in group_ranges.iter().copied().enumerate() {
        let overlaps_outer = start < outer_structure_end;
        let overlaps_header = start < header_minimum_end && end > header_record_offset;
        let overlaps_group =
            group_ranges
                .iter()
                .enumerate()
                .any(|(other_index, (other_start, other_end))| {
                    other_index != index && start < *other_end && end > *other_start
                });
        if overlaps_outer || overlaps_header || overlaps_group {
            return None;
        }
    }

    let mut structural_offsets = interface_group_pointers
        .iter()
        .map(|pointer| usize::from(*pointer))
        .collect::<Vec<_>>();
    structural_offsets.push(header_record_offset);
    let mut occupied_record_pointers = HashSet::new();
    for ((record_count, pointers), group_pointer) in group_record_pointers
        .iter_mut()
        .zip(interface_group_pointers.iter())
    {
        let group_body_offset = usize::from(*group_pointer);
        let record_pointers_offset = group_body_offset.checked_add(2)?;
        for record_index in 0..usize::from(*record_count) {
            let pointer = read_u16(
                body,
                record_pointers_offset.checked_add(record_index.checked_mul(2)?)?,
            )?;
            let record_body_offset = usize::from(pointer);
            let overlaps_structure = record_body_offset < outer_structure_end
                || (record_body_offset >= header_record_offset
                    && record_body_offset < header_minimum_end)
                || group_ranges
                    .iter()
                    .any(|(start, end)| record_body_offset >= *start && record_body_offset < *end);
            if overlaps_structure
                || record_body_offset.checked_add(INTERFACE_STATISTICS_MINIMUM_RECORD_SIZE)?
                    > body.len()
                || !occupied_record_pointers.insert(pointer)
            {
                return None;
            }
            pointers.push(pointer);
            structural_offsets.push(record_body_offset);
        }
    }
    structural_offsets.push(body.len());
    structural_offsets.sort_unstable();
    structural_offsets.dedup();
    let header_record_end = structural_offsets
        .iter()
        .copied()
        .find(|offset| *offset > header_record_offset)?;
    let header_record = body.get(header_record_offset..header_record_end)?;

    let mut interface_groups = Vec::with_capacity(usize::from(interface_group_count));
    for (group_index, ((record_count, record_pointers), group_pointer)) in group_record_pointers
        .into_iter()
        .zip(interface_group_pointers.iter().copied())
        .enumerate()
    {
        let mut raw_records = Vec::with_capacity(usize::from(record_count));
        for record_pointer in &record_pointers {
            let record_body_offset = usize::from(*record_pointer);
            let next_offset = structural_offsets
                .iter()
                .copied()
                .find(|offset| *offset > record_body_offset)?;
            raw_records.push(interface_statistics_record(
                data,
                *record_pointer,
                next_offset,
            )?);
        }
        let selected_stats = raw_records
            .iter()
            .find(|record| record.discriminator_status_word & 0xFFFF_0000 == 0)
            .cloned();
        interface_groups.push(InterfaceStatisticsGroup {
            group_index: u16::try_from(group_index).ok()?,
            group_pointer,
            record_count,
            record_pointers,
            selected_stats,
            raw_records,
        });
    }

    Some(InterfaceStatisticsStatus {
        record_protocol_version,
        header_record_pointer,
        header_record_size_bytes: header_record.len(),
        header_record_hexadecimal: bytes_to_hex(header_record),
        capability_mask,
        utilization_supported: capability_mask & INTERFACE_STATISTICS_UTILIZATION_CAPABILITY != 0,
        errors_supported: capability_mask & INTERFACE_STATISTICS_ERRORS_CAPABILITY != 0,
        clear_errors_supported: capability_mask & INTERFACE_STATISTICS_CLEAR_ERRORS_CAPABILITY != 0,
        interface_group_count,
        interface_group_pointers,
        interface_groups,
        raw_body_hexadecimal: bytes_to_hex(body),
    })
}

pub fn parse_encoding_status(data: &[u8]) -> Option<ConfigurableU32Status> {
    parse_configurable_u32_status(data, CONMON_OPCODE_ENCODING_STATUS, true, false)
}

pub fn parse_sample_rate_pullup_status(data: &[u8]) -> Option<ConfigurableU32Status> {
    parse_configurable_u32_status(data, CONMON_OPCODE_SAMPLE_RATE_PULLUP_STATUS, false, true)
}

pub fn parse_codec_status(data: &[u8]) -> Option<CodecStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_CODEC_STATUS)?;
    let record_protocol_version = read_u16(data, CONMON_BODY_OFFSET)?;
    let parameter_count = usize::try_from(read_u32(data, CONMON_BODY_OFFSET + 8)?).ok()?;
    let descriptor_width = usize::from(read_u16(data, CONMON_BODY_OFFSET + 12)?);
    let descriptor_body_offset = usize::from(read_u16(data, CONMON_BODY_OFFSET + 14)?);
    if descriptor_width != 8 || descriptor_body_offset % 2 != 0 {
        return None;
    }
    let descriptors_offset = CONMON_BODY_OFFSET.checked_add(descriptor_body_offset)?;
    let descriptors_end =
        descriptors_offset.checked_add(parameter_count.checked_mul(descriptor_width)?)?;
    data.get(descriptors_offset..descriptors_end)?;

    let mut parameters = Vec::with_capacity(parameter_count);
    for parameter_index in 0..parameter_count {
        let descriptor_offset =
            descriptors_offset.checked_add(parameter_index.checked_mul(descriptor_width)?)?;
        let parameter_type = *data.get(descriptor_offset)?;
        let mode = *data.get(descriptor_offset + 1)?;
        let value_count = usize::from(read_u16(data, descriptor_offset + 2)?);
        let value_width = usize::from(read_u16(data, descriptor_offset + 4)?);
        let values_body_offset = usize::from(read_u16(data, descriptor_offset + 6)?);
        if value_width != 4 || values_body_offset % 4 != 0 {
            return None;
        }
        let values_offset = CONMON_BODY_OFFSET.checked_add(values_body_offset)?;
        let values_end = values_offset.checked_add(value_count.checked_mul(value_width)?)?;
        data.get(values_offset..values_end)?;
        let mut values = Vec::with_capacity(value_count);
        for value_index in 0..value_count {
            values.push(read_u32(
                data,
                values_offset.checked_add(value_index.checked_mul(value_width)?)?,
            )?);
        }
        parameters.push(CodecParameterStatus {
            parameter_type,
            mode,
            values,
        });
    }
    Some(CodecStatus {
        record_protocol_version,
        parameters,
    })
}
