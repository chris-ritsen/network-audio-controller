use super::conmon_common::*;
use super::*;

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
pub struct LockResetStatus {
    pub record_protocol_identifier: u16,
    pub unmapped_prefix_word: u32,
    pub lock_state_code: u16,
    pub is_locked: Option<bool>,
    pub status_code: u16,
    pub lock_identifier_count: u16,
    pub lock_identifier_width: u16,
    pub lock_identifier_data_offset: u16,
    pub unmapped_trailer_words: [u16; 3],
    pub lock_identifiers: Vec<String>,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConmonExportFragment {
    pub envelope_sequence_identifier: u16,
    pub record_protocol_identifier: u16,
    pub echoed_tag_hexadecimal: String,
    pub total_encoded_size: u32,
    pub selector_value: u16,
    pub fragment_identifier: u16,
    pub has_more_fragments: bool,
    pub fragment_size: u16,
    pub header_size: u16,
    pub data_hexadecimal: String,
}

pub fn parse_conmon_export_fragment(data: &[u8]) -> Option<ConmonExportFragment> {
    validate_conmon_envelope(data, CONMON_OPCODE_EXPORT_FRAGMENT)?;
    if data.len() <= CONMON_EXPORT_DATA_OFFSET
        || data.get(28..32)? != [0, 0, 0, 0]
        || data.get(50..52)? != [0, 0]
    {
        return None;
    }
    let total_encoded_size = read_u32(data, 36)?;
    if total_encoded_size == 0 {
        return None;
    }
    let selector_value = read_u16(data, 40)?;
    let fragment_identifier = read_u16(data, 42)?;
    let continuation_flag = read_u16(data, 44)?;
    let fragment_size = read_u16(data, 46)?;
    let header_size = read_u16(data, 48)?;
    let fragment_data = data.get(CONMON_EXPORT_DATA_OFFSET..)?;
    if fragment_identifier == 0
        || continuation_flag > 1
        || (continuation_flag == 1 && fragment_identifier == u16::MAX)
        || fragment_size == 0
        || usize::from(fragment_size) != fragment_data.len()
        || u32::from(fragment_size) > total_encoded_size
        || usize::from(header_size) != CONMON_EXPORT_HEADER_SIZE
    {
        return None;
    }
    Some(ConmonExportFragment {
        envelope_sequence_identifier: read_u16(data, 4)?,
        record_protocol_identifier: read_u16(data, 24)?,
        echoed_tag_hexadecimal: bytes_to_hex(data.get(32..36)?),
        total_encoded_size,
        selector_value,
        fragment_identifier,
        has_more_fragments: continuation_flag == 1,
        fragment_size,
        header_size,
        data_hexadecimal: bytes_to_hex(fragment_data),
    })
}

pub fn parse_lock_reset_status(data: &[u8]) -> Option<LockResetStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_LOCK_RESET_STATUS)?;
    let record = data.get(CONMON_LOCK_RESET_RECORD_OFFSET..)?;
    if record.len() < CONMON_LOCK_RESET_FIXED_RECORD_SIZE {
        return None;
    }

    let lock_identifier_count = read_u16(record, 12)?;
    let lock_identifier_width = read_u16(record, 14)?;
    let lock_identifier_data_offset = read_u16(record, 16)?;
    if usize::from(lock_identifier_width) != CONMON_LOCK_RESET_IDENTIFIER_WIDTH {
        return None;
    }

    let identifier_bytes =
        usize::from(lock_identifier_count).checked_mul(CONMON_LOCK_RESET_IDENTIFIER_WIDTH)?;
    let expected_record_size = CONMON_LOCK_RESET_FIXED_RECORD_SIZE.checked_add(identifier_bytes)?;
    if record.len() != expected_record_size {
        return None;
    }

    let status_code = read_u16(record, 10)?;
    if lock_identifier_count == 0 {
        if lock_identifier_data_offset != 0 {
            return None;
        }
    } else if usize::from(lock_identifier_data_offset) != CONMON_LOCK_RESET_FIXED_RECORD_SIZE {
        return None;
    }

    let mut lock_identifiers = Vec::with_capacity(usize::from(lock_identifier_count));
    for identifier_index in 0..usize::from(lock_identifier_count) {
        let identifier_offset = CONMON_LOCK_RESET_FIXED_RECORD_SIZE
            .checked_add(identifier_index.checked_mul(CONMON_LOCK_RESET_IDENTIFIER_WIDTH)?)?;
        let identifier_end = identifier_offset.checked_add(CONMON_LOCK_RESET_IDENTIFIER_WIDTH)?;
        lock_identifiers.push(bytes_to_hex(record.get(identifier_offset..identifier_end)?));
    }

    let lock_state_code = read_u16(record, 8)?;
    let is_locked = match lock_state_code {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    };
    Some(LockResetStatus {
        record_protocol_identifier: read_u16(record, 0)?,
        unmapped_prefix_word: read_u32(record, 4)?,
        lock_state_code,
        is_locked,
        status_code,
        lock_identifier_count,
        lock_identifier_width,
        lock_identifier_data_offset,
        unmapped_trailer_words: [
            read_u16(record, 18)?,
            read_u16(record, 20)?,
            read_u16(record, 22)?,
        ],
        lock_identifiers,
        raw_record_hexadecimal: bytes_to_hex(record),
    })
}
