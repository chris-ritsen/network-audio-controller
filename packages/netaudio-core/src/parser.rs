use std::collections::HashSet;

use serde::Serialize;

use crate::bytes::{read_u16, read_u32, string_at_pointer, u16_at};
use crate::protocol::{
    common_arc_protocol_opcodes, is_common_arc_protocol, response_envelope,
    validate_response_envelope, OPCODE_CHANNEL_COUNT, OPCODE_RX_CHANNELS, OPCODE_TX_CHANNEL_INFO,
    OPCODE_TX_CHANNEL_NAMES, RESULT_CODE_MORE_PAGES, RESULT_CODE_SUCCESS,
};

pub use crate::protocol::RESPONSE_HEADER_SIZE;
pub const BODY_HEADER_SIZE: usize = 2;
pub const RX_RECORD_SIZE: usize = 20;
pub const TX_RECORD_SIZE: usize = 8;
pub const TX_FRIENDLY_RECORD_SIZE: usize = 6;
pub const RX_CHANNELS_PER_PAGE: u16 = 16;
pub const TX_CHANNELS_PER_PAGE: u16 = 32;

const CHANNEL_COUNT_TX_OFFSET: usize = 12;
const CHANNEL_COUNT_RX_OFFSET: usize = 14;
const RX_RECORD_CHANNEL_NUMBER: usize = 0;
const RX_RECORD_TX_CHANNEL_POINTER: usize = 6;
const RX_RECORD_TX_DEVICE_POINTER: usize = 8;
const RX_RECORD_RX_CHANNEL_POINTER: usize = 10;
const RX_RECORD_RX_STATUS: usize = 12;
const RX_RECORD_SUBSCRIPTION_STATUS: usize = 14;

const TX_RECORD_CHANNEL_NUMBER: usize = 0;
const TX_RECORD_CHANNEL_GROUP: usize = 4;
const TX_RECORD_NAME_POINTER: usize = 6;

const TX_FRIENDLY_RECORD_CHANNEL_NUMBER: usize = 2;
const TX_FRIENDLY_RECORD_NAME_POINTER: usize = 4;
const CHANNEL_RECORD_METADATA_POINTER: usize = 4;
const CHANNEL_AUDIO_METADATA_SIZE: usize = 16;
const CHANNEL_AUDIO_SAMPLE_RATE_OFFSET: usize = 0;
const CHANNEL_AUDIO_CURRENT_ENCODING_OFFSET: usize = 6;
const CHANNEL_AUDIO_ENCODING_CAPABILITY_BITMAP_OFFSET: usize = 14;
const PCM16_CAPABILITY_BIT: u16 = 0x0002;
const PCM24_CAPABILITY_BIT: u16 = 0x0004;
const PCM32_CAPABILITY_BIT: u16 = 0x0008;
const KNOWN_PCM_CAPABILITY_BITS: u16 =
    PCM16_CAPABILITY_BIT | PCM24_CAPABILITY_BIT | PCM32_CAPABILITY_BIT;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelCount {
    pub tx_count: u16,
    pub rx_count: u16,
    pub locked: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RxChannel {
    pub number: u16,
    pub rx_channel_name: Option<String>,
    pub tx_channel_name: Option<String>,
    pub tx_device_name: Option<String>,
    pub rx_status_code: u16,
    pub subscription_status_code: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TxChannel {
    pub number: u16,
    pub name: Option<String>,
    pub friendly_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelAudioMetadata {
    pub sample_rate: u32,
    pub current_encoding: u16,
    pub encoding_capability_bitmap: u16,
    pub supported_encodings: Option<Vec<u16>>,
}

pub fn parse_channel_audio_metadata(response: &[u8]) -> Option<ChannelAudioMetadata> {
    let envelope = response_envelope(response)?;
    if !is_common_arc_protocol(envelope.protocol_id)
        || ![OPCODE_TX_CHANNEL_INFO, OPCODE_RX_CHANNELS].contains(&envelope.opcode)
        || ![RESULT_CODE_SUCCESS, RESULT_CODE_MORE_PAGES].contains(&envelope.result_code)
    {
        return None;
    }

    let record_size = match envelope.opcode {
        OPCODE_TX_CHANNEL_INFO => TX_RECORD_SIZE,
        OPCODE_RX_CHANNELS => RX_RECORD_SIZE,
        _ => return None,
    };
    let first_record = envelope
        .body
        .get(BODY_HEADER_SIZE..BODY_HEADER_SIZE.checked_add(record_size)?)?;
    let starting_channel = read_u16(first_record, 0)?;
    if starting_channel == 0 {
        return None;
    }
    let record_count = match envelope.opcode {
        OPCODE_TX_CHANNEL_INFO => parse_tx_info_page(response, starting_channel)?.len(),
        OPCODE_RX_CHANNELS => parse_rx_page(response, starting_channel)?.len(),
        _ => return None,
    };
    if record_count == 0 {
        return None;
    }
    let records_size = record_count.checked_mul(record_size)?;
    let records_end = RESPONSE_HEADER_SIZE
        .checked_add(BODY_HEADER_SIZE)?
        .checked_add(records_size)?;
    let metadata_pointer = usize::from(read_u16(first_record, CHANNEL_RECORD_METADATA_POINTER)?);
    if metadata_pointer < records_end {
        return None;
    }
    let metadata = response
        .get(metadata_pointer..metadata_pointer.checked_add(CHANNEL_AUDIO_METADATA_SIZE)?)?;
    let sample_rate = read_u32(metadata, CHANNEL_AUDIO_SAMPLE_RATE_OFFSET)?;
    let current_encoding = read_u16(metadata, CHANNEL_AUDIO_CURRENT_ENCODING_OFFSET)?;
    let encoding_capability_bitmap =
        read_u16(metadata, CHANNEL_AUDIO_ENCODING_CAPABILITY_BITMAP_OFFSET)?;
    if sample_rate == 0 || current_encoding == 0 {
        return None;
    }

    let supported_encodings = if encoding_capability_bitmap != 0
        && encoding_capability_bitmap & !KNOWN_PCM_CAPABILITY_BITS == 0
    {
        let mut encodings = Vec::new();
        for (encoding, capability_bit) in [
            (16, PCM16_CAPABILITY_BIT),
            (24, PCM24_CAPABILITY_BIT),
            (32, PCM32_CAPABILITY_BIT),
        ] {
            if encoding_capability_bitmap & capability_bit != 0 {
                encodings.push(encoding);
            }
        }
        encodings.contains(&current_encoding).then_some(encodings)
    } else {
        None
    };

    Some(ChannelAudioMetadata {
        sample_rate,
        current_encoding,
        encoding_capability_bitmap,
        supported_encodings,
    })
}

pub fn parse_channel_count(response: &[u8]) -> Option<ChannelCount> {
    validate_response_envelope(
        response,
        &common_arc_protocol_opcodes(OPCODE_CHANNEL_COUNT),
        &[RESULT_CODE_SUCCESS],
    )?;
    if response.len() < 16 {
        return None;
    }
    Some(ChannelCount {
        tx_count: read_u16(response, CHANNEL_COUNT_TX_OFFSET)?,
        rx_count: read_u16(response, CHANNEL_COUNT_RX_OFFSET)?,
        locked: None,
    })
}

fn expected_channel_number(starting_channel: u16, index: usize) -> Option<u16> {
    let offset = u16::try_from(index).ok()?;
    starting_channel.checked_add(offset)
}

fn pointed_string(response: &[u8], pointer: u16, minimum_pointer: usize) -> Option<String> {
    if usize::from(pointer) < minimum_pointer {
        return None;
    }
    string_at_pointer(response, pointer)
}

fn optional_pointed_string(
    response: &[u8],
    pointer: u16,
    minimum_pointer: usize,
) -> Option<Option<String>> {
    if pointer == 0 {
        return Some(None);
    }
    pointed_string(response, pointer, minimum_pointer).map(Some)
}

pub fn parse_rx_page(response: &[u8], starting_channel: u16) -> Option<Vec<RxChannel>> {
    let envelope = validate_response_envelope(
        response,
        &common_arc_protocol_opcodes(OPCODE_RX_CHANNELS),
        &[RESULT_CODE_SUCCESS, RESULT_CODE_MORE_PAGES],
    )?;
    let body = envelope.body;
    let body_header = body.get(..BODY_HEADER_SIZE)?;
    let maximum_records = usize::from(body_header[0]);
    let record_count = usize::from(body_header[1]);
    if maximum_records > usize::from(RX_CHANNELS_PER_PAGE) || record_count > maximum_records {
        return None;
    }
    if record_count == 0 {
        if envelope.result_code != RESULT_CODE_SUCCESS {
            return None;
        }
        if maximum_records == 0 {
            return (body.len() == BODY_HEADER_SIZE).then(Vec::new);
        }
        let padded_records_size = maximum_records.checked_mul(RX_RECORD_SIZE)?;
        let padded_records_end = BODY_HEADER_SIZE.checked_add(padded_records_size)?;
        return (body.len() == padded_records_end
            && body[BODY_HEADER_SIZE..padded_records_end]
                .iter()
                .all(|byte| *byte == 0))
        .then(Vec::new);
    }
    if maximum_records != record_count {
        return None;
    }
    let records_size = record_count.checked_mul(RX_RECORD_SIZE)?;
    let records_end = BODY_HEADER_SIZE.checked_add(records_size)?;
    body.get(..records_end)?;
    if envelope.result_code == RESULT_CODE_MORE_PAGES
        && record_count != usize::from(RX_CHANNELS_PER_PAGE)
    {
        return None;
    }
    let minimum_pointer = RESPONSE_HEADER_SIZE.checked_add(records_end)?;
    let mut channels = Vec::with_capacity(record_count);

    for index in 0..record_count {
        let record_offset = BODY_HEADER_SIZE + index * RX_RECORD_SIZE;
        let record = body.get(record_offset..record_offset + RX_RECORD_SIZE)?;
        let channel_number = u16_at(record, RX_RECORD_CHANNEL_NUMBER);
        let expected = expected_channel_number(starting_channel, index)?;
        if channel_number == 0 || channel_number != expected {
            return None;
        }

        let tx_channel_pointer = u16_at(record, RX_RECORD_TX_CHANNEL_POINTER);
        let tx_device_pointer = u16_at(record, RX_RECORD_TX_DEVICE_POINTER);
        let rx_channel_pointer = u16_at(record, RX_RECORD_RX_CHANNEL_POINTER);

        let rx_channel_name = Some(pointed_string(
            response,
            rx_channel_pointer,
            minimum_pointer,
        )?);
        let tx_device_name = optional_pointed_string(response, tx_device_pointer, minimum_pointer)?;
        let tx_channel_name = if tx_channel_pointer != 0 {
            Some(pointed_string(
                response,
                tx_channel_pointer,
                minimum_pointer,
            )?)
        } else {
            rx_channel_name.clone()
        };

        channels.push(RxChannel {
            number: channel_number,
            rx_channel_name,
            tx_channel_name,
            tx_device_name,
            rx_status_code: u16_at(record, RX_RECORD_RX_STATUS),
            subscription_status_code: u16_at(record, RX_RECORD_SUBSCRIPTION_STATUS),
        });
    }

    Some(channels)
}

pub fn parse_tx_friendly_page(
    response: &[u8],
    starting_channel: u16,
) -> Option<Vec<(u16, String)>> {
    let envelope = validate_response_envelope(
        response,
        &common_arc_protocol_opcodes(OPCODE_TX_CHANNEL_NAMES),
        &[RESULT_CODE_SUCCESS, RESULT_CODE_MORE_PAGES],
    )?;
    let body = envelope.body;
    let body_header = body.get(..BODY_HEADER_SIZE)?;
    let result_code = envelope.result_code;
    let mut names = Vec::new();

    if body_header == [0, 0] {
        if body.len() == BODY_HEADER_SIZE {
            return (result_code == RESULT_CODE_SUCCESS).then(Vec::new);
        }
        let first_record =
            body.get(BODY_HEADER_SIZE..BODY_HEADER_SIZE + TX_FRIENDLY_RECORD_SIZE)?;
        if u16_at(first_record, TX_FRIENDLY_RECORD_CHANNEL_NUMBER) == 0 {
            return (result_code == RESULT_CODE_SUCCESS
                && body[BODY_HEADER_SIZE..].iter().all(|byte| *byte == 0))
            .then(Vec::new);
        }
        let first_pointer = usize::from(u16_at(first_record, TX_FRIENDLY_RECORD_NAME_POINTER));
        let records_start = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE;
        let record_bytes = first_pointer.checked_sub(records_start)?;
        if record_bytes == 0 || record_bytes % TX_FRIENDLY_RECORD_SIZE != 0 {
            return None;
        }
        let record_count = record_bytes / TX_FRIENDLY_RECORD_SIZE;
        if record_count > usize::from(TX_CHANNELS_PER_PAGE)
            || (result_code == RESULT_CODE_MORE_PAGES
                && record_count != usize::from(TX_CHANNELS_PER_PAGE))
        {
            return None;
        }
        let body_records_end = BODY_HEADER_SIZE.checked_add(record_bytes)?;
        body.get(..body_records_end)?;
        names.reserve(record_count);
        for index in 0..record_count {
            let record_offset = BODY_HEADER_SIZE + index * TX_FRIENDLY_RECORD_SIZE;
            let record = body.get(record_offset..record_offset + TX_FRIENDLY_RECORD_SIZE)?;
            let channel_number = u16_at(record, TX_FRIENDLY_RECORD_CHANNEL_NUMBER);
            if channel_number != expected_channel_number(starting_channel, index)? {
                return None;
            }
            let name_pointer = u16_at(record, TX_FRIENDLY_RECORD_NAME_POINTER);
            let friendly_name = pointed_string(response, name_pointer, first_pointer)?;
            names.push((channel_number, friendly_name));
        }
        return Some(names);
    }

    let maximum_records = usize::from(body_header[0]);
    let named_count = usize::from(body_header[1]);
    if maximum_records > usize::from(TX_CHANNELS_PER_PAGE) || named_count > maximum_records {
        return None;
    }
    let live_records_size = named_count.checked_mul(TX_FRIENDLY_RECORD_SIZE)?;
    let live_records_end = BODY_HEADER_SIZE.checked_add(live_records_size)?;
    let padded_records_size = maximum_records.checked_mul(TX_RECORD_SIZE)?;
    let padded_records_end = BODY_HEADER_SIZE.checked_add(padded_records_size)?;
    body.get(..padded_records_end)?;
    if named_count == 0 {
        return (body.len() == padded_records_end).then(Vec::new);
    }
    let minimum_name_pointer = RESPONSE_HEADER_SIZE.checked_add(padded_records_end)?;
    names.reserve(named_count);
    let mut channel_numbers = HashSet::with_capacity(named_count);
    for index in 0..named_count {
        let record_offset = BODY_HEADER_SIZE + index * TX_FRIENDLY_RECORD_SIZE;
        let record = body.get(record_offset..record_offset + TX_FRIENDLY_RECORD_SIZE)?;
        let channel_number = u16_at(record, TX_FRIENDLY_RECORD_CHANNEL_NUMBER);
        if channel_number == 0 || !channel_numbers.insert(channel_number) {
            return None;
        }
        let name_pointer = u16_at(record, TX_FRIENDLY_RECORD_NAME_POINTER);
        let friendly_name = pointed_string(response, name_pointer, minimum_name_pointer)?;
        names.push((channel_number, friendly_name));
    }
    body.get(live_records_end..padded_records_end)?;
    Some(names)
}

pub fn parse_tx_info_page(response: &[u8], starting_channel: u16) -> Option<Vec<TxChannel>> {
    let envelope = validate_response_envelope(
        response,
        &common_arc_protocol_opcodes(OPCODE_TX_CHANNEL_INFO),
        &[RESULT_CODE_SUCCESS, RESULT_CODE_MORE_PAGES],
    )?;
    let body = envelope.body;
    let body_header = body.get(..BODY_HEADER_SIZE)?;
    let result_code = envelope.result_code;
    let mut channels = Vec::new();

    if body_header == [0, 0] {
        if body.len() == BODY_HEADER_SIZE
            || (body.len() == BODY_HEADER_SIZE + 4
                && body[BODY_HEADER_SIZE..BODY_HEADER_SIZE + 2] == [0, 0])
        {
            return (result_code == RESULT_CODE_SUCCESS).then(Vec::new);
        }
        let mut first_metadata_pointer: Option<u16> = None;
        let mut name_pointers = Vec::new();
        let mut minimum_name_pointer = usize::MAX;
        let mut terminated = false;
        let mut terminator_body_offset = None;

        for index in 0..usize::from(TX_CHANNELS_PER_PAGE) {
            let record_offset = BODY_HEADER_SIZE + index * TX_RECORD_SIZE;
            let record = body.get(record_offset..record_offset + TX_RECORD_SIZE)?;
            let channel_number = u16_at(record, TX_RECORD_CHANNEL_NUMBER);
            if channel_number == 0 {
                terminated = true;
                terminator_body_offset = Some(record_offset);
                break;
            }
            let expected = expected_channel_number(starting_channel, index)?;
            if channel_number != expected {
                return None;
            }

            let metadata_pointer = u16_at(record, TX_RECORD_CHANNEL_GROUP);
            match first_metadata_pointer {
                None => first_metadata_pointer = Some(metadata_pointer),
                Some(pointer) if metadata_pointer != pointer => return None,
                _ => {}
            }

            let name_pointer = u16_at(record, TX_RECORD_NAME_POINTER);
            let name_pointer_usize = usize::from(name_pointer);
            if name_pointer == 0 || name_pointer_usize >= response.len() {
                return None;
            }
            minimum_name_pointer = minimum_name_pointer.min(name_pointer_usize);
            name_pointers.push(name_pointer);
            channels.push(TxChannel {
                number: channel_number,
                name: None,
                friendly_name: None,
            });
        }

        if channels.len() < usize::from(TX_CHANNELS_PER_PAGE) && !terminated {
            return None;
        }
        if result_code == RESULT_CODE_MORE_PAGES
            && channels.len() != usize::from(TX_CHANNELS_PER_PAGE)
        {
            return None;
        }
        let records_end = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + channels.len() * TX_RECORD_SIZE;
        if channels.is_empty() {
            return None;
        }
        if minimum_name_pointer < records_end {
            return None;
        }
        if let Some(terminator_body_offset) = terminator_body_offset {
            let terminator_absolute = RESPONSE_HEADER_SIZE.checked_add(terminator_body_offset)?;
            if minimum_name_pointer.checked_sub(terminator_absolute)? != 4
                || body.get(terminator_body_offset..terminator_body_offset + 2)? != [0, 0]
            {
                return None;
            }
        }
        for (channel, pointer) in channels.iter_mut().zip(name_pointers) {
            channel.name = Some(pointed_string(response, pointer, minimum_name_pointer)?);
        }
        return Some(channels);
    }

    let maximum_records = usize::from(body_header[0]);
    let channel_count = usize::from(body_header[1]);
    if maximum_records > usize::from(TX_CHANNELS_PER_PAGE) || channel_count > maximum_records {
        return None;
    }
    if result_code == RESULT_CODE_MORE_PAGES
        && (maximum_records == 0 || channel_count != maximum_records)
    {
        return None;
    }
    if channel_count == 0 {
        return (result_code == RESULT_CODE_SUCCESS && body.len() == BODY_HEADER_SIZE)
            .then(Vec::new);
    }

    let records_size = channel_count.checked_mul(TX_RECORD_SIZE)?;
    let records_end = BODY_HEADER_SIZE.checked_add(records_size)?;
    let metadata_pointer = RESPONSE_HEADER_SIZE.checked_add(records_end)?;
    let metadata_pointer = u16::try_from(metadata_pointer).ok()?;
    let metadata_end = usize::from(metadata_pointer).checked_add(16)?;
    let metadata_body_end = metadata_end.checked_sub(RESPONSE_HEADER_SIZE)?;
    body.get(records_end..metadata_body_end)?;

    channels.reserve(channel_count);
    let mut primary_metadata_group_ended = false;
    for index in 0..channel_count {
        let record_offset = BODY_HEADER_SIZE + index * TX_RECORD_SIZE;
        let record = body.get(record_offset..record_offset + TX_RECORD_SIZE)?;
        let channel_number = u16_at(record, TX_RECORD_CHANNEL_NUMBER);
        if channel_number != expected_channel_number(starting_channel, index)? {
            return None;
        }
        let channel_metadata_pointer = usize::from(u16_at(record, TX_RECORD_CHANNEL_GROUP));
        let channel_metadata_end = channel_metadata_pointer.checked_add(16)?;
        response.get(channel_metadata_pointer..channel_metadata_end)?;
        let name_pointer = u16_at(record, TX_RECORD_NAME_POINTER);
        let name = pointed_string(response, name_pointer, channel_metadata_end)?;
        if channel_metadata_pointer != usize::from(metadata_pointer) {
            primary_metadata_group_ended = true;
            continue;
        }
        if primary_metadata_group_ended {
            return None;
        }
        channels.push(TxChannel {
            number: channel_number,
            name: Some(name),
            friendly_name: None,
        });
    }

    (!channels.is_empty()).then_some(channels)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PROTOCOL_ID;

    fn channel_count_response(length: usize) -> Vec<u8> {
        let mut response = vec![0u8; length];
        stamp_response(&mut response, OPCODE_CHANNEL_COUNT, RESULT_CODE_SUCCESS);
        response
    }

    fn stamp_response(response: &mut [u8], opcode: u16, result: u16) {
        let length = response.len() as u16;
        response[0..2].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
        response[2..4].copy_from_slice(&length.to_be_bytes());
        response[6..8].copy_from_slice(&opcode.to_be_bytes());
        response[8..10].copy_from_slice(&result.to_be_bytes());
    }

    fn channel_audio_metadata_response(
        sample_rate: u32,
        current_encoding: u16,
        encoding_capability_bitmap: u16,
    ) -> Vec<u8> {
        let metadata_pointer = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + TX_RECORD_SIZE;
        let name_pointer = metadata_pointer + CHANNEL_AUDIO_METADATA_SIZE;
        let mut response = vec![0u8; name_pointer];
        response.extend_from_slice(b"channel-1\0");
        response[RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE]
            .copy_from_slice(&[1, 1]);
        let record = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE;
        response[record..record + 2].copy_from_slice(&1u16.to_be_bytes());
        response[record + CHANNEL_RECORD_METADATA_POINTER
            ..record + CHANNEL_RECORD_METADATA_POINTER + 2]
            .copy_from_slice(&(metadata_pointer as u16).to_be_bytes());
        response[record + TX_RECORD_NAME_POINTER..record + TX_RECORD_NAME_POINTER + 2]
            .copy_from_slice(&(name_pointer as u16).to_be_bytes());
        response[metadata_pointer + CHANNEL_AUDIO_SAMPLE_RATE_OFFSET
            ..metadata_pointer + CHANNEL_AUDIO_SAMPLE_RATE_OFFSET + 4]
            .copy_from_slice(&sample_rate.to_be_bytes());
        response[metadata_pointer + CHANNEL_AUDIO_CURRENT_ENCODING_OFFSET
            ..metadata_pointer + CHANNEL_AUDIO_CURRENT_ENCODING_OFFSET + 2]
            .copy_from_slice(&current_encoding.to_be_bytes());
        response[metadata_pointer + CHANNEL_AUDIO_ENCODING_CAPABILITY_BITMAP_OFFSET
            ..metadata_pointer + CHANNEL_AUDIO_ENCODING_CAPABILITY_BITMAP_OFFSET + 2]
            .copy_from_slice(&encoding_capability_bitmap.to_be_bytes());
        stamp_response(&mut response, OPCODE_TX_CHANNEL_INFO, RESULT_CODE_SUCCESS);
        response
    }

    #[test]
    fn channel_audio_metadata_decodes_captured_lx_dante_rx_inventory() {
        let response = include_bytes!(
            "../../../tests/fixtures/20250517_200646_289003_lx-dante_get_receivers_response.bin"
        );
        assert_eq!(
            parse_channel_audio_metadata(response),
            Some(ChannelAudioMetadata {
                sample_rate: 48_000,
                current_encoding: 24,
                encoding_capability_bitmap: PCM24_CAPABILITY_BIT,
                supported_encodings: Some(vec![24]),
            })
        );
    }

    #[test]
    fn channel_audio_metadata_decodes_known_pcm_capability_bits() {
        let response = channel_audio_metadata_response(96_000, 24, KNOWN_PCM_CAPABILITY_BITS);
        assert_eq!(
            parse_channel_audio_metadata(&response),
            Some(ChannelAudioMetadata {
                sample_rate: 96_000,
                current_encoding: 24,
                encoding_capability_bitmap: KNOWN_PCM_CAPABILITY_BITS,
                supported_encodings: Some(vec![16, 24, 32]),
            })
        );
    }

    #[test]
    fn channel_audio_metadata_preserves_unknown_bitmap_without_inventing_capabilities() {
        let response = channel_audio_metadata_response(48_000, 24, PCM24_CAPABILITY_BIT | 0x0010);
        let parsed = parse_channel_audio_metadata(&response).unwrap();
        assert_eq!(parsed.current_encoding, 24);
        assert_eq!(parsed.encoding_capability_bitmap, 0x0014);
        assert_eq!(parsed.supported_encodings, None);
    }

    #[test]
    fn channel_audio_metadata_rejects_malformed_inventory_page() {
        let mut response = channel_audio_metadata_response(48_000, 24, PCM24_CAPABILITY_BIT);
        response[RESPONSE_HEADER_SIZE + 1] = 2;
        assert_eq!(parse_channel_audio_metadata(&response), None);
    }

    #[test]
    fn channel_count_parser_does_not_infer_lock_state() {
        let mut response = channel_count_response(36);
        response[12..14].copy_from_slice(&260u16.to_be_bytes());
        response[14..16].copy_from_slice(&520u16.to_be_bytes());
        response[34] = 0x01;
        response[35] = 0x00;
        let parsed = parse_channel_count(&response).unwrap();
        assert_eq!(parsed.tx_count, 260);
        assert_eq!(parsed.rx_count, 520);
        assert_eq!(parsed.locked, None);
    }

    #[test]
    fn channel_count_parser_omits_lock_on_short_response() {
        let mut response = channel_count_response(16);
        response[12..14].copy_from_slice(&2u16.to_be_bytes());
        response[14..16].copy_from_slice(&2u16.to_be_bytes());
        let parsed = parse_channel_count(&response).unwrap();
        assert_eq!(parsed.locked, None);
    }

    #[test]
    fn channel_count_parser_rejects_invalid_response_envelope() {
        let valid = channel_count_response(16);

        let mut wrong_protocol = valid.clone();
        wrong_protocol[0..2].copy_from_slice(&0x1234u16.to_be_bytes());
        assert_eq!(parse_channel_count(&wrong_protocol), None);

        let mut wrong_length = valid.clone();
        wrong_length[2..4].copy_from_slice(&15u16.to_be_bytes());
        assert_eq!(parse_channel_count(&wrong_length), None);

        let mut wrong_opcode = valid.clone();
        wrong_opcode[6..8].copy_from_slice(&0x1002u16.to_be_bytes());
        assert_eq!(parse_channel_count(&wrong_opcode), None);

        let mut failed = valid;
        failed[8..10].copy_from_slice(&0x8001u16.to_be_bytes());
        assert_eq!(parse_channel_count(&failed), None);
    }

    #[test]
    fn rx_parser_decodes_subscription() {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + RX_RECORD_SIZE];
        let strings_base = response.len() as u16;
        response.extend_from_slice(b"rx-1\x00mix-hi\x00mixer\x00");

        let record = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE;
        response[record..record + 2].copy_from_slice(&1u16.to_be_bytes());
        let tx_channel_pointer = strings_base + 5;
        let tx_device_pointer = strings_base + 12;
        let rx_channel_pointer = strings_base;
        response[record + 6..record + 8].copy_from_slice(&tx_channel_pointer.to_be_bytes());
        response[record + 8..record + 10].copy_from_slice(&tx_device_pointer.to_be_bytes());
        response[record + 10..record + 12].copy_from_slice(&rx_channel_pointer.to_be_bytes());
        response[record + 12..record + 14].copy_from_slice(&257u16.to_be_bytes());
        response[record + 14..record + 16].copy_from_slice(&9u16.to_be_bytes());
        response[10..12].copy_from_slice(&[1, 1]);
        stamp_response(&mut response, OPCODE_RX_CHANNELS, RESULT_CODE_SUCCESS);

        let channels = parse_rx_page(&response, 1).unwrap();
        assert_eq!(channels.len(), 1);
        let channel = &channels[0];
        assert_eq!(channel.number, 1);
        assert_eq!(channel.rx_channel_name.as_deref(), Some("rx-1"));
        assert_eq!(channel.tx_channel_name.as_deref(), Some("mix-hi"));
        assert_eq!(channel.tx_device_name.as_deref(), Some("mixer"));
        assert_eq!(channel.rx_status_code, 257);
        assert_eq!(channel.subscription_status_code, 9);
    }

    #[test]
    fn rx_parser_unsubscribed_falls_back_to_rx_name() {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + RX_RECORD_SIZE];
        let strings_base = response.len() as u16;
        response.extend_from_slice(b"unused-1\x00");

        let record = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE;
        response[record..record + 2].copy_from_slice(&1u16.to_be_bytes());
        response[record + 6..record + 8].copy_from_slice(&0u16.to_be_bytes());
        response[record + 8..record + 10].copy_from_slice(&0u16.to_be_bytes());
        response[record + 10..record + 12].copy_from_slice(&strings_base.to_be_bytes());
        response[record + 12..record + 14].copy_from_slice(&0u16.to_be_bytes());
        response[record + 14..record + 16].copy_from_slice(&1u16.to_be_bytes());
        response[10..12].copy_from_slice(&[1, 1]);
        stamp_response(&mut response, OPCODE_RX_CHANNELS, RESULT_CODE_SUCCESS);

        let channels = parse_rx_page(&response, 1).unwrap();
        assert_eq!(channels[0].tx_channel_name.as_deref(), Some("unused-1"));
        assert_eq!(channels[0].tx_device_name, None);
    }

    #[test]
    fn rx_parser_rejects_gap_instead_of_returning_partial_data() {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + RX_RECORD_SIZE];
        response[10..12].copy_from_slice(&[1, 1]);
        stamp_response(&mut response, OPCODE_RX_CHANNELS, RESULT_CODE_SUCCESS);
        assert_eq!(parse_rx_page(&response, 1), None);
    }

    #[test]
    fn rx_parser_accepts_zero_count_padded_final_page() {
        let mut response = vec![
            0u8;
            RESPONSE_HEADER_SIZE
                + BODY_HEADER_SIZE
                + RX_RECORD_SIZE * RX_CHANNELS_PER_PAGE as usize
        ];
        response[10..12].copy_from_slice(&[RX_CHANNELS_PER_PAGE as u8, 0]);
        stamp_response(&mut response, OPCODE_RX_CHANNELS, RESULT_CODE_SUCCESS);

        assert_eq!(parse_rx_page(&response, 49), Some(Vec::new()));

        let last = response.len() - 1;
        response[last] = 1;
        assert_eq!(parse_rx_page(&response, 49), None);
    }

    #[test]
    fn rx_parser_rejects_zero_count_padded_intermediate_page() {
        let mut response = vec![
            0u8;
            RESPONSE_HEADER_SIZE
                + BODY_HEADER_SIZE
                + RX_RECORD_SIZE * RX_CHANNELS_PER_PAGE as usize
        ];
        response[10..12].copy_from_slice(&[RX_CHANNELS_PER_PAGE as u8, 0]);
        stamp_response(&mut response, OPCODE_RX_CHANNELS, RESULT_CODE_MORE_PAGES);

        assert_eq!(parse_rx_page(&response, 49), None);
    }

    #[test]
    fn rx_parser_stops_before_channel_number_wrap_on_final_page() {
        let mut response = vec![
            0u8;
            RESPONSE_HEADER_SIZE
                + BODY_HEADER_SIZE
                + RX_RECORD_SIZE * RX_CHANNELS_PER_PAGE as usize
        ];
        let valid_record_count = RX_CHANNELS_PER_PAGE as usize - 1;
        let starting_channel = u16::MAX - (valid_record_count as u16 - 1);

        for index in 0..valid_record_count {
            let record = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + index * RX_RECORD_SIZE;
            let channel_number = starting_channel.checked_add(index as u16).unwrap();
            response[record..record + 2].copy_from_slice(&channel_number.to_be_bytes());
        }
        response[10..12].copy_from_slice(&[16, 16]);
        stamp_response(&mut response, OPCODE_RX_CHANNELS, RESULT_CODE_MORE_PAGES);

        assert_eq!(parse_rx_page(&response, starting_channel), None);
    }

    #[test]
    fn tx_info_parser_rejects_channel_group_change() {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + TX_RECORD_SIZE * 2];
        let strings_base = response.len() as u16;
        response.extend_from_slice(b"ch-1\x00ch-2\x00");

        let first = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE;
        response[first..first + 2].copy_from_slice(&1u16.to_be_bytes());
        response[first + 4..first + 6].copy_from_slice(&0x0100u16.to_be_bytes());
        response[first + 6..first + 8].copy_from_slice(&strings_base.to_be_bytes());

        let second = first + TX_RECORD_SIZE;
        response[second..second + 2].copy_from_slice(&2u16.to_be_bytes());
        response[second + 4..second + 6].copy_from_slice(&0x0200u16.to_be_bytes());
        response[second + 6..second + 8].copy_from_slice(&(strings_base + 5).to_be_bytes());
        stamp_response(&mut response, OPCODE_TX_CHANNEL_INFO, RESULT_CODE_SUCCESS);

        assert_eq!(parse_tx_info_page(&response, 1), None);
    }

    #[test]
    fn tx_info_parser_accepts_short_empty_page_shapes() {
        let mut short_response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE];
        stamp_response(
            &mut short_response,
            OPCODE_TX_CHANNEL_INFO,
            RESULT_CODE_SUCCESS,
        );
        assert_eq!(parse_tx_info_page(&short_response, 1), Some(Vec::new()));

        let mut padded_response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + 4];
        padded_response[RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + 2..]
            .copy_from_slice(&[0xBB, 0x80]);
        stamp_response(
            &mut padded_response,
            OPCODE_TX_CHANNEL_INFO,
            RESULT_CODE_SUCCESS,
        );
        assert_eq!(parse_tx_info_page(&padded_response, 1), Some(Vec::new()));

        padded_response[RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE] = 1;
        assert_eq!(parse_tx_info_page(&padded_response, 1), None);
    }

    #[test]
    fn tx_info_parser_stops_before_channel_number_wrap_on_final_page() {
        let mut response = vec![
            0u8;
            RESPONSE_HEADER_SIZE
                + BODY_HEADER_SIZE
                + TX_RECORD_SIZE * TX_CHANNELS_PER_PAGE as usize
        ];
        let valid_record_count = TX_CHANNELS_PER_PAGE as usize - 1;
        let starting_channel = u16::MAX - (valid_record_count as u16 - 1);

        for index in 0..valid_record_count {
            let record = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + index * TX_RECORD_SIZE;
            let channel_number = starting_channel.checked_add(index as u16).unwrap();
            response[record..record + 2].copy_from_slice(&channel_number.to_be_bytes());
            response[record + 4..record + 6].copy_from_slice(&1u16.to_be_bytes());
        }
        stamp_response(&mut response, OPCODE_TX_CHANNEL_INFO, RESULT_CODE_SUCCESS);

        assert_eq!(parse_tx_info_page(&response, starting_channel), None);
    }

    #[test]
    fn page_parsers_never_accept_truncated_prefixes() {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE];
        stamp_response(&mut response, OPCODE_RX_CHANNELS, RESULT_CODE_SUCCESS);
        for length in 0..response.len() {
            assert_eq!(parse_rx_page(&response[..length], 1), None);
            assert_eq!(parse_tx_info_page(&response[..length], 1), None);
            assert_eq!(parse_tx_friendly_page(&response[..length], 1), None);
        }
    }

    #[test]
    fn tx_info_zero_channel_cannot_hide_later_records() {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + TX_RECORD_SIZE * 2];
        let later_record = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + TX_RECORD_SIZE;
        response[later_record..later_record + 2].copy_from_slice(&1u16.to_be_bytes());
        stamp_response(&mut response, OPCODE_TX_CHANNEL_INFO, RESULT_CODE_SUCCESS);

        assert_eq!(parse_tx_info_page(&response, 1), None);
    }

    #[test]
    fn hostile_bytes_never_panic_or_return_partial_pages() {
        for length in 0..512usize {
            let data: Vec<u8> = (0..length)
                .map(|index| ((index * 31 + length * 47) & 0xFF) as u8)
                .collect();
            let result = std::panic::catch_unwind(|| {
                let _ = parse_channel_count(&data);
                let _ = parse_channel_audio_metadata(&data);
                let _ = parse_rx_page(&data, 1);
                let _ = parse_tx_info_page(&data, 1);
                let _ = parse_tx_friendly_page(&data, 1);
            });
            assert!(result.is_ok(), "length={length}");
            assert_eq!(parse_rx_page(&data, 1), None);
            assert_eq!(parse_tx_info_page(&data, 1), None);
            assert_eq!(parse_tx_friendly_page(&data, 1), None);
        }
    }
}
