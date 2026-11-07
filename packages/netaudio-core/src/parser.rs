pub mod audio;

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
const RX_RECORD_FLAGS: usize = 2;
const RX_RECORD_TX_CHANNEL_POINTER: usize = 6;
const RX_RECORD_TX_DEVICE_POINTER: usize = 8;
const RX_RECORD_RX_CHANNEL_POINTER: usize = 10;
const RX_RECORD_RX_STATUS: usize = 12;
const RX_RECORD_SUBSCRIPTION_STATUS: usize = 14;
const RX_RECORD_CAN_SUBSCRIBE_SELF_MASK: u16 = 0x0008;
const RX_RECORD_RENAME_PROHIBITED_MASK: u16 = 0x0400;

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
const PCM_ENCODINGS: [(u16, u16); 3] = [
    (16, PCM16_CAPABILITY_BIT),
    (24, PCM24_CAPABILITY_BIT),
    (32, PCM32_CAPABILITY_BIT),
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowAuthoringCapabilities {
    pub transmit_flow_authoring: FlowAuthoringProfile,
    pub receiver_flow_query_family: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowAuthoringProfile {
    pub family: &'static str,
    pub opcode: u16,
    pub identity_field: &'static str,
    pub identifier_max: u16,
    pub media_modes: Vec<&'static str>,
    pub supports_flow_options: bool,
}

pub fn flow_authoring_capabilities(capability_word: u16) -> FlowAuthoringCapabilities {
    if capability_word & 0x1000 != 0 {
        FlowAuthoringCapabilities {
            transmit_flow_authoring: FlowAuthoringProfile {
                family: "segmented",
                opcode: crate::commands::OPCODE_CREATE_TX_FLOW_2809,
                identity_field: "media_local_flow_id",
                identifier_max: u16::MAX,
                media_modes: vec!["native_dante", "rtp_aes67"],
                supports_flow_options: true,
            },
            receiver_flow_query_family: "segmented",
        }
    } else {
        FlowAuthoringCapabilities {
            transmit_flow_authoring: FlowAuthoringProfile {
                family: "fixed",
                opcode: crate::commands::OPCODE_CREATE_TX_FLOW,
                identity_field: "global_flow_id",
                identifier_max: crate::commands::MAX_LEGACY_FLOW_ID,
                media_modes: vec!["native_dante", "rtp_aes67"],
                supports_flow_options: true,
            },
            receiver_flow_query_family: "fixed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelCount {
    pub receiver_telemetry_capacity: serde_json::Value,
    pub transmit_flow_authoring_capability_word: u16,
    pub uses_modern_transmit_flow_authoring: bool,
    pub tx_count: u16,
    pub rx_count: u16,
    pub maximum_transmit_flow_channel_slots: Option<u16>,
    pub maximum_receive_flow_channel_slots: Option<u16>,
    pub maximum_transmit_flows: Option<u16>,
    pub maximum_receive_flows: Option<u16>,
    pub locked: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RxChannel {
    pub number: u16,
    pub receiver_flags: u16,
    pub can_subscribe_self: bool,
    pub can_rename: bool,
    pub rx_channel_name: Option<String>,
    pub tx_channel_name: Option<String>,
    pub tx_device_name: Option<String>,
    pub is_self_connection: bool,
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

#[derive(serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ChannelAudioConfiguration {
    pub sample_rate: u32,
    pub encoding: u16,
    pub supported_encodings: Vec<u16>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ChannelAudioPublication {
    pub channel_metadata: Vec<u8>,
    pub pcm_property: String,
}

pub fn channel_audio_publication(
    config: &ChannelAudioConfiguration,
) -> Option<ChannelAudioPublication> {
    if config.sample_rate == 0 || !config.supported_encodings.contains(&config.encoding) {
        return None;
    }

    let mut bitmap = 0u16;

    for encoding in &config.supported_encodings {
        let (_, bit) = PCM_ENCODINGS.iter().find(|(known, _)| known == encoding)?;
        bitmap |= bit;
    }

    let mut metadata = Vec::with_capacity(CHANNEL_AUDIO_METADATA_SIZE);
    metadata.extend_from_slice(&config.sample_rate.to_be_bytes());

    for value in [
        0x0101,
        config.encoding,
        0x0400,
        config.encoding,
        config.encoding,
        bitmap,
    ] {
        metadata.extend_from_slice(&value.to_be_bytes());
    }

    Some(ChannelAudioPublication {
        channel_metadata: metadata,
        pcm_property: format!("{} 0x{bitmap:x}", config.encoding / 8),
    })
}

pub fn parse_channel_audio_metadata(response: &[u8]) -> Option<ChannelAudioMetadata> {
    let envelope = response_envelope(response)?;
    if !is_common_arc_protocol(envelope.protocol_id)
        || ![OPCODE_TX_CHANNEL_INFO, OPCODE_RX_CHANNELS].contains(&envelope.opcode)
        || ![RESULT_CODE_SUCCESS, RESULT_CODE_MORE_PAGES].contains(&envelope.result_code)
    {
        return None;
    }

    if envelope.opcode == OPCODE_TX_CHANNEL_INFO && *envelope.body.first()? != 0 {
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
        for (encoding, capability_bit) in PCM_ENCODINGS {
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
    let transmit_flow_authoring_capability_word = read_u16(response, RESPONSE_HEADER_SIZE)?;
    let revision = read_u16(response, 0)?;
    let field = |minimum, offset| {
        (revision >= minimum)
            .then(|| read_u16(response, offset))
            .flatten()
    };
    Some(ChannelCount {
        receiver_telemetry_capacity: serde_json::json!({
            "protocol_id": revision,
            "base_transmit_flow_capacity": field(0x2600, 0x16),
            "base_receive_flow_capacity": field(0x2600, 0x18),
            "network_interface_count": field(0x2600, 0x1e),
            "receiver_error_flag_mask": field(0x2711, 0x24),
            "receiver_error_field_mask": field(0x2711, 0x26),
            "resource_extension_offset": if revision < 0x2802 { Some(0) } else { field(0x2802, 0x2c) },
            "raw_response": response,
        }),
        transmit_flow_authoring_capability_word,
        uses_modern_transmit_flow_authoring: flow_authoring_capabilities(
            transmit_flow_authoring_capability_word,
        )
        .transmit_flow_authoring
        .family
            == "segmented",
        tx_count: read_u16(response, CHANNEL_COUNT_TX_OFFSET)?,
        rx_count: read_u16(response, CHANNEL_COUNT_RX_OFFSET)?,
        maximum_transmit_flow_channel_slots: read_u16(response, 0x12),
        maximum_receive_flow_channel_slots: read_u16(response, 0x14),
        maximum_transmit_flows: field(0x2600, 0x16),
        maximum_receive_flows: field(0x2600, 0x18),
        locked: None,
    })
}

fn expected_channel_number(starting_channel: u16, index: usize) -> Option<u16> {
    let offset = u16::try_from(index).ok()?;
    starting_channel.checked_add(offset)
}

pub fn parse_channel_page_start(packet: &[u8]) -> Option<Option<u16>> {
    let envelope = response_envelope(packet)?;
    if !is_common_arc_protocol(envelope.protocol_id)
        || ![
            OPCODE_RX_CHANNELS,
            OPCODE_TX_CHANNEL_INFO,
            OPCODE_TX_CHANNEL_NAMES,
        ]
        .contains(&envelope.opcode)
    {
        return None;
    }
    if envelope.result_code == 0 {
        if envelope.body.len() != 6 || read_u16(envelope.body, 0)? != 1 {
            return None;
        }
        let start = read_u16(envelope.body, 2)?;
        let end = read_u16(envelope.body, 4)?;
        return (start != 0 && (end == 0 || end >= start)).then_some(Some(start));
    }
    if ![RESULT_CODE_SUCCESS, RESULT_CODE_MORE_PAGES].contains(&envelope.result_code) {
        return None;
    }
    let offset = if envelope.opcode == OPCODE_TX_CHANNEL_NAMES {
        4
    } else {
        2
    };
    let start = read_u16(envelope.body, offset).unwrap_or(0).max(1);
    // A standalone dissection can infer its page origin, but still validates the full page.
    let count = match envelope.opcode {
        OPCODE_RX_CHANNELS => parse_rx_page(packet, start)?.len(),
        OPCODE_TX_CHANNEL_INFO => parse_tx_info_page(packet, start)?.len(),
        OPCODE_TX_CHANNEL_NAMES => parse_tx_friendly_page(packet, start)?.len(),
        _ => return None,
    };
    Some((count != 0).then_some(start))
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
        return (body.len() == padded_records_end).then(Vec::new);
    }
    let table_end = BODY_HEADER_SIZE.checked_add(maximum_records.checked_mul(RX_RECORD_SIZE)?)?;
    body.get(..table_end)?;
    let minimum_pointer = RESPONSE_HEADER_SIZE.checked_add(table_end)?;
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
        let tx_channel_name =
            optional_pointed_string(response, tx_channel_pointer, minimum_pointer)?;

        let receiver_flags = u16_at(record, RX_RECORD_FLAGS);
        let subscription_status_code = u16_at(record, RX_RECORD_SUBSCRIPTION_STATUS);
        let is_self_connection = crate::subscription_status::is_self_connection(
            tx_device_name.as_deref(),
            subscription_status_code,
        );

        channels.push(RxChannel {
            number: channel_number,
            receiver_flags,
            can_subscribe_self: receiver_flags & RX_RECORD_CAN_SUBSCRIBE_SELF_MASK != 0,
            can_rename: receiver_flags & RX_RECORD_RENAME_PROHIBITED_MASK == 0,
            rx_channel_name,
            tx_channel_name,
            tx_device_name,
            is_self_connection,
            rx_status_code: u16_at(record, RX_RECORD_RX_STATUS),
            subscription_status_code,
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
    if named_count == 0 {
        let padded_records_size = maximum_records.checked_mul(TX_RECORD_SIZE)?;
        let padded_records_end = BODY_HEADER_SIZE.checked_add(padded_records_size)?;
        return (body.len() == padded_records_end).then(Vec::new);
    }
    body.get(..live_records_end)?;
    let minimum_name_pointer = RESPONSE_HEADER_SIZE.checked_add(live_records_end)?;
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

    let page = audio::parse_transmit_channel_page(response)?;
    let mut channels = Vec::with_capacity(page.channels.len());

    for (index, channel) in page.channels.into_iter().enumerate() {
        if channel.identifier != expected_channel_number(starting_channel, index)? {
            return None;
        }

        let name = match channel.name {
            audio::Field::Present(bytes) => Some(String::from_utf8(bytes).ok()?),
            audio::Field::Missing => None,
            audio::Field::Invalid(_) => return None,
        };
        channels.push(TxChannel {
            number: channel.identifier,
            name,
            friendly_name: None,
        });
    }

    Some(channels)
}
