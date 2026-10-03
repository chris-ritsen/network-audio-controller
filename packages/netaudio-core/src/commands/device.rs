use super::*;

pub fn build_device_info(message_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_device_info_for_protocol(crate::protocol::PROTOCOL_ID, message_id)
}

pub fn build_device_info_for_protocol(
    protocol_id: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_common_device_query(
        protocol_id,
        OPCODE_DEVICE_INFO,
        &[0x00, 0x00],
        message_id,
    )
}

pub fn build_device_name(message_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_DEVICE_NAME, &[0x00, 0x00], message_id)
}

pub fn build_channel_count(message_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_channel_count_for_protocol(crate::protocol::PROTOCOL_ID, message_id)
}

pub fn build_channel_count_for_protocol(
    protocol_id: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_common_device_query(
        protocol_id,
        OPCODE_CHANNEL_COUNT,
        &[0x00, 0x00],
        message_id,
    )
}

pub fn build_device_settings(message_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_DEVICE_SETTINGS, &[0x00, 0x00], message_id)
}

pub fn build_property_directory(message_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_property_directory_for_protocol(crate::protocol::PROTOCOL_ID, message_id)
}

pub fn build_property_directory_for_protocol(
    protocol_id: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_common_device_query(
        protocol_id,
        OPCODE_PROPERTY_DIRECTORY,
        &[0x00, 0x00],
        message_id,
    )
}

fn build_common_device_query(
    protocol_id: u16,
    opcode: u16,
    body: &[u8],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !matches!(
        protocol_id,
        crate::protocol::PROTOCOL_ID | crate::protocol::PROTOCOL_ARC_2809
    ) {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }
    build_control_packet_for_protocol(protocol_id, opcode, body, message_id)
}

pub fn build_reset_name(message_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_DEVICE_NAME_SET, &[0x00, 0x00], message_id)
}

fn page_starting_channel(page: u16, channels_per_page: u16) -> Result<u16, NetaudioError> {
    page.checked_mul(channels_per_page)
        .and_then(|offset| offset.checked_add(1))
        .ok_or(NetaudioError::InvalidPage)
}

pub fn build_receivers(page: u16, message_id: u16) -> Result<Vec<u8>, NetaudioError> {
    let starting_channel = page_starting_channel(page, 16)?;
    build_control_packet(
        OPCODE_RX_CHANNELS,
        &channel_query_payload(starting_channel),
        message_id,
    )
}

pub fn build_transmitters(
    page: u16,
    friendly_names: bool,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if friendly_names {
        return Err(NetaudioError::InvalidChannel);
    }
    let starting_channel = page_starting_channel(page, 32)?;
    build_control_packet(
        OPCODE_TX_CHANNEL_INFO,
        &channel_query_payload(starting_channel),
        message_id,
    )
}

pub fn build_transmitter_names(
    channel_count: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_transmitter_names_for_protocol(
        crate::protocol::PROTOCOL_ID,
        channel_count,
        message_id,
    )
}

pub fn build_transmitter_names_for_protocol(
    protocol_id: u16,
    channel_count: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if channel_count == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    if !matches!(
        protocol_id,
        crate::protocol::PROTOCOL_ID | crate::protocol::PROTOCOL_ARC_2809
    ) {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }
    build_control_packet_for_protocol(
        protocol_id,
        OPCODE_TX_CHANNEL_NAMES,
        &channel_range_query_payload(1, channel_count),
        message_id,
    )
}

fn channel_name_payload(
    channel_type: ChannelType,
    channel_number: u8,
    name: Option<&str>,
) -> Vec<u8> {
    let mut payload = Vec::new();
    match channel_type {
        ChannelType::Rx => {
            payload.extend_from_slice(&[0x00, 0x00, 0x02, 0x01, 0x00, channel_number]);
            payload.extend_from_slice(&0x14u16.to_be_bytes());
            payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        }
        ChannelType::Tx => {
            payload.extend_from_slice(&[0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, channel_number]);
            payload.extend_from_slice(&0x18u16.to_be_bytes());
            payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        }
    }
    if let Some(name) = name {
        payload.extend_from_slice(name.as_bytes());
        payload.push(0);
    }
    payload
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelType {
    Rx,
    Tx,
}

#[derive(Debug, serde::Serialize)]
pub struct ChannelNameChange {
    pub channel_type: ChannelType,
    pub channel_number: u16,
    pub name: String,
}

pub fn parse_set_channel_name_request(data: &[u8]) -> Option<ChannelNameChange> {
    use crate::bytes::{read_u16, string_at_pointer};

    let protocol = read_u16(data, 0)?;
    let opcode = read_u16(data, 6)?;
    let (channel_type, channel_offset, pointer_offset) = match (protocol, opcode) {
        (PROTOCOL_DANTE_FLOW, OPCODE_RX_CHANNEL_NAME_SET) => (ChannelType::Rx, 12, 14),
        (
            PROTOCOL_DANTE_FLOW | PROTOCOL_ARC_2809 | crate::protocol::PROTOCOL_ARC_280F,
            OPCODE_TX_CHANNEL_NAME_SET,
        ) => (ChannelType::Tx, 14, 16),
        (
            PROTOCOL_ARC_2809 | crate::protocol::PROTOCOL_ARC_280F,
            OPCODE_SET_RECEIVER_CHANNEL_NAME_2809,
        ) => (ChannelType::Rx, 20, 24),
        _ => return None,
    };
    let channel_number = read_u16(data, channel_offset)?;
    let pointer = read_u16(data, pointer_offset)?;
    let name = string_at_pointer(data, pointer)?;

    // Accept the supported single-record layout; the encoder validates the name,
    // channel range, reserved fields, pointers, and complete frame length.
    let expected = build_set_channel_name_for_protocol(
        protocol,
        channel_type,
        channel_number,
        &name,
        read_u16(data, 4)?,
    )
    .ok()?;

    if data != expected {
        return None;
    }

    Some(ChannelNameChange {
        channel_type,
        channel_number,
        name,
    })
}

pub fn build_reset_channel_name(
    channel_type: ChannelType,
    channel_number: u8,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if channel_number == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    let opcode = match channel_type {
        ChannelType::Rx => OPCODE_RX_CHANNEL_NAME_SET,
        ChannelType::Tx => OPCODE_TX_CHANNEL_NAME_SET,
    };
    build_control_packet(
        opcode,
        &channel_name_payload(channel_type, channel_number, None),
        message_id,
    )
}

pub fn build_set_channel_name_for_protocol(
    protocol_id: u16,
    channel_type: ChannelType,
    channel_number: u16,
    name: &str,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if channel_number == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    validate_dante_channel_name(name)?;
    match (protocol_id, channel_type) {
        (PROTOCOL_DANTE_FLOW, channel_type) => {
            let channel_number =
                u8::try_from(channel_number).map_err(|_| NetaudioError::InvalidChannel)?;
            let opcode = match channel_type {
                ChannelType::Rx => OPCODE_RX_CHANNEL_NAME_SET,
                ChannelType::Tx => OPCODE_TX_CHANNEL_NAME_SET,
            };
            build_control_packet_for_protocol(
                protocol_id,
                opcode,
                &channel_name_payload(channel_type, channel_number, Some(name)),
                message_id,
            )
        }
        (PROTOCOL_ARC_2809 | crate::protocol::PROTOCOL_ARC_280F, ChannelType::Rx) => {
            build_set_receiver_channel_name_for_protocol(
                protocol_id,
                channel_number,
                name,
                message_id,
            )
        }
        (PROTOCOL_ARC_2809 | crate::protocol::PROTOCOL_ARC_280F, ChannelType::Tx) => {
            let channel_number =
                u8::try_from(channel_number).map_err(|_| NetaudioError::InvalidChannel)?;
            build_control_packet_for_protocol(
                protocol_id,
                OPCODE_TX_CHANNEL_NAME_SET,
                &channel_name_payload(ChannelType::Tx, channel_number, Some(name)),
                message_id,
            )
        }
        _ => Err(NetaudioError::UnsupportedProtocolOperation),
    }
}
