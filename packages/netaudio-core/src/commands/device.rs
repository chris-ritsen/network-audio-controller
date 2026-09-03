use super::*;

pub fn build_device_info(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_device_info_for_protocol(crate::protocol::PROTOCOL_ID, transaction_id)
}

pub fn build_device_info_for_protocol(
    protocol_id: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_common_device_query(
        protocol_id,
        OPCODE_DEVICE_INFO,
        &[0x00, 0x00],
        transaction_id,
    )
}

pub fn build_device_name(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_DEVICE_NAME, &[0x00, 0x00], transaction_id)
}

pub fn build_channel_count(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_channel_count_for_protocol(crate::protocol::PROTOCOL_ID, transaction_id)
}

pub fn build_channel_count_for_protocol(
    protocol_id: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_common_device_query(
        protocol_id,
        OPCODE_CHANNEL_COUNT,
        &[0x00, 0x00],
        transaction_id,
    )
}

pub fn build_device_settings(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_DEVICE_SETTINGS, &[0x00, 0x00], transaction_id)
}

pub fn build_property_directory(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_property_directory_for_protocol(crate::protocol::PROTOCOL_ID, transaction_id)
}

pub fn build_property_directory_for_protocol(
    protocol_id: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_common_device_query(
        protocol_id,
        OPCODE_PROPERTY_DIRECTORY,
        &[0x00, 0x00],
        transaction_id,
    )
}

fn build_common_device_query(
    protocol_id: u16,
    opcode: u16,
    body: &[u8],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !matches!(
        protocol_id,
        crate::protocol::PROTOCOL_ID | crate::protocol::PROTOCOL_ARC_2809
    ) {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }
    build_control_packet_for_protocol(protocol_id, opcode, body, transaction_id)
}

pub fn build_reset_name(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_DEVICE_NAME_SET, &[0x00, 0x00], transaction_id)
}

fn page_starting_channel(page: u16, channels_per_page: u16) -> Result<u16, NetaudioError> {
    page.checked_mul(channels_per_page)
        .and_then(|offset| offset.checked_add(1))
        .ok_or(NetaudioError::InvalidPage)
}

pub fn build_receivers(page: u16, transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    let starting_channel = page_starting_channel(page, 16)?;
    build_control_packet(
        OPCODE_RX_CHANNELS,
        &channel_query_payload(starting_channel),
        transaction_id,
    )
}

pub fn build_transmitters(
    page: u16,
    friendly_names: bool,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if friendly_names {
        return Err(NetaudioError::InvalidChannel);
    }
    let starting_channel = page_starting_channel(page, 32)?;
    build_control_packet(
        OPCODE_TX_CHANNEL_INFO,
        &channel_query_payload(starting_channel),
        transaction_id,
    )
}

pub fn build_transmitter_names(
    channel_count: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_transmitter_names_for_protocol(
        crate::protocol::PROTOCOL_ID,
        channel_count,
        transaction_id,
    )
}

pub fn build_transmitter_names_for_protocol(
    protocol_id: u16,
    channel_count: u16,
    transaction_id: u16,
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
        transaction_id,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelType {
    Rx,
    Tx,
}

pub fn build_reset_channel_name(
    channel_type: ChannelType,
    channel_number: u8,
    transaction_id: u16,
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
        transaction_id,
    )
}

pub fn build_set_channel_name(
    channel_type: ChannelType,
    channel_number: u8,
    name: &str,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_set_channel_name_for_protocol(
        PROTOCOL_DANTE_FLOW,
        channel_type,
        u16::from(channel_number),
        name,
        transaction_id,
    )
}

pub fn build_set_channel_name_for_protocol(
    protocol_id: u16,
    channel_type: ChannelType,
    channel_number: u16,
    name: &str,
    transaction_id: u16,
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
                transaction_id,
            )
        }
        (PROTOCOL_ARC_2809, ChannelType::Rx) => {
            build_set_receiver_channel_name_2809(channel_number, name, transaction_id)
        }
        (PROTOCOL_ARC_2809, ChannelType::Tx) => {
            let channel_number =
                u8::try_from(channel_number).map_err(|_| NetaudioError::InvalidChannel)?;
            build_control_packet_for_protocol(
                protocol_id,
                OPCODE_TX_CHANNEL_NAME_SET,
                &channel_name_payload(ChannelType::Tx, channel_number, Some(name)),
                transaction_id,
            )
        }
        _ => Err(NetaudioError::UnsupportedProtocolOperation),
    }
}
