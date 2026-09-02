use super::*;

pub fn build_volume_start(
    device_name: &str,
    ipv4: [u8; 4],
    mac: [u8; 6],
    port: u16,
    timeout: bool,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    validate_dante_name(device_name)?;
    let mut name_bytes = device_name.as_bytes().to_vec();
    name_bytes.push(0);
    if name_bytes.len() % 2 != 0 {
        name_bytes.push(0);
    }
    let padded_name_len = name_bytes.len();

    let offset_field_1 = padded_name_len
        .checked_add(0x0A)
        .and_then(|offset| u16::try_from(offset).ok())
        .ok_or(NetaudioError::PacketTooLarge)?;
    let offset_field_2 = padded_name_len
        .checked_add(0x0C)
        .and_then(|offset| u16::try_from(offset).ok())
        .ok_or(NetaudioError::PacketTooLarge)?;
    let tail_offset = offset_field_2
        .checked_add(4)
        .ok_or(NetaudioError::PacketTooLarge)?;

    let mut body = Vec::new();
    body.extend_from_slice(&0x3010u16.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&mac);
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&4u16.to_be_bytes());
    body.extend_from_slice(&offset_field_1.to_be_bytes());
    body.extend_from_slice(&2u16.to_be_bytes());
    body.extend_from_slice(&offset_field_2.to_be_bytes());
    body.extend_from_slice(&0x000Au16.to_be_bytes());
    body.extend_from_slice(&name_bytes);
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&tail_offset.to_be_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&port.to_be_bytes());
    body.extend_from_slice(&(if timeout { 1u16 } else { 0u16 }).to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&ipv4);
    body.extend_from_slice(&port.to_be_bytes());
    body.extend(std::iter::repeat_n(0, 10));

    ConmonHeader {
        message_id,
        protocol_id: PROTOCOL_CMC,
    }
    .packet(&body)
}

pub fn build_volume_stop(device_name: &str, mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    build_volume_start(device_name, [0u8; 4], mac, 0, false, 0)
}
