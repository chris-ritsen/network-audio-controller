use super::*;
use crate::bytes::read_u16;

pub const OPCODE_CMC_METERING: u16 = 0x3010;
pub const METERING_SUBSCRIPTION_CLASS: u16 = 1;

const SUBSCRIPTION_HEAP_BASE: usize = 20;
const SUBSCRIPTION_CLIENT_POINTER: usize = SUBSCRIPTION_HEAP_BASE + 2;
const SUBSCRIPTION_SLOT_COUNT: usize = SUBSCRIPTION_HEAP_BASE + 4;
const SUBSCRIPTION_LIST_POINTER: usize = SUBSCRIPTION_HEAP_BASE + 6;
const SUBSCRIPTION_TRANSMITTER_POINTER: usize = SUBSCRIPTION_HEAP_BASE + 8;
const SUBSCRIPTION_RECORD_HEADER_SIZE: usize = 6;
const SUBSCRIPTION_SLOT_SIZE: usize = 8;
const METERING_FRAME_VERSION: u8 = 2;

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct MeteringDestination {
    pub ipv4: std::net::Ipv4Addr,
    pub port: u16,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct MeteringRequest {
    pub destination: Option<MeteringDestination>,
    pub discriminator: u16,
    pub owner: String,
    pub transaction_id: u16,
    pub transmitter_name: String,
}

struct SubscriptionLayout {
    client_offset: usize,
    discriminator: u16,
    slots_offset: usize,
    slot_count: usize,
    transmitter_name: String,
}

fn heap_offset(data: &[u8], pointer_offset: usize) -> Option<usize> {
    let offset =
        SUBSCRIPTION_HEAP_BASE.checked_add(usize::from(read_u16(data, pointer_offset)?))?;
    (offset < data.len()).then_some(offset)
}

fn subscription_layout(data: &[u8]) -> Option<SubscriptionLayout> {
    if read_u16(data, 0)? != PROTOCOL_CMC
        || usize::from(read_u16(data, 2)?) != data.len()
        || read_u16(data, 6)? != OPCODE_CMC_METERING
        || read_u16(data, 8)? != 0
    {
        return None;
    }

    let transmitter_offset = heap_offset(data, SUBSCRIPTION_TRANSMITTER_POINTER)?;
    let transmitter_length = data
        .get(transmitter_offset..)?
        .iter()
        .position(|byte| *byte == 0)?;
    let list_offset = heap_offset(data, SUBSCRIPTION_LIST_POINTER)?;

    if read_u16(data, list_offset)? != 1 {
        return None;
    }

    let record_offset =
        SUBSCRIPTION_HEAP_BASE.checked_add(usize::from(read_u16(data, list_offset + 2)?))?;

    if read_u16(data, record_offset)? != METERING_SUBSCRIPTION_CLASS {
        return None;
    }

    let slot_count = usize::from(read_u16(data, SUBSCRIPTION_SLOT_COUNT)?);
    let slots_offset = record_offset + SUBSCRIPTION_RECORD_HEADER_SIZE;

    if slot_count == 0 || slots_offset + slot_count * SUBSCRIPTION_SLOT_SIZE > data.len() {
        return None;
    }

    Some(SubscriptionLayout {
        client_offset: heap_offset(data, SUBSCRIPTION_CLIENT_POINTER)?,
        discriminator: read_u16(data, record_offset + 2)?,
        slots_offset,
        slot_count,
        transmitter_name: String::from_utf8(
            data.get(transmitter_offset..transmitter_offset + transmitter_length)?
                .to_vec(),
        )
        .ok()?,
    })
}

pub fn parse_metering_request(data: &[u8]) -> Option<MeteringRequest> {
    let layout = subscription_layout(data)?;
    let ipv4: [u8; 4] = data
        .get(layout.slots_offset + 2..layout.slots_offset + 6)?
        .try_into()
        .ok()?;
    let port = read_u16(data, layout.slots_offset + 6)?;

    Some(MeteringRequest {
        destination: (ipv4 != [0; 4] && port != 0).then(|| MeteringDestination {
            ipv4: ipv4.into(),
            port,
        }),
        discriminator: layout.discriminator,
        owner: crate::bytes::hexadecimal(data.get(10..20)?),
        transaction_id: read_u16(data, 4)?,
        transmitter_name: layout.transmitter_name,
    })
}

pub fn build_metering_reply(
    request: &[u8],
    responder_process: u16,
    responder_identity: [u8; 8],
) -> Result<Vec<u8>, NetaudioError> {
    let layout = subscription_layout(request).ok_or(NetaudioError::UnsupportedProtocolOperation)?;
    let mut reply = request.to_vec();
    reply[8..10].copy_from_slice(&crate::protocol::RESULT_CODE_SUCCESS.to_be_bytes());
    reply[10..12].copy_from_slice(&responder_process.to_be_bytes());
    reply[12..20].copy_from_slice(&responder_identity);
    reply[layout.client_offset..layout.client_offset + 2].fill(0);

    for slot in 1..layout.slot_count {
        let port_offset = layout.slots_offset + slot * SUBSCRIPTION_SLOT_SIZE + 6;
        reply[port_offset..port_offset + 2].fill(0);
    }

    Ok(reply)
}

pub fn build_metering_frame(
    source_identifier: [u8; 8],
    sequence: u16,
    tx_levels: &[u8],
    rx_levels: &[u8],
) -> Result<Vec<u8>, NetaudioError> {
    let tx_count = u8::try_from(tx_levels.len()).map_err(|_| NetaudioError::PacketTooLarge)?;
    let rx_count = u8::try_from(rx_levels.len()).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut body = Vec::with_capacity(22 + tx_levels.len() + rx_levels.len());
    body.extend_from_slice(&[0, 0]);
    body.extend_from_slice(&source_identifier);
    body.extend_from_slice(b"Audinate");
    body.extend_from_slice(&[METERING_FRAME_VERSION, tx_count, rx_count]);
    body.extend_from_slice(tx_levels);
    body.extend_from_slice(rx_levels);
    body.push(0);

    ConmonHeader {
        message_id: sequence,
        protocol_id: 0xFFFF,
    }
    .packet(&body)
}

pub fn build_metering_start(
    device_name: &str,
    ipv4: [u8; 4],
    mac: [u8; 6],
    port: u16,
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
    body.extend_from_slice(&OPCODE_CMC_METERING.to_be_bytes());
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
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&tail_offset.to_be_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&port.to_be_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&ipv4);
    body.extend_from_slice(&port.to_be_bytes());
    body.extend(std::iter::repeat_n(0, 6));
    body.extend_from_slice(&port.to_be_bytes());
    body.extend(std::iter::repeat_n(0, 2));

    ConmonHeader {
        message_id,
        protocol_id: PROTOCOL_CMC,
    }
    .packet(&body)
}

pub fn build_volume_stop(
    device_name: &str,
    mac: [u8; 6],
    port: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut packet = build_metering_start(device_name, [0u8; 4], mac, port, 0)?;
    clear_metering_destinations(&mut packet);
    Ok(packet)
}

fn clear_metering_destinations(packet: &mut [u8]) {
    let destinations_start = packet.len() - 16;
    packet[destinations_start..].fill(0);
}
