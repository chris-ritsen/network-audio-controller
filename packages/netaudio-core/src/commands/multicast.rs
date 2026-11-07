use std::collections::HashSet;
use std::net::Ipv4Addr;

use super::*;

const MAX_MODERN_FLOW_PACKET_SIZE: usize = 1400;
const DESTINATION_DESCRIPTOR_SIZE: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MulticastFlowTransport {
    Native,
    RtpAes67,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MulticastFlowDestination {
    pub address: Ipv4Addr,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MulticastFlow2809<'a> {
    pub channels: &'a [u16],
    pub request_options_word: u16,
    pub media_local_flow_id: u16,
    pub transport: MulticastFlowTransport,
    pub flow_name: Option<&'a str>,
    pub frames_per_packet: u16,
    pub destinations: &'a [MulticastFlowDestination],
}

fn checked_u16(value: usize) -> Result<u16, NetaudioError> {
    u16::try_from(value).map_err(|_| NetaudioError::PacketTooLarge)
}

fn checked_u8(value: usize) -> Result<u8, NetaudioError> {
    u8::try_from(value).map_err(|_| NetaudioError::PacketTooLarge)
}

fn checked_add(left: usize, right: usize) -> Result<usize, NetaudioError> {
    left.checked_add(right).ok_or(NetaudioError::PacketTooLarge)
}

fn checked_mul(left: usize, right: usize) -> Result<usize, NetaudioError> {
    left.checked_mul(right).ok_or(NetaudioError::PacketTooLarge)
}

/// Encode the independently observed ARC 2.8.9 segmented transmitter allocation family.
///
/// The global flow identifier is always zero so the device allocates it. Native
/// default-form requests are capture-backed. Names, explicit frames per packet,
/// and RTP/AES67 destinations are static protocol-research vectors pending an
/// independently captured device exchange.
pub fn build_create_multicast_flow_2809(
    specification: &MulticastFlow2809<'_>,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if ![0, 0x0001, 0x0071].contains(&specification.request_options_word) {
        return Err(NetaudioError::InvalidFlowProtocol);
    }
    if specification.media_local_flow_id == 0 {
        return Err(NetaudioError::InvalidFlowIdentity);
    }
    if specification.channels.is_empty() || specification.channels.contains(&0) {
        return Err(NetaudioError::InvalidChannel);
    }
    let mut unique_channels = HashSet::with_capacity(specification.channels.len());
    if !specification
        .channels
        .iter()
        .all(|channel| unique_channels.insert(*channel))
    {
        return Err(NetaudioError::InvalidChannel);
    }

    let destination_count = specification.destinations.len();
    let (transport_selector, flow_flags) = match specification.transport {
        MulticastFlowTransport::Native if destination_count == 0 => (1u16, 2u32),
        MulticastFlowTransport::RtpAes67 if (1..=2).contains(&destination_count) => (3u16, 6u32),
        _ => return Err(NetaudioError::InvalidDestination),
    };
    if specification
        .destinations
        .iter()
        .any(|destination| !destination.address.is_multicast() || destination.port == 0)
    {
        return Err(NetaudioError::InvalidDestination);
    }
    let mut unique_destinations = HashSet::with_capacity(destination_count);
    if !specification
        .destinations
        .iter()
        .all(|destination| unique_destinations.insert((destination.address, destination.port)))
    {
        return Err(NetaudioError::InvalidDestination);
    }

    let channel_count = specification.channels.len();
    let record_words = checked_add(checked_add(41, channel_count)?, destination_count)?;
    if record_words > 127 {
        return Err(NetaudioError::PacketTooLarge);
    }
    let record_words_u8 = u8::try_from(record_words).map_err(|_| NetaudioError::PacketTooLarge)?;
    let channel_count_u16 = checked_u16(channel_count)?;

    let encoded_name = match specification.flow_name {
        None => Vec::new(),
        Some(name) => {
            if name.is_empty() || name.as_bytes().contains(&0) {
                return Err(NetaudioError::NameInvalidChars);
            }
            let mut encoded = name.as_bytes().to_vec();
            encoded.push(0);
            if encoded.len() % 2 != 0 {
                encoded.push(0);
            }
            encoded
        }
    };
    let record_length = checked_add(
        checked_add(82, checked_mul(channel_count, 2)?)?,
        checked_mul(destination_count, 2)?,
    )?;
    let descriptor_bytes = checked_mul(destination_count, DESTINATION_DESCRIPTOR_SIZE)?;
    let packet_length = checked_add(
        checked_add(checked_add(20, encoded_name.len())?, record_length)?,
        descriptor_bytes,
    )?;
    if packet_length > MAX_MODERN_FLOW_PACKET_SIZE {
        return Err(if encoded_name.is_empty() {
            NetaudioError::PacketTooLarge
        } else {
            NetaudioError::NameTooLong
        });
    }

    let record_pointer = checked_u16(checked_add(0x14, encoded_name.len())?)?;
    let name_pointer: u16 = if encoded_name.is_empty() { 0 } else { 0x14 };
    let record_packet_offset = usize::from(record_pointer);
    let descriptors_packet_offset = checked_add(record_packet_offset, record_length)?;

    let mut body = Vec::with_capacity(packet_length - 10);
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&specification.request_options_word.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&0x0101u16.to_be_bytes());
    body.extend_from_slice(&record_pointer.to_be_bytes());
    body.extend_from_slice(&encoded_name);

    let record_start = body.len();
    body.resize(record_start + record_length, 0);
    let record = &mut body[record_start..record_start + record_length];

    record[0..2].copy_from_slice(&[22, record_words_u8]);
    record[6..8].copy_from_slice(&3u16.to_be_bytes());
    record[8..10].copy_from_slice(&specification.media_local_flow_id.to_be_bytes());
    record[12..16].copy_from_slice(&flow_flags.to_be_bytes());
    record[20..22].copy_from_slice(&name_pointer.to_be_bytes());

    let segment_two = 44;
    record[segment_two..segment_two + 2].copy_from_slice(&[
        10,
        checked_u8(checked_add(
            checked_add(19, channel_count)?,
            destination_count,
        )?)?,
    ]);
    record[segment_two + 8..segment_two + 10].copy_from_slice(&transport_selector.to_be_bytes());

    let segment_three = 64;
    record[segment_three..segment_three + 2].copy_from_slice(&[
        checked_u8(checked_add(4, destination_count)?)?,
        checked_u8(checked_add(
            checked_add(9, channel_count)?,
            destination_count,
        )?)?,
    ]);
    for (index, _) in specification.destinations.iter().enumerate() {
        let pointer = checked_u16(checked_add(
            descriptors_packet_offset,
            checked_mul(index, DESTINATION_DESCRIPTOR_SIZE)?,
        )?)?;
        let pointer_offset = checked_add(checked_add(segment_three, 4)?, checked_mul(index, 2)?)?;
        record[pointer_offset..pointer_offset + 2].copy_from_slice(&pointer.to_be_bytes());
    }

    let segment_four = checked_add(
        checked_add(segment_three, 8)?,
        checked_mul(destination_count, 2)?,
    )?;
    record[segment_four..segment_four + 2].copy_from_slice(&[
        checked_u8(checked_add(3, channel_count)?)?,
        checked_u8(checked_add(5, channel_count)?)?,
    ]);
    record[segment_four + 2..segment_four + 4].copy_from_slice(&channel_count_u16.to_be_bytes());
    for (index, channel) in specification.channels.iter().enumerate() {
        let channel_offset = checked_add(checked_add(segment_four, 4)?, checked_mul(index, 2)?)?;
        record[channel_offset..channel_offset + 2].copy_from_slice(&channel.to_be_bytes());
    }

    let segment_five = checked_add(
        checked_add(segment_four, 6)?,
        checked_mul(channel_count, 2)?,
    )?;
    record[segment_five..segment_five + 2].copy_from_slice(&[2, 0]);
    record[segment_five + 2..segment_five + 4]
        .copy_from_slice(&specification.frames_per_packet.to_be_bytes());

    for destination in specification.destinations {
        body.extend_from_slice(&[8, 2]);
        body.extend_from_slice(&destination.port.to_be_bytes());
        body.extend_from_slice(&destination.address.octets());
    }

    arc_packet_with_reserved_word(
        PROTOCOL_ARC_2809,
        OPCODE_CREATE_TX_FLOW_2809,
        &body,
        message_id,
    )
}
