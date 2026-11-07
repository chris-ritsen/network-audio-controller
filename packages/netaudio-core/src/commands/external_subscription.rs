use std::collections::HashSet;
use std::net::Ipv4Addr;

use super::*;

pub const OPCODE_EXTERNAL_RECEIVER_SUBSCRIPTION: u16 = 0x3201;
pub const EXTERNAL_RTP_DEFAULT_PORT: u16 = 4321;

const EXTERNAL_SUBSCRIPTION_PROTOCOL_CAP: u16 = 0x2809;
const DESTINATION_DESCRIPTOR_SIZE: usize = 8;
const IDENTITY_DESCRIPTOR_SIZE: usize = 0x1C;
const BITMAP_FIXED_HEADER_SIZE: usize = 0x14;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalRtpDestination {
    pub address: Ipv4Addr,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalFlowIdentity {
    pub source_address: Ipv4Addr,
    pub session_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalReceiverSubscription<'a> {
    pub device_protocol: u16,
    pub receiver_channel_ids: &'a [u16],
    pub flow_slot_assignments: &'a [u16],
    pub advertised_flow_slot_count: u16,
    pub flow_identity: ExternalFlowIdentity,
    pub clock_offset: u32,
    pub primary_destination: ExternalRtpDestination,
    pub secondary_destination: Option<ExternalRtpDestination>,
    pub advertisement_supports_multiple_interfaces: bool,
    pub receiver_supports_multiple_interfaces: bool,
}

fn checked_pointer(offset: usize) -> Result<u16, NetaudioError> {
    u16::try_from(offset).map_err(|_| NetaudioError::PacketTooLarge)
}

fn destination_descriptor(destination: ExternalRtpDestination) -> Result<[u8; 8], NetaudioError> {
    if destination.address.is_unspecified() {
        return Err(NetaudioError::InvalidDestination);
    }
    let port = if destination.port == 0 {
        EXTERNAL_RTP_DEFAULT_PORT
    } else {
        destination.port
    };
    let mut descriptor = [0u8; DESTINATION_DESCRIPTOR_SIZE];
    descriptor[0] = DESTINATION_DESCRIPTOR_SIZE as u8;
    descriptor[1] = 2;
    descriptor[2..4].copy_from_slice(&port.to_be_bytes());
    descriptor[4..8].copy_from_slice(&destination.address.octets());
    Ok(descriptor)
}

fn identity_descriptor(
    identity: ExternalFlowIdentity,
    clock_offset: u32,
) -> Result<[u8; IDENTITY_DESCRIPTOR_SIZE], NetaudioError> {
    if identity.source_address.is_unspecified() || identity.session_id == 0 {
        return Err(NetaudioError::InvalidFlowIdentity);
    }
    let mut descriptor = [0u8; IDENTITY_DESCRIPTOR_SIZE];
    descriptor[0] = 0x0E;
    descriptor[2..4].copy_from_slice(&0x000Bu16.to_be_bytes());
    descriptor[4..8].copy_from_slice(&identity.source_address.octets());
    descriptor[8..16].copy_from_slice(&identity.session_id.to_be_bytes());
    descriptor[24..28].copy_from_slice(&clock_offset.to_be_bytes());
    Ok(descriptor)
}

fn validated_destinations(
    specification: &ExternalReceiverSubscription<'_>,
) -> Result<Vec<[u8; DESTINATION_DESCRIPTOR_SIZE]>, NetaudioError> {
    let mut destinations = vec![destination_descriptor(specification.primary_destination)?];
    if let Some(secondary) = specification.secondary_destination {
        if !specification.advertisement_supports_multiple_interfaces
            || !specification.receiver_supports_multiple_interfaces
        {
            return Err(NetaudioError::InvalidDestination);
        }
        destinations.push(destination_descriptor(secondary)?);
    }
    Ok(destinations)
}

fn validate_mapping(specification: &ExternalReceiverSubscription<'_>) -> Result<(), NetaudioError> {
    if specification.receiver_channel_ids.is_empty()
        || specification.receiver_channel_ids.len() != specification.flow_slot_assignments.len()
        || specification.advertised_flow_slot_count == 0
    {
        return Err(NetaudioError::InvalidReceiverMapping);
    }
    let mut receiver_ids = HashSet::with_capacity(specification.receiver_channel_ids.len());
    for (&receiver_id, &slot) in specification
        .receiver_channel_ids
        .iter()
        .zip(specification.flow_slot_assignments)
    {
        if receiver_id == 0 || !receiver_ids.insert(receiver_id) {
            return Err(NetaudioError::InvalidReceiverMapping);
        }
        if slot > specification.advertised_flow_slot_count {
            return Err(NetaudioError::InvalidFlowSlot);
        }
    }
    Ok(())
}

fn build_bitmap_payload(
    specification: &ExternalReceiverSubscription<'_>,
    destinations: &[[u8; DESTINATION_DESCRIPTOR_SIZE]],
    identity: &[u8; IDENTITY_DESCRIPTOR_SIZE],
) -> Result<Vec<u8>, NetaudioError> {
    let slot_count = usize::from(specification.advertised_flow_slot_count);
    let pointer_count = destinations
        .len()
        .checked_add(slot_count)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let header_size = BITMAP_FIXED_HEADER_SIZE
        .checked_add(
            pointer_count
                .checked_mul(2)
                .ok_or(NetaudioError::PacketTooLarge)?,
        )
        .ok_or(NetaudioError::PacketTooLarge)?;
    let maximum_receiver_id = specification
        .receiver_channel_ids
        .iter()
        .copied()
        .max()
        .ok_or(NetaudioError::InvalidReceiverMapping)?;
    let bitmap_word_count = maximum_receiver_id
        .checked_add(15)
        .ok_or(NetaudioError::PacketTooLarge)?
        / 16;
    let bitmap_size = usize::from(bitmap_word_count)
        .checked_mul(2)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let bitmaps_size = bitmap_size
        .checked_mul(slot_count)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let destination_bytes = destinations
        .len()
        .checked_mul(DESTINATION_DESCRIPTOR_SIZE)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let total_size = header_size
        .checked_add(destination_bytes)
        .and_then(|size| size.checked_add(IDENTITY_DESCRIPTOR_SIZE))
        .and_then(|size| size.checked_add(bitmaps_size))
        .ok_or(NetaudioError::PacketTooLarge)?;
    checked_pointer(total_size)?;

    let mut payload = vec![0u8; header_size];
    payload[2..4].copy_from_slice(&0x0202u16.to_be_bytes());
    payload[12..14].copy_from_slice(
        &u16::try_from(destinations.len())
            .map_err(|_| NetaudioError::PacketTooLarge)?
            .to_be_bytes(),
    );
    payload[14..16].copy_from_slice(&specification.advertised_flow_slot_count.to_be_bytes());
    payload[16..18].copy_from_slice(&bitmap_word_count.to_be_bytes());

    let mut next_offset = header_size;
    let mut pointer_offset = 0x12;
    for destination in destinations {
        payload[pointer_offset..pointer_offset + 2]
            .copy_from_slice(&checked_pointer(next_offset)?.to_be_bytes());
        pointer_offset += 2;
        next_offset = next_offset
            .checked_add(DESTINATION_DESCRIPTOR_SIZE)
            .ok_or(NetaudioError::PacketTooLarge)?;
        payload.extend_from_slice(destination);
    }
    payload.extend_from_slice(identity);
    next_offset = header_size
        .checked_add(destination_bytes)
        .and_then(|size| size.checked_add(IDENTITY_DESCRIPTOR_SIZE))
        .ok_or(NetaudioError::PacketTooLarge)?;

    let mut bitmaps = vec![vec![0u16; usize::from(bitmap_word_count)]; slot_count];
    for (&receiver_id, &slot) in specification
        .receiver_channel_ids
        .iter()
        .zip(specification.flow_slot_assignments)
    {
        if slot == 0 {
            continue;
        }
        let zero_based_receiver = receiver_id - 1;
        let word_index = usize::from(zero_based_receiver / 16);
        let bit_index = zero_based_receiver % 16;
        bitmaps[usize::from(slot - 1)][word_index] |= 1u16 << bit_index;
    }
    for bitmap in bitmaps {
        payload[pointer_offset..pointer_offset + 2]
            .copy_from_slice(&checked_pointer(next_offset)?.to_be_bytes());
        pointer_offset += 2;
        for word in bitmap {
            payload.extend_from_slice(&word.to_be_bytes());
        }
        next_offset = next_offset
            .checked_add(bitmap_size)
            .ok_or(NetaudioError::PacketTooLarge)?;
    }
    debug_assert_eq!(payload.len(), total_size);
    Ok(payload)
}

pub fn build_external_receiver_subscription(
    specification: &ExternalReceiverSubscription<'_>,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !crate::protocol::is_supported_arc_protocol(specification.device_protocol) {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }

    validate_mapping(specification)?;
    let destinations = validated_destinations(specification)?;
    let identity = identity_descriptor(specification.flow_identity, specification.clock_offset)?;
    let payload = build_bitmap_payload(specification, &destinations, &identity)?;
    build_control_packet_for_protocol(
        specification
            .device_protocol
            .min(EXTERNAL_SUBSCRIPTION_PROTOCOL_CAP),
        OPCODE_EXTERNAL_RECEIVER_SUBSCRIPTION,
        &payload,
        message_id,
    )
}
