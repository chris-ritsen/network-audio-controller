use std::net::Ipv4Addr;

use super::*;

pub const OPCODE_EXTERNAL_RECEIVER_SUBSCRIPTION: u16 = 0x3201;
pub const EXTERNAL_RTP_DEFAULT_PORT: u16 = 4321;

const EXTERNAL_SUBSCRIPTION_PROTOCOL_CAP: u16 = 0x2809;
const EXTERNAL_SUBSCRIPTION_MODERN_PROTOCOL: u16 = 0x2800;
const DESTINATION_DESCRIPTOR_SIZE: usize = 8;
const IDENTITY_DESCRIPTOR_SIZE: usize = 0x1C;
const LEGACY_FIXED_HEADER_SIZE: usize = 0x14;
const MODERN_TOP_RECORD_SIZE: usize = 0x30;
const MODERN_MAPPING_SCRATCH_LIMIT: usize = 64;
const MODERN_MAPPING_SEGMENT_HEADER_SIZE: usize = 6;
const MODERN_GAP_SPLIT_THRESHOLD: u16 = 8;

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModernMappingSegment {
    first_receiver_id: u16,
    assignments: Vec<u8>,
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
    let mut previous_receiver = 0;
    let mut previous_positive_slot = 0;
    let mut has_assignment = false;
    for (&receiver_id, &slot) in specification
        .receiver_channel_ids
        .iter()
        .zip(specification.flow_slot_assignments)
    {
        if receiver_id == 0 || receiver_id <= previous_receiver {
            return Err(NetaudioError::InvalidReceiverMapping);
        }
        if slot > specification.advertised_flow_slot_count {
            return Err(NetaudioError::InvalidFlowSlot);
        }
        if slot != 0 {
            if slot <= previous_positive_slot {
                return Err(NetaudioError::InvalidReceiverMapping);
            }
            previous_positive_slot = slot;
            has_assignment = true;
        }
        previous_receiver = receiver_id;
    }
    if !has_assignment {
        return Err(NetaudioError::InvalidReceiverMapping);
    }
    Ok(())
}

fn build_legacy_payload(
    specification: &ExternalReceiverSubscription<'_>,
    destinations: &[[u8; DESTINATION_DESCRIPTOR_SIZE]],
    identity: &[u8; IDENTITY_DESCRIPTOR_SIZE],
) -> Result<Vec<u8>, NetaudioError> {
    let slot_count = usize::from(specification.advertised_flow_slot_count);
    let pointer_count = destinations
        .len()
        .checked_add(slot_count)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let header_size = LEGACY_FIXED_HEADER_SIZE
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

fn modern_mapping_segments(
    receiver_channel_ids: &[u16],
    flow_slot_assignments: &[u16],
) -> Result<Vec<ModernMappingSegment>, NetaudioError> {
    let assigned = receiver_channel_ids
        .iter()
        .copied()
        .zip(flow_slot_assignments.iter().copied())
        .filter(|(_, slot)| *slot != 0)
        .collect::<Vec<_>>();
    let mut segments = Vec::<ModernMappingSegment>::new();
    for (receiver_id, slot) in assigned {
        let slot = u8::try_from(slot).map_err(|_| NetaudioError::InvalidFlowSlot)?;
        let should_split = segments.last().is_some_and(|segment| {
            let last_receiver = segment
                .first_receiver_id
                .saturating_add(u16::try_from(segment.assignments.len()).unwrap_or(u16::MAX))
                .saturating_sub(1);
            receiver_id.saturating_sub(last_receiver).saturating_sub(1)
                >= MODERN_GAP_SPLIT_THRESHOLD
        });
        if segments.is_empty() || should_split {
            segments.push(ModernMappingSegment {
                first_receiver_id: receiver_id,
                assignments: vec![slot],
            });
            continue;
        }
        let segment = segments
            .last_mut()
            .ok_or(NetaudioError::InvalidReceiverMapping)?;
        let vector_index = usize::from(receiver_id - segment.first_receiver_id);
        segment.assignments.resize(vector_index, 0);
        segment.assignments.push(slot);
    }
    Ok(segments)
}

fn encode_modern_mapping(
    segments: &[ModernMappingSegment],
    mapping_offset: usize,
) -> Result<Vec<u8>, NetaudioError> {
    let segment_sizes = segments
        .iter()
        .map(|segment| {
            let padded_length = segment
                .assignments
                .len()
                .checked_add(segment.assignments.len() % 2)?;
            MODERN_MAPPING_SEGMENT_HEADER_SIZE.checked_add(padded_length)
        })
        .collect::<Option<Vec<_>>>()
        .ok_or(NetaudioError::PacketTooLarge)?;
    let total_size = segment_sizes
        .iter()
        .try_fold(0usize, |total, size| total.checked_add(*size));
    let total_size = total_size.ok_or(NetaudioError::PacketTooLarge)?;
    if total_size > MODERN_MAPPING_SCRATCH_LIMIT {
        return Err(NetaudioError::PacketTooLarge);
    }

    let mut encoded = Vec::with_capacity(total_size);
    let mut segment_offset = mapping_offset;
    for (index, segment) in segments.iter().enumerate() {
        let padded_length = segment.assignments.len() + segment.assignments.len() % 2;
        let next_segment_offset = if index + 1 < segments.len() {
            segment_offset
                .checked_add(segment_sizes[index])
                .ok_or(NetaudioError::PacketTooLarge)?
        } else {
            0
        };
        encoded.extend_from_slice(&segment.first_receiver_id.to_be_bytes());
        encoded.extend_from_slice(
            &u16::try_from(padded_length)
                .map_err(|_| NetaudioError::PacketTooLarge)?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(&checked_pointer(next_segment_offset)?.to_be_bytes());
        encoded.extend_from_slice(&segment.assignments);
        encoded.resize(
            encoded.len() + (padded_length - segment.assignments.len()),
            0,
        );
        segment_offset = segment_offset
            .checked_add(segment_sizes[index])
            .ok_or(NetaudioError::PacketTooLarge)?;
    }
    Ok(encoded)
}

fn build_modern_payload(
    specification: &ExternalReceiverSubscription<'_>,
    destinations: &[[u8; DESTINATION_DESCRIPTOR_SIZE]],
    identity: &[u8; IDENTITY_DESCRIPTOR_SIZE],
) -> Result<Vec<u8>, NetaudioError> {
    if specification.advertised_flow_slot_count > u16::from(u8::MAX) {
        return Err(NetaudioError::InvalidFlowSlot);
    }
    let segments = modern_mapping_segments(
        specification.receiver_channel_ids,
        specification.flow_slot_assignments,
    )?;
    let destination_bytes = destinations
        .len()
        .checked_mul(DESTINATION_DESCRIPTOR_SIZE)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let identity_offset = MODERN_TOP_RECORD_SIZE
        .checked_add(destination_bytes)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let mapping_offset = identity_offset
        .checked_add(IDENTITY_DESCRIPTOR_SIZE)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let mapping = encode_modern_mapping(&segments, mapping_offset)?;

    let mut payload = vec![0u8; MODERN_TOP_RECORD_SIZE];
    payload[2..4].copy_from_slice(&0x4202u16.to_be_bytes());
    payload[12..14].copy_from_slice(
        &u16::try_from(destinations.len())
            .map_err(|_| NetaudioError::PacketTooLarge)?
            .to_be_bytes(),
    );
    for (destination_index, destination) in destinations.iter().enumerate() {
        let destination_offset = MODERN_TOP_RECORD_SIZE
            .checked_add(
                destination_index
                    .checked_mul(DESTINATION_DESCRIPTOR_SIZE)
                    .ok_or(NetaudioError::PacketTooLarge)?,
            )
            .ok_or(NetaudioError::PacketTooLarge)?;
        let pointer_offset = 0x12usize
            .checked_add(
                destination_index
                    .checked_mul(2)
                    .ok_or(NetaudioError::PacketTooLarge)?,
            )
            .ok_or(NetaudioError::PacketTooLarge)?;
        payload[pointer_offset..pointer_offset + 2]
            .copy_from_slice(&checked_pointer(destination_offset)?.to_be_bytes());
        payload.extend_from_slice(destination);
    }
    payload[0x1C..0x1E].copy_from_slice(&checked_pointer(identity_offset)?.to_be_bytes());
    payload[0x24..0x26].copy_from_slice(&specification.advertised_flow_slot_count.to_be_bytes());
    payload[0x26..0x28].copy_from_slice(&checked_pointer(mapping_offset)?.to_be_bytes());
    payload.extend_from_slice(identity);
    payload.extend_from_slice(&mapping);
    Ok(payload)
}

pub fn build_external_receiver_subscription(
    specification: &ExternalReceiverSubscription<'_>,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    validate_mapping(specification)?;
    let destinations = validated_destinations(specification)?;
    let identity = identity_descriptor(specification.flow_identity, specification.clock_offset)?;
    let payload = if specification.device_protocol < EXTERNAL_SUBSCRIPTION_MODERN_PROTOCOL {
        build_legacy_payload(specification, &destinations, &identity)?
    } else {
        build_modern_payload(specification, &destinations, &identity)?
    };
    build_control_packet_for_protocol(
        specification
            .device_protocol
            .min(EXTERNAL_SUBSCRIPTION_PROTOCOL_CAP),
        OPCODE_EXTERNAL_RECEIVER_SUBSCRIPTION,
        &payload,
        transaction_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn destination(address: [u8; 4], port: u16) -> ExternalRtpDestination {
        ExternalRtpDestination {
            address: Ipv4Addr::from(address),
            port,
        }
    }

    fn specification<'a>(
        device_protocol: u16,
        receivers: &'a [u16],
        assignments: &'a [u16],
    ) -> ExternalReceiverSubscription<'a> {
        ExternalReceiverSubscription {
            device_protocol,
            receiver_channel_ids: receivers,
            flow_slot_assignments: assignments,
            advertised_flow_slot_count: 4,
            flow_identity: ExternalFlowIdentity {
                source_address: Ipv4Addr::new(192, 0, 2, 10),
                session_id: 0x0123_4567_89AB_CDEF,
            },
            clock_offset: 7,
            primary_destination: destination([239, 69, 1, 2], 0),
            secondary_destination: None,
            advertisement_supports_multiple_interfaces: false,
            receiver_supports_multiple_interfaces: false,
        }
    }

    #[test]
    fn destination_and_identity_descriptors_preserve_all_fields() {
        assert_eq!(
            destination_descriptor(destination([239, 69, 1, 2], 0)).unwrap(),
            [8, 2, 0x10, 0xE1, 239, 69, 1, 2]
        );
        let identity = identity_descriptor(
            ExternalFlowIdentity {
                source_address: Ipv4Addr::new(192, 0, 2, 10),
                session_id: 0x0123_4567_89AB_CDEF,
            },
            7,
        )
        .unwrap();
        assert_eq!(
            identity,
            [
                0x0E, 0, 0, 0x0B, 192, 0, 2, 10, 1, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
            ]
        );
    }

    #[test]
    fn legacy_payload_uses_per_slot_big_endian_bitmaps() {
        let packet = build_external_receiver_subscription(
            &specification(0x2729, &[1, 2, 17, 18], &[1, 2, 3, 0]),
            0x1234,
        )
        .unwrap();
        assert_eq!(
            &packet[0..8],
            &[0x27, 0x29, 0, 0x5A, 0x12, 0x34, 0x32, 0x01]
        );
        let payload = &packet[8..];
        assert_eq!(&payload[0..4], &[0, 0, 2, 2]);
        assert_eq!(u16::from_be_bytes([payload[12], payload[13]]), 1);
        assert_eq!(u16::from_be_bytes([payload[14], payload[15]]), 4);
        assert_eq!(u16::from_be_bytes([payload[16], payload[17]]), 2);
        assert_eq!(
            &payload[18..28],
            &[0, 0x1E, 0, 0x42, 0, 0x46, 0, 0x4A, 0, 0x4E]
        );
        assert_eq!(&payload[0x1E..0x26], &[8, 2, 0x10, 0xE1, 239, 69, 1, 2]);
        assert_eq!(
            &payload[0x42..],
            &[0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]
        );
    }

    #[test]
    fn modern_payload_caps_header_protocol_and_splits_large_mapping_gaps() {
        let packet = build_external_receiver_subscription(
            &specification(0x280F, &[1, 3, 12], &[1, 2, 3]),
            0xBEEF,
        )
        .unwrap();
        assert_eq!(&packet[0..2], &0x2809u16.to_be_bytes());
        assert_eq!(&packet[4..8], &[0xBE, 0xEF, 0x32, 0x01]);
        let payload = &packet[8..];
        assert_eq!(&payload[0..4], &[0, 0, 0x42, 0x02]);
        assert_eq!(&payload[12..14], &[0, 1]);
        assert_eq!(&payload[0x12..0x14], &[0, 0x30]);
        assert_eq!(&payload[0x1C..0x1E], &[0, 0x38]);
        assert_eq!(&payload[0x24..0x28], &[0, 4, 0, 0x54]);
        assert_eq!(
            &payload[0x54..],
            &[0, 1, 0, 4, 0, 0x5E, 1, 0, 2, 0, 0, 12, 0, 2, 0, 0, 3, 0]
        );
    }

    #[test]
    fn modern_payload_keeps_gaps_under_eight_in_one_segment() {
        let packet =
            build_external_receiver_subscription(&specification(0x2800, &[5, 12], &[1, 2]), 1)
                .unwrap();
        let payload = &packet[8..];
        assert_eq!(
            &payload[0x54..],
            &[0, 5, 0, 8, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2]
        );
    }

    #[test]
    fn secondary_destination_requires_support_on_both_sides() {
        let mut spec = specification(0x2809, &[1], &[1]);
        spec.secondary_destination = Some(destination([239, 69, 1, 3], 5004));
        assert_eq!(
            build_external_receiver_subscription(&spec, 1),
            Err(NetaudioError::InvalidDestination)
        );
        spec.advertisement_supports_multiple_interfaces = true;
        spec.receiver_supports_multiple_interfaces = true;
        let packet = build_external_receiver_subscription(&spec, 1).unwrap();
        let payload = &packet[8..];
        assert_eq!(&payload[12..14], &[0, 2]);
        assert_eq!(&payload[0x12..0x16], &[0, 0x30, 0, 0x38]);
        assert_eq!(
            &payload[0x30..0x40],
            &[8, 2, 0x10, 0xE1, 239, 69, 1, 2, 8, 2, 0x13, 0x8C, 239, 69, 1, 3]
        );
    }

    #[test]
    fn rejects_invalid_or_unproven_mapping_forms() {
        for (receivers, slots, expected) in [
            (vec![], vec![], NetaudioError::InvalidReceiverMapping),
            (vec![1], vec![], NetaudioError::InvalidReceiverMapping),
            (vec![0], vec![1], NetaudioError::InvalidReceiverMapping),
            (
                vec![2, 1],
                vec![1, 2],
                NetaudioError::InvalidReceiverMapping,
            ),
            (
                vec![1, 1],
                vec![1, 2],
                NetaudioError::InvalidReceiverMapping,
            ),
            (
                vec![1, 2],
                vec![2, 1],
                NetaudioError::InvalidReceiverMapping,
            ),
            (
                vec![1, 2],
                vec![1, 1],
                NetaudioError::InvalidReceiverMapping,
            ),
            (
                vec![1, 2],
                vec![0, 0],
                NetaudioError::InvalidReceiverMapping,
            ),
            (vec![1], vec![5], NetaudioError::InvalidFlowSlot),
        ] {
            assert_eq!(
                build_external_receiver_subscription(&specification(0x2809, &receivers, &slots), 1),
                Err(expected),
            );
        }
    }

    #[test]
    fn rejects_modern_mapping_that_exceeds_the_scratch_limit() {
        let receivers = (1..=59).collect::<Vec<_>>();
        let assignments = (1..=59).collect::<Vec<_>>();
        let mut spec = specification(0x2809, &receivers, &assignments);
        spec.advertised_flow_slot_count = 59;
        assert_eq!(
            build_external_receiver_subscription(&spec, 1),
            Err(NetaudioError::PacketTooLarge)
        );
    }
}
