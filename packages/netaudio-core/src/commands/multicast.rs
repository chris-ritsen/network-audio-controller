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
    transaction_id: u16,
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
        transaction_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::decode_hexadecimal;

    fn native(channels: &[u16], options: u16) -> MulticastFlow2809<'_> {
        MulticastFlow2809 {
            channels,
            request_options_word: options,
            media_local_flow_id: 2,
            transport: MulticastFlowTransport::Native,
            flow_name: None,
            frames_per_packet: 0,
            destinations: &[],
        }
    }

    #[test]
    fn aes3_allocations_match_controller_requests_and_acknowledgments() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/multicast_creation_2809_aes3.json"
        ))
        .unwrap();
        for exchange in fixture["exchanges"].as_array().unwrap() {
            let specification = serde_json::json!({
                "command": "create_multicast_flow_2809",
                "channels": exchange["channels"],
                "request_options_word": exchange["request_options_word"],
                "media_local_flow_id": 2,
                "transport": "native",
                "transaction_id": exchange["transaction_id"],
            });
            assert_eq!(
                crate::spec::build_command_from_json(&specification.to_string()).unwrap(),
                decode_hexadecimal(exchange["request"]["hexadecimal"].as_str().unwrap()),
            );
            let response =
                decode_hexadecimal(exchange["response"]["hexadecimal"].as_str().unwrap());
            let allocation =
                crate::responses::parse_multicast_flow_creation_2809(&response).unwrap();
            assert_eq!(
                serde_json::to_value(allocation).unwrap(),
                serde_json::json!({
                    "global_flow_id": 2, "media_type_code": 3,
                    "media_local_flow_id": 2, "channels": exchange["channels"],
                }),
            );
        }
    }

    #[test]
    fn multicast_creation_matches_independent_controller_exchanges() {
        for (channels, options, transaction, expected) in [
            (vec![1, 2], 0x0071, 0x15e0, "2809006a15e02601000000000071000001010014162b0000000000030002000000000002000000000000000000000000000000000000000000000000000000000a15000000000000000100000000000000000000040b0000000000000507000200010002000002000000"),
            (vec![2], 0x0071, 0x1600, "2809006816002601000000000071000001010014162a0000000000030002000000000002000000000000000000000000000000000000000000000000000000000a14000000000000000100000000000000000000040a000000000000040600010002000002000000"),
            (vec![1, 2], 0x0001, 0x171d, "2809006a171d2601000000000001000001010014162b0000000000030002000000000002000000000000000000000000000000000000000000000000000000000a15000000000000000100000000000000000000040b0000000000000507000200010002000002000000"),
        ] {
            assert_eq!(
                build_create_multicast_flow_2809(&native(&channels, options), transaction).unwrap(),
                decode_hexadecimal(expected),
            );
        }
    }

    #[test]
    fn static_rtp_vectors_encode_name_fpp_and_ordered_destination_pointers() {
        let destinations = [
            MulticastFlowDestination {
                address: Ipv4Addr::new(239, 1, 2, 3),
                port: 5004,
            },
            MulticastFlowDestination {
                address: Ipv4Addr::new(239, 1, 2, 4),
                port: 5006,
            },
        ];
        let packet = build_create_multicast_flow_2809(
            &MulticastFlow2809 {
                channels: &[2, 1],
                request_options_word: 1,
                media_local_flow_id: 7,
                transport: MulticastFlowTransport::RtpAes67,
                flow_name: Some("RTP"),
                frames_per_packet: 48,
                destinations: &destinations,
            },
            9,
        )
        .unwrap();
        assert_eq!(&packet[0x14..0x18], b"RTP\0");
        assert_eq!(u16::from_be_bytes([packet[0x12], packet[0x13]]), 0x18);
        let record = 0x18;
        assert_eq!(&packet[record..record + 2], &[22, 45]);
        assert_eq!(&packet[record + 8..record + 10], &7u16.to_be_bytes());
        assert_eq!(&packet[record + 12..record + 16], &6u32.to_be_bytes());
        assert_eq!(&packet[record + 20..record + 22], &0x14u16.to_be_bytes());
        assert_eq!(&packet[record + 52..record + 54], &3u16.to_be_bytes());
        assert_eq!(&packet[record + 68..record + 72], &[0, 0x72, 0, 0x7a]);
        assert_eq!(&packet[record + 80..record + 84], &[0, 2, 0, 1]);
        assert_eq!(&packet[record + 86..record + 90], &[2, 0, 0, 48]);
        assert_eq!(&packet[0x72..0x7a], &[8, 2, 0x13, 0x8c, 239, 1, 2, 3]);
        assert_eq!(&packet[0x7a..0x82], &[8, 2, 0x13, 0x8e, 239, 1, 2, 4]);
    }

    #[test]
    fn static_native_fields_move_the_record_pointer_and_preserve_alignment() {
        for (name, expected_pointer) in [("A", 0x16u16), ("AB", 0x18u16), ("é", 0x18u16)] {
            let packet = build_create_multicast_flow_2809(
                &MulticastFlow2809 {
                    channels: &[3, 1],
                    request_options_word: 0,
                    media_local_flow_id: 9,
                    transport: MulticastFlowTransport::Native,
                    flow_name: Some(name),
                    frames_per_packet: 7,
                    destinations: &[],
                },
                1,
            )
            .unwrap();
            assert_eq!(
                u16::from_be_bytes([packet[0x12], packet[0x13]]),
                expected_pointer
            );
            let record = usize::from(expected_pointer);
            assert_eq!(&packet[record + 8..record + 10], &9u16.to_be_bytes());
            assert_eq!(&packet[record + 20..record + 22], &0x14u16.to_be_bytes());
            assert_eq!(&packet[record + 76..record + 80], &[0, 3, 0, 1]);
            assert_eq!(&packet[record + 82..record + 86], &[2, 0, 0, 7]);
        }
    }

    #[test]
    fn static_rtp_one_destination_has_one_pointer_and_descriptor() {
        let destinations = [MulticastFlowDestination {
            address: Ipv4Addr::new(239, 69, 1, 2),
            port: 5004,
        }];
        let packet = build_create_multicast_flow_2809(
            &MulticastFlow2809 {
                channels: &[1],
                request_options_word: 0x71,
                media_local_flow_id: 4,
                transport: MulticastFlowTransport::RtpAes67,
                flow_name: None,
                frames_per_packet: 0,
                destinations: &destinations,
            },
            2,
        )
        .unwrap();
        let record = 0x14;
        let descriptor = 0x14 + 82 + 2 + 2;
        assert_eq!(&packet[record..record + 2], &[22, 43]);
        assert_eq!(&packet[record + 64..record + 68], &[5, 11, 0, 0]);
        assert_eq!(
            &packet[record + 68..record + 70],
            &u16::try_from(descriptor).unwrap().to_be_bytes()
        );
        assert_eq!(
            &packet[descriptor..descriptor + 8],
            &[8, 2, 0x13, 0x8c, 239, 69, 1, 2]
        );
    }

    #[test]
    fn multicast_creation_rejects_invalid_scope_and_bounds() {
        for channels in [vec![], vec![0], vec![1, 1]] {
            assert_eq!(
                build_create_multicast_flow_2809(&native(&channels, 1), 1),
                Err(NetaudioError::InvalidChannel),
            );
        }
        assert_eq!(
            build_create_multicast_flow_2809(&native(&[1], 2), 1),
            Err(NetaudioError::InvalidFlowProtocol)
        );
        assert_eq!(
            build_create_multicast_flow_2809(&native(&(1..=87).collect::<Vec<_>>(), 1), 1),
            Err(NetaudioError::PacketTooLarge)
        );
        assert!(
            build_create_multicast_flow_2809(&native(&(1..=86).collect::<Vec<_>>(), 1), 1).is_ok()
        );
        let valid_destination = MulticastFlowDestination {
            address: Ipv4Addr::new(239, 1, 2, 3),
            port: 5004,
        };
        let rtp_destinations = [
            valid_destination,
            MulticastFlowDestination {
                address: Ipv4Addr::new(239, 1, 2, 4),
                port: 5006,
            },
        ];
        let rtp_channels_at_limit = (1..=84).collect::<Vec<_>>();
        let rtp_at_limit = MulticastFlow2809 {
            channels: &rtp_channels_at_limit,
            request_options_word: 1,
            media_local_flow_id: 1,
            transport: MulticastFlowTransport::RtpAes67,
            flow_name: None,
            frames_per_packet: 0,
            destinations: &rtp_destinations,
        };
        assert!(build_create_multicast_flow_2809(&rtp_at_limit, 1).is_ok());
        let rtp_channels_over_limit = (1..=85).collect::<Vec<_>>();
        let rtp_over_limit = MulticastFlow2809 {
            channels: &rtp_channels_over_limit,
            ..rtp_at_limit
        };
        assert_eq!(
            build_create_multicast_flow_2809(&rtp_over_limit, 1),
            Err(NetaudioError::PacketTooLarge)
        );
        let mut invalid_identity = native(&[1], 1);
        invalid_identity.media_local_flow_id = 0;
        assert_eq!(
            build_create_multicast_flow_2809(&invalid_identity, 1),
            Err(NetaudioError::InvalidFlowIdentity)
        );
        let destination = [MulticastFlowDestination {
            address: Ipv4Addr::LOCALHOST,
            port: 5004,
        }];
        let mut rtp = native(&[1], 1);
        rtp.transport = MulticastFlowTransport::RtpAes67;
        rtp.destinations = &destination;
        assert_eq!(
            build_create_multicast_flow_2809(&rtp, 1),
            Err(NetaudioError::InvalidDestination)
        );
        let mut native_with_destination = native(&[1], 1);
        native_with_destination.destinations = std::slice::from_ref(&valid_destination);
        assert_eq!(
            build_create_multicast_flow_2809(&native_with_destination, 1),
            Err(NetaudioError::InvalidDestination)
        );
        let mut rtp_without_destination = native(&[1], 1);
        rtp_without_destination.transport = MulticastFlowTransport::RtpAes67;
        assert_eq!(
            build_create_multicast_flow_2809(&rtp_without_destination, 1),
            Err(NetaudioError::InvalidDestination)
        );
        let three_destinations = [valid_destination; 3];
        let mut rtp_with_three_destinations = rtp_without_destination.clone();
        rtp_with_three_destinations.destinations = &three_destinations;
        assert_eq!(
            build_create_multicast_flow_2809(&rtp_with_three_destinations, 1),
            Err(NetaudioError::InvalidDestination)
        );
        let duplicate_destinations = [valid_destination, valid_destination];
        let mut rtp_with_duplicate_destinations = rtp_without_destination.clone();
        rtp_with_duplicate_destinations.destinations = &duplicate_destinations;
        assert_eq!(
            build_create_multicast_flow_2809(&rtp_with_duplicate_destinations, 1),
            Err(NetaudioError::InvalidDestination)
        );
        let zero_port = [MulticastFlowDestination {
            address: Ipv4Addr::new(239, 1, 2, 3),
            port: 0,
        }];
        let mut rtp_with_zero_port = rtp_without_destination;
        rtp_with_zero_port.destinations = &zero_port;
        assert_eq!(
            build_create_multicast_flow_2809(&rtp_with_zero_port, 1),
            Err(NetaudioError::InvalidDestination)
        );
        let mut named = native(&[1], 1);
        named.flow_name = Some("bad\0name");
        assert_eq!(
            build_create_multicast_flow_2809(&named, 1),
            Err(NetaudioError::NameInvalidChars)
        );
        named.flow_name = Some("");
        assert_eq!(
            build_create_multicast_flow_2809(&named, 1),
            Err(NetaudioError::NameInvalidChars)
        );
        let exact_limit_name = "x".repeat(1295);
        let mut exact_limit = native(&[1], 1);
        exact_limit.flow_name = Some(&exact_limit_name);
        assert_eq!(
            build_create_multicast_flow_2809(&exact_limit, 1)
                .unwrap()
                .len(),
            MAX_MODERN_FLOW_PACKET_SIZE
        );
        let over_limit_name = "x".repeat(1296);
        let mut over_limit = native(&[1], 1);
        over_limit.flow_name = Some(&over_limit_name);
        assert_eq!(
            build_create_multicast_flow_2809(&over_limit, 1),
            Err(NetaudioError::NameTooLong)
        );
    }
}
