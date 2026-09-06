use super::*;

/// Encode the ARC 2.8.9 multicast allocation form observed in Controller captures.
///
/// The device allocates the global flow identifier. The request's media-local
/// identifier is 2; it is not a caller-selected global slot. Options words zero,
/// 0x0001 and 0x0071 are observed; their differing bits remain unresolved and
/// must not be derived from a query reply or a presumed device-family mapping.
pub fn build_create_multicast_flow_2809(
    channels: &[u16],
    request_options_word: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if ![0, 0x0001, 0x0071].contains(&request_options_word) {
        return Err(NetaudioError::InvalidFlowProtocol);
    }
    if channels.is_empty() || channels.contains(&0) {
        return Err(NetaudioError::InvalidChannel);
    }
    // Segment headers carry remaining lengths in one-byte word counts.
    let record_words = channels
        .len()
        .checked_add(41)
        .and_then(|length| u8::try_from(length).ok())
        .ok_or(NetaudioError::PacketTooLarge)?;
    let channel_count = u16::try_from(channels.len()).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut unique_channels = HashSet::with_capacity(channels.len());
    if !channels
        .iter()
        .all(|channel| unique_channels.insert(*channel))
    {
        return Err(NetaudioError::InvalidChannel);
    }

    let mut body = vec![0u8; 84];
    body[2..4].copy_from_slice(&request_options_word.to_be_bytes());
    body[6..8].copy_from_slice(&0x0101u16.to_be_bytes());
    body[8..10].copy_from_slice(&20u16.to_be_bytes());
    body[10..12].copy_from_slice(&[22, record_words]);
    body[16..18].copy_from_slice(&3u16.to_be_bytes());
    body[18..20].copy_from_slice(&2u16.to_be_bytes());
    body[24..26].copy_from_slice(&FLOW_TYPE_MULTICAST.to_be_bytes());
    body[54..56].copy_from_slice(&[10, record_words - 22]);
    body[62..64].copy_from_slice(&1u16.to_be_bytes());
    body[74..76].copy_from_slice(&[4, record_words - 32]);
    body[82..84].copy_from_slice(&[record_words - 38, record_words - 36]);
    body.extend_from_slice(&channel_count.to_be_bytes());
    for channel in channels {
        body.extend_from_slice(&channel.to_be_bytes());
    }
    body.extend_from_slice(&[0, 0, 2, 0, 0, 0]);
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
            (
                vec![1, 2], 0x0071, 0x15e0,
                "2809006a15e02601000000000071000001010014162b0000000000030002000000000002000000000000000000000000000000000000000000000000000000000a15000000000000000100000000000000000000040b0000000000000507000200010002000002000000",
            ),
            (
                vec![2], 0x0071, 0x1600,
                "2809006816002601000000000071000001010014162a0000000000030002000000000002000000000000000000000000000000000000000000000000000000000a14000000000000000100000000000000000000040a000000000000040600010002000002000000",
            ),
            (
                vec![1, 2], 0x0001, 0x171d,
                "2809006a171d2601000000000001000001010014162b0000000000030002000000000002000000000000000000000000000000000000000000000000000000000a15000000000000000100000000000000000000040b0000000000000507000200010002000002000000",
            ),
        ] {
            assert_eq!(
                build_create_multicast_flow_2809(&channels, options, transaction).unwrap(),
                decode_hexadecimal(expected),
            );
        }
    }

    #[test]
    fn multicast_creation_rejects_unknown_options_and_invalid_channels() {
        for channels in [vec![], vec![0], vec![1, 1]] {
            assert_eq!(
                build_create_multicast_flow_2809(&channels, 1, 1),
                Err(NetaudioError::InvalidChannel),
            );
        }
        assert_eq!(
            build_create_multicast_flow_2809(&[1], 2, 1),
            Err(NetaudioError::InvalidFlowProtocol),
        );
        assert_eq!(
            build_create_multicast_flow_2809(&(1..=215).collect::<Vec<_>>(), 1, 1),
            Err(NetaudioError::PacketTooLarge),
        );
    }
}
