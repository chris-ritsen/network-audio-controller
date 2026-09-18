use super::*;

#[test]
fn lock_reset_status_parses_authentic_zero_one_and_identifier_bearing_records() {
    let zero = decode_hexadecimal(
            "ffff0030002e00000200000000010000417564696e617465072410090000000000000000000000080000000000000000",
        );
    let parsed_zero = parse_lock_reset_status(&zero).unwrap();
    assert_eq!(parsed_zero.record_protocol_identifier, 0x0724);
    assert_eq!(parsed_zero.unmapped_prefix_word, 0);
    assert_eq!(parsed_zero.lock_state_code, 0);
    assert_eq!(parsed_zero.is_locked, Some(false));
    assert_eq!(parsed_zero.status_code, 0);
    assert_eq!(parsed_zero.lock_identifier_count, 0);
    assert_eq!(parsed_zero.lock_identifier_width, 8);
    assert_eq!(parsed_zero.lock_identifier_data_offset, 0);
    assert_eq!(parsed_zero.unmapped_trailer_words, [0, 0, 0]);
    assert!(parsed_zero.lock_identifiers.is_empty());

    let one = decode_hexadecimal(
            "ffff003008390000001dc1fffe5279b6417564696e617465073810090000000000000001000000080000000000000000",
        );
    let parsed_one = parse_lock_reset_status(&one).unwrap();
    assert_eq!(parsed_one.record_protocol_identifier, 0x0738);
    assert_eq!(parsed_one.status_code, 1);
    assert!(parsed_one.lock_identifiers.is_empty());

    let identifier_bearing = decode_hexadecimal(
            "ffff00500bf50000001dc1fffe53ef37417564696e617465073810090000000000000004000400080018000000000000001dc1fffe081258001dc1fffe510295001dc1fffe50cac5001dc1fffe5279b6",
        );
    let parsed_identifier_bearing = parse_lock_reset_status(&identifier_bearing).unwrap();
    assert_eq!(parsed_identifier_bearing.is_locked, Some(false));
    assert_eq!(parsed_identifier_bearing.status_code, 4);
    assert_eq!(parsed_identifier_bearing.lock_identifier_count, 4);
    assert_eq!(parsed_identifier_bearing.lock_identifier_data_offset, 24);
    assert_eq!(
        parsed_identifier_bearing.lock_identifiers,
        [
            "001dc1fffe081258",
            "001dc1fffe510295",
            "001dc1fffe50cac5",
            "001dc1fffe5279b6",
        ]
    );
    assert_eq!(
            parsed_identifier_bearing.raw_record_hexadecimal,
            "073810090000000000000004000400080018000000000000001dc1fffe081258001dc1fffe510295001dc1fffe50cac5001dc1fffe5279b6"
        );
}

#[test]
fn conmon_export_fragment_parses_known_and_unknown_tags_and_selectors() {
    let mut packet = decode_hexadecimal(
            "ffff0037005e00000200000000010000417564696e6174650724ff05000000004c4f4753000000030001000100000003001c0000616263",
        );
    let original = packet.clone();
    let parsed = parse_conmon_export_fragment(&packet).unwrap();
    assert_eq!(parsed.envelope_sequence_identifier, 0x005E);
    assert_eq!(parsed.record_protocol_identifier, 0x0724);
    assert_eq!(parsed.echoed_tag_hexadecimal, "4c4f4753");
    assert_eq!(parsed.total_encoded_size, 3);
    assert_eq!(parsed.selector_value, 1);
    assert_eq!(parsed.fragment_identifier, 1);
    assert!(!parsed.has_more_fragments);
    assert_eq!(parsed.fragment_size, 3);
    assert_eq!(parsed.header_size, 28);
    assert_eq!(parsed.data_hexadecimal, "616263");

    for length in 0..original.len() {
        assert_eq!(parse_conmon_export_fragment(&original[..length]), None);
    }
    packet[48..50].copy_from_slice(&27u16.to_be_bytes());
    assert_eq!(parse_conmon_export_fragment(&packet), None);
    packet = original.clone();
    packet[32..36].copy_from_slice(b"CAP1");
    packet[40..42].copy_from_slice(&2u16.to_be_bytes());
    let capability_fragment = parse_conmon_export_fragment(&packet).unwrap();
    assert_eq!(capability_fragment.echoed_tag_hexadecimal, "43415031");
    assert_eq!(capability_fragment.selector_value, 2);
    packet = original.clone();
    packet[44..46].copy_from_slice(&2u16.to_be_bytes());
    assert_eq!(parse_conmon_export_fragment(&packet), None);
    packet = original.clone();
    packet[28] = 1;
    assert_eq!(parse_conmon_export_fragment(&packet), None);
}

#[test]
fn lock_reset_status_rejects_inconsistent_variable_records() {
    let valid = decode_hexadecimal(
            "ffff00500bf50000001dc1fffe53ef37417564696e617465073810090000000000000004000400080018000000000000001dc1fffe081258001dc1fffe510295001dc1fffe50cac5001dc1fffe5279b6",
        );
    for length in 0..valid.len() {
        assert_eq!(parse_lock_reset_status(&valid[..length]), None);
    }

    let mut invalid_count = valid.clone();
    invalid_count[36..38].copy_from_slice(&3u16.to_be_bytes());
    assert_eq!(parse_lock_reset_status(&invalid_count), None);

    let mut invalid_width = valid.clone();
    invalid_width[38..40].copy_from_slice(&16u16.to_be_bytes());
    assert_eq!(parse_lock_reset_status(&invalid_width), None);

    let mut invalid_offset = valid.clone();
    invalid_offset[40..42].copy_from_slice(&16u16.to_be_bytes());
    assert_eq!(parse_lock_reset_status(&invalid_offset), None);

    let mut locked_status_zero = valid;
    locked_status_zero[32..34].copy_from_slice(&1u16.to_be_bytes());
    locked_status_zero[34..36].copy_from_slice(&0u16.to_be_bytes());
    let parsed_locked_status_zero = parse_lock_reset_status(&locked_status_zero).unwrap();
    assert_eq!(parsed_locked_status_zero.is_locked, Some(true));
    assert_eq!(parsed_locked_status_zero.status_code, 0);
    assert_eq!(parsed_locked_status_zero.lock_identifier_count, 4);
}
