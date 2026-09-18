use super::*;

#[test]
fn conmon_opcode_extracts_after_magic() {
    let mut data = vec![0u8; 0x20];
    stamp_conmon_response(&mut data, CONMON_OPCODE_PTP_CLOCK_STATUS);
    assert_eq!(
        parse_conmon_opcode(&data).unwrap().opcode,
        Some(CONMON_OPCODE_PTP_CLOCK_STATUS)
    );
    assert_eq!(parse_conmon_opcode(&[0u8; 0x20]), None);
    assert_eq!(parse_conmon_opcode(&[0u8; 4]), None);
}

#[test]
fn captured_clock_status_refresh_response_has_the_paired_opcode() {
    let response = decode_hexadecimal(
            "ffff00a8001b00000200000000010000417564696e6174650724002000000000000100060000007bfff9f9fb020000000001000002000000000100000200000000010000000100340004000002b40000000186a0000000020000000000000000000000000000000000080002005800040003000000600010000000010102010000000002000400070001000201020200000000020003000300010003020202000000000200030003",
        );
    assert_eq!(
        parse_conmon_opcode(&response).unwrap().opcode,
        Some(CONMON_OPCODE_PTP_CLOCK_STATUS)
    );
    assert!(parse_ptp_clock_status(&response).is_some());
}

#[test]
fn routing_capacity_status_parses_settled_and_transitional_authentic_packets() {
    let settled = decode_hexadecimal(
        "ffff002812870000001dc10812580000417564696e61746507240100000000000101000000800080",
    );
    assert_eq!(
        parse_routing_capacity_status(&settled),
        Some(RoutingCapacityStatus {
            unmapped_prefix_word: 0,
            state_code: 0x0101,
            routing_ready: Some(true),
            unmapped_word: 0,
            transmit_channel_count: 128,
            receive_channel_count: 128,
        })
    );

    let transitional = decode_hexadecimal(
        "ffff002812870000001dc10812580000417564696e61746507240100000000000001000000000000",
    );
    assert_eq!(
        parse_routing_capacity_status(&transitional),
        Some(RoutingCapacityStatus {
            unmapped_prefix_word: 0,
            state_code: 0x0001,
            routing_ready: Some(false),
            unmapped_word: 0,
            transmit_channel_count: 0,
            receive_channel_count: 0,
        })
    );
}

#[test]
fn routing_capacity_status_preserves_unknown_state_and_rejects_invalid_packets() {
    let mut unknown = decode_hexadecimal(
        "ffff002812870000001dc10812580000417564696e61746507240100123456789abc55aa00200010",
    );
    assert_eq!(
        parse_routing_capacity_status(&unknown),
        Some(RoutingCapacityStatus {
            unmapped_prefix_word: 0x12345678,
            state_code: 0x9abc,
            routing_ready: None,
            unmapped_word: 0x55aa,
            transmit_channel_count: 32,
            receive_channel_count: 16,
        })
    );

    unknown[26..28].copy_from_slice(&CONMON_OPCODE_SAMPLE_RATE_STATUS.to_be_bytes());
    assert_eq!(parse_routing_capacity_status(&unknown), None);

    let mut wrong_length = decode_hexadecimal(
        "ffff002812870000001dc10812580000417564696e61746507240100000000000101000000800080",
    );
    wrong_length.push(0);
    let wrong_packet_length = wrong_length.len() as u16;
    wrong_length[2..4].copy_from_slice(&wrong_packet_length.to_be_bytes());
    assert_eq!(parse_routing_capacity_status(&wrong_length), None);
}

fn captured_ad4d_switch_configuration_status() -> Vec<u8> {
    decode_hexadecimal(
        "ffff0158004e0000000eddfd4e130000417564696e617465072e00140000000000020018001000040000007f000100010001000053776974636865640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000007f0000000000000000000000000002000053706c69742f526564756e64616e74000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000028000000240000005300000000",
    )
}

#[test]
fn switch_configuration_status_parses_shipping_controller_response() {
    let response = captured_ad4d_switch_configuration_status();
    let parsed = parse_switch_configuration_status(&response).unwrap();

    assert_eq!(parsed.record_protocol_identifier, 0x072E);
    assert_eq!(parsed.unmapped_prefix_word, 0);
    assert_eq!(parsed.choice_count, 2);
    assert_eq!(parsed.choice_table_pointer, 0x0018);
    assert_eq!(parsed.referenced_value_pointer, 0x0010);
    assert_eq!(parsed.referenced_value_size, 4);
    assert_eq!(parsed.referenced_value_hexadecimal, "0000007f");
    assert_eq!(parsed.mode_codes_at_record_offsets_20_and_22, [1, 1]);
    assert_eq!(
        parsed
            .choices
            .iter()
            .map(|choice| (choice.code, choice.label.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "Switched"), (2, "Split/Redundant")]
    );
    assert_eq!(parsed.choices[0].unmapped_word, 0);
    assert_eq!(
        parsed.choices[0].unmapped_trailing_words,
        [0x0000007F, 0, 0, 0]
    );
    assert_eq!(
        parsed.choices[1].unmapped_trailing_words,
        [0x00000028, 0x00000024, 0x00000053, 0]
    );
    assert_eq!(parsed.unmapped_before_choice_table_hexadecimal, "");
    assert_eq!(parsed.unmapped_after_choice_table_hexadecimal, "");
}

#[test]
fn switch_configuration_status_rejects_invalid_pointer_count_label_and_opcode() {
    let response = captured_ad4d_switch_configuration_status();

    let mut invalid_pointer = response.clone();
    invalid_pointer[34..36].copy_from_slice(&0x0017u16.to_be_bytes());
    assert_eq!(parse_switch_configuration_status(&invalid_pointer), None);

    let mut excessive_count = response.clone();
    excessive_count[32..34].copy_from_slice(&3u16.to_be_bytes());
    assert_eq!(parse_switch_configuration_status(&excessive_count), None);

    let mut unterminated_label = response.clone();
    unterminated_label[52..180].fill(b'A');
    assert_eq!(parse_switch_configuration_status(&unterminated_label), None);

    let mut wrong_opcode = response;
    wrong_opcode[26..28].copy_from_slice(&0x0015u16.to_be_bytes());
    assert_eq!(parse_switch_configuration_status(&wrong_opcode), None);
}

#[test]
fn switch_configuration_unknown_label_keeps_raw_choice_without_guessing_mode() {
    let mut response = captured_ad4d_switch_configuration_status();
    response[52..180].fill(0);
    response[52..63].copy_from_slice(b"Future Mode");

    let parsed = parse_switch_configuration_status(&response).unwrap();
    assert_eq!(parsed.mode_codes_at_record_offsets_20_and_22, [1, 1]);
    assert_eq!(parsed.redundancy.current, None);
    assert_eq!(parsed.redundancy.configured, None);
    assert_eq!(parsed.choices[0].code, 1);
    assert_eq!(parsed.choices[0].label, "Future Mode");
    assert!(parsed.choices[0]
        .raw_choice_hexadecimal
        .starts_with("00010000467574757265204d6f646500"));
}

pub(super) fn captured_sample_rate_status_packet_28101() -> Vec<u8> {
    vec![
        0xFF, 0xFF, 0x00, 0x48, 0x16, 0x31, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0x08, 0x12, 0x58, 0x00,
        0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x24, 0x00, 0x80, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x18, 0x00, 0x06, 0x00, 0x00, 0xAC, 0x44, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0xAC, 0x44, 0x00, 0x00, 0xBB, 0x80, 0x00, 0x01, 0x58, 0x88,
        0x00, 0x01, 0x77, 0x00, 0x00, 0x02, 0xB1, 0x10, 0x00, 0x02, 0xEE, 0x00,
    ]
}

#[test]
fn configurable_u32_status_preserves_all_fields_and_follows_relocated_vector() {
    let original = captured_sample_rate_status_packet_28101();
    let parsed = parse_sample_rate_status(&original).unwrap();
    assert_eq!(parsed.record_protocol_version, 0x0724);
    assert_eq!(parsed.current_value, 44_100);
    assert_eq!(parsed.requested_value, 0);
    assert_eq!(parsed.update_mode, 2);
    assert_eq!(
        parsed.available_values,
        vec![44_100, 48_000, 88_200, 96_000, 176_400, 192_000]
    );
    assert_eq!(parsed.flags, None);

    let values = original[48..72].to_vec();
    let mut relocated = original[..48].to_vec();
    relocated.resize(56, 0xA5);
    relocated.extend_from_slice(&values);
    relocated[32..34].copy_from_slice(&0x0020u16.to_be_bytes());
    let length = u16::try_from(relocated.len()).unwrap();
    relocated[2..4].copy_from_slice(&length.to_be_bytes());
    assert_eq!(
        parse_sample_rate_status(&relocated)
            .unwrap()
            .available_values,
        parsed.available_values
    );
}

#[test]
fn configurable_u32_status_accepts_zero_and_one_choice_and_pre_0501_reboot_mode() {
    let mut zero = captured_sample_rate_status_packet_28101();
    zero.truncate(48);
    zero[34..36].copy_from_slice(&0u16.to_be_bytes());
    zero[2..4].copy_from_slice(&48u16.to_be_bytes());
    assert_eq!(
        parse_sample_rate_status(&zero).unwrap().available_values,
        Vec::<u32>::new()
    );

    let mut one = captured_sample_rate_status_packet_28101();
    one.truncate(52);
    one[34..36].copy_from_slice(&1u16.to_be_bytes());
    one[2..4].copy_from_slice(&52u16.to_be_bytes());
    assert_eq!(
        parse_sample_rate_status(&one).unwrap().available_values,
        vec![44_100]
    );

    one[24..26].copy_from_slice(&0x0400u16.to_be_bytes());
    one[44..46].copy_from_slice(&0xFFFFu16.to_be_bytes());
    assert_eq!(parse_sample_rate_status(&one).unwrap().update_mode, 1);
}

#[test]
fn configurable_u32_status_rejects_misaligned_overlapping_and_out_of_bounds_vectors() {
    let original = captured_sample_rate_status_packet_28101();
    for pointer in [0x0014u16, 0x0019] {
        let mut invalid = original.clone();
        invalid[32..34].copy_from_slice(&pointer.to_be_bytes());
        assert_eq!(parse_sample_rate_status(&invalid), None);
    }
    let mut oversized = original;
    oversized[34..36].copy_from_slice(&u16::MAX.to_be_bytes());
    assert_eq!(parse_sample_rate_status(&oversized), None);
}

pub(super) fn captured_encoding_status_packet_204720() -> Vec<u8> {
    vec![
        0xFF, 0xFF, 0x00, 0x3C, 0x21, 0x02, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0x10, 0x73, 0x32, 0x00,
        0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x24, 0x00, 0x82, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x18, 0x00, 0x03, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x20,
    ]
}

#[test]
fn encoding_uses_the_shared_configurable_u32_layout() {
    assert_eq!(
        parse_encoding_status(&captured_encoding_status_packet_204720()).unwrap(),
        ConfigurableU32Status {
            record_protocol_version: 0x0724,
            current_value: 24,
            requested_value: 0,
            update_mode: 2,
            available_values: vec![24, 16, 32],
            flags: None,
        }
    );
}

pub(super) fn captured_sample_rate_pullup_status_packet() -> Vec<u8> {
    decode_hexadecimal(
        "ffff005c001e00000200000000010000417564696e6174650724008400000000003000050000000000000000000200000000000100000000000000000000000000000000000000000000000000000001000000020000000300000004",
    )
}

#[test]
fn sample_rate_pullup_preserves_mode_flags_and_zero_choice_state() {
    let mut packet = captured_sample_rate_pullup_status_packet();
    packet[52..56].copy_from_slice(&1u32.to_be_bytes());
    let parsed = parse_sample_rate_pullup_status(&packet).unwrap();
    assert_eq!(parsed.record_protocol_version, 0x0724);
    assert_eq!(parsed.current_value, 0);
    assert_eq!(parsed.requested_value, 0);
    assert_eq!(parsed.update_mode, 2);
    assert_eq!(parsed.flags, Some(1));
    assert_eq!(parsed.available_values, vec![0, 1, 2, 3, 4]);

    packet[34..36].copy_from_slice(&0u16.to_be_bytes());
    assert!(parse_sample_rate_pullup_status(&packet)
        .unwrap()
        .available_values
        .is_empty());
}

#[test]
fn sample_rate_pullup_uses_reported_mode_before_0501_and_protects_the_flags_field() {
    let mut packet = captured_sample_rate_pullup_status_packet();
    packet[24..26].copy_from_slice(&0x0400u16.to_be_bytes());
    packet[44..46].copy_from_slice(&0u16.to_be_bytes());
    assert_eq!(
        parse_sample_rate_pullup_status(&packet)
            .unwrap()
            .update_mode,
        0
    );
    assert_eq!(
        parse_sample_rate_pullup_status(&packet).unwrap().flags,
        None
    );

    packet[24..26].copy_from_slice(&0x0724u16.to_be_bytes());
    packet[32..34].copy_from_slice(&0x001Cu16.to_be_bytes());
    assert_eq!(parse_sample_rate_pullup_status(&packet), None);
}

pub(super) fn captured_avio_input_codec_status_packet_1528() -> Vec<u8> {
    vec![
        0xFF, 0xFF, 0x00, 0x38, 0x06, 0x11, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x50, 0x69,
        0x2E, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x27, 0x10, 0x0B, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x10, 0x01, 0x02, 0x00, 0x02, 0x00,
        0x04, 0x00, 0x18, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01,
    ]
}

#[test]
fn codec_status_preserves_generic_parameter_type_mode_and_raw_values() {
    assert_eq!(
        parse_codec_status(&captured_avio_input_codec_status_packet_1528()),
        Some(CodecStatus {
            record_protocol_version: 0x0727,
            parameters: vec![CodecParameterStatus {
                parameter_type: 1,
                mode: 2,
                values: vec![5, 1],
            }],
        })
    );

    let mut unknown = captured_avio_input_codec_status_packet_1528();
    unknown[40] = 0xA5;
    unknown[41] = 0x5A;
    let parameter = &parse_codec_status(&unknown).unwrap().parameters[0];
    assert_eq!((parameter.parameter_type, parameter.mode), (0xA5, 0x5A));
    assert_eq!(parameter.values, vec![5, 1]);
}

#[test]
fn gain_status_reads_avio_input_levels_from_codec_status() {
    assert_eq!(
        parse_gain_status(&captured_avio_input_codec_status_packet_1528()),
        Some(GainStatus {
            channel_levels: vec![5, 1],
            device_type: "input".to_owned(),
            supported_levels: vec![1, 2, 3, 4, 5],
        })
    );

    let mut output = captured_avio_input_codec_status_packet_1528();
    output[40] = 2;
    output[41] = 1;
    assert_eq!(parse_gain_status(&output).unwrap().device_type, "output");

    let mut unknown = captured_avio_input_codec_status_packet_1528();
    unknown[40] = 0xA5;
    assert_eq!(parse_gain_status(&unknown), None);

    let mut out_of_range = captured_avio_input_codec_status_packet_1528();
    out_of_range[51] = 9;
    assert_eq!(parse_gain_status(&out_of_range), None);
}

#[test]
fn codec_status_rejects_invalid_descriptor_geometry_but_accepts_empty_values() {
    let mut empty = captured_avio_input_codec_status_packet_1528();
    empty[42..44].copy_from_slice(&0u16.to_be_bytes());
    assert!(parse_codec_status(&empty).unwrap().parameters[0]
        .values
        .is_empty());

    for (range, value) in [(36..38, 6u16), (44..46, 2u16), (46..48, 0x0019u16)] {
        let mut invalid = captured_avio_input_codec_status_packet_1528();
        invalid[range].copy_from_slice(&value.to_be_bytes());
        assert_eq!(parse_codec_status(&invalid), None);
    }
}

fn authentic_0086_status_packet() -> Vec<u8> {
    decode_hexadecimal(
        "ffff0028001100000200000000010000417564696e61746507240086000000001000000129ad36f0",
    )
}

#[test]
fn unmapped_0086_status_parses_authentic_a32_publication() {
    let parsed = parse_unmapped_0086_status(&authentic_0086_status_packet()).unwrap();
    assert_eq!(parsed.unmapped_word_at_body_offset_0, 0);
    assert_eq!(parsed.unmapped_word_at_body_offset_4, 0x1000_0001);
    assert_eq!(parsed.unmapped_word_at_body_offset_8, 0x29AD_36F0);
}

fn authentic_00e0_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff0034000000000200000000010000417564696e617465072400e000000000000100000000000000000000000000000000000a",
        )
}

#[test]
fn unmapped_00e0_status_parses_authentic_a32_publication() {
    let parsed = parse_unmapped_00e0_status(&authentic_00e0_status_packet()).unwrap();
    assert_eq!(parsed.unmapped_word_at_body_offset_0, 0);
    assert_eq!(parsed.unmapped_word_at_body_offset_4, 0x0001_0000);
    assert_eq!(parsed.unmapped_word_at_body_offset_8, 0);
    assert_eq!(parsed.unmapped_word_at_body_offset_12, 0);
    assert_eq!(parsed.unmapped_word_at_body_offset_16, 0);
    assert_eq!(parsed.unmapped_word_at_body_offset_20, 0x0000_000A);
}

#[test]
fn unmapped_00e0_status_parses_solicited_a32_publication() {
    let parsed = parse_unmapped_00e0_status(&decode_hexadecimal(
            "ffff0034008e00000200000000010000417564696e617465072400e0000000000001a5a50000000000000000a5a5a5a50000000a",
        ))
        .unwrap();
    assert_eq!(parsed.unmapped_word_at_body_offset_0, 0);
    assert_eq!(parsed.unmapped_word_at_body_offset_4, 0x0001_A5A5);
    assert_eq!(parsed.unmapped_word_at_body_offset_16, 0xA5A5_A5A5);
    assert_eq!(parsed.unmapped_word_at_body_offset_20, 0x0000_000A);
}

fn authentic_0106_status_packet() -> Vec<u8> {
    decode_hexadecimal("ffff0020003000000200000000010000417564696e6174650724010600000000")
}

#[test]
fn unmapped_0106_status_parses_authentic_a32_publication() {
    let parsed = parse_unmapped_0106_status(&authentic_0106_status_packet()).unwrap();
    assert_eq!(parsed.unmapped_word_at_body_offset_0, 0);
    let after_subdomain_a = parse_unmapped_0106_status(&decode_hexadecimal(
        "ffff0020007c00000200000000010000417564696e6174650724010600000000",
    ))
    .unwrap();
    assert_eq!(after_subdomain_a.unmapped_word_at_body_offset_0, 0);
}

#[test]
fn unmapped_0102_status_parses_controller_visible_variable_tails() {
    let one = parse_unmapped_0102_status(&decode_hexadecimal(
        "ffff002302b500000200000000010000417564696e6174650724010200000000000101",
    ))
    .unwrap();
    assert_eq!(one.unmapped_prefix_word, 0);
    assert_eq!(one.trailing_byte_count, 1);
    assert_eq!(one.trailing_bytes, vec![0x01]);
    let two = parse_unmapped_0102_status(&decode_hexadecimal(
        "ffff0024104200000200000000010000417564696e61746507240102000000000002ffff",
    ))
    .unwrap();
    assert_eq!(two.trailing_byte_count, 2);
    assert_eq!(two.trailing_bytes, vec![0xFF, 0xFF]);
    let eight = parse_unmapped_0102_status(&decode_hexadecimal(
        "ffff002a10c700000200000000010000417564696e617465072401020000000000080000ffffffffffff",
    ))
    .unwrap();
    assert_eq!(eight.trailing_byte_count, 8);
    assert_eq!(
        eight.trailing_bytes,
        vec![0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    );
}

fn authentic_0024_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff0030001a00000200000000010000417564696e617465072400240000000000010008001000000000000000030000",
        )
}

#[test]
fn clock_unicast_status_parses_authentic_a32_publication() {
    let parsed = parse_clock_unicast_status(&authentic_0024_status_packet()).unwrap();
    assert_eq!(parsed.raw_words, [0x0001_0008, 0x0010_0000, 0, 0x0003_0000]);
}

fn authentic_0022_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff0040000100000200000000010000417564696e61746507240022000000000003001400060003000300000000000000000000000000000000000000000000",
        )
}

#[test]
fn clock_master_status_parses_authentic_a32_publication() {
    let parsed = parse_clock_master_status(&authentic_0022_status_packet()).unwrap();
    assert_eq!(parsed.record_count, 3);
    assert_eq!(parsed.block_length, 0x0014);
    assert_eq!(parsed.status_codes, vec![0x0006, 0x0003, 0x0003]);
}

fn authentic_0026_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff004c000000000200000000010000417564696e6174650724002600000000003400010010000a001a000000260000002e4133322d30303030303100000200000000010000020000000001",
        )
}

#[test]
fn clock_identifier_status_parses_authentic_a32_device_name() {
    let parsed = parse_clock_identifier_status(&authentic_0026_status_packet()).unwrap();
    assert_eq!(parsed.name_pointer, 0x001A);
    assert_eq!(parsed.device_name, "A32-000001");
    assert_eq!(parsed.first_identifier, [2, 0, 0, 0, 0, 1]);
    assert_eq!(parsed.second_identifier, [2, 0, 0, 0, 0, 1]);
}

fn authentic_0040_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff008c001400000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c0000000000000000000000000000000000000001000003e80000000000000000000000000000000001000001000003e8000000000000000000000000000000000101000000000000",
        )
}

fn authentic_0040_status_packet_at_100_megabits_per_second() -> Vec<u8> {
    decode_hexadecimal(
            "ffff008c001000000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c000000000000000000000000000000000000000100000064000000000000000000000000000000000100000100000064000000000000000000000000000000000101000000000000",
        )
}

fn authentic_0040_status_packet_on_switch_port_three() -> Vec<u8> {
    decode_hexadecimal(
            "ffff008c001000000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c0000000000000000000000000000000000000001000003e80000000000000000000000000000000001000000000000000000000000000000000000000000000001010001000003e8",
        )
}

fn authentic_0040_status_packet_on_switch_port_three_at_100_megabits_per_second() -> Vec<u8> {
    decode_hexadecimal(
            "ffff008c001000000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c000000000000000000000000000000000000000100000064000000000000000000000000000000000100000000000000000000000000000000000000000000000101000100000064",
        )
}

fn authentic_lx_dante_0040_status_packet() -> Vec<u8> {
    decode_hexadecimal(
        "ffff0074688a0000001dc10812580000417564696e61746507240040000000000002002400400010001400000000000100000000000000000000000700010028002067380014cba8000000000000000000000001000003e800010044000000000000000000000000000000000000000000000000",
    )
}

fn authentic_avio_0040_status_packet() -> Vec<u8> {
    decode_hexadecimal(
        "ffff00589ed40000001dc1fffe50368b417564696e6174650738004000000000000100240010000000140000000000010000000000000000000000030001002800085fd80009926d00000000000000000000000100000064",
    )
}

fn authentic_ad4d_0040_status_packet() -> Vec<u8> {
    decode_hexadecimal(
        "ffff005800220000000eddfd4e130000417564696e617465072e004000000000000100240010000000140000000000010000000000000000000000070001002800169e8c00070964000000000000000000000001000003e8",
    )
}

#[test]
fn interface_statistics_parses_a32_outer_and_nested_pointer_tables() {
    let parsed = parse_interface_statistics_status(&authentic_0040_status_packet()).unwrap();
    assert_eq!(parsed.record_protocol_version, 0x0724);
    assert_eq!(parsed.header_record_pointer, 0x0010);
    assert_eq!(parsed.header_record_size_bytes, 20);
    assert_eq!(parsed.header_record_hexadecimal.len(), 40);
    assert!(parsed.raw_body_hexadecimal.starts_with("0724004000000000"));
    assert_eq!(parsed.capability_mask, 7);
    assert!(parsed.utilization_supported);
    assert!(parsed.errors_supported);
    assert!(parsed.clear_errors_supported);
    assert_eq!(parsed.interface_group_count, 1);
    assert_eq!(parsed.interface_group_pointers, vec![0x0024]);
    let group = &parsed.interface_groups[0];
    assert_eq!(group.record_count, 3);
    assert_eq!(group.record_pointers, vec![0x002C, 0x0044, 0x005C]);
    assert_eq!(group.raw_records.len(), 3);
    let selected = group.selected_stats.as_ref().unwrap();
    assert_eq!(selected.record_pointer, 0x002C);
    assert_eq!(selected.record_size_bytes, 24);
    assert_eq!(selected.transmit_raw_bytes_per_second, 0);
    assert_eq!(selected.receive_raw_bytes_per_second, 0);
    assert_eq!(selected.transmit_bits_per_second, 0);
    assert_eq!(selected.receive_bits_per_second, 0);
    assert_eq!(selected.cumulative_transmit_errors, 0);
    assert_eq!(selected.cumulative_receive_errors, 0);
    assert_eq!(selected.discriminator_status_word, 1);
    assert_eq!(selected.speed_megabits_per_second, 1000);
    assert_eq!(selected.extension_hexadecimal, "");
    assert_eq!(selected.raw_record_hexadecimal.len(), 48);
    assert_eq!(group.raw_records[1].discriminator_status_word, 0x0100_0001);
    assert_eq!(group.raw_records[2].discriminator_status_word, 0x0101_0000);
}

#[test]
fn interface_statistics_preserves_two_lx_interface_groups() {
    let parsed =
        parse_interface_statistics_status(&authentic_lx_dante_0040_status_packet()).unwrap();
    assert_eq!(parsed.interface_group_count, 2);
    assert_eq!(parsed.interface_group_pointers, vec![0x0024, 0x0040]);
    assert_eq!(parsed.interface_groups.len(), 2);
    let primary = parsed.interface_groups[0].selected_stats.as_ref().unwrap();
    assert_eq!(primary.record_pointer, 0x0028);
    assert_eq!(primary.record_size_bytes, 24);
    assert_eq!(primary.transmit_raw_bytes_per_second, 0x0020_6738);
    assert_eq!(primary.receive_raw_bytes_per_second, 0x0014_CBA8);
    assert_eq!(
        primary.transmit_bits_per_second,
        u64::from(0x0020_6738u32) * 8
    );
    assert_eq!(
        primary.receive_bits_per_second,
        u64::from(0x0014_CBA8u32) * 8
    );
    assert_eq!(primary.discriminator_status_word, 1);
    assert_eq!(primary.speed_megabits_per_second, 1000);
    assert_eq!(primary.extension_hexadecimal, "");
    let secondary = parsed.interface_groups[1].selected_stats.as_ref().unwrap();
    assert_eq!(secondary.record_pointer, 0x0044);
    assert_eq!(secondary.record_size_bytes, 24);
    assert_eq!(secondary.transmit_raw_bytes_per_second, 0);
    assert_eq!(secondary.speed_megabits_per_second, 0);
}

#[test]
fn interface_statistics_preserves_avio_rates_and_speed() {
    let parsed = parse_interface_statistics_status(&authentic_avio_0040_status_packet()).unwrap();
    let record = parsed.interface_groups[0].selected_stats.as_ref().unwrap();
    assert_eq!(record.transmit_raw_bytes_per_second, 0x0008_5FD8);
    assert_eq!(record.receive_raw_bytes_per_second, 0x0009_926D);
    assert_eq!(record.cumulative_transmit_errors, 0);
    assert_eq!(record.cumulative_receive_errors, 0);
    assert_eq!(record.discriminator_status_word, 1);
    assert_eq!(record.speed_megabits_per_second, 100);
}

#[test]
fn interface_statistics_accepts_ad4d_conmon_family() {
    let parsed = parse_interface_statistics_status(&authentic_ad4d_0040_status_packet()).unwrap();
    let record = parsed.interface_groups[0].selected_stats.as_ref().unwrap();
    assert_eq!(record.transmit_raw_bytes_per_second, 0x0016_9E8C);
    assert_eq!(record.receive_raw_bytes_per_second, 0x0007_0964);
    assert_eq!(record.record_size_bytes, 24);
    assert_eq!(record.discriminator_status_word, 1);
    assert_eq!(record.speed_megabits_per_second, 1000);
}

#[test]
fn interface_statistics_preserves_record_extensions() {
    let mut packet = authentic_avio_0040_status_packet();
    packet.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let packet_length = u16::try_from(packet.len()).unwrap();
    packet[2..4].copy_from_slice(&packet_length.to_be_bytes());

    let parsed = parse_interface_statistics_status(&packet).unwrap();
    let selected = parsed.interface_groups[0].selected_stats.as_ref().unwrap();
    assert_eq!(selected.record_size_bytes, 28);
    assert_eq!(selected.extension_hexadecimal, "deadbeef");
    assert!(selected.raw_record_hexadecimal.ends_with("deadbeef"));
}

#[test]
fn interface_statistics_uses_legacy_capability_default_before_0713() {
    let mut packet = authentic_avio_0040_status_packet();
    packet[24..26].copy_from_slice(&0x0712u16.to_be_bytes());
    packet[56..60].copy_from_slice(&u32::MAX.to_be_bytes());

    let parsed = parse_interface_statistics_status(&packet).unwrap();
    assert_eq!(parsed.capability_mask, 3);
    assert!(parsed.utilization_supported);
    assert!(parsed.errors_supported);
    assert!(!parsed.clear_errors_supported);
}

#[test]
fn interface_statistics_accepts_a_shorter_legacy_header_record() {
    let mut packet = authentic_avio_0040_status_packet();
    packet[24..26].copy_from_slice(&0x0712u16.to_be_bytes());
    packet[34..36].copy_from_slice(&0x0020u16.to_be_bytes());
    let nested_table = packet[60..64].to_vec();
    packet[56..60].copy_from_slice(&nested_table);

    let parsed = parse_interface_statistics_status(&packet).unwrap();
    assert_eq!(parsed.header_record_size_bytes, 16);
    assert_eq!(parsed.capability_mask, 3);
    assert_eq!(parsed.interface_group_pointers, vec![0x0020]);
    assert_eq!(parsed.interface_groups[0].record_pointers, vec![0x0028]);
}

#[test]
fn interface_statistics_accepts_unordered_structural_pointers() {
    let mut packet = authentic_lx_dante_0040_status_packet();
    packet[34..38].copy_from_slice(&[0x00, 0x40, 0x00, 0x24]);

    let parsed = parse_interface_statistics_status(&packet).unwrap();
    assert_eq!(parsed.interface_group_pointers, vec![0x0040, 0x0024]);
    assert_eq!(parsed.interface_groups[0].record_pointers, vec![0x0044]);
    assert_eq!(parsed.interface_groups[1].record_pointers, vec![0x0028]);
}

#[test]
fn interface_statistics_allows_a_group_without_an_eligible_selected_record() {
    let mut packet = authentic_0040_status_packet();
    packet[84] = 1;

    let parsed = parse_interface_statistics_status(&packet).unwrap();
    assert_eq!(parsed.interface_groups[0].selected_stats, None);
    assert_eq!(parsed.interface_groups[0].raw_records.len(), 3);
}

#[test]
fn interface_statistics_rejects_malformed_pointer_tables() {
    let mut group_count_overruns_packet = authentic_0040_status_packet();
    group_count_overruns_packet[32..34].copy_from_slice(&u16::MAX.to_be_bytes());
    assert_eq!(
        parse_interface_statistics_status(&group_count_overruns_packet),
        None
    );

    let mut group_pointer_before_header = authentic_0040_status_packet();
    group_pointer_before_header[34..36].copy_from_slice(&0u16.to_be_bytes());
    assert_eq!(
        parse_interface_statistics_status(&group_pointer_before_header),
        None
    );

    let mut pointer_before_table = authentic_0040_status_packet();
    pointer_before_table[62..64].copy_from_slice(&0u16.to_be_bytes());
    assert_eq!(
        parse_interface_statistics_status(&pointer_before_table),
        None
    );

    let mut duplicate_pointer = authentic_0040_status_packet();
    duplicate_pointer[64..66].copy_from_slice(&0x002Cu16.to_be_bytes());
    assert_eq!(parse_interface_statistics_status(&duplicate_pointer), None);

    let mut undersized_record = authentic_0040_status_packet();
    undersized_record[64..66].copy_from_slice(&0x0030u16.to_be_bytes());
    assert_eq!(parse_interface_statistics_status(&undersized_record), None);

    let mut group_overlaps_header = authentic_0040_status_packet();
    group_overlaps_header[34..36].copy_from_slice(&0x0018u16.to_be_bytes());
    assert_eq!(
        parse_interface_statistics_status(&group_overlaps_header),
        None
    );

    let mut record_starts_inside_header = authentic_0040_status_packet();
    record_starts_inside_header[62..64].copy_from_slice(&0x0014u16.to_be_bytes());
    assert_eq!(
        parse_interface_statistics_status(&record_starts_inside_header),
        None
    );
}

#[test]
fn interface_statistics_exposes_causally_varied_speed_without_naming_discriminators() {
    let one_thousand = parse_interface_statistics_status(&authentic_0040_status_packet()).unwrap();
    let one_hundred = parse_interface_statistics_status(
        &authentic_0040_status_packet_at_100_megabits_per_second(),
    )
    .unwrap();

    assert_eq!(
        one_thousand.interface_groups[0]
            .raw_records
            .iter()
            .map(|record| record.speed_megabits_per_second)
            .collect::<Vec<_>>(),
        vec![1000, 1000, 0]
    );
    assert_eq!(
        one_hundred.interface_groups[0]
            .raw_records
            .iter()
            .map(|record| record.speed_megabits_per_second)
            .collect::<Vec<_>>(),
        vec![100, 100, 0]
    );
    assert_eq!(
        one_thousand.interface_groups[0]
            .raw_records
            .iter()
            .map(|record| record.discriminator_status_word)
            .collect::<Vec<_>>(),
        one_hundred.interface_groups[0]
            .raw_records
            .iter()
            .map(|record| record.discriminator_status_word)
            .collect::<Vec<_>>()
    );
}

#[test]
fn interface_statistics_selects_only_first_zero_discriminator_record() {
    let port_zero = parse_interface_statistics_status(&authentic_0040_status_packet()).unwrap();
    let port_three =
        parse_interface_statistics_status(&authentic_0040_status_packet_on_switch_port_three())
            .unwrap();
    let port_three_at_one_hundred = parse_interface_statistics_status(
        &authentic_0040_status_packet_on_switch_port_three_at_100_megabits_per_second(),
    )
    .unwrap();

    for parsed in [&port_zero, &port_three, &port_three_at_one_hundred] {
        assert_eq!(
            parsed.interface_groups[0]
                .selected_stats
                .as_ref()
                .unwrap()
                .record_pointer,
            0x002C
        );
    }
    assert_eq!(
        port_three.interface_groups[0].raw_records[1].discriminator_status_word,
        0x0100_0000
    );
    assert_eq!(
        port_three.interface_groups[0].raw_records[2].discriminator_status_word,
        0x0101_0001
    );
}

#[test]
fn clear_configuration_status_parses_authentic_publications_and_preserves_unknown_values() {
    let mode_one = decode_hexadecimal(
        "ffff0028000f00000200000000010000417564696e61746507240078000000000000000300000001",
    );
    assert_eq!(
        parse_clear_configuration_status(&mode_one),
        Some(ClearConfigurationStatus {
            record_protocol_identifier: 0x0724,
            unmapped_first_word: 0,
            available_actions_mask: 3,
            action_result_code: 1,
        })
    );

    let mut unknown = mode_one.clone();
    unknown[CONMON_CLEAR_CONFIGURATION_FIRST_WORD_OFFSET
        ..CONMON_CLEAR_CONFIGURATION_FIRST_WORD_OFFSET + 4]
        .copy_from_slice(&0x11223344u32.to_be_bytes());
    unknown[CONMON_CLEAR_CONFIGURATION_AVAILABLE_ACTIONS_MASK_OFFSET
        ..CONMON_CLEAR_CONFIGURATION_AVAILABLE_ACTIONS_MASK_OFFSET + 4]
        .copy_from_slice(&0x80000003u32.to_be_bytes());
    unknown[CONMON_CLEAR_CONFIGURATION_ACTION_RESULT_CODE_OFFSET
        ..CONMON_CLEAR_CONFIGURATION_ACTION_RESULT_CODE_OFFSET + 4]
        .copy_from_slice(&u32::MAX.to_be_bytes());
    let parsed_unknown = parse_clear_configuration_status(&unknown).unwrap();
    assert_eq!(parsed_unknown.unmapped_first_word, 0x11223344);
    assert_eq!(parsed_unknown.available_actions_mask, 0x80000003);
    assert_eq!(parsed_unknown.action_result_code, u32::MAX);

    assert_eq!(parse_clear_configuration_status(&mode_one[..39]), None);
    let mut wrong_record_identifier = mode_one.clone();
    wrong_record_identifier[25] = 0x3E;
    assert_eq!(
        parse_clear_configuration_status(&wrong_record_identifier),
        None
    );
    let mut wrong_opcode = mode_one;
    wrong_opcode[26..28].copy_from_slice(&0x0077u16.to_be_bytes());
    assert_eq!(parse_clear_configuration_status(&wrong_opcode), None);
}

#[test]
fn aes67_status_maps_state_byte() {
    let mut data = vec![0u8; 0x22];
    stamp_conmon_response(&mut data, CONMON_OPCODE_AES67_CURRENT_NEW);
    data[0x21] = 0x03;
    let parsed = parse_aes67_status(&data).unwrap();
    assert_eq!(parsed.aes67_current, Some(true));
    assert_eq!(parsed.aes67_configured, Some(true));
}
