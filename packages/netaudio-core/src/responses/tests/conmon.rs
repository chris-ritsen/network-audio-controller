use super::super::conmon::sample_rate_pullup_value;
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
    let response = decode_hex(
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
    let settled = decode_hex(
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

    let transitional = decode_hex(
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
    let mut unknown = decode_hex(
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

    let mut wrong_length = decode_hex(
        "ffff002812870000001dc10812580000417564696e61746507240100000000000101000000800080",
    );
    wrong_length.push(0);
    let wrong_packet_length = wrong_length.len() as u16;
    wrong_length[2..4].copy_from_slice(&wrong_packet_length.to_be_bytes());
    assert_eq!(parse_routing_capacity_status(&wrong_length), None);
}

fn captured_ad4d_switch_configuration_status() -> Vec<u8> {
    decode_hex(
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
fn sample_rate_status_parses_captured_packet_28101() {
    let parsed = parse_sample_rate_status(&captured_sample_rate_status_packet_28101()).unwrap();
    assert_eq!(parsed.current_sample_rate, 44_100);
    assert_eq!(
        parsed.supported_sample_rates,
        vec![44_100, 48_000, 88_200, 96_000, 176_400, 192_000]
    );
}

#[test]
fn sample_rate_status_parses_captured_packet_4170820() {
    let data = [
        0xFF, 0xFF, 0x00, 0x34, 0x06, 0x1A, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0x08, 0x12, 0x58, 0x00,
        0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x24, 0x00, 0x80, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0xBB, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0xBB, 0x80,
    ];
    let parsed = parse_sample_rate_status(&data).unwrap();
    assert_eq!(parsed.current_sample_rate, 48_000);
    assert_eq!(parsed.supported_sample_rates, vec![48_000]);
}

#[test]
fn sample_rate_status_parses_captured_packet_9695783() {
    let data = [
        0xFF, 0xFF, 0x00, 0x40, 0xFD, 0x2A, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x53, 0xEF,
        0x37, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x38, 0x00, 0x80, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x18, 0x00, 0x04, 0x00, 0x00, 0xBB, 0x80, 0x00, 0x00, 0xBB, 0x80, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0xAC, 0x44, 0x00, 0x00, 0xBB, 0x80, 0x00, 0x01, 0x58, 0x88,
        0x00, 0x01, 0x77, 0x00,
    ];
    let parsed = parse_sample_rate_status(&data).unwrap();
    assert_eq!(parsed.current_sample_rate, 48_000);
    assert_eq!(
        parsed.supported_sample_rates,
        vec![44_100, 48_000, 88_200, 96_000]
    );
}

#[test]
fn sample_rate_status_rejects_count_exceeding_packet() {
    let mut data = captured_sample_rate_status_packet_28101();
    data[CONMON_SUPPORTED_SAMPLE_RATE_COUNT_OFFSET..CONMON_SUPPORTED_SAMPLE_RATE_COUNT_OFFSET + 2]
        .copy_from_slice(&7u16.to_be_bytes());
    assert_eq!(parse_sample_rate_status(&data), None);
}

#[test]
fn sample_rate_status_preserves_uninterpreted_trailing_bytes() {
    let mut data = captured_sample_rate_status_packet_28101();
    data.extend_from_slice(&[0x12, 0x34]);
    let packet_length = u16::try_from(data.len()).unwrap();
    data[2..4].copy_from_slice(&packet_length.to_be_bytes());
    assert!(parse_sample_rate_status(&data).is_some());
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
fn encoding_status_parses_captured_packet_204720() {
    let parsed = parse_encoding_status(&captured_encoding_status_packet_204720()).unwrap();
    assert_eq!(parsed.current_encoding, 24);
    assert_eq!(parsed.supported_encodings, vec![24, 16, 32]);
}

#[test]
fn encoding_status_parses_captured_packet_645566() {
    let data = [
        0xFF, 0xFF, 0x00, 0x34, 0x79, 0xB2, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x50, 0xCA,
        0xC5, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x38, 0x00, 0x82, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x18, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18,
    ];
    let parsed = parse_encoding_status(&data).unwrap();
    assert_eq!(parsed.current_encoding, 24);
    assert_eq!(parsed.supported_encodings, vec![24]);
}

#[test]
fn encoding_status_rejects_invalid_envelope_and_oversized_count() {
    let mut wrong_protocol = captured_encoding_status_packet_204720();
    wrong_protocol[0..2].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
    assert_eq!(parse_encoding_status(&wrong_protocol), None);

    let mut wrong_opcode = captured_encoding_status_packet_204720();
    wrong_opcode[26..28].copy_from_slice(&CONMON_OPCODE_SAMPLE_RATE_STATUS.to_be_bytes());
    assert_eq!(parse_encoding_status(&wrong_opcode), None);

    let mut wrong_declared_length = captured_encoding_status_packet_204720();
    wrong_declared_length[2..4].copy_from_slice(&59u16.to_be_bytes());
    assert_eq!(parse_encoding_status(&wrong_declared_length), None);

    let mut oversized_count = captured_encoding_status_packet_204720();
    oversized_count
        [CONMON_SUPPORTED_ENCODING_COUNT_OFFSET..CONMON_SUPPORTED_ENCODING_COUNT_OFFSET + 2]
        .copy_from_slice(&4u16.to_be_bytes());
    assert_eq!(parse_encoding_status(&oversized_count), None);
}

pub(super) fn captured_sample_rate_pullup_status_packet() -> Vec<u8> {
    decode_hex(
            "ffff005c001e00000200000000010000417564696e6174650724008400000000003000050000000000000000000200000000000100000000000000000000000000000000000000000000000000000001000000020000000300000004",
        )
}

#[test]
fn sample_rate_pullup_status_parses_authentic_a32_packet_and_semantics() {
    let parsed =
        parse_sample_rate_pullup_status(&captured_sample_rate_pullup_status_packet()).unwrap();

    assert_eq!(parsed.applied_value, sample_rate_pullup_value(0));
    assert_eq!(parsed.requested_value, sample_rate_pullup_value(0));
    assert_eq!(parsed.mode_code, 2);
    assert_eq!(parsed.unmapped_word_at_body_offset_20, 1);
    assert_eq!(
        parsed.supported_values,
        (0..=4).map(sample_rate_pullup_value).collect::<Vec<_>>()
    );
    assert_eq!(
        parsed.supported_values[1],
        SampleRatePullupValue {
            raw_value: 1,
            meaning: SampleRatePullupMeaning::PositiveFourPointOneSixSixSevenPercent,
            rate_multiplier_numerator: Some(25),
            rate_multiplier_denominator: Some(24),
        }
    );
}

fn retained_sample_rate_pullup_status_packet() -> Vec<u8> {
    decode_hex(
            "ffff005c000c00000200000000010000417564696e6174650724008400000000003000050000000100000001000200000000000100000000000000000000000000000000000000000000000000000001000000020000000300000004",
        )
}

#[test]
fn sample_rate_pullup_status_parses_retained_raw_one_publication() {
    let parsed =
        parse_sample_rate_pullup_status(&retained_sample_rate_pullup_status_packet()).unwrap();

    assert_eq!(parsed.applied_value, sample_rate_pullup_value(1));
    assert_eq!(parsed.requested_value, sample_rate_pullup_value(1));
    assert_eq!(parsed.mode_code, 2);
    assert_eq!(parsed.unmapped_word_at_body_offset_20, 1);
    assert_eq!(
        parsed.supported_values,
        (0..=4).map(sample_rate_pullup_value).collect::<Vec<_>>()
    );
    assert_eq!(
        parsed.applied_value.meaning,
        SampleRatePullupMeaning::PositiveFourPointOneSixSixSevenPercent
    );
}

#[test]
fn sample_rate_pullup_status_preserves_unknown_values_and_rejects_invalid_vectors() {
    let mut unknown = captured_sample_rate_pullup_status_packet();
    unknown[CONMON_SAMPLE_RATE_PULLUP_APPLIED_VALUE_OFFSET
        ..CONMON_SAMPLE_RATE_PULLUP_APPLIED_VALUE_OFFSET + 4]
        .copy_from_slice(&9u32.to_be_bytes());
    unknown[88..92].copy_from_slice(&9u32.to_be_bytes());
    let parsed = parse_sample_rate_pullup_status(&unknown).unwrap();
    assert_eq!(parsed.applied_value, sample_rate_pullup_value(9));
    assert_eq!(parsed.supported_values[4], sample_rate_pullup_value(9));

    let mut overlapping_vector = captured_sample_rate_pullup_status_packet();
    overlapping_vector[CONMON_SAMPLE_RATE_PULLUP_VECTOR_OFFSET_FIELD
        ..CONMON_SAMPLE_RATE_PULLUP_VECTOR_OFFSET_FIELD + 2]
        .copy_from_slice(&0x0010u16.to_be_bytes());
    assert_eq!(parse_sample_rate_pullup_status(&overlapping_vector), None);

    let mut oversized_vector = captured_sample_rate_pullup_status_packet();
    oversized_vector[CONMON_SAMPLE_RATE_PULLUP_VECTOR_COUNT_FIELD
        ..CONMON_SAMPLE_RATE_PULLUP_VECTOR_COUNT_FIELD + 2]
        .copy_from_slice(&6u16.to_be_bytes());
    assert_eq!(parse_sample_rate_pullup_status(&oversized_vector), None);
}

pub(super) fn captured_input_gain_status_packet_1528() -> Vec<u8> {
    vec![
        0xFF, 0xFF, 0x00, 0x38, 0x06, 0x11, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x50, 0x69,
        0x2E, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x27, 0x10, 0x0B, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x10, 0x01, 0x02, 0x00, 0x02, 0x00,
        0x04, 0x00, 0x18, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01,
    ]
}

#[test]
fn gain_status_parses_captured_input_packet_1528() {
    assert_eq!(
        parse_gain_status(&captured_input_gain_status_packet_1528()),
        Some(GainStatus {
            device_type: "input".to_owned(),
            channel_levels: vec![5, 1],
        })
    );
}

fn live_avio_input_gain_status_with_unmapped_header_byte() -> Vec<u8> {
    vec![
        0xFF, 0xFF, 0x00, 0x38, 0xEE, 0xE5, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x50, 0x69,
        0x2E, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x38, 0x10, 0x0B, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x10, 0x01, 0x02, 0x00, 0x02, 0x00,
        0x04, 0x00, 0x18, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04,
    ]
}

#[test]
fn gain_status_parses_live_avio_packets_when_unmapped_header_byte_changes() {
    assert_eq!(
        parse_gain_status(&live_avio_input_gain_status_with_unmapped_header_byte()),
        Some(GainStatus {
            device_type: "input".to_owned(),
            channel_levels: vec![4, 4],
        })
    );
    let live_output = [
        0xFF, 0xFF, 0x00, 0x38, 0xEF, 0xB0, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x50, 0x7B,
        0x8D, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x38, 0x10, 0x0B, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x10, 0x02, 0x01, 0x00, 0x02, 0x00,
        0x04, 0x00, 0x18, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04,
    ];
    assert_eq!(
        parse_gain_status(&live_output),
        Some(GainStatus {
            device_type: "output".to_owned(),
            channel_levels: vec![4, 4],
        })
    );
}

#[test]
fn gain_status_parses_captured_output_packet_1585() {
    let data = [
        0xFF, 0xFF, 0x00, 0x38, 0x08, 0x10, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0xFF, 0xFE, 0x50, 0x7B,
        0x8D, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x27, 0x10, 0x0B, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x10, 0x02, 0x01, 0x00, 0x02, 0x00,
        0x04, 0x00, 0x18, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04,
    ];
    assert_eq!(
        parse_gain_status(&data),
        Some(GainStatus {
            device_type: "output".to_owned(),
            channel_levels: vec![4, 4],
        })
    );
}

#[test]
fn gain_status_rejects_unknown_direction_and_inconsistent_channel_count() {
    let mut unknown_direction = captured_input_gain_status_packet_1528();
    unknown_direction[CONMON_GAIN_DIRECTION_OFFSET..CONMON_GAIN_DIRECTION_OFFSET + 2]
        .copy_from_slice(&0x0101u16.to_be_bytes());
    assert_eq!(parse_gain_status(&unknown_direction), None);

    let mut oversized_count = captured_input_gain_status_packet_1528();
    oversized_count[CONMON_GAIN_CHANNEL_COUNT_OFFSET..CONMON_GAIN_CHANNEL_COUNT_OFFSET + 2]
        .copy_from_slice(&3u16.to_be_bytes());
    assert_eq!(parse_gain_status(&oversized_count), None);
}

fn authentic_0086_status_packet() -> Vec<u8> {
    decode_hex("ffff0028001100000200000000010000417564696e61746507240086000000001000000129ad36f0")
}

#[test]
fn unmapped_0086_status_parses_authentic_a32_publication() {
    let parsed = parse_unmapped_0086_status(&authentic_0086_status_packet()).unwrap();
    assert_eq!(parsed.unmapped_word_at_body_offset_0, 0);
    assert_eq!(parsed.unmapped_word_at_body_offset_4, 0x1000_0001);
    assert_eq!(parsed.unmapped_word_at_body_offset_8, 0x29AD_36F0);
}

fn authentic_00e0_status_packet() -> Vec<u8> {
    decode_hex(
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
    let parsed = parse_unmapped_00e0_status(&decode_hex(
            "ffff0034008e00000200000000010000417564696e617465072400e0000000000001a5a50000000000000000a5a5a5a50000000a",
        ))
        .unwrap();
    assert_eq!(parsed.unmapped_word_at_body_offset_0, 0);
    assert_eq!(parsed.unmapped_word_at_body_offset_4, 0x0001_A5A5);
    assert_eq!(parsed.unmapped_word_at_body_offset_16, 0xA5A5_A5A5);
    assert_eq!(parsed.unmapped_word_at_body_offset_20, 0x0000_000A);
}

fn authentic_0106_status_packet() -> Vec<u8> {
    decode_hex("ffff0020003000000200000000010000417564696e6174650724010600000000")
}

#[test]
fn unmapped_0106_status_parses_authentic_a32_publication() {
    let parsed = parse_unmapped_0106_status(&authentic_0106_status_packet()).unwrap();
    assert_eq!(parsed.unmapped_word_at_body_offset_0, 0);
    let after_subdomain_a = parse_unmapped_0106_status(&decode_hex(
        "ffff0020007c00000200000000010000417564696e6174650724010600000000",
    ))
    .unwrap();
    assert_eq!(after_subdomain_a.unmapped_word_at_body_offset_0, 0);
}

#[test]
fn unmapped_0102_status_parses_controller_visible_variable_tails() {
    let one = parse_unmapped_0102_status(&decode_hex(
        "ffff002302b500000200000000010000417564696e6174650724010200000000000101",
    ))
    .unwrap();
    assert_eq!(one.unmapped_prefix_word, 0);
    assert_eq!(one.trailing_byte_count, 1);
    assert_eq!(one.trailing_bytes, vec![0x01]);
    let two = parse_unmapped_0102_status(&decode_hex(
        "ffff0024104200000200000000010000417564696e61746507240102000000000002ffff",
    ))
    .unwrap();
    assert_eq!(two.trailing_byte_count, 2);
    assert_eq!(two.trailing_bytes, vec![0xFF, 0xFF]);
    let eight = parse_unmapped_0102_status(&decode_hex(
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
    decode_hex(
            "ffff0030001a00000200000000010000417564696e617465072400240000000000010008001000000000000000030000",
        )
}

#[test]
fn unmapped_0024_status_parses_authentic_a32_publication() {
    let parsed = parse_unmapped_0024_status(&authentic_0024_status_packet()).unwrap();
    assert_eq!(parsed.unmapped_word_at_body_offset_0, 0);
    assert_eq!(parsed.unmapped_word_at_body_offset_4, 0x0001_0008);
    assert_eq!(parsed.unmapped_word_at_body_offset_8, 0x0010_0000);
    assert_eq!(parsed.unmapped_word_at_body_offset_12, 0);
    assert_eq!(parsed.unmapped_word_at_body_offset_16, 0x0003_0000);
}

fn authentic_0022_status_packet() -> Vec<u8> {
    decode_hex(
            "ffff0040000100000200000000010000417564696e61746507240022000000000003001400060003000300000000000000000000000000000000000000000000",
        )
}

#[test]
fn unmapped_0022_status_parses_authentic_a32_publication() {
    let parsed = parse_unmapped_0022_status(&authentic_0022_status_packet()).unwrap();
    assert_eq!(parsed.unmapped_prefix_word, 0);
    assert_eq!(parsed.record_count, 3);
    assert_eq!(parsed.unmapped_word_at_body_offset_6, 0x0014);
    assert_eq!(parsed.unmapped_codes, vec![0x0006, 0x0003, 0x0003]);
}

fn authentic_0026_status_packet() -> Vec<u8> {
    decode_hex(
            "ffff004c000000000200000000010000417564696e6174650724002600000000003400010010000a001a000000260000002e4133322d30303030303100000200000000010000020000000001",
        )
}

#[test]
fn unmapped_0026_status_parses_authentic_a32_device_name() {
    let parsed = parse_unmapped_0026_status(&authentic_0026_status_packet()).unwrap();
    assert_eq!(parsed.name_pointer, 0x001A);
    assert_eq!(parsed.device_name, "A32-000001");
    assert_eq!(
        parsed.trailing_bytes,
        decode_hex("000200000000010000020000000001")
    );
}

fn authentic_0040_status_packet() -> Vec<u8> {
    decode_hex(
            "ffff008c001400000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c0000000000000000000000000000000000000001000003e80000000000000000000000000000000001000001000003e8000000000000000000000000000000000101000000000000",
        )
}

fn authentic_0040_status_packet_at_100_megabits_per_second() -> Vec<u8> {
    decode_hex(
            "ffff008c001000000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c000000000000000000000000000000000000000100000064000000000000000000000000000000000100000100000064000000000000000000000000000000000101000000000000",
        )
}

fn authentic_0040_status_packet_on_switch_port_three() -> Vec<u8> {
    decode_hex(
            "ffff008c001000000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c0000000000000000000000000000000000000001000003e80000000000000000000000000000000001000000000000000000000000000000000000000000000001010001000003e8",
        )
}

fn authentic_0040_status_packet_on_switch_port_three_at_100_megabits_per_second() -> Vec<u8> {
    decode_hex(
            "ffff008c001000000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c000000000000000000000000000000000000000100000064000000000000000000000000000000000100000000000000000000000000000000000000000000000101000100000064",
        )
}

fn authentic_lx_dante_0040_status_packet() -> Vec<u8> {
    decode_hex(
        "ffff0074688a0000001dc10812580000417564696e61746507240040000000000002002400400010001400000000000100000000000000000000000700010028002067380014cba8000000000000000000000001000003e800010044000000000000000000000000000000000000000000000000",
    )
}

fn authentic_avio_0040_status_packet() -> Vec<u8> {
    decode_hex(
        "ffff00589ed40000001dc1fffe50368b417564696e6174650738004000000000000100240010000000140000000000010000000000000000000000030001002800085fd80009926d00000000000000000000000100000064",
    )
}

fn authentic_ad4d_0040_status_packet() -> Vec<u8> {
    decode_hex(
        "ffff005800220000000eddfd4e130000417564696e617465072e004000000000000100240010000000140000000000010000000000000000000000070001002800169e8c00070964000000000000000000000001000003e8",
    )
}

#[test]
fn unmapped_0040_status_parses_authentic_a32_pointer_table() {
    let parsed = parse_unmapped_0040_status(&authentic_0040_status_packet()).unwrap();
    assert_eq!(parsed.record_count, 3);
    assert_eq!(parsed.record_pointers, vec![0x002C, 0x0044, 0x005C]);
    assert_eq!(parsed.records.len(), 3);
    assert_eq!(parsed.records[0].unmapped_prefix_words, [0, 0, 0, 0]);
    assert_eq!(parsed.records[0].raw_link_status_word, 1);
    assert!(parsed.records[0].link_up);
    assert_eq!(parsed.records[0].link_speed_megabits_per_second, 1000);
    assert_eq!(parsed.records[1].unmapped_prefix_words, [0, 0, 0, 0]);
    assert_eq!(parsed.records[1].raw_link_status_word, 0x0100_0001);
    assert!(parsed.records[1].link_up);
    assert_eq!(parsed.records[1].link_speed_megabits_per_second, 1000);
    assert_eq!(parsed.records[2].unmapped_prefix_words, [0, 0, 0, 0]);
    assert_eq!(parsed.records[2].raw_link_status_word, 0x0101_0000);
    assert!(!parsed.records[2].link_up);
    assert_eq!(parsed.records[2].link_speed_megabits_per_second, 0);
    assert_eq!(parsed.records[0].record_pointer, 0x002C);
    assert_eq!(parsed.records[0].record_size_bytes, 24);
    assert_eq!(parsed.records[0].unmapped_trailing_hexadecimal, "");
    assert_eq!(parsed.records[0].raw_record_hexadecimal.len(), 48);
}

#[test]
fn unmapped_0040_status_preserves_lx_dante_record_extension() {
    let parsed = parse_unmapped_0040_status(&authentic_lx_dante_0040_status_packet()).unwrap();
    assert_eq!(parsed.record_count, 1);
    assert_eq!(parsed.record_pointers, vec![0x0028]);
    assert_eq!(parsed.records[0].record_pointer, 0x0028);
    assert_eq!(parsed.records[0].record_size_bytes, 52);
    assert_eq!(
        parsed.records[0].unmapped_prefix_words,
        [0x0020_6738, 0x0014_CBA8, 0, 0]
    );
    assert_eq!(parsed.records[0].raw_link_status_word, 1);
    assert!(parsed.records[0].link_up);
    assert_eq!(parsed.records[0].link_speed_megabits_per_second, 1000);
    assert_eq!(
        parsed.records[0].unmapped_trailing_hexadecimal,
        "00010044000000000000000000000000000000000000000000000000"
    );
    assert_eq!(parsed.records[0].raw_record_hexadecimal.len(), 104);
}

#[test]
fn unmapped_0040_status_preserves_avio_record() {
    let parsed = parse_unmapped_0040_status(&authentic_avio_0040_status_packet()).unwrap();
    assert_eq!(parsed.record_count, 1);
    assert_eq!(parsed.record_pointers, vec![0x0028]);
    assert_eq!(parsed.records[0].record_pointer, 0x0028);
    assert_eq!(parsed.records[0].record_size_bytes, 24);
    assert_eq!(
        parsed.records[0].unmapped_prefix_words,
        [0x0008_5FD8, 0x0009_926D, 0, 0]
    );
    assert_eq!(parsed.records[0].raw_link_status_word, 1);
    assert!(parsed.records[0].link_up);
    assert_eq!(parsed.records[0].link_speed_megabits_per_second, 100);
    assert_eq!(parsed.records[0].unmapped_trailing_hexadecimal, "");
}

#[test]
fn unmapped_0040_status_accepts_ad4d_conmon_family() {
    let parsed = parse_unmapped_0040_status(&authentic_ad4d_0040_status_packet()).unwrap();
    assert_eq!(parsed.record_count, 1);
    assert_eq!(parsed.record_pointers, vec![0x0028]);
    assert_eq!(parsed.records[0].record_size_bytes, 24);
    assert_eq!(parsed.records[0].raw_link_status_word, 1);
    assert!(parsed.records[0].link_up);
    assert_eq!(parsed.records[0].link_speed_megabits_per_second, 1000);
    assert_eq!(parsed.records[0].unmapped_trailing_hexadecimal, "");
}

#[test]
fn unmapped_0040_status_rejects_malformed_pointer_tables() {
    let mut pointer_before_table = authentic_0040_status_packet();
    pointer_before_table[62..64].copy_from_slice(&0u16.to_be_bytes());
    assert_eq!(parse_unmapped_0040_status(&pointer_before_table), None);

    let mut duplicate_pointer = authentic_0040_status_packet();
    duplicate_pointer[64..66].copy_from_slice(&0x002Cu16.to_be_bytes());
    assert_eq!(parse_unmapped_0040_status(&duplicate_pointer), None);

    let mut undersized_record = authentic_0040_status_packet();
    undersized_record[64..66].copy_from_slice(&0x0030u16.to_be_bytes());
    assert_eq!(parse_unmapped_0040_status(&undersized_record), None);
}

#[test]
fn unmapped_0040_status_exposes_causally_varied_link_speed() {
    let one_thousand = parse_unmapped_0040_status(&authentic_0040_status_packet()).unwrap();
    let one_hundred =
        parse_unmapped_0040_status(&authentic_0040_status_packet_at_100_megabits_per_second())
            .unwrap();

    assert_eq!(
        one_thousand
            .records
            .iter()
            .map(|record| record.link_speed_megabits_per_second)
            .collect::<Vec<_>>(),
        vec![1000, 1000, 0]
    );
    assert_eq!(
        one_hundred
            .records
            .iter()
            .map(|record| record.link_speed_megabits_per_second)
            .collect::<Vec<_>>(),
        vec![100, 100, 0]
    );
    assert_eq!(
        one_thousand
            .records
            .iter()
            .map(|record| record.raw_link_status_word)
            .collect::<Vec<_>>(),
        one_hundred
            .records
            .iter()
            .map(|record| record.raw_link_status_word)
            .collect::<Vec<_>>()
    );
}

#[test]
fn unmapped_0040_status_maps_switch_port_link_state_to_records() {
    let port_zero = parse_unmapped_0040_status(&authentic_0040_status_packet()).unwrap();
    let port_three =
        parse_unmapped_0040_status(&authentic_0040_status_packet_on_switch_port_three()).unwrap();
    let port_three_at_one_hundred = parse_unmapped_0040_status(
        &authentic_0040_status_packet_on_switch_port_three_at_100_megabits_per_second(),
    )
    .unwrap();

    assert_eq!(
        port_zero
            .records
            .iter()
            .map(|record| (record.raw_link_status_word, record.link_up))
            .collect::<Vec<_>>(),
        vec![(1, true), (0x0100_0001, true), (0x0101_0000, false)]
    );
    assert_eq!(
        port_three
            .records
            .iter()
            .map(|record| (record.raw_link_status_word, record.link_up))
            .collect::<Vec<_>>(),
        vec![(1, true), (0x0100_0000, false), (0x0101_0001, true)]
    );
    assert_eq!(
        port_three
            .records
            .iter()
            .map(|record| record.link_speed_megabits_per_second)
            .collect::<Vec<_>>(),
        vec![1000, 0, 1000]
    );
    assert_eq!(
        port_three_at_one_hundred
            .records
            .iter()
            .map(|record| record.link_speed_megabits_per_second)
            .collect::<Vec<_>>(),
        vec![100, 0, 100]
    );
}

#[test]
fn clear_configuration_status_parses_authentic_publications_and_preserves_unknown_values() {
    let mode_one = decode_hex(
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
