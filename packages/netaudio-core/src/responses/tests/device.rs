use super::flows::flow_query_response;
use super::*;

#[test]
fn metering_frame_parses_embedded_counts_and_level_order() {
    let data = metering_frame(&[0xFE, 0x7D, 0xA0], &[0x88, 0x00]);
    assert_eq!(
        parse_metering_frame(&data),
        Some(MeteringFrame {
            sequence: 0x1F81,
            source_eui64: "001dc119245c0000".to_owned(),
            tx_count: 3,
            rx_count: 2,
            tx_levels: vec![0xFE, 0x7D, 0xA0],
            rx_levels: vec![0x88, 0x00],
        })
    );
}

#[test]
fn metering_v3_frame_parses_sixteen_bit_counts_and_level_order() {
    let mut tx_levels = vec![0xFE; 128];
    let mut rx_levels = vec![0xFE; 128];
    tx_levels[16] = 0x7E;
    tx_levels[17] = 0x9F;
    rx_levels[2] = 0x70;
    rx_levels[3] = 0x71;
    let data = metering_frame_v3(&tx_levels, &rx_levels);

    assert_eq!(data.len(), 286);
    assert_eq!(
        parse_metering_frame(&data),
        Some(MeteringFrame {
            sequence: 0xDDFB,
            source_eui64: "001dc10812580000".to_owned(),
            tx_count: 128,
            rx_count: 128,
            tx_levels,
            rx_levels,
        })
    );
}

#[test]
fn metering_frame_rejects_invalid_envelope_and_count_mismatch() {
    let original = metering_frame(&[0xFE, 0x7D], &[0x88]);

    for (offset, value) in [(0, 0x12), (6, 0x01), (16, b'X'), (24, 0x07), (27, 0xFF)] {
        let mut data = original.clone();
        data[offset] = value;
        assert_eq!(parse_metering_frame(&data), None);
    }

    let mut wrong_declared_length = original.clone();
    wrong_declared_length[2..4].copy_from_slice(&(original.len() as u16 - 1).to_be_bytes());
    assert_eq!(parse_metering_frame(&wrong_declared_length), None);

    let mut wrong_count = original.clone();
    wrong_count[METERING_V2_TX_COUNT_OFFSET] += 1;
    assert_eq!(parse_metering_frame(&wrong_count), None);

    let original_v3 = metering_frame_v3(&[0xFE, 0x7D], &[0x88]);
    let mut wrong_reserved = original_v3.clone();
    wrong_reserved[METERING_V3_RESERVED_OFFSET] = 1;
    assert_eq!(parse_metering_frame(&wrong_reserved), None);

    let mut wrong_v3_count = original_v3;
    wrong_v3_count[METERING_V3_TX_COUNT_OFFSET + 1] += 1;
    assert_eq!(parse_metering_frame(&wrong_v3_count), None);
}

#[test]
fn device_name_strips_header_and_terminator() {
    let mut response = vec![0x27, 0xFF, 0x00, 0x16, 0x9e, 0x7f, 0x10, 0x02, 0x00, 0x01];
    response.extend_from_slice(b"avio-aes3-1\x00");
    assert_eq!(parse_device_name(&response).as_deref(), Some("avio-aes3-1"));
}

fn device_settings_response(values: &[(u16, u32)]) -> Vec<u8> {
    let mut body = vec![0x00, values.len() as u8];
    let first_value_offset = RESPONSE_HEADER_SIZE + 2 + values.len() * 4;
    for (index, (info_code, _)) in values.iter().enumerate() {
        body.extend_from_slice(&info_code.to_be_bytes());
        body.extend_from_slice(&((first_value_offset + index * 4) as u16).to_be_bytes());
    }
    for (_, value) in values.iter().copied() {
        body.extend_from_slice(&value.to_be_bytes());
    }
    let mut response = vec![0u8; 10];
    response.extend_from_slice(&body);
    stamp_arc_response(
        &mut response,
        PROTOCOL_ID,
        OPCODE_DEVICE_SETTINGS,
        RESULT_CODE_SUCCESS,
    );
    response
}

#[test]
fn device_settings_decodes_distinct_latency_fields_as_nanoseconds() {
    let response = device_settings_response(&[
        (DEVICE_SETTINGS_INFO_SAMPLE_RATE, 48_000u32),
        (DEVICE_SETTINGS_INFO_DEFAULT_LATENCY_NS, 1_000_000u32),
        (DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS, 150_000u32),
        (DEVICE_SETTINGS_INFO_ACTIVE_LATENCY_NS, 1_000_000u32),
        (DEVICE_SETTINGS_INFO_MAX_LATENCY_NS, 21_333_334u32),
        (DEVICE_SETTINGS_INFO_MIN_LATENCY_NS, 150_000u32),
    ]);

    let settings = parse_device_settings(&response).unwrap();
    assert_eq!(settings.sample_rate, Some(48_000));
    assert_eq!(settings.default_latency_ns, Some(1_000_000));
    assert_eq!(settings.configured_latency_ns, Some(150_000));
    assert_eq!(settings.active_latency_ns, Some(1_000_000));
    assert_eq!(settings.latency_ns, Some(1_000_000));
    assert_eq!(settings.max_latency_ns, Some(21_333_334));
    assert_eq!(settings.min_latency_ns, Some(150_000));
}

#[test]
fn device_settings_uses_configured_latency_when_active_is_absent() {
    let response =
        device_settings_response(&[(DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS, 250_000)]);
    let settings = parse_device_settings(&response).unwrap();
    assert_eq!(settings.configured_latency_ns, Some(250_000));
    assert_eq!(settings.active_latency_ns, None);
    assert_eq!(settings.latency_ns, Some(250_000));
}

fn retained_receive_latency_device_settings_packet() -> Vec<u8> {
    decode_hex(
            "2729008c134011000001171702010001820400688205006c021000100211000400008218000082198301007083020074830600780310000403110002030300048021007c00f000000000806000220001000000630000006400000065000002220212003000008321000f42400003d09000000000000000000003d09000000000000000000000000000000000",
        )
}

#[test]
fn device_settings_parses_retained_configured_latency_after_reboot() {
    let settings =
        parse_device_settings(&retained_receive_latency_device_settings_packet()).unwrap();
    assert_eq!(settings.configured_latency_ns, Some(250_000));
    assert_eq!(settings.active_latency_ns, Some(0));
    assert_eq!(settings.min_latency_ns, Some(250_000));
    assert_eq!(settings.default_latency_ns, Some(1_000_000));
    assert_eq!(settings.latency_ns, Some(0));
}

fn retained_one_nanosecond_latency_device_settings_packet() -> Vec<u8> {
    decode_hex(
            "2729008c134011000001171702010001820400688205006c021000100211000400008218000082198301007083020074830600780310000403110002030300048021007c00f000000000806000220001000000630000006400000065000002220212003000008321000f42400000000100000000000000000003d09000000000000000000000000000000000",
        )
}

#[test]
fn device_settings_parses_avio_aes67_multicast_prefix() {
    let before = decode_hex(
            "28090094180011000001171702010001820400688205006c021000100211001000008218000082198301007083020074830600780310001003110010030300028021007c000000f08060008c002200010063000100000064000000650222138c0212003083210090000f4240000f4240000f42400135f1b4000f424000000000000000000000000000000000ef450000001e8480",
        );
    let after = decode_hex(
            "28090094180211000001171702010001820400688205006c021000100211001000008218000082198301007083020074830600780310001003110010030300028021007c000000f08060008c002200010063000100000064000000650222138c0212003083210090000f4240000f4240000f42400135f1b4000f424000000000000000000000000000000000efee0000001e8480",
        );
    let before_settings = parse_device_settings(&before).unwrap();
    let after_settings = parse_device_settings(&after).unwrap();
    assert_eq!(
        before_settings.aes67_multicast_prefix.as_deref(),
        Some("239.69.0.0")
    );
    assert_eq!(
        after_settings.aes67_multicast_prefix.as_deref(),
        Some("239.238.0.0")
    );
    assert_eq!(parse_aes67_configured(&before), Some(Some(false)));
    assert_eq!(parse_aes67_configured(&after), Some(Some(false)));
}

#[test]
fn device_settings_parses_retained_one_nanosecond_configured_latency() {
    let settings =
        parse_device_settings(&retained_one_nanosecond_latency_device_settings_packet()).unwrap();
    assert_eq!(settings.configured_latency_ns, Some(1));
    assert_eq!(settings.active_latency_ns, Some(0));
    assert_eq!(settings.min_latency_ns, Some(250_000));
    assert_eq!(settings.default_latency_ns, Some(1_000_000));
}

fn captured_selective_device_settings_packet_87509() -> Vec<u8> {
    vec![
        0x28, 0x01, 0x00, 0x94, 0x14, 0x21, 0x11, 0x00, 0x00, 0x01, 0x17, 0x17, 0x02, 0x01, 0x00,
        0x01, 0x82, 0x04, 0x00, 0x68, 0x82, 0x05, 0x00, 0x6C, 0x02, 0x10, 0x00, 0x10, 0x02, 0x11,
        0x00, 0x10, 0x00, 0x00, 0x82, 0x18, 0x00, 0x00, 0x82, 0x19, 0x83, 0x01, 0x00, 0x70, 0x83,
        0x02, 0x00, 0x74, 0x83, 0x06, 0x00, 0x78, 0x03, 0x10, 0x00, 0x10, 0x03, 0x11, 0x00, 0x02,
        0x03, 0x03, 0x00, 0x04, 0x80, 0x21, 0x00, 0x7C, 0x00, 0xF0, 0x00, 0x00, 0x80, 0x60, 0x00,
        0x8C, 0x00, 0x22, 0x00, 0x01, 0x00, 0x63, 0x00, 0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00,
        0x00, 0x65, 0x02, 0x22, 0x13, 0x8C, 0x02, 0x12, 0x00, 0x30, 0x83, 0x21, 0x00, 0x90, 0x00,
        0x0F, 0x42, 0x40, 0x00, 0x0F, 0x42, 0x40, 0x00, 0x0F, 0x42, 0x40, 0x14, 0x58, 0x55, 0x56,
        0x00, 0x03, 0xD0, 0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0x45, 0x00, 0x00, 0x00, 0x1E, 0x84, 0x80,
    ]
}

#[test]
fn device_settings_accepts_captured_2801_response() {
    let settings =
        parse_device_settings(&captured_selective_device_settings_packet_87509()).unwrap();
    assert_eq!(settings.default_latency_ns, Some(1_000_000));
    assert_eq!(settings.configured_latency_ns, Some(1_000_000));
    assert_eq!(settings.active_latency_ns, Some(1_000_000));
    assert_eq!(settings.latency_ns, Some(1_000_000));
    assert_eq!(settings.min_latency_ns, Some(250_000));
    assert_eq!(settings.max_latency_ns, Some(341_333_334));
}

pub(super) fn captured_selective_device_settings_packet_9084571() -> Vec<u8> {
    vec![
        0x28, 0x09, 0x00, 0x94, 0x00, 0x10, 0x11, 0x00, 0x00, 0x01, 0x17, 0x17, 0x02, 0x01, 0x00,
        0x01, 0x82, 0x04, 0x00, 0x68, 0x82, 0x05, 0x00, 0x6C, 0x02, 0x10, 0x00, 0x10, 0x02, 0x11,
        0x00, 0x10, 0x00, 0x00, 0x82, 0x18, 0x00, 0x00, 0x82, 0x19, 0x83, 0x01, 0x00, 0x70, 0x83,
        0x02, 0x00, 0x74, 0x83, 0x06, 0x00, 0x78, 0x03, 0x10, 0x00, 0x10, 0x03, 0x11, 0x00, 0x10,
        0x03, 0x03, 0x00, 0x02, 0x80, 0x21, 0x00, 0x7C, 0x00, 0x00, 0x00, 0xF0, 0x80, 0x60, 0x00,
        0x8C, 0x00, 0x22, 0x00, 0x01, 0x00, 0x63, 0x00, 0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00,
        0x00, 0x65, 0x02, 0x22, 0x13, 0x8C, 0x02, 0x12, 0x00, 0x30, 0x83, 0x21, 0x00, 0x90, 0x00,
        0x0F, 0x42, 0x40, 0x00, 0x0F, 0x42, 0x40, 0x00, 0x0F, 0x42, 0x40, 0x00, 0xA7, 0x87, 0x5F,
        0x00, 0x0F, 0x42, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0x45, 0x00, 0x00, 0x00, 0x1E, 0x84, 0x80,
    ]
}

#[test]
fn device_settings_accepts_captured_repeated_zero_placeholders() {
    let settings =
        parse_device_settings(&captured_selective_device_settings_packet_9084571()).unwrap();
    assert_eq!(settings.default_latency_ns, Some(1_000_000));
    assert_eq!(settings.configured_latency_ns, Some(1_000_000));
    assert_eq!(settings.active_latency_ns, Some(1_000_000));
    assert_eq!(settings.latency_ns, Some(1_000_000));
    assert_eq!(settings.min_latency_ns, Some(1_000_000));
    assert_eq!(settings.max_latency_ns, Some(10_979_167));
    assert!(settings
        .referenced_values
        .iter()
        .all(|value| value.info_code != 0));
    assert_eq!(
        settings.unavailable_property_ids,
        vec![0x8218, 0x8219, 0x00f0, 0x0064, 0x0065]
    );
}

#[test]
fn device_settings_preserves_inline_values_and_unavailable_property_ids() {
    let response = decode_hex(
        "2729008c239511000001171702010001820400688205006c021012340211456700008218000082198301007083020074830600780310789a03110002030300048021007c00f00000000080600022b000000000630000006400000065000002220212003000008321000f4240000249f0000249f001458556000249f000000000000000000000000000000000",
    );

    let settings = parse_device_settings(&response).unwrap();

    assert!(settings.inline_values.contains(&DeviceSettingsInlineValue {
        info_code: 0x0210,
        value: 0x1234,
    }));
    assert!(settings.inline_values.contains(&DeviceSettingsInlineValue {
        info_code: 0x0211,
        value: 0x4567,
    }));
    assert!(settings.inline_values.contains(&DeviceSettingsInlineValue {
        info_code: 0x0310,
        value: 0x789a,
    }));
    assert_eq!(
        settings.unavailable_property_ids,
        vec![0x8218, 0x8219, 0x8060, 0x0063, 0x0064, 0x0065, 0x0222, 0x8321]
    );
}

#[test]
fn device_settings_preserves_full_variable_width_referenced_values() {
    let control = decode_hex(
            "27ff00a80000110000011b18802000788021007c0022b000002300000024000100f00000020100018204008c82050090020a0000020b0000021000000211000002120030021300000214000083010094830600988302009c03100010031100020303000483f000a0060100000000000000000000000000000000000000000000000000000000000000000000000f4240000f4240000000000003d090000000000000000500000000",
        );
    let treatment = decode_hex(
            "27ff00a80000110000011b18802000788021007c0022b000002300000024000100f00000020100018204008c82050090020a0000020b0000021000000211000002120030021300000214000083010094830600988302009c03100010031100020303000483f000a0060100000000000000000000000000000000000000000001000000000000000000000000000f4240000f4240000000000003d090000000000000000500000000",
        );

    let control_settings = parse_device_settings(&control).unwrap();
    let treatment_settings = parse_device_settings(&treatment).unwrap();
    let control_property = control_settings
        .referenced_values
        .iter()
        .find(|value| value.info_code == 0x8021)
        .unwrap();
    let treatment_property = treatment_settings
        .referenced_values
        .iter()
        .find(|value| value.info_code == 0x8021)
        .unwrap();

    assert_eq!(control_property.pointer, 0x007c);
    assert_eq!(
        control_property.value_hexadecimal,
        "00000000000000000000000000000000"
    );
    assert_eq!(treatment_property.pointer, 0x007c);
    assert_eq!(
        treatment_property.value_hexadecimal,
        "00000001000000000000000000000000"
    );
    assert_eq!(
        control_settings
            .referenced_values
            .iter()
            .find(|value| value.info_code == 0x83f0)
            .unwrap()
            .value_hexadecimal,
        "0000000500000000"
    );
}

#[test]
fn device_settings_rejects_duplicate_non_placeholder_info_codes() {
    let mut response = captured_selective_device_settings_packet_9084571();
    response[32..34].copy_from_slice(&DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS.to_be_bytes());
    assert_eq!(parse_device_settings(&response), None);
}

fn property_directory_response(properties: &[(u16, u16)]) -> Vec<u8> {
    let mut response = vec![0u8; RESPONSE_HEADER_SIZE];
    response.extend_from_slice(&(properties.len() as u16).to_be_bytes());
    for (property_id, flags) in properties {
        response.extend_from_slice(&property_id.to_be_bytes());
        response.extend_from_slice(&flags.to_be_bytes());
    }
    stamp_arc_response(
        &mut response,
        PROTOCOL_DANTE_FLOW,
        OPCODE_PROPERTY_DIRECTORY,
        RESULT_CODE_SUCCESS,
    );
    response
}

#[test]
fn property_directory_preserves_raw_records_and_derives_aes67_presence() {
    let response = property_directory_response(&[(0x8020, 0x0001), (0x0063, 0x0003)]);
    let directory = parse_property_directory(&response).unwrap();
    assert_eq!(
        directory.properties,
        vec![
            PropertyDirectoryEntry {
                property_id: 0x8020,
                flags: 0x0001,
            },
            PropertyDirectoryEntry {
                property_id: 0x0063,
                flags: 0x0003,
            },
        ]
    );
    assert!(directory.aes67_supported);

    let unsupported = property_directory_response(&[(0x8020, 0x0001)]);
    assert!(
        !parse_property_directory(&unsupported)
            .unwrap()
            .aes67_supported
    );
}

#[test]
fn property_directory_rejects_duplicate_or_misaligned_records() {
    let duplicate = property_directory_response(&[(0x0063, 0x0001), (0x0063, 0x0003)]);
    assert_eq!(parse_property_directory(&duplicate), None);

    let mut trailing = property_directory_response(&[(0x0063, 0x0001)]);
    trailing.push(0);
    let length = trailing.len() as u16;
    trailing[2..4].copy_from_slice(&length.to_be_bytes());
    assert_eq!(parse_property_directory(&trailing), None);
}

#[test]
fn result_code_reads_header_field() {
    assert_eq!(
        parse_result_code(&flow_query_response()),
        Some(RESULT_CODE_SUCCESS)
    );
    assert_eq!(
        parse_result_code(&decode_hex("2809000a284a34000030")),
        Some(0x0030)
    );
    assert_eq!(
        parse_result_code(&decode_hex("2809000a284c34010030")),
        Some(0x0030)
    );
    assert_eq!(
        parse_result_code(&decode_hex("2809000a022526000030")),
        Some(0x0030)
    );
    assert_eq!(parse_result_code(&[0u8; 4]), None);
}

#[test]
fn cmc_registration_response_parser_validates_envelope_and_fields() {
    let response = decode_hex("120000200000100100010000020000000001000000010000c0a8013d21fc0000");
    assert_eq!(
        parse_cmc_registration_response(&response),
        Some(CmcRegistrationResponse {
            sequence: 0,
            status: 1,
        })
    );

    let mut wrong_command = response.clone();
    wrong_command[6..8].copy_from_slice(&0x1002u16.to_be_bytes());
    assert_eq!(parse_cmc_registration_response(&wrong_command), None);

    let mut wrong_length = response;
    wrong_length[2..4].copy_from_slice(&31u16.to_be_bytes());
    assert_eq!(parse_cmc_registration_response(&wrong_length), None);
}

pub(super) fn aes67_settings_response(records: &[(u16, u16)]) -> Vec<u8> {
    let mut response = vec![0u8; RESPONSE_HEADER_SIZE];
    response.extend_from_slice(&[0, records.len() as u8]);
    for (info_code, inline_value) in records {
        response.extend_from_slice(&info_code.to_be_bytes());
        response.extend_from_slice(&inline_value.to_be_bytes());
    }
    stamp_arc_response(
        &mut response,
        PROTOCOL_ARC_2809,
        OPCODE_DEVICE_SETTINGS,
        RESULT_CODE_SUCCESS,
    );
    response
}

#[test]
fn aes67_configured_uses_property_identity_not_record_offset() {
    let enabled = aes67_settings_response(&[
        (0x0211, 0x0004),
        (DEVICE_SETTINGS_INFO_AES67_CONFIGURED, 0x0003),
        (0x0310, 0x0004),
    ]);
    assert_eq!(parse_aes67_configured(&enabled), Some(Some(true)));

    let disabled = aes67_settings_response(&[
        (DEVICE_SETTINGS_INFO_AES67_CONFIGURED, 0x0001),
        (0x0211, 0x0004),
    ]);
    assert_eq!(parse_aes67_configured(&disabled), Some(Some(false)));

    let unsupported = aes67_settings_response(&[(0x0000, 0x0063), (0x0211, 0x0004)]);
    assert_eq!(parse_aes67_configured(&unsupported), Some(None));

    let duplicate = aes67_settings_response(&[
        (DEVICE_SETTINGS_INFO_AES67_CONFIGURED, 0x0001),
        (DEVICE_SETTINGS_INFO_AES67_CONFIGURED, 0x0003),
    ]);
    assert_eq!(parse_aes67_configured(&duplicate), None);
}

#[test]
fn aes67_configured_accepts_captured_2801_device_settings_response() {
    assert_eq!(
        parse_aes67_configured(&captured_selective_device_settings_packet_87509()),
        Some(Some(false))
    );
}

#[test]
fn make_model_preserves_unmapped_preceding_field_and_four_part_version() {
    let mut data = vec![0u8; 0x170];
    stamp_conmon_response(&mut data, CONMON_OPCODE_MAKE_MODEL_RESPONSE);
    data[CONMON_UNMAPPED_FIELD_BEFORE_MANUFACTURER_OFFSET
        ..CONMON_UNMAPPED_FIELD_BEFORE_MANUFACTURER_OFFSET + 2]
        .copy_from_slice(&1u16.to_be_bytes());
    data[CONMON_MANUFACTURER_OFFSET..CONMON_MANUFACTURER_OFFSET + 14]
        .copy_from_slice(b"Ferrofish GmbH");
    data[CONMON_PRODUCT_NAME_OFFSET..CONMON_PRODUCT_NAME_OFFSET + 25]
        .copy_from_slice(b"A32 Dante AD/DA Converter");
    data[CONMON_PRODUCT_VERSION_OFFSET..CONMON_PRODUCT_VERSION_END].copy_from_slice(&[1, 2, 0, 3]);

    let parsed = parse_make_model(&data).unwrap();
    assert_eq!(parsed.manufacturer, "Ferrofish GmbH");
    assert_eq!(parsed.manufacturer_field_hexadecimal.len(), 256);
    assert_eq!(parsed.unmapped_field_at_byte_offset_74, 1);
    assert_eq!(parsed.product_name, "A32 Dante AD/DA Converter");
    assert_eq!(parsed.product_version, "1.2.0.3");
    assert_eq!(parsed.product_version_components, [1, 2, 0, 3]);
}
