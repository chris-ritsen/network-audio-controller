use super::super::conmon_detail::pending_interface_config_matches_running;
use super::*;

#[test]
fn ptp_clock_status_preserves_version_neutral_port_state() {
    let mut data = vec![0u8; 0x4A];
    stamp_conmon_response(&mut data, CONMON_OPCODE_PTP_CLOCK_STATUS);
    data[0x26] = 0x01;
    data[0x28..0x2C].copy_from_slice(&(-25_473i32).to_be_bytes());
    data[0x48..0x4A].copy_from_slice(&0x0006u16.to_be_bytes());
    let parsed = parse_ptp_clock_status(&data).unwrap();
    assert!(parsed.preferred_leader);
    assert_eq!(parsed.clock_frequency_offset_parts_per_billion, -25_473);
    assert_eq!(parsed.clock_port_state_code, 0x0006);
    assert_eq!(parsed.clock_role.as_deref(), Some("Leader"));
    assert_eq!(parsed.clock_port_records, None);

    data[0x26] = 0x00;
    data[0x48..0x4A].copy_from_slice(&0x0009u16.to_be_bytes());
    let parsed = parse_ptp_clock_status(&data).unwrap();
    assert!(!parsed.preferred_leader);
    assert_eq!(parsed.clock_frequency_offset_parts_per_billion, -25_473);
    assert_eq!(parsed.clock_port_state_code, 0x0009);
    assert_eq!(parsed.clock_role.as_deref(), Some("Follower"));

    data[0x48..0x4A].copy_from_slice(&0x0004u16.to_be_bytes());
    let parsed = parse_ptp_clock_status(&data).unwrap();
    assert_eq!(parsed.clock_port_state_code, 0x0004);
    assert_eq!(parsed.clock_role, None);
}

fn clock_source_zero_preferred_on_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff00a8005000000200000000010000417564696e6174650724002000000000000200060000017900060629020000000001000002000000000100000200000000010000000100340006000002b40000000186a0000000020000000000000000000000000000000000000002005800040003000000600010000000010102010000000002000600070001000201020200000000020003000300010003020202000000000200030003",
        )
}

fn clock_source_ded4_preferred_on_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff00a8004000000200000000010000417564696e6174650724002000000000000200060000017900060629020000000001000002000000000100000200000000010000000100340006000002b40000000186a0000000020000000000000000000000000000000000000002005800040003000000600010000000010102010000000002000600070001000201020200000000020003000300010003020202000000000200030003",
        )
}

#[test]
fn ptp_clock_status_preferred_on_is_independent_of_0021_clock_source_word() {
    let zero_source =
        parse_ptp_clock_status(&clock_source_zero_preferred_on_status_packet()).unwrap();
    let controller_source =
        parse_ptp_clock_status(&clock_source_ded4_preferred_on_status_packet()).unwrap();
    assert!(zero_source.preferred_leader);
    assert!(controller_source.preferred_leader);
    assert_eq!(zero_source.clock_source_code, 0);
    assert_eq!(controller_source.clock_source_code, 0);
    assert_eq!(
        zero_source.clock_port_state_code,
        controller_source.clock_port_state_code
    );
    assert_eq!(zero_source.clock_role, controller_source.clock_role);
    assert_eq!(
        zero_source.clock_port_records,
        controller_source.clock_port_records
    );
}

fn clock_source_bit0_ded4_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff00a8004800000200000000010000417564696e617465072400200000000000020006ded4007900060629020000000001000002000000000100000200000000010000000100340006000002b40000000186a0000000020000000000000000000000000000000000000002005800040003000000600010000000010102010000000002000600070001000201020200000000020003000300010003020202000000000200030003",
        )
}

#[test]
fn ptp_clock_status_parses_applied_clock_source_code() {
    let parsed = parse_ptp_clock_status(&clock_source_bit0_ded4_status_packet()).unwrap();
    assert!(!parsed.preferred_leader);
    assert_eq!(parsed.clock_source_code, 0xDED4);
}

fn clock_source_bit0_one_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff00a8003000000200000000010000417564696e61746507240020000000000002000600010079000c0c0b020000000001000002000000000100000200000000010000000100340006000002b40000000186a0000000020000000000000000000000000000000000000002005800040003000000600010000000010102010000000002000600070001000201020200000000020003000300010003020202000000000200030003",
        )
}

#[test]
fn ptp_clock_status_parses_raw_clock_source_one() {
    let parsed = parse_ptp_clock_status(&clock_source_bit0_one_status_packet()).unwrap();
    assert!(!parsed.preferred_leader);
    assert_eq!(parsed.clock_source_code, 1);
}

fn clock_source_bit0_two_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff00a8003800000200000000010000417564696e6174650724002000000000000200060002007900060629020000000001000002000000000100000200000000010000000100340006000002b40000000186a0000000020000000000000000000000000000000000000002005800040003000000600010000000010102010000000002000600070001000201020200000000020003000300010003020202000000000200030003",
        )
}

#[test]
fn ptp_clock_status_parses_raw_clock_source_two() {
    let parsed = parse_ptp_clock_status(&clock_source_bit0_two_status_packet()).unwrap();
    assert!(!parsed.preferred_leader);
    assert_eq!(parsed.clock_source_code, 2);
}

fn clock_subdomain_bit3_status_packet() -> Vec<u8> {
    decode_hexadecimal(
            "ffff00a8003200000200000000010000417564696e6174650724002000000000000200060000007bffffffdd020000000001000002000000000100000200000000010000000100340006000002b40001000186a0000000027494110701000000000000000000000000000002005800040003000000600010000000010102010000000002000600070001000201020200000000020003000300010003020202000000000200030003",
        )
}

#[test]
fn ptp_clock_status_parses_mask_bit3_subdomain_publication() {
    let parsed = parse_ptp_clock_status(&clock_subdomain_bit3_status_packet()).unwrap();
    assert_eq!(
        parsed.clock_subdomain,
        [
            0x74, 0x94, 0x11, 0x07, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00
        ]
    );
    assert!(!parsed.preferred_leader);
    assert_eq!(parsed.clock_source_code, 0);
}

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

#[test]
fn ptp_clock_status_parses_authentic_variable_port_table() {
    let mut data = vec![0u8; 168];
    stamp_conmon_response(&mut data, CONMON_OPCODE_PTP_CLOCK_STATUS);
    data[CONMON_PREFERRED_LEADER_OFFSET] = 0x01;
    data[CONMON_CLOCK_FREQUENCY_OFFSET_PARTS_PER_BILLION_OFFSET
        ..CONMON_CLOCK_FREQUENCY_OFFSET_PARTS_PER_BILLION_OFFSET + 4]
        .copy_from_slice(&(-394_757i32).to_be_bytes());
    data[CONMON_CLOCK_PORT_STATE_OFFSET..CONMON_CLOCK_PORT_STATE_OFFSET + 2]
        .copy_from_slice(&5u16.to_be_bytes());
    data[68..70].copy_from_slice(&1u16.to_be_bytes());
    data[108..112].copy_from_slice(&[0x00, 0x58, 0x00, 0x04]);
    data[112..120].copy_from_slice(&[0x00, 0x03, 0x00, 0x00, 0x00, 0x60, 0x00, 0x10]);
    data[120..168].copy_from_slice(&decode_hexadecimal(
            "000000010102010000000002000500070001000201020200000000020003000300010003020202000000000200030003",
        ));

    let parsed = parse_ptp_clock_status(&data).unwrap();
    assert_eq!(
        parsed.clock_port_records,
        Some(vec![
            PtpClockPortRecord {
                record_flags: 0,
                link_down: false,
                record_number: 1,
                ptp_version: 1,
                record_format_code: 2,
                transport_path_code: 1,
                transport_path: Some("multicast".to_owned()),
                reserved_byte: 0,
                network_interface_index: 2,
                state_code: 5,
                role: None,
                status_flags: 7,
            },
            PtpClockPortRecord {
                record_flags: 1,
                link_down: false,
                record_number: 2,
                ptp_version: 1,
                record_format_code: 2,
                transport_path_code: 2,
                transport_path: Some("unicast".to_owned()),
                reserved_byte: 0,
                network_interface_index: 2,
                state_code: 3,
                role: None,
                status_flags: 3,
            },
            PtpClockPortRecord {
                record_flags: 1,
                link_down: false,
                record_number: 3,
                ptp_version: 2,
                record_format_code: 2,
                transport_path_code: 2,
                transport_path: Some("unicast".to_owned()),
                reserved_byte: 0,
                network_interface_index: 2,
                state_code: 3,
                role: None,
                status_flags: 3,
            },
        ])
    );

    let mut invalid_stride = data.clone();
    invalid_stride[118..120].copy_from_slice(&15u16.to_be_bytes());
    let parsed_invalid_stride = parse_ptp_clock_status(&invalid_stride).unwrap();
    assert_eq!(parsed_invalid_stride.clock_port_records, None);

    let mut unknown_transport_path = data.clone();
    unknown_transport_path[126] = 0x7f;
    let unknown_transport_path = parse_ptp_clock_status(&unknown_transport_path).unwrap();
    let first_record = &unknown_transport_path.clock_port_records.unwrap()[0];
    assert_eq!(first_record.transport_path_code, 0x7f);
    assert_eq!(first_record.transport_path, None);

    let mut invalid_descriptor = data;
    invalid_descriptor[108..110].copy_from_slice(&0x0059u16.to_be_bytes());
    let parsed_without_port_table = parse_ptp_clock_status(&invalid_descriptor).unwrap();
    assert!(parsed_without_port_table.preferred_leader);
    assert_eq!(parsed_without_port_table.clock_source_code, 0);
    assert_eq!(parsed_without_port_table.clock_port_records, None);
}

#[test]
fn ptp_clock_status_parses_ports_from_live_avio_bluetooth_publication() {
    let data = decode_hexadecimal(
            "ffff00dce1190000001dc1fffe5279b6417564696e6174650738002000000000000300030000009fffff9baf001dc15279b60000001dc10812580000001dc1081258000000010034000900000294000000030d4000000002000000000000000000000000000000000000000100600c000000000c0098002000030000006810000000000101020100000000020009000700010002020202000000000200030003000100030202010000000002000300070003000700b80004001dc1fffe5279b6001dc1fffe081258001dc1fffe081258000100000001000000010000",
        );
    let parsed = parse_ptp_clock_status(&data).unwrap();
    assert!(!parsed.preferred_leader);
    assert_eq!(parsed.clock_source_code, 0);
    assert_eq!(parsed.clock_subdomain, [0u8; 16]);
    assert_eq!(parsed.clock_frequency_offset_parts_per_billion, -25_681);
    assert_eq!(parsed.clock_port_state_code, 0x0009);
    assert_eq!(parsed.clock_role.as_deref(), Some("Follower"));
    let ports = parsed.clock_port_records.unwrap();
    assert_eq!(ports.len(), 3);
    assert_eq!(ports[0].record_number, 1);
    assert_eq!(ports[0].ptp_version, 1);
    assert_eq!(ports[0].transport_path.as_deref(), Some("multicast"));
    assert_eq!(ports[0].role.as_deref(), Some("Follower"));
    assert_eq!(ports[1].ptp_version, 2);
    assert_eq!(ports[1].transport_path.as_deref(), Some("unicast"));
    assert_eq!(ports[2].ptp_version, 2);
    assert_eq!(ports[2].transport_path.as_deref(), Some("multicast"));

    let mut invalid_stride = data.clone();
    invalid_stride[126] = 15;
    assert_eq!(
        parse_ptp_clock_status(&invalid_stride)
            .unwrap()
            .clock_port_records,
        None
    );

    let mut invalid_target = data.clone();
    invalid_target[109] = 0x61;
    assert_eq!(
        parse_ptp_clock_status(&invalid_target)
            .unwrap()
            .clock_port_records,
        None
    );

    let mut truncated = data[..175].to_vec();
    stamp_conmon_response(&mut truncated, CONMON_OPCODE_PTP_CLOCK_STATUS);
    assert_eq!(
        parse_ptp_clock_status(&truncated)
            .unwrap()
            .clock_port_records,
        None
    );
}

#[test]
fn ptp_clock_status_parses_fresh_avio_aes3_capture() {
    let data = decode_hexadecimal(
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/clock_status/avio-aes3.hex"
        ))
        .trim(),
    );
    let parsed = parse_ptp_clock_status(&data).unwrap();
    let ports = parsed.clock_port_records.unwrap();
    assert_eq!(ports.len(), 3);
    assert_eq!(
        ports
            .iter()
            .map(|port| (
                port.record_number,
                port.ptp_version,
                port.transport_path_code,
                port.state_code
            ))
            .collect::<Vec<_>>(),
        vec![(1, 1, 1, 9), (2, 2, 2, 3), (3, 2, 1, 3)]
    );

    let mut unknown_variant = data;
    unknown_variant[24..26].copy_from_slice(&0x0739u16.to_be_bytes());
    assert_eq!(
        parse_ptp_clock_status(&unknown_variant)
            .unwrap()
            .clock_port_records,
        None
    );
}

#[test]
fn ptp_clock_status_identifies_the_selected_leader_across_subdomains() {
    let phase_a = decode_hexadecimal(
        "ffff00dc85080000001dc1fffe510295417564696e6174650738002000000000000300030000009f00007d3f001dc15102950000001dc150692e0000001dc150692e000000010034000900000294000300030d40000000024e412d434c4f434b2d410000000000000000000100600c000000000c0098002000030000006810000000000101020100000000020009000700010002020202000000000200030003000100030202010000000002000300070003000700b80004001dc1fffe510295001dc1fffe50692e001dc1fffe50692e000100000001000000010000",
    );
    let phase_b = decode_hexadecimal(
        "ffff00dc85450000001dc1fffe510295417564696e6174650738002000000000000300030000009f0000284d001dc15102950000001dc1507b8d0000001dc1507b8d000000010034000900000294000300030d40000000024e412d434c4f434b2d420000000000000000000100600c000000000c0098002000030000006810000000000101020100000000020009000700010002020202000000000200030003000100030202010000000002000300070003000700b80004001dc1fffe510295001dc1fffe507b8d001dc1fffe507b8d000100000001000000010000",
    );

    let selected_a = parse_ptp_clock_status(&phase_a).unwrap();
    let selected_b = parse_ptp_clock_status(&phase_b).unwrap();
    assert_eq!(
        selected_a.clock_identity,
        [0x00, 0x1D, 0xC1, 0x51, 0x02, 0x95]
    );
    assert_eq!(selected_b.clock_identity, selected_a.clock_identity);
    assert_eq!(
        selected_a.leader_clock_identity,
        Some([0x00, 0x1D, 0xC1, 0x50, 0x69, 0x2E])
    );
    assert_eq!(
        selected_b.leader_clock_identity,
        Some([0x00, 0x1D, 0xC1, 0x50, 0x7B, 0x8D])
    );
    assert_eq!(
        selected_a.first_leader_clock_identity_field,
        selected_a.second_leader_clock_identity_field
    );
    assert_eq!(
        selected_b.first_leader_clock_identity_field,
        selected_b.second_leader_clock_identity_field
    );

    let mut inconsistent = phase_b;
    inconsistent[CONMON_SECOND_LEADER_CLOCK_IDENTITY_OFFSET + 7] ^= 1;
    let inconsistent = parse_ptp_clock_status(&inconsistent).unwrap();
    assert_eq!(inconsistent.leader_clock_identity, None);
}

fn write_interface_record(
    data: &mut [u8],
    offset: usize,
    mode: u16,
    mac: [u8; 6],
    addresses: [[u8; 4]; 4],
) {
    let [ip_address, netmask, first_extra_address, second_extra_address] = addresses;
    data[offset..offset + 2].copy_from_slice(&mode.to_be_bytes());
    data[offset + 2..offset + 8].copy_from_slice(&mac);
    data[offset + 8..offset + 12].copy_from_slice(&ip_address);
    data[offset + 12..offset + 16].copy_from_slice(&netmask);
    data[offset + 16..offset + 20].copy_from_slice(&first_extra_address);
    data[offset + 20..offset + 24].copy_from_slice(&second_extra_address);
}

#[test]
fn interface_status_parses_pending_dhcp_and_reboot_state() {
    let mut data = vec![0u8; 0x4A];
    data[CONMON_INTERFACE_COUNT_OFFSET..CONMON_INTERFACE_COUNT_OFFSET + 2]
        .copy_from_slice(&1u16.to_be_bytes());
    data[CONMON_INTERFACE_LINK_SPEED_OFFSET..CONMON_INTERFACE_LINK_SPEED_OFFSET + 4]
        .copy_from_slice(&100u32.to_be_bytes());
    write_interface_record(
        &mut data,
        CONMON_INTERFACE_RECORDS_OFFSET,
        INTERFACE_MODE_STATIC,
        [0x00, 0x1D, 0xC1, 0x12, 0x34, 0x56],
        [
            [192, 168, 10, 20],
            [255, 255, 255, 0],
            [192, 168, 10, 1],
            [192, 168, 10, 2],
        ],
    );
    data[CONMON_INTERFACE_REBOOT_FLAG_OFFSET..CONMON_INTERFACE_REBOOT_FLAG_OFFSET + 2]
        .copy_from_slice(&INTERFACE_REBOOT_PENDING_DYNAMIC.to_be_bytes());
    stamp_conmon_response(&mut data, CONMON_OPCODE_INTERFACE_STATUS);

    let parsed = parse_interface_status(&data).unwrap();
    assert_eq!(parsed.link_speed_mbps, 100);
    assert!(parsed.reboot_required);
    assert_eq!(parsed.interfaces.len(), 1);
    assert_eq!(parsed.interfaces[0].mode, "static");
    assert_eq!(parsed.interfaces[0].mac_address, "00:1D:C1:12:34:56");
    assert_eq!(parsed.interfaces[0].ip_address, "192.168.10.20");
    assert_eq!(parsed.interfaces[0].netmask, "255.255.255.0");
    assert_eq!(
        parsed.interfaces[0].gateway.as_deref(),
        Some("192.168.10.2")
    );
    assert_eq!(
        parsed.interfaces[0].dns_server.as_deref(),
        Some("192.168.10.1")
    );

    let pending = parsed.pending_config.as_ref().unwrap();
    assert_eq!(pending.mode, "dynamic");
    assert_eq!(pending.ip_address, None);
    let json = serde_json::to_value(&parsed).unwrap();
    assert_eq!(json["reboot_required"], true);
    assert!(json["pending_config"].get("ip_address").is_none());
}

#[test]
fn interface_status_clears_applied_dynamic_target() {
    let mut data = vec![0u8; 0x4A];
    data[CONMON_INTERFACE_COUNT_OFFSET..CONMON_INTERFACE_COUNT_OFFSET + 2]
        .copy_from_slice(&1u16.to_be_bytes());
    data[CONMON_INTERFACE_LINK_SPEED_OFFSET..CONMON_INTERFACE_LINK_SPEED_OFFSET + 4]
        .copy_from_slice(&100u32.to_be_bytes());
    write_interface_record(
        &mut data,
        CONMON_INTERFACE_RECORDS_OFFSET,
        INTERFACE_MODE_DYNAMIC,
        [0x00, 0x1D, 0xC1, 0x50, 0x69, 0x2E],
        [
            [192, 168, 1, 139],
            [255, 255, 255, 0],
            [192, 168, 1, 1],
            [192, 168, 1, 1],
        ],
    );
    data[CONMON_INTERFACE_REBOOT_FLAG_OFFSET..CONMON_INTERFACE_REBOOT_FLAG_OFFSET + 2]
        .copy_from_slice(&INTERFACE_REBOOT_PENDING_DYNAMIC.to_be_bytes());
    stamp_conmon_response(&mut data, CONMON_OPCODE_INTERFACE_STATUS);

    let parsed = parse_interface_status(&data).unwrap();
    assert!(!parsed.reboot_required);
    assert_eq!(parsed.pending_config, None);
}

#[test]
fn interface_status_parses_retained_static_reboot_publication() {
    let data = decode_hexadecimal(
            "ffff0061001600000200000000010000417564696e617465072400110000000000010001000003e80003020000000001c0a80124ffffff0008080808c0a80101001800300000000000000000000000000000000000000000000000000048000000",
        );
    let parsed = parse_interface_status(&data).unwrap();
    assert_eq!(parsed.link_speed_mbps, 1000);
    assert!(!parsed.reboot_required);
    assert_eq!(parsed.pending_config, None);
    assert_eq!(parsed.interfaces.len(), 1);
    assert_eq!(parsed.interfaces[0].mode, "static");
    assert_eq!(parsed.interfaces[0].mac_address, "02:00:00:00:00:01");
    assert_eq!(parsed.interfaces[0].ip_address, "192.168.1.36");
    assert_eq!(parsed.interfaces[0].netmask, "255.255.255.0");
    assert_eq!(parsed.interfaces[0].dns_server.as_deref(), Some("8.8.8.8"));
    assert_eq!(parsed.interfaces[0].gateway.as_deref(), Some("192.168.1.1"));
}

#[test]
fn interface_status_keeps_static_target_when_dns_differs() {
    let running_interface = InterfaceStatusEntry {
        mode: "static".to_owned(),
        mac_address: "00:1D:C1:50:69:2E".to_owned(),
        ip_address: "192.168.1.42".to_owned(),
        netmask: "255.255.255.0".to_owned(),
        gateway: Some("192.168.1.1".to_owned()),
        dns_server: Some("192.168.1.1".to_owned()),
    };
    let pending_config = PendingInterfaceConfig {
        mode: "static".to_owned(),
        ip_address: Some("192.168.1.42".to_owned()),
        netmask: Some("255.255.255.0".to_owned()),
        gateway: Some("192.168.1.1".to_owned()),
        dns_server: Some("8.8.8.8".to_owned()),
    };

    assert!(!pending_interface_config_matches_running(
        &pending_config,
        &running_interface
    ));
}

#[test]
fn interface_status_clears_fully_applied_static_target() {
    let running_interface = InterfaceStatusEntry {
        mode: "static".to_owned(),
        mac_address: "00:1D:C1:50:69:2E".to_owned(),
        ip_address: "192.168.1.42".to_owned(),
        netmask: "255.255.255.0".to_owned(),
        gateway: Some("192.168.1.1".to_owned()),
        dns_server: Some("8.8.8.8".to_owned()),
    };
    let pending_config = PendingInterfaceConfig {
        mode: "static".to_owned(),
        ip_address: Some("192.168.1.42".to_owned()),
        netmask: Some("255.255.255.0".to_owned()),
        gateway: Some("192.168.1.1".to_owned()),
        dns_server: Some("8.8.8.8".to_owned()),
    };

    assert!(pending_interface_config_matches_running(
        &pending_config,
        &running_interface
    ));
}

#[test]
fn interface_status_parses_pending_static_configuration() {
    let mut data = vec![0u8; 0x5C];
    data[CONMON_INTERFACE_COUNT_OFFSET..CONMON_INTERFACE_COUNT_OFFSET + 2]
        .copy_from_slice(&1u16.to_be_bytes());
    data[CONMON_INTERFACE_LINK_SPEED_OFFSET..CONMON_INTERFACE_LINK_SPEED_OFFSET + 4]
        .copy_from_slice(&1_000u32.to_be_bytes());
    write_interface_record(
        &mut data,
        CONMON_INTERFACE_RECORDS_OFFSET,
        INTERFACE_MODE_STATIC,
        [0x00, 0x1D, 0xC1, 0x65, 0x43, 0x21],
        [
            [10, 0, 0, 20],
            [255, 255, 255, 0],
            [10, 0, 0, 53],
            [10, 0, 0, 1],
        ],
    );
    data[CONMON_INTERFACE_REBOOT_FLAG_OFFSET..CONMON_INTERFACE_REBOOT_FLAG_OFFSET + 2]
        .copy_from_slice(&INTERFACE_REBOOT_PENDING_STATIC.to_be_bytes());
    data[CONMON_INTERFACE_PENDING_STATIC_OFFSET..CONMON_INTERFACE_PENDING_STATIC_OFFSET + 4]
        .copy_from_slice(&[172, 16, 1, 50]);
    data[CONMON_INTERFACE_PENDING_STATIC_OFFSET + 4..CONMON_INTERFACE_PENDING_STATIC_OFFSET + 8]
        .copy_from_slice(&[255, 255, 0, 0]);
    data[CONMON_INTERFACE_PENDING_STATIC_OFFSET + 8..CONMON_INTERFACE_PENDING_STATIC_OFFSET + 12]
        .copy_from_slice(&[172, 16, 1, 53]);
    data[CONMON_INTERFACE_PENDING_STATIC_OFFSET + 12..CONMON_INTERFACE_PENDING_STATIC_OFFSET + 16]
        .copy_from_slice(&[172, 16, 1, 1]);
    stamp_conmon_response(&mut data, CONMON_OPCODE_INTERFACE_STATUS);

    let parsed = parse_interface_status(&data).unwrap();
    assert_eq!(parsed.link_speed_mbps, 1_000);
    assert!(parsed.reboot_required);
    assert_eq!(parsed.interfaces[0].mode, "static");
    assert_eq!(
        parsed.interfaces[0].dns_server.as_deref(),
        Some("10.0.0.53")
    );
    assert_eq!(parsed.interfaces[0].gateway.as_deref(), Some("10.0.0.1"));

    let pending = parsed.pending_config.unwrap();
    assert_eq!(pending.mode, "static");
    assert_eq!(pending.ip_address.as_deref(), Some("172.16.1.50"));
    assert_eq!(pending.netmask.as_deref(), Some("255.255.0.0"));
    assert_eq!(pending.dns_server.as_deref(), Some("172.16.1.53"));
    assert_eq!(pending.gateway.as_deref(), Some("172.16.1.1"));
}

#[test]
fn interface_status_parses_multiple_interfaces() {
    let mut data = vec![0u8; 0x5C];
    data[CONMON_INTERFACE_COUNT_OFFSET..CONMON_INTERFACE_COUNT_OFFSET + 2]
        .copy_from_slice(&2u16.to_be_bytes());
    data[CONMON_INTERFACE_LINK_SPEED_OFFSET..CONMON_INTERFACE_LINK_SPEED_OFFSET + 4]
        .copy_from_slice(&10_000u32.to_be_bytes());
    write_interface_record(
        &mut data,
        CONMON_INTERFACE_RECORDS_OFFSET,
        INTERFACE_MODE_DYNAMIC,
        [0x00, 0x1D, 0xC1, 0x00, 0x00, 0x01],
        [
            [192, 168, 1, 10],
            [255, 255, 255, 0],
            [192, 168, 1, 1],
            [1, 1, 1, 1],
        ],
    );
    write_interface_record(
        &mut data,
        CONMON_INTERFACE_RECORDS_OFFSET + CONMON_INTERFACE_CONFIGURED_RECORD_STRIDE,
        INTERFACE_MODE_STATIC,
        [0x00, 0x1D, 0xC1, 0xAA, 0xBB, 0xCC],
        [
            [192, 168, 2, 20],
            [255, 255, 0, 0],
            [8, 8, 8, 8],
            [192, 168, 2, 1],
        ],
    );
    stamp_conmon_response(&mut data, CONMON_OPCODE_INTERFACE_STATUS);

    let parsed = parse_interface_status(&data).unwrap();
    assert_eq!(parsed.link_speed_mbps, 10_000);
    assert_eq!(parsed.interfaces.len(), 2);
    assert_eq!(parsed.interfaces[0].mode, "dynamic");
    assert_eq!(parsed.interfaces[1].mode, "static");
    assert_eq!(parsed.interfaces[1].mac_address, "00:1D:C1:AA:BB:CC");
    assert_eq!(parsed.interfaces[1].ip_address, "192.168.2.20");
    assert_eq!(parsed.interfaces[1].dns_server.as_deref(), Some("8.8.8.8"));
    assert_eq!(parsed.interfaces[1].gateway.as_deref(), Some("192.168.2.1"));
    assert!(!parsed.reboot_required);
    assert_eq!(parsed.pending_config, None);

    let json = serde_json::to_value(&parsed).unwrap();
    assert!(json.get("interfaces").is_some());
    assert_eq!(json["pending_config"], serde_json::Value::Null);
}
