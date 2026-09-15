use super::super::flows::receiver_channel_numbers;
use super::*;

pub(super) fn flow_query_response() -> Vec<u8> {
    let mut response = vec![0u8; RESPONSE_HEADER_SIZE];
    response.extend_from_slice(&[0x10, 0x02]);
    response.extend_from_slice(&44u16.to_be_bytes());
    response.extend_from_slice(&112u16.to_be_bytes());
    response.extend(std::iter::repeat_n(0, 28));

    let mut unicast_record = vec![0u8; 68];
    unicast_record[0..2].copy_from_slice(&1u16.to_be_bytes());
    unicast_record[2..4].copy_from_slice(&FLOW_TYPE_UNICAST.to_be_bytes());
    unicast_record[4..8].copy_from_slice(&48_000u32.to_be_bytes());
    unicast_record[8..12].copy_from_slice(&24u32.to_be_bytes());
    unicast_record[12..14].copy_from_slice(&1u16.to_be_bytes());
    unicast_record[14..16].copy_from_slice(&1u16.to_be_bytes());
    response.extend_from_slice(&unicast_record);

    let mut multicast_record = vec![0u8; 64];
    multicast_record[0..2].copy_from_slice(&17u16.to_be_bytes());
    multicast_record[2..4].copy_from_slice(&FLOW_TYPE_MULTICAST.to_be_bytes());
    multicast_record[4..8].copy_from_slice(&48_000u32.to_be_bytes());
    multicast_record[8..12].copy_from_slice(&24u32.to_be_bytes());
    multicast_record[12..14].copy_from_slice(&2u16.to_be_bytes());
    multicast_record[14..16].copy_from_slice(&2u16.to_be_bytes());
    multicast_record[20..22].copy_from_slice(&1u16.to_be_bytes());
    multicast_record[22..24].copy_from_slice(&2u16.to_be_bytes());
    response.extend_from_slice(&multicast_record);
    stamp_arc_response(
        &mut response,
        PROTOCOL_DANTE_FLOW,
        OPCODE_QUERY_TX_FLOWS,
        RESULT_CODE_SUCCESS,
    );
    response
}

pub(super) fn captured_receiver_flow_response() -> Vec<u8> {
    decode_hexadecimal(
            "2729017c033a320000011004002c008000d40128000000000000000000000000000000000000000000000000000100010000bb80000000180001000200080068\
             0046005600700000001000000000000000000000000000000020000000000000000000000000000008023813c0a8016c0009000108000000000f424000000000\
             000200010000bb800000001800010002000800bc009a00aa00c40000000100000000000000000000000000000002000000000000000000000000000008023803\
             c0a8016c0009000108000000000f424000000000000300010000bb8000000018000100020008011000ee00fe0118400000000000000000000000000000008000\
             0000000000000000000000000000000008023829c0a8016c0009000108000000001e848000000000000500010000bb8000000018000100020008016401420152\
             016c04000000000000000000000000000000080000000000000000000000000000000000080210e1efffff38000a000008000000000f424000000000",
        )
}

fn external_receiver_flow_response() -> Vec<u8> {
    let mut response = vec![0u8; RESPONSE_HEADER_SIZE];
    let mut body = vec![0u8; 112];
    body[0] = 8;
    body[1] = 1;
    body[2..4].copy_from_slice(&28u16.to_be_bytes());

    let record = 18usize;
    body[record..record + 2].copy_from_slice(&3u16.to_be_bytes());
    body[record + 2..record + 4].copy_from_slice(&1u16.to_be_bytes());
    body[record + 4..record + 8].copy_from_slice(&48_000u32.to_be_bytes());
    body[record + 8..record + 12].copy_from_slice(&24u32.to_be_bytes());
    body[record + 12..record + 14].copy_from_slice(&2u16.to_be_bytes());
    body[record + 14..record + 16].copy_from_slice(&2u16.to_be_bytes());
    body[record + 16..record + 18].copy_from_slice(&2u16.to_be_bytes());
    // Semantic pointer-table order is endpoint 1, endpoint 2, slot 1,
    // slot 2, status. The target addresses are deliberately unordered.
    for (index, pointer) in [114u16, 62, 56, 118, 70].into_iter().enumerate() {
        let offset = record + 18 + index * 2;
        body[offset..offset + 2].copy_from_slice(&pointer.to_be_bytes());
    }

    body[46..50].copy_from_slice(&[0x00, 0x01, 0x00, 0x01]);
    body[52..60].copy_from_slice(&[0x08, 0x02, 0x13, 0x8E, 239, 69, 1, 11]);
    body[60..76].copy_from_slice(&[
        0x00, 0x09, 0x00, 0x03, 0x12, 0x34, 0xAB, 0xCD, 0x00, 0x0F, 0x42, 0x40, 0x00, 0x03, 0x00,
        0x56,
    ]);
    body[76..104].copy_from_slice(&[
        0x0E, 0xAA, 0x00, 0x0B, 192, 0, 2, 44, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x00, 0x00, 0x00, 0x11,
    ]);
    body[104..108].copy_from_slice(&[0x04, 0x02, 0x13, 0x8C]);
    body[108..112].copy_from_slice(&[0x00, 0x04, 0x00, 0x00]);
    response.extend_from_slice(&body);
    stamp_arc_response(
        &mut response,
        PROTOCOL_DANTE_FLOW,
        OPCODE_QUERY_RECEIVER_FLOWS,
        RESULT_CODE_SUCCESS,
    );
    response
}

#[test]
fn transmitter_channel_name_reconciliation_parser_decodes_controller_avio_response() {
    let response = decode_hexadecimal(
        "280900394a0c243800010000000000000600020200010003002000020003002c7672726f6f6d3a6c656674007672726f6f6d3a726967687400",
    );
    assert_eq!(
        parse_transmitter_channel_name_reconciliation_2809(&response),
        Some(TransmitterChannelNameReconciliation2809 {
            declared_channel_count: 2,
            reported_record_count: 2,
            records: vec![
                TransmitterChannelNameReconciliationRecord2809 {
                    channel_number: 1,
                    record_type_code: 3,
                    name_pointer: 0x20,
                    name: "vrroom:left".to_owned(),
                },
                TransmitterChannelNameReconciliationRecord2809 {
                    channel_number: 2,
                    record_type_code: 3,
                    name_pointer: 0x2C,
                    name: "vrroom:right".to_owned(),
                },
            ],
            raw_body_hexadecimal: "0000000000000600020200010003002000020003002c7672726f6f6d3a6c656674007672726f6f6d3a726967687400".to_owned(),
        })
    );

    let mut malformed = response;
    malformed[28..30].copy_from_slice(&0x0021u16.to_be_bytes());
    assert_eq!(
        parse_transmitter_channel_name_reconciliation_2809(&malformed),
        None
    );
}

#[test]
fn receiver_flow_parser_decodes_shipping_controller_response() {
    let page = parse_receiver_flow_page(&captured_receiver_flow_response()).unwrap();
    assert_eq!(page.maximum_flow_slots, 16);
    assert_eq!(
        page.flows
            .iter()
            .map(|flow| flow.flow_number)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 5]
    );

    let first = &page.flows[0];
    assert_eq!(first.flags, 1);
    assert_eq!(first.flow_type.as_deref(), Some("unicast"));
    assert_eq!(first.sample_rate, 48_000);
    assert_eq!(first.encoding, 24);
    assert_eq!(first.interface_count, 1);
    assert_eq!(first.flow_channel_slot_count, 2);
    assert_eq!(first.receiver_bitmap_word_count, 8);
    assert_eq!(first.interface_endpoints.len(), 1);
    assert_eq!(first.interface_endpoints[0].descriptor_length_bytes, 8);
    assert_eq!(
        first.interface_endpoints[0].raw_descriptor_hexadecimal,
        "08023813c0a8016c"
    );
    assert_eq!(first.interface_endpoints[0].udp_port, 0x3813);
    assert_eq!(
        first.interface_endpoints[0].ipv4_address.as_deref(),
        Some("192.168.1.108")
    );
    assert_eq!(
        first.receiver_bitmaps_hexadecimal,
        vec![
            "00000010000000000000000000000000",
            "00000020000000000000000000000000"
        ]
    );
    assert_eq!(
        first.receiver_channel_numbers_by_flow_channel,
        vec![vec![21], vec![22]]
    );
    assert_eq!(first.subscription_status_code, 9);
    assert_eq!(first.interface_state_bitmap, 1);
    assert_eq!(first.status_flags, 0x0800);
    assert_eq!(first.status_unknown, 0);
    assert_eq!(first.latency_nanoseconds, 1_000_000);
    assert_eq!(first.transport, 0);
    assert_eq!(first.external_identity_pointer, 0);
    assert_eq!(first.external_identity, None);
    assert!(first.effective_subscription_identities.is_empty());
    assert_eq!(first.raw_record_hexadecimal.len(), 168);

    let third = &page.flows[2];
    assert_eq!(third.latency_nanoseconds, 2_000_000);
    assert_eq!(
        third.receiver_channel_numbers_by_flow_channel,
        vec![vec![15], vec![16]]
    );

    let multicast = &page.flows[3];
    assert_eq!(multicast.flow_type.as_deref(), Some("multicast"));
    assert_eq!(multicast.interface_endpoints[0].udp_port, 0x10e1);
    assert_eq!(
        multicast.interface_endpoints[0].ipv4_address.as_deref(),
        Some("239.255.255.56")
    );
    assert_eq!(multicast.subscription_status_code, 10);
    assert_eq!(multicast.interface_state_bitmap, 0);
    assert_eq!(
        multicast.receiver_bitmaps_hexadecimal,
        vec![
            "04000000000000000000000000000000",
            "08000000000000000000000000000000"
        ]
    );
    assert_eq!(
        multicast.receiver_channel_numbers_by_flow_channel,
        vec![vec![11], vec![12]]
    );
}

#[test]
fn receiver_channel_bitmap_preserves_multiple_and_empty_mappings() {
    assert_eq!(
        receiver_channel_numbers(&decode_hexadecimal("00010010000000000000000000000000")),
        Some(vec![1, 21])
    );
    assert_eq!(receiver_channel_numbers(&[0; 16]), Some(Vec::new()));
    assert_eq!(receiver_channel_numbers(&[0; 15]), None);
}

#[test]
fn receiver_flow_parser_uses_variable_bitmaps_and_semantic_pointer_order() {
    let page = parse_receiver_flow_page(&external_receiver_flow_response()).unwrap();
    let flow = &page.flows[0];
    assert_eq!(flow.flow_number, 3);
    assert_eq!(flow.interface_count, 2);
    assert_eq!(flow.flow_channel_slot_count, 2);
    assert_eq!(flow.receiver_bitmap_word_count, 2);
    assert_eq!(flow.receiver_bitmaps_hexadecimal, ["00010001", "00040000"]);
    assert_eq!(
        flow.receiver_channel_numbers_by_flow_channel,
        [vec![1, 17], vec![3]]
    );
    assert_eq!(flow.interface_endpoints[0].pointer, 114);
    assert_eq!(flow.interface_endpoints[0].descriptor_length_bytes, 4);
    assert_eq!(flow.interface_endpoints[0].udp_port, 5004);
    assert_eq!(flow.interface_endpoints[0].ipv4_address, None);
    assert_eq!(flow.interface_endpoints[1].pointer, 62);
    assert_eq!(flow.interface_endpoints[1].descriptor_length_bytes, 8);
    assert_eq!(flow.interface_endpoints[1].udp_port, 5006);
    assert_eq!(
        flow.interface_endpoints[1].ipv4_address.as_deref(),
        Some("239.69.1.11")
    );
    assert_eq!(flow.subscription_status_code, 9);
    assert_eq!(flow.interface_state_bitmap, 3);
    assert_eq!(flow.status_flags, 0x1234);
    assert_eq!(flow.status_unknown, 0xABCD);
    assert_eq!(flow.latency_nanoseconds, 1_000_000);
    assert_eq!(flow.transport, 3);
    let identity = flow.external_identity.as_ref().unwrap();
    assert_eq!(identity.pointer, 86);
    assert_eq!(identity.length_words, 14);
    assert_eq!(identity.reserved, 0xAA);
    assert_eq!(identity.presence_mask, 0x000B);
    assert_eq!(identity.source_ipv4.as_deref(), Some("192.0.2.44"));
    assert_eq!(identity.session_id, Some(0x0102_0304_0506_0708));
    assert_eq!(identity.unknown_optional_field_raw, 0x1122_3344_5566_7788);
    assert_eq!(identity.clock_offset, Some(17));
    assert_eq!(flow.effective_subscription_identities.len(), 3);
    assert_eq!(
        flow.effective_subscription_identities[0].receiver_channel,
        1
    );
    assert_eq!(flow.effective_subscription_identities[0].flow_slot, 1);
    assert_eq!(
        flow.effective_subscription_identities[2].receiver_channel,
        3
    );
    assert_eq!(flow.effective_subscription_identities[2].flow_slot, 2);
}

#[test]
fn receiver_flow_parser_rejects_unsupported_form_and_bad_external_identity() {
    let response = external_receiver_flow_response();
    let mut alternate_form = response.clone();
    alternate_form[30..32].copy_from_slice(&0x4001u16.to_be_bytes());
    assert_eq!(parse_receiver_flow_page(&alternate_form), None);

    let mut short_identity = response;
    short_identity[86] = 0x0D;
    assert_eq!(parse_receiver_flow_page(&short_identity), None);
}

#[test]
fn receiver_flow_parser_accepts_authentic_empty_virtual_a32_response() {
    let response = decode_hexadecimal(
        "2729002c033a3200000110000000000000000000000000000000000000000000000000000000000000000000",
    );
    assert_eq!(
        parse_receiver_flow_page(&response),
        Some(ReceiverFlowPage {
            result_code: RESULT_CODE_SUCCESS,
            page_disposition: ModernArcPageDisposition::Complete,
            maximum_flow_slots: 16,
            reported_flow_count: 0,
            flows: Vec::new(),
        })
    );
}

#[test]
fn receiver_port_ranges_parse_controller_and_authentic_firmware_response() {
    let response = decode_hexadecimal("27290012033c330000013800397f398039ff");
    assert_eq!(parse_result_code(&response), Some(RESULT_CODE_SUCCESS));
    assert_eq!(
        parse_receiver_port_ranges(&response),
        Some(ReceiverPortRanges {
            first_port_range_start: 0x3800,
            first_port_range_end: 0x397F,
            second_port_range_start: 0x3980,
            second_port_range_end: 0x39FF,
            second_port_range_available: true,
        })
    );

    let mut overlapping = response;
    overlapping[14..16].copy_from_slice(&0x397Fu16.to_be_bytes());
    assert_eq!(parse_receiver_port_ranges(&overlapping), None);
}

#[test]
fn receiver_port_ranges_parse_managed_modern_responses() {
    let input_response = decode_hexadecimal("28090012007033000001380038ff390038ff");
    assert_eq!(
        parse_result_code(&input_response),
        Some(RESULT_CODE_SUCCESS)
    );
    assert_eq!(
        parse_receiver_port_ranges(&input_response),
        Some(ReceiverPortRanges {
            first_port_range_start: 0x3800,
            first_port_range_end: 0x38FF,
            second_port_range_start: 0x3900,
            second_port_range_end: 0x38FF,
            second_port_range_available: false,
        })
    );

    let output_response = decode_hexadecimal("28090012000333000001380038fd38fe38ff");
    assert_eq!(
        parse_result_code(&output_response),
        Some(RESULT_CODE_SUCCESS)
    );
    assert_eq!(
        parse_receiver_port_ranges(&output_response),
        Some(ReceiverPortRanges {
            first_port_range_start: 0x3800,
            first_port_range_end: 0x38FD,
            second_port_range_start: 0x38FE,
            second_port_range_end: 0x38FF,
            second_port_range_available: true,
        })
    );
}

#[test]
fn transmit_channel_capabilities_parse_controller_and_authentic_firmware_responses() {
    let physical_response = decode_hexadecimal("272900120329203200010001000100807fff");
    assert_eq!(
        parse_result_code(&physical_response),
        Some(RESULT_CODE_SUCCESS)
    );
    assert_eq!(
        parse_transmit_channel_capabilities(&physical_response),
        Some(TransmitChannelCapabilities {
            record_count: 1,
            ranges: vec![TransmitChannelCapabilityRange {
                first_transmit_channel: 1,
                last_transmit_channel: 128,
                unknown_value: 0x7FFF,
            }],
        })
    );

    let virtual_response = decode_hexadecimal("272900120329203200010001000100207fff");
    assert_eq!(
        parse_transmit_channel_capabilities(&virtual_response),
        Some(TransmitChannelCapabilities {
            record_count: 1,
            ranges: vec![TransmitChannelCapabilityRange {
                first_transmit_channel: 1,
                last_transmit_channel: 32,
                unknown_value: 0x7FFF,
            }],
        })
    );
    assert_eq!(
        parse_transmit_channel_capabilities(&virtual_response[..17]),
        None
    );

    let multiple_ranges = decode_hexadecimal("272900180329203200010002000100200123002100400456");
    assert_eq!(
        parse_transmit_channel_capabilities(&multiple_ranges),
        Some(TransmitChannelCapabilities {
            record_count: 2,
            ranges: vec![
                TransmitChannelCapabilityRange {
                    first_transmit_channel: 1,
                    last_transmit_channel: 32,
                    unknown_value: 0x0123,
                },
                TransmitChannelCapabilityRange {
                    first_transmit_channel: 33,
                    last_transmit_channel: 64,
                    unknown_value: 0x0456,
                },
            ],
        })
    );

    let empty = decode_hexadecimal("2729000c0000203200010000");
    assert_eq!(
        parse_transmit_channel_capabilities(&empty),
        Some(TransmitChannelCapabilities {
            record_count: 0,
            ranges: Vec::new(),
        })
    );

    let mut nonzero_reserved = empty;
    nonzero_reserved[10] = 1;
    assert_eq!(parse_transmit_channel_capabilities(&nonzero_reserved), None);
}

#[test]
fn receiver_flow_parser_rejects_invalid_record_pointers() {
    let original = captured_receiver_flow_response();
    for invalid_pointer in [0u16, 43, u16::MAX] {
        let mut response = original.clone();
        response[12..14].copy_from_slice(&invalid_pointer.to_be_bytes());
        assert_eq!(parse_receiver_flow_page(&response), None);
    }
}

#[test]
fn tx_flows_parser_decodes_multicast_record() {
    let page = parse_tx_flow_page(&flow_query_response()).unwrap();
    assert_eq!(page.max_flow_slots, 16);
    let flows = page.flows;
    assert_eq!(flows.len(), 2);
    assert_eq!(flows[0].flow_number, 1);
    assert_eq!(flows[0].flow_type, "unicast");
    assert_eq!(flows[0].channel_count, 1);
    assert!(flows[0].channels.is_empty());
    let flow = &flows[1];
    assert_eq!(flow.flow_number, 17);
    assert_eq!(flow.flow_type, "multicast");
    assert_eq!(flow.sample_rate, 48_000);
    assert_eq!(flow.encoding, 24);
    assert_eq!(flow.frames_per_packet, 2);
    assert_eq!(flow.channel_count, 2);
    assert_eq!(flow.channels, vec![1, 2]);
}

#[test]
fn tx_flows_parser_uses_variable_channel_offset_in_short_records() {
    let mut response = vec![0u8; RESPONSE_HEADER_SIZE];
    response.extend_from_slice(&[0x10, 0x01]);
    response.extend_from_slice(&44u16.to_be_bytes());
    response.extend(std::iter::repeat_n(0, 30));

    let mut record = vec![0u8; 55];
    record[0..2].copy_from_slice(&17u16.to_be_bytes());
    record[2..4].copy_from_slice(&FLOW_TYPE_MULTICAST.to_be_bytes());
    record[4..8].copy_from_slice(&48_000u32.to_be_bytes());
    record[8..12].copy_from_slice(&24u32.to_be_bytes());
    record[12..14].copy_from_slice(&1u16.to_be_bytes());
    record[14..16].copy_from_slice(&2u16.to_be_bytes());
    record[18..20].copy_from_slice(&7u16.to_be_bytes());
    record[20..22].copy_from_slice(&8u16.to_be_bytes());
    response.extend_from_slice(&record);
    stamp_arc_response(
        &mut response,
        PROTOCOL_DANTE_FLOW_2801,
        OPCODE_QUERY_TX_FLOWS,
        RESULT_CODE_SUCCESS,
    );

    let flows = parse_tx_flows(&response).unwrap();
    assert_eq!(flows.len(), 1);
    assert_eq!(flows[0].channels, vec![7, 8]);
}

#[test]
fn tx_flows_parser_preserves_authentic_zero_channel_placeholders() {
    let response = decode_hexadecimal(
            "2729006f286c220000011001002c000000000000000000000000000000000000000000000000000000000000002000020002ee0000000018000100080050001000000000000000000000000000000058080210e1efff45670a000001000000000010006c000f424000000000333200",
        );
    let flows = parse_tx_flows(&response).unwrap();
    assert_eq!(flows.len(), 1);
    assert_eq!(flows[0].flow_number, 32);
    assert_eq!(flows[0].sample_rate, 192_000);
    assert_eq!(flows[0].channel_count, 8);
    assert_eq!(flows[0].channels, vec![16, 0, 0, 0, 0, 0, 0, 0]);

    let mut duplicate_nonzero = response;
    duplicate_nonzero[64..66].copy_from_slice(&16u16.to_be_bytes());
    assert_eq!(parse_tx_flows(&duplicate_nonzero), None);
}

#[test]
fn tx_flows_parser_accepts_paginated_2729_and_2801_responses() {
    let mut response = flow_query_response();
    stamp_arc_response(
        &mut response,
        PROTOCOL_DANTE_FLOW_2801,
        OPCODE_QUERY_TX_FLOWS,
        crate::protocol::RESULT_CODE_MORE_PAGES,
    );
    assert_eq!(parse_tx_flows(&response).unwrap().len(), 2);

    stamp_arc_response(
        &mut response,
        PROTOCOL_ARC_2809,
        OPCODE_QUERY_TX_FLOWS_2809,
        RESULT_CODE_SUCCESS,
    );
    assert_eq!(parse_tx_flows(&response), None);
}

#[test]
fn transmitter_flow_status_parser_decodes_zero_unicast_and_multicast_records() {
    let zero_record = decode_hexadecimal("28090016294126000001000000000000020000000000");
    let zero_page = parse_transmitter_flow_status_page(&zero_record).unwrap();
    assert_eq!(zero_page.maximum_flow_slots, 2);
    assert_eq!(zero_page.reported_flow_count, 0);
    assert!(zero_page.flows.is_empty());
    assert_eq!(zero_page.raw_body_hexadecimal, "000000000000020000000000");

    let unicast = decode_hexadecimal(
            "2809008329422600000100000000000002010020000031000000bb80000000181427000100000003000100000000001100000000001600180000000000000000000000000000000008130000007800810001000000000000040b0101007000000507000200010002000002000010000008023805c0a8016c6c782d64616e7465003300",
        );
    let unicast_page = parse_transmitter_flow_status_page(&unicast).unwrap();
    assert_eq!(unicast_page.maximum_flow_slots, 2);
    assert_eq!(unicast_page.reported_flow_count, 1);
    let unicast_flow = &unicast_page.flows[0];
    assert_eq!(unicast_flow.record_pointer, 32);
    assert_eq!(unicast_flow.global_flow_id, 1);
    assert_eq!(unicast_flow.flow_name_pointer, 22);
    assert_eq!(unicast_flow.flow_name, "1");
    assert_eq!(unicast_flow.flow_type_code, FLOW_TYPE_UNICAST);
    assert_eq!(unicast_flow.flow_type.as_deref(), Some("unicast"));
    assert_eq!(unicast_flow.format_pointer, 24);
    assert_eq!(unicast_flow.sample_rate, Some(48_000));
    assert_eq!(unicast_flow.encoding, Some(24));
    assert_eq!(unicast_flow.populated_slot_count, 2);
    assert_eq!(unicast_flow.endpoint_descriptor_pointer, 112);
    assert_eq!(
        unicast_flow.endpoint_descriptor_hexadecimal,
        "08023805c0a8016c"
    );
    assert_eq!(unicast_flow.destination_user_datagram_port, Some(0x3805));
    assert_eq!(
        unicast_flow
            .destination_internet_protocol_version_four_address
            .as_deref(),
        Some("192.168.1.108")
    );
    assert_eq!(unicast_flow.subscriber_device_name_pointer, 120);
    assert_eq!(
        unicast_flow.subscriber_device_name.as_deref(),
        Some("lx-dante")
    );
    assert_eq!(unicast_flow.subscriber_flow_name_pointer, 129);
    assert_eq!(unicast_flow.subscriber_flow_name.as_deref(), Some("3"));
    assert_eq!(unicast_flow.record_length_bytes, 76);
    assert_eq!(unicast_flow.raw_record_hexadecimal.len(), 152);
    assert_eq!(parse_tx_flows(&unicast), None);

    let multicast = decode_hexadecimal(
            "2809007802a02600000100000000000002010020000232000000bb8000000018142700020000000300020000000000020000000000160018000f424000000000000000000000000008130000000000000001000000000000040b01010070000005070002000100020000020000100000080210e1efffff38",
        );
    let multicast_page = parse_transmitter_flow_status_page(&multicast).unwrap();
    let multicast_flow = &multicast_page.flows[0];
    assert_eq!(multicast_flow.global_flow_id, 2);
    assert_eq!(multicast_flow.flow_name, "2");
    assert_eq!(multicast_flow.flow_type_code, FLOW_TYPE_MULTICAST);
    assert_eq!(multicast_flow.flow_type.as_deref(), Some("multicast"));
    assert_eq!(multicast_flow.populated_slot_count, 2);
    assert_eq!(multicast_flow.destination_user_datagram_port, Some(4321));
    assert_eq!(
        multicast_flow
            .destination_internet_protocol_version_four_address
            .as_deref(),
        Some("239.255.255.56")
    );
    assert_eq!(multicast_flow.subscriber_device_name_pointer, 0);
    assert_eq!(multicast_flow.subscriber_device_name, None);
    assert_eq!(multicast_flow.subscriber_flow_name_pointer, 0);
    assert_eq!(multicast_flow.subscriber_flow_name, None);
}

#[test]
fn transmitter_flow_status_parser_preserves_unknown_types_and_rejects_malformed_frames() {
    let response = decode_hexadecimal(
            "2809008329422600000100000000000002010020000031000000bb80000000181427000100000003000100000000001100000000001600180000000000000000000000000000000008130000007800810001000000000000040b0101007000000507000200010002000002000010000008023805c0a8016c6c782d64616e7465003300",
        );
    for length in 0..response.len() {
        assert_eq!(
            parse_transmitter_flow_status_page(&response[..length]),
            None
        );
    }

    let mut unknown_type = response.clone();
    unknown_type[46..48].copy_from_slice(&0x9999u16.to_be_bytes());
    let parsed_unknown = parse_transmitter_flow_status_page(&unknown_type).unwrap();
    assert_eq!(parsed_unknown.flows[0].flow_type_code, 0x9999);
    assert_eq!(parsed_unknown.flows[0].flow_type, None);

    let mut invalid_record_pointer = response.clone();
    invalid_record_pointer[18..20].copy_from_slice(&18u16.to_be_bytes());
    assert_eq!(
        parse_transmitter_flow_status_page(&invalid_record_pointer),
        None
    );

    let mut invalid_flow_number = response.clone();
    invalid_flow_number[34..36].copy_from_slice(&3u16.to_be_bytes());
    assert_eq!(
        parse_transmitter_flow_status_page(&invalid_flow_number),
        None
    );

    let mut invalid_flow_name_pointer = response.clone();
    invalid_flow_name_pointer[52..54].copy_from_slice(&0u16.to_be_bytes());
    assert_eq!(
        parse_transmitter_flow_status_page(&invalid_flow_name_pointer),
        None
    );

    let mut invalid_endpoint_pointer = response;
    invalid_endpoint_pointer[92..94].copy_from_slice(&u16::MAX.to_be_bytes());
    assert_eq!(
        parse_transmitter_flow_status_page(&invalid_endpoint_pointer),
        None
    );

    let mut stale_zero_record = decode_hexadecimal("2809001629a92600000100000000000002006f2d6368");
    stale_zero_record[4..6].copy_from_slice(&0x1234u16.to_be_bytes());
    let stale_page = parse_transmitter_flow_status_page(&stale_zero_record).unwrap();
    assert_eq!(stale_page.reported_flow_count, 0);
    assert!(stale_page.flows.is_empty());
}

#[test]
fn transmitter_channel_status_2809_parser_decodes_shipping_controller_response() {
    let response = decode_hexadecimal(
            "280900a42852240000010000000000000202003c007c00030000bb80010100180400001800180004626c7565746f6f74683a6c656674004c6566740014140001000000030001000000000007000000000028001800000000000000370000000000000000626c7565746f6f74683a726967687400526967687400000014140002000000030002000000000007000000000064001800000000000000740000000000000000",
        );
    let page = parse_modern_arc_transmitter_channel_status_page(&response).unwrap();
    assert_eq!(page.page_capacity, 2);
    assert_eq!(page.reported_record_count, 2);
    assert_eq!(page.records.len(), 2);

    let left = &page.records[0];
    assert_eq!(left.record_pointer, 60);
    assert_eq!(left.record_type_code, 0x1414);
    assert_eq!(left.channel_number, 1);
    assert_eq!(left.channel_name_pointer, 40);
    assert_eq!(left.channel_name, "bluetooth:left");
    assert_eq!(left.format_pointer, 24);
    assert_eq!(
        left.format_descriptor_hexadecimal,
        "0000bb80010100180400001800180004"
    );
    assert_eq!(left.sample_rate, Some(48_000));
    assert_eq!(left.encoding, Some(24));
    assert_eq!(left.friendly_channel_name_pointer, 55);
    assert_eq!(left.friendly_channel_name, "Left");
    assert_eq!(left.raw_record_hexadecimal.len(), 80);

    let right = &page.records[1];
    assert_eq!(right.record_pointer, 124);
    assert_eq!(right.channel_number, 2);
    assert_eq!(right.channel_name_pointer, 100);
    assert_eq!(right.channel_name, "bluetooth:right");
    assert_eq!(right.friendly_channel_name_pointer, 116);
    assert_eq!(right.friendly_channel_name, "Right");
    assert_eq!(page.raw_body_hexadecimal, bytes_to_hex(&response[10..]));
}

#[test]
fn transmitter_channel_status_2809_parser_rejects_malformed_responses() {
    let response = decode_hexadecimal(
            "280900a42852240000010000000000000202003c007c00030000bb80010100180400001800180004626c7565746f6f74683a6c656674004c6566740014140001000000030001000000000007000000000028001800000000000000370000000000000000626c7565746f6f74683a726967687400526967687400000014140002000000030002000000000007000000000064001800000000000000740000000000000000",
        );
    for length in 0..response.len() {
        assert_eq!(
            parse_modern_arc_transmitter_channel_status_page(&response[..length]),
            None
        );
    }

    let mut unknown_record_type = response.clone();
    unknown_record_type[60..62].copy_from_slice(&0x9999u16.to_be_bytes());
    assert_eq!(
        parse_modern_arc_transmitter_channel_status_page(&unknown_record_type),
        None
    );

    let mutations = [
        (16, 17, vec![1]),
        (20, 22, 60u16.to_be_bytes().to_vec()),
        (20, 22, 80u16.to_be_bytes().to_vec()),
        (62, 64, 0u16.to_be_bytes().to_vec()),
        (62, 64, 2u16.to_be_bytes().to_vec()),
        (80, 82, 17u16.to_be_bytes().to_vec()),
        (82, 84, 159u16.to_be_bytes().to_vec()),
        (90, 92, 17u16.to_be_bytes().to_vec()),
        (24, 28, 0u32.to_be_bytes().to_vec()),
        (30, 32, 0u16.to_be_bytes().to_vec()),
    ];
    for (start, end, replacement) in mutations {
        let mut malformed = response.clone();
        malformed[start..end].copy_from_slice(&replacement);
        assert_eq!(
            parse_modern_arc_transmitter_channel_status_page(&malformed),
            None
        );
    }

    let bodyless_a32_response = decode_hexadecimal("2809000a285224000030");
    assert_eq!(parse_result_code(&bodyless_a32_response), Some(0x0030));
    assert_eq!(
        parse_modern_arc_transmitter_channel_status_page(&bodyless_a32_response),
        None
    );
}

#[test]
fn receiver_flow_status_2809_parser_decodes_controller_refresh_pages() {
    let empty = decode_hexadecimal("28090016285636000001000000000000020000020002");
    let empty_page = parse_modern_arc_receiver_flow_status_page(&empty).unwrap();
    assert_eq!(empty_page.maximum_flow_slots, 2);
    assert_eq!(empty_page.reported_flow_count, 0);
    assert!(empty_page.flows.is_empty());

    let active = decode_hexadecimal(
            "2809007401c93600000100000000000002010020000231000000bb8000000018142200010000000300010000000000010000000000160018000f42400000000000000000000000000a0e000000000000000000000001006c00000000040001010064000008023801c0a8013d0001000200000100",
        );
    let page = parse_modern_arc_receiver_flow_status_page(&active).unwrap();
    assert_eq!(page.maximum_flow_slots, 2);
    assert_eq!(page.reported_flow_count, 1);
    assert_eq!(page.flows.len(), 1);
    let flow = &page.flows[0];
    assert_eq!(flow.record_pointer, 32);
    assert_eq!(flow.record_type_code, 0x1422);
    assert_eq!(flow.global_flow_id, 1);
    assert_eq!(flow.media_type_code, 3);
    assert_eq!(flow.media_local_flow_id, 1);
    assert_eq!(flow.flow_type_code, 1);
    assert_eq!(flow.flow_name_pointer, 22);
    assert_eq!(flow.flow_name, "1");
    assert_eq!(flow.format_pointer, 24);
    assert_eq!(flow.sample_rate, Some(48_000));
    assert_eq!(flow.encoding, Some(24));
    assert_eq!(flow.latency_nanoseconds, Some(1_000_000));
    assert_eq!(flow.local_receiver_channel_count, 1);
    assert_eq!(flow.receiver_mapping_descriptor_pointer, 108);
    assert_eq!(
        flow.receiver_mapping_descriptor_hexadecimal,
        "0001000200000100"
    );
    assert_eq!(flow.status_flags, 0x0400);
    assert_eq!(flow.status_code, 0x0101);
    assert_eq!(flow.endpoint_descriptor_hexadecimal, "08023801c0a8013d");
    assert_eq!(flow.destination_user_datagram_port, Some(0x3801));
    assert_eq!(
        flow.destination_internet_protocol_version_four_address
            .as_deref(),
        Some("192.168.1.61")
    );
    assert_eq!(flow.raw_record_hexadecimal.len(), 168);
    assert_eq!(page.raw_body_hexadecimal, bytes_to_hex(&active[10..]));

    let two_local_receivers = decode_hexadecimal(
            "2809007402d23600000100000000000002010020000231000000bb8000000018142200010000000300010000000000010000000000160018000f42400000000000000000000000000a0e000000000000000000000002006c00000000040001010064000008023801c0a801240001000200000101",
        );
    let two_receiver_flow = &parse_modern_arc_receiver_flow_status_page(&two_local_receivers)
        .unwrap()
        .flows[0];
    assert_eq!(two_receiver_flow.local_receiver_channel_count, 2);
    assert_eq!(
        two_receiver_flow.receiver_mapping_descriptor_hexadecimal,
        "0001000200000101"
    );
}

#[test]
fn receiver_flow_status_2809_parser_rejects_malformed_responses() {
    let response = decode_hexadecimal(
            "2809007401c93600000100000000000002010020000231000000bb8000000018142200010000000300010000000000010000000000160018000f42400000000000000000000000000a0e000000000000000000000001006c00000000040001010064000008023801c0a8013d0001000200000100",
        );
    for length in 0..response.len() {
        assert_eq!(
            parse_modern_arc_receiver_flow_status_page(&response[..length]),
            None
        );
    }

    let mut unknown_record_type = response.clone();
    unknown_record_type[32..34].copy_from_slice(&0x9999u16.to_be_bytes());
    assert_eq!(
        parse_modern_arc_receiver_flow_status_page(&unknown_record_type),
        None
    );

    let mutations = [
        (16, 17, vec![0]),
        (17, 18, vec![3]),
        (18, 20, 18u16.to_be_bytes().to_vec()),
        (34, 36, 0u16.to_be_bytes().to_vec()),
        (34, 36, 3u16.to_be_bytes().to_vec()),
        (40, 42, 0u16.to_be_bytes().to_vec()),
        (52, 54, 0u16.to_be_bytes().to_vec()),
        (54, 56, u16::MAX.to_be_bytes().to_vec()),
        (24, 28, 0u32.to_be_bytes().to_vec()),
        (28, 32, 0u32.to_be_bytes().to_vec()),
        (84, 86, 0u16.to_be_bytes().to_vec()),
        (86, 88, u16::MAX.to_be_bytes().to_vec()),
    ];
    for (start, end, replacement) in mutations {
        let mut malformed = response.clone();
        malformed[start..end].copy_from_slice(&replacement);
        assert_eq!(parse_modern_arc_receiver_flow_status_page(&malformed), None);
    }

    let bodyless_a32_response = decode_hexadecimal("2809000a285636000030");
    assert_eq!(parse_result_code(&bodyless_a32_response), Some(0x0030));
    assert_eq!(
        parse_modern_arc_receiver_flow_status_page(&bodyless_a32_response),
        None
    );
}

#[test]
fn receiver_channel_status_2809_parser_decodes_controller_rename_readbacks() {
    let first = decode_hexadecimal(
            "2809007c284a34000001000000000000010100446d69632d6d69782d68696768006c782d64616e74650000000000bb800101001804000018001800043031004c65667400141c000100000003000100000000000600000000003c002c000000000000003f000000000000000006080000001400210010000002020000",
        );
    let first_page = parse_modern_arc_receiver_channel_status_page(&first).unwrap();
    assert_eq!(first_page.page_capacity, 1);
    assert_eq!(first_page.reported_record_count, 1);
    assert_eq!(first_page.records.len(), 1);
    let first_record = &first_page.records[0];
    assert_eq!(first_record.record_pointer, 68);
    assert_eq!(first_record.record_type_code, 0x141C);
    assert_eq!(first_record.channel_number, 1);
    assert_eq!(first_record.local_channel_name_pointer, 60);
    assert_eq!(first_record.local_channel_name, "01");
    assert_eq!(first_record.format_pointer, 44);
    assert_eq!(
        first_record.format_descriptor_hexadecimal,
        "0000bb80010100180400001800180004"
    );
    assert_eq!(first_record.sample_rate, Some(48_000));
    assert_eq!(first_record.encoding, Some(24));
    assert_eq!(first_record.friendly_channel_name_pointer, 63);
    assert_eq!(first_record.friendly_channel_name, "Left");
    assert_eq!(first_record.source_channel_name_pointer, 20);
    assert_eq!(
        first_record.source_channel_name.as_deref(),
        Some("mic-mix-high")
    );
    assert_eq!(first_record.source_device_name_pointer, 33);
    assert_eq!(first_record.source_device_name.as_deref(), Some("lx-dante"));
    assert_eq!(first_record.subscription_status_code, 0x0010);
    assert_eq!(first_record.receiver_status_code, 0);
    assert_eq!(first_record.receiver_capability_flags, 0x0000_0006);
    assert!(!first_record.can_subscribe_self);
    assert_eq!(first_record.status_flags, Some(0x0202));
    assert_eq!(first_record.raw_record_hexadecimal.len(), 112);

    let second = decode_hexadecimal(
            "28090084284d340000010000000000000101004c6d69632d6d69782d68696768006c782d64616e74650000000000bb800101001804000018001800046d69632d6d6978004c65667400000000141c000100000003000100000000000600000000003c002c0000000000000044000000000000000006080000001400210010000002020000",
        );
    let second_page = parse_modern_arc_receiver_channel_status_page(&second).unwrap();
    let second_record = &second_page.records[0];
    assert_eq!(second_record.record_pointer, 76);
    assert_eq!(second_record.local_channel_name, "mic-mix");
    assert_eq!(second_record.friendly_channel_name_pointer, 68);
    assert_eq!(second_record.friendly_channel_name, "Left");
    assert_eq!(second_record.subscription_status_code, 0x0010);
    assert_eq!(second_record.receiver_status_code, 0);
    assert_eq!(second_record.receiver_capability_flags, 0x0000_0006);
    assert!(!second_record.can_subscribe_self);
    assert_eq!(second_record.status_flags, Some(0x0202));
}

#[test]
fn receiver_channel_status_capability_mask_preserves_flags_and_trailing_status() {
    let mut response = decode_hexadecimal(
        "2809007c284a34000001000000000000010100446d69632d6d69782d68696768006c782d64616e74650000000000bb800101001804000018001800043031004c65667400141c000100000003000100000000000600000000003c002c000000000000003f000000000000000006080000001400210010000002020000",
    );
    let record_pointer = usize::from(read_u16(&response, MODERN_ARC_POINTER_TABLE_OFFSET).unwrap());
    response[record_pointer + 0x0c..record_pointer + 0x10]
        .copy_from_slice(&0xa5a5_000fu32.to_be_bytes());

    let record = &parse_modern_arc_receiver_channel_status_page(&response)
        .unwrap()
        .records[0];
    assert_eq!(record.receiver_capability_flags, 0xa5a5_000f);
    assert!(record.can_subscribe_self);
    assert!(record.can_rename);
    assert_eq!(record.status_flags, Some(0x0202));

    response[record_pointer + 0x0c..record_pointer + 0x10]
        .copy_from_slice(&0xa5a5_0007u32.to_be_bytes());
    let record = &parse_modern_arc_receiver_channel_status_page(&response)
        .unwrap()
        .records[0];
    assert_eq!(record.receiver_capability_flags, 0xa5a5_0007);
    assert!(!record.can_subscribe_self);
    assert!(record.can_rename);
    assert_eq!(record.status_flags, Some(0x0202));

    response[record_pointer + 0x0c..record_pointer + 0x10]
        .copy_from_slice(&0xa5a5_040fu32.to_be_bytes());
    let record = &parse_modern_arc_receiver_channel_status_page(&response)
        .unwrap()
        .records[0];
    assert_eq!(record.receiver_capability_flags, 0xa5a5_040f);
    assert!(record.can_subscribe_self);
    assert!(!record.can_rename);
}

#[test]
fn receiver_channel_status_2809_parser_handles_two_channels_and_rejects_malformed_records() {
    let response = decode_hexadecimal(
            "280900a801f13400000100000000000002020030007000030000bb8001010018040000200020000e303100434831006c141c00010000000300010000000000060000000000280018000000000000002b0000000000000000060800000000000000000000020200003032004348320032141c00020000000300020000000000060000000000680018000000000000006b000000000000000006080000000000000000000002020000",
        );
    let page = parse_modern_arc_receiver_channel_status_page(&response).unwrap();
    assert_eq!(page.page_capacity, 2);
    assert_eq!(page.reported_record_count, 2);
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.channel_number)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(page.records[0].source_channel_name, None);
    assert_eq!(page.records[0].source_device_name, None);
    assert_eq!(page.records[1].local_channel_name, "02");

    let mut larger_capacity = response.clone();
    larger_capacity[16] = 64;
    assert_eq!(
        parse_modern_arc_receiver_channel_status_page(&larger_capacity)
            .unwrap()
            .page_capacity,
        64
    );

    for length in 0..response.len() {
        assert_eq!(
            parse_modern_arc_receiver_channel_status_page(&response[..length]),
            None
        );
    }

    let mut unknown_record_type = response.clone();
    unknown_record_type[48..50].copy_from_slice(&0x9999u16.to_be_bytes());
    assert_eq!(
        parse_modern_arc_receiver_channel_status_page(&unknown_record_type),
        None
    );

    let mut duplicate_record_pointer = response.clone();
    duplicate_record_pointer[20..22].copy_from_slice(&48u16.to_be_bytes());
    assert_eq!(
        parse_modern_arc_receiver_channel_status_page(&duplicate_record_pointer),
        None
    );

    let mut invalid_channel_number = response.clone();
    invalid_channel_number[50..52].copy_from_slice(&0u16.to_be_bytes());
    assert_eq!(
        parse_modern_arc_receiver_channel_status_page(&invalid_channel_number),
        None
    );

    let mut invalid_local_name_pointer = response.clone();
    invalid_local_name_pointer[68..70].copy_from_slice(&0u16.to_be_bytes());
    assert_eq!(
        parse_modern_arc_receiver_channel_status_page(&invalid_local_name_pointer),
        None
    );

    let mut invalid_format_pointer = response;
    invalid_format_pointer[70..72].copy_from_slice(&u16::MAX.to_be_bytes());
    assert_eq!(
        parse_modern_arc_receiver_channel_status_page(&invalid_format_pointer),
        None
    );
}

#[test]
fn tx_flows_parser_rejects_failure_result_code() {
    let mut response = flow_query_response();
    response[8..10].copy_from_slice(&0x0600u16.to_be_bytes());
    assert_eq!(parse_tx_flows(&response), None);
}

#[test]
fn dante_brooklyn_control_protocol_flow_setup_parsers_decode_authentic_fallback_exchange() {
    let request = decode_hexadecimal(
        "1102005000000100000000380000bb8000000018000100040048000100000000\
             000000240a000002001000430000000000000000000000004133322d30303030\
             3031003100000000080238010afe4e0b",
    );
    let response = decode_hexadecimal("1102001800000100000100017fef911d0001000100000000");

    let parsed_request =
        parse_dante_brooklyn_control_protocol_flow_setup_request(&request).unwrap();
    assert_eq!(parsed_request.transaction_identifier_hex, "00000100");
    assert_eq!(parsed_request.receiver_device_name_pointer, 56);
    assert_eq!(parsed_request.sample_rate, 48_000);
    assert_eq!(parsed_request.encoding, 24);
    assert_eq!(parsed_request.transport_descriptor_pointer, 72);
    assert_eq!(parsed_request.transport_descriptor_count, 1);
    assert_eq!(parsed_request.address_value_pointer, 36);
    assert_eq!(parsed_request.flow_span_value, 16);
    assert_eq!(parsed_request.receiver_channel_name_pointer, 67);
    assert_eq!(parsed_request.receiver_device_name, "A32-000001");
    assert_eq!(parsed_request.receiver_channel_name, "1");
    assert_eq!(parsed_request.address_at_pointer, "10.0.0.2");
    assert_eq!(parsed_request.transport_descriptor_hex, "08023801");
    assert_eq!(parsed_request.receiver_address, "10.254.78.11");
    assert_eq!(parsed_request.raw_payload_hex, bytes_to_hex(&request));

    let parsed_response =
        parse_dante_brooklyn_control_protocol_flow_setup_response(&response).unwrap();
    assert_eq!(parsed_response.transaction_identifier_hex, "00000100");
    assert_eq!(parsed_response.field_at_offset_8_hex, "00010001");
    assert_eq!(parsed_response.flow_identifier, 0x7FEF911D);
    assert_eq!(parsed_response.field_at_offset_16_hex, "00010001");
    assert_eq!(parsed_response.field_at_offset_20_hex, "00000000");
    assert_eq!(parsed_response.raw_payload_hex, bytes_to_hex(&response));
}

#[test]
fn dante_brooklyn_control_protocol_flow_setup_parsers_reject_malformed_and_truncated_messages() {
    let request = decode_hexadecimal(
        "1102005000000100000000380000bb8000000018000100040048000100000000\
             000000240a000002001000430000000000000000000000004133322d30303030\
             3031003100000000080238010afe4e0b",
    );
    let response = decode_hexadecimal("1102001800000100000100017fef911d0001000100000000");

    for length in 0..request.len() {
        assert_eq!(
            parse_dante_brooklyn_control_protocol_flow_setup_request(&request[..length]),
            None
        );
    }
    for length in 0..response.len() {
        assert_eq!(
            parse_dante_brooklyn_control_protocol_flow_setup_response(&response[..length]),
            None
        );
    }

    let mut invalid_pointer = request.clone();
    invalid_pointer[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        parse_dante_brooklyn_control_protocol_flow_setup_request(&invalid_pointer),
        None
    );

    let mut invalid_descriptor_count = request;
    invalid_descriptor_count[26..28].copy_from_slice(&u16::MAX.to_be_bytes());
    assert_eq!(
        parse_dante_brooklyn_control_protocol_flow_setup_request(&invalid_descriptor_count),
        None
    );

    let mut invalid_response_length = response;
    invalid_response_length[2..4].copy_from_slice(&23u16.to_be_bytes());
    assert_eq!(
        parse_dante_brooklyn_control_protocol_flow_setup_response(&invalid_response_length),
        None
    );
}

#[test]
fn flow_parser_rejects_duplicate_records_and_truncated_channel_lists() {
    let mut duplicate = flow_query_response();
    duplicate[112..114].copy_from_slice(&1u16.to_be_bytes());
    assert_eq!(parse_tx_flows(&duplicate), None);

    let mut truncated = flow_query_response();
    truncated[126..128].copy_from_slice(&23u16.to_be_bytes());
    assert_eq!(parse_tx_flows(&truncated), None);

    let mut overlapping = flow_query_response();
    overlapping[14..16].copy_from_slice(&60u16.to_be_bytes());
    assert_eq!(parse_tx_flows(&overlapping), None);

    let mut invalid_max = flow_query_response();
    invalid_max[10] = 0;
    assert_eq!(parse_tx_flows(&invalid_max), None);

    let mut active_above_max = flow_query_response();
    active_above_max[10] = 1;
    active_above_max[11] = 2;
    assert_eq!(parse_tx_flows(&active_above_max), None);

    let mut flow_number_above_max = flow_query_response();
    flow_number_above_max[44..46].copy_from_slice(&33u16.to_be_bytes());
    assert_eq!(parse_tx_flows(&flow_number_above_max), None);

    let mut missing_pointer = flow_query_response();
    missing_pointer[14..16].copy_from_slice(&0u16.to_be_bytes());
    assert_eq!(parse_tx_flows(&missing_pointer), None);

    let mut oversized_encoding = flow_query_response();
    oversized_encoding[52..56].copy_from_slice(&65_536u32.to_be_bytes());
    assert_eq!(parse_tx_flows(&oversized_encoding), None);
}

fn studio_video_packet(name: &str) -> Vec<u8> {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/studio_video_modern_arc.json"
    ))
    .unwrap();
    decode_hexadecimal(fixture["packets"][name]["payload"].as_str().unwrap())
}

#[test]
fn modern_arc_280f_video_channel_pages_preserve_media_specific_data() {
    let transmitter = parse_modern_arc_transmitter_channel_status_page(&studio_video_packet(
        "transmitter_channel_response",
    ))
    .unwrap();
    let tx = &transmitter.records[0];
    assert_eq!(transmitter.protocol_id, PROTOCOL_ARC_280F);
    assert_eq!(tx.record_type_code, 0x1616);
    assert_eq!(tx.record_length_bytes, 44);
    assert_eq!(tx.media_type_code, MEDIA_TYPE_VIDEO);
    assert_eq!(tx.channel_name, "01");
    assert_eq!(
        tx.format_descriptor_hexadecimal,
        "02080000060000000000008200000000"
    );
    assert_eq!(tx.sample_rate, None);
    assert_eq!(tx.encoding, None);

    let receiver = parse_modern_arc_receiver_channel_status_page(&studio_video_packet(
        "receiver_channel_response",
    ))
    .unwrap();
    let rx = &receiver.records[0];
    assert_eq!(receiver.protocol_id, PROTOCOL_ARC_280F);
    assert_eq!(rx.record_type_code, 0x161C);
    assert_eq!(rx.record_length_bytes, 56);
    assert_eq!(rx.media_type_code, MEDIA_TYPE_VIDEO);
    assert_eq!(rx.source_channel_name.as_deref(), Some("01"));
    assert_eq!(rx.source_device_name.as_deref(), Some("studio-media-b"));
    assert_eq!(rx.subscription_status_code, 9);
    assert_eq!(rx.receiver_status_code, 0x0101);
    assert_eq!(rx.receiver_capability_flags, 6);
    assert!(!rx.can_subscribe_self);
    assert_eq!(rx.status_flags, None);
    assert_eq!(rx.sample_rate, None);
    assert_eq!(rx.encoding, None);
}

#[test]
fn modern_arc_280f_receiver_capability_mask_is_independent_of_record_variant() {
    let mut response = studio_video_packet("receiver_channel_response");
    let record_pointer = usize::from(read_u16(&response, MODERN_ARC_POINTER_TABLE_OFFSET).unwrap());
    response[record_pointer + 0x0c..record_pointer + 0x10]
        .copy_from_slice(&0x0100_0008u32.to_be_bytes());

    let record = &parse_modern_arc_receiver_channel_status_page(&response)
        .unwrap()
        .records[0];
    assert_eq!(record.receiver_capability_flags, 0x0100_0008);
    assert!(record.can_subscribe_self);
    assert_eq!(record.status_flags, None);
}

#[test]
fn modern_arc_280f_video_flow_pages_do_not_mislabel_the_format_as_audio() {
    let transmitter =
        parse_transmitter_flow_status_page(&studio_video_packet("transmitter_flow_response"))
            .unwrap();
    let tx = &transmitter.flows[0];
    assert_eq!(tx.media_type_code, MEDIA_TYPE_VIDEO);
    assert_eq!(tx.record_length_bytes, 74);
    assert_eq!(
        tx.format_descriptor_hexadecimal,
        "02080000060000000000008200000000"
    );
    assert_eq!(tx.sample_rate, None);
    assert_eq!(tx.encoding, None);
    assert_eq!(tx.channel_slot_count, None);

    let receiver =
        parse_modern_arc_receiver_flow_status_page(&studio_video_packet("receiver_flow_response"))
            .unwrap();
    let rx = &receiver.flows[0];
    assert_eq!(receiver.protocol_id, PROTOCOL_ARC_280F);
    assert_eq!(rx.record_type_code, 0x1626);
    assert_eq!(rx.record_length_bytes, 92);
    assert_eq!(rx.global_flow_id, 1);
    assert_eq!(rx.media_type_code, MEDIA_TYPE_VIDEO);
    assert_eq!(rx.media_local_flow_id, 1);
    assert_eq!(rx.flow_type_code, 0x8001);
    assert_eq!(
        rx.format_descriptor_hexadecimal,
        "02080000060000000000008200000000"
    );
    assert_eq!(rx.sample_rate, None);
    assert_eq!(rx.encoding, None);
    assert_eq!(rx.latency_nanoseconds, None);
    assert_eq!(rx.local_receiver_channel_count, 1);
    assert_eq!(rx.status_flags, 0x0400);
    assert_eq!(rx.status_code, 0x0101);
    assert_eq!(
        rx.receiver_mapping_descriptor_hexadecimal,
        "0001000200000100"
    );
}

#[test]
fn issue_59_receiver_flow_partial_page_preserves_result_and_disposition() {
    let response =
        include_bytes!("../../../../../tests/fixtures/issue_59/receiver_flow_partial.bin");
    assert_eq!(response.len(), 1400);
    let page = parse_modern_arc_receiver_flow_status_page(response).unwrap();
    assert_eq!(page.result_code, crate::protocol::RESULT_CODE_MORE_PAGES);
    assert_eq!(page.page_disposition, ModernArcPageDisposition::MorePages);
    assert_eq!(page.maximum_flow_slots, 16);
    assert_eq!(page.flows.len(), 15);
    let mut complete = response.to_vec();
    complete[8..10].copy_from_slice(&RESULT_CODE_SUCCESS.to_be_bytes());
    let complete_page = parse_modern_arc_receiver_flow_status_page(&complete).unwrap();
    assert_eq!(
        complete_page.page_disposition,
        ModernArcPageDisposition::Complete
    );
    assert_eq!(complete_page.flows, page.flows);
    for result in [0, 2, 0x30, 0xffff] {
        complete[8..10].copy_from_slice(&u16::to_be_bytes(result));
        assert_eq!(parse_modern_arc_receiver_flow_status_page(&complete), None);
    }
    for length in 0..response.len() {
        assert_eq!(
            parse_modern_arc_receiver_flow_status_page(&response[..length]),
            None
        );
    }
}
