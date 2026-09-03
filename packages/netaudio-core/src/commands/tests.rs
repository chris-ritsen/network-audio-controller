use super::*;
use crate::test_support::decode_hexadecimal;

#[test]
fn set_clock_source_uses_mask_bit_zero_and_raw_selection() {
    let packet =
        build_set_clock_source(0xDED4, [0x52, 0x55, 0x0A, 0x00, 0x02, 0x02], 0x0021).unwrap();
    assert_eq!(&packet[0x20..0x25], &[0x00, 0x01, 0xDE, 0xD4, 0x00]);
}

#[test]
fn set_clock_subdomain_uses_mask_bit_three_and_sixteen_byte_field() {
    let mut subdomain = [0u8; 16];
    subdomain[..5].copy_from_slice(&[0x74, 0x94, 0x11, 0x07, 0x01]);
    let packet =
        build_set_clock_subdomain(subdomain, [0x52, 0x55, 0x0A, 0x00, 0x02, 0x02], 0x0021).unwrap();
    assert_eq!(&packet[0x20..0x22], &[0x00, 0x08]);
    assert_eq!(&packet[0x28..0x38], &subdomain);
}

#[test]
fn refresh_clock_status_matches_shipping_controller_frame_7536() {
    let packet = build_refresh_clock_status([0x84, 0x2F, 0x57, 0x74, 0xE8, 0x6D], 0x0021).unwrap();
    assert_eq!(
            packet,
            decode_hexadecimal(
                "ffff005c00210000842f5774e86d0000417564696e617465073a002100000064000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            )
        );
}

#[test]
fn cmc_register_builder_matches_wire_layout() {
    let packet = build_cmc_register(0x1234, [0x00, 0x1D, 0xC1, 0x50, 0x23, 0x68]).unwrap();
    assert_eq!(
        packet,
        [
            0x12, 0x00, 0x00, 0x14, 0x12, 0x34, 0x10, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1D,
            0xC1, 0x50, 0x23, 0x68, 0x00, 0x00,
        ]
    );
}

#[test]
fn channel_count_query_carries_opcode_1000_with_an_empty_reserved_word() {
    let packet = build_channel_count(1).unwrap();
    assert_eq!(
        packet,
        [0x27, 0xFF, 0x00, 0x0A, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00]
    );
}

#[test]
fn receivers_query_requests_sixteen_channels_from_the_page_start() {
    let packet = build_receivers(0, 0x1234).unwrap();
    assert_eq!(
        packet,
        [
            0x27, 0xFF, 0x00, 0x10, 0x12, 0x34, 0x30, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x00, 0x00
        ]
    );
}

#[test]
fn receivers_builder_paginates() {
    let packet = build_receivers(2, 0).unwrap();
    assert_eq!(&packet[10..14], &[0x00, 0x01, 0x00, 33]);
}

#[test]
fn receivers_builder_rejects_page_overflow() {
    let last_page = build_receivers(4095, 0).unwrap();
    assert_eq!(&last_page[12..14], &[0xFF, 0xF1]);
    assert_eq!(build_receivers(4096, 0), Err(NetaudioError::InvalidPage));
    assert_eq!(
        build_receivers(u16::MAX, 0),
        Err(NetaudioError::InvalidPage)
    );
}

#[test]
fn transmitters_builder_queries_raw_pages_and_rejects_unbounded_friendly_queries() {
    let raw = build_transmitters(0, false, 0).unwrap();
    assert_eq!(&raw[6..8], &OPCODE_TX_CHANNEL_INFO.to_be_bytes());
    assert_eq!(
        build_transmitters(1, true, 0),
        Err(NetaudioError::InvalidChannel)
    );
}

#[test]
fn transmitter_names_builder_queries_the_full_channel_range() {
    assert_eq!(
        build_transmitter_names(2, 0x1234).unwrap(),
        [
            0x27, 0xFF, 0x00, 0x10, 0x12, 0x34, 0x20, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x00, 0x02,
        ]
    );
    assert_eq!(
        &build_transmitter_names(256, 0).unwrap()[10..],
        &[0x00, 0x01, 0x00, 0x01, 0x01, 0x00]
    );
    assert_eq!(
        build_transmitter_names(0, 0),
        Err(NetaudioError::InvalidChannel)
    );
}

#[test]
fn transmitters_builder_rejects_page_overflow() {
    let last_page = build_transmitters(2047, false, 0).unwrap();
    assert_eq!(&last_page[12..14], &[0xFF, 0xE1]);
    assert_eq!(
        build_transmitters(2048, false, 0),
        Err(NetaudioError::InvalidPage)
    );
    assert_eq!(
        build_transmitters(u16::MAX, true, 0),
        Err(NetaudioError::InvalidChannel)
    );
}

#[test]
fn add_subscriptions_layout_is_stable() {
    let packet = build_add_subscriptions(
        &[
            (1, "tx-a".to_owned(), "dev-a".to_owned()),
            (2, "tx-b".to_owned(), "dev-b".to_owned()),
        ],
        0,
    )
    .unwrap();
    assert_eq!(&packet[6..8], &OPCODE_SUBSCRIPTION_ADD.to_be_bytes());
    assert_eq!(&packet[8..12], &[0x00, 0x00, 0x02, 0x02]);
    assert_eq!(&packet[12..18], &[0x00, 0x01, 0x00, 52, 0x00, 57]);
    assert_eq!(&packet[18..24], &[0x00, 0x02, 0x00, 63, 0x00, 68]);
    assert!(packet[24..52].iter().all(|&byte| byte == 0));
    assert_eq!(&packet[52..], b"tx-a\x00dev-a\x00tx-b\x00dev-b\x00");
}

#[test]
fn add_subscriptions_accepts_firmware_reported_channel_labels() {
    let packet = build_add_subscriptions(
        &[(1, "Output 01".to_owned(), "A32-000005".to_owned())],
        0x1234,
    )
    .unwrap();
    assert_eq!(&packet[52..], b"Output 01\0A32-000005\0");
}

#[test]
fn transmit_channel_name_matches_controller_request() {
    let packet = build_set_channel_name(ChannelType::Tx, 1, "tett", 0x49A4).unwrap();
    assert_eq!(
        packet,
        vec![
            0x27, 0x29, 0x00, 0x1D, 0x49, 0xA4, 0x20, 0x13, 0x00, 0x00, 0x02, 0x01, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, b't', b'e', b't', b't',
            0x00,
        ]
    );
}

#[test]
fn receive_channel_name_page_2729_layout_is_stable() {
    let records: Vec<ReceiveChannelNamePageRecord> = (1..=32)
        .map(|rx_channel_number| ReceiveChannelNamePageRecord {
            rx_channel_number,
            name: format!("channel-{rx_channel_number:02}"),
        })
        .collect();
    let packet = build_receive_channel_name_page_2729(&records, 0x0749).unwrap();
    assert_eq!(packet.len(), 492);
    assert_eq!(
        &packet[..12],
        &[0x27, 0x29, 0x01, 0xEC, 0x07, 0x49, 0x30, 0x01, 0, 0, 32, 32]
    );
    for (index, record) in packet[12..140].chunks_exact(4).enumerate() {
        assert_eq!(u16::from_be_bytes([record[0], record[1]]), index as u16 + 1);
        assert_eq!(
            u16::from_be_bytes([record[2], record[3]]),
            0x008C + index as u16 * 11
        );
    }
    assert_eq!(&packet[140..151], b"channel-01\0");
    assert_eq!(&packet[481..], b"channel-32\0");
}

#[test]
fn receive_channel_name_page_2729_rejects_invalid_records() {
    let mut records: Vec<ReceiveChannelNamePageRecord> = (1..=32)
        .map(|rx_channel_number| ReceiveChannelNamePageRecord {
            rx_channel_number,
            name: format!("channel-{rx_channel_number:02}"),
        })
        .collect();
    assert_eq!(
        build_receive_channel_name_page_2729(&records[..31], 0),
        Err(NetaudioError::InvalidPage)
    );
    records[0].rx_channel_number = 0;
    assert_eq!(
        build_receive_channel_name_page_2729(&records, 0),
        Err(NetaudioError::InvalidChannel)
    );
    records[0].rx_channel_number = 2;
    assert_eq!(
        build_receive_channel_name_page_2729(&records, 0),
        Err(NetaudioError::InvalidChannel)
    );
    records[0].rx_channel_number = 1;
    records[0].name = "bad name".to_owned();
    assert_eq!(
        build_receive_channel_name_page_2729(&records, 0),
        Err(NetaudioError::NameInvalidChars)
    );
}

#[test]
fn subscription_page_2729_clear_layout_is_stable() {
    let records: Vec<SubscriptionPageRecord> = (1..=32)
        .map(|rx_channel_number| SubscriptionPageRecord::Clear { rx_channel_number })
        .collect();
    let packet = build_subscription_page_2729(&records, 0x0768).unwrap();
    assert_eq!(packet.len(), 652);
    assert_eq!(
        &packet[..12],
        &[0x27, 0x29, 0x02, 0x8C, 0x07, 0x68, 0x30, 0x10, 0, 0, 32, 32]
    );
    for (index, record) in packet[12..204].chunks_exact(6).enumerate() {
        assert_eq!(u16::from_be_bytes([record[0], record[1]]), index as u16 + 1);
        assert_eq!(&record[2..], &[0, 0, 0, 0]);
    }
    assert!(packet[204..].iter().all(|byte| *byte == 0));
}

#[test]
fn subscription_page_2729_interns_repeated_strings() {
    let records = [
        SubscriptionPageRecord::Set {
            rx_channel_number: 65,
            tx_channel_name: "65".to_owned(),
            tx_device_name: ".".to_owned(),
        },
        SubscriptionPageRecord::Set {
            rx_channel_number: 66,
            tx_channel_name: "66".to_owned(),
            tx_device_name: ".".to_owned(),
        },
    ];
    let packet = build_subscription_page_2729(&records, 0x1234).unwrap();
    assert_eq!(
        &packet[..12],
        &[0x27, 0x29, 0x02, 0x94, 0x12, 0x34, 0x30, 0x10, 0, 0, 32, 2]
    );
    assert_eq!(&packet[12..18], &[0, 65, 0x02, 0x8C, 0x02, 0x8F]);
    assert_eq!(&packet[18..24], &[0, 66, 0x02, 0x91, 0x02, 0x8F]);
    assert_eq!(&packet[652..], &[b'6', b'5', 0, b'.', 0, b'6', b'6', 0]);
}

#[test]
fn subscription_page_2729_rejects_invalid_record_sets() {
    assert_eq!(
        build_subscription_page_2729(&[], 0),
        Err(NetaudioError::SubscriptionCount)
    );
    let too_many: Vec<SubscriptionPageRecord> = (1..=33)
        .map(|rx_channel_number| SubscriptionPageRecord::Clear { rx_channel_number })
        .collect();
    assert_eq!(
        build_subscription_page_2729(&too_many, 0),
        Err(NetaudioError::SubscriptionCount)
    );
    let duplicate = [
        SubscriptionPageRecord::Clear {
            rx_channel_number: 1,
        },
        SubscriptionPageRecord::Clear {
            rx_channel_number: 1,
        },
    ];
    assert_eq!(
        build_subscription_page_2729(&duplicate, 0),
        Err(NetaudioError::InvalidSubscriptionChannel)
    );
    assert_eq!(
        build_subscription_page_2729(
            &[SubscriptionPageRecord::Clear {
                rx_channel_number: 0,
            }],
            0,
        ),
        Err(NetaudioError::InvalidSubscriptionChannel)
    );
}

#[test]
fn add_subscriptions_rejects_channels_that_do_not_fit_on_wire() {
    let result = build_add_subscriptions(&[(257, "tx-a".to_owned(), "dev-a".to_owned())], 0);
    assert_eq!(result, Err(NetaudioError::InvalidSubscriptionChannel));
}

#[test]
fn subscriptions_reject_zero_channels_and_invalid_string_table_entries() {
    assert_eq!(
        build_add_subscriptions(&[(0, "tx-a".to_owned(), "dev-a".to_owned())], 0),
        Err(NetaudioError::InvalidSubscriptionChannel)
    );
    assert_eq!(
        build_add_subscriptions(&[(1, "tx\0a".to_owned(), "dev-a".to_owned())], 0),
        Err(NetaudioError::NameInvalidChars)
    );
    assert_eq!(
        build_add_subscriptions(&[(1, "tx-a".to_owned(), "dev\0a".to_owned())], 0),
        Err(NetaudioError::NameInvalidChars)
    );
    assert!(build_add_subscriptions(&[(1, "tx-a".to_owned(), ".".to_owned())], 0).is_ok());
}

#[test]
fn remove_subscriptions_rejects_zero_channels_and_packet_overflow() {
    assert_eq!(
        build_remove_subscriptions(&[0], 0),
        Err(NetaudioError::InvalidChannel)
    );
    assert_eq!(
        build_remove_subscriptions(&vec![1; 16_381], 0),
        Err(NetaudioError::PacketTooLarge)
    );
}

#[test]
fn set_aes67_multicast_prefix_matches_controller_usb_write() {
    let packet =
        build_set_aes67_multicast_prefix(std::net::Ipv4Addr::new(239, 238, 0, 0), 0x0403).unwrap();
    assert_eq!(
        packet,
        decode_hexadecimal("28090014040311010000010180600010efee0000")
    );
    let restored =
        build_set_aes67_multicast_prefix(std::net::Ipv4Addr::new(239, 69, 0, 0), 0x00c0).unwrap();
    assert_eq!(
        restored,
        decode_hexadecimal("2809001400c011010000010180600010ef450000")
    );
}

#[test]
fn set_latency_matches_captured_250_microsecond_packet() {
    let packet = build_set_latency(0.25, 0).unwrap();
    assert_eq!(packet.len(), 40);
    assert_eq!(
        &packet[0..8],
        &[0x27, 0xFF, 0x00, 0x28, 0x00, 0x00, 0x11, 0x01]
    );
    assert_eq!(
        &packet[8..32],
        &[
            0x00, 0x00, 0x05, 0x04, 0x82, 0x05, 0x00, 0x20, 0x02, 0x11, 0x00, 0x04, 0x83, 0x01,
            0x00, 0x24, 0x03, 0x10, 0x00, 0x04, 0x83, 0x02, 0x83, 0x06,
        ]
    );
    assert_eq!(
        &packet[32..40],
        &[0x00, 0x03, 0xD0, 0x90, 0x00, 0x03, 0xD0, 0x90]
    );
}

#[test]
fn set_latency_can_select_the_managed_arc_protocol_without_changing_the_default() {
    let packet = build_set_latency_for_protocol(PROTOCOL_ARC_2809, 2.0, 0x4C11).unwrap();
    assert_eq!(
        &packet[..10],
        &[0x28, 0x09, 0x00, 0x28, 0x4C, 0x11, 0x11, 0x01, 0x00, 0x00]
    );
    assert_eq!(
        &packet[32..40],
        &[0x00, 0x1E, 0x84, 0x80, 0x00, 0x1E, 0x84, 0x80]
    );
    assert_eq!(&build_set_latency(2.0, 0x4C11).unwrap()[..2], &[0x27, 0xFF]);
    assert_eq!(
        build_set_latency_for_protocol(0x2729, 2.0, 0x4C11),
        Err(NetaudioError::UnsupportedProtocolOperation)
    );
}

#[test]
fn set_latency_preserves_full_nanosecond_high_byte() {
    let packet = build_set_latency(20.3125, 0).unwrap();
    assert_eq!(
        &packet[32..40],
        &[0x01, 0x35, 0xF1, 0xB4, 0x01, 0x35, 0xF1, 0xB4]
    );
}

#[test]
fn set_latency_rejects_values_that_cannot_be_encoded() {
    for latency in [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -0.001,
        MAX_LATENCY_MILLISECONDS + 0.001,
    ] {
        assert_eq!(
            build_set_latency(latency, 0),
            Err(NetaudioError::InvalidLatency),
            "{latency:?}"
        );
    }

    let maximum = build_set_latency(MAX_LATENCY_MILLISECONDS, 0).unwrap();
    assert_eq!(&maximum[32..36], &u32::MAX.to_be_bytes());
}

#[test]
fn audio_settings_accept_nonzero_wire_values_without_truncation() {
    for sample_rate in [44_100, 48_000, 192_000, 123_456, u32::MAX] {
        assert!(
            build_set_sample_rate(sample_rate, 1).is_ok(),
            "{sample_rate}"
        );
    }
    assert_eq!(
        &build_set_sample_rate(u32::MAX, 1).unwrap()[36..40],
        &u32::MAX.to_be_bytes()
    );
    assert_eq!(
        build_set_sample_rate(0, 1),
        Err(NetaudioError::InvalidSampleRate)
    );
    assert_eq!(
        &build_set_sample_rate(48_000, 0x18B1).unwrap()[4..6],
        &0x18B1u16.to_be_bytes()
    );
    assert_eq!(
        build_set_sample_rate(48_000, 0),
        Err(NetaudioError::InvalidSequence)
    );

    for encoding in [1, 16, 24, 32, 256, u32::MAX] {
        assert!(build_set_encoding(encoding, 1).is_ok(), "{encoding}");
    }
    assert_eq!(
        &build_set_encoding(u32::MAX, 1).unwrap()[36..40],
        &u32::MAX.to_be_bytes()
    );
    assert_eq!(
        build_set_encoding(0, 1),
        Err(NetaudioError::InvalidEncoding)
    );
    assert_eq!(
        build_set_encoding(24, 0),
        Err(NetaudioError::InvalidSequence)
    );

    assert_eq!(
        build_set_gain_level([1, 2, 3, 4, 5, 6], 1, 0, 1, true),
        Err(NetaudioError::InvalidChannel)
    );
    for level in [0, 6, u8::MAX] {
        assert_eq!(
            build_set_gain_level([1, 2, 3, 4, 5, 6], 1, 1, level, true),
            Err(NetaudioError::InvalidGainLevel),
            "{level}"
        );
    }
    for level in MIN_GAIN_LEVEL..=MAX_GAIN_LEVEL {
        assert!(
            build_set_gain_level([1, 2, 3, 4, 5, 6], 1, u16::MAX, level, false).is_ok(),
            "{level}"
        );
    }
}

#[test]
fn identify_uses_caller_transaction_sequence() {
    assert_eq!(
        build_identify(0x0BC9).unwrap(),
        decode_hexadecimal("ffff00200bc900000000000000000000417564696e6174650731006300000064")
    );
    assert_eq!(build_identify(0), Err(NetaudioError::InvalidSequence));
}

#[test]
fn sample_rate_pullup_control_matches_authentic_a32_requests() {
    let host_mac = [0x52, 0x55, 0x0A, 0x00, 0x02, 0x02];
    assert_eq!(
            build_probe_sample_rate_pullup(host_mac, 0x0047).unwrap(),
            decode_hexadecimal("ffff00380047000052550a0002020000417564696e617465073a008500000000000000000000000000000000000000000000000000000000")
        );
    assert_eq!(
            build_set_sample_rate_pullup(host_mac, 0x0047, 1).unwrap(),
            decode_hexadecimal("ffff00380047000052550a0002020000417564696e617465073a008500000000000000010000000100000000000000000000000000000000")
        );
    assert_eq!(
        build_probe_sample_rate_pullup(host_mac, 0),
        Err(NetaudioError::InvalidSequence)
    );
    assert_eq!(
        build_set_sample_rate_pullup(host_mac, 0, 1),
        Err(NetaudioError::InvalidSequence)
    );
}

#[test]
fn probe_gain_level_matches_captured_input_packet_716() {
    let packet = build_probe_gain_level([0x84, 0x2F, 0x57, 0x74, 0xE8, 0x6D], 0x045A).unwrap();
    assert_eq!(
        packet,
        [
            0xFF, 0xFF, 0x00, 0x28, 0x04, 0x5A, 0x00, 0x00, 0x84, 0x2F, 0x57, 0x74, 0xE8, 0x6D,
            0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x3A, 0x10, 0x0A,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]
    );
}

#[test]
fn set_gain_level_matches_captured_input_packet_1372() {
    let packet =
        build_set_gain_level([0x84, 0x2F, 0x57, 0x74, 0xE8, 0x6D], 0xC001, 1, 4, true).unwrap();
    assert_eq!(
        packet,
        [
            0xFF, 0xFF, 0x00, 0x34, 0xC0, 0x01, 0x00, 0x00, 0x84, 0x2F, 0x57, 0x74, 0xE8, 0x6D,
            0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x3A, 0x10, 0x0A,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x0C, 0x00, 0x10, 0x01, 0x02,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04,
        ]
    );
}

#[test]
fn set_gain_level_encodes_output_direction_and_full_channel_number() {
    let packet = build_set_gain_level([1, 2, 3, 4, 5, 6], 0x1234, 257, 5, false).unwrap();
    assert_eq!(&packet[40..42], &0x0201u16.to_be_bytes());
    assert_eq!(&packet[46..48], &257u16.to_be_bytes());
    assert_eq!(&packet[48..52], &5u32.to_be_bytes());
}

#[test]
fn probe_sample_rate_matches_captured_packet_4170622() {
    let packet = build_probe_sample_rate([0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24], 0x0042).unwrap();
    assert_eq!(
        packet,
        [
            0xFF, 0xFF, 0x00, 0x28, 0x00, 0x42, 0x00, 0x00, 0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24,
            0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x3A, 0x00, 0x81,
            0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]
    );
}

#[test]
fn probe_encoding_matches_captured_packet_204680() {
    let packet = build_probe_encoding([0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24], 0x985A).unwrap();
    assert_eq!(
        packet,
        [
            0xFF, 0xFF, 0x00, 0x28, 0x98, 0x5A, 0x00, 0x00, 0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24,
            0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x3A, 0x00, 0x83,
            0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]
    );
}

#[test]
fn query_tx_flows_selects_opcode_per_protocol() {
    let legacy = build_query_tx_flows(0x2729, 7).unwrap();
    assert_eq!(
        legacy,
        [
            0x27, 0x29, 0x00, 0x10, 0x00, 0x07, 0x22, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x00, 0x00,
        ]
    );

    let legacy_2801 = build_query_tx_flows(0x2801, 7).unwrap();
    assert_eq!(&legacy_2801[6..8], &OPCODE_QUERY_TX_FLOWS.to_be_bytes());

    let later_page = build_query_tx_flows_from(0x2729, 29, 7).unwrap();
    assert_eq!(&later_page[10..], &[0x00, 0x01, 0x00, 0x1D, 0x00, 0x00]);

    let status_frontend = build_query_tx_flows(PROTOCOL_ARC_2809, 0x292C).unwrap();
    assert_eq!(
        status_frontend,
        [
            0x28, 0x09, 0x00, 0x22, 0x29, 0x2C, 0x26, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]
    );
}

#[test]
fn receiver_channel_status_2809_query_matches_shipping_controller_request() {
    assert_eq!(
        build_query_receiver_channel_status_2809(0x284A).unwrap(),
        decode_hexadecimal("28090022284a34000000000000000000000100010001000000000000830283060310")
    );
}

#[test]
fn transmitter_channel_status_2809_query_matches_shipping_controller_request() {
    assert_eq!(
        build_query_transmitter_channel_status_2809(0x2852).unwrap(),
        decode_hexadecimal("28090022285224000000000000000000000100010001000000000000830283060310")
    );
}

#[test]
fn receiver_flow_status_2809_query_matches_shipping_controller_request() {
    assert_eq!(
        build_query_receiver_flow_status_2809(0x2856).unwrap(),
        decode_hexadecimal("28090022285636000000000000000000000100010001000000000000830283060310")
    );
}

#[test]
fn receiver_channel_name_2809_builder_matches_shipping_controller_requests() {
    assert_eq!(
        build_set_receiver_channel_name_2809(1, "01", 0x2849).unwrap(),
        decode_hexadecimal("2809001d2849340100000000000000000600010100010003001a303100")
    );
    assert_eq!(
        build_set_channel_name_for_protocol(
            PROTOCOL_ARC_2809,
            ChannelType::Rx,
            1,
            "mic-mix",
            0x284C,
        )
        .unwrap(),
        decode_hexadecimal("28090022284c340100000000000000000600010100010003001a6d69632d6d697800")
    );
    assert_eq!(
        build_set_channel_name_for_protocol(
            PROTOCOL_ARC_2809,
            ChannelType::Tx,
            2,
            "tv-probe2",
            0x0411,
        )
        .unwrap(),
        decode_hexadecimal("28090022041120130000020100000002001800000000000074762d70726f62653200")
    );
    assert_eq!(
        build_set_receiver_channel_name_2809(0, "rx-a", 0),
        Err(NetaudioError::InvalidChannel)
    );
}

#[test]
fn query_receiver_flows_matches_shipping_controller_request() {
    let packet = build_query_receiver_flows(1, 0x033A).unwrap();
    assert_eq!(
        packet,
        [
            0x27, 0x29, 0x00, 0x10, 0x03, 0x3A, 0x32, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x00, 0x00,
        ]
    );
    assert_eq!(
        build_query_receiver_flows(0, 0x033A),
        Err(NetaudioError::InvalidFlowSlot)
    );
}

#[test]
fn query_transmit_channel_capabilities_matches_shipping_controller_request() {
    assert_eq!(
        build_query_transmit_channel_capabilities(1, 0, 0x0329).unwrap(),
        [
            0x27, 0x29, 0x00, 0x10, 0x03, 0x29, 0x20, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x00, 0x00,
        ]
    );
    assert_eq!(
        build_query_transmit_channel_capabilities(33, 32, 0x0329).unwrap(),
        [
            0x27, 0x29, 0x00, 0x10, 0x03, 0x29, 0x20, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x21,
            0x00, 0x20,
        ]
    );
    assert_eq!(
        build_query_transmit_channel_capabilities(0, 0, 0x0329),
        Err(NetaudioError::InvalidChannel)
    );
}

#[test]
fn query_receiver_port_ranges_matches_shipping_controller_request() {
    assert_eq!(
        build_query_receiver_port_ranges(0x033C).unwrap(),
        [0x27, 0x29, 0x00, 0x0A, 0x03, 0x3C, 0x33, 0x00, 0x00, 0x00]
    );
}

#[test]
fn managed_status_queries_match_the_captured_2809_packets() {
    // Explicit Status refresh frames 523, 535, 543, 567, and 597 from
    // protocol-research run ddm-dapi-arc-read-20260902-01. Source PCAP
    // SHA-256: fda27481bd7cd64f1450b09d29c88daa6e27c68235b353ccd9adceeb4a512954.
    assert_eq!(
        build_channel_count_for_protocol(PROTOCOL_ARC_2809, 0x0072).unwrap(),
        decode_hexadecimal("2809000a007210000000")
    );
    assert_eq!(
        build_property_directory_for_protocol(PROTOCOL_ARC_2809, 0x0073).unwrap(),
        decode_hexadecimal("2809000a007311020000")
    );
    assert_eq!(
        build_device_info_for_protocol(PROTOCOL_ARC_2809, 0x0074).unwrap(),
        decode_hexadecimal("2809000a007410030000")
    );
    assert_eq!(
        build_transmitter_names_for_protocol(PROTOCOL_ARC_2809, 2, 0x0076).unwrap(),
        decode_hexadecimal("28090010007620100000000100010002")
    );
    assert_eq!(
        build_query_receiver_port_ranges_for_protocol(PROTOCOL_ARC_2809, 0x0079).unwrap(),
        decode_hexadecimal("2809000a007933000000")
    );

    for invalid_protocol in [0, 0x2729, 0x2801, 0x280F, u16::MAX] {
        assert_eq!(
            build_channel_count_for_protocol(invalid_protocol, 1),
            Err(NetaudioError::UnsupportedProtocolOperation)
        );
    }
    assert_eq!(
        build_query_receiver_port_ranges_for_protocol(0x27FF, 1),
        Err(NetaudioError::UnsupportedProtocolOperation)
    );
}

#[test]
fn flow_builders_reject_unknown_protocols() {
    for protocol in [0, 0x2728, 0x2800, 0x2808, u16::MAX] {
        assert_eq!(
            build_query_tx_flows(protocol, 0),
            Err(NetaudioError::InvalidFlowProtocol),
            "{protocol:#06x}"
        );
    }
    for protocol in [0, 0x2728, 0x2800, 0x2808, u16::MAX] {
        assert_eq!(
            build_create_tx_flow(protocol, 1, &[1], 0),
            Err(NetaudioError::InvalidFlowProtocol),
            "{protocol:#06x}"
        );
        assert_eq!(
            build_delete_tx_flow(protocol, 1, 0),
            Err(NetaudioError::InvalidFlowProtocol),
            "{protocol:#06x}"
        );
    }
    assert_eq!(
        build_create_tx_flow(PROTOCOL_ARC_2809, 2, &[1], 0),
        Err(NetaudioError::InvalidFlowProtocol)
    );
}

#[test]
fn query_tx_flows_rejects_starting_slots_outside_device_range() {
    for starting_flow in [0, 33, u16::MAX] {
        assert_eq!(
            build_query_tx_flows_from(PROTOCOL_DANTE_FLOW, starting_flow, 0),
            Err(NetaudioError::InvalidFlowSlot),
            "{starting_flow}"
        );
    }
    assert_eq!(
        build_query_tx_flows_from(PROTOCOL_ARC_2809, 2, 0),
        Err(NetaudioError::InvalidFlowSlot)
    );
}

#[test]
fn create_tx_flow_matches_captured_layout() {
    let packet = build_create_tx_flow(0x2729, 1, &[1, 2], 0).unwrap();
    let mut expected = vec![0x27, 0x29, 0x00, 0x3C, 0x00, 0x00, 0x22, 0x01, 0x00, 0x00];
    expected.extend_from_slice(&[0x01, 0x01, 0x00, 0x10]);
    expected.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    expected.extend_from_slice(&[0x00, 0x02]);
    expected.extend(std::iter::repeat_n(0, 10));
    expected.extend_from_slice(&[0x00, 0x02, 0x00, 0x01, 0x00, 0x02]);
    expected.extend_from_slice(&[0x00, 0x28]);
    expected.extend_from_slice(&[0x00, 0x00, 0x0a, 0x00]);
    expected.extend(std::iter::repeat_n(0, 14));
    expected.extend_from_slice(&[0x00, 0x01, 0x00, 0x00]);
    assert_eq!(packet, expected);
}

#[test]
fn create_tx_flow_rejects_invalid_channels_and_packet_overflow() {
    assert_eq!(
        build_create_tx_flow(0x2729, 1, &[], 0),
        Err(NetaudioError::InvalidChannel)
    );
    assert_eq!(
        build_create_tx_flow(0x2729, 1, &[0], 0),
        Err(NetaudioError::InvalidChannel)
    );
    assert_eq!(
        build_create_tx_flow(0x2729, 1, &[1, 1], 0),
        Err(NetaudioError::InvalidChannel)
    );
    assert_eq!(
        build_create_tx_flow(0x2729, 1, &vec![1; 32_740], 0),
        Err(NetaudioError::PacketTooLarge)
    );
}

#[test]
fn delete_tx_flow_2809_matches_shipping_controller_slot_two_request() {
    assert_eq!(
        build_delete_tx_flow(PROTOCOL_ARC_2809, 2, 0x1602).unwrap(),
        [
            0x28, 0x09, 0x00, 0x22, 0x16, 0x02, 0x26, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]
    );
    assert_eq!(
        build_delete_tx_flow(PROTOCOL_ARC_2809, 1, 0x1602),
        Err(NetaudioError::InvalidFlowSlot)
    );
}

#[test]
fn flow_mutations_reject_slots_outside_device_range() {
    for slot in [0, 33, u16::MAX] {
        assert_eq!(
            build_create_tx_flow(PROTOCOL_DANTE_FLOW, slot, &[1], 0),
            Err(NetaudioError::InvalidFlowSlot),
            "{slot}"
        );
        assert_eq!(
            build_delete_tx_flow(PROTOCOL_DANTE_FLOW, slot, 0),
            Err(NetaudioError::InvalidFlowSlot),
            "{slot}"
        );
    }

    for slot in 1..=32 {
        assert!(build_create_tx_flow(PROTOCOL_DANTE_FLOW_2801, slot, &[1], 0).is_ok());
        assert!(build_delete_tx_flow(PROTOCOL_DANTE_FLOW, slot, 0).is_ok());
    }
}

#[test]
fn channel_mutations_reject_channel_zero() {
    assert_eq!(
        build_reset_channel_name(ChannelType::Rx, 0, 0),
        Err(NetaudioError::InvalidChannel)
    );
    assert_eq!(
        build_set_channel_name(ChannelType::Tx, 0, "tx-a", 0),
        Err(NetaudioError::InvalidChannel)
    );
}

#[test]
fn volume_builder_rejects_unrepresentable_names_before_constructing_offsets() {
    assert_eq!(
        build_volume_start(&"a".repeat(65_521), [0; 4], [0; 6], 0, false, 0),
        Err(NetaudioError::NameTooLong)
    );
    assert_eq!(
        build_volume_start("dev\0name", [0; 4], [0; 6], 0, false, 0),
        Err(NetaudioError::NameInvalidChars)
    );
}

#[test]
fn metering_start_matches_captured_ad4d_packet_7298186() {
    assert_eq!(
            build_volume_start(
                "ad4d",
                [192, 168, 1, 156],
                [0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24],
                8752,
                true,
                0,
            )
            .unwrap(),
            decode_hexadecimal(
                "1200004200003010000000003e42274cff2400000004001000020012000a6164346400000000000100160001223000010000c0a8019c223000000000000000000000"
            )
        );
}

#[test]
fn metering_start_matches_captured_a32_packet_7298185() {
    assert_eq!(
            build_volume_start(
                "a32",
                [192, 168, 1, 156],
                [0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24],
                8752,
                true,
                0,
            )
            .unwrap(),
            decode_hexadecimal(
                "1200004000003010000000003e42274cff2400000004000e00020010000a613332000000000100140001223000010000c0a8019c223000000000000000000000"
            )
        );
}

#[test]
fn delete_tx_flow_2729_encodes_flow_slot_after_a_unit_count() {
    let packet = build_delete_tx_flow(0x2729, 3, 0).unwrap();
    assert_eq!(
        packet,
        [
            0x27, 0x29, 0x00, 0x10, 0x00, 0x00, 0x22, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x03
        ]
    );
}

mod device_management;
