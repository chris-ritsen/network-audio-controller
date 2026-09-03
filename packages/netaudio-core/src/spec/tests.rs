use super::*;

const TEST_HOST_MAC: [u8; 6] = [0x00, 0x11, 0x22, 0x33, 0x44, 0x55];

fn routed_with_assigned_id(json: &str, host_mac: [u8; 6], assigned_id: u16) -> Routed {
    build_routed_command(json, Some(host_mac), || assigned_id).unwrap()
}

fn route_for(json: &str) -> (Target, IoMode) {
    let routed = routed_with_assigned_id(json, TEST_HOST_MAC, 1);
    (routed.target, routed.io)
}

#[test]
fn conmon_export_commands_fire_to_the_settings_port_without_waiting() {
    for json in [
        r#"{"command":"device_log_export"}"#,
        r#"{"command":"capability_partition_export"}"#,
    ] {
        assert_eq!(
            route_for(json),
            (
                Target::Settings,
                IoMode::Fire {
                    repeat: 1,
                    interval_ms: 0
                }
            ),
            "{json}"
        );
    }
}

#[test]
fn representative_commands_keep_their_routes() {
    assert_eq!(
        route_for(r#"{"command":"channel_count"}"#),
        (Target::Arc, IoMode::Request)
    );
    assert_eq!(
        route_for(r#"{"command":"identify"}"#),
        (
            Target::Settings,
            IoMode::Fire {
                repeat: 1,
                interval_ms: 0
            }
        )
    );
    assert_eq!(
        route_for(r#"{"command":"enable_aes67","enabled":true}"#),
        (
            Target::Settings,
            IoMode::Fire {
                repeat: 3,
                interval_ms: 100
            }
        )
    );
    assert_eq!(
        route_for(r#"{"command":"set_clock_source","clock_source":1}"#),
        (
            Target::Settings,
            IoMode::Fire {
                repeat: 3,
                interval_ms: 500
            }
        )
    );
    assert_eq!(
        route_for(r#"{"command":"cmc_register","sequence":1,"host_mac":"001122334455"}"#),
        (Target::Control, IoMode::Request)
    );
    assert_eq!(
        route_for(r#"{"command":"metering_stop","device_name":"avio","mac":"001122334455"}"#),
        (
            Target::Control,
            IoMode::Fire {
                repeat: 1,
                interval_ms: 0
            }
        )
    );
}

#[test]
fn default_host_mac_fills_in_when_omitted() {
    let mac = [0x0c, 0x9d, 0x92, 0xc5, 0x12, 0xf8];
    let routed = routed_with_assigned_id("{\"command\":\"reboot\"}", mac, 1);
    assert_eq!(&routed.packet[8..14], &mac);
}

#[test]
fn omitted_message_id_is_assigned_and_explicit_ids_are_honored() {
    let assigned = routed_with_assigned_id("{\"command\":\"identify\"}", [0xFF; 6], 0x0C01);
    assert_eq!(assigned.message_id, 0x0C01);
    assert_eq!(
        assigned.packet,
        crate::commands::build_identify(0x0C01).unwrap()
    );
    for json in [
        "{\"command\":\"identify\",\"message_id\":3017}",
        "{\"command\":\"identify\",\"sequence\":3017}",
        "{\"command\":\"identify\",\"transaction_id\":3017}",
    ] {
        let explicit = routed_with_assigned_id(json, [0xFF; 6], 0x0C01);
        assert_eq!(explicit.message_id, 0x0BC9, "{json}");
        assert_eq!(
            explicit.packet,
            crate::commands::build_identify(0x0BC9).unwrap(),
            "{json}"
        );
    }
    let arc = routed_with_assigned_id("{\"command\":\"device_info\"}", [0xFF; 6], 0x0C02);
    assert_eq!(arc.message_id, 0x0C02);
    assert_eq!(
        arc.packet,
        crate::commands::build_device_info(0x0C02).unwrap()
    );
    let zero = routed_with_assigned_id(
        "{\"command\":\"device_info\",\"transaction_id\":0}",
        [0xFF; 6],
        0x0C03,
    );
    assert_eq!(zero.message_id, 0x0C03);
}

#[test]
fn commands_without_a_message_id_field_never_consume_one() {
    let routed = build_routed_command(
        "{\"command\":\"make_model\",\"mac\":\"001122334455\"}",
        None,
        || panic!("make_model has no message id"),
    )
    .unwrap();
    assert_eq!(routed.message_id, 0);
}

#[test]
fn standalone_builder_requires_a_message_id_for_conmon_writes() {
    assert!(matches!(
        build_command_from_json("{\"command\":\"reboot\",\"host_mac\":\"001122334455\"}"),
        Err(SpecError::Protocol(NetaudioError::InvalidSequence))
    ));
    assert!(build_command_from_json(
        "{\"command\":\"reboot\",\"host_mac\":\"001122334455\",\"message_id\":9}"
    )
    .is_ok());
    assert!(build_command_from_json("{\"command\":\"device_info\"}").is_ok());
}

#[test]
fn unknown_fields_are_rejected_with_the_serde_message() {
    let error = build_command_from_json("{\"command\":\"identify\",\"sequenc\":1}").unwrap_err();
    match error {
        SpecError::InvalidJson(message) => {
            assert!(message.contains("unknown field `sequenc`"), "{message}")
        }
        other => panic!("unexpected error {other:?}"),
    }
    let nested = build_command_from_json(
        "{\"command\":\"add_subscriptions\",\"subscriptions\":[{\"rx_channel\":1,\"tx_channel\":\"a\",\"tx_device\":\"b\",\"extra\":1}]}",
    )
    .unwrap_err();
    assert!(
        matches!(nested, SpecError::InvalidJson(message) if message.contains("unknown field `extra`"))
    );
}

#[test]
fn explicit_host_mac_overrides_default() {
    let routed = routed_with_assigned_id(
        "{\"command\":\"reboot\",\"host_mac\":\"001122334455\"}",
        [0xFF; 6],
        1,
    );
    assert_eq!(&routed.packet[8..14], &[0x00, 0x11, 0x22, 0x33, 0x44, 0x55]);
}

#[test]
fn standalone_builder_rejects_omitted_host_mac() {
    assert!(matches!(
        build_command_from_json("{\"command\":\"reboot\"}"),
        Err(SpecError::InvalidMac)
    ));
}

#[test]
fn probe_encoding_keeps_its_message_type_independent_of_the_message_id() {
    let routed = routed_with_assigned_id(
        r#"{"command":"probe_encoding"}"#,
        [0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24],
        0x4321,
    );
    assert_eq!(&routed.packet[4..6], &0x4321u16.to_be_bytes());
    assert_eq!(&routed.packet[26..28], &0x0083u16.to_be_bytes());
}

#[test]
fn refresh_clock_status_preserves_the_requested_sequence_and_mac() {
    let host_mac = [0x84, 0x2F, 0x57, 0x74, 0xE8, 0x6D];
    let routed = routed_with_assigned_id(
        r#"{"command":"refresh_clock_status","sequence":33}"#,
        host_mac,
        1,
    );
    assert_eq!(
        routed.packet,
        commands::build_refresh_clock_status(host_mac, 33).unwrap()
    );
    assert_eq!(&routed.packet[4..6], &33u16.to_be_bytes());
    assert_eq!(&routed.packet[8..14], &host_mac);
    assert_eq!(
        routed.io,
        IoMode::Fire {
            repeat: 1,
            interval_ms: 0
        }
    );
}

#[test]
fn sample_rate_pullup_commands_use_the_authentic_wire_contract() {
    let host_mac = [0x72, 0xE7, 0x8A, 0x7B, 0x8D, 0x82];
    let probe = routed_with_assigned_id(
        r#"{"command":"probe_sample_rate_pullup"}"#,
        host_mac,
        0x0085,
    );
    assert_eq!(
        probe.packet,
        commands::build_probe_sample_rate_pullup(host_mac, 0x0085).unwrap()
    );

    let write = routed_with_assigned_id(
        r#"{"command":"set_sample_rate_pullup","raw_value":4,"sequence":71}"#,
        host_mac,
        1,
    );
    assert_eq!(
        write.packet,
        commands::build_set_sample_rate_pullup(host_mac, 71, 4).unwrap()
    );
}

#[test]
fn transmitter_names_command_requires_and_encodes_the_full_range() {
    let packet = build_command_from_json(
        r#"{"command":"transmitter_names","channel_count":256,"transaction_id":4660}"#,
    )
    .unwrap();
    assert_eq!(
        packet,
        [
            0x27, 0xFF, 0x00, 0x10, 0x12, 0x34, 0x20, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x01, 0x00,
        ]
    );
    assert!(matches!(
        build_command_from_json(r#"{"command":"transmitters","page":0,"friendly_names":true}"#),
        Err(SpecError::Protocol(NetaudioError::InvalidChannel))
    ));
}

#[test]
fn managed_status_commands_select_2809_explicitly_without_changing_defaults() {
    let cases = [
        (
            r#"{"command":"channel_count","protocol_id":10249,"transaction_id":114}"#,
            "2809000a007210000000",
        ),
        (
            r#"{"command":"property_directory","protocol_id":10249,"transaction_id":115}"#,
            "2809000a007311020000",
        ),
        (
            r#"{"command":"device_info","protocol_id":10249,"transaction_id":116}"#,
            "2809000a007410030000",
        ),
        (
            r#"{"command":"transmitter_names","protocol_id":10249,"channel_count":2,"transaction_id":118}"#,
            "28090010007620100000000100010002",
        ),
        (
            r#"{"command":"query_receiver_port_ranges","protocol_id":10249,"transaction_id":121}"#,
            "2809000a007933000000",
        ),
    ];
    for (specification, expected_hex) in cases {
        assert_eq!(
            build_command_from_json(specification).unwrap(),
            (0..expected_hex.len())
                .step_by(2)
                .map(|index| u8::from_str_radix(&expected_hex[index..index + 2], 16).unwrap())
                .collect::<Vec<_>>()
        );
    }

    assert_eq!(
        &build_command_from_json(r#"{"command":"channel_count","transaction_id":114}"#).unwrap()
            [..2],
        &[0x27, 0xFF]
    );
    assert_eq!(
        &build_command_from_json(
            r#"{"command":"query_receiver_port_ranges","transaction_id":121}"#,
        )
        .unwrap()[..2],
        &[0x27, 0x29]
    );

    let managed_latency_write = build_command_from_json(
        r#"{"command":"set_latency","latency":2.0,"protocol_id":10249,"transaction_id":19473}"#,
    )
    .unwrap();
    assert_eq!(
        &managed_latency_write[..10],
        &[0x28, 0x09, 0x00, 0x28, 0x4C, 0x11, 0x11, 0x01, 0x00, 0x00]
    );
    assert_eq!(
        &managed_latency_write[32..40],
        &[0x00, 0x1E, 0x84, 0x80, 0x00, 0x1E, 0x84, 0x80]
    );
    assert_eq!(
        &build_command_from_json(
            r#"{"command":"set_latency","latency":2.0,"transaction_id":19473}"#
        )
        .unwrap()[..2],
        &[0x27, 0xFF]
    );
}

#[test]
fn receiver_flow_query_command_matches_shipping_controller() {
    let packet = build_command_from_json(
        r#"{"command":"query_receiver_flows","starting_flow":1,"transaction_id":826}"#,
    )
    .unwrap();
    assert_eq!(
        packet,
        [
            0x27, 0x29, 0x00, 0x10, 0x03, 0x3A, 0x32, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x00, 0x00,
        ]
    );
}

#[test]
fn receiver_channel_2809_commands_match_shipping_controller() {
    let transmitter_query = build_command_from_json(
        r#"{"command":"query_transmitter_channel_status_2809","transaction_id":10322}"#,
    )
    .unwrap();
    assert_eq!(
        transmitter_query,
        [
            0x28, 0x09, 0x00, 0x22, 0x28, 0x52, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x83, 0x02, 0x83, 0x06, 0x03, 0x10,
        ]
    );

    let query = build_command_from_json(
        r#"{"command":"query_receiver_channel_status_2809","transaction_id":10314}"#,
    )
    .unwrap();
    assert_eq!(
        query,
        [
            0x28, 0x09, 0x00, 0x22, 0x28, 0x4A, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x83, 0x02, 0x83, 0x06, 0x03, 0x10,
        ]
    );

    let receiver_flow_query = build_command_from_json(
        r#"{"command":"query_receiver_flow_status_2809","transaction_id":10326}"#,
    )
    .unwrap();
    assert_eq!(
        receiver_flow_query,
        [
            0x28, 0x09, 0x00, 0x22, 0x28, 0x56, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x83, 0x02, 0x83, 0x06, 0x03, 0x10,
        ]
    );

    let rename = build_command_from_json(
            r#"{"command":"set_channel_name","channel_type":"rx","channel_number":1,"name":"mic-mix","protocol_id":10249,"transaction_id":10316}"#,
        )
        .unwrap();
    assert_eq!(
        rename,
        [
            0x28, 0x09, 0x00, 0x22, 0x28, 0x4C, 0x34, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x06, 0x00, 0x01, 0x01, 0x00, 0x01, 0x00, 0x03, 0x00, 0x1A, 0x6D, 0x69,
            0x63, 0x2D, 0x6D, 0x69, 0x78, 0x00,
        ]
    );

    let reconciliation = build_command_from_json(
        r#"{"command":"reconcile_transmitter_channel_names_2809","records":[{"channel_number":1,"name":"vrroom:left"},{"channel_number":2,"name":"vrroom:right"}],"transaction_id":18956}"#,
    )
    .unwrap();
    assert_eq!(
        reconciliation,
        [
            0x28, 0x09, 0x00, 0x39, 0x4A, 0x0C, 0x24, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x06, 0x00, 0x02, 0x02, 0x00, 0x01, 0x00, 0x03, 0x00, 0x20, 0x00, 0x02,
            0x00, 0x03, 0x00, 0x2C, 0x76, 0x72, 0x72, 0x6F, 0x6F, 0x6D, 0x3A, 0x6C, 0x65, 0x66,
            0x74, 0x00, 0x76, 0x72, 0x72, 0x6F, 0x6F, 0x6D, 0x3A, 0x72, 0x69, 0x67, 0x68, 0x74,
            0x00,
        ]
    );
}

#[test]
fn receiver_port_range_query_command_matches_shipping_controller() {
    let packet =
        build_command_from_json(r#"{"command":"query_receiver_port_ranges","transaction_id":828}"#)
            .unwrap();
    assert_eq!(
        packet,
        [0x27, 0x29, 0x00, 0x0A, 0x03, 0x3C, 0x33, 0x00, 0x00, 0x00]
    );
}

#[test]
fn transmit_channel_capability_query_command_matches_shipping_controller() {
    let packet = build_command_from_json(
        r#"{"command":"query_transmit_channel_capabilities","transaction_id":809}"#,
    )
    .unwrap();
    assert_eq!(
        packet,
        [
            0x27, 0x29, 0x00, 0x10, 0x03, 0x29, 0x20, 0x32, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x00, 0x00,
        ]
    );
}

#[test]
fn gain_device_type_accepts_only_exact_wire_types() {
    for device_type in ["input", "output"] {
        let json = format!(
                "{{\"command\":\"set_gain_level\",\"channel_number\":1,\"gain_level\":2,\"device_type\":\"{device_type}\",\"host_mac\":\"001122334455\"}}"
            );
        assert!(build_command_from_json(&json).is_ok(), "{device_type}");
    }

    for device_type in ["outputs", "Input", "", "speaker"] {
        let json = format!(
                "{{\"command\":\"set_gain_level\",\"channel_number\":1,\"gain_level\":2,\"device_type\":\"{device_type}\",\"host_mac\":\"001122334455\"}}"
            );
        assert!(
            matches!(
                build_command_from_json(&json),
                Err(SpecError::InvalidDeviceType)
            ),
            "{device_type}"
        );
    }
}

#[test]
fn static_interface_requires_ip_address_and_netmask() {
    for json in [
        r#"{"command":"set_interface_static","ip":"","netmask":"255.255.255.0","host_mac":"001122334455"}"#,
        r#"{"command":"set_interface_static","ip":"192.168.1.10","netmask":"","host_mac":"001122334455"}"#,
    ] {
        assert!(matches!(
            build_command_from_json(json),
            Err(SpecError::InvalidIp)
        ));
    }

    assert!(build_command_from_json(
            r#"{"command":"set_interface_static","ip":"192.168.1.10","netmask":"255.255.255.0","host_mac":"001122334455","message_id":1}"#
        )
        .is_ok());
}

#[test]
fn non_ascii_mac_address_is_rejected_without_panicking() {
    assert!(matches!(
        build_command_from_json(r#"{"command":"make_model","mac":"€aaaaaaaaa"}"#),
        Err(SpecError::InvalidMac)
    ));
}
