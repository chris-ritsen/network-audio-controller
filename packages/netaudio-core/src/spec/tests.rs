use super::*;

#[test]
fn routes_arc_commands_as_stamped_requests() {
    for command in [
        "set_channel_name",
        "add_subscriptions",
        "set_latency",
        "channel_count",
        "query_transmitter_channel_status_2809",
        "query_receiver_channel_status_2809",
        "query_receiver_flow_status_2809",
        "query_receiver_flows",
        "query_transmit_channel_capabilities",
        "query_receiver_port_ranges",
    ] {
        assert_eq!(
            route(command),
            (Target::Arc, IoMode::StampedRequest),
            "{command}"
        );
    }
}

#[test]
fn routes_unacknowledged_settings_writes_as_single_fire_commands() {
    for command in [
        "set_encoding",
        "set_sample_rate",
        "set_sample_rate_pullup",
        "set_gain_level",
        "clear_all_configuration",
        "clear_all_configuration_preserving_internet_protocol_settings",
    ] {
        assert_eq!(
            route(command),
            (
                Target::Settings,
                IoMode::Fire {
                    repeat: 1,
                    interval_ms: 0
                }
            ),
            "{command}"
        );
    }
}

#[test]
fn routes_cmc_registration_as_control_request() {
    assert_eq!(route("cmc_register"), (Target::Control, IoMode::Request));
}

#[test]
fn routes_system_resets_as_single_fire_commands() {
    for command in ["reboot", "factory_reset"] {
        assert_eq!(
            route(command),
            (
                Target::Settings,
                IoMode::Fire {
                    repeat: 1,
                    interval_ms: 0
                }
            ),
            "{command}"
        );
    }
}

#[test]
fn routes_repeated_fire_commands() {
    assert_eq!(
        route("enable_aes67"),
        (
            Target::Settings,
            IoMode::Fire {
                repeat: 3,
                interval_ms: 100
            }
        )
    );
    assert_eq!(
        route("set_preferred_leader"),
        (
            Target::Settings,
            IoMode::Fire {
                repeat: 3,
                interval_ms: 500
            }
        )
    );
}

#[test]
fn routes_single_fire_and_control_commands() {
    assert_eq!(
        route("identify"),
        (
            Target::Settings,
            IoMode::Fire {
                repeat: 1,
                interval_ms: 0
            }
        )
    );
    assert_eq!(
        route("probe_sample_rate"),
        (
            Target::Settings,
            IoMode::Fire {
                repeat: 1,
                interval_ms: 0
            }
        )
    );
    assert_eq!(
        route("probe_encoding"),
        (
            Target::Settings,
            IoMode::Fire {
                repeat: 1,
                interval_ms: 0
            }
        )
    );
    assert_eq!(
        route("probe_sample_rate_pullup"),
        (
            Target::Settings,
            IoMode::Fire {
                repeat: 1,
                interval_ms: 0
            }
        )
    );
    assert_eq!(
        route("probe_gain_level"),
        (
            Target::Settings,
            IoMode::Fire {
                repeat: 1,
                interval_ms: 0
            }
        )
    );
    assert_eq!(
        route("metering_start"),
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
    let routed = build_routed_command("{\"command\":\"reboot\"}", mac).unwrap();
    assert_eq!(&routed.packet[8..14], &mac);
}

#[test]
fn explicit_host_mac_overrides_default() {
    let routed = build_routed_command(
        "{\"command\":\"reboot\",\"host_mac\":\"001122334455\"}",
        [0xFF; 6],
    )
    .unwrap();
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
fn probe_encoding_defaults_to_captured_message_sequence() {
    let routed = build_routed_command(
        r#"{"command":"probe_encoding"}"#,
        [0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24],
    )
    .unwrap();
    assert_eq!(&routed.packet[4..6], &0x0083u16.to_be_bytes());
    assert_eq!(&routed.packet[26..28], &0x0083u16.to_be_bytes());
}

#[test]
fn refresh_clock_status_preserves_the_requested_sequence_and_mac() {
    let host_mac = [0x84, 0x2F, 0x57, 0x74, 0xE8, 0x6D];
    let routed = build_routed_command(
        r#"{"command":"refresh_clock_status","sequence":33}"#,
        host_mac,
    )
    .unwrap();
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
    let probe =
        build_routed_command(r#"{"command":"probe_sample_rate_pullup"}"#, host_mac).unwrap();
    assert_eq!(
        probe.packet,
        commands::build_probe_sample_rate_pullup(host_mac, 0x0085).unwrap()
    );

    let write = build_routed_command(
        r#"{"command":"set_sample_rate_pullup","raw_value":4,"sequence":71}"#,
        host_mac,
    )
    .unwrap();
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
            r#"{"command":"set_interface_static","ip":"192.168.1.10","netmask":"255.255.255.0","host_mac":"001122334455"}"#
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
