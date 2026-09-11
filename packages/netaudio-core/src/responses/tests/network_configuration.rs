use super::*;
use crate::network::{DanteRedundancyMode as Mode, NetworkInterface};

fn captured(name: &str) -> Vec<u8> {
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/network_configuration.json"
    ))
    .unwrap();
    decode_hexadecimal(fixtures["cases"][name]["hexadecimal"].as_str().unwrap())
}

fn set_word(data: &mut [u8], offset: usize, value: u16) {
    data[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn managed_capture(name: &str) -> Vec<u8> {
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/managed_network_configuration.json"
    ))
    .unwrap();
    decode_hexadecimal(fixtures["cases"][name]["hexadecimal"].as_str().unwrap())
}

#[test]
fn managed_network_status_preserves_active_and_pending_dns() {
    for (name, dns, pending) in [
        ("before", "8.8.8.8", false),
        ("pending", "192.0.2.1", true),
        ("restored", "8.8.8.8", false),
    ] {
        let status = parse_interface_status(&managed_capture(name)).unwrap();
        assert_eq!(status.record_protocol_identifier, 0x0738);
        assert_eq!(status.interfaces.len(), 1);
        let primary = &status.interfaces[0];
        assert_eq!(primary.dns_server.as_deref(), Some("8.8.8.8"));
        assert_eq!(
            primary.configured.as_ref().unwrap().dns_server.as_deref(),
            Some(dns)
        );
        assert_eq!(primary.reboot_required, pending);
        assert_eq!(status.reboot_required, pending);
        let redundancy = status.redundancy.unwrap();
        assert_eq!(redundancy.current, redundancy.configured);
        assert!(!redundancy.reboot_required);
    }
}

#[test]
fn managed_network_status_rejects_bad_pending_descriptors() {
    let data = managed_capture("pending");
    for length in 72..92 {
        let mut short = data[..length].to_vec();
        set_word(&mut short, 2, length as u16);
        assert!(parse_interface_status(&short).is_none(), "length {length}");
    }
    for (offset, value) in [(64, 23), (66, 0), (66, u16::MAX), (72, 7)] {
        let mut invalid = data.clone();
        set_word(&mut invalid, offset, value);
        assert!(
            parse_interface_status(&invalid).is_none(),
            "offset {offset}"
        );
    }
    let mut other_revision = data;
    set_word(&mut other_revision, 24, 0x07fe);
    let status = parse_interface_status(&other_revision).unwrap();
    assert_eq!(status.record_protocol_identifier, 0x07fe);
    assert!(status.interfaces[0].configured.is_some());
}

#[test]
fn captured_redundancy_treatment_distinguishes_current_from_configured() {
    for (name, current, configured, reboot) in [
        (
            "a32_switched_switched",
            Mode::Switched,
            Mode::Switched,
            false,
        ),
        (
            "a32_switched_redundant",
            Mode::Switched,
            Mode::Redundant,
            true,
        ),
        (
            "a32_redundant_redundant",
            Mode::Redundant,
            Mode::Redundant,
            false,
        ),
    ] {
        let parsed = parse_interface_status(&captured(name)).unwrap();
        let redundancy = parsed.redundancy.unwrap();
        assert_eq!(redundancy.current, Some(current), "{name}");
        assert_eq!(redundancy.configured, Some(configured), "{name}");
        assert_eq!(redundancy.reboot_required, reboot, "{name}");
        assert_eq!(parsed.reboot_required, reboot, "{name}");
    }
}

#[test]
fn inferred_redundancy_bits_support_pending_return_to_switched() {
    let mut data = captured("a32_redundant_redundant");
    set_word(&mut data, 96, 1);
    let status = parse_interface_status(&data).unwrap().redundancy.unwrap();
    assert_eq!(status.current, Some(Mode::Redundant));
    assert_eq!(status.configured, Some(Mode::Switched));
    assert!(status.reboot_required);
}

#[test]
fn captured_dual_pending_static_configuration_preserves_interface_identity() {
    let status = parse_interface_status(&captured("a32_static_pending_both")).unwrap();
    assert!(status.reboot_required);
    assert_eq!(status.interfaces.len(), 2);
    let primary = &status.interfaces[0];
    let secondary = &status.interfaces[1];
    assert_eq!(primary.interface, Some(NetworkInterface::Primary));
    assert_eq!(secondary.interface, Some(NetworkInterface::Secondary));
    assert_eq!(primary.mode, "dynamic");
    assert_eq!(secondary.mode, "dynamic");
    assert_eq!(secondary.ip_address, "198.51.100.62");
    for (entry, address) in [(primary, "192.0.2.34"), (secondary, "192.0.2.244")] {
        assert!(entry.reboot_required);
        let configured = entry.configured.as_ref().unwrap();
        assert_eq!(configured.mode, "static");
        assert_eq!(configured.ip_address.as_deref(), Some(address));
        assert_eq!(configured.netmask.as_deref(), Some("255.255.255.0"));
        assert_eq!(configured.dns_server.as_deref(), Some("8.8.8.8"));
        assert_eq!(configured.gateway.as_deref(), Some("192.0.2.1"));
    }
}

#[test]
fn captured_secondary_static_record_is_applied_despite_retained_configuration() {
    let status = parse_interface_status(&captured("a32_static_active_both")).unwrap();
    assert!(!status.reboot_required);
    for (entry, address) in status.interfaces.iter().zip(["192.0.2.34", "192.0.2.244"]) {
        assert_eq!(entry.mode, "static");
        assert_eq!(entry.ip_address, address);
        assert!(!entry.reboot_required);
        assert_eq!(
            entry.configured.as_ref().unwrap().dns_server.as_deref(),
            Some("8.8.8.8")
        );
    }
    assert_eq!(status.interfaces[1].dns_server, None);
    assert_eq!(status.interfaces[1].gateway, None);
}

#[test]
fn primary_dns_change_remains_pending_but_matching_static_target_is_applied() {
    let mut data = captured("a32_static_pending_both");
    set_word(&mut data, 40, 3);
    data[56..60].copy_from_slice(&[8, 8, 4, 4]);
    let status = parse_interface_status(&data).unwrap();
    assert!(status.interfaces[0].reboot_required);
    data[56..60].copy_from_slice(&[8, 8, 8, 8]);
    let status = parse_interface_status(&data).unwrap();
    assert!(!status.interfaces[0].reboot_required);
    assert!(status.interfaces[1].reboot_required);
}

#[test]
fn dhcp_pending_and_applied_are_checked_per_interface() {
    let mut data = captured("a32_static_active_both");
    set_word(&mut data, 100, 4);
    set_word(&mut data, 124, 4);
    assert!(parse_interface_status(&data)
        .unwrap()
        .interfaces
        .iter()
        .all(|v| v.reboot_required));
    set_word(&mut data, 40, 1);
    set_word(&mut data, 68, 0);
    let parsed = parse_interface_status(&data).unwrap();
    assert!(!parsed.reboot_required);
    assert!(parsed
        .interfaces
        .iter()
        .all(|v| v.configured.as_ref().unwrap().mode == "dynamic"));
}

#[test]
fn interface_configuration_rejects_truncated_records_and_bad_descriptors() {
    let data = captured("a32_static_pending_both");
    let records_only = 92..=96;
    for length in 0..144 {
        let mut short = data[..length].to_vec();
        if length >= 4 {
            set_word(&mut short, 2, length as u16);
        }
        if records_only.contains(&length) {
            let status = parse_interface_status(&short).unwrap();
            assert!(status
                .interfaces
                .iter()
                .all(|entry| entry.configured.is_none()));
            assert!(status.redundancy.is_none());
            continue;
        }
        assert!(parse_interface_status(&short).is_none(), "length {length}");
    }
    for (offset, value) in [
        (32, 9),
        (92, 23),
        (94, 0),
        (94, u16::MAX),
        (100, 7),
        (124, 7),
    ] {
        let mut invalid = data.clone();
        set_word(&mut invalid, offset, value);
        assert!(
            parse_interface_status(&invalid).is_none(),
            "offset {offset}"
        );
    }
}

#[test]
fn duplicate_interface_mac_addresses_are_rejected() {
    let mut data = captured("a32_static_active_both");
    let mac = data[42..48].to_vec();
    data[70..76].copy_from_slice(&mac);
    assert!(parse_interface_status(&data).is_none());
}

#[test]
fn redundancy_flags_decode_by_structure_for_any_revision() {
    let mut data = captured("a32_switched_switched");
    set_word(&mut data, 68, 8);
    assert!(parse_interface_status(&data).unwrap().redundancy.is_none());
    set_word(&mut data, 68, 0);
    set_word(&mut data, 24, 0x07fe);
    let status = parse_interface_status(&data).unwrap();
    assert_eq!(status.record_protocol_identifier, 0x07fe);
    let redundancy = status.redundancy.unwrap();
    assert_eq!(redundancy.current, Some(Mode::Switched));
    assert_eq!(redundancy.configured, Some(Mode::Switched));
    assert!(status
        .interfaces
        .iter()
        .all(|entry| entry.configured.is_some()));
}

#[test]
fn switch_choice_status_preserves_current_and_pending_labels() {
    let status =
        parse_switch_configuration_status(&captured("ad4d_switched_split_redundant")).unwrap();
    assert_eq!(status.redundancy.current, Some(Mode::Switched));
    assert_eq!(status.redundancy.configured, Some(Mode::SplitRedundant));
    assert!(status.redundancy.reboot_required);
    let restored = parse_switch_configuration_status(&captured("ad4d_switched_switched")).unwrap();
    assert!(!restored.redundancy.reboot_required);
}

#[test]
fn redundancy_setters_reproduce_independent_controller_requests() {
    for (name, protocol, mode, choice) in [
        ("a32_set_switched", 0x0724, Mode::Switched, None),
        ("a32_set_redundant", 0x0724, Mode::Redundant, None),
        (
            "ad4d_set_split_redundant",
            0x072e,
            Mode::SplitRedundant,
            Some(2),
        ),
    ] {
        let packet = captured(name);
        let mac = packet[8..14].try_into().unwrap();
        let sequence = read_u16(&packet, 4).unwrap();
        let encoded = crate::commands::build_set_dante_redundancy(
            Some(protocol),
            mode,
            choice,
            mac,
            sequence,
        )
        .unwrap();
        assert_eq!(&encoded[24..], &packet[24..], "{name}");
        assert_eq!(&encoded[8..14], &packet[8..14]);
        // Controller's transport-specific prefix word is independent of the
        // command body and the NetAudio host identifier representation.
        assert_eq!(encoded.len(), packet.len());
        let json = serde_json::json!({"command":"set_dante_redundancy", "record_protocol_identifier":protocol, "mode":mode, "switch_configuration_choice":choice, "host_mac":"020000000010", "sequence":sequence});
        assert!(crate::spec::build_command_from_json(&json.to_string()).is_ok());
    }
}

#[test]
fn redundancy_setter_form_follows_reported_switch_configuration_not_revision() {
    let flag_form =
        crate::commands::build_set_dante_redundancy(Some(0x07fe), Mode::Redundant, None, [0; 6], 1)
            .unwrap();
    let known_revision =
        crate::commands::build_set_dante_redundancy(Some(0x0724), Mode::Redundant, None, [0; 6], 1)
            .unwrap();
    assert_eq!(flag_form, known_revision);
    assert_eq!(&flag_form[26..28], &[0x00, 0x13]);
    let choice_form =
        crate::commands::build_set_dante_redundancy(None, Mode::Switched, Some(1), [0; 6], 1)
            .unwrap();
    assert_eq!(&choice_form[26..28], &[0x00, 0x15]);
    assert_eq!(
        &choice_form[choice_form.len() - 4..],
        &[0x00, 0x01, 0x00, 0x01]
    );
    assert!(crate::commands::build_set_dante_redundancy(
        Some(0x072e),
        Mode::SplitRedundant,
        None,
        [0; 6],
        1
    )
    .is_err());
    assert!(crate::commands::build_set_dante_redundancy(
        Some(0x0724),
        Mode::Switched,
        None,
        [0; 6],
        0
    )
    .is_err());
}

fn dhcp_lease_capture() -> Vec<u8> {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/dhcp_dns_gateway_order.json"
    ))
    .unwrap();
    decode_hexadecimal(fixture["hexadecimal"].as_str().unwrap())
}

#[test]
fn dynamic_running_record_reads_dns_at_16_and_gateway_at_20() {
    let status = parse_interface_status(&dhcp_lease_capture()).unwrap();
    assert_eq!(status.interfaces.len(), 1);
    let primary = &status.interfaces[0];
    assert_eq!(primary.mode, "dynamic");
    assert_eq!(primary.ip_address, "192.168.1.42");
    assert_eq!(primary.gateway.as_deref(), Some("192.168.1.1"));
    assert_eq!(primary.dns_server.as_deref(), Some("8.8.8.8"));
}
