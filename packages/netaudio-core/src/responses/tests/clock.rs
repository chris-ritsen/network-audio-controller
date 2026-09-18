use super::*;
use crate::commands::{build_clock_control, build_refresh_clock_status, ClockControl};

// Synthetic vectors constructed from the supplied clock specification, not device observations.
fn word(r: &mut [u8], at: usize, value: u16) {
    r[at..at + 2].copy_from_slice(&value.to_be_bytes());
}
fn packet(rev: u16, extension: usize) -> Vec<u8> {
    let mut data = vec![0; 24 + extension + 156];
    stamp_conmon_response(&mut data, 0x0020);
    let r = &mut data[24..];
    word(r, 0, rev);
    word(r, 8, 3);
    word(r, 10, 3);
    r[14] = 1;
    r[15] = 7;
    r[20..26].copy_from_slice(&[1, 2, 3, 4, 5, 6]);
    r[28..34].copy_from_slice(&[7, 8, 9, 10, 11, 12]);
    r[36..42].copy_from_slice(&[13, 14, 15, 16, 17, 18]);
    word(r, 44, 1);
    word(r, 46, extension as u16);
    word(r, 48, 9);
    word(r, extension, 0x0284);
    word(r, extension + 10, 0x1800);
    r[extension + 9] = 23;
    r[extension + 12..extension + 16].copy_from_slice(b"lab\0");
    word(r, extension + 28, 0x0013);
    word(r, extension + 30, 3);
    let v = extension + 48;
    let records = extension + 64;
    let v3 = extension + 128;
    word(r, extension + 32, v as u16);
    word(r, extension + 34, 1);
    word(r, extension + 38, 12);
    word(r, extension + 40, v3 as u16);
    word(r, extension + 42, 1);
    word(r, v, 4);
    word(r, v + 2, 0x1234);
    word(r, v + 4, records as u16);
    word(r, v + 6, 16);
    word(r, v3, 4);
    word(r, v3 + 4, (v3 + 8) as u16);
    word(r, v3 + 6, 4);
    for i in 0..4 {
        let p = records + i * 16;
        word(r, p, 4);
        word(r, p + 2, (i + 1) as u16);
        r[p + 4] = 1;
        r[p + 6] = 1;
        r[p + 8..p + 12].copy_from_slice(&0xdeadbeefu32.to_be_bytes());
        word(r, p + 12, if i == 0 { 6 } else { 9 });
        word(r, p + 14, 7);
        word(r, v3 + 8 + i * 4, if i == 3 { 0 } else { 1 });
        word(r, v3 + 10 + i * 4, [0, 1, 77, 88][i]);
    }
    data
}

#[test]
fn dynamic_pointer_distinct_uuids_and_interface_geometry() {
    for e in [64, 112, 216] {
        let bytes = packet(0x073a, e);
        let s = parse_ptp_clock_status(&bytes).unwrap();
        assert_ne!(s.ptpv1_master_uuid, s.ptpv1_grandmaster_uuid);
        assert_eq!(s.ptpv1_device_uuid, Some([1, 2, 3, 4, 5, 6]));
        assert_eq!(s.raw_record, bytes[24..]);
        assert_eq!(s.synchronization, "synchronized");
        assert_eq!(s.ptpv2_domain, Some(23));
        assert_eq!(s.mute_flags, Some(19));
        assert_eq!(s.mute_reasons.len(), 3);
        assert_eq!(s.preferred_leader_locked, Some(true));
        let ports = s.clock_port_records.unwrap();
        assert_eq!(
            ports
                .iter()
                .map(|p| p.network_interface_index)
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(77), None]
        );
        assert!(ports.iter().all(|p| p.unknown_word == 0xdeadbeef));
        assert_eq!(ports[0].role.as_deref(), Some("Leader"));
        assert_eq!(ports[1].role.as_deref(), Some("Follower"));
    }
}

#[test]
fn flag_validity_and_revision_gates() {
    for rev in [0x071c, 0x071e, 0x071f, 0x0724, 0x073a, 0x0800] {
        let mut bytes = packet(rev, 64);
        word(&mut bytes[24..], 128 + 14, 0);
        let s = parse_ptp_clock_status(&bytes).unwrap();
        let p = &s.clock_port_records.unwrap()[0];
        assert_eq!(p.link_down, if rev < 0x071f { Some(false) } else { None });
        assert_eq!(
            p.user_disabled,
            if rev < 0x071f { Some(false) } else { None }
        );
        assert_eq!(p.unicast_delay_requests, None);
        assert_eq!(
            p.network_interface_index,
            if rev < 0x0726 { None } else { Some(0) }
        );
        assert_eq!(s.ptpv2_domain, if rev < 0x0728 { None } else { Some(23) });
    }
}

#[test]
fn legacy_unknown_and_missing_extensions() {
    let mut bytes = packet(0x0100, 64);
    let r = &mut bytes[24..];
    word(r, 20, 3);
    word(r, 24, 6);
    word(r, 28, 9);
    word(r, 32, 0xeeee);
    let s = parse_ptp_clock_status(&bytes).unwrap();
    assert_eq!(s.base_ports.len(), 3);
    assert_eq!(s.base_ports[2].state, None);
    assert_eq!(s.ptpv1_device_uuid, None);
    for pointer in [0, 0xffff] {
        let mut bytes = packet(0x073a, 64);
        word(&mut bytes[24..], 46, pointer);
        assert_eq!(
            parse_ptp_clock_status(&bytes).unwrap().clock_capabilities,
            None
        );
    }
    word(&mut bytes[24..], 0, 0x0101);
    let s = parse_ptp_clock_status(&bytes).unwrap();
    assert!(!s.status_supported);
    assert_eq!(s.synchronization, "unknown");
}

#[test]
fn malformed_regions_and_lengths_fail_closed() {
    let bytes = packet(0x073a, 64);
    for length in 0..bytes.len() {
        assert!(parse_ptp_clock_status(&bytes[..length]).is_none());
    }
    for (at, value) in [
        (44, 0xffff),
        (46, 48),
        (112, 0xffff),
        (116, 0xffff),
        (118, 15),
        (192 + 6, 3),
        (192 + 4, 128),
    ] {
        let mut bad = bytes.clone();
        word(&mut bad[24..], at, value);
        assert!(parse_ptp_clock_status(&bad).is_none(), "offset {at}");
    }
    let mut bad = bytes.clone();
    bad[24 + 64 + 12..24 + 64 + 28].fill(b'x');
    assert!(parse_ptp_clock_status(&bad).is_none());
}

#[test]
fn synchronization_is_servo_and_loss_predicate_only() {
    for (clock, servo, stratum, port, expected) in [
        (0, 3, 255, 4, "synchronized"),
        (3, 2, 1, 9, "lost"),
        (2, 2, 1, 6, "synchronizing"),
        (2, 6, 255, 4, "lost"),
        (2, 6, 1, 6, "unknown"),
    ] {
        let mut bytes = packet(0x073a, 64);
        let r = &mut bytes[24..];
        word(r, 8, clock);
        word(r, 10, servo);
        r[15] = stratum;
        word(r, 48, port);
        assert_eq!(
            parse_ptp_clock_status(&bytes).unwrap().synchronization,
            expected
        );
    }
}

#[test]
fn control_masks_query_padding_and_gates() {
    let mac = [1, 2, 3, 4, 5, 6];
    let query = build_refresh_clock_status(0x0738, mac, 9).unwrap();
    assert_eq!(query.len(), 92);
    assert_eq!(&query[24..26], &[7, 0x38]);
    assert!(query[32..].iter().all(|v| *v == 0));
    assert_eq!(
        build_refresh_clock_status(0x0724, mac, 9).unwrap().len(),
        64
    );
    assert!(build_refresh_clock_status(0, mac, 9).is_err());
    let base = ClockControl {
        record_revision: 0x073a,
        clock_capabilities: Some(0x0204),
        extension_flags: Some(0),
        ..Default::default()
    };
    for (field, mask) in [(0, 1u16), (1, 2), (2, 8)] {
        let mut c = base.clone();
        match field {
            0 => c.clock_source = Some(0),
            1 => c.preferred_leader = Some(true),
            _ => c.subdomain = Some(b"lab".to_vec()),
        };
        let data = build_clock_control(&c, mac, 9).unwrap();
        assert_eq!(&data[32..34], &mask.to_be_bytes());
    }
    let mut c = base.clone();
    c.clock_source = Some(0);
    c.preferred_leader = Some(true);
    c.subdomain = Some(b"lab".to_vec());
    c.aggregate_ptpv1_unicast_delay_requests = Some(true);
    let data = build_clock_control(&c, mac, 9).unwrap();
    assert_eq!(&data[32..34], &11u16.to_be_bytes());
    assert_eq!(&data[56..60], &[2, 0, 2, 0]);
    assert_eq!(&data[37..40], &[0, 0, 0]);
    for cap in [0x20, 0x100] {
        let mut bad = c.clone();
        bad.clock_capabilities = Some(cap);
        assert!(build_clock_control(&bad, mac, 9).is_err());
    }
    c.extension_flags = Some(0x1000);
    assert!(build_clock_control(&c, mac, 9).is_err());
    let mut c = base.clone();
    c.clock_source = Some(1);
    assert!(build_clock_control(&c, mac, 9).is_err());
    c.supported_clock_sources = vec![1, 2];
    assert!(build_clock_control(&c, mac, 9).is_ok());
    c.clock_source = Some(2);
    assert!(build_clock_control(&c, mac, 9).is_ok());
    let mut c = base.clone();
    c.clock_capabilities = Some(8);
    c.global_unicast_delay_requests = Some(true);
    let data = build_clock_control(&c, mac, 9).unwrap();
    assert_eq!(&data[56..60], &[0, 1, 0, 1]);
    c.clock_capabilities = Some(0x208);
    assert!(build_clock_control(&c, mac, 9).is_err());
}

#[test]
fn diagnostic_records_preserve_raw_data_and_enforce_their_own_bounds() {
    let mut master = vec![0; 24 + 32];
    stamp_conmon_response(&mut master, 0x0022);
    word(&mut master[24..], 8, 2);
    word(&mut master[24..], 10, 12);
    word(&mut master[24..], 12, 3);
    word(&mut master[24..], 14, 9);
    master[40..44].copy_from_slice(&[1, 2, 3, 4]);
    let parsed = parse_clock_master_status(&master).unwrap();
    assert_eq!(parsed.status_codes, vec![3, 9]);
    assert_eq!(parsed.block_padding, vec![1, 2, 3, 4]);
    assert_eq!(parsed.raw_record, master[24..]);
    word(&mut master[24..], 8, u16::MAX);
    assert!(parse_clock_master_status(&master).is_none());
    word(&mut master[24..], 8, 2);
    word(&mut master[24..], 10, u16::MAX);
    assert!(parse_clock_master_status(&master).is_none());

    let mut unicast = vec![0; 48];
    stamp_conmon_response(&mut unicast, 0x0024);
    unicast[32..48].fill(0xff);
    let parsed = parse_clock_unicast_status(&unicast).unwrap();
    assert_eq!(parsed.raw_words, [u32::MAX; 4]);
    assert_eq!(parsed.raw_record, unicast[24..]);
    unicast.pop();
    word(&mut unicast, 2, 47);
    assert!(parse_clock_unicast_status(&unicast).is_none());

    let mut named = vec![0; 76];
    stamp_conmon_response(&mut named, 0x0026);
    let r = &mut named[24..];
    for (at, value) in [
        (8, 52),
        (10, 1),
        (12, 16),
        (14, 3),
        (16, 26),
        (20, 38),
        (24, 46),
    ] {
        word(r, at, value);
    }
    r[26..30].copy_from_slice(b"abc\0");
    r[38..44].copy_from_slice(&[1, 2, 3, 4, 5, 6]);
    r[46..52].copy_from_slice(&[7, 8, 9, 10, 11, 12]);
    let parsed = parse_clock_identifier_status(&named).unwrap();
    assert_ne!(parsed.first_identifier, parsed.second_identifier);
    assert_eq!(parsed.name_bytes, b"abc");
    for (at, value) in [(8, 51), (14, 16), (16, 25), (20, 46), (24, u16::MAX)] {
        let mut bad = named.clone();
        word(&mut bad[24..], at, value);
        assert!(parse_clock_identifier_status(&bad).is_none());
    }
    named[24 + 29] = b'x';
    assert!(parse_clock_identifier_status(&named).is_none());
}

#[test]
fn selected_false_values_and_invalid_control_fields() {
    let mac = [1, 2, 3, 4, 5, 6];
    for (capability, global, aggregate, expected) in [
        (8, Some(false), None, 1u16),
        (0x200, None, Some(false), 0x200),
    ] {
        let control = ClockControl {
            record_revision: 0x0738,
            clock_capabilities: Some(capability),
            global_unicast_delay_requests: global,
            aggregate_ptpv1_unicast_delay_requests: aggregate,
            ..Default::default()
        };
        let packet = build_clock_control(&control, mac, 1).unwrap();
        assert_eq!(&packet[56..58], &expected.to_be_bytes());
        assert_eq!(&packet[58..60], &[0, 0]);
    }
    let mut control = ClockControl {
        record_revision: 0x0738,
        clock_capabilities: Some(4),
        subdomain: Some(vec![b'x'; 16]),
        ..Default::default()
    };
    assert!(build_clock_control(&control, mac, 1).is_err());
    control.subdomain = None;
    control.clock_source = Some(3);
    assert!(build_clock_control(&control, mac, 1).is_err());
}
