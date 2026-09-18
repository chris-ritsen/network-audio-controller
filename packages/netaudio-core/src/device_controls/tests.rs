//! Synthetic vectors constructed from the supplied device-control specification (2026-09-18).
//! Specification SHA-256: affe89a224d5f9004f37e0e3f01f80cdbe15c8e724bc56d00b57e42bfad8096d
use super::*;
fn status(payload: &[u8], family: &str) -> PanelStatus {
    let mut contents = pb::message(1, b"opaque");
    pb::put_uint(&mut contents, 3, 133);
    pb::put_uint(&mut contents, 2, 7);
    pb::put_bytes(&mut contents, 4, payload);
    let envelope = pb::message(2, &contents);
    let mut r = vec![0u8; 20];
    r[..2].copy_from_slice(&0x0738u16.to_be_bytes());
    r[2..4].copy_from_slice(&0x100eu16.to_be_bytes());
    r[8..10].copy_from_slice(&(envelope.len() as u16).to_be_bytes());
    r[10..12].copy_from_slice(&20u16.to_be_bytes());
    r.extend(envelope);
    let mut body = vec![0u8; 10];
    body.extend_from_slice(b"Audinate");
    body.extend(r);
    let packet = ConmonHeader {
        message_id: 1,
        protocol_id: 0xffff,
    }
    .packet(&body)
    .unwrap();
    parse_panel_status(&packet, Some(family)).unwrap()
}
#[test]
fn specified_query_vectors() {
    let a = build_panel_control(
        &PanelRequest::BluetoothQuery { selector: 1 },
        0,
        9,
        [0; 6],
        0,
    )
    .unwrap();
    assert_eq!(
        &a[24..],
        &[7, 0x34, 0x10, 0x0d, 0, 0, 0, 0, 0, 12, 0, 12, 10, 10, 16, 9, 26, 6, 10, 4, 10, 2, 8, 1]
    );
    let b =
        build_panel_control(&PanelRequest::VideoQuery { selector: 1 }, 0, 1, [0; 6], 0).unwrap();
    assert_eq!(
        &b[24..],
        &[7, 0x34, 0x10, 0x0d, 0, 0, 0, 0, 0, 10, 0, 12, 10, 8, 16, 1, 26, 4, 10, 2, 8, 1]
    );
}
#[test]
fn bluetooth_defaults_unknowns_utf8_and_reordering() {
    for state in [0, 1, 2, 3, 99] {
        let mut d = pb::message(2, "é".repeat(80).as_bytes());
        pb::put_uint(&mut d, 1, state);
        pb::put_uint(&mut d, 55, 900);
        let s = status(
            &pb::message(1, &pb::message(2, &pb::message(1, &d))),
            "bluetooth",
        );
        assert_eq!(s.sequence, 133);
        assert_eq!(s.requester, 7);
        assert_eq!(s.source_identifier, b"opaque");
        let PanelObservation::BluetoothConnection(c) = &s.observations[0] else {
            panic!()
        };
        assert_eq!(c.state, state);
        assert_eq!(c.peer_name.chars().count(), 80);
    }
    for selector in 1..=4 {
        assert!(PanelRequest::BluetoothQuery { selector }.encode().is_ok());
    }
    for field in [3, 5, 7] {
        let b = if field == 7 {
            pb::scalar(1, 5)
        } else {
            pb::message(1, &pb::scalar(1, 2))
        };
        assert_eq!(
            status(&pb::message(1, &pb::message(field, &b)), "bluetooth")
                .observations
                .len(),
            1
        );
    }
    assert!(PanelRequest::BluetoothIdentification {
        name_source: 2,
        custom_name: "😀".repeat(32)
    }
    .encode()
    .is_ok());
    assert!(PanelRequest::BluetoothIdentification {
        name_source: 2,
        custom_name: "😀".repeat(33)
    }
    .encode()
    .is_err());
    assert!(PanelRequest::BluetoothClearPairing { confirmed: false }
        .encode()
        .is_err());
    assert!(PanelRequest::BluetoothClearPairing { confirmed: true }
        .encode()
        .is_ok());
    assert_eq!(
        PanelRequest::BluetoothDiscovery {
            discoverable: false
        }
        .encode()
        .unwrap(),
        vec![10, 6, 50, 4, 10, 2, 8, 2]
    );
    let invalid = pb::message(
        1,
        &pb::message(2, &pb::message(1, &pb::message(2, &[0xff]))),
    );
    assert!(status(&invalid, "bluetooth").diagnostic_error.is_some());
}
#[test]
fn video_alternatives_and_optional_fields() {
    let format = VideoFormat {
        resolution: 999,
        bit_depth: 14,
        color_space: 24,
    };
    let mut b = pb::message(1, &format.encode());
    pb::put_bytes(&mut b, 2, &format.encode());
    pb::put_bytes(&mut b, 3, &SelectionMode::default().encode());
    pb::put_bytes(
        &mut b,
        4,
        &VideoFormat {
            resolution: 16,
            bit_depth: 2,
            color_space: 1,
        }
        .encode(),
    );
    let s = status(&pb::message(2, &b), "dante_av");
    let PanelObservation::VideoFormat(v) = &s.observations[0] else {
        panic!()
    };
    assert_eq!(v.direction, 0);
    assert_eq!(v.configured.as_ref().unwrap().resolution, 999);
    assert_eq!(v.actual.as_ref().unwrap().resolution, 16);
    for field in [4, 6, 7, 8, 9, 11] {
        assert_eq!(
            status(&pb::message(field, &[]), "dante_av")
                .observations
                .len(),
            1
        );
    }
    let mut hdcp = pb::scalar(1, 3);
    pb::put_uint(&mut hdcp, 2, 1);
    pb::put_bytes(&mut hdcp, 2, &[2, 3, 0x81, 1]);
    let s = status(&pb::message(9, &hdcp), "dante_av");
    let PanelObservation::Hdcp(h) = &s.observations[0] else {
        panic!()
    };
    assert_eq!(h.supported_modes, vec![1, 2, 3, 129]);
    assert_eq!(h.configured_mode, 3);
    let mut signal = pb::scalar(2, 999);
    pb::put_uint(&mut signal, 3, 3);
    let s = status(&pb::message(6, &signal), "dante_av");
    let PanelObservation::VideoChannel(c) = &s.observations[0] else {
        panic!()
    };
    assert_eq!(c.status_code, 999);
    assert_eq!(c.observed_hdcp_version, Some(3));
    for selector in 1..=7 {
        assert!(PanelRequest::VideoQuery { selector }.encode().is_ok());
    }
    for field in [1, 3, 5, 10] {
        assert!(status(&pb::message(field, &[]), "dante_av")
            .observations
            .is_empty());
    }
}
#[test]
fn serial_bandwidth_and_visca_writers() {
    let mut serial = SerialSettings {
        baud_rate: 9600,
        hardware_flow_control: 0,
        software_flow_control: 0,
        data_bits: 8,
        parity: 0,
        stop_bits: 1,
    };
    assert!(PanelRequest::Serial {
        settings: serial.clone()
    }
    .encode()
    .is_ok());
    serial.data_bits = 7;
    assert!(PanelRequest::Serial {
        settings: serial.clone()
    }
    .encode()
    .is_err());
    serial.parity = 1;
    assert!(PanelRequest::Serial {
        settings: serial.clone()
    }
    .encode()
    .is_ok());
    serial.hardware_flow_control = 1;
    assert!(PanelRequest::Serial {
        settings: serial.clone()
    }
    .encode()
    .is_err());
    assert!(
        matches!(&status(&pb::message(7,&serial.encode()),"dante_av").observations[0],PanelObservation::Serial(s) if s.hardware_flow_control==1)
    );
    assert!(PanelRequest::Bandwidth {
        target: 701,
        enabled: true
    }
    .encode()
    .is_err());
    assert!(PanelRequest::Bandwidth {
        target: 0,
        enabled: true
    }
    .encode()
    .is_err());
    assert_eq!(
        PanelRequest::Bandwidth {
            target: 100,
            enabled: true
        }
        .encode()
        .unwrap(),
        vec![0x42, 4, 8, 100, 32, 1]
    );
    let f = VideoFormat {
        resolution: 257,
        color_space: 16,
        bit_depth: 8,
    };
    let m = SelectionMode {
        manual_resolution: true,
        manual_color_space: true,
        manual_bit_depth: true,
    };
    assert_eq!(
        scoped_visca(&f, &m).unwrap(),
        vec![0x81, 1, 0x99, 1, 1, 1, 1, 2, 0xff]
    );
    assert!(scoped_visca(
        &VideoFormat {
            color_space: 4,
            ..f.clone()
        },
        &m
    )
    .is_err());
    assert!(scoped_visca(&f, &SelectionMode::default()).is_err());
}
#[test]
fn malformed_protobuf_is_bounded() {
    for bad in [
        vec![0x80; 11],
        vec![0x0a, 0xff, 0xff, 0xff, 0xff, 0x7f],
        vec![0],
        vec![0x0b],
        vec![8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2],
    ] {
        assert!(pb::parse(&bad).is_none());
    }
    let mut valid = build_panel_control(
        &PanelRequest::BluetoothQuery { selector: 1 },
        0,
        9,
        [0; 6],
        0,
    )
    .unwrap();
    valid[26..28].copy_from_slice(&0x100eu16.to_be_bytes());
    for n in 0..valid.len() {
        assert!(parse_panel_status(&valid[..n], None).is_none());
    }
}

#[test]
fn serial_writes_all_six_fields_including_zeroes() {
    let bytes = PanelRequest::Serial {
        settings: SerialSettings {
            baud_rate: 9600,
            data_bits: 8,
            parity: 0,
            stop_bits: 1,
            hardware_flow_control: 0,
            software_flow_control: 0,
        },
    }
    .encode()
    .unwrap();
    let root = pb::parse(&bytes).unwrap();
    let serial = pb::nested(&root, 7).unwrap().unwrap();
    assert_eq!(
        serial.iter().map(|f| f.number).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6]
    );
}

#[test]
fn all_analog_directions_channels_levels_and_raw_count_word() {
    use crate::{commands, responses};
    for input in [true, false] {
        for channel in [1, 2] {
            for level in 1..=5 {
                let packet =
                    commands::build_set_gain_level([2, 0, 0, 0, 0, 1], 1, channel, level, input)
                        .unwrap();
                assert_eq!(&packet[40..42], if input { &[1, 2] } else { &[2, 1] });
                assert_eq!(&packet[44..48], &(1u32 << (channel - 1)).to_be_bytes());
                assert_eq!(&packet[48..52], &u32::from(level).to_be_bytes());
            }
        }
    }
    assert!(commands::build_set_gain_level([0; 6], 1, 3, 1, true).is_err());
    let mut r = vec![0u8; 48];
    r[..4].copy_from_slice(&[7, 0x38, 0x10, 0x0b]);
    r[8..10].copy_from_slice(&0xabcd_u16.to_be_bytes());
    r[10..16].copy_from_slice(&[0, 2, 0, 8, 0, 16]);
    r[16..24].copy_from_slice(&[1, 2, 0, 2, 0, 4, 0, 32]);
    r[24..32].copy_from_slice(&[99, 88, 0, 2, 0, 4, 0, 40]);
    r[32..48].copy_from_slice(&[0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff]);
    let mut b = vec![0u8; 10];
    b.extend_from_slice(b"Audinate");
    b.extend(r);
    let mut p = ConmonHeader {
        message_id: 1,
        protocol_id: 0xffff,
    }
    .packet(&b)
    .unwrap();
    let s = responses::parse_codec_status(&p).unwrap();
    assert_eq!(s.raw_header_word, 0xabcd);
    assert_eq!(s.parameters.len(), 2);
    assert_eq!(s.parameters[1].values, vec![0, u32::MAX]);
    p[54..56].copy_from_slice(&16u16.to_be_bytes());
    assert!(responses::parse_codec_status(&p).is_none());
}
