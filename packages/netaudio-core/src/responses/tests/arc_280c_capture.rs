use super::*;
use crate::commands::{
    build_query_receiver_channel_status, build_query_receiver_flow_status,
    build_query_transmitter_channel_status, build_query_tx_flows,
};
use crate::protocol::PROTOCOL_ARC_280C;

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/arc_280c_capture.json"
    )))
    .unwrap()
}

fn packet(device: &str, query: &str, direction: &str) -> Vec<u8> {
    decode_hexadecimal(fixture()[device][query][direction].as_str().unwrap())
}

fn transaction_id(request: &[u8]) -> u16 {
    u16::from_be_bytes([request[4], request[5]])
}

#[test]
fn channel_status_query_builders_match_the_controller_requests_for_280c() {
    for device in ["shure_mxwapx4", "shure_mxa920"] {
        let request = packet(device, "transmitter_channel_status", "request");
        assert_eq!(
            build_query_transmitter_channel_status(
                PROTOCOL_ARC_280C,
                1,
                1,
                0,
                transaction_id(&request)
            )
            .unwrap(),
            request
        );
        let request = packet(device, "receiver_channel_status", "request");
        assert_eq!(
            build_query_receiver_channel_status(
                PROTOCOL_ARC_280C,
                1,
                1,
                0,
                transaction_id(&request)
            )
            .unwrap(),
            request
        );
        let request = packet(device, "transmitter_flow_status", "request");
        assert_eq!(
            build_query_tx_flows(PROTOCOL_ARC_280C, transaction_id(&request)).unwrap(),
            request
        );
        let request = packet(device, "receiver_flow_status", "request");
        let built =
            build_query_receiver_flow_status(PROTOCOL_ARC_280C, 1, transaction_id(&request))
                .unwrap();
        assert_eq!(built.len(), request.len());
        assert_eq!(built[..10], request[..10]);
    }
}

#[test]
fn channel_status_pages_from_280c_devices_use_the_16xx_record_family() {
    for (device, transmitter_count, receiver_count) in
        [("shure_mxwapx4", 5, 4), ("shure_mxa920", 10, 5)]
    {
        let transmitter_page = parse_modern_arc_transmitter_channel_status_page(&packet(
            device,
            "transmitter_channel_status",
            "response",
        ))
        .unwrap();
        assert_eq!(transmitter_page.protocol_id, PROTOCOL_ARC_280C);
        assert_eq!(
            transmitter_page.page_disposition,
            ModernArcPageDisposition::Complete
        );
        assert_eq!(transmitter_page.records.len(), transmitter_count);
        for (index, record) in transmitter_page.records.iter().enumerate() {
            assert_eq!(record.record_type_code, 0x1616);
            assert_eq!(usize::from(record.channel_number), index + 1);
            assert_eq!(record.sample_rate, Some(48_000));
        }

        let receiver_page = parse_modern_arc_receiver_channel_status_page(&packet(
            device,
            "receiver_channel_status",
            "response",
        ))
        .unwrap();
        assert_eq!(receiver_page.protocol_id, PROTOCOL_ARC_280C);
        assert_eq!(
            receiver_page.page_disposition,
            ModernArcPageDisposition::Complete
        );
        assert_eq!(receiver_page.records.len(), receiver_count);
        for (index, record) in receiver_page.records.iter().enumerate() {
            assert_eq!(record.record_type_code, 0x161E);
            assert_eq!(usize::from(record.channel_number), index + 1);
            assert_eq!(record.sample_rate, Some(48_000));
        }
    }
}

#[test]
fn receiver_flow_status_from_280c_uses_the_92_byte_1626_record() {
    let page = parse_modern_arc_receiver_flow_status_page(&packet(
        "shure_mxa920",
        "receiver_flow_status",
        "response",
    ))
    .unwrap();
    assert_eq!(page.protocol_id, PROTOCOL_ARC_280C);
    assert_eq!(page.maximum_flow_slots, 16);
    assert_eq!(page.flows.len(), 1);
    let flow = &page.flows[0];
    assert_eq!(flow.record_type_code, 0x1626);
    assert_eq!(flow.record_length_bytes, 92);
    assert_eq!(flow.sample_rate, Some(48_000));
    assert_eq!(flow.encoding, Some(24));
    assert_eq!(flow.latency_nanoseconds, Some(1_000_000));
    assert_eq!(flow.local_receiver_channel_count, 4);
    assert_eq!(flow.destination_user_datagram_port, Some(14337));
    assert_eq!(
        flow.destination_internet_protocol_version_four_address
            .as_deref(),
        Some("192.0.2.95")
    );

    let empty = parse_modern_arc_receiver_flow_status_page(&packet(
        "shure_mxwapx4",
        "receiver_flow_status",
        "response",
    ))
    .unwrap();
    assert_eq!(empty.protocol_id, PROTOCOL_ARC_280C);
    assert!(empty.flows.is_empty());
}

#[test]
fn transmitter_flow_status_from_280c_reports_the_unicast_destination() {
    for (device, record_type, channel, port, address) in [
        ("shure_mxwapx4", "162b", 1, 14337, "192.0.2.88"),
        ("shure_mxa920", "162d", 9, 14339, "192.0.2.94"),
    ] {
        let page = parse_transmitter_flow_status_page(&packet(
            device,
            "transmitter_flow_status",
            "response",
        ))
        .unwrap();
        assert_eq!(page.flows.len(), 1);
        let flow = &page.flows[0];
        assert!(flow.raw_record_hexadecimal.starts_with(record_type));
        assert_eq!(flow.flow_type.as_deref(), Some("unicast"));
        assert_eq!(flow.sample_rate, Some(48_000));
        assert_eq!(flow.populated_transmitter_channel_ids, vec![channel]);
        assert_eq!(flow.destination_user_datagram_port, Some(port));
        assert_eq!(
            flow.destination_internet_protocol_version_four_address
                .as_deref(),
            Some(address)
        );
    }
}

#[test]
fn write_builders_still_reject_280c_without_captured_evidence() {
    assert_eq!(
        crate::commands::build_set_receiver_channel_name_for_protocol(
            PROTOCOL_ARC_280C,
            1,
            "Name",
            1
        ),
        Err(crate::protocol::NetaudioError::UnsupportedProtocolOperation)
    );
}
