use super::*;
use crate::commands::{
    build_query_receiver_channel_status, build_query_transmitter_channel_status,
};
use crate::protocol::{NetaudioError, PROTOCOL_ARC_280F};

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/modern_arc_capture.json"
    )))
    .unwrap()
}

fn payloads(path: &[&str], source_port: Option<u16>) -> Vec<Vec<u8>> {
    let document = fixture();
    let mut value = &document;
    for component in path {
        value = &value[*component];
    }
    value
        .as_array()
        .unwrap()
        .iter()
        .filter(|packet| {
            source_port.is_none_or(|port| packet["source_port"].as_u64() == Some(u64::from(port)))
        })
        .map(|packet| decode_hexadecimal(packet["payload"].as_str().unwrap()))
        .collect()
}

#[test]
fn captured_280f_channel_requests_are_reproduced_exactly() {
    for (path, opcode, builder) in [
        (
            &["pagination", "transmitter_0x2400"][..],
            OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
            build_query_transmitter_channel_status
                as fn(u16, u16, u16, u16, u16) -> Result<Vec<u8>, NetaudioError>,
        ),
        (
            &["pagination", "receiver_0x3400"][..],
            OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809,
            build_query_receiver_channel_status
                as fn(u16, u16, u16, u16, u16) -> Result<Vec<u8>, NetaudioError>,
        ),
    ] {
        for request in payloads(path, Some(49_818)) {
            assert_eq!(read_u16(&request, 6), Some(opcode));
            let transaction_id = read_u16(&request, 4).unwrap();
            let media_type = read_u16(&request, 18).unwrap();
            let first_id = read_u16(&request, 20).unwrap();
            let last_id = read_u16(&request, 22).unwrap();
            assert_eq!(
                builder(
                    PROTOCOL_ARC_280F,
                    media_type,
                    first_id,
                    last_id,
                    transaction_id
                )
                .unwrap(),
                request
            );
        }
    }
}

#[test]
fn captured_channel_pages_preserve_capacity_identity_and_disposition() {
    let transmitter_pages: Vec<_> = payloads(&["pagination", "transmitter_0x2400"], Some(4_840))
        .iter()
        .map(|packet| {
            assert!(matches!(parse_result_code(packet), Some(1 | 0x8112)));
            parse_transmitter_channel_status_page_2809(packet).unwrap()
        })
        .collect();
    assert_eq!(transmitter_pages.len(), 4);
    assert_eq!(transmitter_pages[0].page_capacity, 32);
    assert_eq!(transmitter_pages[0].reported_record_count, 16);
    assert_eq!(
        transmitter_pages[0].page_disposition,
        ModernArcPageDisposition::MorePages
    );
    assert_eq!(
        transmitter_pages[3].page_disposition,
        ModernArcPageDisposition::Complete
    );
    assert_eq!(transmitter_pages[2].records[0].channel_number, 33);
    assert_eq!(transmitter_pages[2].records[0].media_type, 3);
    assert_eq!(transmitter_pages[2].records[0].media_local_channel_id, 33);
    assert_eq!(
        transmitter_pages[3].records.last().unwrap().channel_number,
        64
    );

    let receiver_pages: Vec<_> = payloads(&["pagination", "receiver_0x3400"], Some(4_840))
        .iter()
        .map(|packet| parse_receiver_channel_status_page_2809(packet).unwrap())
        .collect();
    assert_eq!(receiver_pages.len(), 6);
    assert_eq!(receiver_pages[0].page_capacity, 16);
    assert_eq!(receiver_pages[0].reported_record_count, 12);
    assert_eq!(receiver_pages[4].records[0].channel_number, 49);
    assert_eq!(receiver_pages[5].reported_record_count, 4);
    assert_eq!(
        receiver_pages[5].page_disposition,
        ModernArcPageDisposition::Complete
    );
    assert_eq!(
        receiver_pages[5]
            .records
            .last()
            .unwrap()
            .media_local_channel_id,
        64
    );
}

#[test]
fn captured_audio_flow_page_models_global_media_local_and_slots_separately() {
    let response = payloads(
        &["transmitter_flow_0x2600", "accepted_audio_baseline"],
        Some(4_940),
    )
    .pop()
    .unwrap();
    let page = parse_transmitter_flow_status_page(&response).unwrap();
    assert_eq!(page.flows.len(), 3);
    assert_eq!(
        page.flows
            .iter()
            .map(|flow| (
                flow.global_flow_id,
                flow.media_type,
                flow.media_local_flow_id
            ))
            .collect::<Vec<_>>(),
        [(1, 3, 1), (2, 3, 2), (3, 3, 3)]
    );
    assert_eq!(page.flows[0].record_length_bytes, 80);
    assert_eq!(page.flows[0].channel_slot_segment_header, Some(0x0709));
    assert_eq!(page.flows[0].channel_slot_count, Some(4));
    assert_eq!(page.flows[0].transmitter_channel_ids_by_slot, [5, 6, 7, 8]);
    assert_eq!(page.flows[2].transmitter_channel_ids_by_slot, [7, 8, 0, 0]);
    assert_eq!(page.flows[2].populated_transmitter_channel_ids, [7, 8]);
    assert_eq!(page.flows[2].populated_slot_count, 2);
}

#[test]
fn captured_mixed_media_flow_page_accepts_reused_media_local_ids() {
    let response = payloads(
        &["transmitter_flow_0x2600", "accepted_mixed_media"],
        Some(5_040),
    )
    .pop()
    .unwrap();
    let page = parse_transmitter_flow_status_page(&response).unwrap();
    assert_eq!(page.flows.len(), 2);
    assert_eq!(page.flows[0].media_type, 3);
    assert_eq!(page.flows[0].media_local_flow_id, 1);
    assert_eq!(page.flows[1].global_flow_id, 2);
    assert_eq!(page.flows[1].media_type, 4);
    assert_eq!(page.flows[1].media_local_flow_id, 1);
    assert_eq!(page.flows[1].channel_slot_count, None);
    assert!(page.flows[1].transmitter_channel_ids_by_slot.is_empty());
}

#[test]
fn captured_rejected_treatment_exposes_changed_media_local_ids() {
    let response = payloads(
        &[
            "transmitter_flow_0x2600",
            "rejected_media_local_identity_treatment",
        ],
        Some(4_940),
    )
    .into_iter()
    .next()
    .unwrap();
    let page = parse_transmitter_flow_status_page(&response).unwrap();
    assert_eq!(
        page.flows
            .iter()
            .map(|flow| flow.media_local_flow_id)
            .collect::<Vec<_>>(),
        [21, 38, 55]
    );
    assert_eq!(
        page.flows
            .iter()
            .map(|flow| flow.channel_slot_count)
            .collect::<Vec<_>>(),
        [Some(4), Some(4), Some(4)]
    );
}

#[test]
fn captured_flow_page_rejects_malformed_slot_extent() {
    let mut response = payloads(
        &["transmitter_flow_0x2600", "accepted_audio_baseline"],
        Some(4_940),
    )
    .pop()
    .unwrap();
    let first_record = usize::from(read_u16(&response, 18).unwrap());
    response[first_record + 66..first_record + 68].copy_from_slice(&5u16.to_be_bytes());
    assert_eq!(parse_transmitter_flow_status_page(&response), None);
}
