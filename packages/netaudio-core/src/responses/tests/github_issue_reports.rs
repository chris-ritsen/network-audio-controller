use super::*;
use crate::protocol::{PROTOCOL_ARC_2809, RESULT_CODE_MORE_PAGES};

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/github_issue_reports.json"
    )))
    .unwrap()
}

fn payload(path: &[&str]) -> Vec<u8> {
    let document = fixture();
    let mut value = &document;
    for component in path {
        value = &value[*component];
    }
    decode_hexadecimal(value["payload"].as_str().unwrap())
}

#[test]
fn issue_52_tesira_partial_channel_pages_are_valid_and_explicitly_incomplete() {
    let transmitter_response = payload(&["issue_52", "tesira_transmitter_channel_partial_page"]);
    let transmitter_page =
        parse_transmitter_channel_status_page_2809(&transmitter_response).unwrap();
    assert_eq!(transmitter_page.protocol_id, PROTOCOL_ARC_2809);
    assert_eq!(transmitter_page.result_code, RESULT_CODE_MORE_PAGES);
    assert_eq!(
        transmitter_page.page_disposition,
        ModernArcPageDisposition::MorePages
    );
    assert_eq!(transmitter_page.page_capacity, 32);
    assert_eq!(transmitter_page.reported_record_count, 28);
    assert_eq!(transmitter_page.records.first().unwrap().channel_number, 1);
    assert_eq!(transmitter_page.records.last().unwrap().channel_number, 28);

    let receiver_response = payload(&["issue_52", "tesira_receiver_channel_partial_page"]);
    let receiver_page = parse_receiver_channel_status_page_2809(&receiver_response).unwrap();
    assert_eq!(receiver_page.protocol_id, PROTOCOL_ARC_2809);
    assert_eq!(receiver_page.result_code, RESULT_CODE_MORE_PAGES);
    assert_eq!(
        receiver_page.page_disposition,
        ModernArcPageDisposition::MorePages
    );
    assert_eq!(receiver_page.page_capacity, 16);
    assert_eq!(receiver_page.reported_record_count, 16);
    assert_eq!(receiver_page.records.first().unwrap().channel_number, 1);
    assert_eq!(receiver_page.records.last().unwrap().channel_number, 16);
}

#[test]
fn issue_52_unsupported_reply_remains_distinct_from_a_partial_page() {
    let response = payload(&["issue_52", "unsupported_mxwani4_receiver_channel_status"]);
    assert_eq!(parse_result_code(&response), Some(0x0030));
    assert_eq!(parse_receiver_channel_status_page_2809(&response), None);
}

#[test]
fn issue_53_tesira_offset_8_is_not_used_as_the_audio_channel_count() {
    let response = payload(&["issue_53", "tesira_transmitter_flow_status"]);
    let page = parse_transmitter_flow_status_page(&response).unwrap();

    assert_eq!(page.reported_flow_count, 3);
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
    assert_eq!(
        page.flows
            .iter()
            .map(|flow| flow.transmitter_channel_ids_by_slot.as_slice())
            .collect::<Vec<_>>(),
        [&[5, 6, 7, 8], &[1, 3, 2, 4], &[7, 8, 0, 0]]
    );
    assert_eq!(
        page.flows
            .iter()
            .map(|flow| flow.populated_slot_count)
            .collect::<Vec<_>>(),
        [4, 4, 2]
    );

    let offset_8_values = page
        .flows
        .iter()
        .map(|flow| read_u16(&response, usize::from(flow.record_pointer) + 8).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(offset_8_values, [1, 2, 3]);
    assert_ne!(
        offset_8_values,
        page.flows
            .iter()
            .map(|flow| flow.populated_slot_count)
            .collect::<Vec<_>>()
    );
}
