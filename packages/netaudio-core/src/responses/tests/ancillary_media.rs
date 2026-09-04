use super::*;

fn studio_video_channel_packet(name: &str) -> Vec<u8> {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/studio_video_modern_arc.json"
    ))
    .unwrap();
    decode_hexadecimal(fixture["packets"][name]["payload"].as_str().unwrap())
}

fn set_first_record_media_type(packet: &mut [u8], media_type_code: u16) {
    let record_pointer = usize::from(u16::from_be_bytes([packet[18], packet[19]]));
    packet[record_pointer + CHANNEL_STATUS_RECORD_MEDIA_TYPE
        ..record_pointer + CHANNEL_STATUS_RECORD_MEDIA_TYPE + 2]
        .copy_from_slice(&media_type_code.to_be_bytes());
}

#[test]
fn modern_arc_channel_pages_accept_ancillary_media_type() {
    let mut transmitter = studio_video_channel_packet("transmitter_channel_response");
    set_first_record_media_type(&mut transmitter, MEDIA_TYPE_ANCILLARY);
    let transmitter = parse_modern_arc_transmitter_channel_status_page(&transmitter).unwrap();
    assert_eq!(transmitter.records[0].media_type_code, MEDIA_TYPE_ANCILLARY);
    assert_eq!(transmitter.records[0].media_type, "ancillary");

    let mut receiver = studio_video_channel_packet("receiver_channel_response");
    set_first_record_media_type(&mut receiver, MEDIA_TYPE_ANCILLARY);
    let receiver = parse_modern_arc_receiver_channel_status_page(&receiver).unwrap();
    assert_eq!(receiver.records[0].media_type_code, MEDIA_TYPE_ANCILLARY);
    assert_eq!(receiver.records[0].media_type, "ancillary");
}
