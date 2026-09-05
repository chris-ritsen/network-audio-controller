use super::conmon::{
    captured_encoding_status_packet_204720, captured_input_gain_status_packet_1528,
    captured_sample_rate_pullup_status_packet, captured_sample_rate_status_packet_28101,
};
use super::device::{aes67_settings_response, captured_selective_device_settings_packet_9084571};
use super::flows::{captured_receiver_flow_response, flow_query_response};
use super::*;

#[test]
fn typed_parsers_reject_every_truncated_prefix_without_panicking() {
    let flow = flow_query_response();
    for length in 0..flow.len() {
        assert_eq!(parse_tx_flows(&flow[..length]), None);
        assert_eq!(parse_result_code(&flow[..length]), None);
    }

    let receiver_flow = captured_receiver_flow_response();
    for length in 0..receiver_flow.len() {
        assert_eq!(parse_receiver_flow_page(&receiver_flow[..length]), None);
        assert_eq!(parse_result_code(&receiver_flow[..length]), None);
    }

    let receiver_port_ranges = decode_hexadecimal("27290012033c330000013800397f398039ff");
    for length in 0..receiver_port_ranges.len() {
        assert_eq!(
            parse_receiver_port_ranges(&receiver_port_ranges[..length]),
            None
        );
    }

    let cmc_registration =
        decode_hexadecimal("120000200000100100010000020000000001000000010000c0a8013d21fc0000");
    for length in 0..cmc_registration.len() {
        assert_eq!(
            parse_cmc_registration_response(&cmc_registration[..length]),
            None
        );
    }

    let mut conmon = vec![0u8; 0x4A];
    stamp_conmon_response(&mut conmon, CONMON_OPCODE_PTP_CLOCK_STATUS);
    for length in 0..conmon.len() {
        assert_eq!(parse_ptp_clock_status(&conmon[..length]), None);
        assert_eq!(parse_conmon_opcode(&conmon[..length]), None);
    }

    let sample_rate_status = captured_sample_rate_status_packet_28101();
    for length in 0..sample_rate_status.len() {
        assert_eq!(
            parse_sample_rate_status(&sample_rate_status[..length]),
            None
        );
    }

    let encoding_status = captured_encoding_status_packet_204720();
    for length in 0..encoding_status.len() {
        assert_eq!(parse_encoding_status(&encoding_status[..length]), None);
    }

    let sample_rate_pullup_status = captured_sample_rate_pullup_status_packet();
    for length in 0..sample_rate_pullup_status.len() {
        assert_eq!(
            parse_sample_rate_pullup_status(&sample_rate_pullup_status[..length]),
            None
        );
    }

    let gain_status = captured_input_gain_status_packet_1528();
    for length in 0..gain_status.len() {
        assert_eq!(parse_gain_status(&gain_status[..length]), None);
    }

    let metering = metering_frame(&[0xFE, 0x7D, 0xA0], &[0x88, 0x00]);
    for length in 0..metering.len() {
        assert_eq!(parse_metering_frame(&metering[..length]), None);
    }

    let device_settings = captured_selective_device_settings_packet_9084571();
    for length in 0..device_settings.len() {
        assert_eq!(parse_device_settings(&device_settings[..length]), None);
    }
}

#[test]
fn every_typed_response_parser_rejects_truncation() {
    let mut device_info = vec![0u8; RESPONSE_HEADER_SIZE + 18];
    stamp_arc_response(
        &mut device_info,
        PROTOCOL_ID,
        OPCODE_DEVICE_INFO,
        RESULT_CODE_SUCCESS,
    );

    let aes67_config = aes67_settings_response(&[(DEVICE_SETTINGS_INFO_AES67_CONFIGURED, 0x0003)]);

    let mut bluetooth = vec![0u8; 62];
    stamp_conmon_response(&mut bluetooth, CONMON_OPCODE_BLUETOOTH_STATUS);
    bluetooth[36..40].copy_from_slice(&[0x12, 0x18, 0x0A, 0x0A]);
    bluetooth[50..54].copy_from_slice(&[0x18, 0x09, 0x22, 0x08]);
    bluetooth[54..62].copy_from_slice(&[0x0A, 0x06, 0x12, 0x04, 0x0A, 0x02, 0x08, 0x02]);

    let mut make_model = vec![0u8; CONMON_PRODUCT_VERSION_END];
    stamp_conmon_response(&mut make_model, CONMON_OPCODE_MAKE_MODEL_RESPONSE);
    let mut dante_model = vec![0u8; CONMON_BOARD_NAME_END];
    stamp_conmon_response(&mut dante_model, CONMON_OPCODE_DANTE_MODEL_RESPONSE);

    for length in 0..device_info.len() {
        assert_eq!(parse_device_info(&device_info[..length]), None);
    }
    for length in 0..aes67_config.len() {
        assert_eq!(parse_aes67_configured(&aes67_config[..length]), None);
    }
    for length in 0..bluetooth.len() {
        assert_eq!(parse_bluetooth_status(&bluetooth[..length]), None);
    }
    for length in 0..make_model.len() {
        assert_eq!(parse_make_model(&make_model[..length]), None);
    }
    for length in 0..dante_model.len() {
        assert_eq!(parse_dante_model(&dante_model[..length]), None);
    }
}

#[test]
fn hostile_bytes_never_panic_or_decode_as_typed_responses() {
    for length in 0..256usize {
        let data: Vec<u8> = (0..length)
            .map(|index| ((index * 73 + length * 19) & 0xFF) as u8)
            .collect();
        assert_eq!(parse_device_name(&data), None);
        assert_eq!(parse_device_info(&data), None);
        assert_eq!(parse_device_settings(&data), None);
        assert_eq!(parse_aes67_configured(&data), None);
        assert_eq!(parse_make_model(&data), None);
        assert_eq!(parse_dante_model(&data), None);
        assert_eq!(parse_result_code(&data), None);
        assert_eq!(parse_cmc_registration_response(&data), None);
        assert_eq!(parse_tx_flows(&data), None);
        assert_eq!(parse_receiver_port_ranges(&data), None);
        assert_eq!(parse_bluetooth_status(&data), None);
        assert_eq!(parse_conmon_opcode(&data), None);
        assert_eq!(parse_ptp_clock_status(&data), None);
        assert_eq!(parse_aes67_status(&data), None);
        assert_eq!(parse_interface_status(&data), None);
        assert_eq!(parse_sample_rate_status(&data), None);
        assert_eq!(parse_encoding_status(&data), None);
        assert_eq!(parse_sample_rate_pullup_status(&data), None);
        assert_eq!(parse_gain_status(&data), None);
        assert_eq!(parse_metering_frame(&data), None);
        assert_eq!(
            parse_dante_brooklyn_control_protocol_flow_setup_request(&data),
            None
        );
        assert_eq!(
            parse_dante_brooklyn_control_protocol_flow_setup_response(&data),
            None
        );
    }
}
