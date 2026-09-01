use super::*;

#[no_mangle]
pub unsafe extern "C" fn netaudio_parse_response(
    kind: *const c_char,
    data: *const u8,
    data_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard_panic(|| {
        if let Err(status) = unsafe { prepare_output(out_buffer, out_capacity, out_length) } {
            return status;
        }
        if kind.is_null() || data.is_null() {
            return NetaudioStatus::NullPointer;
        }
        let kind = match unsafe { CStr::from_ptr(kind) }.to_str() {
            Ok(value) => value,
            Err(_) => return NetaudioStatus::InvalidUtf8,
        };
        let bytes = unsafe { std::slice::from_raw_parts(data, data_len) };

        use crate::responses;
        unsafe {
            match kind {
                "channel_count" => write_optional_json(
                    crate::parser::parse_channel_count(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "channel_audio_metadata" => write_optional_json(
                    crate::parser::parse_channel_audio_metadata(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "device_name" => write_optional_json(
                    responses::parse_device_name(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "device_info" => write_optional_json(
                    responses::parse_device_info(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "device_settings" => write_optional_json(
                    responses::parse_device_settings(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "property_directory" => write_optional_json(
                    responses::parse_property_directory(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "aes67_configured" => write_optional_json(
                    responses::parse_aes67_configured(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "make_model" => write_optional_json(
                    responses::parse_make_model(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "dante_model" => write_optional_json(
                    responses::parse_dante_model(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "bluetooth_status" => write_optional_json(
                    responses::parse_bluetooth_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "conmon_opcode" => write_optional_json(
                    responses::parse_conmon_opcode(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "ptp_clock_status" => write_optional_json(
                    responses::parse_ptp_clock_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "aes67_status" => write_optional_json(
                    responses::parse_aes67_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "lock_reset_status" => write_optional_json(
                    responses::parse_lock_reset_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "conmon_export_fragment" => write_optional_json(
                    responses::parse_conmon_export_fragment(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "clear_configuration_status" => write_optional_json(
                    responses::parse_clear_configuration_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "routing_capacity_status" => write_optional_json(
                    responses::parse_routing_capacity_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "interface_status" => write_optional_json(
                    responses::parse_interface_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "switch_configuration_status" => write_optional_json(
                    responses::parse_switch_configuration_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "sample_rate_status" => write_optional_json(
                    responses::parse_sample_rate_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "unmapped_0086_status" => write_optional_json(
                    responses::parse_unmapped_0086_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "unmapped_00e0_status" => write_optional_json(
                    responses::parse_unmapped_00e0_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "unmapped_0106_status" => write_optional_json(
                    responses::parse_unmapped_0106_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "unmapped_0102_status" => write_optional_json(
                    responses::parse_unmapped_0102_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "unmapped_0024_status" => write_optional_json(
                    responses::parse_unmapped_0024_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "unmapped_0022_status" => write_optional_json(
                    responses::parse_unmapped_0022_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "unmapped_0026_status" => write_optional_json(
                    responses::parse_unmapped_0026_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "unmapped_0040_status" => write_optional_json(
                    responses::parse_unmapped_0040_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "encoding_status" => write_optional_json(
                    responses::parse_encoding_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "sample_rate_pullup_status" => write_optional_json(
                    responses::parse_sample_rate_pullup_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "gain_status" => write_optional_json(
                    responses::parse_gain_status(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "metering" => write_optional_json(
                    responses::parse_metering_frame(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "signal_presence" => write_optional_json(
                    crate::signal_presence::parse_signal_presence_packet(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "heartbeat_clock_frequency_offset" => write_optional_json(
                    crate::heartbeat_clock::parse_heartbeat_clock_frequency_offset_packet(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "heartbeat_identity" => write_optional_json(
                    crate::heartbeat::parse_heartbeat_identity(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "heartbeat_connection_health" => write_optional_json(
                    crate::heartbeat_connection_health::parse_heartbeat_connection_health_packet(
                        bytes,
                    ),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "heartbeat_interface_traffic" => write_optional_json(
                    crate::heartbeat_interface_traffic::parse_heartbeat_interface_traffic_packet(
                        bytes,
                    ),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "result_code" => write_optional_json(
                    responses::parse_result_code(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "tx_flows" => write_optional_json(
                    responses::parse_tx_flows(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "tx_flow_page" => write_optional_json(
                    responses::parse_tx_flow_page(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "transmitter_flow_status_page" => write_optional_json(
                    responses::parse_transmitter_flow_status_page(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "transmitter_channel_status_page_2809" => write_optional_json(
                    responses::parse_transmitter_channel_status_page_2809(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "transmitter_channel_name_reconciliation_2809" => write_optional_json(
                    responses::parse_transmitter_channel_name_reconciliation_2809(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "receiver_channel_status_page_2809" => write_optional_json(
                    responses::parse_receiver_channel_status_page_2809(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "receiver_flow_status_page_2809" => write_optional_json(
                    responses::parse_receiver_flow_status_page_2809(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "receiver_flow_page" => write_optional_json(
                    responses::parse_receiver_flow_page(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "receiver_port_ranges" => write_optional_json(
                    responses::parse_receiver_port_ranges(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "transmit_channel_capabilities" => write_optional_json(
                    responses::parse_transmit_channel_capabilities(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "cmc_registration" => write_optional_json(
                    responses::parse_cmc_registration_response(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "dante_brooklyn_control_protocol_flow_setup_request" => write_optional_json(
                    responses::parse_dante_brooklyn_control_protocol_flow_setup_request(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "dante_brooklyn_control_protocol_flow_setup_response" => write_optional_json(
                    responses::parse_dante_brooklyn_control_protocol_flow_setup_response(bytes),
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                _ => NetaudioStatus::InvalidChannelType,
            }
        }
    })
}
