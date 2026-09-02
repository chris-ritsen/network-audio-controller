use super::*;

fn parse_response_kind(kind: &str, bytes: &[u8]) -> Result<Vec<u8>, FfiError> {
    use crate::responses;
    match kind {
        "aes67_configured" => serialize_optional(kind, responses::parse_aes67_configured(bytes)),
        "aes67_status" => serialize_optional(kind, responses::parse_aes67_status(bytes)),
        "bluetooth_status" => serialize_optional(kind, responses::parse_bluetooth_status(bytes)),
        "channel_audio_metadata" => {
            serialize_optional(kind, crate::parser::parse_channel_audio_metadata(bytes))
        }
        "channel_count" => serialize_optional(kind, crate::parser::parse_channel_count(bytes)),
        "clear_configuration_status" => {
            serialize_optional(kind, responses::parse_clear_configuration_status(bytes))
        }
        "cmc_registration" => {
            serialize_optional(kind, responses::parse_cmc_registration_response(bytes))
        }
        "conmon_export_fragment" => {
            serialize_optional(kind, responses::parse_conmon_export_fragment(bytes))
        }
        "conmon_opcode" => serialize_optional(kind, responses::parse_conmon_opcode(bytes)),
        "dante_brooklyn_control_protocol_flow_setup_request" => serialize_optional(
            kind,
            responses::parse_dante_brooklyn_control_protocol_flow_setup_request(bytes),
        ),
        "dante_brooklyn_control_protocol_flow_setup_response" => serialize_optional(
            kind,
            responses::parse_dante_brooklyn_control_protocol_flow_setup_response(bytes),
        ),
        "dante_model" => serialize_optional(kind, responses::parse_dante_model(bytes)),
        "device_info" => serialize_optional(kind, responses::parse_device_info(bytes)),
        "device_name" => serialize_optional(kind, responses::parse_device_name(bytes)),
        "device_settings" => serialize_optional(kind, responses::parse_device_settings(bytes)),
        "encoding_status" => serialize_optional(kind, responses::parse_encoding_status(bytes)),
        "gain_status" => serialize_optional(kind, responses::parse_gain_status(bytes)),
        "heartbeat_clock_frequency_offset" => serialize_optional(
            kind,
            crate::heartbeat_clock::parse_heartbeat_clock_frequency_offset_packet(bytes),
        ),
        "heartbeat_connection_health" => serialize_optional(
            kind,
            crate::heartbeat_connection_health::parse_heartbeat_connection_health_packet(bytes),
        ),
        "heartbeat_identity" => {
            serialize_optional(kind, crate::heartbeat::parse_heartbeat_identity(bytes))
        }
        "heartbeat_interface_traffic" => serialize_optional(
            kind,
            crate::heartbeat_interface_traffic::parse_heartbeat_interface_traffic_packet(bytes),
        ),
        "interface_status" => serialize_optional(kind, responses::parse_interface_status(bytes)),
        "lock_reset_status" => serialize_optional(kind, responses::parse_lock_reset_status(bytes)),
        "make_model" => serialize_optional(kind, responses::parse_make_model(bytes)),
        "metering" => serialize_optional(kind, responses::parse_metering_frame(bytes)),
        "property_directory" => {
            serialize_optional(kind, responses::parse_property_directory(bytes))
        }
        "ptp_clock_status" => serialize_optional(kind, responses::parse_ptp_clock_status(bytes)),
        "receiver_channel_status_page_2809" => serialize_optional(
            kind,
            responses::parse_receiver_channel_status_page_2809(bytes),
        ),
        "receiver_flow_page" => {
            serialize_optional(kind, responses::parse_receiver_flow_page(bytes))
        }
        "receiver_flow_status_page_2809" => {
            serialize_optional(kind, responses::parse_receiver_flow_status_page_2809(bytes))
        }
        "receiver_port_ranges" => {
            serialize_optional(kind, responses::parse_receiver_port_ranges(bytes))
        }
        "result_code" => serialize_optional(kind, responses::parse_result_code(bytes)),
        "routing_capacity_status" => {
            serialize_optional(kind, responses::parse_routing_capacity_status(bytes))
        }
        "sample_rate_pullup_status" => {
            serialize_optional(kind, responses::parse_sample_rate_pullup_status(bytes))
        }
        "sample_rate_status" => {
            serialize_optional(kind, responses::parse_sample_rate_status(bytes))
        }
        "signal_presence" => serialize_optional(
            kind,
            crate::signal_presence::parse_signal_presence_packet(bytes),
        ),
        "switch_configuration_status" => {
            serialize_optional(kind, responses::parse_switch_configuration_status(bytes))
        }
        "transmit_channel_capabilities" => {
            serialize_optional(kind, responses::parse_transmit_channel_capabilities(bytes))
        }
        "transmitter_channel_name_reconciliation_2809" => serialize_optional(
            kind,
            responses::parse_transmitter_channel_name_reconciliation_2809(bytes),
        ),
        "transmitter_channel_status_page_2809" => serialize_optional(
            kind,
            responses::parse_transmitter_channel_status_page_2809(bytes),
        ),
        "transmitter_flow_status_page" => {
            serialize_optional(kind, responses::parse_transmitter_flow_status_page(bytes))
        }
        "tx_flow_page" => serialize_optional(kind, responses::parse_tx_flow_page(bytes)),
        "tx_flows" => serialize_optional(kind, responses::parse_tx_flows(bytes)),
        "unmapped_0022_status" => {
            serialize_optional(kind, responses::parse_unmapped_0022_status(bytes))
        }
        "unmapped_0024_status" => {
            serialize_optional(kind, responses::parse_unmapped_0024_status(bytes))
        }
        "unmapped_0026_status" => {
            serialize_optional(kind, responses::parse_unmapped_0026_status(bytes))
        }
        "unmapped_0040_status" => {
            serialize_optional(kind, responses::parse_unmapped_0040_status(bytes))
        }
        "unmapped_0086_status" => {
            serialize_optional(kind, responses::parse_unmapped_0086_status(bytes))
        }
        "unmapped_00e0_status" => {
            serialize_optional(kind, responses::parse_unmapped_00e0_status(bytes))
        }
        "unmapped_0102_status" => {
            serialize_optional(kind, responses::parse_unmapped_0102_status(bytes))
        }
        "unmapped_0106_status" => {
            serialize_optional(kind, responses::parse_unmapped_0106_status(bytes))
        }
        _ => Err(FfiError::new(
            NetaudioStatus::UnknownKind,
            format!("unknown response kind {kind:?}"),
        )),
    }
}

fn serialize_optional<T: serde::Serialize>(
    kind: &str,
    value: Option<T>,
) -> Result<Vec<u8>, FfiError> {
    let value = value.ok_or_else(|| {
        FfiError::new(
            NetaudioStatus::MalformedResponse,
            format!("bytes did not parse as a {kind} response"),
        )
    })?;
    serde_json::to_vec(&value).map_err(|error| {
        FfiError::new(
            NetaudioStatus::SerializationError,
            format!("could not serialize result: {error}"),
        )
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_parse_response(
    kind: *const c_char,
    data: *const u8,
    data_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let kind = unsafe { c_string(kind)? };
        if data.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        let bytes = unsafe { std::slice::from_raw_parts(data, data_len) };
        let serialized = parse_response_kind(kind, bytes)?;
        unsafe { write_bytes(&serialized, out_buffer, out_capacity, out_length) }
    })
}
