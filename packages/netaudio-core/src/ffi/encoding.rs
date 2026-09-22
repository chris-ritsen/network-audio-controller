use super::*;

/// Advance a sender's publication counter, including zero when it wraps.
#[no_mangle]
pub extern "C" fn netaudio_next_publication_id(previous: u16) -> u16 {
    crate::publications::next_publication_id(previous)
}

/// Construct virtual-device discovery records without registering network services.
#[no_mangle]
pub unsafe extern "C" fn netaudio_virtual_device_advertisements(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            Ok(crate::discovery::virtual_device(decode_json(c_string(
                json,
            )?)?))
        })
    }
}

/// Build a managed-control packet and its transport/completion plan from {specification, host_mac, message_id}.
/// The packet is a JSON byte array; host_mac is an optional default, and explicit command identities are preserved.
#[no_mangle]
pub unsafe extern "C" fn netaudio_build_managed_command(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::spec::ManagedCommandRequest = decode_json(json)?;
            let plan = crate::spec::build_managed_command(request)?;

            Ok(plan)
        })
    }
}

/// Derive channel metadata bytes and the discovery PCM property from one configuration.
/// Input: sample_rate, encoding, supported_encodings. Returns JSON null when the
/// encoding capabilities are unknown or inconsistent, otherwise channel_metadata
/// (byte array) and pcm_property (string).
#[no_mangle]
pub unsafe extern "C" fn netaudio_channel_audio_publication(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let config: crate::parser::ChannelAudioConfiguration = decode_json(json)?;
            let publication = crate::parser::channel_audio_publication(&config);

            Ok(publication)
        })
    }
}

/// Encode {source_ip, message_id, publication} without opening a transport.
/// Publication kinds are audio (capability, current_value, supported_values),
/// heartbeat (tx_count, rx_count), and raw (protocol_id, eight-byte opcode, body).
#[no_mangle]
pub unsafe extern "C" fn netaudio_build_publication(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        bytes_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let publication: crate::publications::Publication = decode_json(json)?;
            let packet = publication.packet()?;
            Ok(packet)
        })
    }
}

/// Build an ARC response from protocol_id, transaction_id, result_code, and response JSON.
/// Response kinds: raw (opcode, body byte array), device_name (name),
/// channel_count (tx_count, rx_count), device_info (model_name, display_name,
/// model_code, port), device_settings (sample_rate and default, configured,
/// active, maximum, minimum latency_ns fields), transmitter_names (names array).
/// channel_status accepts channel_type (rx/tx), channels (name and optional
/// source with channel/device), and audio (sample_rate, encoding, supported_encodings).
/// Unsupported audio metadata produces a rejection reply. Layouts are for virtual devices.
#[no_mangle]
pub unsafe extern "C" fn netaudio_build_response(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        bytes_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let response: crate::publications::ArcResponse = decode_json(json)?;
            let packet = response.packet()?;

            Ok(packet)
        })
    }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_build_command(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        bytes_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let packet = crate::spec::build_command_from_json(json)?;
            Ok(packet)
        })
    }
}
