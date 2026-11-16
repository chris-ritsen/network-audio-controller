use super::*;

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
