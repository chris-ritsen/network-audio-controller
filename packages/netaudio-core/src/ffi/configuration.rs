use super::*;

/// Validate configuration intent or derive restorable settings from fresh panel observations.
/// No device revision is selected and no commands are sent.
#[no_mangle]
pub unsafe extern "C" fn netaudio_configuration(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            crate::configuration::resolve(request)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidJson, message))
        })
    }
}
