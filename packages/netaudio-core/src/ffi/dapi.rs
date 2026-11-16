use super::*;

/// Validate a credential without echoing its contents in error messages.
#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_validate_credential(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let credential: crate::dapi::ManagedCredential = decode_json(c_string(json)?)?;
            credential
                .validate()
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidJson, message))?;
            Ok(())
        })
    }
}

/// Advance a transport-independent managed operation. The input state is not mutated,
/// so output buffer sizing and retries do not send or consume protocol messages.
#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_advance_session(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            crate::managed_session::advance(request)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidSequence, message))
        })
    }
}
