use super::*;

/// Decide analog read/write access from advertised support and current transport/lock facts.
#[no_mangle]
pub unsafe extern "C" fn netaudio_analog_access(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let facts: crate::responses::AnalogAccess = decode_json(c_string(json)?)?;
            Ok(facts.denial())
        })
    }
}

/// Return analog reference-level labels and channel directions.
#[no_mangle]
pub unsafe extern "C" fn netaudio_gain_metadata(
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            Ok(crate::responses::gain_metadata())
        })
    }
}

/// Resolve an analog level request against a decoded gain adapter.
/// Input contains adapter, channel, level, and optional direction.
#[no_mangle]
pub unsafe extern "C" fn netaudio_analog_level_control(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::responses::AnalogLevelRequest = decode_json(json)?;
            let plan = crate::responses::analog_level_control(request);

            Ok(plan)
        })
    }
}
