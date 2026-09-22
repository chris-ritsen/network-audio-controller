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

/// Verify a configurable audio value against applied, not merely requested, readback.
#[no_mangle]
pub unsafe extern "C" fn netaudio_audio_capability_readback(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::responses::AudioCapabilityReadback = decode_json(json)?;

            Ok(request.resolve())
        })
    }
}

/// Resolve a latency request from its acknowledgement and configured-state readback.
#[no_mangle]
pub unsafe extern "C" fn netaudio_latency_control(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::latency_configuration::LatencyControl = decode_json(json)?;
            let result = request.resolve()?;

            Ok(result)
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

/// Interpret audio update mode, advertised choices, and host-disabled state.
#[no_mangle]
pub unsafe extern "C" fn netaudio_audio_capability_control(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::responses::AudioCapabilityControl = decode_json(json)?;
            let reasons = crate::responses::audio_capability_control(&request);

            Ok(reasons)
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

/// Project a parsed device-settings JSON object into latency state and device controls.
/// Uses active/configured/default/min/max_latency_ns fields; other settings are ignored.
/// Null bounds remain unknown. Choices and fixed-latency fallback share the native policy.
#[no_mangle]
pub unsafe extern "C" fn netaudio_latency_configuration(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let settings: serde_json::Map<String, serde_json::Value> = decode_json(json)?;
            let configuration = crate::latency_configuration::configuration(&settings)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidJson, message))?;

            Ok(configuration)
        })
    }
}
