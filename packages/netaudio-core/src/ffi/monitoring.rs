use super::*;

/// Return detailed and signal_presence scales, each indexed by the raw byte value.
/// Each entry contains dbfs (null for special values) and state. Callers must select
/// the scale matching the sample source; heartbeat dBFS values are estimates.
#[no_mangle]
pub unsafe extern "C" fn netaudio_metering_scale(
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            Ok(crate::metering::metering_scale())
        })
    }
}

/// Interpret parsed connection-health records and prior stream observations.
/// Returns null for replayed or absent updates. Invalid evidence changes no state.
#[no_mangle]
pub unsafe extern "C" fn netaudio_connection_health_update(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let input = c_string(json)?;
            let request = decode_json(input)?;
            let update = crate::heartbeat_connection_health::plan_update(request)
                .map_err(crate::spec::SpecError::InvalidJson)?;

            Ok(update)
        })
    }
}
