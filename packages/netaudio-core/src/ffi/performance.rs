use super::*;

/// Resolve fresh performance readback and acknowledgement independently.
/// Storage acknowledgements never establish persistence.
#[no_mangle]
pub unsafe extern "C" fn netaudio_performance_completion(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            Ok(crate::performance_configuration::completion(request)?)
        })
    }
}

/// Resolve performance capabilities from protocol_id, managed, property_ids, and platform_software_version.
/// Return normalized software version, supported property IDs, and per-operation availability.
#[no_mangle]
pub unsafe extern "C" fn netaudio_performance_capabilities(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let facts: crate::performance_configuration::PerformanceFacts = decode_json(json)?;
            let capabilities = crate::performance_configuration::capabilities(facts)?;

            Ok(capabilities)
        })
    }
}

/// Project advertised performance property_ids and observed values into reusable control settings.
/// values maps decimal property IDs to observed integers. Incomplete or conflicting settings are omitted.
#[no_mangle]
pub unsafe extern "C" fn netaudio_performance_snapshot(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let facts: crate::performance_configuration::PerformanceSnapshotFacts =
                decode_json(json)?;
            let snapshot = crate::performance_configuration::snapshot(facts);

            Ok(snapshot)
        })
    }
}

/// Validate a performance command and return its ordered property_id/value records as JSON.
#[no_mangle]
pub unsafe extern "C" fn netaudio_plan_performance_command(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let properties = crate::spec::plan_performance_command(json)?;

            Ok(properties)
        })
    }
}
