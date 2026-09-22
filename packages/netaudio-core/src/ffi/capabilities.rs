use super::*;

/// Resolve settings capabilities from observed values and advertised property entries.
#[no_mangle]
pub unsafe extern "C" fn netaudio_settings_capabilities(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let facts = decode_json(c_string(json)?)?;
            Ok(crate::capabilities::settings_capabilities(facts))
        })
    }
}

/// Resolve receiver self-connection evidence. Input: authority (direct, managed,
/// or observed) and channels containing direct/managed optional booleans and an
/// explicit managed_fresh boolean. Returns per-channel supported/conflict and
/// aggregate support. Observed authority is for display, not permission to write.
#[no_mangle]
pub unsafe extern "C" fn netaudio_receiver_self_connection_capabilities(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let input = c_string(json)?;
            let result = crate::receiver_capabilities::self_connection(input)?;

            Ok(result)
        })
    }
}

/// Validate sample-rate status, advertised capacities, or active receiver identities.
/// The tagged request selects evidence; malformed or conflicting reports are rejected.
#[no_mangle]
pub unsafe extern "C" fn netaudio_sample_rate_evidence(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let input = c_string(json)?;
            let request: crate::sample_rate_topology::EvidenceRequest = decode_json(input)?;
            let result = request
                .validate()
                .map_err(crate::spec::SpecError::InvalidJson)?;

            Ok(result)
        })
    }
}

/// Evaluate operation availability from explicit capability, lock, transport,
/// and permission facts. Unknown support remains distinct from a denied write.
#[no_mangle]
pub unsafe extern "C" fn netaudio_operation_availability(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let input = c_string(json)?;
            let request = decode_json(input)?;
            let result = crate::capabilities::availability(request);

            Ok(result)
        })
    }
}
