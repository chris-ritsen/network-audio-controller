use super::*;

/// Present a validated subdomain without exposing unrecognized bytes as a name.
#[no_mangle]
pub unsafe extern "C" fn netaudio_clock_subdomain_presentation(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            Ok(crate::clock_configuration::subdomain_presentation(
                &decode_json(c_string(json)?)?,
            ))
        })
    }
}

/// Derive allowed clock controls using the command encoder's capability rules.
#[no_mangle]
pub unsafe extern "C" fn netaudio_clock_control_availability(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let status = decode_json(c_string(json)?)?;
            Ok(crate::clock_configuration::control_availability(&status))
        })
    }
}

/// Resolve current clock-source name and advertised choices from current and supported JSON facts.
/// Unknown sources have no label and are not offered as choices. Internal clock is always a choice.
#[no_mangle]
pub unsafe extern "C" fn netaudio_clock_sources(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let facts: crate::clock_configuration::ClockSourceFacts = decode_json(json)?;
            let sources = crate::clock_configuration::clock_sources(facts);

            Ok(sources)
        })
    }
}

/// Normalize a JSON Latin-1 string or byte array to a 16-byte JSON array.
/// The name must have a NUL terminator followed only by zero padding.
#[no_mangle]
pub unsafe extern "C" fn netaudio_normalize_clock_subdomain(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let value: serde_json::Value = decode_json(json)?;
            let normalized = crate::clock_configuration::normalize_subdomain(&value)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidJson, message))?;

            Ok(normalized)
        })
    }
}

/// Resolve a clock revision from explicit and device-reported facts, returning a JSON integer.
/// Input keys: explicit_revision, clock_revision, model_revision, interface_revision.
/// Missing/null values are unavailable. Precedence follows that order; an explicit
/// revision conflicting with clock_revision or an invalid selected value is an error.
#[no_mangle]
pub unsafe extern "C" fn netaudio_clock_record_revision(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let facts: crate::clock_configuration::ClockRevisionFacts = decode_json(json)?;
            let revision = crate::clock_configuration::record_revision(&facts)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidJson, message))?;

            Ok(revision)
        })
    }
}

/// Normalize and validate clock changes; return requested, before, changes, and control as JSON.
/// Input: status (parsed clock status), changes, optional revisions (revision facts),
/// and optional supported_clock_sources (integer array). The status record supplies
/// clock_revision. Null changes are omitted; subdomain accepts a Latin-1 string or
/// byte array and normalizes to 16 bytes with a NUL terminator and zero padding.
/// Pass control to the clock_control command and requested to the readback comparison.
#[no_mangle]
pub unsafe extern "C" fn netaudio_plan_clock_configuration(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::clock_configuration::ClockPlanRequest = decode_json(json)?;
            let plan = crate::clock_configuration::plan_configuration(request)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidJson, message))?;

            Ok(plan)
        })
    }
}

/// Compare {status, requested} JSON and return a JSON boolean.
/// Requested settings must use normalized names and values; unknown fields are errors.
#[no_mangle]
pub unsafe extern "C" fn netaudio_clock_configuration_matches(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::clock_configuration::ClockReadbackRequest = decode_json(json)?;
            let matches = crate::clock_configuration::configuration_matches(&request)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidJson, message))?;

            Ok(matches)
        })
    }
}
