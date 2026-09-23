use super::*;

/// Derive panel editor choices from the command planner and observed device state.
#[no_mangle]
pub unsafe extern "C" fn netaudio_panel_presentation(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            Ok(crate::device_controls::presentation::presentation(request))
        })
    }
}

/// Allocate a shared nonzero panel transaction sequence.
#[no_mangle]
pub extern "C" fn netaudio_next_panel_sequence() -> u32 {
    crate::device_controls::allocate_panel_sequence()
}

/// Plan a panel mutation from fresh observations and advertised capabilities.
#[no_mangle]
pub unsafe extern "C" fn netaudio_plan_panel(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            Ok(crate::device_controls::planning::plan(request))
        })
    }
}

/// Compare panel readback with the effective values from a validated plan.
#[no_mangle]
pub unsafe extern "C" fn netaudio_panel_readback_matches(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request: crate::device_controls::planning::PanelReadbackRequest =
                decode_json(c_string(json)?)?;
            Ok(crate::device_controls::planning::matches(
                &request.observed,
                &request.expected,
            ))
        })
    }
}

/// Resolve panel identity and ordered queries from advertised plugin identifiers.
#[no_mangle]
pub unsafe extern "C" fn netaudio_panel_profile(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let facts = decode_json(c_string(json)?)?;
            Ok(crate::device_controls::profile(&facts))
        })
    }
}
