use super::*;

/// Select a supported flow-inventory revision from observed or advertised evidence.
#[no_mangle]
pub unsafe extern "C" fn netaudio_flow_inventory_protocol(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            crate::protocol::flow_inventory_protocol(decode_json(c_string(json)?)?).map_err(
                |message| FfiError::new(NetaudioStatus::UnsupportedProtocolOperation, message),
            )
        })
    }
}

/// Classify an announcement for an existing, unexpired SAP session.
#[no_mangle]
pub unsafe extern "C" fn netaudio_sap_transition(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            Ok(crate::sap::transition(decode_json(c_string(json)?)?))
        })
    }
}

/// Normalize an explicitly identified MAC, clock, or managed identity.
/// Returns a JSON string, or null when the supplied value is invalid for its kind.
#[no_mangle]
pub unsafe extern "C" fn netaudio_device_identity(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            Ok(crate::device_identity::normalize(request))
        })
    }
}

#[no_mangle]
/// Resolve advertised ARC version metadata. Null means unadvertised, not a
/// default revision. Managed control selects its observed transport revision.
pub unsafe extern "C" fn netaudio_arc_protocol(
    version: *const c_char,
    managed: bool,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let version = if version.is_null() {
                None
            } else {
                Some(c_string(version)?)
            };
            let protocol = crate::protocol::arc_protocol(version, managed).map_err(|message| {
                FfiError::new(NetaudioStatus::UnsupportedProtocolOperation, message)
            })?;

            Ok(protocol)
        })
    }
}

/// Allocate from the library's shared nonzero transaction counter, wrapping after 65535.
#[no_mangle]
pub extern "C" fn netaudio_next_message_id() -> u16 {
    crate::protocol::allocate_message_id()
}
