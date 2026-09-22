use super::*;

/// Return a canonical MAC/device identity as a JSON string, or null if unavailable.
/// Accepts hexadecimal MAC/device IDs with colon, hyphen, or dot separators.
/// Known 64-bit MAC-derived representations collapse to 48 bits; other 64-bit
/// identities remain intact. Invalid and all-zero identities return null.
#[no_mangle]
pub unsafe extern "C" fn netaudio_canonical_device_mac(
    value: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let value = c_string(value)?;
            let identity = crate::device_identity::canonical_device_mac(value);

            Ok(identity)
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
