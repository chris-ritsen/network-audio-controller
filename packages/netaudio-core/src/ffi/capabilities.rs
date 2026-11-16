use super::*;

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
