use super::*;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    path: Option<std::path::PathBuf>,
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_lock_key_search(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request: Request = decode_json(c_string(json)?)?;
            Ok(crate::lock_key::search(request.path.as_deref()))
        })
    }
}
