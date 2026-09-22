use super::*;

#[no_mangle]
pub extern "C" fn netaudio_abi_version() -> u32 {
    NETAUDIO_ABI_VERSION
}

#[no_mangle]
pub extern "C" fn netaudio_status_name(status: i32) -> *const c_char {
    NetaudioStatus::from_code(status)
        .map(NetaudioStatus::name)
        .unwrap_or(c"unknown")
        .as_ptr()
}

#[no_mangle]
pub extern "C" fn netaudio_status_description(status: i32) -> *const c_char {
    NetaudioStatus::from_code(status)
        .map(NetaudioStatus::description)
        .unwrap_or(c"unknown native error")
        .as_ptr()
}

#[no_mangle]
pub extern "C" fn netaudio_status_category(status: i32) -> *const c_char {
    NetaudioStatus::from_code(status)
        .map(NetaudioStatus::category)
        .unwrap_or(c"api")
        .as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_last_error_message(
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    if out_length.is_null() || (out_buffer.is_null() && out_capacity != 0) {
        return NetaudioStatus::NullPointer;
    }
    let message = last_error_message();
    unsafe {
        *out_length = message.len();
    }
    if message.len() > out_capacity {
        return NetaudioStatus::BufferTooSmall;
    }
    if !message.is_empty() {
        unsafe {
            ptr::copy_nonoverlapping(message.as_ptr(), out_buffer, message.len());
        }
    }
    NetaudioStatus::Ok
}
