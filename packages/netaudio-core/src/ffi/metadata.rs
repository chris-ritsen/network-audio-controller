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

#[no_mangle]
pub unsafe extern "C" fn netaudio_build_command(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let json = unsafe { c_string(json)? };
        let packet = crate::spec::build_command_from_json(json)?;
        unsafe { write_bytes(&packet, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_subscription_status(
    code: u16,
    receiver_status_code: u16,
    has_receiver_status: bool,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let status = crate::subscription_status::decode(
            code,
            has_receiver_status.then_some(receiver_status_code),
        );
        unsafe { write_json(&status, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_subscription_state_for_identifier(
    identifier: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let identifier = unsafe { c_string(identifier)? };
        let state = crate::subscription_status::state_for_identifier(identifier);
        unsafe { write_json(&state, out_buffer, out_capacity, out_length) }
    })
}
