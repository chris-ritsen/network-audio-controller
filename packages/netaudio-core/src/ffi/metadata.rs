use super::*;

#[no_mangle]
pub extern "C" fn netaudio_abi_version() -> u32 {
    NETAUDIO_ABI_VERSION
}

#[no_mangle]
pub extern "C" fn netaudio_status_name(status: i32) -> *const c_char {
    let name: &'static [u8] = match status {
        0 => b"ok\0",
        1 => b"null_pointer\0",
        2 => b"invalid_utf8\0",
        3 => b"name_too_long\0",
        4 => b"name_invalid_hyphen\0",
        5 => b"name_invalid_chars\0",
        6 => b"buffer_too_small\0",
        7 => b"invalid_address\0",
        8 => b"io_error\0",
        9 => b"timeout\0",
        10 => b"malformed_response\0",
        11 => b"serialization_error\0",
        12 => b"subscription_count\0",
        13 => b"invalid_json\0",
        14 => b"invalid_mac\0",
        15 => b"invalid_ip\0",
        16 => b"invalid_channel_type\0",
        17 => b"invalid_key\0",
        18 => b"invalid_pin\0",
        19 => b"crypto_error\0",
        20 => b"invalid_page\0",
        21 => b"invalid_subscription_channel\0",
        22 => b"invalid_device_type\0",
        23 => b"packet_too_large\0",
        24 => b"invalid_channel\0",
        25 => b"invalid_latency\0",
        26 => b"invalid_sample_rate\0",
        27 => b"invalid_encoding\0",
        28 => b"invalid_gain_level\0",
        29 => b"invalid_flow_slot\0",
        30 => b"invalid_flow_protocol\0",
        31 => b"invalid_sequence\0",
        32 => b"unsupported_protocol_operation\0",
        33 => b"internal_panic\0",
        _ => b"unknown\0",
    };
    name.as_ptr() as *const c_char
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_build_command(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard_panic(|| {
        if let Err(status) = unsafe { prepare_output(out_buffer, out_capacity, out_length) } {
            return status;
        }
        if json.is_null() {
            return NetaudioStatus::NullPointer;
        }

        let json = match unsafe { CStr::from_ptr(json) }.to_str() {
            Ok(value) => value,
            Err(_) => return NetaudioStatus::InvalidUtf8,
        };

        let packet = match crate::spec::build_command_from_json(json) {
            Ok(packet) => packet,
            Err(error) => return error.into(),
        };

        unsafe { write_bytes(&packet, out_buffer, out_capacity, out_length) }
    })
}
