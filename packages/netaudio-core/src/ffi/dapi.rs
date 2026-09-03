use super::*;

unsafe fn input_bytes<'a>(data: *const u8, data_len: usize) -> Result<&'a [u8], FfiError> {
    if data.is_null() {
        return Err(NetaudioStatus::NullPointer.into());
    }
    Ok(unsafe { std::slice::from_raw_parts(data, data_len) })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_build_session_open(
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        unsafe {
            write_bytes(
                &crate::dapi::build_session_open(),
                out_buffer,
                out_capacity,
                out_length,
            )
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_build_authentication(
    auth_token: *const u8,
    auth_token_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let auth_token = unsafe { input_bytes(auth_token, auth_token_len)? };
        let frame = crate::dapi::build_authentication(auth_token).ok_or_else(|| {
            FfiError::new(
                NetaudioStatus::InvalidLength,
                "DAPI v2 credential must be an observed 36-byte API key or 43-byte Controller token",
            )
        })?;
        unsafe { write_bytes(&frame, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_build_domain_subscription(
    domain_id: *const u8,
    domain_id_len: usize,
    subscription_id: u16,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let domain_id = unsafe { input_bytes(domain_id, domain_id_len)? };
        let frame = crate::dapi::build_domain_subscription(domain_id, subscription_id).ok_or_else(
            || {
                FfiError::new(
                    NetaudioStatus::InvalidLength,
                    "DAPI domain ID must be 16 bytes and subscription ID must be 2 through 5",
                )
            },
        )?;
        unsafe { write_bytes(&frame, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_build_device_inventory_subscription(
    domain_id: *const u8,
    domain_id_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let domain_id = unsafe { input_bytes(domain_id, domain_id_len)? };
        let frame =
            crate::dapi::build_device_inventory_subscription(domain_id).ok_or_else(|| {
                FfiError::new(
                    NetaudioStatus::InvalidLength,
                    "DAPI domain ID must be exactly 16 bytes",
                )
            })?;
        unsafe { write_bytes(&frame, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_build_inventory_initialization(
    domain_id: *const u8,
    domain_id_len: usize,
    first_message_id: u16,
    notification_port: u16,
    local_ipv4: *const u8,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let domain_id = unsafe { input_bytes(domain_id, domain_id_len)? };
        let local_ipv4 = unsafe { input_bytes(local_ipv4, 4)? };
        let frames = crate::dapi::build_inventory_initialization(
            domain_id,
            first_message_id,
            notification_port,
            local_ipv4.try_into().expect("four-byte slice"),
        )
        .ok_or_else(|| {
            FfiError::new(
                NetaudioStatus::InvalidLength,
                "DAPI domain ID must be 16 bytes; message ID and notification port must be nonzero",
            )
        })?;
        unsafe { write_bytes(&frames, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_build_identify(
    target_selector: u16,
    wrapper_id: u16,
    message_id: u16,
    host_mac: *const u8,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let host_mac = unsafe { input_bytes(host_mac, 6)? };
        let frame = crate::dapi::build_identify(
            target_selector,
            wrapper_id,
            message_id,
            host_mac.try_into().expect("six-byte slice"),
        )
        .ok_or_else(|| {
            FfiError::new(
                NetaudioStatus::InvalidSequence,
                "DAPI wrapper and message IDs must be nonzero",
            )
        })?;
        unsafe { write_bytes(&frame, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_build_arc_request(
    target_selector: u16,
    wrapper_id: u16,
    arc_packet: *const u8,
    arc_packet_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let arc_packet = unsafe { input_bytes(arc_packet, arc_packet_len)? };
        let frame = crate::dapi::build_arc_request(target_selector, wrapper_id, arc_packet)
            .ok_or_else(|| {
                FfiError::new(
                    NetaudioStatus::InvalidLength,
                    "DAPI ARC request requires a nonzero wrapper ID and a valid 0x2809 request packet",
                )
            })?;
        unsafe { write_bytes(&frame, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_build_settings_request(
    target_selector: u16,
    wrapper_id: u16,
    settings_packet: *const u8,
    settings_packet_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let settings_packet = unsafe { input_bytes(settings_packet, settings_packet_len)? };
        let frame =
            crate::dapi::build_settings_request(target_selector, wrapper_id, settings_packet)
                .ok_or_else(|| {
                    FfiError::new(
                NetaudioStatus::InvalidLength,
                "DAPI settings request requires a nonzero wrapper ID and a valid settings packet",
            )
                })?;
        unsafe { write_bytes(&frame, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_dapi_build_service_acknowledgement(
    announcement_frame: *const u8,
    announcement_frame_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let announcement_frame =
            unsafe { input_bytes(announcement_frame, announcement_frame_len)? };
        let frame =
            crate::dapi::build_service_acknowledgement(announcement_frame).ok_or_else(|| {
                FfiError::new(
                    NetaudioStatus::MalformedResponse,
                    "DAPI service announcement is malformed or unsupported",
                )
            })?;
        unsafe { write_bytes(&frame, out_buffer, out_capacity, out_length) }
    })
}
