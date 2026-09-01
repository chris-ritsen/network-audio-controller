use super::*;

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_execute(
    client: *mut NetaudioClient,
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard_panic(|| {
        let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
            Ok(client) => client,
            Err(status) => return status,
        };
        if json.is_null() {
            return NetaudioStatus::NullPointer;
        }

        let json = match unsafe { CStr::from_ptr(json) }.to_str() {
            Ok(value) => value,
            Err(_) => return NetaudioStatus::InvalidUtf8,
        };

        let response = match lock_client(client).execute(json) {
            Ok(response) => response,
            Err(error) => return error.into(),
        };

        unsafe { write_bytes(&response, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_lock_token(
    pin: *const c_char,
    nonce: *const u8,
    nonce_len: usize,
    key: *const u8,
    key_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard_panic(|| {
        if let Err(status) = unsafe { prepare_output(out_buffer, out_capacity, out_length) } {
            return status;
        }
        if pin.is_null() || nonce.is_null() || key.is_null() {
            return NetaudioStatus::NullPointer;
        }
        if nonce_len != crate::lock::NONCE_LENGTH {
            return NetaudioStatus::CryptoError;
        }
        if key_len != crate::lock::KEY_LENGTH {
            return NetaudioStatus::InvalidKey;
        }
        let pin = match unsafe { CStr::from_ptr(pin) }.to_str() {
            Ok(value) => value,
            Err(_) => return NetaudioStatus::InvalidUtf8,
        };
        let nonce = unsafe { std::slice::from_raw_parts(nonce, nonce_len) };
        let key = unsafe { std::slice::from_raw_parts(key, key_len) };

        let token = match crate::lock::compute_token(pin, nonce, key) {
            Ok(token) => token,
            Err(error) => return error.into(),
        };

        unsafe { write_bytes(&token, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_lock(
    client: *mut NetaudioClient,
    pin: *const c_char,
    key: *const u8,
    key_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard_panic(|| unsafe {
        lock_or_unlock(
            client,
            pin,
            key,
            key_len,
            out_buffer,
            out_capacity,
            out_length,
            true,
        )
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_unlock(
    client: *mut NetaudioClient,
    pin: *const c_char,
    key: *const u8,
    key_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard_panic(|| unsafe {
        lock_or_unlock(
            client,
            pin,
            key,
            key_len,
            out_buffer,
            out_capacity,
            out_length,
            false,
        )
    })
}

#[allow(clippy::too_many_arguments)]
unsafe fn lock_or_unlock(
    client: *mut NetaudioClient,
    pin: *const c_char,
    key: *const u8,
    key_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
    locking: bool,
) -> NetaudioStatus {
    let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
        Ok(client) => client,
        Err(status) => return status,
    };
    if pin.is_null() || key.is_null() {
        return NetaudioStatus::NullPointer;
    }
    if key_len != crate::lock::KEY_LENGTH {
        return NetaudioStatus::InvalidKey;
    }
    let pin = match unsafe { CStr::from_ptr(pin) }.to_str() {
        Ok(value) => value,
        Err(_) => return NetaudioStatus::InvalidUtf8,
    };
    let key = unsafe { std::slice::from_raw_parts(key, key_len) };

    let result = if locking {
        lock_client(client).lock_device(pin, key)
    } else {
        lock_client(client).unlock_device(pin, key)
    };
    match result {
        Ok(lock_result) => unsafe {
            write_json(&lock_result, out_buffer, out_capacity, out_length)
        },
        Err(error) => error.into(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_request(
    client: *mut NetaudioClient,
    packet: *const u8,
    packet_len: usize,
    target_port: u16,
    expect_response: bool,
    repeat: u32,
    interval_ms: u64,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard_panic(|| {
        let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
            Ok(client) => client,
            Err(status) => return status,
        };
        if packet.is_null() {
            return NetaudioStatus::NullPointer;
        }
        let packet = unsafe { std::slice::from_raw_parts(packet, packet_len) };

        let response = match lock_client(client).request_raw(
            packet,
            target_port,
            expect_response,
            repeat,
            interval_ms,
        ) {
            Ok(response) => response,
            Err(error) => return error.into(),
        };

        unsafe { write_bytes(&response, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_clear_wire_captures(
    client: *mut NetaudioClient,
) -> NetaudioStatus {
    guard_panic(|| {
        if client.is_null() {
            return NetaudioStatus::NullPointer;
        }
        let client = unsafe { &*client };
        lock_client(client).clear_wire_captures();
        NetaudioStatus::Ok
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_wire_captures_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard_panic(|| {
        let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
            Ok(client) => client,
            Err(status) => return status,
        };
        let captures = lock_client(client).wire_captures();
        unsafe { write_json(&captures, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_set_host_mac(
    client: *mut NetaudioClient,
    host_mac: *const u8,
) -> NetaudioStatus {
    guard_panic(|| {
        if client.is_null() || host_mac.is_null() {
            return NetaudioStatus::NullPointer;
        }
        let client = unsafe { &*client };
        let mut mac = [0u8; 6];
        unsafe {
            ptr::copy_nonoverlapping(host_mac, mac.as_mut_ptr(), 6);
        }
        lock_client(client).set_host_mac(mac);
        NetaudioStatus::Ok
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_host_mac(out_mac: *mut u8) -> NetaudioStatus {
    guard_panic(|| {
        if out_mac.is_null() {
            return NetaudioStatus::NullPointer;
        }
        unsafe {
            ptr::write_bytes(out_mac, 0, 6);
        }
        match crate::netif::discover_host_mac() {
            Some(mac) => {
                unsafe {
                    ptr::copy_nonoverlapping(mac.as_ptr(), out_mac, 6);
                }
                NetaudioStatus::Ok
            }
            None => NetaudioStatus::IoError,
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_free(client: *mut NetaudioClient) {
    if client.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(client) });
}
