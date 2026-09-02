use super::*;

use crate::client::fire_repeated;
use crate::spec::IoMode;

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_execute(
    client: *mut NetaudioClient,
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        let client = unsafe { nonnull_client(client, out_buffer, out_capacity, out_length)? };
        let json = unsafe { c_string(json)? };
        let prepared = lock_client(client).prepare_command(json)?;
        let response = match prepared.io {
            IoMode::Fire {
                repeat,
                interval_ms,
            } => fire_repeated(repeat, interval_ms, || {
                lock_client(client).send_prepared(&prepared)
            })?,
            IoMode::Request => lock_client(client).request_prepared(&prepared)?,
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
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        if nonce.is_null() || key.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        let pin = unsafe { c_string(pin)? };
        if nonce_len != crate::lock::NONCE_LENGTH {
            return Err(crate::lock::LockError::InvalidNonce.into());
        }
        if key_len != crate::lock::KEY_LENGTH {
            return Err(crate::lock::LockError::InvalidKey.into());
        }
        let nonce = unsafe { std::slice::from_raw_parts(nonce, nonce_len) };
        let key = unsafe { std::slice::from_raw_parts(key, key_len) };
        let token = crate::lock::compute_token(pin, nonce, key)?;
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
    guard(|| unsafe {
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
    guard(|| unsafe {
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
) -> FfiResult {
    let client = unsafe { nonnull_client(client, out_buffer, out_capacity, out_length)? };
    if key.is_null() {
        return Err(NetaudioStatus::NullPointer.into());
    }
    let pin = unsafe { c_string(pin)? };
    if key_len != crate::lock::KEY_LENGTH {
        return Err(crate::lock::LockError::InvalidKey.into());
    }
    let key = unsafe { std::slice::from_raw_parts(key, key_len) };

    let lock_result = if locking {
        lock_client(client).lock_device(pin, key)?
    } else {
        lock_client(client).unlock_device(pin, key)?
    };
    unsafe { write_json(&lock_result, out_buffer, out_capacity, out_length) }
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
    guard(|| {
        let client = unsafe { nonnull_client(client, out_buffer, out_capacity, out_length)? };
        if packet.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        let packet = unsafe { std::slice::from_raw_parts(packet, packet_len) };
        let response = if expect_response {
            lock_client(client).request_raw(packet, target_port)?
        } else {
            fire_repeated(repeat, interval_ms, || {
                lock_client(client).send_raw(packet, target_port)
            })?
        };
        unsafe { write_bytes(&response, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_clear_wire_captures(
    client: *mut NetaudioClient,
) -> NetaudioStatus {
    guard(|| {
        if client.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        let client = unsafe { &*client };
        lock_client(client).clear_wire_captures();
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_wire_captures_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        let client = unsafe { nonnull_client(client, out_buffer, out_capacity, out_length)? };
        let captures = lock_client(client).wire_captures();
        unsafe { write_json(&captures, out_buffer, out_capacity, out_length) }
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_set_host_mac(
    client: *mut NetaudioClient,
    host_mac: *const u8,
) -> NetaudioStatus {
    guard(|| {
        if client.is_null() || host_mac.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        let client = unsafe { &*client };
        let mut mac = [0u8; 6];
        unsafe {
            ptr::copy_nonoverlapping(host_mac, mac.as_mut_ptr(), 6);
        }
        lock_client(client).set_host_mac(mac);
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_host_mac(out_mac: *mut u8) -> NetaudioStatus {
    guard(|| {
        if out_mac.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        unsafe {
            ptr::write_bytes(out_mac, 0, 6);
        }
        let mac = crate::netif::discover_host_mac().ok_or_else(|| {
            FfiError::new(
                NetaudioStatus::IoError,
                "no interface with a non-zero MAC address routes to the Dante multicast group",
            )
        })?;
        unsafe {
            ptr::copy_nonoverlapping(mac.as_ptr(), out_mac, 6);
        }
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_free(client: *mut NetaudioClient) {
    if client.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(client) });
}
