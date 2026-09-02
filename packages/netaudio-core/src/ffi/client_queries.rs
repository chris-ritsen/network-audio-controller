use super::*;

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_new(
    device_ip: *const c_char,
    arc_port: u16,
    timeout_milliseconds: u32,
    attempts: u32,
    out_client: *mut *mut NetaudioClient,
) -> NetaudioStatus {
    guard(|| {
        if out_client.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        unsafe {
            *out_client = ptr::null_mut();
        }
        let device_ip = unsafe { c_string(device_ip)? };
        let device_ip: IpAddr = device_ip.parse().map_err(|_| {
            FfiError::new(
                NetaudioStatus::InvalidAddress,
                format!("device address {device_ip:?} is not an IP address"),
            )
        })?;
        let client = Client::new(
            device_ip,
            arc_port,
            Duration::from_millis(timeout_milliseconds as u64),
            attempts,
        )?;
        unsafe {
            *out_client = Box::into_raw(Box::new(NetaudioClient::new(client)));
        }
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_set_device_name(
    client: *mut NetaudioClient,
    name: *const c_char,
) -> NetaudioStatus {
    guard(|| {
        if client.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        let name = unsafe { c_string(name)? };
        let client = unsafe { &*client };
        lock_client(client).set_device_name(name)?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_channel_count(
    client: *mut NetaudioClient,
    out_tx_count: *mut u16,
    out_rx_count: *mut u16,
    out_locked: *mut i32,
) -> NetaudioStatus {
    guard(|| {
        if !out_tx_count.is_null() {
            unsafe {
                *out_tx_count = 0;
            }
        }
        if !out_rx_count.is_null() {
            unsafe {
                *out_rx_count = 0;
            }
        }
        if !out_locked.is_null() {
            unsafe {
                *out_locked = -1;
            }
        }
        if client.is_null()
            || out_tx_count.is_null()
            || out_rx_count.is_null()
            || out_locked.is_null()
        {
            return Err(NetaudioStatus::NullPointer.into());
        }

        let client = unsafe { &*client };
        let count = lock_client(client).get_channel_count()?;
        unsafe {
            *out_tx_count = count.tx_count;
            *out_rx_count = count.rx_count;
            *out_locked = match count.locked {
                Some(true) => 1,
                Some(false) => 0,
                None => -1,
            };
        }
        Ok(())
    })
}

unsafe fn client_json_query<T: serde::Serialize>(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
    query: impl FnOnce(&mut Client) -> Result<T, ClientError>,
) -> FfiResult {
    let client = unsafe { nonnull_client(client, out_buffer, out_capacity, out_length)? };
    let value = query(&mut lock_client(client))?;
    unsafe { write_json(&value, out_buffer, out_capacity, out_length) }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_rx_channels_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| unsafe {
        client_json_query(client, out_buffer, out_capacity, out_length, |client| {
            client.get_rx_channels()
        })
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_rx_inventory_json(
    client: *mut NetaudioClient,
    rx_count: u16,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| unsafe {
        client_json_query(client, out_buffer, out_capacity, out_length, |client| {
            client.get_rx_inventory(rx_count)
        })
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_tx_channels_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| unsafe {
        client_json_query(client, out_buffer, out_capacity, out_length, |client| {
            client.get_tx_channels()
        })
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_device_name_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| unsafe {
        client_json_query(client, out_buffer, out_capacity, out_length, |client| {
            client.get_device_name()
        })
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_device_info_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| unsafe {
        client_json_query(client, out_buffer, out_capacity, out_length, |client| {
            client.get_device_info()
        })
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_device_settings_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| unsafe {
        client_json_query(client, out_buffer, out_capacity, out_length, |client| {
            client.get_device_settings()
        })
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_property_directory_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| unsafe {
        client_json_query(client, out_buffer, out_capacity, out_length, |client| {
            client.get_property_directory()
        })
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_aes67_configured(
    client: *mut NetaudioClient,
    out_state: *mut i32,
) -> NetaudioStatus {
    guard(|| {
        if !out_state.is_null() {
            unsafe {
                *out_state = -1;
            }
        }
        if client.is_null() || out_state.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        let client = unsafe { &*client };
        let state = lock_client(client).get_aes67_configured()?;
        unsafe {
            *out_state = match state {
                Some(true) => 1,
                Some(false) => 0,
                None => -1,
            };
        }
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_parse_page(
    kind: *const c_char,
    data: *const u8,
    data_len: usize,
    starting_channel: u16,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let kind = unsafe { c_string(kind)? };
        if data.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        let bytes = unsafe { std::slice::from_raw_parts(data, data_len) };

        use crate::parser;
        unsafe {
            match kind {
                "rx" => write_optional_json(
                    parser::parse_rx_page(bytes, starting_channel),
                    kind,
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "tx_friendly" => write_optional_json(
                    parser::parse_tx_friendly_page(bytes, starting_channel),
                    kind,
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                "tx_info" => write_optional_json(
                    parser::parse_tx_info_page(bytes, starting_channel),
                    kind,
                    out_buffer,
                    out_capacity,
                    out_length,
                ),
                _ => Err(FfiError::new(
                    NetaudioStatus::UnknownKind,
                    format!("unknown page kind {kind:?}"),
                )),
            }
        }
    })
}
