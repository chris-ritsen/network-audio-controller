use super::*;

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_new(
    device_ip: *const c_char,
    arc_port: u16,
    timeout_milliseconds: u32,
    attempts: u32,
    out_client: *mut *mut NetaudioClient,
) -> NetaudioStatus {
    if out_client.is_null() {
        return NetaudioStatus::NullPointer;
    }
    unsafe {
        *out_client = ptr::null_mut();
    }
    if device_ip.is_null() {
        return NetaudioStatus::NullPointer;
    }

    let device_ip = match unsafe { CStr::from_ptr(device_ip) }.to_str() {
        Ok(value) => value,
        Err(_) => return NetaudioStatus::InvalidUtf8,
    };

    let device_ip: IpAddr = match device_ip.parse() {
        Ok(value) => value,
        Err(_) => return NetaudioStatus::InvalidAddress,
    };

    let client = match Client::new(
        device_ip,
        arc_port,
        Duration::from_millis(timeout_milliseconds as u64),
        attempts,
    ) {
        Ok(client) => client,
        Err(error) => return error.into(),
    };

    unsafe {
        *out_client = Box::into_raw(Box::new(NetaudioClient {
            inner: Mutex::new(client),
        }));
    }

    NetaudioStatus::Ok
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_set_device_name(
    client: *mut NetaudioClient,
    name: *const c_char,
) -> NetaudioStatus {
    if client.is_null() || name.is_null() {
        return NetaudioStatus::NullPointer;
    }

    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(value) => value,
        Err(_) => return NetaudioStatus::InvalidUtf8,
    };

    let client = unsafe { &*client };
    match lock_client(client).set_device_name(name) {
        Ok(_) => NetaudioStatus::Ok,
        Err(error) => error.into(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_channel_count(
    client: *mut NetaudioClient,
    out_tx_count: *mut u16,
    out_rx_count: *mut u16,
    out_locked: *mut i32,
) -> NetaudioStatus {
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
    if client.is_null() || out_tx_count.is_null() || out_rx_count.is_null() || out_locked.is_null()
    {
        return NetaudioStatus::NullPointer;
    }

    let client = unsafe { &*client };
    let count = match lock_client(client).get_channel_count() {
        Ok(count) => count,
        Err(error) => return error.into(),
    };

    unsafe {
        *out_tx_count = count.tx_count;
        *out_rx_count = count.rx_count;
        *out_locked = match count.locked {
            Some(true) => 1,
            Some(false) => 0,
            None => -1,
        };
    }

    NetaudioStatus::Ok
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_rx_channels_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
        Ok(client) => client,
        Err(status) => return status,
    };

    let channels = match lock_client(client).get_rx_channels() {
        Ok(channels) => channels,
        Err(error) => return error.into(),
    };

    unsafe { write_json(&channels, out_buffer, out_capacity, out_length) }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_rx_inventory_json(
    client: *mut NetaudioClient,
    rx_count: u16,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
        Ok(client) => client,
        Err(status) => return status,
    };

    let inventory = match lock_client(client).get_rx_inventory(rx_count) {
        Ok(inventory) => inventory,
        Err(error) => return error.into(),
    };

    unsafe { write_json(&inventory, out_buffer, out_capacity, out_length) }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_tx_channels_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
        Ok(client) => client,
        Err(status) => return status,
    };

    let channels = match lock_client(client).get_tx_channels() {
        Ok(channels) => channels,
        Err(error) => return error.into(),
    };

    unsafe { write_json(&channels, out_buffer, out_capacity, out_length) }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_device_name_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
        Ok(client) => client,
        Err(status) => return status,
    };
    match lock_client(client).get_device_name() {
        Ok(name) => unsafe { write_json(&name, out_buffer, out_capacity, out_length) },
        Err(error) => error.into(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_device_info_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
        Ok(client) => client,
        Err(status) => return status,
    };
    match lock_client(client).get_device_info() {
        Ok(info) => unsafe { write_json(&info, out_buffer, out_capacity, out_length) },
        Err(error) => error.into(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_device_settings_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
        Ok(client) => client,
        Err(status) => return status,
    };
    match lock_client(client).get_device_settings() {
        Ok(settings) => unsafe { write_json(&settings, out_buffer, out_capacity, out_length) },
        Err(error) => error.into(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_property_directory_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
        Ok(client) => client,
        Err(status) => return status,
    };

    let property_directory = match lock_client(client).get_property_directory() {
        Ok(property_directory) => property_directory,
        Err(error) => return error.into(),
    };

    unsafe { write_json(&property_directory, out_buffer, out_capacity, out_length) }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_aes67_configured(
    client: *mut NetaudioClient,
    out_state: *mut i32,
) -> NetaudioStatus {
    if !out_state.is_null() {
        unsafe {
            *out_state = -1;
        }
    }
    if client.is_null() || out_state.is_null() {
        return NetaudioStatus::NullPointer;
    }
    let client = unsafe { &*client };
    match lock_client(client).get_aes67_configured() {
        Ok(state) => {
            unsafe {
                *out_state = match state {
                    Some(true) => 1,
                    Some(false) => 0,
                    None => -1,
                };
            }
            NetaudioStatus::Ok
        }
        Err(error) => error.into(),
    }
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
    if let Err(status) = unsafe { prepare_output(out_buffer, out_capacity, out_length) } {
        return status;
    }
    if kind.is_null() || data.is_null() {
        return NetaudioStatus::NullPointer;
    }
    let kind = match unsafe { CStr::from_ptr(kind) }.to_str() {
        Ok(value) => value,
        Err(_) => return NetaudioStatus::InvalidUtf8,
    };
    let bytes = unsafe { std::slice::from_raw_parts(data, data_len) };

    use crate::parser;
    unsafe {
        match kind {
            "rx" => write_optional_json(
                parser::parse_rx_page(bytes, starting_channel),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "tx_info" => write_optional_json(
                parser::parse_tx_info_page(bytes, starting_channel),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "tx_friendly" => write_optional_json(
                parser::parse_tx_friendly_page(bytes, starting_channel),
                out_buffer,
                out_capacity,
                out_length,
            ),
            _ => NetaudioStatus::InvalidChannelType,
        }
    }
}
