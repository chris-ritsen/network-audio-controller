use std::ffi::{c_char, CStr};
use std::net::IpAddr;
use std::ptr;
use std::time::Duration;

use crate::client::{Client, ClientError};
use crate::protocol::{build_set_device_name, NetaudioError, SERVICE_ARC};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetaudioStatus {
    Ok = 0,
    NullPointer = 1,
    InvalidUtf8 = 2,
    NameTooLong = 3,
    NameInvalidHyphen = 4,
    NameInvalidChars = 5,
    BufferTooSmall = 6,
    InvalidAddress = 7,
    IoError = 8,
    Timeout = 9,
    MalformedResponse = 10,
    SerializationError = 11,
    SubscriptionCount = 12,
    InvalidJson = 13,
    InvalidMac = 14,
    InvalidIp = 15,
    InvalidChannelType = 16,
    InvalidKey = 17,
    InvalidPin = 18,
    CryptoError = 19,
}

impl From<crate::lock::LockError> for NetaudioStatus {
    fn from(error: crate::lock::LockError) -> Self {
        use crate::lock::LockError;
        match error {
            LockError::InvalidKey => NetaudioStatus::InvalidKey,
            LockError::InvalidPin => NetaudioStatus::InvalidPin,
            LockError::Crypto => NetaudioStatus::CryptoError,
            LockError::Io(_) => NetaudioStatus::IoError,
            LockError::Timeout => NetaudioStatus::Timeout,
        }
    }
}

impl From<crate::spec::SpecError> for NetaudioStatus {
    fn from(error: crate::spec::SpecError) -> Self {
        use crate::spec::SpecError;
        match error {
            SpecError::Protocol(protocol_error) => protocol_error.into(),
            SpecError::InvalidJson => NetaudioStatus::InvalidJson,
            SpecError::InvalidMac => NetaudioStatus::InvalidMac,
            SpecError::InvalidIp => NetaudioStatus::InvalidIp,
            SpecError::InvalidChannelType => NetaudioStatus::InvalidChannelType,
        }
    }
}

impl From<NetaudioError> for NetaudioStatus {
    fn from(error: NetaudioError) -> Self {
        match error {
            NetaudioError::NameTooLong => NetaudioStatus::NameTooLong,
            NetaudioError::NameInvalidHyphen => NetaudioStatus::NameInvalidHyphen,
            NetaudioError::NameInvalidChars => NetaudioStatus::NameInvalidChars,
            NetaudioError::SubscriptionCount => NetaudioStatus::SubscriptionCount,
        }
    }
}

impl From<ClientError> for NetaudioStatus {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::Protocol(protocol_error) => protocol_error.into(),
            ClientError::Io(_) => NetaudioStatus::IoError,
            ClientError::Timeout => NetaudioStatus::Timeout,
            ClientError::MalformedResponse => NetaudioStatus::MalformedResponse,
            ClientError::Spec(spec_error) => spec_error.into(),
        }
    }
}

pub struct NetaudioClient {
    inner: Client,
}

const SERVICE_ARC_CSTR: &[u8] = b"_netaudio-arc._udp.local.\0";

pub const NETAUDIO_ABI_VERSION: u32 = 1;

#[no_mangle]
pub extern "C" fn netaudio_abi_version() -> u32 {
    NETAUDIO_ABI_VERSION
}

#[no_mangle]
pub extern "C" fn netaudio_service_arc() -> *const c_char {
    debug_assert_eq!(&SERVICE_ARC_CSTR[..SERVICE_ARC_CSTR.len() - 1], SERVICE_ARC.as_bytes());
    SERVICE_ARC_CSTR.as_ptr() as *const c_char
}

#[no_mangle]
pub extern "C" fn netaudio_status_name(status: NetaudioStatus) -> *const c_char {
    let name: &'static [u8] = match status {
        NetaudioStatus::Ok => b"ok\0",
        NetaudioStatus::NullPointer => b"null_pointer\0",
        NetaudioStatus::InvalidUtf8 => b"invalid_utf8\0",
        NetaudioStatus::NameTooLong => b"name_too_long\0",
        NetaudioStatus::NameInvalidHyphen => b"name_invalid_hyphen\0",
        NetaudioStatus::NameInvalidChars => b"name_invalid_chars\0",
        NetaudioStatus::BufferTooSmall => b"buffer_too_small\0",
        NetaudioStatus::InvalidAddress => b"invalid_address\0",
        NetaudioStatus::IoError => b"io_error\0",
        NetaudioStatus::Timeout => b"timeout\0",
        NetaudioStatus::MalformedResponse => b"malformed_response\0",
        NetaudioStatus::SerializationError => b"serialization_error\0",
        NetaudioStatus::SubscriptionCount => b"subscription_count\0",
        NetaudioStatus::InvalidJson => b"invalid_json\0",
        NetaudioStatus::InvalidMac => b"invalid_mac\0",
        NetaudioStatus::InvalidIp => b"invalid_ip\0",
        NetaudioStatus::InvalidChannelType => b"invalid_channel_type\0",
        NetaudioStatus::InvalidKey => b"invalid_key\0",
        NetaudioStatus::InvalidPin => b"invalid_pin\0",
        NetaudioStatus::CryptoError => b"crypto_error\0",
    };
    name.as_ptr() as *const c_char
}

#[no_mangle]
pub extern "C" fn netaudio_lock_nonce_length() -> usize {
    crate::lock::NONCE_LENGTH
}

#[no_mangle]
pub extern "C" fn netaudio_lock_key_length() -> usize {
    crate::lock::KEY_LENGTH
}

#[no_mangle]
pub extern "C" fn netaudio_build_set_device_name(
    name: *const c_char,
    transaction_id: u16,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    if name.is_null() || output_arguments_invalid(out_buffer, out_capacity, out_length) {
        return NetaudioStatus::NullPointer;
    }

    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(value) => value,
        Err(_) => return NetaudioStatus::InvalidUtf8,
    };

    let packet = match build_set_device_name(name, transaction_id) {
        Ok(packet) => packet,
        Err(error) => return error.into(),
    };

    write_bytes(&packet, out_buffer, out_capacity, out_length)
}

#[no_mangle]
pub extern "C" fn netaudio_build_command(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    if json.is_null() || output_arguments_invalid(out_buffer, out_capacity, out_length) {
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

    write_bytes(&packet, out_buffer, out_capacity, out_length)
}

#[no_mangle]
pub extern "C" fn netaudio_client_new(
    device_ip: *const c_char,
    arc_port: u16,
    timeout_milliseconds: u32,
    attempts: u32,
    out_client: *mut *mut NetaudioClient,
) -> NetaudioStatus {
    if device_ip.is_null() || out_client.is_null() {
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
        *out_client = Box::into_raw(Box::new(NetaudioClient { inner: client }));
    }

    NetaudioStatus::Ok
}

#[no_mangle]
pub extern "C" fn netaudio_client_set_device_name(
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

    let client = unsafe { &mut *client };
    match client.inner.set_device_name(name) {
        Ok(_) => NetaudioStatus::Ok,
        Err(error) => error.into(),
    }
}

#[no_mangle]
pub extern "C" fn netaudio_client_get_channel_count(
    client: *mut NetaudioClient,
    out_tx_count: *mut u16,
    out_rx_count: *mut u16,
    out_locked: *mut i32,
) -> NetaudioStatus {
    if client.is_null() || out_tx_count.is_null() || out_rx_count.is_null() || out_locked.is_null() {
        return NetaudioStatus::NullPointer;
    }

    let client = unsafe { &mut *client };
    let count = match client.inner.get_channel_count() {
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
pub extern "C" fn netaudio_client_get_rx_channels_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match nonnull_client(client, out_buffer, out_capacity, out_length) {
        Ok(client) => client,
        Err(status) => return status,
    };

    let channels = match client.inner.get_rx_channels() {
        Ok(channels) => channels,
        Err(error) => return error.into(),
    };

    write_json(&channels, out_buffer, out_capacity, out_length)
}

#[no_mangle]
pub extern "C" fn netaudio_client_get_tx_channels_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match nonnull_client(client, out_buffer, out_capacity, out_length) {
        Ok(client) => client,
        Err(status) => return status,
    };

    let channels = match client.inner.get_tx_channels() {
        Ok(channels) => channels,
        Err(error) => return error.into(),
    };

    write_json(&channels, out_buffer, out_capacity, out_length)
}

#[no_mangle]
pub extern "C" fn netaudio_client_get_device_name_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match nonnull_client(client, out_buffer, out_capacity, out_length) {
        Ok(client) => client,
        Err(status) => return status,
    };
    match client.inner.get_device_name() {
        Ok(name) => write_json(&name, out_buffer, out_capacity, out_length),
        Err(error) => error.into(),
    }
}

#[no_mangle]
pub extern "C" fn netaudio_client_get_device_info_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match nonnull_client(client, out_buffer, out_capacity, out_length) {
        Ok(client) => client,
        Err(status) => return status,
    };
    match client.inner.get_device_info() {
        Ok(info) => write_json(&info, out_buffer, out_capacity, out_length),
        Err(error) => error.into(),
    }
}

#[no_mangle]
pub extern "C" fn netaudio_client_get_device_settings_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match nonnull_client(client, out_buffer, out_capacity, out_length) {
        Ok(client) => client,
        Err(status) => return status,
    };
    match client.inner.get_device_settings() {
        Ok(settings) => write_json(&settings, out_buffer, out_capacity, out_length),
        Err(error) => error.into(),
    }
}

#[no_mangle]
pub extern "C" fn netaudio_client_get_aes67_configured(
    client: *mut NetaudioClient,
    out_state: *mut i32,
) -> NetaudioStatus {
    if client.is_null() || out_state.is_null() {
        return NetaudioStatus::NullPointer;
    }
    let client = unsafe { &mut *client };
    match client.inner.get_aes67_configured() {
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
pub extern "C" fn netaudio_parse_page(
    kind: *const c_char,
    data: *const u8,
    data_len: usize,
    starting_channel: u16,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    if kind.is_null() || data.is_null() || output_arguments_invalid(out_buffer, out_capacity, out_length) {
        return NetaudioStatus::NullPointer;
    }
    let kind = match unsafe { CStr::from_ptr(kind) }.to_str() {
        Ok(value) => value,
        Err(_) => return NetaudioStatus::InvalidUtf8,
    };
    let bytes = unsafe { std::slice::from_raw_parts(data, data_len) };

    use crate::parser;
    match kind {
        "rx" => write_json(&parser::parse_rx_page(bytes, starting_channel), out_buffer, out_capacity, out_length),
        "tx_info" => write_json(&parser::parse_tx_info_page(bytes, starting_channel), out_buffer, out_capacity, out_length),
        "tx_friendly" => write_json(&parser::parse_tx_friendly_page(bytes), out_buffer, out_capacity, out_length),
        _ => NetaudioStatus::InvalidChannelType,
    }
}

#[no_mangle]
pub extern "C" fn netaudio_parse_response(
    kind: *const c_char,
    data: *const u8,
    data_len: usize,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    if kind.is_null() || data.is_null() || output_arguments_invalid(out_buffer, out_capacity, out_length) {
        return NetaudioStatus::NullPointer;
    }
    let kind = match unsafe { CStr::from_ptr(kind) }.to_str() {
        Ok(value) => value,
        Err(_) => return NetaudioStatus::InvalidUtf8,
    };
    let bytes = unsafe { std::slice::from_raw_parts(data, data_len) };

    use crate::responses;
    match kind {
        "channel_count" => match crate::parser::parse_channel_count(bytes) {
            Some(value) => write_json(&value, out_buffer, out_capacity, out_length),
            None => NetaudioStatus::MalformedResponse,
        },
        "device_name" => write_json(&responses::parse_device_name(bytes), out_buffer, out_capacity, out_length),
        "device_info" => match responses::parse_device_info(bytes) {
            Some(value) => write_json(&value, out_buffer, out_capacity, out_length),
            None => NetaudioStatus::MalformedResponse,
        },
        "device_settings" => match responses::parse_device_settings(bytes) {
            Some(value) => write_json(&value, out_buffer, out_capacity, out_length),
            None => NetaudioStatus::MalformedResponse,
        },
        "aes67_configured" => write_json(&responses::parse_aes67_configured(bytes), out_buffer, out_capacity, out_length),
        "make_model" => write_json(&responses::parse_make_model(bytes), out_buffer, out_capacity, out_length),
        "dante_model" => write_json(&responses::parse_dante_model(bytes), out_buffer, out_capacity, out_length),
        "bluetooth_status" => write_json(&responses::parse_bluetooth_status(bytes), out_buffer, out_capacity, out_length),
        "result_code" => write_json(&responses::parse_result_code(bytes), out_buffer, out_capacity, out_length),
        "tx_flows" => write_json(&responses::parse_tx_flows(bytes), out_buffer, out_capacity, out_length),
        _ => NetaudioStatus::InvalidChannelType,
    }
}

#[no_mangle]
pub extern "C" fn netaudio_client_execute(
    client: *mut NetaudioClient,
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match nonnull_client(client, out_buffer, out_capacity, out_length) {
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

    let response = match client.inner.execute(json) {
        Ok(response) => response,
        Err(error) => return error.into(),
    };

    write_bytes(&response, out_buffer, out_capacity, out_length)
}

#[no_mangle]
pub extern "C" fn netaudio_lock_token(
    pin: *const c_char,
    nonce: *const u8,
    key: *const u8,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    if pin.is_null() || nonce.is_null() || key.is_null() || output_arguments_invalid(out_buffer, out_capacity, out_length) {
        return NetaudioStatus::NullPointer;
    }
    let pin = match unsafe { CStr::from_ptr(pin) }.to_str() {
        Ok(value) => value,
        Err(_) => return NetaudioStatus::InvalidUtf8,
    };
    let nonce = unsafe { std::slice::from_raw_parts(nonce, crate::lock::NONCE_LENGTH) };
    let key = unsafe { std::slice::from_raw_parts(key, crate::lock::KEY_LENGTH) };

    let token = match crate::lock::compute_token(pin, nonce, key) {
        Ok(token) => token,
        Err(error) => return error.into(),
    };

    write_bytes(&token, out_buffer, out_capacity, out_length)
}

#[no_mangle]
pub extern "C" fn netaudio_client_lock(
    client: *mut NetaudioClient,
    pin: *const c_char,
    key: *const u8,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    lock_or_unlock(client, pin, key, out_buffer, out_capacity, out_length, true)
}

#[no_mangle]
pub extern "C" fn netaudio_client_unlock(
    client: *mut NetaudioClient,
    pin: *const c_char,
    key: *const u8,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    lock_or_unlock(client, pin, key, out_buffer, out_capacity, out_length, false)
}

#[allow(clippy::too_many_arguments)]
fn lock_or_unlock(
    client: *mut NetaudioClient,
    pin: *const c_char,
    key: *const u8,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
    locking: bool,
) -> NetaudioStatus {
    let client = match nonnull_client(client, out_buffer, out_capacity, out_length) {
        Ok(client) => client,
        Err(status) => return status,
    };
    if pin.is_null() || key.is_null() {
        return NetaudioStatus::NullPointer;
    }
    let pin = match unsafe { CStr::from_ptr(pin) }.to_str() {
        Ok(value) => value,
        Err(_) => return NetaudioStatus::InvalidUtf8,
    };
    let key = unsafe { std::slice::from_raw_parts(key, crate::lock::KEY_LENGTH) };

    let result = if locking {
        client.inner.lock_device(pin, key)
    } else {
        client.inner.unlock_device(pin, key)
    };
    match result {
        Ok(lock_result) => write_json(&lock_result, out_buffer, out_capacity, out_length),
        Err(error) => error.into(),
    }
}

#[no_mangle]
pub extern "C" fn netaudio_client_request(
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
    let client = match nonnull_client(client, out_buffer, out_capacity, out_length) {
        Ok(client) => client,
        Err(status) => return status,
    };
    if packet.is_null() {
        return NetaudioStatus::NullPointer;
    }
    let packet = unsafe { std::slice::from_raw_parts(packet, packet_len) };

    let response = match client.inner.request_raw(packet, target_port, expect_response, repeat, interval_ms) {
        Ok(response) => response,
        Err(error) => return error.into(),
    };

    write_bytes(&response, out_buffer, out_capacity, out_length)
}

#[no_mangle]
pub extern "C" fn netaudio_client_set_host_mac(
    client: *mut NetaudioClient,
    host_mac: *const u8,
) -> NetaudioStatus {
    if client.is_null() || host_mac.is_null() {
        return NetaudioStatus::NullPointer;
    }
    let client = unsafe { &mut *client };
    let mut mac = [0u8; 6];
    unsafe {
        ptr::copy_nonoverlapping(host_mac, mac.as_mut_ptr(), 6);
    }
    client.inner.set_host_mac(mac);
    NetaudioStatus::Ok
}

#[no_mangle]
pub extern "C" fn netaudio_host_mac(out_mac: *mut u8) -> NetaudioStatus {
    if out_mac.is_null() {
        return NetaudioStatus::NullPointer;
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
}

#[no_mangle]
pub extern "C" fn netaudio_client_free(client: *mut NetaudioClient) {
    if client.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(client) });
}

fn output_arguments_invalid(out_buffer: *mut u8, out_capacity: usize, out_length: *mut usize) -> bool {
    out_length.is_null() || (out_buffer.is_null() && out_capacity != 0)
}

fn nonnull_client<'a>(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> Result<&'a mut NetaudioClient, NetaudioStatus> {
    if client.is_null() || output_arguments_invalid(out_buffer, out_capacity, out_length) {
        return Err(NetaudioStatus::NullPointer);
    }
    Ok(unsafe { &mut *client })
}

fn write_json<T: serde::Serialize>(
    value: &T,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let serialized = match serde_json::to_vec(value) {
        Ok(serialized) => serialized,
        Err(_) => return NetaudioStatus::SerializationError,
    };
    write_bytes(&serialized, out_buffer, out_capacity, out_length)
}

fn write_bytes(
    bytes: &[u8],
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        *out_length = bytes.len();
    }
    if bytes.len() > out_capacity {
        return NetaudioStatus::BufferTooSmall;
    }
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), out_buffer, bytes.len());
    }
    NetaudioStatus::Ok
}
