#![allow(clippy::missing_safety_doc)]

use std::ffi::{c_char, CStr};
use std::net::IpAddr;
use std::ptr;
use std::sync::{Mutex, MutexGuard};
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
    InvalidPage = 20,
    InvalidSubscriptionChannel = 21,
    InvalidDeviceType = 22,
    PacketTooLarge = 23,
    InvalidChannel = 24,
    InvalidLatency = 25,
    InvalidSampleRate = 26,
    InvalidEncoding = 27,
    InvalidGainLevel = 28,
    InvalidFlowSlot = 29,
    InvalidFlowProtocol = 30,
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
            SpecError::InvalidDeviceType => NetaudioStatus::InvalidDeviceType,
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
            NetaudioError::InvalidPage => NetaudioStatus::InvalidPage,
            NetaudioError::InvalidSubscriptionChannel => NetaudioStatus::InvalidSubscriptionChannel,
            NetaudioError::PacketTooLarge => NetaudioStatus::PacketTooLarge,
            NetaudioError::InvalidChannel => NetaudioStatus::InvalidChannel,
            NetaudioError::InvalidLatency => NetaudioStatus::InvalidLatency,
            NetaudioError::InvalidSampleRate => NetaudioStatus::InvalidSampleRate,
            NetaudioError::InvalidEncoding => NetaudioStatus::InvalidEncoding,
            NetaudioError::InvalidGainLevel => NetaudioStatus::InvalidGainLevel,
            NetaudioError::InvalidFlowSlot => NetaudioStatus::InvalidFlowSlot,
            NetaudioError::InvalidFlowProtocol => NetaudioStatus::InvalidFlowProtocol,
        }
    }
}

impl From<ClientError> for NetaudioStatus {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::Protocol(protocol_error) => protocol_error.into(),
            ClientError::Io(_) => NetaudioStatus::IoError,
            ClientError::InvalidAddress => NetaudioStatus::InvalidAddress,
            ClientError::Timeout => NetaudioStatus::Timeout,
            ClientError::MalformedResponse => NetaudioStatus::MalformedResponse,
            ClientError::Spec(spec_error) => spec_error.into(),
        }
    }
}

pub struct NetaudioClient {
    inner: Mutex<Client>,
}

const SERVICE_ARC_CSTR: &[u8] = b"_netaudio-arc._udp.local.\0";

pub const NETAUDIO_ABI_VERSION: u32 = 2;

#[no_mangle]
pub extern "C" fn netaudio_abi_version() -> u32 {
    NETAUDIO_ABI_VERSION
}

#[no_mangle]
pub extern "C" fn netaudio_service_arc() -> *const c_char {
    debug_assert_eq!(
        &SERVICE_ARC_CSTR[..SERVICE_ARC_CSTR.len() - 1],
        SERVICE_ARC.as_bytes()
    );
    SERVICE_ARC_CSTR.as_ptr() as *const c_char
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
        _ => b"unknown\0",
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
pub unsafe extern "C" fn netaudio_build_set_device_name(
    name: *const c_char,
    transaction_id: u16,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    if let Err(status) = unsafe { prepare_output(out_buffer, out_capacity, out_length) } {
        return status;
    }
    if name.is_null() {
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

    unsafe { write_bytes(&packet, out_buffer, out_capacity, out_length) }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_build_command(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
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
}

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

#[no_mangle]
pub unsafe extern "C" fn netaudio_parse_response(
    kind: *const c_char,
    data: *const u8,
    data_len: usize,
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

    use crate::responses;
    unsafe {
        match kind {
            "channel_count" => write_optional_json(
                crate::parser::parse_channel_count(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "channel_audio_metadata" => write_optional_json(
                crate::parser::parse_channel_audio_metadata(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "device_name" => write_optional_json(
                responses::parse_device_name(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "device_info" => write_optional_json(
                responses::parse_device_info(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "device_settings" => write_optional_json(
                responses::parse_device_settings(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "property_directory" => write_optional_json(
                responses::parse_property_directory(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "aes67_configured" => write_optional_json(
                responses::parse_aes67_configured(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "make_model" => write_optional_json(
                responses::parse_make_model(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "dante_model" => write_optional_json(
                responses::parse_dante_model(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "bluetooth_status" => write_optional_json(
                responses::parse_bluetooth_status(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "conmon_opcode" => write_optional_json(
                responses::parse_conmon_opcode(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "ptp_clock_status" => write_optional_json(
                responses::parse_ptp_clock_status(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "aes67_status" => write_optional_json(
                responses::parse_aes67_status(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "interface_status" => write_optional_json(
                responses::parse_interface_status(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "sample_rate_status" => write_optional_json(
                responses::parse_sample_rate_status(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "encoding_status" => write_optional_json(
                responses::parse_encoding_status(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "gain_status" => write_optional_json(
                responses::parse_gain_status(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "metering" => write_optional_json(
                responses::parse_metering_frame(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "result_code" => write_optional_json(
                responses::parse_result_code(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "tx_flows" => write_optional_json(
                responses::parse_tx_flows(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            "tx_flow_page" => write_optional_json(
                responses::parse_tx_flow_page(bytes),
                out_buffer,
                out_capacity,
                out_length,
            ),
            _ => NetaudioStatus::InvalidChannelType,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_execute(
    client: *mut NetaudioClient,
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
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
    unsafe {
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
    }
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
    unsafe {
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
    }
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
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_clear_wire_captures(
    client: *mut NetaudioClient,
) -> NetaudioStatus {
    if client.is_null() {
        return NetaudioStatus::NullPointer;
    }
    let client = unsafe { &*client };
    lock_client(client).clear_wire_captures();
    NetaudioStatus::Ok
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_get_wire_captures_json(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    let client = match unsafe { nonnull_client(client, out_buffer, out_capacity, out_length) } {
        Ok(client) => client,
        Err(status) => return status,
    };
    let captures = lock_client(client).wire_captures();
    unsafe { write_json(&captures, out_buffer, out_capacity, out_length) }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_set_host_mac(
    client: *mut NetaudioClient,
    host_mac: *const u8,
) -> NetaudioStatus {
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
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_host_mac(out_mac: *mut u8) -> NetaudioStatus {
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
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_client_free(client: *mut NetaudioClient) {
    if client.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(client) });
}

unsafe fn prepare_output(
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> Result<(), NetaudioStatus> {
    if out_length.is_null() {
        return Err(NetaudioStatus::NullPointer);
    }
    unsafe {
        *out_length = 0;
    }
    if out_buffer.is_null() && out_capacity != 0 {
        return Err(NetaudioStatus::NullPointer);
    }
    Ok(())
}

unsafe fn nonnull_client<'a>(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> Result<&'a NetaudioClient, NetaudioStatus> {
    unsafe {
        prepare_output(out_buffer, out_capacity, out_length)?;
    }
    if client.is_null() {
        return Err(NetaudioStatus::NullPointer);
    }
    Ok(unsafe { &*client })
}

fn lock_client(client: &NetaudioClient) -> MutexGuard<'_, Client> {
    match client.inner.lock() {
        Ok(client) => client,
        Err(poisoned_client) => poisoned_client.into_inner(),
    }
}

unsafe fn write_json<T: serde::Serialize>(
    value: &T,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        *out_length = 0;
    }
    let serialized = match serde_json::to_vec(value) {
        Ok(serialized) => serialized,
        Err(_) => return NetaudioStatus::SerializationError,
    };
    unsafe { write_bytes(&serialized, out_buffer, out_capacity, out_length) }
}

unsafe fn write_optional_json<T: serde::Serialize>(
    value: Option<T>,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        *out_length = 0;
    }
    match value {
        Some(value) => unsafe { write_json(&value, out_buffer, out_capacity, out_length) },
        None => NetaudioStatus::MalformedResponse,
    }
}

unsafe fn write_bytes(
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
    if !bytes.is_empty() {
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), out_buffer, bytes.len());
        }
    }
    NetaudioStatus::Ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn lock_token_call_with_pin(pin: &str, nonce: &[u8], key: &[u8]) -> (NetaudioStatus, Vec<u8>) {
        let pin = CString::new(pin).unwrap();
        let mut output = vec![0u8; 64];
        let mut output_length = 0usize;
        let status = unsafe {
            netaudio_lock_token(
                pin.as_ptr(),
                nonce.as_ptr(),
                nonce.len(),
                key.as_ptr(),
                key.len(),
                output.as_mut_ptr(),
                output.len(),
                &mut output_length,
            )
        };
        output.truncate(output_length);
        (status, output)
    }

    fn lock_token_call(nonce: &[u8], key: &[u8]) -> (NetaudioStatus, Vec<u8>) {
        lock_token_call_with_pin("1234", nonce, key)
    }

    fn client_lock_call(key: &[u8], locking: bool) -> NetaudioStatus {
        let inner = Client::new(
            "127.0.0.1".parse().unwrap(),
            4440,
            Duration::from_millis(1),
            1,
        )
        .unwrap();
        let mut client = NetaudioClient {
            inner: Mutex::new(inner),
        };
        let pin = CString::new("1234").unwrap();
        let mut output = vec![0u8; 64];
        let mut output_length = 0usize;

        if locking {
            unsafe {
                netaudio_client_lock(
                    &mut client,
                    pin.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    output.as_mut_ptr(),
                    output.len(),
                    &mut output_length,
                )
            }
        } else {
            unsafe {
                netaudio_client_unlock(
                    &mut client,
                    pin.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    output.as_mut_ptr(),
                    output.len(),
                    &mut output_length,
                )
            }
        }
    }

    fn build_command_status(json: &str) -> NetaudioStatus {
        let json = CString::new(json).unwrap();
        let mut output = vec![0u8; 256];
        let mut output_length = 0usize;
        unsafe {
            netaudio_build_command(
                json.as_ptr(),
                output.as_mut_ptr(),
                output.len(),
                &mut output_length,
            )
        }
    }

    fn parse_response_call(kind: &str, data: &[u8]) -> (NetaudioStatus, Vec<u8>) {
        let kind = CString::new(kind).unwrap();
        let mut output = vec![0u8; 1024];
        let mut output_length = 0usize;
        let status = unsafe {
            netaudio_parse_response(
                kind.as_ptr(),
                data.as_ptr(),
                data.len(),
                output.as_mut_ptr(),
                output.len(),
                &mut output_length,
            )
        };
        output.truncate(output_length);
        (status, output)
    }

    #[test]
    fn status_name_handles_unknown_c_discriminants_without_enum_ub() {
        for status in [-1, 31, i32::MAX] {
            let name = unsafe { CStr::from_ptr(netaudio_status_name(status)) };
            assert_eq!(name.to_str().unwrap(), "unknown");
        }
        let ok = unsafe { CStr::from_ptr(netaudio_status_name(NetaudioStatus::Ok as i32)) };
        assert_eq!(ok.to_str().unwrap(), "ok");
    }

    #[test]
    fn empty_byte_output_accepts_null_zero_capacity_buffer() {
        let mut output_length = usize::MAX;
        let status = unsafe { write_bytes(&[], ptr::null_mut(), 0, &mut output_length) };

        assert_eq!(status, NetaudioStatus::Ok);
        assert_eq!(output_length, 0);
    }

    #[test]
    fn failed_client_creation_clears_output_pointer() {
        let invalid_address = CString::new("invalid address").unwrap();
        let mut client_pointer = std::ptr::NonNull::<NetaudioClient>::dangling().as_ptr();

        let status = unsafe {
            netaudio_client_new(invalid_address.as_ptr(), 4440, 1, 1, &mut client_pointer)
        };

        assert_eq!(status, NetaudioStatus::InvalidAddress);
        assert!(client_pointer.is_null());
    }

    #[test]
    fn null_client_address_clears_output_pointer() {
        let mut client_pointer = std::ptr::NonNull::<NetaudioClient>::dangling().as_ptr();

        let status = unsafe { netaudio_client_new(ptr::null(), 4440, 1, 1, &mut client_pointer) };

        assert_eq!(status, NetaudioStatus::NullPointer);
        assert!(client_pointer.is_null());
    }

    #[test]
    fn ipv6_client_creation_is_rejected_and_clears_output_pointer() {
        let address = CString::new("::1").unwrap();
        let mut client_pointer = std::ptr::NonNull::<NetaudioClient>::dangling().as_ptr();

        let status =
            unsafe { netaudio_client_new(address.as_ptr(), 4440, 1, 1, &mut client_pointer) };

        assert_eq!(status, NetaudioStatus::InvalidAddress);
        assert!(client_pointer.is_null());
    }

    #[test]
    fn ffi_errors_clear_buffer_and_scalar_outputs() {
        let mut output = [0u8; 16];
        let mut output_length = usize::MAX;
        let status = unsafe {
            netaudio_build_command(
                ptr::null(),
                output.as_mut_ptr(),
                output.len(),
                &mut output_length,
            )
        };
        assert_eq!(status, NetaudioStatus::NullPointer);
        assert_eq!(output_length, 0);

        let kind = CString::new("device_name").unwrap();
        let malformed_response = [0u8; 10];
        output_length = usize::MAX;
        let status = unsafe {
            netaudio_parse_response(
                kind.as_ptr(),
                malformed_response.as_ptr(),
                malformed_response.len(),
                output.as_mut_ptr(),
                output.len(),
                &mut output_length,
            )
        };
        assert_eq!(status, NetaudioStatus::MalformedResponse);
        assert_eq!(output_length, 0);

        let mut tx_count = u16::MAX;
        let mut rx_count = u16::MAX;
        let mut locked = i32::MAX;
        let status = unsafe {
            netaudio_client_get_channel_count(
                ptr::null_mut(),
                &mut tx_count,
                &mut rx_count,
                &mut locked,
            )
        };
        assert_eq!(status, NetaudioStatus::NullPointer);
        assert_eq!(tx_count, 0);
        assert_eq!(rx_count, 0);
        assert_eq!(locked, -1);

        let mut aes67_state = i32::MAX;
        let status =
            unsafe { netaudio_client_get_aes67_configured(ptr::null_mut(), &mut aes67_state) };
        assert_eq!(status, NetaudioStatus::NullPointer);
        assert_eq!(aes67_state, -1);
    }

    #[test]
    fn same_client_handle_serializes_concurrent_calls() {
        let client = Client::new(
            "127.0.0.1".parse().unwrap(),
            4440,
            Duration::from_millis(1),
            1,
        )
        .unwrap();
        let client_pointer = Box::into_raw(Box::new(NetaudioClient {
            inner: Mutex::new(client),
        }));
        let client_address = client_pointer as usize;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(5));
        let mut threads = Vec::new();

        for _ in 0..4 {
            let barrier = std::sync::Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    assert_eq!(
                        unsafe {
                            netaudio_client_clear_wire_captures(
                                client_address as *mut NetaudioClient,
                            )
                        },
                        NetaudioStatus::Ok
                    );
                }
            }));
        }

        barrier.wait();
        for thread in threads {
            thread.join().unwrap();
        }
        unsafe {
            netaudio_client_free(client_pointer);
        }
    }

    #[test]
    fn interface_status_response_kind_serializes_expected_schema() {
        let mut data = [0u8; 0x40];
        data[0..2].copy_from_slice(&0xFFFFu16.to_be_bytes());
        data[2..4].copy_from_slice(&0x40u16.to_be_bytes());
        data[16..24].copy_from_slice(b"Audinate");
        data[24] = 0x07;
        data[26..28]
            .copy_from_slice(&crate::responses::CONMON_OPCODE_INTERFACE_STATUS.to_be_bytes());
        data[36..40].copy_from_slice(&1_000u32.to_be_bytes());
        let (status, output) = parse_response_call("interface_status", &data);

        assert_eq!(status, NetaudioStatus::Ok);
        let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(json["link_speed_mbps"], 1_000);
        assert_eq!(json["interfaces"], serde_json::json!([]));
        assert_eq!(json["reboot_required"], false);
        assert_eq!(json["pending_config"], serde_json::Value::Null);
    }

    #[test]
    fn encoding_status_response_kind_serializes_expected_schema() {
        let data = [
            0xFF, 0xFF, 0x00, 0x3C, 0x21, 0x02, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0x10, 0x73, 0x32,
            0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x24, 0x00, 0x82,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x03, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x10,
            0x00, 0x00, 0x00, 0x20,
        ];
        let (status, output) = parse_response_call("encoding_status", &data);

        assert_eq!(status, NetaudioStatus::Ok);
        let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(json["current_encoding"], 24);
        assert_eq!(json["supported_encodings"], serde_json::json!([24, 16, 32]));
    }

    #[test]
    fn metering_response_kind_serializes_expected_schema() {
        let data = [
            0xFF, 0xFF, 0x00, 0x21, 0x1F, 0x81, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0x19, 0x24, 0x5C,
            0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x02, 0x03, 0x02, 0xFE,
            0xFE, 0x7D, 0xA0, 0x88, 0x00,
        ];
        let (status, output) = parse_response_call("metering", &data);

        assert_eq!(status, NetaudioStatus::Ok);
        let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(json["sequence"], 0x1F81);
        assert_eq!(json["source_eui64"], "001dc119245c0000");
        assert_eq!(json["tx_count"], 3);
        assert_eq!(json["rx_count"], 2);
        assert_eq!(json["tx_levels"], serde_json::json!([0xFE, 0x7D, 0xA0]));
        assert_eq!(json["rx_levels"], serde_json::json!([0x88, 0x00]));
    }

    #[test]
    fn invalid_page_is_reported_across_ffi_without_panicking() {
        assert_eq!(
            build_command_status(r#"{"command":"receivers","page":4096}"#),
            NetaudioStatus::InvalidPage
        );
        assert_eq!(
            build_command_status(r#"{"command":"transmitters","page":2048}"#),
            NetaudioStatus::InvalidPage
        );
    }

    #[test]
    fn invalid_subscription_channel_is_reported_across_ffi() {
        assert_eq!(
            build_command_status(
                r#"{"command":"add_subscriptions","subscriptions":[{"rx_channel":257,"tx_channel":"tx","tx_device":"device"}]}"#,
            ),
            NetaudioStatus::InvalidSubscriptionChannel
        );
    }

    #[test]
    fn invalid_gain_device_type_is_reported_across_ffi() {
        assert_eq!(
            build_command_status(
                r#"{"command":"set_gain_level","channel_number":1,"gain_level":2,"device_type":"outputs","host_mac":"001122334455"}"#,
            ),
            NetaudioStatus::InvalidDeviceType
        );
    }

    #[test]
    fn invalid_wire_values_are_reported_across_ffi() {
        for (json, expected) in [
            (
                r#"{"command":"set_latency","latency":-1}"#,
                NetaudioStatus::InvalidLatency,
            ),
            (
                r#"{"command":"set_sample_rate","sample_rate":0}"#,
                NetaudioStatus::InvalidSampleRate,
            ),
            (
                r#"{"command":"set_encoding","encoding":0}"#,
                NetaudioStatus::InvalidEncoding,
            ),
            (
                r#"{"command":"set_gain_level","channel_number":0,"gain_level":1,"device_type":"input","host_mac":"001122334455"}"#,
                NetaudioStatus::InvalidChannel,
            ),
            (
                r#"{"command":"set_gain_level","channel_number":1,"gain_level":6,"device_type":"input","host_mac":"001122334455"}"#,
                NetaudioStatus::InvalidGainLevel,
            ),
            (
                r#"{"command":"create_tx_flow","flow_protocol_id":10025,"flow_slot":0,"channels":[1]}"#,
                NetaudioStatus::InvalidFlowSlot,
            ),
            (
                r#"{"command":"query_tx_flows","flow_protocol_id":4660}"#,
                NetaudioStatus::InvalidFlowProtocol,
            ),
        ] {
            assert_eq!(build_command_status(json), expected, "{json}");
        }
    }

    #[test]
    fn lock_token_ffi_preserves_valid_token_parity() {
        let nonce = [0x11; crate::lock::NONCE_LENGTH];
        let key = [0x22; crate::lock::KEY_LENGTH];
        let expected = crate::lock::compute_token("1234", &nonce, &key).unwrap();

        let (status, token) = lock_token_call(&nonce, &key);

        assert_eq!(status, NetaudioStatus::Ok);
        assert_eq!(token, expected);
    }

    #[test]
    fn lock_token_ffi_rejects_incorrect_buffer_lengths() {
        let valid_nonce = [0u8; crate::lock::NONCE_LENGTH];
        let valid_key = [0u8; crate::lock::KEY_LENGTH];

        for length in [0, 1, 23, 25, 64] {
            let nonce = vec![0u8; length];
            assert_eq!(
                lock_token_call(&nonce, &valid_key).0,
                NetaudioStatus::CryptoError
            );
        }
        for length in [0, 1, 31, 33, 64] {
            let key = vec![0u8; length];
            assert_eq!(
                lock_token_call(&valid_nonce, &key).0,
                NetaudioStatus::InvalidKey
            );
        }
    }

    #[test]
    fn lock_token_ffi_rejects_invalid_pins() {
        let nonce = [0u8; crate::lock::NONCE_LENGTH];
        let key = [0u8; crate::lock::KEY_LENGTH];
        for pin in ["", "123", "12345", "abcd", "１２３４"] {
            assert_eq!(
                lock_token_call_with_pin(pin, &nonce, &key).0,
                NetaudioStatus::InvalidPin
            );
        }
    }

    #[test]
    fn client_lock_ffi_rejects_incorrect_key_lengths_before_network_io() {
        for length in [0, 1, 31, 33, 64] {
            let key = vec![0u8; length];
            assert_eq!(client_lock_call(&key, true), NetaudioStatus::InvalidKey);
            assert_eq!(client_lock_call(&key, false), NetaudioStatus::InvalidKey);
        }
    }
}
