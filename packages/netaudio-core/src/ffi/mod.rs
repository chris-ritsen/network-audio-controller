#![allow(clippy::missing_safety_doc)]

use std::cell::RefCell;
use std::ffi::{c_char, CStr};
use std::net::IpAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::client::{Client, ClientError};
use crate::protocol::NetaudioError;

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
    InvalidSequence = 31,
    UnsupportedProtocolOperation = 32,
    InternalPanic = 33,
    UnknownKind = 34,
    InvalidLength = 35,
    InvalidDestination = 36,
    InvalidFlowIdentity = 37,
    InvalidReceiverMapping = 38,
}

impl NetaudioStatus {
    pub const ALL: [NetaudioStatus; 39] = [
        NetaudioStatus::Ok,
        NetaudioStatus::NullPointer,
        NetaudioStatus::InvalidUtf8,
        NetaudioStatus::NameTooLong,
        NetaudioStatus::NameInvalidHyphen,
        NetaudioStatus::NameInvalidChars,
        NetaudioStatus::BufferTooSmall,
        NetaudioStatus::InvalidAddress,
        NetaudioStatus::IoError,
        NetaudioStatus::Timeout,
        NetaudioStatus::MalformedResponse,
        NetaudioStatus::SerializationError,
        NetaudioStatus::SubscriptionCount,
        NetaudioStatus::InvalidJson,
        NetaudioStatus::InvalidMac,
        NetaudioStatus::InvalidIp,
        NetaudioStatus::InvalidChannelType,
        NetaudioStatus::InvalidKey,
        NetaudioStatus::InvalidPin,
        NetaudioStatus::CryptoError,
        NetaudioStatus::InvalidPage,
        NetaudioStatus::InvalidSubscriptionChannel,
        NetaudioStatus::InvalidDeviceType,
        NetaudioStatus::PacketTooLarge,
        NetaudioStatus::InvalidChannel,
        NetaudioStatus::InvalidLatency,
        NetaudioStatus::InvalidSampleRate,
        NetaudioStatus::InvalidEncoding,
        NetaudioStatus::InvalidGainLevel,
        NetaudioStatus::InvalidFlowSlot,
        NetaudioStatus::InvalidFlowProtocol,
        NetaudioStatus::InvalidSequence,
        NetaudioStatus::UnsupportedProtocolOperation,
        NetaudioStatus::InternalPanic,
        NetaudioStatus::UnknownKind,
        NetaudioStatus::InvalidLength,
        NetaudioStatus::InvalidDestination,
        NetaudioStatus::InvalidFlowIdentity,
        NetaudioStatus::InvalidReceiverMapping,
    ];

    pub fn from_code(code: i32) -> Option<NetaudioStatus> {
        NetaudioStatus::ALL
            .into_iter()
            .find(|status| *status as i32 == code)
    }

    pub fn name(self) -> &'static CStr {
        match self {
            NetaudioStatus::BufferTooSmall => c"buffer_too_small",
            NetaudioStatus::CryptoError => c"crypto_error",
            NetaudioStatus::InternalPanic => c"internal_panic",
            NetaudioStatus::InvalidAddress => c"invalid_address",
            NetaudioStatus::InvalidChannel => c"invalid_channel",
            NetaudioStatus::InvalidChannelType => c"invalid_channel_type",
            NetaudioStatus::InvalidDeviceType => c"invalid_device_type",
            NetaudioStatus::InvalidDestination => c"invalid_destination",
            NetaudioStatus::InvalidEncoding => c"invalid_encoding",
            NetaudioStatus::InvalidFlowIdentity => c"invalid_flow_identity",
            NetaudioStatus::InvalidFlowProtocol => c"invalid_flow_protocol",
            NetaudioStatus::InvalidFlowSlot => c"invalid_flow_slot",
            NetaudioStatus::InvalidGainLevel => c"invalid_gain_level",
            NetaudioStatus::InvalidIp => c"invalid_ip",
            NetaudioStatus::InvalidJson => c"invalid_json",
            NetaudioStatus::InvalidKey => c"invalid_key",
            NetaudioStatus::InvalidLatency => c"invalid_latency",
            NetaudioStatus::InvalidLength => c"invalid_length",
            NetaudioStatus::InvalidMac => c"invalid_mac",
            NetaudioStatus::InvalidPage => c"invalid_page",
            NetaudioStatus::InvalidPin => c"invalid_pin",
            NetaudioStatus::InvalidReceiverMapping => c"invalid_receiver_mapping",
            NetaudioStatus::InvalidSampleRate => c"invalid_sample_rate",
            NetaudioStatus::InvalidSequence => c"invalid_sequence",
            NetaudioStatus::InvalidSubscriptionChannel => c"invalid_subscription_channel",
            NetaudioStatus::InvalidUtf8 => c"invalid_utf8",
            NetaudioStatus::IoError => c"io_error",
            NetaudioStatus::MalformedResponse => c"malformed_response",
            NetaudioStatus::NameInvalidChars => c"name_invalid_chars",
            NetaudioStatus::NameInvalidHyphen => c"name_invalid_hyphen",
            NetaudioStatus::NameTooLong => c"name_too_long",
            NetaudioStatus::NullPointer => c"null_pointer",
            NetaudioStatus::Ok => c"ok",
            NetaudioStatus::PacketTooLarge => c"packet_too_large",
            NetaudioStatus::SerializationError => c"serialization_error",
            NetaudioStatus::SubscriptionCount => c"subscription_count",
            NetaudioStatus::Timeout => c"timeout",
            NetaudioStatus::UnknownKind => c"unknown_kind",
            NetaudioStatus::UnsupportedProtocolOperation => c"unsupported_protocol_operation",
        }
    }

    pub fn description(self) -> &'static CStr {
        match self {
            NetaudioStatus::Ok => c"ok",
            NetaudioStatus::NullPointer => c"null pointer",
            NetaudioStatus::InvalidUtf8 => c"invalid utf-8",
            NetaudioStatus::NameTooLong => c"name too long",
            NetaudioStatus::NameInvalidHyphen => c"name cannot begin or end with a hyphen",
            NetaudioStatus::NameInvalidChars => c"name contains unsupported characters",
            NetaudioStatus::BufferTooSmall => c"buffer too small",
            NetaudioStatus::InvalidAddress => c"invalid address",
            NetaudioStatus::IoError => c"io error",
            NetaudioStatus::Timeout => c"device did not respond",
            NetaudioStatus::MalformedResponse => c"malformed binary response",
            NetaudioStatus::SerializationError => c"FFI/API serialization failure",
            NetaudioStatus::SubscriptionCount => c"subscription count must be 1-16",
            NetaudioStatus::InvalidJson => c"invalid command json",
            NetaudioStatus::InvalidMac => c"invalid mac",
            NetaudioStatus::InvalidIp => c"invalid ip",
            NetaudioStatus::InvalidChannelType => c"invalid channel type",
            NetaudioStatus::InvalidKey => c"invalid lock key",
            NetaudioStatus::InvalidPin => c"pin must be exactly 4 digits",
            NetaudioStatus::CryptoError => c"crypto error",
            NetaudioStatus::InvalidPage => c"invalid page",
            NetaudioStatus::InvalidSubscriptionChannel => {
                c"subscription receiver channel must fit in one byte"
            }
            NetaudioStatus::InvalidDeviceType => c"device type must be 'input' or 'output'",
            NetaudioStatus::PacketTooLarge => c"command packet exceeds the protocol length limit",
            NetaudioStatus::InvalidChannel => c"channel number must be at least 1",
            NetaudioStatus::InvalidLatency => {
                c"latency must be finite, nonnegative, and fit on the wire"
            }
            NetaudioStatus::InvalidSampleRate => c"sample rate must be nonzero",
            NetaudioStatus::InvalidEncoding => c"encoding value must be nonzero",
            NetaudioStatus::InvalidGainLevel => c"gain level must be an integer from 1 through 5",
            NetaudioStatus::InvalidFlowSlot => c"flow slot must be from 1 through 32",
            NetaudioStatus::InvalidFlowProtocol => c"unsupported flow protocol",
            NetaudioStatus::InvalidSequence => c"sequence must be nonzero",
            NetaudioStatus::UnsupportedProtocolOperation => {
                c"the selected protocol does not support this operation"
            }
            NetaudioStatus::InternalPanic => c"internal panic",
            NetaudioStatus::UnknownKind => c"unknown response or page kind",
            NetaudioStatus::InvalidLength => c"byte buffer has the wrong length",
            NetaudioStatus::InvalidDestination => c"invalid external RTP destination",
            NetaudioStatus::InvalidFlowIdentity => c"invalid external flow identity",
            NetaudioStatus::InvalidReceiverMapping => c"invalid external receiver mapping",
        }
    }

    pub fn category(self) -> &'static CStr {
        match self {
            NetaudioStatus::IoError | NetaudioStatus::Timeout => c"transport",
            NetaudioStatus::MalformedResponse => c"binary_response",
            NetaudioStatus::SerializationError => c"api_serialization",
            NetaudioStatus::InvalidJson => c"json_input",
            _ => c"api",
        }
    }
}

impl From<crate::lock::LockError> for NetaudioStatus {
    fn from(error: crate::lock::LockError) -> Self {
        use crate::lock::LockError;
        match error {
            LockError::Crypto => NetaudioStatus::CryptoError,
            LockError::InvalidKey => NetaudioStatus::InvalidKey,
            LockError::InvalidNonce => NetaudioStatus::InvalidLength,
            LockError::InvalidPin => NetaudioStatus::InvalidPin,
            LockError::Io(_) => NetaudioStatus::IoError,
            LockError::Timeout => NetaudioStatus::Timeout,
        }
    }
}

impl From<crate::spec::SpecError> for NetaudioStatus {
    fn from(error: crate::spec::SpecError) -> Self {
        use crate::spec::SpecError;
        match error {
            SpecError::InvalidChannelType => NetaudioStatus::InvalidChannelType,
            SpecError::InvalidDeviceType => NetaudioStatus::InvalidDeviceType,
            SpecError::InvalidIp => NetaudioStatus::InvalidIp,
            SpecError::InvalidJson(_) => NetaudioStatus::InvalidJson,
            SpecError::InvalidMac => NetaudioStatus::InvalidMac,
            SpecError::Protocol(protocol_error) => protocol_error.into(),
        }
    }
}

impl From<NetaudioError> for NetaudioStatus {
    fn from(error: NetaudioError) -> Self {
        match error {
            NetaudioError::InvalidChannel => NetaudioStatus::InvalidChannel,
            NetaudioError::InvalidDestination => NetaudioStatus::InvalidDestination,
            NetaudioError::InvalidEncoding => NetaudioStatus::InvalidEncoding,
            NetaudioError::InvalidFlowIdentity => NetaudioStatus::InvalidFlowIdentity,
            NetaudioError::InvalidFlowProtocol => NetaudioStatus::InvalidFlowProtocol,
            NetaudioError::InvalidFlowSlot => NetaudioStatus::InvalidFlowSlot,
            NetaudioError::InvalidGainLevel => NetaudioStatus::InvalidGainLevel,
            NetaudioError::InvalidNetworkConfiguration(_) => NetaudioStatus::InvalidIp,
            NetaudioError::InvalidLatency => NetaudioStatus::InvalidLatency,
            NetaudioError::InvalidPage => NetaudioStatus::InvalidPage,
            NetaudioError::InvalidReceiverMapping => NetaudioStatus::InvalidReceiverMapping,
            NetaudioError::InvalidSampleRate => NetaudioStatus::InvalidSampleRate,
            NetaudioError::InvalidSequence => NetaudioStatus::InvalidSequence,
            NetaudioError::InvalidSubscriptionChannel => NetaudioStatus::InvalidSubscriptionChannel,
            NetaudioError::NameInvalidChars => NetaudioStatus::NameInvalidChars,
            NetaudioError::NameInvalidHyphen => NetaudioStatus::NameInvalidHyphen,
            NetaudioError::NameTooLong => NetaudioStatus::NameTooLong,
            NetaudioError::PacketTooLarge => NetaudioStatus::PacketTooLarge,
            NetaudioError::SubscriptionCount => NetaudioStatus::SubscriptionCount,
            NetaudioError::UnsupportedProtocolOperation => {
                NetaudioStatus::UnsupportedProtocolOperation
            }
        }
    }
}

impl From<ClientError> for NetaudioStatus {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::InvalidAddress => NetaudioStatus::InvalidAddress,
            ClientError::InvalidLength => NetaudioStatus::InvalidLength,
            ClientError::Io(_) => NetaudioStatus::IoError,
            ClientError::MalformedResponse | ClientError::MalformedResponseAt(_) => {
                NetaudioStatus::MalformedResponse
            }
            ClientError::Protocol(protocol_error) => protocol_error.into(),
            ClientError::Spec(spec_error) => spec_error.into(),
            ClientError::Timeout => NetaudioStatus::Timeout,
        }
    }
}

#[derive(Debug)]
pub(crate) struct FfiError {
    message: String,
    status: NetaudioStatus,
}

impl FfiError {
    fn new(status: NetaudioStatus, message: impl Into<String>) -> FfiError {
        FfiError {
            message: message.into(),
            status,
        }
    }
}

impl From<NetaudioStatus> for FfiError {
    fn from(status: NetaudioStatus) -> Self {
        let message = match status {
            NetaudioStatus::BufferTooSmall => "output buffer is smaller than the result",
            NetaudioStatus::InvalidUtf8 => "string argument is not valid UTF-8",
            NetaudioStatus::NullPointer => "required pointer argument is null",
            _ => "",
        };
        FfiError::new(
            status,
            if message.is_empty() {
                status.name().to_string_lossy().into_owned()
            } else {
                message.to_owned()
            },
        )
    }
}

impl From<ClientError> for FfiError {
    fn from(error: ClientError) -> Self {
        let message = error.to_string();
        FfiError::new(error.into(), message)
    }
}

impl From<crate::lock::LockError> for FfiError {
    fn from(error: crate::lock::LockError) -> Self {
        let message = error.to_string();
        FfiError::new(error.into(), message)
    }
}

impl From<crate::spec::SpecError> for FfiError {
    fn from(error: crate::spec::SpecError) -> Self {
        let message = error.to_string();
        FfiError::new(error.into(), message)
    }
}

impl From<NetaudioError> for FfiError {
    fn from(error: NetaudioError) -> Self {
        FfiError::new(error.into(), error.to_string())
    }
}

pub(crate) type FfiResult = Result<(), FfiError>;

thread_local! {
    static LAST_ERROR_MESSAGE: RefCell<String> = const { RefCell::new(String::new()) };
}

fn set_last_error_message(message: &str) {
    LAST_ERROR_MESSAGE.with(|slot| {
        let mut slot = slot.borrow_mut();
        slot.clear();
        slot.push_str(message);
    });
}

pub(crate) fn last_error_message() -> String {
    LAST_ERROR_MESSAGE.with(|slot| slot.borrow().clone())
}

pub struct NetaudioClient {
    inner: Mutex<Client>,
}

impl NetaudioClient {
    pub fn new(client: Client) -> NetaudioClient {
        NetaudioClient {
            inner: Mutex::new(client),
        }
    }
}

pub const NETAUDIO_ABI_VERSION: u32 = 13;

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return format!("internal panic: {message}");
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return format!("internal panic: {message}");
    }
    "internal panic".to_owned()
}

fn guard(operation: impl FnOnce() -> FfiResult) -> NetaudioStatus {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(())) => {
            set_last_error_message("");
            NetaudioStatus::Ok
        }
        Ok(Err(error)) => {
            set_last_error_message(&error.message);
            error.status
        }
        Err(payload) => {
            set_last_error_message(&panic_message(payload));
            NetaudioStatus::InternalPanic
        }
    }
}

unsafe fn prepare_output(
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> FfiResult {
    if out_length.is_null() {
        return Err(NetaudioStatus::NullPointer.into());
    }
    unsafe {
        *out_length = 0;
    }
    if out_buffer.is_null() && out_capacity != 0 {
        return Err(NetaudioStatus::NullPointer.into());
    }
    Ok(())
}

unsafe fn nonnull_client<'a>(
    client: *mut NetaudioClient,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> Result<&'a NetaudioClient, FfiError> {
    unsafe {
        prepare_output(out_buffer, out_capacity, out_length)?;
    }
    if client.is_null() {
        return Err(NetaudioStatus::NullPointer.into());
    }
    Ok(unsafe { &*client })
}

unsafe fn c_string<'a>(pointer: *const c_char) -> Result<&'a str, FfiError> {
    if pointer.is_null() {
        return Err(NetaudioStatus::NullPointer.into());
    }
    unsafe { CStr::from_ptr(pointer) }
        .to_str()
        .map_err(|_| NetaudioStatus::InvalidUtf8.into())
}

fn lock_client(client: &NetaudioClient) -> MutexGuard<'_, Client> {
    match client.inner.lock() {
        Ok(client) => client,
        Err(poisoned_client) => poisoned_client.into_inner(),
    }
}

type OutputBuffer = (*mut u8, usize, *mut usize);

unsafe fn json_output<T: serde::Serialize>(
    output: OutputBuffer,
    operation: impl FnOnce() -> Result<T, FfiError>,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(output.0, output.1, output.2)? };
        let value = operation()?;
        unsafe { write_json(&value, output.0, output.1, output.2) }
    })
}

unsafe fn bytes_output(
    output: OutputBuffer,
    operation: impl FnOnce() -> Result<Vec<u8>, FfiError>,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(output.0, output.1, output.2)? };
        let value = operation()?;
        unsafe { write_bytes(&value, output.0, output.1, output.2) }
    })
}

fn decode_json<T: serde::de::DeserializeOwned>(input: &str) -> Result<T, FfiError> {
    serde_json::from_str(input)
        .map_err(|error| crate::spec::SpecError::InvalidJson(error.to_string()).into())
}

unsafe fn write_json<T: serde::Serialize>(
    value: &T,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> FfiResult {
    unsafe {
        *out_length = 0;
    }
    let serialized = serde_json::to_vec(value).map_err(|error| {
        FfiError::new(
            NetaudioStatus::SerializationError,
            format!("could not serialize result: {error}"),
        )
    })?;
    unsafe { write_bytes(&serialized, out_buffer, out_capacity, out_length) }
}

unsafe fn write_optional_json<T: serde::Serialize>(
    value: Option<T>,
    kind: &str,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> FfiResult {
    unsafe {
        *out_length = 0;
    }
    match value {
        Some(value) => unsafe { write_json(&value, out_buffer, out_capacity, out_length) },
        None => Err(FfiError::new(
            NetaudioStatus::MalformedResponse,
            format!("bytes did not parse as a {kind} response"),
        )),
    }
}

unsafe fn write_bytes(
    bytes: &[u8],
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> FfiResult {
    unsafe {
        *out_length = bytes.len();
    }
    if bytes.len() > out_capacity {
        return Err(FfiError::new(
            NetaudioStatus::BufferTooSmall,
            format!(
                "result needs {} bytes but the buffer holds {out_capacity}",
                bytes.len()
            ),
        ));
    }
    if !bytes.is_empty() {
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), out_buffer, bytes.len());
        }
    }
    Ok(())
}

mod audio;
mod capabilities;
mod client_actions;
mod client_queries;
mod clock;
mod configuration;
mod conmon_export;
mod dapi;
mod encoding;
mod flows;
mod inventory;
mod monitoring;
mod network;
mod panels;
mod parsing;
mod performance;
mod protocol;
mod runtime;
mod subscriptions;

pub use audio::*;
pub use capabilities::*;
pub use client_actions::*;
pub use client_queries::*;
pub use clock::*;
pub use conmon_export::*;
pub use dapi::*;
pub use encoding::*;
pub use flows::*;
pub use inventory::*;
pub use monitoring::*;
pub use network::*;
pub use panels::*;
pub use parsing::*;
pub use performance::*;
pub use protocol::*;
pub use runtime::*;
pub use subscriptions::*;

#[cfg(test)]
mod tests;
