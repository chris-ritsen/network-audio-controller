#![allow(clippy::missing_safety_doc)]

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
            NetaudioError::InvalidSequence => NetaudioStatus::InvalidSequence,
            NetaudioError::UnsupportedProtocolOperation => {
                NetaudioStatus::UnsupportedProtocolOperation
            }
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

pub const NETAUDIO_ABI_VERSION: u32 = 3;

fn guard_panic(operation: impl FnOnce() -> NetaudioStatus) -> NetaudioStatus {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(status) => status,
        Err(_) => NetaudioStatus::InternalPanic,
    }
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

mod client_actions;
mod client_queries;
mod metadata;
mod parsing;

pub use client_actions::*;
pub use client_queries::*;
pub use metadata::*;
pub use parsing::*;

#[cfg(test)]
mod tests;
