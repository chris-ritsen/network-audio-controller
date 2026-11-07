use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::Duration;

use crypto_secretbox::aead::AeadInPlace;
use crypto_secretbox::{KeyInit, XSalsa20Poly1305};
use serde::Serialize;

pub const DEVICE_LOCK_PORT: u16 = 8002;
pub const NONCE_LENGTH: usize = 24;
pub const KEY_LENGTH: usize = 32;

const LOCK_DDP_HEADER: [u8; 8] = [0x00, 0x08, 0x00, 0x01, 0x10, 0x00, 0x02, 0x00];
const LOCK_TIMEOUT_SECONDS: u64 = 5;
const LOCK_MAX_RETRIES: u32 = 4;
const LOCK_STATUS_SUCCESS: u16 = 0x0000;
const LOCK_STATUS_ALREADY: u16 = 0x1102;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockOperation {
    Lock = 1,
    Unlock = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LockResult {
    pub status: u16,
    pub lock_state: u16,
    pub success: bool,
    pub already: bool,
}

#[derive(Debug)]
pub enum LockError {
    Crypto,
    InvalidKey,
    InvalidNonce,
    InvalidPin,
    Io(io::Error),
    Timeout,
}

impl std::fmt::Display for LockError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Crypto => formatter.write_str("secretbox encryption failed"),
            LockError::InvalidKey => {
                write!(formatter, "lock key must be exactly {KEY_LENGTH} bytes")
            }
            LockError::InvalidNonce => {
                write!(formatter, "lock nonce must be exactly {NONCE_LENGTH} bytes")
            }
            LockError::InvalidPin => formatter.write_str("pin must be exactly 4 digits"),
            LockError::Io(error) => write!(formatter, "socket error: {error}"),
            LockError::Timeout => formatter.write_str("device did not answer the lock exchange"),
        }
    }
}

impl From<io::Error> for LockError {
    fn from(error: io::Error) -> Self {
        LockError::Io(error)
    }
}

pub fn validate_pin(pin: &str) -> Result<(), LockError> {
    if pin.len() == 4 && pin.bytes().all(|byte| byte.is_ascii_digit()) {
        Ok(())
    } else {
        Err(LockError::InvalidPin)
    }
}

pub fn compute_token(pin: &str, nonce: &[u8], key: &[u8]) -> Result<Vec<u8>, LockError> {
    validate_pin(pin)?;
    if key.len() != KEY_LENGTH {
        return Err(LockError::InvalidKey);
    }
    if nonce.len() != NONCE_LENGTH {
        return Err(LockError::InvalidNonce);
    }

    let cipher = XSalsa20Poly1305::new_from_slice(key).map_err(|_| LockError::InvalidKey)?;
    let mut buffer = pin.as_bytes().to_vec();
    let tag = cipher
        .encrypt_in_place_detached(nonce.into(), b"", &mut buffer)
        .map_err(|_| LockError::Crypto)?;

    let mut token = Vec::with_capacity(tag.len() + buffer.len());
    token.extend_from_slice(&tag);
    token.extend_from_slice(&buffer);
    Ok(token)
}

pub fn build_challenge_request(sequence: u16) -> Vec<u8> {
    let mut packet = Vec::with_capacity(20);
    packet.extend_from_slice(&LOCK_DDP_HEADER);
    packet.extend_from_slice(&0x000Cu16.to_be_bytes());
    packet.extend_from_slice(&0x2FFEu16.to_be_bytes());
    packet.extend_from_slice(&0x0004u16.to_be_bytes());
    packet.extend_from_slice(&0x0000u16.to_be_bytes());
    packet.extend_from_slice(&sequence.to_be_bytes());
    packet.extend_from_slice(&0x0000u16.to_be_bytes());
    packet
}

pub fn build_auth_message(sequence: u16, operation: LockOperation, token: &[u8]) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(&LOCK_DDP_HEADER);
    packet.extend_from_slice(&0x0028u16.to_be_bytes());
    packet.extend_from_slice(&0x2FFFu16.to_be_bytes());
    packet.extend_from_slice(&0x0004u16.to_be_bytes());
    packet.extend_from_slice(&0x0008u16.to_be_bytes());
    packet.extend_from_slice(&sequence.to_be_bytes());
    packet.extend_from_slice(&0x0000u16.to_be_bytes());
    packet.extend_from_slice(&(operation as u16).to_be_bytes());
    packet.extend_from_slice(&0x0014u16.to_be_bytes());
    packet.extend_from_slice(&0x0014u16.to_be_bytes());
    packet.extend_from_slice(&0x0000u16.to_be_bytes());
    packet.extend_from_slice(token);
    packet
}

pub fn parse_nonce(challenge_response: &[u8]) -> Option<Vec<u8>> {
    let message = challenge_response.get(8..)?;
    let nonce_offset = u16::from_be_bytes([*message.get(12)?, *message.get(13)?]) as usize;
    let nonce_length = u16::from_be_bytes([*message.get(14)?, *message.get(15)?]) as usize;
    let nonce = message.get(nonce_offset..nonce_offset + nonce_length)?;
    if nonce.len() != NONCE_LENGTH {
        return None;
    }
    Some(nonce.to_vec())
}

pub fn parse_lock_result(auth_response: &[u8]) -> Option<LockResult> {
    let message = auth_response.get(8..)?;
    let status = u16::from_be_bytes([*message.get(10)?, *message.get(11)?]);
    let lock_state = u16::from_be_bytes([*message.get(12)?, *message.get(13)?]);
    Some(LockResult {
        status,
        lock_state,
        success: status == LOCK_STATUS_SUCCESS || status == LOCK_STATUS_ALREADY,
        already: status == LOCK_STATUS_ALREADY,
    })
}

pub fn parse_challenge_response(response: &[u8], sequence: u16) -> Option<Vec<u8>> {
    correlated_response(response, sequence, 0x2FFE)?;
    if crate::bytes::read_u16(response, 20)? < 16 {
        return None;
    }
    parse_nonce(response)
}

pub fn parse_authentication_response(response: &[u8], sequence: u16) -> Option<LockResult> {
    correlated_response(response, sequence, 0x2FFF)?;
    parse_lock_result(response)
}

fn correlated_response(response: &[u8], sequence: u16, operation: u16) -> Option<()> {
    let header = crate::protocol::diagnostic_packet_header(response)?;
    let message_length = usize::from(crate::bytes::read_u16(response, 8)?);
    if header.family != "ddp_lock"
        || header.transaction_id != Some(sequence)
        || header.opcode != operation
        || message_length < 12
        || message_length.checked_add(8)? != response.len()
    {
        return None;
    }
    Some(())
}

pub fn lock_operation(
    device_ip: IpAddr,
    local_ip: IpAddr,
    pin: &str,
    key: &[u8],
    operation: LockOperation,
) -> Result<LockResult, LockError> {
    validate_pin(pin)?;
    if key.len() != KEY_LENGTH {
        return Err(LockError::InvalidKey);
    }

    let socket = lock_socket(local_ip)?;
    let address = SocketAddr::new(device_ip, DEVICE_LOCK_PORT);
    let sequence = 1u16;

    for _ in 0..LOCK_MAX_RETRIES {
        socket.send_to(&build_challenge_request(sequence), address)?;

        let mut buffer = [0u8; 1024];
        let length = match socket.recv_from(&mut buffer) {
            Ok((length, _)) => length,
            Err(error) if is_timeout(&error) => continue,
            Err(error) => return Err(error.into()),
        };

        let nonce = match parse_nonce(&buffer[..length]) {
            Some(nonce) => nonce,
            None => continue,
        };

        let token = compute_token(pin, &nonce, key)?;
        socket.send_to(&build_auth_message(sequence, operation, &token), address)?;

        let length = match socket.recv_from(&mut buffer) {
            Ok((length, _)) => length,
            Err(error) if is_timeout(&error) => continue,
            Err(error) => return Err(error.into()),
        };

        if let Some(result) = parse_lock_result(&buffer[..length]) {
            return Ok(result);
        }
    }

    Err(LockError::Timeout)
}

fn lock_socket(local_ip: IpAddr) -> Result<UdpSocket, LockError> {
    let socket = UdpSocket::bind((local_ip, 0))?;
    socket.set_read_timeout(Some(Duration::from_secs(LOCK_TIMEOUT_SECONDS)))?;
    Ok(socket)
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}
