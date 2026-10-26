use serde::Serialize;

use crate::bytes::read_u16;
use crate::commands::OPCODE_DEVICE_INFO;
use crate::protocol::{response_envelope, PROTOCOL_ARC_2809, RESULT_CODE_SUCCESS};

const DECLARED_LENGTH_OFFSET: usize = 2;
const METADATA_EXTENT_OFFSET: usize = 10;
const DESCRIPTOR_POINTER_OFFSET: usize = 12;
const DEFAULT_DESCRIPTOR_MINIMUM: usize = 20;
const MINIMUM_METADATA_EXTENT: usize = 14;
const DESCRIPTOR_BYTES: usize = 16;
const CURRENT_NAME_POINTER_OFFSET: usize = 2;
const DEFAULT_NAME_POINTER_OFFSET: usize = 4;
const ADVERTISED_NAME_POINTER_OFFSET: usize = 6;
const MINIMUM_NAME_POINTER: usize = 28;
const RESULT_STATUS_MASK: u16 = 0x0fff;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum IdentityName {
    Absent,
    Present {
        bytes: Vec<u8>,
    },
    Malformed {
        pointer: u16,
        reason: IdentityNameFailure,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityNameFailure {
    OutsideNameExtent,
    MissingTerminator,
}

impl IdentityName {
    pub fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Present { bytes } => Some(bytes),
            Self::Absent | Self::Malformed { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StructuredDeviceIdentity {
    pub protocol_revision: u16,
    pub transaction: u16,
    pub result: u16,
    pub descriptor_pointer: u16,
    pub descriptor_minimum: usize,
    pub raw_metadata: Vec<u8>,
    pub raw_descriptor: [u8; DESCRIPTOR_BYTES],
    pub current_name: IdentityName,
    pub default_name: IdentityName,
    pub advertised_name: IdentityName,
    pub trailing_datagram_bytes: Vec<u8>,
}

fn name(packet: &[u8], pointer: u16) -> IdentityName {
    if pointer == 0 {
        return IdentityName::Absent;
    }

    let offset = usize::from(pointer);

    if !(MINIMUM_NAME_POINTER..packet.len()).contains(&offset) {
        return IdentityName::Malformed {
            pointer,
            reason: IdentityNameFailure::OutsideNameExtent,
        };
    }

    let suffix = &packet[offset..];

    match suffix.iter().position(|byte| *byte == 0) {
        Some(length) => IdentityName::Present {
            bytes: suffix[..length].to_vec(),
        },
        None => IdentityName::Malformed {
            pointer,
            reason: IdentityNameFailure::MissingTerminator,
        },
    }
}

pub fn parse_structured_device_identity(datagram: &[u8]) -> Option<StructuredDeviceIdentity> {
    let length = usize::from(read_u16(datagram, DECLARED_LENGTH_OFFSET)?);
    let packet = datagram.get(..length)?;
    let envelope = response_envelope(packet)?;

    if envelope.opcode != OPCODE_DEVICE_INFO
        || envelope.result_code & RESULT_STATUS_MASK != RESULT_CODE_SUCCESS
    {
        return None;
    }

    let descriptor_pointer = read_u16(packet, DESCRIPTOR_POINTER_OFFSET)?;
    let descriptor_minimum = if envelope.protocol_id >= PROTOCOL_ARC_2809 {
        match usize::from(read_u16(packet, METADATA_EXTENT_OFFSET)?) {
            0 => DEFAULT_DESCRIPTOR_MINIMUM,
            extent if extent >= MINIMUM_METADATA_EXTENT => extent,
            _ => return None,
        }
    } else {
        DEFAULT_DESCRIPTOR_MINIMUM
    };
    let start = usize::from(descriptor_pointer);

    if start < descriptor_minimum || start % 2 != 0 {
        return None;
    }

    let descriptor = packet.get(start..start.checked_add(DESCRIPTOR_BYTES)?)?;

    Some(StructuredDeviceIdentity {
        protocol_revision: envelope.protocol_id,
        transaction: envelope.transaction_id,
        result: envelope.result_code,
        descriptor_pointer,
        descriptor_minimum,
        raw_metadata: packet[METADATA_EXTENT_OFFSET..start].to_vec(),
        raw_descriptor: descriptor.try_into().ok()?,
        current_name: name(packet, read_u16(descriptor, CURRENT_NAME_POINTER_OFFSET)?),
        default_name: name(packet, read_u16(descriptor, DEFAULT_NAME_POINTER_OFFSET)?),
        advertised_name: name(
            packet,
            read_u16(descriptor, ADVERTISED_NAME_POINTER_OFFSET)?,
        ),
        trailing_datagram_bytes: datagram[length..].to_vec(),
    })
}
