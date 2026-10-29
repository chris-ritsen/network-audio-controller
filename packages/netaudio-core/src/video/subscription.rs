use crate::bytes::read_u16;
use crate::commands::OPCODE_MODERN_ARC_SUBSCRIPTION;
use crate::protocol::{RESPONSE_HEADER_SIZE, RESULT_CODE_SUCCESS};

use super::Error;

const HEADER_BYTES: usize = RESPONSE_HEADER_SIZE;
const PREFIX_BYTES: usize = 10;
const ROW_BYTES: usize = 8;
const MEDIA_SELECTOR: u16 = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Intent {
    Set {
        receiver: u16,
        source_channel: Vec<u8>,
        source_device: Vec<u8>,
    },
    Clear {
        receiver: u16,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub revision: u16,
    pub transaction: u16,
    pub intents: Vec<Intent>,
    pub unsupported_form: bool,
}

fn word(bytes: &[u8], offset: usize) -> Result<u16, Error> {
    read_u16(bytes, offset).ok_or(Error("video subscription is truncated"))
}

fn label(bytes: &[u8], pointer: u16, minimum: usize) -> Result<Vec<u8>, Error> {
    let start = usize::from(pointer);
    if start < minimum || start >= bytes.len() {
        return Err(Error(
            "video subscription label pointer is outside its heap",
        ));
    }
    let suffix = &bytes[start..];
    let Some(length) = suffix.iter().position(|value| *value == 0) else {
        return Err(Error("video subscription label lacks a terminator"));
    };
    if length == 0 {
        return Err(Error("video subscription label is empty"));
    }

    Ok(suffix[..length].to_vec())
}

impl Request {
    pub fn decode(datagram: &[u8]) -> Result<Self, Error> {
        if datagram.len() < HEADER_BYTES + PREFIX_BYTES {
            return Err(Error(
                "video subscription is shorter than its fixed request prefix",
            ));
        }
        let declared = usize::from(word(datagram, 2)?);
        if declared != datagram.len() {
            return Err(Error(
                "video subscription declared length differs from its datagram",
            ));
        }
        let revision = word(datagram, 0)?;
        let transaction = word(datagram, 4)?;
        if !(0x2802..=0x280f).contains(&revision)
            || word(datagram, 6)? != OPCODE_MODERN_ARC_SUBSCRIPTION
            || word(datagram, 8)? != 0
            || datagram[10..16] != [0; 6]
            || datagram[16] != ROW_BYTES as u8
            || datagram[17] != 0
        {
            return Err(Error(
                "video subscription is outside the selected Media-4 profile",
            ));
        }
        let capacity = usize::from(datagram[18]);
        let count = usize::from(datagram[19]);
        if count == 0 {
            return Err(Error("video subscription has no live receiver rows"));
        }
        if count > capacity {
            return Err(Error(
                "video subscription live receiver rows exceed its declared capacity",
            ));
        }
        let heap = HEADER_BYTES
            .checked_add(PREFIX_BYTES)
            .and_then(|value| value.checked_add(capacity * ROW_BYTES))
            .ok_or(Error("video subscription row extent overflows"))?;
        if heap > declared {
            return Err(Error("video subscription rows exceed the declared extent"));
        }

        let mut intents = Vec::new();
        intents
            .try_reserve(count)
            .map_err(|_| Error("video subscription staging allocation failed"))?;
        let mut unsupported_form = false;
        for index in 0..count {
            let row = HEADER_BYTES + PREFIX_BYTES + index * ROW_BYTES;
            let receiver = word(datagram, row)?;
            let media = word(datagram, row + 2)?;
            let channel = word(datagram, row + 4)?;
            let device = word(datagram, row + 6)?;
            if receiver == 0 {
                return Err(Error("video subscription receiver identifier is zero"));
            }
            unsupported_form |= media != MEDIA_SELECTOR;
            let intent = match (channel, device) {
                (0, 0) => Intent::Clear { receiver },
                (0, _) | (_, 0) => {
                    return Err(Error("video subscription set requires both source labels"));
                }
                _ => Intent::Set {
                    receiver,
                    source_channel: label(datagram, channel, heap)?,
                    source_device: label(datagram, device, heap)?,
                },
            };
            if let Intent::Set { source_device, .. } = &intent {
                unsupported_form |= source_device == b".";
            }
            intents.push(intent);
        }

        Ok(Self {
            revision,
            transaction,
            intents,
            unsupported_form,
        })
    }

    pub fn acknowledgement(&self) -> [u8; HEADER_BYTES] {
        let mut bytes = [0; HEADER_BYTES];
        bytes[0..2].copy_from_slice(&self.revision.to_be_bytes());
        bytes[2..4].copy_from_slice(&(HEADER_BYTES as u16).to_be_bytes());
        bytes[4..6].copy_from_slice(&self.transaction.to_be_bytes());
        bytes[6..8].copy_from_slice(&OPCODE_MODERN_ARC_SUBSCRIPTION.to_be_bytes());
        let result = if self.unsupported_form {
            0x0030
        } else {
            RESULT_CODE_SUCCESS
        };
        bytes[8..10].copy_from_slice(&result.to_be_bytes());
        bytes
    }
}
