use std::net::SocketAddrV4;
use std::num::NonZeroU16;
use std::ops::Range;

use serde::Serialize;

use crate::bytes::{read_u16, read_u32};
use crate::protocol::{response_envelope, RESPONSE_HEADER_SIZE};

use super::Error;

pub const REVISION: u16 = 0x1200;
pub const ALLOCATION_OPERATION: u16 = 0x0100;
pub const MAXIMUM_DATAGRAM_BYTES: usize = 1400;
const MEDIA_SELECTOR: u16 = 4;
const ALIGNMENT: usize = 4;
const REQUEST_BASE_BYTES: usize = 30;
const REQUEST_EXTENSION_OFFSET: usize = 32;
const REQUEST_EXTENSION_BYTES: usize = 36;
const REQUEST_EXTENSION_WORDS: u8 = 18;
const REQUEST_EXTENSION_KIND: u16 = 2;
const ENDPOINT_BYTES: usize = 8;
const ENDPOINT_LENGTH: u8 = 8;
const IPV4_FAMILY: u8 = 2;
const OPAQUE_REQUEST_BYTES: usize = 49;
const REPLY_COUNT_OFFSET: usize = 16;
const REPLY_OUTCOMES_OFFSET: usize = 18;
const REPLY_BASE_BYTES: usize = 22;
const REPLY_EXTENSION_BYTES: usize = 12;
const REPLY_EXTENSION_MINIMUM_WORDS: u8 = 6;
const RESULT_LOW_BITS: u16 = 0x0fff;
const RESULT_SUCCESS: u16 = 1;

pub struct OptionalFields<'a> {
    pub raw_at_8: Option<u16>,
    pub raw_at_28: Option<u16>,
    pub opaque_span: Option<&'a [u8; OPAQUE_REQUEST_BYTES]>,
    pub raw_option: Option<u8>,
}

pub struct Allocation<'a> {
    pub receiver_host_label: &'a [u8],
    pub flow_label: Option<&'a [u8]>,
    pub source_service_channel: NonZeroU16,
    pub destination: SocketAddrV4,
    pub optional: OptionalFields<'a>,
}

fn label(value: &[u8]) -> Result<(), Error> {
    if value.is_empty() || value.contains(&0) || value.len() > MAXIMUM_DATAGRAM_BYTES {
        return Err(Error(
            "video source-control label is empty, contains NUL or exceeds the packet budget",
        ));
    }

    Ok(())
}

fn put_word(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + size_of::<u16>()].copy_from_slice(&value.to_be_bytes());
}

fn word(bytes: &[u8], offset: usize) -> Result<u16, Error> {
    read_u16(bytes, offset).ok_or(Error("video source-control word is truncated"))
}

fn double_word(bytes: &[u8], offset: usize) -> Result<u32, Error> {
    read_u32(bytes, offset).ok_or(Error("video source-control double word is truncated"))
}

fn append_label(bytes: &mut Vec<u8>, value: &[u8]) -> Result<u16, Error> {
    label(value)?;
    let offset = u16::try_from(bytes.len()).map_err(|_| Error("video label offset overflow"))?;
    bytes.extend_from_slice(value);
    bytes.push(0);
    Ok(offset)
}

impl Allocation<'_> {
    pub fn encode(&self, transaction: u16) -> Result<Vec<u8>, Error> {
        label(self.receiver_host_label)?;

        if let Some(value) = self.flow_label {
            label(value)?;
        }

        if self.destination.port() == 0 || self.destination.ip().is_unspecified() {
            return Err(Error(
                "video allocation requires an owned nonzero media destination",
            ));
        }

        let mut bytes = vec![0; REQUEST_EXTENSION_OFFSET + REQUEST_EXTENSION_BYTES];
        let host_pointer = append_label(&mut bytes, self.receiver_host_label)?;
        let flow_pointer = self
            .flow_label
            .map(|value| append_label(&mut bytes, value))
            .transpose()?;
        let opaque_pointer = if let Some(span) = self.optional.opaque_span {
            let pointer =
                u16::try_from(bytes.len()).map_err(|_| Error("video opaque offset overflow"))?;
            bytes.extend_from_slice(span);
            pointer
        } else {
            0
        };

        bytes.resize(bytes.len().next_multiple_of(ALIGNMENT), 0);
        let endpoint = bytes.len();
        bytes.resize(endpoint + ENDPOINT_BYTES, 0);

        if bytes.len() > MAXIMUM_DATAGRAM_BYTES {
            return Err(Error(
                "video allocation exceeds the selected datagram budget",
            ));
        }

        let length =
            u16::try_from(bytes.len()).map_err(|_| Error("video allocation length overflow"))?;
        put_word(&mut bytes, 0, REVISION);
        put_word(&mut bytes, 2, length);
        put_word(&mut bytes, 4, transaction);
        put_word(&mut bytes, 6, ALLOCATION_OPERATION);
        put_word(&mut bytes, 10, host_pointer);
        put_word(&mut bytes, 20, 1);
        put_word(&mut bytes, 22, 1);
        put_word(&mut bytes, 24, endpoint as u16);
        put_word(&mut bytes, 26, self.source_service_channel.get());
        put_word(
            &mut bytes,
            REQUEST_BASE_BYTES - size_of::<u16>(),
            REQUEST_EXTENSION_OFFSET as u16,
        );

        let extension = REQUEST_EXTENSION_OFFSET;
        bytes[extension] = REQUEST_EXTENSION_WORDS;
        put_word(&mut bytes, extension + 2, REQUEST_EXTENSION_KIND);
        put_word(&mut bytes, extension + 6, flow_pointer.unwrap_or(0));
        put_word(
            &mut bytes,
            extension + 8,
            self.optional.raw_at_8.unwrap_or(0),
        );
        put_word(&mut bytes, extension + 20, MEDIA_SELECTOR);
        put_word(
            &mut bytes,
            extension + 28,
            self.optional.raw_at_28.unwrap_or(0),
        );
        put_word(&mut bytes, extension + 30, opaque_pointer);
        bytes[extension + 32] = self.optional.raw_option.unwrap_or(0);
        bytes[endpoint] = ENDPOINT_LENGTH;
        bytes[endpoint + 1] = IPV4_FAMILY;
        put_word(&mut bytes, endpoint + 2, self.destination.port());
        bytes[endpoint + 4..endpoint + ENDPOINT_BYTES]
            .copy_from_slice(&self.destination.ip().octets());

        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct AllocationHandle {
    pub global_flow_identifier: u16,
    pub ownership_value: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Extension {
    pub raw_reserved: [u8; 3],
    pub details_pointer: u16,
    pub channel_details: Option<Vec<u16>>,
    pub raw_media_value: u32,
    pub unknown_tail: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AllocationReply {
    pub transaction: u16,
    pub result: u16,
    pub allocation: Option<AllocationHandle>,
    pub interface_outcomes: Option<Vec<u16>>,
    pub raw_base_tail: Option<[u8; 2]>,
    pub extension: Option<Extension>,
    pub trailing_datagram_bytes: Vec<u8>,
}

fn object(bytes: &[u8], pointer: u16, length: usize, base: usize) -> Result<Range<usize>, Error> {
    let start = usize::from(pointer);
    let end = start
        .checked_add(length)
        .ok_or(Error("video reply object extent overflow"))?;

    if start < base || start % size_of::<u16>() != 0 || start >= bytes.len() || end > bytes.len() {
        return Err(Error(
            "video reply object is misaligned or outside the declared heap",
        ));
    }

    Ok(start..end)
}

fn extension(bytes: &[u8], pointer: u16, base: usize) -> Result<Extension, Error> {
    let minimum = object(bytes, pointer, REPLY_EXTENSION_BYTES, base)?;
    let start = minimum.start;
    let words = bytes[start];

    if words < REPLY_EXTENSION_MINIMUM_WORDS {
        return Err(Error(
            "video allocation extension is shorter than its known fields",
        ));
    }

    let extent = object(bytes, pointer, usize::from(words) * size_of::<u16>(), base)?;
    let count = usize::from(word(bytes, start + 4)?);
    let details_pointer = word(bytes, start + 6)?;
    let channel_details = if details_pointer == 0 {
        if count != 0 {
            return Err(Error("video allocation channel details have no pointer"));
        }

        None
    } else {
        let details = object(bytes, details_pointer, count * size_of::<u16>(), base)?;

        if extent.contains(&details.start)
            || (details.start < extent.end && extent.start < details.end)
        {
            return Err(Error("video allocation details overlap the extension"));
        }

        Some(
            bytes[details]
                .chunks_exact(size_of::<u16>())
                .map(|value| u16::from_be_bytes([value[0], value[1]]))
                .collect(),
        )
    };

    Ok(Extension {
        raw_reserved: [bytes[start + 1], bytes[start + 2], bytes[start + 3]],
        details_pointer,
        channel_details,
        raw_media_value: double_word(bytes, start + 8)?,
        unknown_tail: bytes[start + REPLY_EXTENSION_BYTES..extent.end].to_vec(),
    })
}

pub fn decode_allocation_reply(datagram: &[u8]) -> Result<AllocationReply, Error> {
    if !(RESPONSE_HEADER_SIZE..=MAXIMUM_DATAGRAM_BYTES).contains(&datagram.len()) {
        return Err(Error(
            "video allocation reply is outside the selected datagram budget",
        ));
    }

    let declared = usize::from(word(datagram, 2)?);
    let bytes = datagram
        .get(..declared)
        .ok_or(Error("video allocation reply is truncated"))?;
    let header =
        response_envelope(bytes).ok_or(Error("invalid video allocation reply envelope"))?;

    if header.protocol_id != REVISION
        || header.opcode != ALLOCATION_OPERATION
        || header.result_code == 0
    {
        return Err(Error(
            "reply is outside the selected video allocation profile",
        ));
    }

    let mut reply = AllocationReply {
        transaction: header.transaction_id,
        result: header.result_code,
        allocation: None,
        interface_outcomes: None,
        raw_base_tail: None,
        extension: None,
        trailing_datagram_bytes: datagram[declared..].to_vec(),
    };

    if declared == RESPONSE_HEADER_SIZE && header.result_code & RESULT_LOW_BITS != RESULT_SUCCESS {
        return Ok(reply);
    }

    let count = usize::from(word(bytes, REPLY_COUNT_OFFSET)?);
    let base = REPLY_BASE_BYTES + count * size_of::<u16>();
    let directory = bytes
        .get(REPLY_OUTCOMES_OFFSET..base)
        .ok_or(Error("video allocation interface outcomes are truncated"))?;
    let pointer = word(bytes, base - size_of::<u32>())?;
    let parsed_extension = if pointer == 0 {
        None
    } else {
        Some(extension(bytes, pointer, base)?)
    };

    reply.allocation = Some(AllocationHandle {
        global_flow_identifier: word(bytes, RESPONSE_HEADER_SIZE)?,
        ownership_value: double_word(bytes, RESPONSE_HEADER_SIZE + size_of::<u16>())?,
    });
    reply.interface_outcomes = Some(
        directory[..count * size_of::<u16>()]
            .chunks_exact(size_of::<u16>())
            .map(|value| u16::from_be_bytes([value[0], value[1]]))
            .collect(),
    );
    reply.raw_base_tail = Some([bytes[base - 2], bytes[base - 1]]);
    reply.extension = parsed_extension;

    Ok(reply)
}
