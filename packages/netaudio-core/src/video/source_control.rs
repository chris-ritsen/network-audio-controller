use std::net::{Ipv4Addr, SocketAddrV4};
use std::num::NonZeroU16;
use std::ops::Range;

use serde::Serialize;

use crate::bytes::{read_u16, read_u32};
use crate::protocol::{response_envelope, RESPONSE_HEADER_SIZE};

use super::Error;

pub const REVISION: u16 = 0x1200;
pub const ALLOCATION_OPERATION: u16 = 0x0100;
pub const STOP_OPERATION: u16 = 0x0101;
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
const STOP_REQUEST_BYTES: usize = 16;

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

pub struct AllocationRequest<'a> {
    pub transaction: u16,
    pub allocation: Allocation<'a>,
}

fn request_header(bytes: &[u8], operation: u16) -> Result<u16, Error> {
    if !(RESPONSE_HEADER_SIZE..=MAXIMUM_DATAGRAM_BYTES).contains(&bytes.len())
        || usize::from(word(bytes, 2)?) != bytes.len()
        || word(bytes, 0)? != REVISION
        || word(bytes, 6)? != operation
        || word(bytes, 8)? != 0
    {
        return Err(Error(
            "request is outside the selected video source-control profile",
        ));
    }
    word(bytes, 4)
}

fn request_label<'a>(bytes: &'a [u8], pointer: u16, cursor: &mut usize) -> Result<&'a [u8], Error> {
    if usize::from(pointer) != *cursor {
        return Err(Error(
            "video request label is outside its selected object position",
        ));
    }
    let tail = bytes
        .get(*cursor..)
        .ok_or(Error("video request label pointer is out of bounds"))?;
    let length = tail
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(Error("video request label is unterminated"))?;
    let value = &tail[..length];
    label(value)?;
    *cursor += length + 1;
    Ok(value)
}

pub fn decode_allocation_request(bytes: &[u8]) -> Result<AllocationRequest<'_>, Error> {
    let transaction = request_header(bytes, ALLOCATION_OPERATION)?;
    let extension = REQUEST_EXTENSION_OFFSET;
    let fixed = bytes
        .get(..extension + REQUEST_EXTENSION_BYTES)
        .ok_or(Error("video allocation request fixed fields are truncated"))?;
    if fixed[12..20].iter().any(|byte| *byte != 0)
        || word(bytes, 20)? != 1
        || word(bytes, 22)? != 1
        || usize::from(word(bytes, 28)?) != extension
        || fixed[30..32].iter().any(|byte| *byte != 0)
        || fixed[extension] != REQUEST_EXTENSION_WORDS
        || fixed[extension + 1] != 0
        || word(bytes, extension + 2)? != REQUEST_EXTENSION_KIND
        || word(bytes, extension + 4)? != 0
        || fixed[extension + 10..extension + 20]
            .iter()
            .any(|byte| *byte != 0)
        || word(bytes, extension + 20)? != MEDIA_SELECTOR
        || fixed[extension + 22..extension + 28]
            .iter()
            .any(|byte| *byte != 0)
        || fixed[extension + 33..extension + 36]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(Error(
            "video allocation request uses an unsupported object form",
        ));
    }
    let source_service_channel = NonZeroU16::new(word(bytes, 26)?)
        .ok_or(Error("video allocation source-service channel is zero"))?;
    let mut cursor = extension + REQUEST_EXTENSION_BYTES;
    let receiver_host_label = request_label(bytes, word(bytes, 10)?, &mut cursor)?;
    let flow_pointer = word(bytes, extension + 6)?;
    let flow_label = if flow_pointer == 0 {
        None
    } else {
        Some(request_label(bytes, flow_pointer, &mut cursor)?)
    };
    let opaque_pointer = word(bytes, extension + 30)?;
    let opaque_span = if opaque_pointer == 0 {
        None
    } else {
        if usize::from(opaque_pointer) != cursor {
            return Err(Error(
                "video allocation opaque span is outside its selected object position",
            ));
        }
        let span = bytes
            .get(cursor..cursor + OPAQUE_REQUEST_BYTES)
            .ok_or(Error("video allocation opaque span is truncated"))?;
        cursor += OPAQUE_REQUEST_BYTES;
        Some(
            span.try_into()
                .map_err(|_| Error("video allocation opaque span has invalid length"))?,
        )
    };
    let endpoint = cursor.next_multiple_of(ALIGNMENT);
    if usize::from(word(bytes, 24)?) != endpoint
        || endpoint + ENDPOINT_BYTES != bytes.len()
        || bytes[cursor..endpoint].iter().any(|byte| *byte != 0)
        || bytes[endpoint] != ENDPOINT_LENGTH
        || bytes[endpoint + 1] != IPV4_FAMILY
    {
        return Err(Error("video allocation endpoint has invalid geometry"));
    }
    let destination = SocketAddrV4::new(
        Ipv4Addr::new(
            bytes[endpoint + 4],
            bytes[endpoint + 5],
            bytes[endpoint + 6],
            bytes[endpoint + 7],
        ),
        word(bytes, endpoint + 2)?,
    );
    if destination.port() == 0 || destination.ip().is_unspecified() {
        return Err(Error(
            "video allocation requires a nonzero media destination",
        ));
    }
    Ok(AllocationRequest {
        transaction,
        allocation: Allocation {
            receiver_host_label,
            flow_label,
            source_service_channel,
            destination,
            optional: OptionalFields {
                raw_at_8: Some(word(bytes, extension + 8)?),
                raw_at_28: Some(word(bytes, extension + 28)?),
                opaque_span,
                raw_option: Some(bytes[extension + 32]),
            },
        },
    })
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

pub struct AllocationOutcome<'a> {
    pub allocation: AllocationHandle,
    pub interface_outcomes: &'a [u16],
}

fn reply_header(
    length: usize,
    transaction: u16,
    operation: u16,
    result: u16,
) -> Result<Vec<u8>, Error> {
    if !(RESPONSE_HEADER_SIZE..=MAXIMUM_DATAGRAM_BYTES).contains(&length) || result == 0 {
        return Err(Error(
            "video source-control reply has invalid extent or result",
        ));
    }
    let mut bytes = vec![0; length];
    put_word(&mut bytes, 0, REVISION);
    put_word(&mut bytes, 2, length as u16);
    put_word(&mut bytes, 4, transaction);
    put_word(&mut bytes, 6, operation);
    put_word(&mut bytes, 8, result);
    Ok(bytes)
}

pub fn encode_allocation_reply(
    transaction: u16,
    result: u16,
    outcome: Option<AllocationOutcome<'_>>,
) -> Result<Vec<u8>, Error> {
    let Some(outcome) = outcome else {
        if result & RESULT_LOW_BITS == RESULT_SUCCESS {
            return Err(Error(
                "successful video allocation reply requires its complete handle and outcomes",
            ));
        }
        return reply_header(
            RESPONSE_HEADER_SIZE,
            transaction,
            ALLOCATION_OPERATION,
            result,
        );
    };
    let count = outcome.interface_outcomes.len();
    let length = count
        .checked_mul(size_of::<u16>())
        .and_then(|length| length.checked_add(REPLY_BASE_BYTES))
        .ok_or(Error("video allocation reply extent overflow"))?;
    let mut bytes = reply_header(length, transaction, ALLOCATION_OPERATION, result)?;
    put_word(&mut bytes, 10, outcome.allocation.global_flow_identifier);
    bytes[12..16].copy_from_slice(&outcome.allocation.ownership_value.to_be_bytes());
    put_word(&mut bytes, REPLY_COUNT_OFFSET, count as u16);
    for (index, value) in outcome.interface_outcomes.iter().enumerate() {
        put_word(
            &mut bytes,
            REPLY_OUTCOMES_OFFSET + index * size_of::<u16>(),
            *value,
        );
    }
    Ok(bytes)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StopRequest {
    pub transaction: u16,
    pub allocation: AllocationHandle,
}

impl StopRequest {
    pub fn encode(self) -> [u8; STOP_REQUEST_BYTES] {
        let mut bytes = [0; STOP_REQUEST_BYTES];
        put_word(&mut bytes, 0, REVISION);
        put_word(&mut bytes, 2, STOP_REQUEST_BYTES as u16);
        put_word(&mut bytes, 4, self.transaction);
        put_word(&mut bytes, 6, STOP_OPERATION);
        put_word(&mut bytes, 10, self.allocation.global_flow_identifier);
        bytes[12..16].copy_from_slice(&self.allocation.ownership_value.to_be_bytes());
        bytes
    }
}

pub fn decode_stop_request(bytes: &[u8]) -> Result<StopRequest, Error> {
    let transaction = request_header(bytes, STOP_OPERATION)?;
    if bytes.len() != STOP_REQUEST_BYTES {
        return Err(Error(
            "video STOP request must contain exactly its current allocation handle",
        ));
    }
    Ok(StopRequest {
        transaction,
        allocation: AllocationHandle {
            global_flow_identifier: word(bytes, 10)?,
            ownership_value: double_word(bytes, 12)?,
        },
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct StopReply {
    pub transaction: u16,
    pub result: u16,
}

impl StopReply {
    pub fn encode(self) -> Result<Vec<u8>, Error> {
        reply_header(
            RESPONSE_HEADER_SIZE,
            self.transaction,
            STOP_OPERATION,
            self.result,
        )
    }
}

pub fn decode_stop_reply(bytes: &[u8]) -> Result<StopReply, Error> {
    if bytes.len() != RESPONSE_HEADER_SIZE {
        return Err(Error(
            "video STOP reply must contain exactly its result header",
        ));
    }
    let header = response_envelope(bytes).ok_or(Error("invalid video STOP reply envelope"))?;
    if usize::from(word(bytes, 2)?) != bytes.len()
        || header.protocol_id != REVISION
        || header.opcode != STOP_OPERATION
        || header.result_code == 0
    {
        return Err(Error("reply is outside the selected video STOP profile"));
    }
    Ok(StopReply {
        transaction: header.transaction_id,
        result: header.result_code,
    })
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
