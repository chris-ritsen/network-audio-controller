use std::ops::Range;

use serde::Serialize;

use crate::bytes::{read_u16, read_u32};
use crate::protocol::{
    common_arc_protocol_opcodes, validate_response_envelope, OPCODE_TX_CHANNEL_INFO,
    RESPONSE_HEADER_SIZE, RESULT_CODE_MORE_PAGES, RESULT_CODE_SUCCESS,
};

use super::{BODY_HEADER_SIZE, TX_CHANNELS_PER_PAGE, TX_RECORD_SIZE};

const DIRECTORY_BYTES: usize = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE;
const ROW_IDENTIFIER_OFFSET: usize = 0;
const ROW_FLAGS_OFFSET: usize = 2;
const ROW_AVAILABILITY_OFFSET: usize = 4;
const ROW_NAME_OFFSET: usize = 6;
const DESCRIPTOR_RATE_OFFSET: usize = 0;
const DESCRIPTOR_CAPACITY_OFFSET: usize = 4;
const DESCRIPTOR_COUNT_OFFSET: usize = 5;
const EXTENSION_PREFERENCE_OFFSET: usize = 2;
const EXTENSION_SCALAR_OFFSET: usize = 4;
const EXTENSION_MAP_OFFSET: usize = 6;
const DESCRIPTOR_PREFIX_BYTES: usize = 6;
const EMPTY_DESCRIPTOR_BYTES: usize = 8;
const DESCRIPTOR_EXTENSION_BYTES: usize = 8;
const ENCODING_BYTES: usize = 2;
const DESCRIPTOR_ALIGNMENT: usize = 4;
const KNOWN_ENCODING_MAP: u16 = 0x00ff;
const ENCODINGS_PER_GROUP: u16 = 8;
const MAXIMUM_MAPPED_ENCODING: u16 = 64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum Field<T> {
    Present(T),
    Missing,
    Invalid(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AvailabilityExtension {
    pub prefix: [u8; 2],
    pub preferred_encoding: u16,
    pub scalar: u16,
    pub encoding_map: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Availability {
    pub sample_rate: u32,
    pub capacity: u8,
    pub encodings: Vec<u16>,
    pub extension: Option<AvailabilityExtension>,
    pub raw_bytes: Vec<u8>,
}

impl Availability {
    pub fn supports_encoding(&self, encoding: u16) -> Option<bool> {
        if self.encodings.contains(&encoding) {
            return Some(true);
        }

        let Some(extension) = &self.extension else {
            return Some(false);
        };

        if (1..=MAXIMUM_MAPPED_ENCODING).contains(&encoding)
            && extension.encoding_map & (1 << ((encoding - 1) / ENCODINGS_PER_GROUP)) != 0
        {
            return Some(true);
        }

        (extension.encoding_map & !KNOWN_ENCODING_MAP == 0).then_some(false)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TransmitChannel {
    pub identifier: u16,
    pub flags: u16,
    pub name: Field<Vec<u8>>,
    pub availability: Field<Availability>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TransmitChannelPage {
    pub result: u16,
    pub capacity: u8,
    pub channels: Vec<TransmitChannel>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ObjectKind {
    Name,
    Availability,
}

struct Object {
    channel: usize,
    kind: ObjectKind,
    extent: Range<usize>,
}

fn availability(packet: &[u8], pointer: usize) -> Option<(Availability, usize)> {
    if pointer % DESCRIPTOR_ALIGNMENT != 0 {
        return None;
    }

    let descriptor = packet.get(pointer..)?;
    descriptor.get(..EMPTY_DESCRIPTOR_BYTES)?;
    let capacity = descriptor[DESCRIPTOR_CAPACITY_OFFSET];
    let count = descriptor[DESCRIPTOR_COUNT_OFFSET];

    if count > capacity {
        return None;
    }

    let extension_offset = DESCRIPTOR_PREFIX_BYTES + ENCODING_BYTES * usize::from(capacity);
    let extent = if capacity == 0 {
        EMPTY_DESCRIPTOR_BYTES
    } else {
        extension_offset + DESCRIPTOR_EXTENSION_BYTES
    };
    let raw_bytes = descriptor.get(..extent)?.to_vec();
    let mut encodings = Vec::with_capacity(usize::from(count));

    for index in 0..usize::from(count) {
        encodings.push(read_u16(
            descriptor,
            DESCRIPTOR_PREFIX_BYTES + ENCODING_BYTES * index,
        )?);
    }

    let extension = if capacity == 0 {
        None
    } else {
        let bytes = descriptor.get(extension_offset..extent)?;
        Some(AvailabilityExtension {
            prefix: [bytes[0], bytes[1]],
            preferred_encoding: read_u16(bytes, EXTENSION_PREFERENCE_OFFSET)?,
            scalar: read_u16(bytes, EXTENSION_SCALAR_OFFSET)?,
            encoding_map: read_u16(bytes, EXTENSION_MAP_OFFSET)?,
        })
    };

    Some((
        Availability {
            sample_rate: read_u32(descriptor, DESCRIPTOR_RATE_OFFSET)?,
            capacity,
            encodings,
            extension,
            raw_bytes,
        },
        extent,
    ))
}

fn field<T>(
    packet: &[u8],
    pointer: u16,
    table_end: usize,
    channel: usize,
    kind: ObjectKind,
    objects: &mut Vec<Object>,
    decode: impl FnOnce(&[u8], usize) -> Option<(T, usize)>,
) -> Field<T> {
    let pointer = usize::from(pointer);

    if pointer == 0 {
        return Field::Missing;
    }

    if pointer < table_end {
        return Field::Invalid("channel object overlaps the reserved table");
    }

    let Some((value, length)) = decode(packet, pointer) else {
        return Field::Invalid("channel object is malformed or truncated");
    };
    objects.push(Object {
        channel,
        kind,
        extent: pointer..pointer + length,
    });
    Field::Present(value)
}

pub fn parse_transmit_channel_page(packet: &[u8]) -> Option<TransmitChannelPage> {
    let envelope = validate_response_envelope(
        packet,
        &common_arc_protocol_opcodes(OPCODE_TX_CHANNEL_INFO),
        &[RESULT_CODE_SUCCESS, RESULT_CODE_MORE_PAGES],
    )?;
    let capacity = *envelope.body.first()?;
    let count = *envelope.body.get(1)?;

    if u16::from(capacity) > TX_CHANNELS_PER_PAGE || count > capacity {
        return None;
    }

    let table_end = DIRECTORY_BYTES + usize::from(capacity) * TX_RECORD_SIZE;
    packet.get(..table_end)?;
    let mut channels = Vec::with_capacity(usize::from(count));
    let mut objects = Vec::with_capacity(usize::from(count) * 2);
    let mut previous_identifier = 0;

    for index in 0..usize::from(count) {
        let row = packet.get(DIRECTORY_BYTES + index * TX_RECORD_SIZE..)?;
        let identifier = read_u16(row, ROW_IDENTIFIER_OFFSET)?;

        if identifier <= previous_identifier {
            return None;
        }

        previous_identifier = identifier;
        let name = field(
            packet,
            read_u16(row, ROW_NAME_OFFSET)?,
            table_end,
            index,
            ObjectKind::Name,
            &mut objects,
            |packet, pointer| {
                let bytes = packet.get(pointer..)?;
                let length = bytes.iter().position(|byte| *byte == 0)?;
                Some((bytes[..length].to_vec(), length + 1))
            },
        );
        let availability = field(
            packet,
            read_u16(row, ROW_AVAILABILITY_OFFSET)?,
            table_end,
            index,
            ObjectKind::Availability,
            &mut objects,
            availability,
        );
        channels.push(TransmitChannel {
            identifier,
            flags: read_u16(row, ROW_FLAGS_OFFSET)?,
            name,
            availability,
        });
    }

    for (index, left) in objects.iter().enumerate() {
        for right in &objects[index + 1..] {
            let overlaps =
                left.extent.start < right.extent.end && right.extent.start < left.extent.end;

            if overlaps && (left.extent != right.extent || left.kind != right.kind) {
                for object in [left, right] {
                    match object.kind {
                        ObjectKind::Name => {
                            channels[object.channel].name =
                                Field::Invalid("channel objects overlap")
                        }
                        ObjectKind::Availability => {
                            channels[object.channel].availability =
                                Field::Invalid("channel objects overlap")
                        }
                    }
                }
            }
        }
    }

    Some(TransmitChannelPage {
        result: envelope.result_code,
        capacity,
        channels,
    })
}
