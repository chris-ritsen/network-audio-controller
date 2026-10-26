use super::Error;

pub const HEADER_BYTES: usize = 12;
pub const MAXIMUM_DATAGRAM_BYTES: usize = 65_507;
const VERSION: u8 = 2;
const PADDING_FLAG: u8 = 0x20;
const EXTENSION_FLAG: u8 = 0x10;
const CONTRIBUTING_SOURCE_MASK: u8 = 0x0f;
const PAYLOAD_TYPE_MASK: u8 = 0x7f;
const MARKER_FLAG: u8 = 0x80;
const EXTENSION_HEADER_BYTES: usize = 4;
const EXTENSION_WORD_BYTES: usize = 4;
const ONE_BYTE_EXTENSION_PROFILE: u16 = 0xbede;
const RESERVED_EXTENSION_IDENTIFIER: u8 = 15;

#[derive(Debug)]
pub struct Packet<'a> {
    pub sequence: u16,
    pub timestamp: u32,
    pub marker: bool,
    pub payload: &'a [u8],
}

impl<'a> Packet<'a> {
    pub fn parse(
        bytes: &'a [u8],
        payload_type: u8,
        synchronization_source: u32,
    ) -> Result<Self, Error> {
        if payload_type > PAYLOAD_TYPE_MASK || bytes.len() < HEADER_BYTES {
            return Err(Error("invalid RTP header or configured payload type"));
        }

        if bytes[0] >> 6 != VERSION || bytes[0] & (PADDING_FLAG | CONTRIBUTING_SOURCE_MASK) != 0 {
            return Err(Error(
                "unsupported RTP version, padding or contributing sources",
            ));
        }

        if bytes[1] & PAYLOAD_TYPE_MASK != payload_type
            || u32::from_be_bytes(bytes[8..12].try_into().unwrap()) != synchronization_source
        {
            return Err(Error(
                "RTP payload type or source differs from the admitted session",
            ));
        }

        let mut payload_offset = HEADER_BYTES;

        if bytes[0] & EXTENSION_FLAG != 0 {
            let extension = bytes
                .get(HEADER_BYTES..HEADER_BYTES + EXTENSION_HEADER_BYTES)
                .ok_or(Error("truncated RTP extension header"))?;
            let profile = u16::from_be_bytes([extension[0], extension[1]]);
            let words = usize::from(u16::from_be_bytes([extension[2], extension[3]]));
            payload_offset = HEADER_BYTES + EXTENSION_HEADER_BYTES + words * EXTENSION_WORD_BYTES;
            let elements = bytes
                .get(HEADER_BYTES + EXTENSION_HEADER_BYTES..payload_offset)
                .ok_or(Error("RTP extension exceeds the datagram"))?;

            if profile == ONE_BYTE_EXTENSION_PROFILE {
                validate_one_byte_extensions(elements)?;
            }
        }

        let payload = bytes
            .get(payload_offset..)
            .ok_or(Error("missing RTP payload"))?;

        if payload.len() < 2 {
            return Err(Error("H.264 RTP payload is shorter than two bytes"));
        }

        Ok(Self {
            sequence: u16::from_be_bytes([bytes[2], bytes[3]]),
            timestamp: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            marker: bytes[1] & MARKER_FLAG != 0,
            payload,
        })
    }
}

fn validate_one_byte_extensions(bytes: &[u8]) -> Result<(), Error> {
    let mut offset = 0;

    while offset < bytes.len() {
        let header = bytes[offset];
        offset += 1;

        if header == 0 {
            continue;
        }

        if header >> 4 == RESERVED_EXTENSION_IDENTIFIER {
            break;
        }

        offset += usize::from(header & 0x0f) + 1;

        if offset > bytes.len() {
            return Err(Error(
                "RTP one-byte extension element exceeds its declared extent",
            ));
        }
    }

    Ok(())
}
