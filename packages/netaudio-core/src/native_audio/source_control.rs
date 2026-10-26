use std::fmt;
use std::net::SocketAddrV4;

use serde::{Deserialize, Serialize};

use super::Format;

pub const REFERENCE_REVISION: u16 = 0x1102;
pub const MAXIMUM_CONTROL_BYTES: usize = 1400;
pub const ALLOCATION_OPERATION: u16 = 0x0100;
pub const STOP_OPERATION: u16 = 0x0101;

#[derive(Debug)]
pub struct Error(pub &'static str);

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyProfile {
    ReferenceHandle,
    DetailedInterfaceOutcomes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Header {
    pub revision: u16,
    pub declared_length: u16,
    pub sequence: u16,
    pub operation: u16,
    pub result: u16,
}

impl Header {
    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 10 || data.len() > MAXIMUM_CONTROL_BYTES {
            return Err(Error(
                "source-control datagram is outside the selected extent",
            ));
        }

        let header = Self {
            revision: word(data, 0)?,
            declared_length: word(data, 2)?,
            sequence: word(data, 4)?,
            operation: word(data, 6)?,
            result: word(data, 8)?,
        };

        if usize::from(header.declared_length) != data.len() {
            return Err(Error("source-control declared and received extents differ"));
        }

        Ok(header)
    }

    pub fn owns_reply(self, reply: Self) -> bool {
        reply.revision == self.revision
            && reply.sequence == self.sequence
            && reply.operation == self.operation
            && reply.result != 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Handle {
    bytes: [u8; 6],
}

impl Handle {
    pub fn bytes(self) -> [u8; 6] {
        self.bytes
    }
}

pub struct Allocation<'a> {
    pub receiver_hostname: &'a str,
    pub receiver_flow_name: &'a str,
    pub destination: SocketAddrV4,
    pub format: Format,
    pub source_channel_identifiers: &'a [u16],
    pub requested_frames_per_packet: u16,
}

fn word(data: &[u8], offset: usize) -> Result<u16, Error> {
    let bytes = data
        .get(offset..offset.checked_add(2).ok_or(Error("word extent overflow"))?)
        .ok_or(Error("source-control word is truncated"))?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn put_word(data: &mut [u8], offset: usize, value: u16) {
    data[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn header(length: usize, sequence: u16, operation: u16) -> Result<Vec<u8>, Error> {
    if !(10..=MAXIMUM_CONTROL_BYTES).contains(&length) {
        return Err(Error(
            "source-control request exceeds the selected packet budget",
        ));
    }

    let mut data = vec![0; length];
    put_word(&mut data, 0, REFERENCE_REVISION);
    put_word(&mut data, 2, length as u16);
    put_word(&mut data, 4, sequence);
    put_word(&mut data, 6, operation);
    Ok(data)
}

pub fn allocate(allocation: &Allocation<'_>, sequence: u16) -> Result<Vec<u8>, Error> {
    let slots = allocation.source_channel_identifiers;
    if slots.len() != usize::from(allocation.format.slot_count())
        || !slots.iter().any(|identifier| *identifier != 0)
        || allocation.requested_frames_per_packet == 0
    {
        return Err(Error(
            "allocation needs matching ordered slots, a source channel and positive packetization",
        ));
    }

    if allocation.destination.port() == 0
        || allocation.destination.ip().is_unspecified()
        || allocation.destination.ip().is_multicast()
        || allocation.destination.ip().is_broadcast()
    {
        return Err(Error(
            "allocation destination must be an owned unicast receiver endpoint",
        ));
    }

    for name in [allocation.receiver_hostname, allocation.receiver_flow_name] {
        if name.is_empty() || name.as_bytes().contains(&0) {
            return Err(Error(
                "receiver names must be nonempty and contain no NUL bytes",
            ));
        }
    }

    let extension = slots
        .len()
        .checked_mul(2)
        .and_then(|length| length.checked_add(28))
        .ok_or(Error("allocation slot directory overflow"))?;
    let hostname = extension
        .checked_add(20)
        .ok_or(Error("hostname offset overflow"))?;
    let flow_name = hostname
        .checked_add(allocation.receiver_hostname.len())
        .and_then(|length| length.checked_add(1))
        .ok_or(Error("flow-name offset overflow"))?;
    let endpoint = flow_name
        .checked_add(allocation.receiver_flow_name.len())
        .and_then(|length| length.checked_add(1))
        .and_then(|length| length.checked_add(7))
        .map(|length| length & !7)
        .ok_or(Error("endpoint offset overflow"))?;
    let length = endpoint
        .checked_add(8)
        .ok_or(Error("allocation length overflow"))?;
    let mut data = header(length, sequence, ALLOCATION_OPERATION)?;
    put_word(&mut data, 10, hostname as u16);
    data[12..16].copy_from_slice(&allocation.format.sample_rate().to_be_bytes());
    data[16..20]
        .copy_from_slice(&((allocation.format.sample_width().bytes() * 8) as u32).to_be_bytes());
    put_word(&mut data, 20, 1);
    put_word(&mut data, 22, slots.len() as u16);
    put_word(&mut data, 24, endpoint as u16);
    for (index, identifier) in slots.iter().enumerate() {
        put_word(&mut data, 26 + 2 * index, *identifier);
    }
    put_word(&mut data, extension - 2, extension as u16);
    data[extension] = 10;
    put_word(&mut data, extension + 2, 2);
    put_word(
        &mut data,
        extension + 4,
        allocation.requested_frames_per_packet,
    );
    put_word(&mut data, extension + 6, flow_name as u16);
    data[hostname..hostname + allocation.receiver_hostname.len()]
        .copy_from_slice(allocation.receiver_hostname.as_bytes());
    data[flow_name..flow_name + allocation.receiver_flow_name.len()]
        .copy_from_slice(allocation.receiver_flow_name.as_bytes());
    data[endpoint] = 8;
    data[endpoint + 1] = 2;
    put_word(&mut data, endpoint + 2, allocation.destination.port());
    data[endpoint + 4..endpoint + 8].copy_from_slice(&allocation.destination.ip().octets());
    Ok(data)
}

pub fn stop(handle: Handle, sequence: u16) -> Result<Vec<u8>, Error> {
    let mut data = header(16, sequence, STOP_OPERATION)?;
    data[10..16].copy_from_slice(&handle.bytes);
    Ok(data)
}

#[derive(Debug, Serialize)]
pub struct AllocationOutcome {
    pub header: Header,
    pub handle: Option<Handle>,
    pub interface_outcomes: Option<Vec<u16>>,
    pub channel_details: Option<Vec<u16>>,
    pub extension_hexadecimal: Option<String>,
    pub raw_reply_hexadecimal: String,
}

pub fn allocation_reply(data: &[u8], profile: ReplyProfile) -> Result<AllocationOutcome, Error> {
    let response = Header::decode(data)?;
    if response.revision != REFERENCE_REVISION
        || response.operation != ALLOCATION_OPERATION
        || response.result == 0
    {
        return Err(Error("reply is outside the selected allocation profile"));
    }

    let eligible = response.result == 1
        || matches!(profile, ReplyProfile::DetailedInterfaceOutcomes)
            && response.result & 0x8000 != 0;
    let mut outcome = AllocationOutcome {
        header: response,
        handle: None,
        interface_outcomes: None,
        channel_details: None,
        extension_hexadecimal: None,
        raw_reply_hexadecimal: hexadecimal(data),
    };

    if !eligible {
        return Ok(outcome);
    }

    let bytes: [u8; 6] = data
        .get(10..16)
        .ok_or(Error("allocation reply has no complete handle"))?
        .try_into()
        .map_err(|_| Error("allocation handle extent is invalid"))?;
    if matches!(profile, ReplyProfile::DetailedInterfaceOutcomes) {
        if word(&bytes, 0)? == 0 || bytes[2..] == [0; 4] {
            return Err(Error(
                "detailed allocation reply has no current nonzero ownership handle",
            ));
        }

        let count = usize::from(word(data, 16)?);
        let base = 22 + 2 * count;
        data.get(..base)
            .ok_or(Error("allocation interface directory is truncated"))?;
        outcome.interface_outcomes = Some(
            (0..count)
                .map(|index| word(data, 18 + 2 * index))
                .collect::<Result<_, _>>()?,
        );
        let extension = usize::from(word(data, 18 + 2 * count)?);
        if extension != 0 {
            if extension < base || extension % 2 != 0 {
                return Err(Error(
                    "allocation extension overlaps its base or is misaligned",
                ));
            }

            let prefix = data
                .get(extension..extension + 12)
                .ok_or(Error("allocation extension is truncated"))?;
            let end = extension + 2 * usize::from(prefix[0]);
            if end < extension + 12 {
                return Err(Error(
                    "allocation extension word extent omits required fields",
                ));
            }

            let extension_bytes = data
                .get(extension..end)
                .ok_or(Error("allocation extension word extent is truncated"))?;
            outcome.extension_hexadecimal = Some(hexadecimal(extension_bytes));
            let count = usize::from(word(data, extension + 4)?);
            let pointer = usize::from(word(data, extension + 6)?);
            if count == 0 {
                outcome.channel_details = Some(Vec::new());
            } else if pointer != 0 {
                let array_end = pointer + 2 * count;
                if pointer < base || pointer % 2 != 0 || pointer < end && array_end > extension {
                    return Err(Error(
                        "allocation detail array overlaps another required region",
                    ));
                }
                data.get(pointer..array_end)
                    .ok_or(Error("allocation detail array is truncated"))?;
                outcome.channel_details = Some(
                    (0..count)
                        .map(|index| word(data, pointer + 2 * index))
                        .collect::<Result<_, _>>()?,
                );
            }
        }
    }
    outcome.handle = Some(Handle { bytes });
    Ok(outcome)
}

pub fn stop_reply(data: &[u8]) -> Result<Header, Error> {
    let response = Header::decode(data)?;
    if response.revision != REFERENCE_REVISION
        || response.operation != STOP_OPERATION
        || response.result == 0
        || data.len() != 10
    {
        return Err(Error(
            "reply is outside the selected header-only STOP profile",
        ));
    }

    Ok(response)
}

fn hexadecimal(data: &[u8]) -> String {
    crate::bytes::hexadecimal(data)
}
