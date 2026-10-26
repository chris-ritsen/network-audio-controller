use serde::Serialize;

use crate::bytes::{read_u16, read_u32};
use crate::commands::{
    AudioDirection, OPCODE_QUERY_RECEIVER_PORT_RANGES, OPCODE_QUERY_RECEIVE_AVAILABILITY,
    OPCODE_QUERY_TRANSMIT_AVAILABILITY,
};
use crate::protocol::{
    response_envelope, ResponseEnvelope, RESULT_CODE_FRONTEND_UNAVAILABLE, RESULT_CODE_SUCCESS,
};

const DECLARED_LENGTH_OFFSET: usize = 2;
const FIRST_RANGE_OFFSET: usize = 10;
const NATIVE_RANGE_OFFSET: usize = 14;
const OPTIONAL_RANGE_OFFSET: usize = 18;
const BASE_RANGE_BYTES: usize = 18;
const EXTENDED_RANGE_BYTES: usize = 22;
const COVERAGE_OFFSET: usize = 16;
const FLAGS_OFFSET: usize = 20;
const AVAILABILITY_PREFIX_BYTES: usize = 24;
const AVAILABILITY_COVERAGE_WORDS: u8 = 4;
const RECEIVE_RANGE_OFFSET: usize = 24;
const RECEIVE_RANGE_PREFIX_BYTES: usize = 28;
const RECEIVE_RANGE_COVERAGE_WORDS: u8 = 6;
const UNICAST_AVAILABLE_MASK: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortIntervalState {
    Usable,
    Unavailable,
    Invalid,
    Empty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct PortInterval {
    pub lower: u16,
    pub upper: u16,
    pub state: PortIntervalState,
}

impl PortInterval {
    fn read(packet: &[u8], offset: usize) -> Option<Self> {
        let lower = read_u16(packet, offset)?;
        let upper = read_u16(packet, offset.checked_add(size_of::<u16>())?)?;
        let state = match (lower, upper) {
            (0, 0) => PortIntervalState::Unavailable,
            (0, _) | (_, 0) => PortIntervalState::Invalid,
            _ if lower > upper => PortIntervalState::Empty,
            _ => PortIntervalState::Usable,
        };

        Some(Self {
            lower,
            upper,
            state,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum OptionalPortInterval {
    Absent,
    Incomplete { bytes: Vec<u8> },
    Present { interval: PortInterval },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReceiverPortRanges {
    pub first: PortInterval,
    pub native_manual: PortInterval,
    pub aes67_unicast: OptionalPortInterval,
    pub unknown_extension: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum PropertyReadOutcome<T> {
    Reported { value: T },
    Unsupported,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PropertyReadReply<T> {
    pub protocol_id: u16,
    pub transaction_id: u16,
    pub operation: u16,
    pub result_code: u16,
    pub outcome: PropertyReadOutcome<T>,
    pub raw_body: Vec<u8>,
    pub trailing_datagram_bytes: Vec<u8>,
}

fn declared_packet(datagram: &[u8]) -> Option<(&[u8], ResponseEnvelope<'_>)> {
    let length = usize::from(read_u16(datagram, DECLARED_LENGTH_OFFSET)?);
    let packet = datagram.get(..length)?;
    let envelope = response_envelope(packet)?;

    Some((packet, envelope))
}

fn reply<T>(
    datagram: &[u8],
    packet: &[u8],
    envelope: ResponseEnvelope<'_>,
    decode: impl FnOnce() -> Option<T>,
) -> Option<PropertyReadReply<T>> {
    let outcome = match envelope.result_code {
        RESULT_CODE_SUCCESS => PropertyReadOutcome::Reported { value: decode()? },
        RESULT_CODE_FRONTEND_UNAVAILABLE => PropertyReadOutcome::Unsupported,
        _ => PropertyReadOutcome::Failed,
    };

    Some(PropertyReadReply {
        protocol_id: envelope.protocol_id,
        transaction_id: envelope.transaction_id,
        operation: envelope.opcode,
        result_code: envelope.result_code,
        outcome,
        raw_body: envelope.body.to_vec(),
        trailing_datagram_bytes: datagram[packet.len()..].to_vec(),
    })
}

pub fn parse_receiver_port_ranges(
    datagram: &[u8],
) -> Option<PropertyReadReply<ReceiverPortRanges>> {
    let (packet, envelope) = declared_packet(datagram)?;

    if envelope.opcode != OPCODE_QUERY_RECEIVER_PORT_RANGES {
        return None;
    }

    reply(datagram, packet, envelope, || receiver_port_ranges(packet))
}

fn receiver_port_ranges(packet: &[u8]) -> Option<ReceiverPortRanges> {
    packet.get(..BASE_RANGE_BYTES)?;

    let aes67_unicast = match packet.len() {
        BASE_RANGE_BYTES => OptionalPortInterval::Absent,
        length if length < EXTENDED_RANGE_BYTES => OptionalPortInterval::Incomplete {
            bytes: packet[OPTIONAL_RANGE_OFFSET..].to_vec(),
        },
        _ => OptionalPortInterval::Present {
            interval: PortInterval::read(packet, OPTIONAL_RANGE_OFFSET)?,
        },
    };

    Some(ReceiverPortRanges {
        first: PortInterval::read(packet, FIRST_RANGE_OFFSET)?,
        native_manual: PortInterval::read(packet, NATIVE_RANGE_OFFSET)?,
        aes67_unicast,
        unknown_extension: packet
            .get(EXTENDED_RANGE_BYTES..)
            .unwrap_or_default()
            .to_vec(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DirectionalAudioAvailability {
    pub direction: AudioDirection,
    pub coverage_words: u8,
    pub raw_flags: u32,
    pub aes67_unicast_available: bool,
    pub receive_interval: OptionalPortInterval,
}

pub fn parse_directional_audio_availability(
    datagram: &[u8],
) -> Option<PropertyReadReply<DirectionalAudioAvailability>> {
    let (packet, envelope) = declared_packet(datagram)?;
    let direction = match envelope.opcode {
        OPCODE_QUERY_TRANSMIT_AVAILABILITY => AudioDirection::Transmit,
        OPCODE_QUERY_RECEIVE_AVAILABILITY => AudioDirection::Receive,
        _ => return None,
    };

    reply(datagram, packet, envelope, || {
        directional_availability(packet, direction)
    })
}

fn directional_availability(
    packet: &[u8],
    direction: AudioDirection,
) -> Option<DirectionalAudioAvailability> {
    let coverage_words = *packet.get(COVERAGE_OFFSET)?;
    let coverage_end = COVERAGE_OFFSET.checked_add(usize::from(coverage_words).checked_mul(2)?)?;

    if packet.len() < AVAILABILITY_PREFIX_BYTES
        || coverage_words < AVAILABILITY_COVERAGE_WORDS
        || coverage_end > packet.len()
    {
        return None;
    }

    let raw_flags = read_u32(packet, FLAGS_OFFSET)?;
    let receive_interval =
        if direction != AudioDirection::Receive || coverage_words == AVAILABILITY_COVERAGE_WORDS {
            OptionalPortInterval::Absent
        } else if coverage_words < RECEIVE_RANGE_COVERAGE_WORDS {
            OptionalPortInterval::Incomplete {
                bytes: packet[RECEIVE_RANGE_OFFSET..coverage_end].to_vec(),
            }
        } else {
            packet.get(..RECEIVE_RANGE_PREFIX_BYTES)?;
            OptionalPortInterval::Present {
                interval: PortInterval::read(packet, RECEIVE_RANGE_OFFSET)?,
            }
        };

    Some(DirectionalAudioAvailability {
        direction,
        coverage_words,
        raw_flags,
        aes67_unicast_available: raw_flags & UNICAST_AVAILABLE_MASK != 0,
        receive_interval,
    })
}
