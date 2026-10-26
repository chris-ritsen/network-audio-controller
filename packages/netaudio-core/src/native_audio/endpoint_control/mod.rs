pub mod monitoring;
pub mod source;

use std::net::SocketAddrV4;

use serde::{Deserialize, Serialize};

use super::domain::{NamedDomain, NAMED_DOMAIN_BYTES};
use super::source_control::{Error, Header, MAXIMUM_CONTROL_BYTES};

use super::properties::identifier;
use crate::protocol::{
    DANTE_NAME_MAX_LENGTH, OPCODE_CHANNEL_COUNT, OPCODE_RX_CHANNELS, OPCODE_TX_CHANNEL_INFO,
    OPCODE_TX_CHANNEL_NAMES, RESPONSE_HEADER_SIZE, RESULT_CODE_FRONTEND_UNAVAILABLE,
    RESULT_CODE_MORE_PAGES, RESULT_CODE_SUCCESS,
};

pub const ARC_REVISION: u16 = 0x27ff;
pub const SAMPLE_RATE: u32 = 48_000;
pub const FRAMES_PER_PACKET: u16 = 6;
pub const MAXIMUM_TRANSMIT_FRAMES_PER_PACKET: u16 = 48;
pub const RECEIVE_LATENCY_NANOSECONDS: u32 = 20_000_000;
pub const CHANNEL_COUNT: u16 = 2;
pub const SAMPLE_BITS: u16 = 24;
pub const TRANSMIT_LATENCY_NANOSECONDS: u32 = 4_000_000;
pub const NAMED_CLOCK_DOMAIN: NamedDomain = NamedDomain::DEFAULT;

const DEVICE_NAME_OPERATION: u16 = 0x1002;
const PROPERTY_DIRECTORY_OPERATION: u16 = 0x1102;
const PROPERTY_READ_OPERATION: u16 = 0x1100;
const TRANSMIT_FLOW_OPERATION: u16 = 0x2200;
const RECEIVE_FLOW_OPERATION: u16 = 0x3200;
const ROUTE_OPERATION: u16 = 0x3010;
const CLEAR_RECEIVER_OPERATION: u16 = 0x3014;
const UNSUPPORTED_REVISION: u16 = 0x0010;
const REQUEST_TOO_SHORT: u16 = 0x0022;
const REQUEST_TOO_LONG: u16 = 0x0023;
const UNKNOWN_RECEIVER_WARNING: u16 = 0x8101;
const RESULT_STATUS_MASK: u16 = 0x0fff;
const CHANNEL_RENAME_PROHIBITED: u16 = 0x0400;
const MAXIMUM_RANGE_RESULTS: usize = 32;
const DIRECTORY_HEADER_BYTES: usize = RESPONSE_HEADER_SIZE + 2;
const TRANSMIT_CHANNEL_RECORD_BYTES: usize = 8;
const TRANSMIT_FRIENDLY_NAME_RESERVED_BYTES: usize = 8;
const RECEIVE_CHANNEL_RECORD_BYTES: usize = 20;
const ROUTE_RECORD_BYTES: usize = 6;
const ROUTE_RESERVED_RECORD_BYTES: usize = 20;

pub(crate) fn word(bytes: &[u8], offset: usize) -> Result<u16, Error> {
    let slice = bytes
        .get(
            offset
                ..offset
                    .checked_add(2)
                    .ok_or(Error("control offset overflow"))?,
        )
        .ok_or(Error("control word is incomplete"))?;
    Ok(u16::from_be_bytes([slice[0], slice[1]]))
}

pub(crate) fn double_word(bytes: &[u8], offset: usize) -> Result<u32, Error> {
    let slice = bytes
        .get(
            offset
                ..offset
                    .checked_add(4)
                    .ok_or(Error("control offset overflow"))?,
        )
        .ok_or(Error("control value is incomplete"))?;
    Ok(u32::from_be_bytes(slice.try_into().unwrap()))
}

pub(crate) fn string(bytes: &[u8], pointer: usize, minimum: usize) -> Result<String, Error> {
    if pointer < minimum {
        return Err(Error("control string overlaps its fixed record"));
    }

    let suffix = bytes
        .get(pointer..)
        .ok_or(Error("control string pointer is outside the packet"))?;
    let end = suffix
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(Error("control string is not terminated"))?;
    String::from_utf8(suffix[..end].to_vec())
        .map_err(|_| Error("control string is not supported UTF-8"))
}

pub(crate) struct Writer {
    pub bytes: Vec<u8>,
}

impl Writer {
    pub fn response(request: Header, length: usize, result: u16) -> Result<Self, Error> {
        if !(RESPONSE_HEADER_SIZE..=MAXIMUM_CONTROL_BYTES).contains(&length) {
            return Err(Error("control response is outside the packet budget"));
        }

        let mut writer = Self {
            bytes: vec![0; length],
        };
        writer.word(0, request.revision);
        writer.word(4, request.sequence);
        writer.word(6, request.operation);
        writer.word(8, result);
        Ok(writer)
    }

    pub fn word(&mut self, offset: usize, value: u16) {
        self.bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
    }

    pub fn double_word(&mut self, offset: usize, value: u32) {
        self.bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }

    pub fn reserve(&mut self, length: usize, alignment: usize) -> Result<usize, Error> {
        if !alignment.is_power_of_two() {
            return Err(Error("invalid control object alignment"));
        }

        let start = self
            .bytes
            .len()
            .checked_add(alignment - 1)
            .map(|value| value & !(alignment - 1))
            .ok_or(Error("control heap overflow"))?;
        let end = start
            .checked_add(length)
            .ok_or(Error("control heap overflow"))?;
        if end > MAXIMUM_CONTROL_BYTES {
            return Err(Error("control response exceeds its packet budget"));
        }

        self.bytes.resize(end, 0);
        Ok(start)
    }

    pub fn text(&mut self, value: &str) -> Result<u16, Error> {
        if value.as_bytes().contains(&0) {
            return Err(Error("control name contains an embedded NUL"));
        }

        let start = self.reserve(
            value
                .len()
                .checked_add(1)
                .ok_or(Error("control string extent overflow"))?,
            2,
        )?;
        self.bytes[start..start + value.len()].copy_from_slice(value.as_bytes());
        Ok(start as u16)
    }

    pub fn endpoint(&mut self, endpoint: SocketAddrV4) -> Result<u16, Error> {
        let start = self.reserve(8, 4)?;
        self.bytes[start] = 8;
        self.bytes[start + 1] = 2;
        self.word(start + 2, endpoint.port());
        self.bytes[start + 4..start + 8].copy_from_slice(&endpoint.ip().octets());
        Ok(start as u16)
    }

    pub fn finish(mut self) -> Vec<u8> {
        self.word(2, self.bytes.len() as u16);
        self.bytes
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RouteIntent {
    pub device: String,
    pub channel: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReceiverChannel {
    pub name: String,
    pub desired: Option<RouteIntent>,
    pub subscription_status: u16,
    pub available_interfaces: u8,
    pub asserted_interfaces: u8,
}

#[derive(Clone, Debug, Serialize)]
pub struct TransmitFlow {
    pub frames_per_packet: u16,
    pub generation: u32,
    pub destination: SocketAddrV4,
    pub channels: Vec<u16>,
    pub receiver_hostname: String,
    pub receiver_flow_name: String,
    pub active: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReceiveFlow {
    pub source: SocketAddrV4,
    pub slots: Vec<Vec<u16>>,
    pub active: bool,
    pub status: u16,
}

#[derive(Clone, Debug, Serialize)]
pub struct Endpoint {
    pub name: String,
    pub transmit_names: [String; 2],
    pub receive_channels: [ReceiverChannel; 2],
    pub transmit_flow: Option<TransmitFlow>,
    pub receive_flow: Option<ReceiveFlow>,
    pub route_generation: u64,
}

pub struct Answer {
    pub bytes: Vec<u8>,
    pub routes_changed: bool,
}

impl Endpoint {
    pub fn new(name: String) -> Result<Self, Error> {
        if name.is_empty()
            || name.len() > DANTE_NAME_MAX_LENGTH
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || !name.as_bytes()[0].is_ascii_alphanumeric()
            || !name.as_bytes()[name.len() - 1].is_ascii_alphanumeric()
        {
            return Err(Error(
                "endpoint name must contain 1 through 31 ASCII letters, digits or internal hyphens",
            ));
        }

        Ok(Self {
            name,
            transmit_names: ["tx-1".into(), "tx-2".into()],
            receive_channels: [1, 2].map(|identifier| ReceiverChannel {
                name: format!("rx-{identifier}"),
                desired: None,
                subscription_status: 0,
                available_interfaces: 0,
                asserted_interfaces: 0,
            }),
            transmit_flow: None,
            receive_flow: None,
            route_generation: 1,
        })
    }

    pub fn answer(&mut self, request: &[u8]) -> Result<Option<Answer>, Error> {
        let declared_length = usize::from(word(request, 2)?);
        let request = request
            .get(..declared_length)
            .ok_or(Error("control request exceeds its received extent"))?;
        let header = Header::decode(request)?;
        if header.result != 0 {
            return Ok(None);
        }

        let response = if header.revision != ARC_REVISION {
            if header.operation == OPCODE_TX_CHANNEL_NAMES {
                return Err(Error("unsupported friendly-name request revision"));
            }

            Writer::response(header, RESPONSE_HEADER_SIZE, UNSUPPORTED_REVISION)?.finish()
        } else {
            match header.operation {
                OPCODE_CHANNEL_COUNT if request.len() == RESPONSE_HEADER_SIZE => {
                    self.capabilities(header)?
                }
                DEVICE_NAME_OPERATION if request.len() == RESPONSE_HEADER_SIZE => {
                    let mut reply =
                        Writer::response(header, 11 + self.name.len(), RESULT_CODE_SUCCESS)?;
                    reply.bytes[10..10 + self.name.len()].copy_from_slice(self.name.as_bytes());
                    reply.finish()
                }
                PROPERTY_DIRECTORY_OPERATION if request.len() == RESPONSE_HEADER_SIZE => {
                    Writer::response(header, DIRECTORY_HEADER_BYTES, RESULT_CODE_SUCCESS)?.finish()
                }
                PROPERTY_READ_OPERATION => self.properties(header, request)?,
                OPCODE_TX_CHANNEL_INFO | OPCODE_RX_CHANNELS => self.channels(header, request)?,
                OPCODE_TX_CHANNEL_NAMES => self.transmit_friendly_names(header, request)?,
                TRANSMIT_FLOW_OPERATION | RECEIVE_FLOW_OPERATION => self.flows(header, request)?,
                ROUTE_OPERATION => return self.route(header, request).map(Some),
                CLEAR_RECEIVER_OPERATION => return self.clear_receiver(header, request).map(Some),
                OPCODE_CHANNEL_COUNT | DEVICE_NAME_OPERATION | PROPERTY_DIRECTORY_OPERATION => {
                    Writer::response(header, RESPONSE_HEADER_SIZE, REQUEST_TOO_LONG)?.finish()
                }
                _ => Writer::response(
                    header,
                    RESPONSE_HEADER_SIZE,
                    RESULT_CODE_FRONTEND_UNAVAILABLE,
                )?
                .finish(),
            }
        };

        Ok(Some(Answer {
            bytes: response,
            routes_changed: false,
        }))
    }

    fn capabilities(&self, header: Header) -> Result<Vec<u8>, Error> {
        let mut reply = Writer::response(header, 56, RESULT_CODE_SUCCESS)?;
        for (offset, value) in [
            (12, 2),
            (14, 2),
            (18, 2),
            (20, 2),
            (22, 1),
            (24, 1),
            (30, 1),
        ] {
            reply.word(offset, value);
        }

        Ok(reply.finish())
    }

    fn properties(&self, header: Header, request: &[u8]) -> Result<Vec<u8>, Error> {
        if request.len() < DIRECTORY_HEADER_BYTES {
            return Ok(Writer::response(header, RESPONSE_HEADER_SIZE, REQUEST_TOO_SHORT)?.finish());
        }

        let count = usize::from(request[11]);
        let expected = DIRECTORY_HEADER_BYTES + count * 2;
        if request.len() != expected {
            return Ok(Writer::response(
                header,
                10,
                if request.len() < expected {
                    REQUEST_TOO_SHORT
                } else {
                    REQUEST_TOO_LONG
                },
            )?
            .finish());
        }

        let mut reply = Writer::response(
            header,
            DIRECTORY_HEADER_BYTES + count * 4,
            RESULT_CODE_SUCCESS,
        )?;
        reply.bytes[10] = count as u8;
        reply.bytes[11] = count as u8;
        for index in 0..count {
            let identifier = word(request, 12 + 2 * index)?;
            let row = 12 + 4 * index;
            match identifier {
                identifier::SOURCE_DEFAULT_FRAMES_PER_PACKET
                | identifier::UNICAST_FRAMES_PER_PACKET
                | identifier::SOURCE_MINIMUM_FRAMES_PER_PACKET
                | identifier::RECEIVE_FRAMES_PER_PACKET => {
                    reply.word(row, identifier);
                    reply.word(row + 2, FRAMES_PER_PACKET);
                }
                identifier::SOURCE_MAXIMUM_FRAMES_PER_PACKET => {
                    reply.word(row, identifier);
                    reply.word(row + 2, MAXIMUM_TRANSMIT_FRAMES_PER_PACKET);
                }
                identifier::UNICAST_LATENCY | identifier::RECEIVE_LATENCY => {
                    let value = if identifier == identifier::UNICAST_LATENCY {
                        TRANSMIT_LATENCY_NANOSECONDS
                    } else {
                        RECEIVE_LATENCY_NANOSECONDS
                    };

                    let offset = reply.reserve(4, 4)?;
                    reply.double_word(offset, value);
                    reply.word(row, identifier);
                    reply.word(row + 2, offset as u16);
                }
                identifier::CLOCK_DOMAIN => {
                    let offset = reply.reserve(NAMED_DOMAIN_BYTES, 2)?;
                    reply.bytes[offset..offset + NAMED_DOMAIN_BYTES]
                        .copy_from_slice(NAMED_CLOCK_DOMAIN.bytes());
                    reply.word(row, identifier);
                    reply.word(row + 2, offset as u16);
                }
                _ => reply.word(row + 2, identifier),
            }
        }

        Ok(reply.finish())
    }

    fn transmit_friendly_names(&self, header: Header, request: &[u8]) -> Result<Vec<u8>, Error> {
        if request.get(RESPONSE_HEADER_SIZE) != Some(&0) {
            return Err(Error("unsupported friendly-name request prefix"));
        }

        ranges(request, CHANNEL_COUNT)
            .map_err(|_| Error("friendly-name request has invalid range extent"))?;

        let capacity = self.transmit_names.len().min(MAXIMUM_RANGE_RESULTS);
        let length = DIRECTORY_HEADER_BYTES + capacity * TRANSMIT_FRIENDLY_NAME_RESERVED_BYTES;
        let mut reply = Writer::response(header, length, RESULT_CODE_SUCCESS)?;
        reply.bytes[RESPONSE_HEADER_SIZE] = capacity as u8;
        Ok(reply.finish())
    }

    fn channels(&self, header: Header, request: &[u8]) -> Result<Vec<u8>, Error> {
        let selected = match ranges(request, CHANNEL_COUNT) {
            Ok(value) => value,
            Err(result) => {
                return Ok(Writer::response(header, RESPONSE_HEADER_SIZE, result)?.finish());
            }
        };

        let receiving = header.operation == OPCODE_RX_CHANNELS;
        let stride = if receiving {
            RECEIVE_CHANNEL_RECORD_BYTES
        } else {
            TRANSMIT_CHANNEL_RECORD_BYTES
        };

        let mut reply = Writer::response(
            header,
            DIRECTORY_HEADER_BYTES + usize::from(CHANNEL_COUNT) * stride,
            if selected.len() > usize::from(CHANNEL_COUNT) {
                RESULT_CODE_MORE_PAGES
            } else {
                RESULT_CODE_SUCCESS
            },
        )?;
        reply.bytes[10] = 2;
        let descriptor = format_descriptor(&mut reply)?;
        for (index, identifier) in selected.iter().take(usize::from(CHANNEL_COUNT)).enumerate() {
            let row = DIRECTORY_HEADER_BYTES + index * stride;
            let channel_index = usize::from(*identifier - 1);
            reply.word(row, *identifier);
            reply.word(row + 4, descriptor);
            if receiving {
                let channel = &self.receive_channels[channel_index];
                reply.word(row + 2, CHANNEL_RENAME_PROHIBITED);
                if let Some(desired) = &channel.desired {
                    let label = reply.text(&desired.channel)?;
                    let device = reply.text(&desired.device)?;
                    reply.word(row + 6, label);
                    reply.word(row + 8, device);
                }

                let name = reply.text(&channel.name)?;
                reply.word(row + 10, name);
                reply.bytes[row + 12] = channel.available_interfaces;
                reply.bytes[row + 13] = channel.asserted_interfaces;
                reply.word(row + 14, channel.subscription_status);
            } else {
                let name = reply.text(&self.transmit_names[channel_index])?;
                reply.word(row + 6, name);
            }
            reply.bytes[11] += 1;
        }

        Ok(reply.finish())
    }

    fn flows(&self, header: Header, request: &[u8]) -> Result<Vec<u8>, Error> {
        let selected = match ranges(request, 1) {
            Ok(value) => value,
            Err(result) => {
                return Ok(Writer::response(header, RESPONSE_HEADER_SIZE, result)?.finish());
            }
        };

        let mut reply = Writer::response(header, 14, RESULT_CODE_SUCCESS)?;
        reply.bytes[10] = 1;
        if selected.is_empty() {
            return Ok(reply.finish());
        }

        if header.operation == TRANSMIT_FLOW_OPERATION {
            if let Some(flow) = &self.transmit_flow {
                let count = flow.channels.len();
                let record = reply.reserve(20 + 2 * count, 4)?;
                reply.word(12, record as u16);
                reply.bytes[11] = 1;
                reply.word(record, 1);
                reply.word(record + 2, 1);
                reply.double_word(record + 4, SAMPLE_RATE);
                reply.word(record + 10, SAMPLE_BITS);
                reply.word(record + 12, 1);
                reply.word(record + 14, count as u16);
                let endpoint = reply.endpoint(flow.destination)?;
                reply.word(record + 16, endpoint);
                for (index, channel) in flow.channels.iter().enumerate() {
                    reply.word(record + 18 + 2 * index, *channel);
                }

                let status = reply.reserve(20, 4)?;
                reply.word(record + 18 + count * 2, status as u16);
                reply.bytes[status] = 10;
                reply.word(status + 2, u16::from(flow.active));
                let device = reply.text(&flow.receiver_hostname)?;
                let name = reply.text(&flow.receiver_flow_name)?;
                reply.word(status + 4, device);
                reply.word(status + 6, name);
                reply.word(status + 8, flow.frames_per_packet);
                reply.double_word(status + 12, TRANSMIT_LATENCY_NANOSECONDS);
            }
        } else if let Some(flow) = &self.receive_flow {
            let count = flow.slots.len();
            let record = reply.reserve(22 + 2 * count, 4)?;
            reply.word(12, record as u16);
            reply.bytes[11] = 1;
            reply.word(record, 1);
            reply.word(record + 2, 1);
            reply.double_word(record + 4, SAMPLE_RATE);
            reply.word(record + 10, SAMPLE_BITS);
            reply.word(record + 12, 1);
            reply.word(record + 14, count as u16);
            reply.word(record + 16, 1);
            let endpoint = reply.endpoint(flow.source)?;
            reply.word(record + 18, endpoint);
            for (index, channels) in flow.slots.iter().enumerate() {
                let bitmap = reply.reserve(2, 2)?;
                let mut mask = 0;
                for channel in channels {
                    if (1..=CHANNEL_COUNT).contains(channel) {
                        mask |= 1 << (*channel - 1);
                    }
                }
                reply.word(bitmap, mask);
                reply.word(record + 20 + 2 * index, bitmap as u16);
            }

            let status = reply.reserve(16, 4)?;
            reply.word(record + 20 + count * 2, status as u16);
            reply.word(status, flow.status);
            reply.word(status + 2, u16::from(flow.active));
            reply.bytes[status + 4] = 8;
            reply.double_word(status + 8, RECEIVE_LATENCY_NANOSECONDS);
        }

        Ok(reply.finish())
    }

    fn clear_receiver(&mut self, header: Header, request: &[u8]) -> Result<Answer, Error> {
        const SINGLETON_CLEAR_BYTES: usize = DIRECTORY_HEADER_BYTES + 4;

        if request.len() != SINGLETON_CLEAR_BYTES {
            return Ok(Answer {
                bytes: Writer::response(
                    header,
                    RESPONSE_HEADER_SIZE,
                    if request.len() < SINGLETON_CLEAR_BYTES {
                        REQUEST_TOO_SHORT
                    } else {
                        REQUEST_TOO_LONG
                    },
                )?
                .finish(),
                routes_changed: false,
            });
        }

        if request[10] != 0 || request[11] != 1 || word(request, 12)? != 0 {
            return Err(Error(
                "receiver clear requires one explicit singleton selector",
            ));
        }

        let receiver = word(request, 14)?;

        if receiver == 0 {
            return Err(Error(
                "receiver clear requires a nonzero receiver identifier",
            ));
        }

        self.apply_routes(header, vec![(receiver, None)])
    }

    fn route(&mut self, header: Header, request: &[u8]) -> Result<Answer, Error> {
        if request.len() < DIRECTORY_HEADER_BYTES {
            return Ok(Answer {
                bytes: Writer::response(header, RESPONSE_HEADER_SIZE, REQUEST_TOO_SHORT)?.finish(),
                routes_changed: false,
            });
        }

        let capacity = usize::from(request[10]);
        let count = usize::from(request[11]);
        let heap = DIRECTORY_HEADER_BYTES + ROUTE_RESERVED_RECORD_BYTES * capacity;
        if count > capacity || heap > request.len() {
            return Err(Error("routing directory or reserved extent is incomplete"));
        }

        let mut changes = Vec::with_capacity(count);
        for index in 0..count {
            let row = DIRECTORY_HEADER_BYTES + ROUTE_RECORD_BYTES * index;
            let receiver = word(request, row)?;
            let channel = usize::from(word(request, row + 2)?);
            let device = usize::from(word(request, row + 4)?);
            let desired = match (channel, device) {
                (0, 0) => None,
                (0, _) | (_, 0) => {
                    return Err(Error("route must supply both source names or neither"));
                }
                _ => {
                    let channel = string(request, channel, heap)?;
                    let device = string(request, device, heap)?;
                    if channel.is_empty() || device.is_empty() {
                        return Err(Error("route names cannot be empty"));
                    }
                    Some(RouteIntent { device, channel })
                }
            };
            changes.push((receiver, desired));
        }

        self.apply_routes(header, changes)
    }

    fn apply_routes(
        &mut self,
        header: Header,
        changes: Vec<(u16, Option<RouteIntent>)>,
    ) -> Result<Answer, Error> {
        let next_generation = self
            .route_generation
            .checked_add(1)
            .ok_or(Error("route generation is exhausted"))?;
        let mut result = RESULT_CODE_SUCCESS;
        let mut changed = false;
        for (receiver, desired) in changes {
            if receiver == 0 {
                continue;
            }

            let Some(channel) = self.receive_channels.get_mut(usize::from(receiver - 1)) else {
                if result & RESULT_STATUS_MASK == 1 {
                    result = UNKNOWN_RECEIVER_WARNING;
                }
                continue;
            };

            if channel.desired != desired {
                channel.desired = desired;
                channel.subscription_status = if channel.desired.is_some() { 1 } else { 0 };
                channel.available_interfaces = 0;
                channel.asserted_interfaces = 0;
                changed = true;
            }
        }

        if changed {
            self.route_generation = next_generation;
        }

        Ok(Answer {
            bytes: Writer::response(header, RESPONSE_HEADER_SIZE, result)?.finish(),
            routes_changed: changed,
        })
    }
}

pub(crate) fn ranges(request: &[u8], maximum: u16) -> Result<Vec<u16>, u16> {
    if request.len() < DIRECTORY_HEADER_BYTES {
        return Err(REQUEST_TOO_SHORT);
    }

    let expected = 12 + usize::from(request[11]) * 4;
    if request.len() < expected {
        return Err(REQUEST_TOO_SHORT);
    }

    if request.len() > expected {
        return Err(REQUEST_TOO_LONG);
    }

    let mut identifiers = Vec::new();
    for index in 0..usize::from(request[11]) {
        let start = word(request, 12 + index * 4).map_err(|_| REQUEST_TOO_SHORT)?;
        let end = word(request, 14 + index * 4).map_err(|_| REQUEST_TOO_SHORT)?;
        let (start, end) = if start == 0 {
            if end == 0 || end > maximum {
                continue;
            }
            (end, end)
        } else {
            (
                start,
                if end == 0 || end > maximum {
                    maximum
                } else {
                    end
                },
            )
        };

        if start <= end {
            identifiers.extend(start..=end);
        }

        if identifiers.len() > MAXIMUM_RANGE_RESULTS {
            break;
        }
    }

    Ok(identifiers)
}

fn format_descriptor(reply: &mut Writer) -> Result<u16, Error> {
    let descriptor = reply.reserve(16, 4)?;
    reply.double_word(descriptor, SAMPLE_RATE);
    reply.bytes[descriptor + 4] = 1;
    reply.bytes[descriptor + 5] = 1;
    reply.word(descriptor + 6, SAMPLE_BITS);
    reply.word(descriptor + 10, SAMPLE_BITS);
    Ok(descriptor as u16)
}
