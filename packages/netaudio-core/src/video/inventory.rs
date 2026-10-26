use std::net::SocketAddrV4;
use std::num::NonZeroU16;

use crate::commands::{
    OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809, OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809,
    OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809, OPCODE_QUERY_TX_FLOWS_2809,
};
use crate::protocol::{OPCODE_CHANNEL_COUNT, RESULT_CODE_MORE_PAGES, RESULT_CODE_SUCCESS};

use super::{discovery::H264_SUBTYPE, Error};

const MINIMUM_REVISION: u16 = 0x2802;
const MAXIMUM_REVISION: u16 = 0x280f;
const DISPLAY_GROUP_REVISION: u16 = 0x2807;
const COMPATIBILITY_FLAGS_REVISION: u16 = 0x280e;
const MESSAGE_FLAGS_REVISION: u16 = 0x280f;
const MEDIA_SELECTOR: u16 = 4;
const MEDIA_AWARE_CAPABILITY: u16 = 0x1000;
const CAPABILITY_BASE_BYTES: usize = 56;
const CAPABILITY_EXTENSION_BYTES: usize = 40;
const CAPABILITY_EXTENSION_WORDS: u8 = 20;
const PAGE_BASE_BYTES: usize = 18;
const COMMON_ROW_BYTES: usize = 44;
const COMMON_ROW_WORDS: u8 = 22;
const TRANSMITTER_CHANNEL_BYTES: usize = 44;
const RECEIVER_CHANNEL_BYTES: usize = 56;
const TRANSMITTER_FLOW_BYTES: usize = 80;
const RECEIVER_FLOW_BYTES: usize = 76;
const FORMAT_BYTES: usize = 16;
const FORMAT_PREFIX: &[u8; 8] = &[2, 8, 0, 0, 6, 6, 0, 0];
const ENDPOINT_BYTES: usize = 8;
const ENDPOINT_LENGTH: u8 = 8;
const IPV4_FAMILY: u8 = 2;
const OBJECT_ALIGNMENT: usize = 4;
const WORD_ALIGNMENT: usize = 2;
const MAXIMUM_ENDPOINTS: usize = 2;
const SINGLE_CHANNEL_CAPACITY: u8 = 1;

#[derive(Clone, Copy)]
pub struct Context {
    pub revision: u16,
    pub transaction: u16,
}

struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn new(context: Context, operation: u16, result: u16, base: usize) -> Result<Self, Error> {
        if !(MINIMUM_REVISION..=MAXIMUM_REVISION).contains(&context.revision) {
            return Err(Error(
                "video inventory revision is outside the implemented profile",
            ));
        }

        let mut writer = Self {
            bytes: vec![0; base],
        };
        writer.word(0, context.revision);
        writer.word(4, context.transaction);
        writer.word(6, operation);
        writer.word(8, result);
        Ok(writer)
    }

    fn word(&mut self, offset: usize, value: u16) {
        self.bytes[offset..offset + size_of::<u16>()].copy_from_slice(&value.to_be_bytes());
    }

    fn double_word(&mut self, offset: usize, value: u32) {
        self.bytes[offset..offset + size_of::<u32>()].copy_from_slice(&value.to_be_bytes());
    }

    fn allocate(&mut self, length: usize, alignment: usize) -> Result<usize, Error> {
        let start = self.bytes.len().next_multiple_of(alignment);
        let end = start
            .checked_add(length)
            .filter(|end| *end <= usize::from(u16::MAX))
            .ok_or(Error("video inventory exceeds its declared extent"))?;
        self.bytes.resize(end, 0);
        Ok(start)
    }

    fn name(&mut self, value: &[u8]) -> Result<u16, Error> {
        if value.is_empty() || value.contains(&0) {
            return Err(Error(
                "video inventory name must be complete, nonempty and contain no NUL",
            ));
        }

        let length = value
            .len()
            .checked_add(1)
            .ok_or(Error("video name extent overflow"))?;
        let start = self.allocate(length, 1)?;
        self.bytes[start..start + value.len()].copy_from_slice(value);
        Ok(start as u16)
    }

    fn optional_name(&mut self, value: Option<&[u8]>) -> Result<u16, Error> {
        value
            .map(|value| self.name(value))
            .transpose()
            .map(|value| value.unwrap_or(0))
    }

    fn format(&mut self, codec_identifier: Option<u32>) -> Result<u16, Error> {
        let Some(identifier) = codec_identifier else {
            return Ok(0);
        };

        let start = self.allocate(FORMAT_BYTES, OBJECT_ALIGNMENT)?;
        self.bytes[start..start + FORMAT_PREFIX.len()].copy_from_slice(FORMAT_PREFIX);
        self.double_word(start + 8, H264_SUBTYPE);
        self.double_word(start + 12, identifier);
        Ok(start as u16)
    }

    fn endpoint(&mut self, endpoint: SocketAddrV4) -> Result<u16, Error> {
        if endpoint.port() == 0 || endpoint.ip().is_unspecified() {
            return Err(Error(
                "video inventory endpoint requires an owned address and port",
            ));
        }

        let start = self.allocate(ENDPOINT_BYTES, OBJECT_ALIGNMENT)?;
        self.bytes[start] = ENDPOINT_LENGTH;
        self.bytes[start + 1] = IPV4_FAMILY;
        self.word(start + 2, endpoint.port());
        self.bytes[start + 4..start + ENDPOINT_BYTES].copy_from_slice(&endpoint.ip().octets());
        Ok(start as u16)
    }

    fn finish(mut self) -> Vec<u8> {
        self.word(2, self.bytes.len() as u16);
        self.bytes
    }
}

pub struct Capability {
    pub capability_word: u16,
    pub raw_status: u32,
    pub receiver_error_fields: [u16; 2],
    pub raw_base_bytes: [u8; 4],
    pub display_group_upper_identifier: u16,
    pub raw_compatibility_flags: u32,
    pub message_flags: u32,
    pub transmitter_provisioned: bool,
    pub receiver_provisioned: bool,
    pub raw_extension_before_scalar: [u8; 2],
    pub raw_extension_scalar: u16,
    pub raw_extension_after_scalar: [u8; 2],
    pub receiver_format_capacity: u8,
}

impl Capability {
    pub fn encode(&self, context: Context) -> Result<Vec<u8>, Error> {
        if self.capability_word & MEDIA_AWARE_CAPABILITY == 0
            || !(self.transmitter_provisioned || self.receiver_provisioned)
            || (!self.receiver_provisioned && self.receiver_format_capacity != 0)
            || (context.revision < DISPLAY_GROUP_REVISION
                && self.display_group_upper_identifier != 0)
            || (context.revision < COMPATIBILITY_FLAGS_REVISION
                && self.raw_compatibility_flags != 0)
            || (context.revision < MESSAGE_FLAGS_REVISION && self.message_flags != 0)
        {
            return Err(Error(
                "video capability inputs contradict the selected provisioned profile",
            ));
        }

        let mut writer = Writer::new(
            context,
            OPCODE_CHANNEL_COUNT,
            RESULT_CODE_SUCCESS,
            CAPABILITY_BASE_BYTES + CAPABILITY_EXTENSION_BYTES,
        )?;
        writer.word(10, self.capability_word);
        writer.word(30, 1);
        writer.double_word(32, self.raw_status);
        writer.word(36, self.receiver_error_fields[0]);
        writer.word(38, self.receiver_error_fields[1]);
        writer.bytes[40..44].copy_from_slice(&self.raw_base_bytes);
        writer.word(44, CAPABILITY_BASE_BYTES as u16);
        writer.word(46, self.display_group_upper_identifier);
        writer.double_word(48, self.raw_compatibility_flags);
        writer.double_word(52, self.message_flags);

        let extension = CAPABILITY_BASE_BYTES;
        writer.bytes[extension] = CAPABILITY_EXTENSION_WORDS;
        writer.bytes[extension + 1] = CAPABILITY_EXTENSION_WORDS;
        writer.word(extension + 2, MEDIA_SELECTOR);

        for offset in [4, 6] {
            writer.word(extension + offset, 1);
        }

        for offset in [8, 10, 16, 18, 24, 26] {
            writer.word(extension + offset, u16::from(self.transmitter_provisioned));
        }

        for offset in [12, 14, 20, 22, 28, 30] {
            writer.word(extension + offset, u16::from(self.receiver_provisioned));
        }

        writer.bytes[extension + 32..extension + 34]
            .copy_from_slice(&self.raw_extension_before_scalar);
        writer.word(extension + 34, self.raw_extension_scalar);
        writer.bytes[extension + 36..extension + 38]
            .copy_from_slice(&self.raw_extension_after_scalar);
        writer.word(extension + 38, u16::from(self.receiver_format_capacity));
        Ok(writer.finish())
    }
}

pub struct Identity {
    pub global: NonZeroU16,
    pub media_local: NonZeroU16,
}

pub struct Channel<'a> {
    pub identity: Identity,
    pub effective_name: &'a [u8],
    pub default_name: &'a [u8],
    pub codec_identifier: Option<u32>,
    pub raw_state: u32,
    pub external_readiness: u16,
    pub raw_protection_scheme: u8,
    pub display_group: u16,
    pub raw_scalar: u16,
}

pub struct ReceiverChannel<'a> {
    pub channel: Channel<'a>,
    pub receiver_processing: u16,
    pub supported_codec_identifiers: &'a [u32],
    pub source_labels: Option<(&'a [u8], &'a [u8])>,
    pub raw_subscription_status: u16,
    pub raw_availability: u8,
    pub raw_asserted_state: u8,
}

pub struct Flow<'a> {
    pub identity: Identity,
    pub effective_name: Option<&'a [u8]>,
    pub codec_identifier: Option<u32>,
    pub raw_flags: u32,
    pub raw_current_scalar: u32,
    pub raw_state: u8,
    pub raw_endpoint_state: u8,
    pub endpoints: &'a [SocketAddrV4],
}

pub struct EndpointProperties<'a> {
    pub raw_state: u8,
    pub values: &'a [u16],
}

pub struct TransmitterFlow<'a> {
    pub flow: Flow<'a>,
    pub source_labels: [Option<&'a [u8]>; 2],
    pub source_channel: NonZeroU16,
    pub endpoint_properties: Option<EndpointProperties<'a>>,
}

pub struct ReceiverFlow<'a> {
    pub flow: Flow<'a>,
    pub transport_description_selector: u16,
    pub receiver_channel: Option<NonZeroU16>,
    pub slot_count: u16,
}

pub enum Row<'a> {
    TransmitterChannel(Channel<'a>),
    ReceiverChannel(ReceiverChannel<'a>),
    TransmitterFlow(TransmitterFlow<'a>),
    ReceiverFlow(ReceiverFlow<'a>),
}

#[derive(Clone, Copy)]
pub enum Table {
    TransmitterChannels,
    ReceiverChannels,
    TransmitterFlows,
    ReceiverFlows,
}

impl Table {
    fn operation(self) -> u16 {
        match self {
            Self::TransmitterChannels => OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
            Self::ReceiverChannels => OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809,
            Self::TransmitterFlows => OPCODE_QUERY_TX_FLOWS_2809,
            Self::ReceiverFlows => OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809,
        }
    }
}

fn common(
    writer: &mut Writer,
    length: usize,
    identity: &Identity,
    raw_state: u32,
    name: Option<&[u8]>,
    codec_identifier: Option<u32>,
) -> Result<usize, Error> {
    let start = writer.allocate(length, OBJECT_ALIGNMENT)?;
    writer.bytes[start] = COMMON_ROW_WORDS;
    writer.bytes[start + 1] = (length / size_of::<u16>()) as u8;
    writer.word(start + 2, identity.global.get());
    writer.word(start + 6, MEDIA_SELECTOR);
    writer.word(start + 8, identity.media_local.get());
    writer.double_word(start + 12, raw_state);
    let name_pointer = writer.optional_name(name)?;
    let format_pointer = writer.format(codec_identifier)?;
    writer.word(start + 20, name_pointer);
    writer.word(start + 22, format_pointer);
    Ok(start)
}

fn channel(writer: &mut Writer, value: &Channel<'_>, receiver: bool) -> Result<usize, Error> {
    let size = if receiver {
        RECEIVER_CHANNEL_BYTES
    } else {
        TRANSMITTER_CHANNEL_BYTES
    };
    let start = common(
        writer,
        size,
        &value.identity,
        value.raw_state,
        Some(value.effective_name),
        value.codec_identifier,
    )?;
    let default_pointer = if value.default_name == value.effective_name {
        u16::from_be_bytes([writer.bytes[start + 20], writer.bytes[start + 21]])
    } else {
        writer.name(value.default_name)?
    };
    writer.word(start + 28, value.external_readiness);
    writer.word(start + 30, default_pointer);
    writer.bytes[start + 34] = value.raw_protection_scheme;
    writer.word(start + 36, value.display_group);
    writer.word(start + 40, value.raw_scalar);
    Ok(start)
}

fn receiver_channel(writer: &mut Writer, value: &ReceiverChannel<'_>) -> Result<usize, Error> {
    let start = channel(writer, &value.channel, true)?;
    let count = u8::try_from(value.supported_codec_identifiers.len())
        .map_err(|_| Error("video supported-format list exceeds its capacity field"))?;

    if count != 0 {
        let list = writer.allocate(2 + usize::from(count) * size_of::<u16>(), WORD_ALIGNMENT)?;
        writer.bytes[list] = count;
        writer.bytes[list + 1] = count;

        for (index, identifier) in value.supported_codec_identifiers.iter().enumerate() {
            let pointer = writer.format(Some(*identifier))?;
            writer.word(list + 2 + index * size_of::<u16>(), pointer);
        }

        writer.word(start + 38, list as u16);
    }

    writer.word(start + 32, value.receiver_processing);
    writer.bytes[start + COMMON_ROW_BYTES] = 6;
    writer.bytes[start + COMMON_ROW_BYTES + 1] = 6;

    if let Some((source_channel, source_device)) = value.source_labels {
        let channel = writer.name(source_channel)?;
        let device = writer.name(source_device)?;
        writer.word(start + 48, channel);
        writer.word(start + 50, device);
    }

    writer.word(start + 52, value.raw_subscription_status);
    writer.bytes[start + 54] = value.raw_availability;
    writer.bytes[start + 55] = value.raw_asserted_state;
    Ok(start)
}

fn flow(writer: &mut Writer, value: &Flow<'_>, receiver: bool) -> Result<usize, Error> {
    if value.endpoints.len() > MAXIMUM_ENDPOINTS || (receiver && value.effective_name.is_none()) {
        return Err(Error(
            "video flow needs a supported endpoint count and required effective name",
        ));
    }

    let length = if receiver {
        RECEIVER_FLOW_BYTES
    } else {
        TRANSMITTER_FLOW_BYTES
    };
    let start = common(
        writer,
        length,
        &value.identity,
        value.raw_flags,
        value.effective_name,
        value.codec_identifier,
    )?;
    writer.double_word(start + 24, value.raw_current_scalar);
    writer.bytes[start + 34] = value.raw_state;
    let segment = start + if receiver { 68 } else { 64 };
    writer.bytes[segment] = 4;
    writer.bytes[segment + 1] = if receiver { 4 } else { 8 };
    writer.bytes[segment + 2] = value.endpoints.len() as u8;
    writer.bytes[segment + 3] = value.raw_endpoint_state;

    for (index, endpoint) in value.endpoints.iter().enumerate() {
        let pointer = writer.endpoint(*endpoint)?;
        writer.word(segment + 4 + index * size_of::<u16>(), pointer);
    }

    Ok(start)
}

fn transmitter_flow(writer: &mut Writer, value: &TransmitterFlow<'_>) -> Result<usize, Error> {
    let start = flow(writer, &value.flow, false)?;
    writer.bytes[start + 44] = 10;
    writer.bytes[start + 45] = 18;

    for (index, name) in value.source_labels.iter().enumerate() {
        let pointer = writer.optional_name(*name)?;
        writer.word(start + 48 + index * size_of::<u16>(), pointer);
    }

    writer.bytes[start + 72] = 4;
    writer.bytes[start + 73] = 4;
    writer.word(start + 74, 1);
    writer.word(start + 76, value.source_channel.get());

    if let Some(properties) = &value.endpoint_properties {
        if properties.values.is_empty() || properties.values.len() > MAXIMUM_ENDPOINTS {
            return Err(Error(
                "video endpoint properties exceed the selected endpoint profile",
            ));
        }

        let vector = writer.allocate(
            6 + std::mem::size_of_val(properties.values),
            OBJECT_ALIGNMENT,
        )?;
        writer.bytes[vector] = 3;
        writer.bytes[vector + 1] = 3;
        writer.bytes[vector + 2] = properties.values.len() as u8;
        writer.bytes[vector + 3] = properties.raw_state;
        writer.word(vector + 4, (vector + 6) as u16);

        for (index, value) in properties.values.iter().enumerate() {
            writer.word(vector + 6 + index * size_of::<u16>(), *value);
        }

        writer.word(start + 42, vector as u16);
    }

    Ok(start)
}

fn receiver_flow(writer: &mut Writer, value: &ReceiverFlow<'_>) -> Result<usize, Error> {
    if value.slot_count > 1 || (value.receiver_channel.is_some() && value.slot_count != 1) {
        return Err(Error(
            "video receive mapping requires the selected one-slot flow",
        ));
    }

    let start = flow(writer, &value.flow, true)?;
    writer.bytes[start + 44] = 12;
    writer.bytes[start + 45] = 16;
    writer.word(start + 52, value.transport_description_selector);
    writer.word(start + 56, value.slot_count);

    if let Some(channel) = value.receiver_channel {
        let mapping = writer.allocate(8, OBJECT_ALIGNMENT)?;
        writer.word(mapping, channel.get());
        writer.word(mapping + 2, 2);
        writer.bytes[mapping + 6] = 1;
        writer.word(start + 58, mapping as u16);
    }

    Ok(start)
}

pub fn page(
    context: Context,
    table: Table,
    capacity: u8,
    row: Option<&Row<'_>>,
    partial: bool,
) -> Result<Vec<u8>, Error> {
    if capacity > SINGLE_CHANNEL_CAPACITY || (capacity == 0 && row.is_some()) {
        return Err(Error(
            "video inventory capacity does not admit the selected row",
        ));
    }

    let result = if partial {
        RESULT_CODE_MORE_PAGES
    } else {
        RESULT_CODE_SUCCESS
    };
    let mut writer = Writer::new(
        context,
        table.operation(),
        result,
        PAGE_BASE_BYTES + usize::from(capacity) * size_of::<u16>(),
    )?;
    writer.bytes[16] = capacity;

    if let Some(row) = row {
        let pointer = match (table, row) {
            (Table::TransmitterChannels, Row::TransmitterChannel(value)) => {
                channel(&mut writer, value, false)?
            }
            (Table::ReceiverChannels, Row::ReceiverChannel(value)) => {
                receiver_channel(&mut writer, value)?
            }
            (Table::TransmitterFlows, Row::TransmitterFlow(value)) => {
                transmitter_flow(&mut writer, value)?
            }
            (Table::ReceiverFlows, Row::ReceiverFlow(value)) => receiver_flow(&mut writer, value)?,
            _ => {
                return Err(Error(
                    "video inventory row does not belong to the selected table",
                ))
            }
        };
        writer.bytes[17] = 1;
        writer.word(PAGE_BASE_BYTES, pointer as u16);
    }

    Ok(writer.finish())
}

pub fn selected_interval(
    first: u16,
    last: u16,
    bound: u16,
) -> Option<std::ops::RangeInclusive<u16>> {
    if first == 0 {
        return (last != 0 && last <= bound).then_some(last..=last);
    }

    let last = if last == 0 || last > bound {
        bound
    } else {
        last
    };
    (first <= last).then_some(first..=last)
}
