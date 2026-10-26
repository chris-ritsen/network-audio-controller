use serde::Serialize;

use crate::bytes::read_u16;
use crate::commands;
use crate::parser::audio::{parse_transmit_channel_page, TransmitChannelPage};
use crate::parser::{parse_channel_count, ChannelCount};
use crate::protocol::{
    response_envelope, OPCODE_CHANNEL_COUNT, OPCODE_TX_CHANNEL_INFO, RESULT_CODE_MORE_PAGES,
    RESULT_CODE_SUCCESS,
};
use crate::responses::{
    parse_device_name_observation, parse_device_settings, parse_property_directory,
    parse_structured_device_identity, DeviceNameObservation, DeviceSettings, PropertyDirectory,
    StructuredDeviceIdentity,
};

use super::properties::identifier;
use super::source_control::Error;

pub const AUDIO_PROPERTY_IDENTIFIERS: [u16; 8] = [
    identifier::SOURCE_DEFAULT_FRAMES_PER_PACKET,
    identifier::UNICAST_FRAMES_PER_PACKET,
    identifier::SOURCE_MINIMUM_FRAMES_PER_PACKET,
    identifier::SOURCE_MAXIMUM_FRAMES_PER_PACKET,
    identifier::RECEIVE_FRAMES_PER_PACKET,
    identifier::UNICAST_LATENCY,
    identifier::RECEIVE_LATENCY,
    identifier::CLOCK_DOMAIN,
];

pub const STRUCTURED_IDENTITY_CAPABILITY: u16 = 0x0100;
pub const MEDIA_RESOURCES_CAPABILITY: u16 = 0x1000;
const DECLARED_LENGTH_OFFSET: usize = 2;
const TRANSACTION_OFFSET: usize = 4;
const OPERATION_OFFSET: usize = 6;
const MAXIMUM_BASIC_NAME_BYTES: usize = 31;
const RESULT_STATUS_MASK: u16 = 0x0fff;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Capabilities,
    Name,
    Identity,
    PropertyDirectory,
    AudioProperties,
    TransmitChannels {
        first: u16,
        last: u16,
    },
}

impl Stage {
    pub fn operation(self) -> u16 {
        match self {
            Self::Capabilities => OPCODE_CHANNEL_COUNT,
            Self::Name => commands::OPCODE_DEVICE_NAME,
            Self::Identity => commands::OPCODE_DEVICE_INFO,
            Self::PropertyDirectory => commands::OPCODE_PROPERTY_DIRECTORY,
            Self::AudioProperties => commands::OPCODE_DEVICE_SETTINGS,
            Self::TransmitChannels {
                ..
            } => OPCODE_TX_CHANNEL_INFO,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Request {
    pub protocol_revision: u16,
    pub stage: Stage,
    pub transaction: u16,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "stage", content = "value", rename_all = "snake_case")]
pub enum Value {
    Capabilities(Box<ChannelCount>),
    Name(DeviceNameObservation),
    Identity(StructuredDeviceIdentity),
    PropertyDirectory(PropertyDirectory),
    AudioProperties(Box<DeviceSettings>),
    TransmitChannels(TransmitChannelPage),
}

#[derive(Clone, Debug, Serialize)]
pub struct Reply {
    pub request: Request,
    pub value: Value,
    pub packet: Vec<u8>,
    pub trailing_datagram_bytes: Vec<u8>,
}

impl Request {
    pub fn encode(self) -> Result<Vec<u8>, Error> {
        let packet = match self.stage {
            Stage::Capabilities => {
                commands::build_channel_count_for_protocol(self.protocol_revision, self.transaction)
            }
            Stage::Name => {
                commands::build_device_name_for_protocol(self.protocol_revision, self.transaction)
            }
            Stage::Identity => commands::build_structured_device_identity_query(
                self.protocol_revision,
                self.transaction,
            ),
            Stage::PropertyDirectory => commands::build_property_directory_for_protocol(
                self.protocol_revision,
                self.transaction,
            ),
            Stage::AudioProperties => commands::build_query_performance_settings(
                self.protocol_revision,
                &AUDIO_PROPERTY_IDENTIFIERS,
                self.transaction,
            ),
            Stage::TransmitChannels {
                first,
                last,
            } => commands::build_transmitters_range_for_protocol(
                self.protocol_revision,
                first,
                last,
                self.transaction,
            ),
        };

        packet.map_err(|_| Error("source acquisition request could not be encoded"))
    }

    pub fn decode_reply(self, datagram: &[u8]) -> Result<Option<Reply>, Error> {
        if read_u16(datagram, TRANSACTION_OFFSET) != Some(self.transaction)
            || read_u16(datagram, OPERATION_OFFSET) != Some(self.stage.operation())
        {
            return Ok(None);
        }

        let length = usize::from(
            read_u16(datagram, DECLARED_LENGTH_OFFSET)
                .ok_or(Error("source acquisition reply length is absent"))?,
        );
        let packet =
            datagram.get(..length).ok_or(Error("source acquisition reply is truncated"))?;
        let envelope = response_envelope(packet)
            .ok_or(Error("source acquisition reply envelope is invalid"))?;

        if envelope.protocol_id != self.protocol_revision {
            return Err(Error("source acquisition reply revision is unsupported"));
        }

        if envelope.result_code == 0 {
            return Ok(None);
        }

        let partial_channels = matches!(self.stage, Stage::TransmitChannels { .. })
            && envelope.result_code == RESULT_CODE_MORE_PAGES;

        let structured_identity_success = self.stage == Stage::Identity
            && envelope.result_code & RESULT_STATUS_MASK == RESULT_CODE_SUCCESS;

        if envelope.result_code != RESULT_CODE_SUCCESS
            && !partial_channels
            && !structured_identity_success
        {
            return Err(Error("source acquisition reply did not report success"));
        }

        let value = match self.stage {
            Stage::Capabilities => Value::Capabilities(Box::new(
                parse_channel_count(packet)
                    .ok_or(Error("source capability reply is incomplete or invalid"))?,
            )),
            Stage::Name => {
                let name = parse_device_name_observation(packet)
                    .ok_or(Error("source basic-name reply is incomplete or invalid"))?;

                if name.name_bytes.len() > MAXIMUM_BASIC_NAME_BYTES {
                    return Err(Error("source basic-name reply exceeds the selected field limit"));
                }

                Value::Name(name)
            }
            Stage::Identity => {
                let identity = parse_structured_device_identity(packet)
                    .ok_or(Error("source structured identity is incomplete or invalid"))?;

                if identity.current_name.bytes().is_none() {
                    return Err(Error("source structured current name is absent or malformed"));
                }

                Value::Identity(identity)
            }
            Stage::PropertyDirectory => Value::PropertyDirectory(
                parse_property_directory(packet)
                    .ok_or(Error("source property directory is incomplete or invalid"))?,
            ),
            Stage::AudioProperties => Value::AudioProperties(Box::new(
                parse_device_settings(packet)
                    .ok_or(Error("source audio property reply is incomplete or invalid"))?,
            )),
            Stage::TransmitChannels {
                first,
                last,
            } => {
                let page = parse_transmit_channel_page(packet)
                    .ok_or(Error("source channel page is incomplete or invalid"))?;

                if page
                    .channels
                    .iter()
                    .any(|channel| channel.identifier < first || channel.identifier > last)
                {
                    return Err(Error("source channel page escaped its requested interval"));
                }

                Value::TransmitChannels(page)
            }
        };

        Ok(Some(Reply {
            request: self,
            value,
            packet: packet.to_vec(),
            trailing_datagram_bytes: datagram[length..].to_vec(),
        }))
    }
}
