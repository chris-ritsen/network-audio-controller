use std::net::Ipv4Addr;

use serde::Deserialize;

use crate::protocol::{ConmonHeader, NetaudioError};

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum ChannelDirection {
    Rx,
    Tx,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelSource {
    channel: String,
    device: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedChannel {
    name: String,
    source: Option<ChannelSource>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArcResponse {
    protocol_id: u16,
    transaction_id: u16,
    result_code: u16,
    response: ArcResponseBody,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ArcResponseBody {
    Acknowledgement {
        request: Vec<u8>,
        accepted: bool,
    },
    EmptyFlows {
        channel_type: ChannelDirection,
    },
    EmptyTransmitterFlowLabels,
    LatencyApplied {
        latency_ns: u32,
    },
    CmcRegistration {
        device_ip: Ipv4Addr,
        settings_port: std::num::NonZeroU16,
    },
    PropertyDirectory,
    ReceiverPortRanges,
    DeviceName {
        name: String,
    },
    ChannelCount {
        tx_count: u16,
        rx_count: u16,
    },
    TransmitterNames {
        names: Vec<String>,
    },
    ChannelStatus {
        channel_type: ChannelDirection,
        channels: Vec<PublishedChannel>,
        audio: crate::parser::ChannelAudioConfiguration,
    },
    DeviceInfo {
        model_name: String,
        display_name: String,
        model_code: String,
        port: String,
    },
    DeviceSettings {
        sample_rate: u32,
        default_latency_ns: u32,
        configured_latency_ns: u32,
        active_latency_ns: u32,
        maximum_latency_ns: u32,
        minimum_latency_ns: u32,
    },
}

const EMPTY_FLOW_PAGE: [u8; 2] = [2, 0];

fn append_response_string(body: &mut Vec<u8>, value: &str) -> Result<u16, NetaudioError> {
    if value.as_bytes().contains(&0) {
        return Err(NetaudioError::NameInvalidChars);
    }

    let pointer = crate::protocol::RESPONSE_HEADER_SIZE + body.len();
    let end = pointer
        .checked_add(value.len())
        .and_then(|end| end.checked_add(1))
        .ok_or(NetaudioError::PacketTooLarge)?;

    if end > usize::from(u16::MAX) {
        return Err(NetaudioError::PacketTooLarge);
    }

    body.extend_from_slice(value.as_bytes());
    body.push(0);
    Ok(pointer as u16)
}

impl ArcResponse {
    pub fn packet(&self) -> Result<Vec<u8>, NetaudioError> {
        use crate::protocol::{self, ArcHeader};

        let is_arc = protocol::is_supported_arc_protocol(self.protocol_id);
        let is_cmc = self.protocol_id == crate::commands::PROTOCOL_CMC;
        let supported = match &self.response {
            ArcResponseBody::CmcRegistration { .. } => is_cmc,
            _ => is_arc,
        };

        if !supported {
            return Err(NetaudioError::UnsupportedProtocolOperation);
        }

        let mut result_code = self.result_code;
        let (opcode, body) = match &self.response {
            ArcResponseBody::Acknowledgement { request, accepted } => {
                let header = protocol::control_request_header(request)
                    .ok_or(NetaudioError::UnsupportedProtocolOperation)?;

                if header.protocol_id != self.protocol_id
                    || header.transaction_id != self.transaction_id
                {
                    return Err(NetaudioError::UnsupportedProtocolOperation);
                }

                result_code = if *accepted {
                    protocol::RESULT_CODE_SUCCESS
                } else {
                    protocol::RESULT_CODE_FRONTEND_UNAVAILABLE
                };

                (header.opcode, Vec::new())
            }
            ArcResponseBody::EmptyFlows { channel_type } => {
                let opcode = match channel_type {
                    ChannelDirection::Tx => crate::commands::OPCODE_QUERY_TX_FLOWS,
                    ChannelDirection::Rx => crate::commands::OPCODE_QUERY_RECEIVER_FLOWS,
                };
                (opcode, EMPTY_FLOW_PAGE.to_vec())
            }
            ArcResponseBody::EmptyTransmitterFlowLabels => (
                crate::commands::OPCODE_QUERY_TX_FLOW_LABELS,
                EMPTY_FLOW_PAGE.to_vec(),
            ),
            ArcResponseBody::LatencyApplied { latency_ns } => {
                use crate::commands::*;

                require_latency_protocol(self.protocol_id)?;
                let body = property_write_body(&[
                    (
                        PROPERTY_UNICAST_CONFIGURED_LATENCY_NS,
                        PropertyValue::ReferencedU32(*latency_ns),
                    ),
                    (
                        PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET,
                        PropertyValue::InlineU16(4),
                    ),
                    (
                        PROPERTY_RX_FLOW_LATENCY_NS,
                        PropertyValue::ReferencedU32(*latency_ns),
                    ),
                    (
                        PROPERTY_RX_FLOW_FRAMES_PER_PACKET,
                        PropertyValue::InlineU16(4),
                    ),
                ])?;
                (OPCODE_DEVICE_SETTINGS_SET, body)
            }
            ArcResponseBody::CmcRegistration {
                device_ip,
                settings_port,
            } => {
                let mut body = vec![0; 4];
                body.extend_from_slice(&device_ip.octets());
                body.extend_from_slice(&[0, 0, 0, 1, 0, 0]);
                body.extend_from_slice(&device_ip.octets());
                body.extend_from_slice(&settings_port.get().to_be_bytes());
                body.extend_from_slice(&[0, 0]);
                (crate::commands::OPCODE_CMC_REGISTER, body)
            }
            ArcResponseBody::PropertyDirectory => {
                let properties: [(u16, u16); 31] = [
                    (0x8020, 1),
                    (0x8021, 3),
                    (0x0022, 3),
                    (0x0023, 3),
                    (0x0024, 1),
                    (
                        crate::responses::DEVICE_SETTINGS_INFO_AES67_MULTICAST_PREFIX,
                        3,
                    ),
                    (0x0062, 3),
                    (0x0063, 1),
                    (0x0201, 3),
                    (0x8204, 3),
                    (0x8205, 3),
                    (0x020A, 1),
                    (0x020B, 1),
                    (0x0210, 3),
                    (0x0211, 3),
                    (0x0212, 3),
                    (0x0213, 1),
                    (0x0214, 1),
                    (0x0222, 3),
                    (0x8301, 3),
                    (0x8306, 1),
                    (0x8302, 1),
                    (0x8321, 1),
                    (0x0310, 1),
                    (0x0311, 1),
                    (0x0312, 1),
                    (0x0303, 3),
                    (0x83F0, 1),
                    (0x0601, 1),
                    (0x0309, 1),
                    (0x0209, 1),
                ];
                let mut body = (properties.len() as u16).to_be_bytes().to_vec();

                for (property, flags) in properties {
                    body.extend_from_slice(&property.to_be_bytes());
                    body.extend_from_slice(&flags.to_be_bytes());
                }

                (crate::commands::OPCODE_PROPERTY_DIRECTORY, body)
            }
            ArcResponseBody::ReceiverPortRanges => (
                crate::commands::OPCODE_QUERY_RECEIVER_PORT_RANGES,
                vec![0x38, 0x00, 0x38, 0xFD, 0x38, 0xFE, 0x38, 0xFF],
            ),
            ArcResponseBody::DeviceName { name } => {
                protocol::validate_dante_name(name)?;
                let mut body = name.as_bytes().to_vec();
                body.push(0);
                (crate::commands::OPCODE_DEVICE_NAME, body)
            }
            ArcResponseBody::ChannelCount { tx_count, rx_count } => {
                let total = tx_count
                    .checked_add(*rx_count)
                    .ok_or(NetaudioError::PacketTooLarge)?;
                let mut body = Vec::with_capacity(34);

                for word in [0x0030, *tx_count, *rx_count, 4, 8, 8, 32, 32, total, 1, 1] {
                    body.extend_from_slice(&word.to_be_bytes());
                }

                body.extend_from_slice(&[0; 12]);
                (protocol::OPCODE_CHANNEL_COUNT, body)
            }
            ArcResponseBody::ChannelStatus {
                channel_type,
                channels,
                audio,
            } => {
                let receiver = matches!(channel_type, ChannelDirection::Rx);
                let opcode = if receiver {
                    protocol::OPCODE_RX_CHANNELS
                } else {
                    protocol::OPCODE_TX_CHANNEL_INFO
                };
                let body = match channel_status_body(receiver, channels, audio)? {
                    Some(body) => body,
                    None => {
                        result_code = 0x0030;
                        Vec::new()
                    }
                };
                (opcode, body)
            }
            ArcResponseBody::TransmitterNames { names } => {
                if names.len() > usize::from(crate::parser::TX_CHANNELS_PER_PAGE) {
                    return Err(NetaudioError::PacketTooLarge);
                }

                let count = u8::try_from(names.len()).map_err(|_| NetaudioError::PacketTooLarge)?;
                let records_end = protocol::RESPONSE_HEADER_SIZE + 2 + names.len() * 6;
                let padding = (4 - records_end % 4) % 4;
                let mut body = vec![0; 2 + names.len() * 6 + padding];
                body[0] = count;
                body[1] = count;

                for (index, name) in names.iter().enumerate() {
                    let pointer = append_response_string(&mut body, name)?;
                    let number = index as u16 + 1;

                    for (word, value) in [number, number, pointer].into_iter().enumerate() {
                        let offset = 2 + index * 6 + word * 2;
                        body[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
                    }
                }

                (protocol::OPCODE_TX_CHANNEL_NAMES, body)
            }
            ArcResponseBody::DeviceInfo {
                model_name,
                display_name,
                model_code,
                port,
            } => {
                let mut body = vec![0; 34];
                body[10..12].copy_from_slice(&0x0500u16.to_be_bytes());
                body[30..32].copy_from_slice(&0x2729u16.to_be_bytes());

                for (offset, value) in [
                    (6, model_code),
                    (8, port),
                    (12, model_name),
                    (14, display_name),
                ] {
                    let pointer = append_response_string(&mut body, value)?;
                    body[offset..offset + 2].copy_from_slice(&pointer.to_be_bytes());
                }

                let display_pointer = [body[14], body[15]];
                body[16..18].copy_from_slice(&display_pointer);
                (crate::commands::OPCODE_DEVICE_INFO, body)
            }
            ArcResponseBody::DeviceSettings {
                sample_rate,
                default_latency_ns,
                configured_latency_ns,
                active_latency_ns,
                maximum_latency_ns,
                minimum_latency_ns,
            } => {
                use crate::responses::*;

                let settings = [
                    (DEVICE_SETTINGS_INFO_SAMPLE_RATE, sample_rate),
                    (DEVICE_SETTINGS_INFO_DEFAULT_LATENCY_NS, default_latency_ns),
                    (
                        DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS,
                        configured_latency_ns,
                    ),
                    (DEVICE_SETTINGS_INFO_ACTIVE_LATENCY_NS, active_latency_ns),
                    (DEVICE_SETTINGS_INFO_MAX_LATENCY_NS, maximum_latency_ns),
                    (DEVICE_SETTINGS_INFO_MIN_LATENCY_NS, minimum_latency_ns),
                ];
                let mut body = vec![2, settings.len() as u8];
                let first_value_offset = RESPONSE_HEADER_SIZE + 2 + settings.len() * 4;

                for (index, (property, _)) in settings.iter().enumerate() {
                    body.extend_from_slice(&property.to_be_bytes());
                    body.extend_from_slice(
                        &((first_value_offset + index * 4) as u16).to_be_bytes(),
                    );
                }

                for (_, value) in settings {
                    body.extend_from_slice(&value.to_be_bytes());
                }

                (crate::commands::OPCODE_DEVICE_SETTINGS, body)
            }
        };
        let mut payload = Vec::with_capacity(2 + body.len());
        payload.extend_from_slice(&result_code.to_be_bytes());
        payload.extend_from_slice(&body);

        ArcHeader {
            message_id: self.transaction_id,
            opcode,
            protocol_id: self.protocol_id,
        }
        .packet(&payload)
    }
}

fn channel_status_body(
    receiver: bool,
    channels: &[PublishedChannel],
    audio: &crate::parser::ChannelAudioConfiguration,
) -> Result<Option<Vec<u8>>, NetaudioError> {
    let maximum = if receiver {
        crate::parser::RX_CHANNELS_PER_PAGE
    } else {
        crate::parser::TX_CHANNELS_PER_PAGE
    };

    if channels.len() > usize::from(maximum) {
        return Err(NetaudioError::PacketTooLarge);
    }

    if !receiver && channels.iter().any(|channel| channel.source.is_some()) {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }

    let Some(publication) = crate::parser::channel_audio_publication(audio) else {
        return Ok(None);
    };

    if channels.is_empty() {
        return Ok(Some(vec![0, 0]));
    }

    let record_size = if receiver { 20 } else { 8 };
    let metadata_offset = 2 + channels.len() * record_size;
    let metadata_pointer = (crate::protocol::RESPONSE_HEADER_SIZE + metadata_offset) as u16;
    let mut body = vec![0; metadata_offset];
    body[0] = channels.len() as u8;
    body[1] = channels.len() as u8;
    body.extend_from_slice(&publication.channel_metadata);

    for (index, channel) in channels.iter().enumerate() {
        let name = append_response_string(&mut body, &channel.name)?;
        let number = index as u16 + 1;
        let words = if receiver {
            let (source_channel, source_device, status) = match &channel.source {
                Some(source) => (
                    append_response_string(&mut body, &source.channel)?,
                    append_response_string(&mut body, &source.device)?,
                    9,
                ),
                None => (0, 0, 0),
            };
            vec![
                number,
                6,
                metadata_pointer,
                source_channel,
                source_device,
                name,
                0,
                status,
                0,
                0,
            ]
        } else {
            vec![number, 7, metadata_pointer, name]
        };

        for (word, value) in words.into_iter().enumerate() {
            let offset = 2 + index * record_size + word * 2;
            body[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
        }
    }

    Ok(Some(body))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioCapabilityKind {
    Encoding,
    SampleRate,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Publication {
    source_ip: Ipv4Addr,
    message_id: u16,
    publication: PublicationBody,
}

pub fn next_publication_id(previous: u16) -> u16 {
    previous.wrapping_add(1)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PublicationBody {
    BoardInfo {
        name: String,
    },
    ProductInfo {
        name: String,
        manufacturer: String,
        model: String,
    },
    ClockStatus {
        mac_address: [u8; 6],
    },
    InterfaceStatus {
        mac_address: [u8; 6],
    },
    Audio {
        capability: AudioCapabilityKind,
        current_value: u32,
        supported_values: Vec<u32>,
    },
    Heartbeat {
        tx_count: u16,
        rx_count: u16,
    },
}

fn fixed_text(destination: &mut [u8], text: &str) -> Result<(), NetaudioError> {
    if text.as_bytes().contains(&0) {
        return Err(NetaudioError::NameInvalidChars);
    }

    let length = destination.len().min(text.len());
    destination[..length].copy_from_slice(&text.as_bytes()[..length]);
    Ok(())
}

impl Publication {
    pub fn packet(&self) -> Result<Vec<u8>, NetaudioError> {
        match &self.publication {
            PublicationBody::BoardInfo { name } => {
                let mut body = vec![0; 200];
                body[..8].copy_from_slice(&[4, 1, 0, 6, 4, 1, 0, 3]);
                body[0x23] = 2;
                body[0x27] = 1;
                body[0x28] = 1;
                body[0x16] = 0x10;
                body[0xBB] = 0x1F;
                fixed_text(&mut body[12..20], name)?;
                fixed_text(&mut body[0x38..0x40], name)?;

                self.frame(0xFFFF, &[7, 0x2A, 0, 0x60, 0, 0, 0, 0], &body)
            }
            PublicationBody::ProductInfo {
                name,
                manufacturer,
                model,
            } => {
                let mut body = vec![0; 336];
                fixed_text(&mut body[..8], manufacturer)?;
                fixed_text(&mut body[8..16], name)?;
                fixed_text(&mut body[0x2C..0x3C], manufacturer)?;
                fixed_text(&mut body[0xAC..0xBC], model)?;
                body[0x1D] = 1;

                self.frame(0xFFFF, &[7, 0x2A, 0, 0xC0, 0, 0, 0, 0], &body)
            }
            PublicationBody::ClockStatus { mac_address } => {
                let mut body = vec![0; 120];
                body[..8].copy_from_slice(&[0, 3, 0, 3, 0, 0, 0, 0x9F]);
                body[12..18].copy_from_slice(mac_address);

                self.frame(0xFFFF, &[7, 0x2A, 0, 0x20, 0, 0, 0, 0], &body)
            }
            PublicationBody::InterfaceStatus { mac_address } => {
                let mut body = vec![0; 36];
                body[..2].copy_from_slice(&1u16.to_be_bytes());
                body[4..8].copy_from_slice(&1000u32.to_be_bytes());
                body[8..10].copy_from_slice(&1u16.to_be_bytes());
                body[10..16].copy_from_slice(mac_address);
                body[16..20].copy_from_slice(&self.source_ip.octets());
                body[20..24].copy_from_slice(&[255, 255, 255, 0]);
                body[24..28].copy_from_slice(&self.source_ip.octets());
                body[28..32].copy_from_slice(&self.source_ip.octets());

                self.frame(0xFFFF, &[7, 0x2A, 0, 0x11, 0, 0, 0, 0], &body)
            }
            PublicationBody::Audio {
                capability,
                current_value,
                supported_values,
            } => {
                if supported_values.len() > (usize::from(u16::MAX) - 48) / 4 {
                    return Err(NetaudioError::PacketTooLarge);
                }

                let opcode = match capability {
                    AudioCapabilityKind::Encoding => 0x82,
                    AudioCapabilityKind::SampleRate => 0x80,
                };
                let mut content = Vec::with_capacity(16 + supported_values.len() * 4);
                content.extend_from_slice(&0x0018u16.to_be_bytes());
                content.extend_from_slice(&(supported_values.len() as u16).to_be_bytes());
                content.extend_from_slice(&current_value.to_be_bytes());
                content.extend_from_slice(&0u32.to_be_bytes());
                content.extend_from_slice(&0x00020000u32.to_be_bytes());

                for value in supported_values {
                    content.extend_from_slice(&value.to_be_bytes());
                }

                self.frame(0xffff, &[7, 0x24, 0, opcode, 0, 0, 0, 0], &content)
            }
            PublicationBody::Heartbeat { tx_count, rx_count } => {
                let total = usize::from(*tx_count) + usize::from(*rx_count);
                let payload_length = (12 + total + 3) & !3;
                let record_length = 12 + payload_length;

                if 32 + 16 + record_length > usize::from(u16::MAX) {
                    return Err(NetaudioError::PacketTooLarge);
                }

                let mut content = Vec::with_capacity(16 + record_length);

                for word in [16, 0x8001, 4, 4, self.message_id, 0, 0, 0] {
                    content.extend_from_slice(&word.to_be_bytes());
                }

                for word in [
                    record_length as u16,
                    0x8002,
                    4,
                    payload_length as u16,
                    self.message_id,
                    0,
                    *tx_count,
                    0,
                    *rx_count,
                    0,
                    24,
                    0,
                ] {
                    content.extend_from_slice(&word.to_be_bytes());
                }

                content.resize(40 + total, 0xff);
                content.resize(16 + record_length, 0);
                self.frame(0xfffe, &[0, 8, 0, 1, 0x10, 0, 0, 0], &content)
            }
        }
    }

    fn frame(
        &self,
        protocol_id: u16,
        opcode: &[u8; 8],
        content: &[u8],
    ) -> Result<Vec<u8>, NetaudioError> {
        if !matches!(protocol_id, 0xffff | 0xfffe) {
            return Err(NetaudioError::UnsupportedProtocolOperation);
        }

        if content.len() > usize::from(u16::MAX) - 32 {
            return Err(NetaudioError::PacketTooLarge);
        }

        let mut body = Vec::with_capacity(26 + content.len());
        body.extend_from_slice(&[0; 4]);
        body.extend_from_slice(&self.source_ip.octets());
        body.extend_from_slice(&[0; 2]);
        body.extend_from_slice(b"Audinate");
        body.extend_from_slice(opcode);
        body.extend_from_slice(content);

        ConmonHeader {
            message_id: self.message_id,
            protocol_id,
        }
        .packet(&body)
    }
}
