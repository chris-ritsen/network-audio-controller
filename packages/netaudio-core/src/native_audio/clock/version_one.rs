use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

use super::{Error, Timestamp};

pub const EVENT_PORT: u16 = 319;
pub const GENERAL_PORT: u16 = 320;
pub const DEFAULT_DOMAIN: [u8; 16] = *b"_DFLT\0\0\0\0\0\0\0\0\0\0\0";

pub const ETHERNET_TECHNOLOGY: u8 = 1;
pub const ASSISTED_FLAG: u8 = 0x08;
pub const UNSYNCHRONIZED_FLAG: u8 = 0x40;
const PROTOCOL_VERSION: u16 = 1;
const DOMAIN_BYTES: usize = 16;
const IDENTITY_BYTES: usize = 6;
const CLOCK_IDENTIFIER_BYTES: usize = 4;
const HEADER_BYTES: usize = 40;
const SYNC_BYTES: usize = 124;
const FOLLOW_UP_BYTES: usize = 52;
const DELAY_RESPONSE_BYTES: usize = 60;
const SYNC_CONTROL: u8 = 0;
const DELAY_REQUEST_CONTROL: u8 = 1;
const FOLLOW_UP_CONTROL: u8 = 2;
const DELAY_RESPONSE_CONTROL: u8 = 3;
const EVENT_CATEGORY: u8 = 1;
const GENERAL_CATEGORY: u8 = 2;

mod layout {
    pub const VERSION: usize = 0;
    pub const NETWORK_VERSION: usize = 2;
    pub const DOMAIN: usize = 4;
    pub const MESSAGE_CATEGORY: usize = 20;
    pub const SOURCE_TECHNOLOGY: usize = 21;
    pub const SOURCE_IDENTIFIER: usize = 22;
    pub const SOURCE_PORT: usize = 28;
    pub const SEQUENCE: usize = 30;
    pub const CONTROL: usize = 32;
    pub const FLAGS: usize = 35;
    pub const ORIGIN: usize = 40;
    pub const EPOCH: usize = 48;
    pub const UNIVERSAL_TIME_OFFSET: usize = 50;
    pub const GRANDMASTER_TECHNOLOGY: usize = 53;
    pub const GRANDMASTER_IDENTIFIER: usize = 54;
    pub const GRANDMASTER_PORT: usize = 60;
    pub const GRANDMASTER_SEQUENCE: usize = 62;
    pub const GRANDMASTER_STRATUM: usize = 67;
    pub const GRANDMASTER_CLOCK_IDENTIFIER: usize = 68;
    pub const GRANDMASTER_VARIANCE: usize = 74;
    pub const PREFERRED: usize = 77;
    pub const BOUNDARY: usize = 79;
    pub const SYNCHRONIZATION_INTERVAL: usize = 83;
    pub const LOCAL_VARIANCE: usize = 86;
    pub const STEPS_REMOVED: usize = 90;
    pub const STRATUM: usize = 95;
    pub const CLOCK_IDENTIFIER: usize = 96;
    pub const PARENT_TECHNOLOGY: usize = 101;
    pub const PARENT_IDENTIFIER: usize = 102;
    pub const PARENT_PORT: usize = 110;
    pub const ESTIMATED_VARIANCE: usize = 114;
    pub const ESTIMATED_DRIFT: usize = 116;
    pub const UNIVERSAL_TIME_REASONABLE: usize = 123;
    pub const ASSOCIATED_SEQUENCE: usize = 42;
    pub const PRECISE_ORIGIN: usize = 44;
    pub const REQUESTER_TECHNOLOGY: usize = 49;
    pub const REQUESTER_IDENTIFIER: usize = 50;
    pub const REQUESTER_PORT: usize = 56;
    pub const REQUEST_SEQUENCE: usize = 58;
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct PortIdentity {
    pub communication_technology: u8,
    pub identifier: [u8; 6],
    pub port: u16,
}

impl PortIdentity {
    pub fn validate(self) -> Result<(), Error> {
        if self.communication_technology != ETHERNET_TECHNOLOGY {
            return Err(Error::UnsupportedTechnology);
        }

        Ok(())
    }

    pub fn projected_identifier(self) -> [u8; 8] {
        [
            self.identifier[0],
            self.identifier[1],
            self.identifier[2],
            0xff,
            0xfe,
            self.identifier[3],
            self.identifier[4],
            self.identifier[5],
        ]
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EpochContext {
    pub number: u16,
    pub universal_time_offset: i16,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    pub grandmaster: PortIdentity,
    pub grandmaster_sequence: u16,
    pub grandmaster_stratum: u8,
    pub grandmaster_clock_identifier: [u8; 4],
    pub grandmaster_variance: i16,
    pub preferred: u8,
    pub boundary: u8,
    pub synchronization_interval: i8,
    pub local_variance: i16,
    pub steps_removed: u16,
    pub stratum: u8,
    pub clock_identifier: [u8; 4],
    pub parent: PortIdentity,
    pub estimated_variance: u16,
    pub estimated_drift: u32,
    pub universal_time_reasonable: u8,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Body {
    Sync {
        origin: Timestamp,
        epoch: EpochContext,
        dataset: Dataset,
    },
    DelayRequest {
        origin: Timestamp,
        epoch: EpochContext,
    },
    FollowUp {
        associated_sequence: u16,
        origin: Timestamp,
    },
    DelayResponse {
        received: Timestamp,
        requester: PortIdentity,
        request_sequence: u16,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct Packet {
    pub domain: [u8; 16],
    pub message_category: u8,
    pub source: PortIdentity,
    pub sequence: u16,
    pub flags: u8,
    pub body: Body,
    pub raw: Vec<u8>,
}

fn word(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([bytes[offset], bytes[offset + 1]])
}

fn double_word(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn timestamp(bytes: &[u8], offset: usize) -> Result<Timestamp, Error> {
    Timestamp::new(double_word(bytes, offset), double_word(bytes, offset + 4))
}

fn identity(bytes: &[u8], technology: usize, identifier: usize, port: usize) -> PortIdentity {
    PortIdentity {
        communication_technology: bytes[technology],
        identifier: bytes[identifier..identifier + IDENTITY_BYTES]
            .try_into()
            .unwrap(),
        port: word(bytes, port),
    }
}

impl Packet {
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < HEADER_BYTES {
            return Err(Error::ShortPacket);
        }

        if word(bytes, layout::VERSION) != PROTOCOL_VERSION
            || word(bytes, layout::NETWORK_VERSION) != PROTOCOL_VERSION
        {
            return Err(Error::UnsupportedVersion);
        }

        let source = identity(
            bytes,
            layout::SOURCE_TECHNOLOGY,
            layout::SOURCE_IDENTIFIER,
            layout::SOURCE_PORT,
        );
        source.validate()?;
        let required = match bytes[layout::CONTROL] {
            SYNC_CONTROL | DELAY_REQUEST_CONTROL => SYNC_BYTES,
            FOLLOW_UP_CONTROL => FOLLOW_UP_BYTES,
            DELAY_RESPONSE_CONTROL => DELAY_RESPONSE_BYTES,
            value => return Err(Error::UnsupportedMessage(value)),
        };

        if bytes.len() < required {
            return Err(Error::ShortPacket);
        }

        let body = match bytes[layout::CONTROL] {
            SYNC_CONTROL => Body::Sync {
                origin: timestamp(bytes, layout::ORIGIN)?,
                epoch: EpochContext {
                    number: word(bytes, layout::EPOCH),
                    universal_time_offset: word(bytes, layout::UNIVERSAL_TIME_OFFSET) as i16,
                },
                dataset: Dataset {
                    grandmaster: identity(
                        bytes,
                        layout::GRANDMASTER_TECHNOLOGY,
                        layout::GRANDMASTER_IDENTIFIER,
                        layout::GRANDMASTER_PORT,
                    ),
                    grandmaster_sequence: word(bytes, layout::GRANDMASTER_SEQUENCE),
                    grandmaster_stratum: bytes[layout::GRANDMASTER_STRATUM],
                    grandmaster_clock_identifier: bytes[layout::GRANDMASTER_CLOCK_IDENTIFIER
                        ..layout::GRANDMASTER_CLOCK_IDENTIFIER + CLOCK_IDENTIFIER_BYTES]
                        .try_into()
                        .unwrap(),
                    grandmaster_variance: word(bytes, layout::GRANDMASTER_VARIANCE) as i16,
                    preferred: bytes[layout::PREFERRED],
                    boundary: bytes[layout::BOUNDARY],
                    synchronization_interval: bytes[layout::SYNCHRONIZATION_INTERVAL] as i8,
                    local_variance: word(bytes, layout::LOCAL_VARIANCE) as i16,
                    steps_removed: word(bytes, layout::STEPS_REMOVED),
                    stratum: bytes[layout::STRATUM],
                    clock_identifier: bytes[layout::CLOCK_IDENTIFIER
                        ..layout::CLOCK_IDENTIFIER + CLOCK_IDENTIFIER_BYTES]
                        .try_into()
                        .unwrap(),
                    parent: identity(
                        bytes,
                        layout::PARENT_TECHNOLOGY,
                        layout::PARENT_IDENTIFIER,
                        layout::PARENT_PORT,
                    ),
                    estimated_variance: word(bytes, layout::ESTIMATED_VARIANCE),
                    estimated_drift: double_word(bytes, layout::ESTIMATED_DRIFT),
                    universal_time_reasonable: bytes[layout::UNIVERSAL_TIME_REASONABLE],
                },
            },
            DELAY_REQUEST_CONTROL => Body::DelayRequest {
                origin: timestamp(bytes, layout::ORIGIN)?,
                epoch: EpochContext {
                    number: word(bytes, layout::EPOCH),
                    universal_time_offset: word(bytes, layout::UNIVERSAL_TIME_OFFSET) as i16,
                },
            },
            FOLLOW_UP_CONTROL => Body::FollowUp {
                associated_sequence: word(bytes, layout::ASSOCIATED_SEQUENCE),
                origin: timestamp(bytes, layout::PRECISE_ORIGIN)?,
            },
            DELAY_RESPONSE_CONTROL => Body::DelayResponse {
                received: timestamp(bytes, layout::ORIGIN)?,
                requester: identity(
                    bytes,
                    layout::REQUESTER_TECHNOLOGY,
                    layout::REQUESTER_IDENTIFIER,
                    layout::REQUESTER_PORT,
                ),
                request_sequence: word(bytes, layout::REQUEST_SEQUENCE),
            },
            _ => unreachable!(),
        };

        Ok(Self {
            domain: bytes[layout::DOMAIN..layout::DOMAIN + DOMAIN_BYTES]
                .try_into()
                .unwrap(),
            message_category: bytes[layout::MESSAGE_CATEGORY],
            source,
            sequence: word(bytes, layout::SEQUENCE),
            flags: bytes[layout::FLAGS],
            body,
            raw: bytes.to_vec(),
        })
    }
}

fn write_word(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn write_double_word(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn write_timestamp(bytes: &mut [u8], offset: usize, value: Timestamp) -> Result<(), Error> {
    value.total_nanoseconds()?;
    write_double_word(bytes, offset, value.seconds());
    write_double_word(bytes, offset + 4, value.nanoseconds());
    Ok(())
}

pub fn encode(
    domain: [u8; 16],
    source: PortIdentity,
    sequence: u16,
    flags: u8,
    body: &Body,
) -> Result<Vec<u8>, Error> {
    source.validate()?;
    if domain.iter().all(|byte| *byte == 0) {
        return Err(Error::InvalidDomain);
    }

    let (control, length, category) = match body {
        Body::Sync { .. } => (SYNC_CONTROL, SYNC_BYTES, EVENT_CATEGORY),
        Body::DelayRequest { .. } => (DELAY_REQUEST_CONTROL, SYNC_BYTES, EVENT_CATEGORY),
        Body::FollowUp { .. } => (FOLLOW_UP_CONTROL, FOLLOW_UP_BYTES, GENERAL_CATEGORY),
        Body::DelayResponse { .. } => (
            DELAY_RESPONSE_CONTROL,
            DELAY_RESPONSE_BYTES,
            GENERAL_CATEGORY,
        ),
    };

    let mut bytes = vec![0; length];
    write_word(&mut bytes, layout::VERSION, PROTOCOL_VERSION);
    write_word(&mut bytes, layout::NETWORK_VERSION, PROTOCOL_VERSION);
    bytes[layout::DOMAIN..layout::DOMAIN + DOMAIN_BYTES].copy_from_slice(&domain);
    bytes[layout::MESSAGE_CATEGORY] = category;
    bytes[layout::SOURCE_TECHNOLOGY] = source.communication_technology;
    bytes[layout::SOURCE_IDENTIFIER..layout::SOURCE_IDENTIFIER + IDENTITY_BYTES]
        .copy_from_slice(&source.identifier);
    write_word(&mut bytes, layout::SOURCE_PORT, source.port);
    write_word(&mut bytes, layout::SEQUENCE, sequence);
    bytes[layout::CONTROL] = control;
    bytes[layout::FLAGS] = flags;
    match body {
        Body::Sync {
            origin,
            epoch,
            dataset,
        } => {
            write_timestamp(&mut bytes, layout::ORIGIN, *origin)?;
            write_word(&mut bytes, layout::EPOCH, epoch.number);
            write_word(
                &mut bytes,
                layout::UNIVERSAL_TIME_OFFSET,
                epoch.universal_time_offset as u16,
            );
            bytes[layout::GRANDMASTER_TECHNOLOGY] = dataset.grandmaster.communication_technology;
            bytes[layout::GRANDMASTER_IDENTIFIER..layout::GRANDMASTER_IDENTIFIER + IDENTITY_BYTES]
                .copy_from_slice(&dataset.grandmaster.identifier);
            write_word(
                &mut bytes,
                layout::GRANDMASTER_PORT,
                dataset.grandmaster.port,
            );
            write_word(
                &mut bytes,
                layout::GRANDMASTER_SEQUENCE,
                dataset.grandmaster_sequence,
            );
            bytes[layout::GRANDMASTER_STRATUM] = dataset.grandmaster_stratum;
            bytes[layout::GRANDMASTER_CLOCK_IDENTIFIER
                ..layout::GRANDMASTER_CLOCK_IDENTIFIER + CLOCK_IDENTIFIER_BYTES]
                .copy_from_slice(&dataset.grandmaster_clock_identifier);
            write_word(
                &mut bytes,
                layout::GRANDMASTER_VARIANCE,
                dataset.grandmaster_variance as u16,
            );
            bytes[layout::PREFERRED] = dataset.preferred;
            bytes[layout::BOUNDARY] = dataset.boundary;
            bytes[layout::SYNCHRONIZATION_INTERVAL] = dataset.synchronization_interval as u8;
            write_word(
                &mut bytes,
                layout::LOCAL_VARIANCE,
                dataset.local_variance as u16,
            );
            write_word(&mut bytes, layout::STEPS_REMOVED, dataset.steps_removed);
            bytes[layout::STRATUM] = dataset.stratum;
            bytes[layout::CLOCK_IDENTIFIER..layout::CLOCK_IDENTIFIER + CLOCK_IDENTIFIER_BYTES]
                .copy_from_slice(&dataset.clock_identifier);
            bytes[layout::PARENT_TECHNOLOGY] = dataset.parent.communication_technology;
            bytes[layout::PARENT_IDENTIFIER..layout::PARENT_IDENTIFIER + IDENTITY_BYTES]
                .copy_from_slice(&dataset.parent.identifier);
            write_word(&mut bytes, layout::PARENT_PORT, dataset.parent.port);
            write_word(
                &mut bytes,
                layout::ESTIMATED_VARIANCE,
                dataset.estimated_variance,
            );
            write_double_word(&mut bytes, layout::ESTIMATED_DRIFT, dataset.estimated_drift);
            bytes[layout::UNIVERSAL_TIME_REASONABLE] = dataset.universal_time_reasonable;
        }
        Body::DelayRequest { origin, epoch } => {
            write_timestamp(&mut bytes, layout::ORIGIN, *origin)?;
            write_word(&mut bytes, layout::EPOCH, epoch.number);
            write_word(
                &mut bytes,
                layout::UNIVERSAL_TIME_OFFSET,
                epoch.universal_time_offset as u16,
            );
        }
        Body::FollowUp {
            associated_sequence,
            origin,
        } => {
            write_word(
                &mut bytes,
                layout::ASSOCIATED_SEQUENCE,
                *associated_sequence,
            );
            write_timestamp(&mut bytes, layout::PRECISE_ORIGIN, *origin)?;
        }
        Body::DelayResponse {
            received,
            requester,
            request_sequence,
        } => {
            requester.validate()?;
            write_timestamp(&mut bytes, layout::ORIGIN, *received)?;
            bytes[layout::REQUESTER_TECHNOLOGY] = requester.communication_technology;
            bytes[layout::REQUESTER_IDENTIFIER..layout::REQUESTER_IDENTIFIER + IDENTITY_BYTES]
                .copy_from_slice(&requester.identifier);
            write_word(&mut bytes, layout::REQUESTER_PORT, requester.port);
            write_word(&mut bytes, layout::REQUEST_SEQUENCE, *request_sequence);
        }
    }

    Ok(bytes)
}

pub fn multicast_group(domain: [u8; 16]) -> Result<Ipv4Addr, Error> {
    if domain.iter().all(|byte| *byte == 0) {
        return Err(Error::InvalidDomain);
    }

    for (index, name) in [
        DEFAULT_DOMAIN,
        *b"_ALT1\0\0\0\0\0\0\0\0\0\0\0",
        *b"_ALT2\0\0\0\0\0\0\0\0\0\0\0",
        *b"_ALT3\0\0\0\0\0\0\0\0\0\0\0",
    ]
    .iter()
    .enumerate()
    {
        if domain == *name {
            return Ok(Ipv4Addr::new(224, 0, 1, 129 + index as u8));
        }
    }

    let mut checksum = u32::MAX;
    for byte in domain {
        checksum ^= u32::from(byte);
        for _ in 0..8 {
            checksum = (checksum >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(checksum & 1));
        }
    }

    Ok(Ipv4Addr::new(224, 0, 1, 130 + ((!checksum) % 3) as u8))
}
