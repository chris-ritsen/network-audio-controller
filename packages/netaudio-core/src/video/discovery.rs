use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::num::NonZeroU16;
use std::str::FromStr;

use serde::Serialize;

use super::{source_control, Error};

pub use crate::protocol::SERVICE_VIDEO as CHANNEL_SERVICE;
pub const FLOW_SERVICE: &str = "_dantevid-flow._udp.local.";
pub const FLOW_SERVICE_PORT: u16 = 4322;
pub const H264_SUBTYPE: u32 = 130;
const CHANNEL_TEXT_REVISION: u16 = 2;
const FLOW_TEXT_REVISION: u16 = 1;
const MAXIMUM_TEXT_BYTES: usize = u16::MAX as usize;
const MAXIMUM_DESTINATIONS: usize = 2;
const CHANNEL_KEYS: &[&[u8]] = &[
    b"txtvers",
    b"dbcp1",
    b"id",
    b"nchan",
    b"codec",
    b"tpa",
    b"hdcp",
    b"hdcp_port",
];
const FLOW_KEYS: &[&[u8]] = &[
    b"txtvers",
    b"id",
    b"nchan",
    b"codec",
    b"latency_ns",
    b"a.0",
    b"p.0",
    b"a.1",
    b"p.1",
    b"hdcp",
    b"hdcp_port",
];
const ASSOCIATION_PREFIX: &[u8] = b"b.";
const H264_PREFIX: &[u8] = b"H264:";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

fn entries(bytes: &[u8]) -> Result<Vec<Entry>, Error> {
    if bytes.is_empty() || bytes.len() > MAXIMUM_TEXT_BYTES {
        return Err(Error(
            "video TXT record is empty or exceeds the DNS record extent",
        ));
    }

    let mut remaining = bytes;
    let mut result = Vec::new();

    while let Some((&length, rest)) = remaining.split_first() {
        let value = rest
            .get(..usize::from(length))
            .ok_or(Error("video TXT string is truncated"))?;
        let separator = value.iter().position(|byte| *byte == b'=');
        let key = &value[..separator.unwrap_or(value.len())];

        if key.is_empty()
            || !key
                .iter()
                .all(|byte| (b' '..=b'~').contains(byte) && *byte != b'=')
        {
            return Err(Error("video TXT key is not complete printable ASCII"));
        }

        result.push(Entry {
            key: key.to_vec(),
            value: separator.map(|position| value[position + 1..].to_vec()),
        });
        remaining = &rest[usize::from(length)..];
    }

    Ok(result)
}

struct Fields<'a> {
    entries: &'a [Entry],
}

impl<'a> Fields<'a> {
    fn new(entries: &'a [Entry], recognized: &[&[u8]]) -> Result<Self, Error> {
        let mut seen = BTreeSet::new();

        for entry in entries {
            if (recognized.contains(&entry.key.as_slice())
                || entry.key.starts_with(ASSOCIATION_PREFIX))
                && !seen.insert(&entry.key)
            {
                return Err(Error("video TXT contains a duplicate recognized field"));
            }
        }

        Ok(Self { entries })
    }

    fn entry(&self, key: &[u8]) -> Option<&'a Entry> {
        self.entries.iter().find(|entry| entry.key == key)
    }

    fn optional(&self, key: &[u8]) -> Result<Option<&'a [u8]>, Error> {
        self.entry(key)
            .map(|entry| {
                entry
                    .value
                    .as_deref()
                    .ok_or(Error("video TXT field has no value"))
            })
            .transpose()
    }

    fn required(&self, key: &[u8]) -> Result<&'a [u8], Error> {
        self.optional(key)?
            .ok_or(Error("video TXT lacks a required field"))
    }

    fn number<T: FromStr>(&self, key: &[u8]) -> Result<T, Error> {
        decimal(self.required(key)?)
    }
}

fn decimal<T: FromStr>(bytes: &[u8]) -> Result<T, Error> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(Error("video TXT number must be complete unsigned decimal"));
    }

    std::str::from_utf8(bytes)
        .map_err(|_| Error("video TXT number is not ASCII"))?
        .parse()
        .map_err(|_| Error("video TXT number exceeds its field range"))
}

fn address(bytes: &[u8]) -> Result<Ipv4Addr, Error> {
    std::str::from_utf8(bytes)
        .map_err(|_| Error("video TXT address is not ASCII"))?
        .parse()
        .map_err(|_| Error("video TXT address is not complete dotted IPv4"))
}

fn positive(value: u16) -> Result<NonZeroU16, Error> {
    NonZeroU16::new(value).ok_or(Error(
        "video TXT identity and channel count must be positive",
    ))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Association {
    pub flow_label: Vec<u8>,
    pub one_based_slot: NonZeroU16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Channel {
    pub source_service_channel: NonZeroU16,
    pub channel_count: NonZeroU16,
    pub source_control_revision: u16,
    pub codec_identifier: u32,
    pub transport_peer_metadata: Option<Ipv4Addr>,
    pub association: Option<Association>,
    pub protection_metadata_present: bool,
    pub entries: Vec<Entry>,
}

impl Channel {
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let entries = entries(bytes)?;
        let fields = Fields::new(&entries, CHANNEL_KEYS)?;

        if fields.number::<u16>(b"txtvers")? != CHANNEL_TEXT_REVISION
            || fields.required(b"dbcp1")? != b"0x1200"
        {
            return Err(Error(
                "unsupported video channel TXT or source-control revision",
            ));
        }

        let codec = fields
            .required(b"codec")?
            .strip_prefix(H264_PREFIX)
            .ok_or(Error(
                "video channel does not declare the scoped H.264 codec",
            ))?;
        let associations = entries
            .iter()
            .filter(|entry| entry.key.starts_with(ASSOCIATION_PREFIX))
            .collect::<Vec<_>>();

        if associations.len() > 1 {
            return Err(Error(
                "video channel exceeds the selected single-flow association profile",
            ));
        }

        let association = associations
            .first()
            .map(|entry| {
                let flow_label = &entry.key[ASSOCIATION_PREFIX.len()..];
                let slot: u16 = decimal(
                    entry
                        .value
                        .as_deref()
                        .ok_or(Error("video flow association has no slot value"))?,
                )?;

                if flow_label.is_empty() || slot != 1 {
                    return Err(Error(
                        "video flow association requires a complete name and slot one",
                    ));
                }

                Ok(Association {
                    flow_label: flow_label.to_vec(),
                    one_based_slot: positive(slot)?,
                })
            })
            .transpose()?;

        Ok(Self {
            source_service_channel: positive(fields.number(b"id")?)?,
            channel_count: positive(fields.number(b"nchan")?)?,
            source_control_revision: source_control::REVISION,
            codec_identifier: decimal(codec)?,
            transport_peer_metadata: fields.optional(b"tpa")?.map(address).transpose()?,
            association,
            protection_metadata_present: fields.entry(b"hdcp").is_some()
                || fields.entry(b"hdcp_port").is_some(),
            entries,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Flow {
    pub identifier: NonZeroU16,
    pub channel_count: NonZeroU16,
    pub latency_nanoseconds: u64,
    pub destinations: [Option<SocketAddrV4>; MAXIMUM_DESTINATIONS],
    pub protection_value: Option<u32>,
    pub protection_port_present: bool,
    pub entries: Vec<Entry>,
}

impl Flow {
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let entries = entries(bytes)?;
        let fields = Fields::new(&entries, FLOW_KEYS)?;

        if fields.number::<u16>(b"txtvers")? != FLOW_TEXT_REVISION
            || fields.number::<u32>(b"codec")? != H264_SUBTYPE
            || fields.number::<u16>(b"nchan")? != 1
        {
            return Err(Error(
                "unsupported video flow revision, codec or slot count",
            ));
        }

        let mut destinations = [None; MAXIMUM_DESTINATIONS];

        for (index, destination) in destinations.iter_mut().enumerate() {
            let address_key = format!("a.{index}");
            let port_key = format!("p.{index}");
            let pair = (
                fields.optional(address_key.as_bytes())?,
                fields.optional(port_key.as_bytes())?,
            );

            match pair {
                (None, None) => {}
                (Some(address_bytes), Some(port_bytes)) => {
                    let address = address(address_bytes)?;
                    let port: u16 = decimal(port_bytes)?;

                    if !address.is_multicast() || port == 0 {
                        return Err(Error(
                            "named video flow requires a multicast address and nonzero port",
                        ));
                    }

                    *destination = Some(SocketAddrV4::new(address, port));
                }
                _ => {
                    return Err(Error(
                        "named video flow has an incomplete address and port pair",
                    ))
                }
            }
        }

        if destinations.iter().all(Option::is_none) {
            return Err(Error("named video flow has no complete media destination"));
        }

        Ok(Self {
            identifier: positive(fields.number(b"id")?)?,
            channel_count: positive(fields.number(b"nchan")?)?,
            latency_nanoseconds: fields.number(b"latency_ns")?,
            destinations,
            protection_value: fields.optional(b"hdcp")?.map(decimal).transpose()?,
            protection_port_present: fields.entry(b"hdcp_port").is_some(),
            entries,
        })
    }
}

fn text(entries: &[Entry]) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();

    for entry in entries {
        let length = entry.key.len() + entry.value.as_ref().map_or(0, |value| value.len() + 1);
        bytes.push(
            u8::try_from(length).map_err(|_| Error("video TXT field exceeds one DNS string"))?,
        );
        bytes.extend_from_slice(&entry.key);

        if let Some(value) = &entry.value {
            bytes.push(b'=');
            bytes.extend_from_slice(value);
        }
    }

    if bytes.len() > MAXIMUM_TEXT_BYTES {
        return Err(Error("video TXT record exceeds the DNS record extent"));
    }

    Ok(bytes)
}

fn field(key: &[u8], value: impl ToString) -> Entry {
    Entry {
        key: key.to_vec(),
        value: Some(value.to_string().into_bytes()),
    }
}

pub struct ChannelPublication<'a> {
    pub source_service_channel: NonZeroU16,
    pub codec_identifier: u32,
    pub transport_peer_metadata: Option<Ipv4Addr>,
    pub multicast_flow_label: Option<&'a [u8]>,
}

impl ChannelPublication<'_> {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut entries = vec![
            field(b"txtvers", CHANNEL_TEXT_REVISION),
            field(b"dbcp1", format!("{:#06x}", source_control::REVISION)),
            field(b"id", self.source_service_channel),
            field(b"nchan", 1),
            field(b"codec", format!("H264:{}", self.codec_identifier)),
        ];

        if let Some(address) = self.transport_peer_metadata {
            entries.push(field(b"tpa", address));
        }

        if let Some(label) = self.multicast_flow_label {
            let mut key = ASSOCIATION_PREFIX.to_vec();
            key.extend_from_slice(label);
            entries.push(field(&key, 1));
        }

        let bytes = text(&entries)?;
        Channel::decode(&bytes)?;
        Ok(bytes)
    }
}

pub struct FlowPublication {
    pub identifier: NonZeroU16,
    pub latency_nanoseconds: u64,
    pub destination: SocketAddrV4,
}

impl FlowPublication {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let bytes = text(&[
            field(b"txtvers", FLOW_TEXT_REVISION),
            field(b"id", self.identifier),
            field(b"nchan", 1),
            field(b"codec", H264_SUBTYPE),
            field(b"latency_ns", self.latency_nanoseconds),
            field(b"a.0", self.destination.ip()),
            field(b"p.0", self.destination.port()),
        ])?;
        Flow::decode(&bytes)?;
        Ok(bytes)
    }
}
