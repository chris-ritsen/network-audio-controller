use std::collections::BTreeSet;

use serde::Serialize;
use crate::protocol::NetaudioProtocol;

use super::domain::NamedDomain;
use super::endpoint_control::{
    ARC_REVISION, RECEIVE_LATENCY_NANOSECONDS, SAMPLE_BITS, SAMPLE_RATE,
};
use super::source_control::{Error, REFERENCE_REVISION};

const TEXT_REVISION: u16 = 2;
const MAXIMUM_ENCODINGS: usize = 8;
const MAXIMUM_TEXT_BYTES: usize = 1500;
pub const SUPPORTED_PCM_BYTES: u16 = 3;
pub const SUPPORTED_PCM_BITMAP: u16 = 4;
const MAXIMUM_VERSION_TEXT_BYTES: usize = 63;
const MAXIMUM_ROUTING_VERSION_COMPONENT: u16 = 15;
const ROUTING_MAJOR_SHIFT: u32 = 12;
const ROUTING_MINOR_SHIFT: u32 = 8;
const ROUTING_CURRENT_VERSION_KEY: &[u8] = b"arcp_vers";
const ROUTING_MINIMUM_VERSION_KEY: &[u8] = b"arcp_min";
const ROUTER_VERSION_KEY: &[u8] = b"router_vers";
const SOURCE_FIELDS: &[&[u8]] = &[
    b"txtvers",
    b"id",
    b"nchan",
    b"dbcp1",
    b"dbcp",
    b"rate",
    b"en",
    b"enc",
    b"pcm",
    b"fpp",
    b"latency_ns",
    b"at2",
    b"at3",
    b"subdomain",
    b"ptp2",
    b"mc",
    b"nred",
    b"sec",
    b"ctr256",
    b"st",
    b"gid",
    b"rid",
];
const ROUTING_FIELDS: &[&[u8]] = &[
    ROUTING_CURRENT_VERSION_KEY,
    ROUTING_MINIMUM_VERSION_KEY,
    ROUTER_VERSION_KEY,
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TextEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Packetization {
    pub raw: String,
    pub first: u16,
    pub second: u16,
    pub third: u16,
    pub second_present: bool,
    pub third_present: bool,
}

impl Packetization {
    pub fn decode(value: &str) -> Result<Self, Error> {
        let (first, remaining) = match value.split_once(',') {
            Some((first, remaining)) => (decimal(first.as_bytes())?, Some(remaining)),
            None => (decimal(value.as_bytes())?, None),
        };
        let (second, third, third_present) = match remaining {
            Some(remaining) => match remaining.split_once('-') {
                Some((second, third)) => (
                    decimal(second.as_bytes())?,
                    decimal(third.as_bytes())?,
                    true,
                ),
                None => (decimal(remaining.as_bytes())?, 0, false),
            },
            None => (0, 0, false),
        };

        Ok(Self {
            raw: value.to_owned(),
            first,
            second: if second == 0 { first } else { second },
            third,
            second_present: remaining.is_some(),
            third_present,
        })
    }

    pub fn request(&self, receiver_default: u16) -> Result<u16, Error> {
        if self.first == 0 || (self.third != 0 && self.third < self.first) {
            return Err(Error(
                "source packetization is zero or has a contradictory ceiling",
            ));
        }

        let configured = if receiver_default < self.first {
            self.first
        } else if self.third != 0 {
            receiver_default.min(self.third)
        } else {
            receiver_default
        };

        Ok(configured.max(self.first))
    }
}

fn decimal<T: std::str::FromStr>(value: &[u8]) -> Result<T, Error> {
    if value.is_empty()
        || (value.len() > 1 && value[0] == b'0')
        || !value.iter().all(u8::is_ascii_digit)
    {
        return Err(Error(
            "source numeric field is not complete canonical unsigned decimal",
        ));
    }

    std::str::from_utf8(value)
        .map_err(|_| Error("source numeric field is not ASCII"))?
        .parse()
        .map_err(|_| Error("source numeric field exceeds its supported range"))
}

pub fn text_entries(bytes: &[u8]) -> Result<Vec<TextEntry>, Error> {
    if bytes.is_empty() || bytes.len() > MAXIMUM_TEXT_BYTES {
        return Err(Error(
            "source TXT record is empty or exceeds the selected profile budget",
        ));
    }

    let mut entries = Vec::new();
    let mut offset = 0;

    while offset < bytes.len() {
        let length = usize::from(bytes[offset]);
        offset += 1;
        let entry = bytes
            .get(offset..offset + length)
            .ok_or(Error("source TXT entry exceeds its complete record"))?;

        if entry.is_empty() || entry.contains(&0) {
            return Err(Error(
                "source TXT entry is empty or contains an embedded NUL",
            ));
        }

        let (key, value) = match entry.iter().position(|byte| *byte == b'=') {
            Some(index) => (&entry[..index], Some(entry[index + 1..].to_vec())),
            None => (entry, None),
        };

        if key.is_empty()
            || !key
                .iter()
                .all(|byte| byte.is_ascii_graphic() && *byte != b'=')
        {
            return Err(Error(
                "source TXT key is not a complete printable ASCII key",
            ));
        }

        entries.push(TextEntry {
            key: key.to_vec(),
            value,
        });
        offset += length;
    }

    Ok(entries)
}

struct Fields<'a> {
    entries: &'a [TextEntry],
}

impl<'a> Fields<'a> {
    fn new(entries: &'a [TextEntry], recognized: &[&[u8]]) -> Result<Self, Error> {
        let mut seen = BTreeSet::new();

        for entry in entries {
            if recognized.contains(&entry.key.as_slice()) && !seen.insert(&entry.key) {
                return Err(Error("source TXT contains duplicate recognized fields"));
            }
        }

        Ok(Self { entries })
    }

    fn entry(&self, key: &[u8]) -> Option<&'a TextEntry> {
        self.entries.iter().find(|entry| entry.key == key)
    }

    fn optional(&self, key: &[u8]) -> Result<Option<&'a [u8]>, Error> {
        self.entry(key)
            .map(|entry| {
                entry
                    .value
                    .as_deref()
                    .ok_or(Error("source scalar TXT field has no value"))
            })
            .transpose()
    }

    fn required(&self, key: &[u8]) -> Result<&'a [u8], Error> {
        self.optional(key)?
            .ok_or(Error("source TXT lacks a required explicit field"))
    }

    fn number<T: std::str::FromStr>(&self, key: &[u8]) -> Result<T, Error> {
        decimal(self.required(key)?)
    }
}

fn version(value: &[u8]) -> Result<[u16; 3], Error> {
    if value.is_empty() || value.len() > MAXIMUM_VERSION_TEXT_BYTES {
        return Err(Error("routing version exceeds its complete text extent"));
    }

    let mut components = value.split(|byte| *byte == b'.');
    let mut result = [0; 3];

    for component in &mut result {
        let digits = components
            .next()
            .ok_or(Error("routing version is not a complete triple"))?;

        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return Err(Error("routing version component is not unsigned decimal"));
        }

        *component = std::str::from_utf8(digits)
            .map_err(|_| Error("routing version component is not ASCII"))?
            .parse()
            .map_err(|_| Error("routing version component exceeds u16"))?;
    }

    if components.next().is_some() {
        return Err(Error("routing version contains excess components"));
    }

    Ok(result)
}

fn routing_revision(value: &[u8]) -> Result<u16, Error> {
    let [major, minor, patch] = version(value)?;

    if major > MAXIMUM_ROUTING_VERSION_COMPONENT
        || minor > MAXIMUM_ROUTING_VERSION_COMPONENT
        || patch > u16::from(u8::MAX)
    {
        return Err(Error(
            "routing version is outside its lossless header representation",
        ));
    }

    Ok((major << ROUTING_MAJOR_SHIFT) | (minor << ROUTING_MINOR_SHIFT) | patch)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Routing {
    pub current_revision: u16,
    pub minimum_revision: u16,
    pub router_version: [u16; 3],
    pub entries: Vec<TextEntry>,
}

impl Routing {
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let entries = text_entries(bytes)?;
        let fields = Fields::new(&entries, ROUTING_FIELDS)?;
        let current_revision = routing_revision(fields.required(ROUTING_CURRENT_VERSION_KEY)?)?;
        let minimum_revision = routing_revision(fields.required(ROUTING_MINIMUM_VERSION_KEY)?)?;
        let router_version = version(fields.required(ROUTER_VERSION_KEY)?)?;

        if router_version[0] == 0
            || router_version[0] > u16::from(u8::MAX)
            || router_version[1] > u16::from(u8::MAX)
        {
            return Err(Error(
                "router version is outside the supported complete tuple",
            ));
        }

        if minimum_revision > current_revision {
            return Err(Error(
                "routing minimum exceeds the current advertised revision",
            ));
        }

        if ![ARC_REVISION, NetaudioProtocol::Arc2729 as u16].contains(&current_revision) {
            return Err(Error(
                "source routing service has no implemented acquisition profile",
            ));
        }

        Ok(Self {
            current_revision,
            minimum_revision,
            router_version,
            entries,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Source {
    pub channel_identifier: u16,
    pub slot_capacity: u16,
    pub interface_count: u16,
    pub packetization: Packetization,
    pub latency_nanoseconds: Option<u32>,
    pub named_domain: NamedDomain,
    pub entries: Vec<TextEntry>,
}

impl Source {
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let entries = text_entries(bytes)?;
        let fields = Fields::new(&entries, SOURCE_FIELDS)?;

        if fields.number::<u16>(b"txtvers")? != TEXT_REVISION {
            return Err(Error(
                "source TXT revision is outside the implemented profile",
            ));
        }

        let revision = std::str::from_utf8(fields.required(b"dbcp1")?)
            .map_err(|_| Error("source control revision is not ASCII"))?;
        let revision = if let Some(hexadecimal) = revision.strip_prefix("0x") {
            if hexadecimal.len() != 4 || !hexadecimal.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(Error(
                    "source control revision is not a complete hexadecimal u16",
                ));
            }

            u16::from_str_radix(hexadecimal, 16)
                .map_err(|_| Error("source control revision exceeds u16"))?
        } else {
            decimal(revision.as_bytes())?
        };

        if revision != REFERENCE_REVISION {
            return Err(Error(
                "source control revision has no implemented allocation contract",
            ));
        }

        if fields.entry(b"at2").is_none() && fields.entry(b"at3").is_some() {
            return Err(Error("source does not support native-v2 transport"));
        }

        for key in [b"mc".as_slice(), b"sec", b"ctr256", b"gid", b"rid"] {
            if fields.entry(key).is_some() {
                return Err(Error(
                    "source requires an unimplemented multicast, protected or projected context",
                ));
            }
        }

        let interface_count = fields
            .optional(b"nred")?
            .map(decimal::<u16>)
            .transpose()?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(Error("source interface count exceeds its representation"))?;

        if fields
                .optional(b"st")?
                .map(decimal::<u16>)
                .transpose()?
                .is_some_and(|value| value != 0)
        {
            return Err(Error(
                "source requires an unimplemented source-status context",
            ));
        }

        if fields.number::<u32>(b"rate")? != SAMPLE_RATE {
            return Err(Error(
                "source rate differs from the selected local PCM rate",
            ));
        }

        let preferred = fields.number::<u16>(b"en")?;
        let encodings = fields
            .required(b"enc")?
            .split(|byte| *byte == b' ')
            .map(decimal::<u16>)
            .collect::<Result<Vec<_>, _>>()?;
        let unique: BTreeSet<_> = encodings.iter().copied().collect();

        if encodings.len() > MAXIMUM_ENCODINGS
            || unique.len() != encodings.len()
            || !unique.contains(&preferred)
            || !unique.contains(&SAMPLE_BITS)
            || unique
                .iter()
                .any(|encoding| !matches!(encoding, 16 | 24 | 32))
        {
            return Err(Error(
                "source encoding set is ambiguous or has no supported PCM intersection",
            ));
        }

        if let Some(compact) = fields.optional(b"pcm")? {
            let mut members = compact.split(|byte| *byte == b' ');
            let width = decimal::<u16>(members.next().unwrap_or_default())?;
            let bitmap = members
                .next()
                .ok_or(Error("source PCM bitmap is missing"))?;

            if members.next().is_some()
                || bitmap.is_empty()
                || !bitmap.iter().all(u8::is_ascii_hexdigit)
            {
                return Err(Error(
                    "source PCM capability is not a complete width and bitmap",
                ));
            }

            let bitmap = u16::from_str_radix(
                std::str::from_utf8(bitmap).map_err(|_| Error("source PCM bitmap is not ASCII"))?,
                16,
            )
            .map_err(|_| Error("source PCM bitmap exceeds its supported range"))?;

            if width != SUPPORTED_PCM_BYTES
                || bitmap != SUPPORTED_PCM_BITMAP
                || preferred != SAMPLE_BITS
                || unique != BTreeSet::from([SAMPLE_BITS])
            {
                return Err(Error(
                    "source compact PCM capabilities differ from the supported explicit PCM24 profile",
                ));
            }
        }

        let named_domain = match fields.optional(b"subdomain")? {
            Some(value) => NamedDomain::from_text(value)?,
            None => NamedDomain::DEFAULT,
        };

        let channel_identifier = fields.number::<u16>(b"id")?;
        let slot_capacity = fields.number::<u16>(b"nchan")?;

        if channel_identifier == 0 || slot_capacity == 0 {
            return Err(Error(
                "source channel identifier and flow slot capacity must be positive",
            ));
        }

        let packetization = Packetization::decode(
            std::str::from_utf8(fields.required(b"fpp")?)
                .map_err(|_| Error("source packetization is not ASCII"))?,
        )?;
        packetization.request(super::endpoint_control::FRAMES_PER_PACKET)?;
        let latency_nanoseconds = fields
            .optional(b"latency_ns")?
            .map(decimal::<u32>)
            .transpose()?;

        if latency_nanoseconds.is_some_and(|value| value > RECEIVE_LATENCY_NANOSECONDS) {
            return Err(Error(
                "source latency requirement exceeds local receive capacity",
            ));
        }

        Ok(Self {
            channel_identifier,
            slot_capacity,
            interface_count,
            packetization,
            latency_nanoseconds,
            named_domain,
            entries,
        })
    }
}
