use crate::bytes::{hexadecimal, read_u16, read_u32};
use crate::conmon_identity::Identity;
use serde::Serialize;
use std::collections::BTreeMap;

pub const REGISTRATION: u16 = 0x2009;
pub const DEREGISTRATION: u16 = 0x200A;
pub const UPDATE: u16 = 0x200B;
pub const MONITORING_SERVICE: &str = "_netaudio-cmc._udp";
pub const RECOGNIZED_SERVICES: [&str; 3] = [
    "_netaudio-arc._udp",
    "_netaudio-cmc._udp",
    "_netaudio-via._tcp",
];

const NORMAL_MESSAGE: u32 = 2;
const PACKET_HEADER_BLOCK: u16 = 1;
const REQUEST_FORM: u8 = 2;
const RESPONSE_FORM: u8 = 3;
const UPDATE_PAYLOAD_BYTES: usize = 24;
const UPDATE_TERMINAL: u16 = 0x0001;
const UPDATE_ENTRY: u16 = 0x0002;
const ENTRY_INFORMATION: u16 = 0x0001;
const ENTRY_SERVICES: u16 = 0x0002;
const DESTINATION_ADDRESS: u16 = 0x0840;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LookupKind {
    Deregistration,
    Registration,
    Update,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LookupMessage {
    pub destination_component: Option<u16>,
    pub kind: LookupKind,
    pub request_id: u16,
    pub response: bool,
    pub status: u16,
    pub update: Option<LookupUpdate>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LookupUpdate {
    pub domain_id: String,
    pub entry: Option<LookupEntry>,
    pub flags: u16,
    pub lookup_id: u32,
    pub terminal: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LookupEntry {
    pub flags: u16,
    pub information: Option<LookupInformation>,
    pub name: String,
    pub services: Option<Vec<LookupService>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LookupInformation {
    pub classification: Option<u8>,
    pub first_extension: Option<u32>,
    pub flags: u16,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub name: Option<String>,
    pub route: Option<LookupRoute>,
    pub second_extension: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LookupRoute {
    pub client: u32,
    pub component: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LookupService {
    pub extra: Option<u16>,
    pub flags: u16,
    pub name: Option<String>,
    pub reserved: u16,
    pub txt: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryChange {
    Observation,
    Unsupported,
    Withdrawal,
}

impl LookupEntry {
    pub fn change(&self) -> EntryChange {
        if self.flags == 0 && self.information.is_none() && self.services.is_none() {
            EntryChange::Withdrawal
        } else if self.information.is_some() {
            EntryChange::Observation
        } else {
            EntryChange::Unsupported
        }
    }
}

impl LookupService {
    pub fn advertised(&self) -> bool {
        self.txt.as_ref().is_some_and(|txt| !txt.is_empty())
    }

    pub fn properties(&self) -> Option<BTreeMap<String, Option<String>>> {
        let mut properties = BTreeMap::new();
        for element in txt_elements(self.txt.as_deref()?)? {
            if element.is_empty() {
                continue;
            }
            let text = std::str::from_utf8(element).ok()?;
            let (key, value) = match text.split_once('=') {
                Some((key, value)) => (key, Some(value.to_owned())),
                None => (text, None),
            };
            if key.is_empty() {
                return None;
            }
            properties.entry(key.to_ascii_lowercase()).or_insert(value);
        }
        Some(properties)
    }

    pub fn monitoring_identity(&self) -> Option<Identity> {
        (self.name.as_deref() == Some(MONITORING_SERVICE) && self.advertised())
            .then(|| {
                self.properties()
                    .map(|properties| Identity::from_properties(&properties))
            })
            .flatten()
    }
}

struct Message<'a> {
    bytes: &'a [u8],
    heap: usize,
}

impl<'a> Message<'a> {
    fn region(&self, reference: u16, length: usize) -> Result<&'a [u8], &'static str> {
        let start = usize::from(reference);
        if start == 0 || start < self.heap {
            return Err("lookup reference points outside the message heap");
        }
        let end = start
            .checked_add(length)
            .ok_or("lookup reference extent overflows")?;
        self.bytes
            .get(start..end)
            .ok_or("lookup reference extends beyond its message")
    }

    fn string(&self, reference: u16) -> Result<String, &'static str> {
        let start = usize::from(reference);
        if start == 0 || start < self.heap {
            return Err("lookup string points outside the message heap");
        }
        let text = self
            .bytes
            .get(start..)
            .ok_or("lookup string begins beyond its message")?;
        let length = text
            .iter()
            .position(|byte| *byte == 0)
            .ok_or("lookup string is not terminated within its message")?;
        String::from_utf8(text[..length].to_vec()).map_err(|_| "lookup string is not UTF-8")
    }

    fn word(&self, base: &[u8], offset: usize) -> Result<u16, &'static str> {
        read_u16(base, offset).ok_or("lookup structure is truncated")
    }

    fn entry(&self, reference: u16) -> Result<LookupEntry, &'static str> {
        let fixed = self.region(reference, 8)?;
        let name = self.string(self.word(fixed, 0)?)?;
        if name.is_empty() {
            return Err("lookup entry name is empty");
        }
        let flags = self.word(fixed, 2)?;
        let information_reference = self.word(fixed, 4)?;
        let information = if flags & ENTRY_INFORMATION != 0 && information_reference != 0 {
            Some(self.information(information_reference)?)
        } else if flags & ENTRY_INFORMATION != 0 {
            return Err("lookup entry selects information without a reference");
        } else {
            None
        };
        let services = if information.is_some() && flags & ENTRY_SERVICES != 0 {
            Some(self.services(self.word(fixed, 6)?)?)
        } else {
            None
        };
        Ok(LookupEntry {
            flags,
            information,
            name,
            services,
        })
    }

    fn information(&self, reference: u16) -> Result<LookupInformation, &'static str> {
        let flags = self.word(self.region(reference, 8)?, 0)?;
        let length = [(0x40, 20), (0x20, 16), (0x10, 12), (0x08, 10)]
            .into_iter()
            .find(|(mask, _)| flags & mask != 0)
            .map_or(8, |(_, length)| length);
        let fixed = self.region(reference, length)?;
        let opaque = |mask: u16, offset: usize| -> Result<Option<String>, &'static str> {
            (flags & mask != 0)
                .then(|| Ok(hexadecimal(self.region(self.word(fixed, offset)?, 8)?)))
                .transpose()
        };
        let route = (flags & 0x08 != 0)
            .then(|| -> Result<LookupRoute, &'static str> {
                let route = self.region(self.word(fixed, 8)?, 6)?;
                Ok(LookupRoute {
                    client: read_u32(route, 0).ok_or("lookup route is truncated")?,
                    component: self.word(route, 4)?,
                })
            })
            .transpose()?;
        let extension = |mask: u16, offset: usize| {
            (flags & mask != 0)
                .then(|| read_u32(fixed, offset).ok_or("lookup information is truncated"))
                .transpose()
        };
        Ok(LookupInformation {
            classification: (flags & 0x10 != 0).then(|| fixed[11]),
            first_extension: extension(0x20, 12)?,
            flags,
            manufacturer: opaque(0x02, 4)?,
            model: opaque(0x04, 6)?,
            name: (flags & 0x01 != 0)
                .then(|| self.string(self.word(fixed, 2)?))
                .transpose()?,
            route,
            second_extension: extension(0x40, 16)?,
        })
    }

    fn services(&self, reference: u16) -> Result<Vec<LookupService>, &'static str> {
        let count = usize::from(self.word(self.region(reference, 6)?, 0)?);
        let list = self.region(reference, (4 + 2 * count).max(6))?;
        (0..count)
            .map(|index| self.service(self.word(list, 4 + 2 * index)?))
            .collect()
    }

    fn service(&self, reference: u16) -> Result<LookupService, &'static str> {
        let fixed = self.region(reference, 12)?;
        let flags = self.word(fixed, 0)?;
        let txt = if flags & 0x02 != 0 {
            let count = usize::from(self.word(fixed, 6)?);
            let txt = if count == 0 {
                Vec::new()
            } else {
                self.region(self.word(fixed, 4)?, count)?.to_vec()
            };
            txt_elements(&txt).ok_or("lookup service TXT is malformed")?;
            Some(txt)
        } else {
            None
        };
        Ok(LookupService {
            extra: (flags & 0x04 != 0)
                .then(|| self.word(fixed, 8))
                .transpose()?,
            flags,
            name: (flags & 0x01 != 0)
                .then(|| self.string(self.word(fixed, 2)?))
                .transpose()?,
            reserved: self.word(fixed, 10)?,
            txt,
        })
    }
}

fn txt_elements(txt: &[u8]) -> Option<Vec<&[u8]>> {
    let mut elements = Vec::new();
    let mut remaining = txt;
    while let Some((&length, rest)) = remaining.split_first() {
        elements.push(rest.get(..usize::from(length))?);
        remaining = &rest[usize::from(length)..];
    }
    Some(elements)
}

fn destination_component(header: &[u8]) -> Option<u16> {
    header
        .get(8..)?
        .chunks_exact(8)
        .find(|block| read_u16(block, 0) == Some(DESTINATION_ADDRESS))
        .and_then(|block| read_u16(block, 6))
}

pub fn parse(frame: &[u8]) -> Result<Option<LookupMessage>, &'static str> {
    let Some(frame) = crate::dapi::parse_frame(frame) else {
        return Ok(None);
    };
    let packet = frame.payload;
    if !frame.server_to_client || frame.message_type != NORMAL_MESSAGE || packet.len() < 16 {
        return Ok(None);
    }
    let header = usize::from(read_u16(packet, 0).unwrap_or_default());
    if read_u16(packet, 2) != Some(PACKET_HEADER_BLOCK)
        || header < 8
        || header % 4 != 0
        || header + 8 > packet.len()
    {
        return Ok(None);
    }
    let kind = match read_u16(packet, header + 2) {
        Some(REGISTRATION) => LookupKind::Registration,
        Some(DEREGISTRATION) => LookupKind::Deregistration,
        Some(UPDATE) => LookupKind::Update,
        _ => return Ok(None),
    };
    let response = match packet[6] & 0x03 {
        REQUEST_FORM => false,
        RESPONSE_FORM => true,
        _ => return Err("lookup message has an unsupported packet form"),
    };
    let message = &packet[header..];
    let word = |offset| read_u16(message, offset).map(usize::from);
    let (Some(length), Some(subheader), Some(payload)) = (word(0), word(4), word(6)) else {
        return Err("lookup message header is truncated");
    };
    if length != message.len()
        || length % 4 != 0
        || subheader % 4 != 0
        || payload % 4 != 0
        || subheader < 4
        || 8 + subheader + payload > length
    {
        return Err("lookup message extents are inconsistent");
    }
    let request_id = read_u16(message, 8).expect("subheader is present");
    let status = read_u16(message, 10).expect("subheader is present");
    let expects_update = match (kind, response) {
        (LookupKind::Update, false) => true,
        (LookupKind::Registration, true) => status == 0,
        (LookupKind::Deregistration, true) => false,
        _ => return Err("lookup message has an unsupported direction"),
    };
    let update = if expects_update {
        if payload < UPDATE_PAYLOAD_BYTES {
            return Err("lookup update payload is shorter than its fixed fields");
        }
        let start = 8 + subheader;
        let fixed = &message[start..start + UPDATE_PAYLOAD_BYTES];
        let reader = Message {
            bytes: message,
            heap: start + payload,
        };
        let flags = reader.word(fixed, 20)?;
        let entry_reference = reader.word(fixed, 22)?;
        let entry = match (flags & UPDATE_ENTRY != 0, entry_reference) {
            (true, 0) => return Err("lookup update selects an entry without a reference"),
            (true, reference) => Some(reader.entry(reference)?),
            (false, _) => None,
        };
        Some(LookupUpdate {
            domain_id: hexadecimal(&fixed[..16]),
            entry,
            flags,
            lookup_id: read_u32(fixed, 16).expect("fixed update fields are present"),
            terminal: flags & UPDATE_TERMINAL != 0,
        })
    } else {
        None
    };
    Ok(Some(LookupMessage {
        destination_component: destination_component(&packet[..header]),
        kind,
        request_id,
        response,
        status,
        update,
    }))
}
