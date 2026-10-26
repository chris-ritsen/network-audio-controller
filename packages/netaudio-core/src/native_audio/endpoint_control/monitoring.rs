use std::net::{Ipv4Addr, SocketAddrV4};

use serde::{Deserialize, Serialize};

use super::NAMED_CLOCK_DOMAIN;
use super::{double_word, string, word, Writer};
use crate::native_audio::clock::discipline::Snapshot;
use crate::native_audio::clock::version_one::PortIdentity;
use crate::native_audio::source_control::{Error, Header};

pub const CLASS_TWO_MULTICAST_DESTINATION: SocketAddrV4 =
    SocketAddrV4::new(Ipv4Addr::new(224, 0, 0, 231), 8702);

const MONITORING_REVISION: u16 = 0x1200;
const CONTROL_ADDRESS_OPERATION: u16 = 0x1001;
const SUBSCRIPTION_WRITE_OPERATION: u16 = 0x3010;
const SUBSCRIPTION_READ_OPERATION: u16 = 0x3011;
const UNSUPPORTED_REVISION: u16 = 0x0010;
const UNSUPPORTED_OPERATION: u16 = 0x0030;
const INVALID_SUBSCRIPTION: u16 = 0x0100;
const SUBSCRIPTION_CAPACITY_EXHAUSTED: u16 = 0xfff0;
const TARGET_NAME_MISMATCH: u16 = 0x0101;
const MANUFACTURER_CLASS: u16 = 0xffff;
const STATUS_MESSAGE: u16 = 0x073c;
const QUERY_REVISION: u16 = 0x073e;
const LEGACY_QUERY_REVISION: u16 = 0x073a;
const IDENTITY_QUERY_REVISION: u16 = 0x0731;
const VERSION_QUERY: u16 = 0x0061;
const VERSION_STATUS: u16 = 0x0060;
const INTERFACE_QUERY: u16 = 0x0013;
const INTERFACE_STATUS: u16 = 0x0011;
const CLOCK_QUERY: u16 = 0x0021;
const CLOCK_STATUS: u16 = 0x0020;
const STATISTICS_QUERY: u16 = 0x0041;
const STATISTICS_STATUS: u16 = 0x0040;
const PRIMARY_CAPABILITIES: u32 = 0x1000_0080;
const SECONDARY_CAPABILITIES: u32 = 0x600;
const CLOCK_EXTENSION_REVISION: u16 = 0x0100;
const SUBSCRIPTION_LIFETIME_NANOSECONDS: u64 = 30_000_000_000;
const MAXIMUM_SUBSCRIPTION_RECORDS: usize = 64;
const MAXIMUM_PUBLICATION_BYTES: usize = 1472;
const STATUS_QUERY_DELAY_VALUE: u32 = 100;
const STATUS_PUBLICATION_DELAY_NANOSECONDS: u64 = 100_000_000;
const CLOCK_QUERY_BODY_BYTES: usize = 68;
const CLOCK_QUERY_FIELD_SELECTOR_OFFSET: usize = 8;
const CLOCK_QUERY_FLAG_SELECTOR_OFFSET: usize = 32;
const CLOCK_QUERY_EXTENDED_REGION_OFFSET: usize = 40;
const MANUFACTURER_TAG: &[u8; 8] = b"Audinate";

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub instance: [u8; 8],
    pub process: u16,
}

#[derive(Clone, Debug, Serialize)]
struct Subscription {
    identity: Identity,
    class: u16,
    discriminator: u16,
    flags: u16,
    addresses: Vec<SocketAddrV4>,
    expires_at: Option<u64>,
}

#[derive(Clone)]
struct Record {
    class: u16,
    discriminator: u16,
    flags: u16,
    addresses: Vec<SocketAddrV4>,
}

pub struct Manager {
    identity: Identity,
    name: String,
    control_address: SocketAddrV4,
    subscriptions: Vec<Subscription>,
    network_sequence: u16,
}

impl Manager {
    pub fn new(
        identity: Identity,
        name: String,
        control_address: SocketAddrV4,
    ) -> Result<Self, Error> {
        if identity.instance.iter().all(|value| *value == 0)
            || name.is_empty()
            || name.len() > crate::protocol::DANTE_NAME_MAX_LENGTH
            || name.as_bytes().contains(&0)
            || control_address.port() == 0
            || control_address.ip().is_unspecified()
        {
            return Err(Error(
                "monitoring identity, name and owned control address are required",
            ));
        }

        Ok(Self {
            identity,
            name,
            control_address,
            subscriptions: Vec::new(),
            network_sequence: 0,
        })
    }

    pub fn identity(&self) -> Identity {
        self.identity
    }

    pub fn expire(&mut self, now: u64) {
        self.subscriptions
            .retain(|entry| entry.expires_at.is_none_or(|deadline| now < deadline));
    }

    pub fn next_expiry(&self) -> Option<u64> {
        self.subscriptions
            .iter()
            .filter_map(|entry| entry.expires_at)
            .min()
    }

    fn response(&self, request: Header, length: usize, result: u16) -> Result<Writer, Error> {
        let mut reply = Writer::response(request, length, result)?;
        reply.word(0, MONITORING_REVISION);
        reply.word(10, self.identity.process);
        reply.bytes[12..20].copy_from_slice(&self.identity.instance);
        Ok(reply)
    }

    pub fn answer(&mut self, request: &[u8], now: u64) -> Result<Option<Vec<u8>>, Error> {
        let header = Header::decode(request)?;
        if request.len() < 20 {
            return Err(Error("monitoring control envelope is incomplete"));
        }

        if header.result != 0 {
            return Ok(None);
        }

        self.expire(now);
        if header.revision != MONITORING_REVISION {
            return Ok(Some(
                self.response(header, 20, UNSUPPORTED_REVISION)?.finish(),
            ));
        }

        let reply = match header.operation {
            CONTROL_ADDRESS_OPERATION if request.len() == 20 => {
                let mut reply = self.response(header, 32, 1)?;
                reply.word(20, 1);
                reply.bytes[24..28].copy_from_slice(&self.control_address.ip().octets());
                reply.word(28, self.control_address.port());
                reply.finish()
            }
            SUBSCRIPTION_WRITE_OPERATION => self.apply(header, request, now)?,
            SUBSCRIPTION_READ_OPERATION if request.len() == 20 => {
                let records: Vec<_> = self
                    .subscriptions
                    .iter()
                    .map(|entry| {
                        Some(Record {
                            class: entry.class,
                            discriminator: entry.discriminator,
                            flags: entry.flags & 3,
                            addresses: entry.addresses.clone(),
                        })
                    })
                    .collect();
                let address_count = self
                    .subscriptions
                    .iter()
                    .map(|entry| entry.addresses.len())
                    .max()
                    .unwrap_or(1);
                self.heap_response(header, 1, &records, address_count)?
            }
            _ => self.response(header, 20, UNSUPPORTED_OPERATION)?.finish(),
        };

        Ok(Some(reply))
    }

    fn apply(&mut self, header: Header, request: &[u8], now: u64) -> Result<Vec<u8>, Error> {
        let body = &request[20..];
        let root_count = usize::from(word(body, 0)?);
        if root_count < 3 || 2 + root_count * 2 > body.len() {
            return Err(Error("monitoring subscription root is incomplete"));
        }

        let address_count = usize::from(word(body, 4)?);
        if address_count > 2 {
            return Err(Error(
                "monitoring subscription address count is unsupported",
            ));
        }

        let root_end = 2 + root_count * 2;
        let target = if root_count >= 4 && word(body, 8)? != 0 {
            Some(string(body, usize::from(word(body, 8)?), root_end)?)
        } else {
            None
        };

        if word(body, 2)? != 0 {
            string(body, usize::from(word(body, 2)?), root_end)?;
        }

        let list = usize::from(word(body, 6)?);
        let count = if list == 0 {
            0
        } else {
            if list < root_end || list % 2 != 0 {
                return Err(Error("monitoring subscription pointer list is invalid"));
            }
            usize::from(word(body, list)?)
        };

        if count > MAXIMUM_SUBSCRIPTION_RECORDS
            || list
                .checked_add(2 + count * 2)
                .is_none_or(|end| end > body.len())
        {
            return Err(Error("monitoring subscription pointer list is incomplete"));
        }

        let requester = Identity {
            process: word(request, 10)?,
            instance: request[12..20].try_into().unwrap(),
        };

        let mut records = Vec::with_capacity(count);
        for index in 0..count {
            let offset = usize::from(word(body, list + 2 + index * 2)?);
            if offset == 0 {
                records.push(None);
                continue;
            }

            if offset < root_end
                || offset % 2 != 0
                || offset
                    .checked_add(8 + address_count * 8)
                    .is_none_or(|end| end > body.len())
            {
                return Err(Error("monitoring subscription record is invalid"));
            }

            let mut addresses = Vec::with_capacity(address_count);
            for index in 0..address_count {
                let slot = offset + 8 + 8 * index;
                addresses.push(SocketAddrV4::new(
                    Ipv4Addr::new(body[slot], body[slot + 1], body[slot + 2], body[slot + 3]),
                    word(body, slot + 4)?,
                ));
            }
            records.push(Some(Record {
                class: word(body, offset)?,
                discriminator: word(body, offset + 2)?,
                flags: word(body, offset + 4)?,
                addresses,
            }));
        }

        let mut result = 1;
        let mut applied = Vec::with_capacity(count);
        for record in records {
            let Some(record) = record else {
                result = INVALID_SUBSCRIPTION;
                applied.push(None);
                continue;
            };

            if record.class != 2 {
                result = INVALID_SUBSCRIPTION;
                applied.push(None);
                continue;
            }

            let existing = self.subscriptions.iter().position(|entry| {
                entry.identity == requester
                    && entry.class == record.class
                    && entry.discriminator == record.discriminator
            });
            let usable = record
                .addresses
                .iter()
                .any(|address| !address.ip().is_unspecified() && address.port() != 0);
            if !usable {
                if let Some(index) = existing {
                    self.subscriptions.remove(index);
                }
            } else {
                let expires_at = if record.flags & 1 != 0 {
                    Some(
                        now.checked_add(SUBSCRIPTION_LIFETIME_NANOSECONDS)
                            .ok_or(Error("monitoring expiry overflow"))?,
                    )
                } else {
                    None
                };

                let entry = Subscription {
                    identity: requester,
                    class: record.class,
                    discriminator: record.discriminator,
                    flags: record.flags & 3,
                    addresses: record.addresses.clone(),
                    expires_at,
                };

                if let Some(index) = existing {
                    self.subscriptions[index] = entry;
                } else if self.subscriptions.len() < 8 {
                    self.subscriptions.push(entry);
                } else {
                    result = SUBSCRIPTION_CAPACITY_EXHAUSTED;
                    applied.push(None);
                    continue;
                }
            }
            applied.push(Some(record));
        }

        if target
            .as_ref()
            .is_some_and(|target| !target.is_empty() && target != &self.name)
        {
            result = TARGET_NAME_MISMATCH;
        }

        self.heap_response(header, result, &applied, address_count)
    }

    fn heap_response(
        &self,
        header: Header,
        result: u16,
        records: &[Option<Record>],
        address_count: usize,
    ) -> Result<Vec<u8>, Error> {
        let mut reply = self.response(header, 30, result)?;
        reply.word(20, 4);
        reply.word(24, address_count as u16);
        let target = reply.text(&self.name)?;
        let client = reply.text("")?;
        reply.word(22, client - 20);
        reply.word(28, target - 20);
        let list = reply.reserve(2 + 2 * records.len(), 2)?;
        reply.word(26, (list - 20) as u16);
        reply.word(list, records.len() as u16);
        for (index, record) in records.iter().enumerate() {
            let Some(record) = record else {
                continue;
            };

            let offset = reply.reserve(8 + 8 * address_count, 2)?;
            reply.word(list + 2 + index * 2, (offset - 20) as u16);
            reply.word(offset, record.class);
            reply.word(offset + 2, record.discriminator);
            reply.word(offset + 4, record.flags);
            for (index, address) in record.addresses.iter().take(address_count).enumerate() {
                let slot = offset + 8 + 8 * index;
                reply.bytes[slot..slot + 4].copy_from_slice(&address.ip().octets());
                reply.word(slot + 4, address.port());
            }
        }

        Ok(reply.finish())
    }

    pub fn destinations(&mut self, now: u64) -> Vec<SocketAddrV4> {
        self.expire(now);
        let mut destinations = Vec::new();
        for subscription in &self.subscriptions {
            for address in &subscription.addresses {
                if !address.ip().is_unspecified()
                    && !address.ip().is_multicast()
                    && !address.ip().is_broadcast()
                    && address.port() != 0
                    && !destinations.contains(address)
                {
                    destinations.push(*address);
                }
            }
        }
        destinations
    }

    pub fn publication(&mut self, manufacturer_body: &[u8]) -> Result<Vec<u8>, Error> {
        let length = 24 + manufacturer_body.len();
        if manufacturer_body.len() < 8 || length > MAXIMUM_PUBLICATION_BYTES {
            return Err(Error("manufacturer publication has an invalid extent"));
        }

        let mut packet = vec![0; length];
        self.network_sequence = self.network_sequence.wrapping_add(1);
        put_word(&mut packet, 0, MANUFACTURER_CLASS);
        put_word(&mut packet, 2, length as u16);
        put_word(&mut packet, 4, self.network_sequence);
        put_word(&mut packet, 6, self.identity.process);
        packet[8..16].copy_from_slice(&self.identity.instance);
        packet[16..24].copy_from_slice(MANUFACTURER_TAG);
        packet[24..].copy_from_slice(manufacturer_body);
        Ok(packet)
    }
}

fn put_word(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn put_double_word(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interface {
    pub index: u32,
    pub hardware_address: [u8; 6],
    pub address: Ipv4Addr,
    pub netmask: Ipv4Addr,
    pub gateway: Ipv4Addr,
    pub name_server: Ipv4Addr,
    pub link_up: bool,
    pub speed_megabits_per_second: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct Statistics {
    pub transmit_bytes_per_second: u32,
    pub receive_bytes_per_second: u32,
    pub transmit_errors: u32,
    pub receive_errors: u32,
}

pub struct StatusContext<'a> {
    pub interface: &'a Interface,
    pub clock: &'a Snapshot,
    pub local_clock_identity: PortIdentity,
    pub statistics: Option<&'a Statistics>,
    pub software_version: u32,
}

fn body(kind: u16, length: usize) -> Vec<u8> {
    let mut bytes = vec![0; length];
    put_word(&mut bytes, 0, STATUS_MESSAGE);
    put_word(&mut bytes, 2, kind);
    bytes
}

#[derive(Clone, Copy, Debug)]
pub enum StatusQuery {
    Version,
    Interface,
    Clock,
    Statistics,
}

impl StatusQuery {
    pub fn decode(packet: &[u8]) -> Result<Option<Self>, Error> {
        if packet.len() < 32
            || word(packet, 0)? != MANUFACTURER_CLASS
            || usize::from(word(packet, 2)?) != packet.len()
            || &packet[16..24] != MANUFACTURER_TAG
        {
            return Err(Error("manufacturer control envelope is invalid"));
        }

        let query = &packet[24..];

        let revision = word(query, 0)?;
        let operation = word(query, 2)?;
        let supported_revision = matches!(
            (revision, operation),
            (QUERY_REVISION | LEGACY_QUERY_REVISION | IDENTITY_QUERY_REVISION, VERSION_QUERY)
                | (QUERY_REVISION | LEGACY_QUERY_REVISION, INTERFACE_QUERY)
                | (QUERY_REVISION | LEGACY_QUERY_REVISION, CLOCK_QUERY)
                | (QUERY_REVISION | LEGACY_QUERY_REVISION, STATISTICS_QUERY)
        );

        if !supported_revision {
            return Ok(None);
        }

        let kind = match operation {
            VERSION_QUERY if query.len() == 8 && double_word(query, 4)? == 0 => Self::Version,
            INTERFACE_QUERY
                if query.len() == 16
                    && double_word(query, 4)? == STATUS_QUERY_DELAY_VALUE
                    && query[8..].iter().all(|byte| *byte == 0) =>
            {
                Self::Interface
            }
            CLOCK_QUERY
                if query.len() == CLOCK_QUERY_BODY_BYTES
                    && double_word(query, 4)? == STATUS_QUERY_DELAY_VALUE
                    && word(query, CLOCK_QUERY_FIELD_SELECTOR_OFFSET)? == 0
                    && word(query, CLOCK_QUERY_FLAG_SELECTOR_OFFSET)? == 0
                    && query[CLOCK_QUERY_EXTENDED_REGION_OFFSET..]
                        .iter()
                        .all(|byte| *byte == 0) =>
            {
                Self::Clock
            }
            STATISTICS_QUERY
                if query.len() == 8 && double_word(query, 4)? == STATUS_QUERY_DELAY_VALUE =>
            {
                Self::Statistics
            }
            _ => return Ok(None),
        };

        Ok(Some(kind))
    }

    pub fn delay_nanoseconds(self) -> u64 {
        match self {
            Self::Version => 0,
            _ => STATUS_PUBLICATION_DELAY_NANOSECONDS,
        }
    }
}

pub fn answer_status_query(
    packet: &[u8],
    context: &StatusContext<'_>,
) -> Result<Option<Vec<u8>>, Error> {
    let Some(query) = StatusQuery::decode(packet)? else {
        return Ok(None);
    };

    match query {
        StatusQuery::Version => {
            let mut status = body(VERSION_STATUS, 256);
            put_double_word(&mut status, 8, context.software_version);
            status[20..28].copy_from_slice(b"NetAudio");
            put_double_word(&mut status, 28, PRIMARY_CAPABILITIES);
            put_double_word(&mut status, 52, 1);
            let model = b"NetAudio Virtual Endpoint";
            status[64..64 + model.len()].copy_from_slice(model);
            put_double_word(&mut status, 196, SECONDARY_CAPABILITIES);
            Ok(Some(status))
        }
        StatusQuery::Interface => {
            let interface = context.interface;
            let mut status = body(INTERFACE_STATUS, 72);
            put_word(&mut status, 8, 1);
            put_double_word(
                &mut status,
                12,
                interface.speed_megabits_per_second.unwrap_or(0),
            );
            put_word(&mut status, 16, u16::from(interface.link_up));
            status[18..24].copy_from_slice(&interface.hardware_address);
            status[24..28].copy_from_slice(&interface.address.octets());
            status[28..32].copy_from_slice(&interface.netmask.octets());
            status[32..36].copy_from_slice(&interface.name_server.octets());
            status[36..40].copy_from_slice(&interface.gateway.octets());
            put_word(&mut status, 40, 24);
            put_word(&mut status, 42, 48);
            Ok(Some(status))
        }
        StatusQuery::Clock => Ok(Some(clock_status(context))),
        StatusQuery::Statistics => {
            let Some(statistics) = context.statistics else {
                return Ok(None);
            };

            let mut status = body(STATISTICS_STATUS, 64);
            put_word(&mut status, 8, 1);
            put_word(&mut status, 10, 36);
            put_word(&mut status, 12, 16);
            put_word(&mut status, 16, 20);
            status[20] = 1;
            put_double_word(&mut status, 32, 3);
            put_word(&mut status, 36, 1);
            put_word(&mut status, 38, 40);
            put_double_word(&mut status, 40, statistics.transmit_bytes_per_second);
            put_double_word(&mut status, 44, statistics.receive_bytes_per_second);
            put_double_word(&mut status, 48, statistics.transmit_errors);
            put_double_word(&mut status, 52, statistics.receive_errors);
            put_word(&mut status, 58, 1);
            Ok(Some(status))
        }
    }
}

fn clock_status(context: &StatusContext<'_>) -> Vec<u8> {
    let mut status = body(CLOCK_STATUS, 200);
    let usable = context.clock.usable;
    put_word(&mut status, 8, if usable { 3 } else { 2 });
    put_word(&mut status, 10, if usable { 3 } else { 6 });
    status[15] = context
        .clock
        .last_dataset
        .as_ref()
        .map_or(255, |dataset| dataset.stratum);
    status[20..26].copy_from_slice(&context.local_clock_identity.identifier);
    status[28..34].copy_from_slice(&context.clock.parent_identity.identifier);
    if let Some(dataset) = &context.clock.last_dataset {
        status[36..42].copy_from_slice(&dataset.grandmaster.identifier);
    }
    put_word(&mut status, 44, 1);
    put_word(&mut status, 46, 52);
    put_word(&mut status, 48, 9);
    put_word(&mut status, 52, CLOCK_EXTENSION_REVISION);
    put_word(&mut status, 62, 0x1004);
    status[64..80].copy_from_slice(NAMED_CLOCK_DOMAIN.bytes());
    put_word(&mut status, 80, if usable { 0 } else { 8 });
    put_word(&mut status, 82, 1);
    put_word(&mut status, 84, 96);
    status[86..88].copy_from_slice(&[0x0c, 0]);
    put_word(&mut status, 90, 12);
    put_word(&mut status, 92, 120);
    put_word(&mut status, 94, 64);
    put_word(&mut status, 96, 1);
    put_word(&mut status, 100, 104);
    status[102..104].copy_from_slice(&[0x10, 0]);
    put_word(
        &mut status,
        104,
        if context.interface.link_up { 0 } else { 2 },
    );
    put_word(&mut status, 106, context.local_clock_identity.port);
    status[108] = 1;
    status[109] = 2;
    status[110] = 1;
    put_double_word(&mut status, 112, context.interface.index);
    put_word(&mut status, 116, 9);
    put_word(&mut status, 118, 7);
    put_word(&mut status, 120, 1);
    put_word(&mut status, 124, 184);
    put_word(&mut status, 126, 16);
    put_word(&mut status, 184, 1);
    status
}
