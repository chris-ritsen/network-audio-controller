use std::net::Ipv4Addr;

use anyhow::{Context, Result, bail};
use hickory_proto::op::{Header, Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{Name, RecordType};
use hickory_proto::serialize::binary::{BinDecodable, BinDecoder};

const INTERNET_CLASS: u16 = 1;
const CACHE_FLUSH_FLAG: u16 = 0x8000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    Address,
    Text,
    Service,
}

impl Kind {
    fn record_type(self) -> RecordType {
        match self {
            Self::Address => RecordType::A,
            Self::Text => RecordType::TXT,
            Self::Service => RecordType::SRV,
        }
    }

    fn from_record_type(value: u16) -> Option<Self> {
        match RecordType::from(value) {
            RecordType::A => Some(Self::Address),
            RecordType::TXT => Some(Self::Text),
            RecordType::SRV => Some(Self::Service),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key {
    pub name: Name,
    pub kind: Kind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Data {
    Address(Ipv4Addr),
    Text(Vec<u8>),
    Service {
        priority: u16,
        weight: u16,
        port: u16,
        target: Name,
    },
    Invalid(Vec<u8>),
}

#[derive(Clone, Debug)]
pub struct Record {
    pub key: Key,
    pub lifetime_seconds: u32,
    pub cache_flush: bool,
    pub data: Data,
}

pub fn service_name(instance: &[u8], service: &[u8]) -> Result<Name> {
    if instance.is_empty() || instance.contains(&0) {
        bail!("service instance must be a complete nonempty label without NUL bytes");
    }

    Ok(Name::from_labels([instance, service, b"_udp", b"local"])?)
}

fn service_data(decoder: &mut BinDecoder<'_>, end: usize) -> Result<Data> {
    let priority = decoder.read_u16()?.unverified();
    let weight = decoder.read_u16()?.unverified();
    let port = decoder.read_u16()?.unverified();
    let target = Name::read(decoder)?;

    if decoder.index() != end {
        bail!("service record does not end at its declared extent");
    }

    Ok(Data::Service {
        priority,
        weight,
        port,
        target,
    })
}

pub fn response(bytes: &[u8]) -> Result<Vec<Record>> {
    let mut decoder = BinDecoder::new(bytes);
    let header = Header::read(&mut decoder)?;

    if header.message_type() != MessageType::Response {
        return Ok(Vec::new());
    }

    if header.id() != 0
        || header.op_code() != OpCode::Query
        || header.response_code() != ResponseCode::NoError
        || header.truncated()
    {
        bail!("discovery response is outside the complete multicast response profile");
    }

    for _ in 0..header.query_count() {
        Name::read(&mut decoder)?;
        decoder.read_u16()?;
        decoder.read_u16()?;
    }

    let total = usize::from(header.answer_count())
        + usize::from(header.name_server_count())
        + usize::from(header.additional_count());
    let authority_start = usize::from(header.answer_count());
    let authority_end = authority_start + usize::from(header.name_server_count());
    let mut records = Vec::new();

    for index in 0..total {
        let name = Name::read(&mut decoder)?;
        let kind = Kind::from_record_type(decoder.read_u16()?.unverified());
        let class = decoder.read_u16()?.unverified();
        let lifetime_seconds = decoder.read_u32()?.unverified();
        let length = usize::from(decoder.read_u16()?.unverified());
        let offset = decoder.index();
        let raw = decoder.read_slice(length)?.unverified();

        if (authority_start..authority_end).contains(&index)
            || class & !CACHE_FLUSH_FLAG != INTERNET_CLASS
        {
            continue;
        }

        let Some(kind) = kind else {
            continue;
        };

        let data = match kind {
            Kind::Text => Data::Text(raw.to_vec()),
            Kind::Address => match <[u8; 4]>::try_from(raw) {
                Ok(octets) => Data::Address(octets.into()),
                Err(_) => Data::Invalid(raw.to_vec()),
            },
            Kind::Service => {
                let mut data_decoder = decoder.clone(offset.try_into()?);
                service_data(&mut data_decoder, offset + length)
                    .unwrap_or_else(|_| Data::Invalid(raw.to_vec()))
            }
        };

        records.push(Record {
            key: Key {
                name,
                kind,
            },
            lifetime_seconds,
            cache_flush: class & CACHE_FLUSH_FLAG != 0,
            data,
        });
    }

    if !decoder.is_empty() {
        bail!("discovery response has bytes beyond its declared records");
    }

    Ok(records)
}

pub fn query(keys: impl IntoIterator<Item = Key>) -> Result<Vec<u8>> {
    let mut message = Message::new();

    for key in keys {
        message.add_query(Query::query(key.name, key.kind.record_type()));
    }

    message.to_vec().context("encode discovery question")
}
