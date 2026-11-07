use std::fmt::Write;

use serde::Serialize;

use crate::bytes::read_u16;

pub const HEARTBEAT_PROTOCOL: u16 = 0xFFFE;
pub const HEARTBEAT_HEADER_SIZE: usize = 0x20;

pub fn monitoring_payload(record: &[u8], minimum: usize) -> Option<usize> {
    let total = usize::from(read_u16(record, 0)?);
    let subheader = usize::from(read_u16(record, 4)?);
    let length = usize::from(read_u16(record, 6)?);
    let payload = 8usize.checked_add(subheader)?;
    if total != record.len()
        || subheader < 2
        || length < minimum
        || payload.checked_add(length)? > total
    {
        return None;
    }
    Some(payload)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatRecord<'a> {
    pub record_type: u16,
    pub bytes: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatIdentity {
    pub device_extended_unique_identifier: String,
}

pub fn parse_heartbeat_device_extended_unique_identifier(data: &[u8]) -> Option<String> {
    parse_heartbeat_records(data)?;
    let identifier_bytes = data.get(8..16)?;
    if identifier_bytes.iter().all(|byte| *byte == 0)
        || identifier_bytes.iter().all(|byte| *byte == u8::MAX)
    {
        return None;
    }
    let mut identifier = String::with_capacity(16);
    for byte in identifier_bytes {
        write!(&mut identifier, "{byte:02x}").ok()?;
    }
    Some(identifier)
}

pub fn parse_heartbeat_identity(data: &[u8]) -> Option<HeartbeatIdentity> {
    Some(HeartbeatIdentity {
        device_extended_unique_identifier: parse_heartbeat_device_extended_unique_identifier(data)?,
    })
}

pub fn parse_heartbeat_records(data: &[u8]) -> Option<Vec<HeartbeatRecord<'_>>> {
    if data.len() < HEARTBEAT_HEADER_SIZE
        || read_u16(data, 0)? != HEARTBEAT_PROTOCOL
        || usize::from(read_u16(data, 2)?) != data.len()
    {
        return None;
    }

    let mut records = Vec::new();
    let mut offset = HEARTBEAT_HEADER_SIZE;
    while offset < data.len() {
        let record_length = usize::from(read_u16(data, offset)?);
        if record_length < 4 || record_length % 4 != 0 {
            return None;
        }
        let record_end = offset.checked_add(record_length)?;
        let record = data.get(offset..record_end)?;
        records.push(HeartbeatRecord {
            record_type: read_u16(record, 2)?,
            bytes: record,
        });
        offset = record_end;
    }

    Some(records)
}
