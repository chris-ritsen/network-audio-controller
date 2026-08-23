use std::fmt::Write;

use serde::Serialize;

use crate::bytes::read_u16;

pub const HEARTBEAT_PROTOCOL: u16 = 0xFFFE;
pub const HEARTBEAT_HEADER_SIZE: usize = 0x20;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(records: &[u8]) -> Vec<u8> {
        let length = HEARTBEAT_HEADER_SIZE + records.len();
        let mut data = vec![0; HEARTBEAT_HEADER_SIZE];
        data[0..2].copy_from_slice(&HEARTBEAT_PROTOCOL.to_be_bytes());
        data[2..4].copy_from_slice(&u16::try_from(length).unwrap().to_be_bytes());
        data.extend_from_slice(records);
        data
    }

    #[test]
    fn preserves_order_types_and_raw_record_bytes() {
        let records = [
            0x00, 0x04, 0x80, 0x00, 0x00, 0x08, 0x80, 0x01, 0x12, 0x34, 0x56, 0x78,
        ];
        let data = packet(&records);
        let parsed = parse_heartbeat_records(&data).unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].record_type, 0x8000);
        assert_eq!(parsed[0].bytes, &records[0..4]);
        assert_eq!(parsed[1].record_type, 0x8001);
        assert_eq!(parsed[1].bytes, &records[4..12]);
    }

    #[test]
    fn rejects_invalid_outer_and_record_boundaries() {
        let valid = packet(&[0x00, 0x04, 0x80, 0x00]);
        assert_eq!(parse_heartbeat_records(&valid[..31]), None);

        let mut wrong_outer_length = valid.clone();
        wrong_outer_length[2..4].copy_from_slice(&0x20u16.to_be_bytes());
        assert_eq!(parse_heartbeat_records(&wrong_outer_length), None);

        for record_length in [0u16, 3, 8] {
            let mut malformed = valid.clone();
            malformed[HEARTBEAT_HEADER_SIZE..HEARTBEAT_HEADER_SIZE + 2]
                .copy_from_slice(&record_length.to_be_bytes());
            assert_eq!(parse_heartbeat_records(&malformed), None);
        }
    }

    #[test]
    fn validates_and_formats_the_device_extended_unique_identifier() {
        let mut valid = packet(&[0x00, 0x04, 0x80, 0x00]);
        valid[8..16].copy_from_slice(&[0x00, 0x1D, 0xC1, 0x08, 0x12, 0x58, 0x00, 0x00]);
        assert_eq!(
            parse_heartbeat_identity(&valid),
            Some(HeartbeatIdentity {
                device_extended_unique_identifier: "001dc10812580000".to_owned(),
            })
        );

        valid[8..16].fill(0);
        assert_eq!(parse_heartbeat_identity(&valid), None);
        valid[8..16].fill(u8::MAX);
        assert_eq!(parse_heartbeat_identity(&valid), None);
    }
}
