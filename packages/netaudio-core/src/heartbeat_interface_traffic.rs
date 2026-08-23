use serde::Serialize;

use crate::bytes::{read_u16, read_u32};
use crate::heartbeat::parse_heartbeat_records;

const INTERFACE_TRAFFIC_RECORD_TYPE: u16 = 0x8000;
const INTERFACE_TRAFFIC_HEADER_SIZE: usize = 20;
const INTERFACE_TRAFFIC_EXTENSION_VALUE: u16 = 4;
const INTERFACE_TRAFFIC_PAYLOAD_VALUE: u16 = 4;
const INTERFACE_TRAFFIC_ENTRY_WIDTH: u16 = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatInterfaceTrafficEntry {
    pub entry_index: u16,
    pub transmit_octets: u32,
    pub receive_octets: u32,
    pub unknown_word_at_offset_8: u32,
    pub unknown_word_at_offset_12: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatInterfaceTrafficRecord {
    pub record_length: u16,
    pub extension_value: u16,
    pub payload_value: u16,
    pub sequence: u16,
    pub unknown_word_at_offset_10: u16,
    pub unknown_word_at_offset_12: u16,
    pub unknown_word_at_offset_14: u16,
    pub interface_entry_count: u16,
    pub interface_entry_width: u16,
    pub interfaces: Vec<HeartbeatInterfaceTrafficEntry>,
}

fn parse_interface_traffic_record(record: &[u8]) -> Option<HeartbeatInterfaceTrafficRecord> {
    if record.len() < INTERFACE_TRAFFIC_HEADER_SIZE {
        return None;
    }

    let record_length = read_u16(record, 0)?;
    let extension_value = read_u16(record, 4)?;
    let payload_value = read_u16(record, 6)?;
    let interface_entry_count = read_u16(record, 16)?;
    let interface_entry_width = read_u16(record, 18)?;
    let entries_size =
        usize::from(interface_entry_count).checked_mul(usize::from(interface_entry_width))?;
    let expected_record_length = INTERFACE_TRAFFIC_HEADER_SIZE.checked_add(entries_size)?;
    if usize::from(record_length) != record.len()
        || record_length % 4 != 0
        || extension_value != INTERFACE_TRAFFIC_EXTENSION_VALUE
        || payload_value != INTERFACE_TRAFFIC_PAYLOAD_VALUE
        || interface_entry_width != INTERFACE_TRAFFIC_ENTRY_WIDTH
        || expected_record_length != record.len()
    {
        return None;
    }

    let mut interfaces = Vec::with_capacity(usize::from(interface_entry_count));
    for entry_index in 0..interface_entry_count {
        let entry_offset = INTERFACE_TRAFFIC_HEADER_SIZE.checked_add(
            usize::from(entry_index).checked_mul(usize::from(interface_entry_width))?,
        )?;
        interfaces.push(HeartbeatInterfaceTrafficEntry {
            entry_index,
            transmit_octets: read_u32(record, entry_offset)?,
            receive_octets: read_u32(record, entry_offset + 4)?,
            unknown_word_at_offset_8: read_u32(record, entry_offset + 8)?,
            unknown_word_at_offset_12: read_u32(record, entry_offset + 12)?,
        });
    }

    Some(HeartbeatInterfaceTrafficRecord {
        record_length,
        extension_value,
        payload_value,
        sequence: read_u16(record, 8)?,
        unknown_word_at_offset_10: read_u16(record, 10)?,
        unknown_word_at_offset_12: read_u16(record, 12)?,
        unknown_word_at_offset_14: read_u16(record, 14)?,
        interface_entry_count,
        interface_entry_width,
        interfaces,
    })
}

pub fn parse_heartbeat_interface_traffic_packet(
    data: &[u8],
) -> Option<Vec<HeartbeatInterfaceTrafficRecord>> {
    let records = parse_heartbeat_records(data)?;
    Some(
        records
            .into_iter()
            .filter(|record| record.record_type == INTERFACE_TRAFFIC_RECORD_TYPE)
            .filter_map(|record| parse_interface_traffic_record(record.bytes))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heartbeat::{HEARTBEAT_HEADER_SIZE, HEARTBEAT_PROTOCOL};

    fn decode_hexadecimal(encoded: &str) -> Vec<u8> {
        encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16).unwrap();
                let low = (pair[1] as char).to_digit(16).unwrap();
                ((high << 4) | low) as u8
            })
            .collect()
    }

    fn packet(records: &[u8]) -> Vec<u8> {
        let length = HEARTBEAT_HEADER_SIZE + records.len();
        let mut data = vec![0; HEARTBEAT_HEADER_SIZE];
        data[0..2].copy_from_slice(&HEARTBEAT_PROTOCOL.to_be_bytes());
        data[2..4].copy_from_slice(&u16::try_from(length).unwrap().to_be_bytes());
        data.extend_from_slice(records);
        data
    }

    #[test]
    fn parses_causal_avio_baseline_and_treatment_records() {
        let cases = [
            (
                "0024800000040004a3e6000000100000000100100008562f000988fd0000000000000000",
                41958,
                546351,
                624893,
            ),
            (
                "0024800000040004a3e800000010000000010010000886cc000c4cac0000000000000000",
                41960,
                558796,
                806060,
            ),
        ];

        for (encoded, sequence, transmit_octets, receive_octets) in cases {
            let parsed =
                parse_heartbeat_interface_traffic_packet(&packet(&decode_hexadecimal(encoded)))
                    .unwrap();
            assert_eq!(parsed.len(), 1);
            assert_eq!(parsed[0].sequence, sequence);
            assert_eq!(parsed[0].interface_entry_count, 1);
            assert_eq!(parsed[0].interfaces[0].transmit_octets, transmit_octets);
            assert_eq!(parsed[0].interfaces[0].receive_octets, receive_octets);
        }
    }

    #[test]
    fn parses_two_interface_lx_dante_record() {
        let record = decode_hexadecimal(
            "00348000000400045dcf00000010000000020010002067a20014ccb2000000000000000000000000000000000000000000000000",
        );
        let parsed = parse_heartbeat_interface_traffic_packet(&packet(&record)).unwrap();

        assert_eq!(parsed[0].interface_entry_count, 2);
        assert_eq!(parsed[0].interfaces.len(), 2);
        assert_eq!(parsed[0].interfaces[0].transmit_octets, 2123682);
        assert_eq!(parsed[0].interfaces[0].receive_octets, 1363122);
        assert_eq!(parsed[0].interfaces[1].transmit_octets, 0);
        assert_eq!(parsed[0].interfaces[1].receive_octets, 0);
    }

    #[test]
    fn rejects_malformed_target_geometry_without_partial_values() {
        let valid = decode_hexadecimal(
            "0024800000040004a3e6000000100000000100100008562f000988fd0000000000000000",
        );
        for (offset, value) in [(16usize, 2u16), (18, 12)] {
            let mut malformed = valid.clone();
            malformed[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
            let parsed = parse_heartbeat_interface_traffic_packet(&packet(&malformed)).unwrap();
            assert!(parsed.is_empty());
        }
    }
}
