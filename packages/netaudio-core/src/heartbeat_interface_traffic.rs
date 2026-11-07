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
    pub transmit_rate_raw: u32,
    pub receive_rate_raw: u32,
    pub transmit_rate_bits_per_second: u64,
    pub receive_rate_bits_per_second: u64,
    pub transmit_error_count: u32,
    pub receive_error_count: u32,
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
        let transmit_rate_raw = read_u32(record, entry_offset)?;
        let receive_rate_raw = read_u32(record, entry_offset + 4)?;
        interfaces.push(HeartbeatInterfaceTrafficEntry {
            entry_index,
            transmit_rate_raw,
            receive_rate_raw,
            transmit_rate_bits_per_second: u64::from(transmit_rate_raw) * 8,
            receive_rate_bits_per_second: u64::from(receive_rate_raw) * 8,
            transmit_error_count: read_u32(record, entry_offset + 8)?,
            receive_error_count: read_u32(record, entry_offset + 12)?,
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
