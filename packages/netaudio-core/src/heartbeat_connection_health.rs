use serde::Serialize;
use std::collections::BTreeSet;

use crate::bytes::{read_u16, read_u32};
use crate::heartbeat::{
    parse_heartbeat_device_extended_unique_identifier, parse_heartbeat_records,
};

const FLOW_LATENCY_RECORD_TYPE: u16 = 0x8003;
const RAW_IMPAIRMENT_RECORD_TYPE: u16 = 0x8004;
const EXTENSION_LENGTH: u16 = 4;
const FLOW_LATENCY_VECTOR_OFFSET: u16 = 24;
const RAW_IMPAIRMENT_VECTOR_OFFSET: u16 = 20;
const VECTOR_ENTRY_WIDTH: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatFlowLatencyEntry {
    pub receiver_flow_index: u16,
    pub latency_sample_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatFlowLatencyRecord {
    pub record_length: u16,
    pub extension_length: u16,
    pub payload_length: u16,
    pub sequence: u16,
    pub unknown_word_at_offset_10: u16,
    pub entry_count: u16,
    pub start_receiver_flow_index: u16,
    pub vector_offset: u16,
    pub unknown_word_at_offset_18: u16,
    pub sample_rate_hertz: u32,
    pub entries: Vec<HeartbeatFlowLatencyEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatRawImpairmentEntry {
    pub receiver_flow_index: u16,
    pub raw_impairment_value: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatRawImpairmentRecord {
    pub record_length: u16,
    pub extension_length: u16,
    pub payload_length: u16,
    pub sequence: u16,
    pub unknown_word_at_offset_10: u16,
    pub entry_count: u16,
    pub start_receiver_flow_index: u16,
    pub vector_offset: u16,
    pub unknown_word_at_offset_18: u16,
    pub entries: Vec<HeartbeatRawImpairmentEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatConnectionHealthRecords {
    pub device_extended_unique_identifier: String,
    pub latency_records: Vec<HeartbeatFlowLatencyRecord>,
    pub raw_impairment_records: Vec<HeartbeatRawImpairmentRecord>,
}

fn validate_vector_geometry(
    record: &[u8],
    minimum_record_length: usize,
    expected_vector_offset: u16,
) -> Option<(u16, u16, u16, u16)> {
    if record.len() < minimum_record_length {
        return None;
    }

    let record_length = read_u16(record, 0)?;
    let extension_length = read_u16(record, 4)?;
    let payload_length = read_u16(record, 6)?;
    let entry_count = read_u16(record, 12)?;
    let start_receiver_flow_index = read_u16(record, 14)?;
    let vector_offset = read_u16(record, 16)?;
    let vector_length = usize::from(entry_count).checked_mul(VECTOR_ENTRY_WIDTH)?;
    let expected_record_length = usize::from(vector_offset).checked_add(vector_length)?;
    let flow_index_end =
        u32::from(start_receiver_flow_index).checked_add(u32::from(entry_count))?;

    if usize::from(record_length) != record.len()
        || record_length % 4 != 0
        || extension_length != EXTENSION_LENGTH
        || 8usize
            .checked_add(usize::from(extension_length))?
            .checked_add(usize::from(payload_length))?
            != record.len()
        || vector_offset != expected_vector_offset
        || expected_record_length != record.len()
        || entry_count == 0
        || flow_index_end > u32::from(u16::MAX) + 1
    {
        return None;
    }

    Some((record_length, extension_length, payload_length, entry_count))
}

fn parse_flow_latency_record(record: &[u8]) -> Option<HeartbeatFlowLatencyRecord> {
    let (record_length, extension_length, payload_length, entry_count) =
        validate_vector_geometry(record, 24, FLOW_LATENCY_VECTOR_OFFSET)?;
    let start_receiver_flow_index = read_u16(record, 14)?;
    let mut entries = Vec::with_capacity(usize::from(entry_count));
    for entry_index in 0..entry_count {
        let entry_offset = usize::from(FLOW_LATENCY_VECTOR_OFFSET)
            .checked_add(usize::from(entry_index).checked_mul(VECTOR_ENTRY_WIDTH)?)?;
        entries.push(HeartbeatFlowLatencyEntry {
            receiver_flow_index: start_receiver_flow_index.checked_add(entry_index)?,
            latency_sample_count: read_u32(record, entry_offset)?,
        });
    }

    Some(HeartbeatFlowLatencyRecord {
        record_length,
        extension_length,
        payload_length,
        sequence: read_u16(record, 8)?,
        unknown_word_at_offset_10: read_u16(record, 10)?,
        entry_count,
        start_receiver_flow_index,
        vector_offset: read_u16(record, 16)?,
        unknown_word_at_offset_18: read_u16(record, 18)?,
        sample_rate_hertz: read_u32(record, 20)?,
        entries,
    })
}

fn parse_raw_impairment_record(record: &[u8]) -> Option<HeartbeatRawImpairmentRecord> {
    let (record_length, extension_length, payload_length, entry_count) =
        validate_vector_geometry(record, 20, RAW_IMPAIRMENT_VECTOR_OFFSET)?;
    let start_receiver_flow_index = read_u16(record, 14)?;
    let mut entries = Vec::with_capacity(usize::from(entry_count));
    for entry_index in 0..entry_count {
        let entry_offset = usize::from(RAW_IMPAIRMENT_VECTOR_OFFSET)
            .checked_add(usize::from(entry_index).checked_mul(VECTOR_ENTRY_WIDTH)?)?;
        entries.push(HeartbeatRawImpairmentEntry {
            receiver_flow_index: start_receiver_flow_index.checked_add(entry_index)?,
            raw_impairment_value: read_u32(record, entry_offset)?,
        });
    }

    Some(HeartbeatRawImpairmentRecord {
        record_length,
        extension_length,
        payload_length,
        sequence: read_u16(record, 8)?,
        unknown_word_at_offset_10: read_u16(record, 10)?,
        entry_count,
        start_receiver_flow_index,
        vector_offset: read_u16(record, 16)?,
        unknown_word_at_offset_18: read_u16(record, 18)?,
        entries,
    })
}

fn paired_record_keys(
    latency_records: &[HeartbeatFlowLatencyRecord],
    raw_impairment_records: &[HeartbeatRawImpairmentRecord],
) -> Option<BTreeSet<(u16, u16, u16)>> {
    if latency_records.is_empty() || latency_records.len() != raw_impairment_records.len() {
        return None;
    }

    let mut latency_keys = BTreeSet::new();
    for record in latency_records {
        if record.sample_rate_hertz == 0
            || !latency_keys.insert((
                record.sequence,
                record.start_receiver_flow_index,
                record.entry_count,
            ))
        {
            return None;
        }
    }

    let mut raw_impairment_keys = BTreeSet::new();
    for record in raw_impairment_records {
        if !raw_impairment_keys.insert((
            record.sequence,
            record.start_receiver_flow_index,
            record.entry_count,
        )) {
            return None;
        }
    }
    if latency_keys != raw_impairment_keys {
        return None;
    }

    let mut sequence = None;
    let mut previous_range_end = None;
    for (record_sequence, start_receiver_flow_index, entry_count) in &latency_keys {
        if sequence.is_some_and(|value| value != *record_sequence) {
            return None;
        }
        sequence = Some(*record_sequence);
        if previous_range_end
            .is_some_and(|range_end| u32::from(*start_receiver_flow_index) < range_end)
        {
            return None;
        }
        previous_range_end = Some(u32::from(*start_receiver_flow_index) + u32::from(*entry_count));
    }

    Some(latency_keys)
}

pub fn parse_heartbeat_connection_health_packet(
    data: &[u8],
) -> Option<HeartbeatConnectionHealthRecords> {
    let records = parse_heartbeat_records(data)?;
    let mut latency_records = Vec::new();
    let mut raw_impairment_records = Vec::new();

    for record in records {
        match record.record_type {
            FLOW_LATENCY_RECORD_TYPE => {
                latency_records.push(parse_flow_latency_record(record.bytes)?);
            }
            RAW_IMPAIRMENT_RECORD_TYPE => {
                raw_impairment_records.push(parse_raw_impairment_record(record.bytes)?);
            }
            _ => {}
        }
    }

    paired_record_keys(&latency_records, &raw_impairment_records)?;

    Some(HeartbeatConnectionHealthRecords {
        device_extended_unique_identifier: parse_heartbeat_device_extended_unique_identifier(data)?,
        latency_records,
        raw_impairment_records,
    })
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
        data[8..16].copy_from_slice(&decode_hexadecimal("001dc1fffe50368b"));
        data.extend_from_slice(records);
        data
    }

    fn paired_records(sequence: u16, latency: [u32; 2], raw_impairment: [u32; 2]) -> Vec<u8> {
        let mut latency_record =
            decode_hexadecimal("00208003000400140000000000020000001800000000bb800000000000000000");
        latency_record[8..10].copy_from_slice(&sequence.to_be_bytes());
        latency_record[24..28].copy_from_slice(&latency[0].to_be_bytes());
        latency_record[28..32].copy_from_slice(&latency[1].to_be_bytes());

        let mut raw_impairment_record =
            decode_hexadecimal("001c8004000400100000000000020000001400000000000000000000");
        raw_impairment_record[8..10].copy_from_slice(&sequence.to_be_bytes());
        raw_impairment_record[20..24].copy_from_slice(&raw_impairment[0].to_be_bytes());
        raw_impairment_record[24..28].copy_from_slice(&raw_impairment[1].to_be_bytes());
        latency_record.extend_from_slice(&raw_impairment_record);
        latency_record
    }

    #[test]
    fn reproduces_baseline_treatment_and_latched_raw_impairment_records() {
        let cases = [
            (41130, [14, 0], [0, 0]),
            (41132, [1006, 0], [825, 0]),
            (41133, [14, 0], [825, 0]),
        ];

        for (sequence, latency, raw_impairment) in cases {
            let parsed = parse_heartbeat_connection_health_packet(&packet(&paired_records(
                sequence,
                latency,
                raw_impairment,
            )))
            .unwrap();

            assert_eq!(parsed.latency_records[0].sequence, sequence);
            assert_eq!(parsed.device_extended_unique_identifier, "001dc1fffe50368b");
            assert_eq!(parsed.latency_records[0].sample_rate_hertz, 48_000);
            assert_eq!(
                parsed.latency_records[0].entries[0].latency_sample_count,
                latency[0]
            );
            assert_eq!(
                parsed.raw_impairment_records[0].entries[0].raw_impairment_value,
                raw_impairment[0]
            );
        }
    }

    #[test]
    fn preserves_zero_based_receiver_flow_indices() {
        let mut records = paired_records(0x406F, [0, 18], [0, 7]);
        records[14..16].copy_from_slice(&7u16.to_be_bytes());
        records[32 + 14..32 + 16].copy_from_slice(&7u16.to_be_bytes());
        let parsed = parse_heartbeat_connection_health_packet(&packet(&records)).unwrap();

        assert_eq!(parsed.latency_records[0].entries[0].receiver_flow_index, 7);
        assert_eq!(parsed.latency_records[0].entries[1].receiver_flow_index, 8);
        assert_eq!(
            parsed.raw_impairment_records[0].entries[1].receiver_flow_index,
            8
        );
    }

    #[test]
    fn rejects_malformed_target_geometry_without_partial_values() {
        let valid = paired_records(41132, [1006, 0], [825, 0]);
        for (offset, value) in [(16usize, 20u16), (32 + 12, 3), (32 + 16, 24)] {
            let mut malformed = valid.clone();
            malformed[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
            assert_eq!(
                parse_heartbeat_connection_health_packet(&packet(&malformed)),
                None
            );
        }
    }

    #[test]
    fn rejects_empty_zero_rate_unpaired_and_overlapping_vectors() {
        let valid = paired_records(41132, [1006, 0], [825, 0]);

        let mut zero_count = valid.clone();
        zero_count[12..14].copy_from_slice(&0u16.to_be_bytes());
        assert_eq!(
            parse_heartbeat_connection_health_packet(&packet(&zero_count)),
            None
        );

        let mut zero_rate = valid.clone();
        zero_rate[20..24].copy_from_slice(&0u32.to_be_bytes());
        assert_eq!(
            parse_heartbeat_connection_health_packet(&packet(&zero_rate)),
            None
        );

        assert_eq!(
            parse_heartbeat_connection_health_packet(&packet(&valid[..32])),
            None
        );

        let mut overlapping = valid.clone();
        let mut second_pair = paired_records(41132, [18, 0], [7, 0]);
        second_pair[14..16].copy_from_slice(&1u16.to_be_bytes());
        second_pair[32 + 14..32 + 16].copy_from_slice(&1u16.to_be_bytes());
        overlapping.extend_from_slice(&second_pair);
        assert_eq!(
            parse_heartbeat_connection_health_packet(&packet(&overlapping)),
            None
        );

        let mut missing_identity = packet(&valid);
        missing_identity[8..16].fill(0);
        assert_eq!(
            parse_heartbeat_connection_health_packet(&missing_identity),
            None
        );
    }
}
