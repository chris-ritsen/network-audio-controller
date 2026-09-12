use crate::bytes::{read_u16, read_u32};
use crate::heartbeat::{
    parse_heartbeat_device_extended_unique_identifier, parse_heartbeat_records,
};
use serde::Serialize;

const FLOW_LATENCY_RECORD_TYPE: u16 = 0x8003;
const LATE_PACKET_RECORD_TYPE: u16 = 0x8004;
const EXTENSION_LENGTH: u16 = 4;
const FLOW_LATENCY_VECTOR_OFFSET: u16 = 24;
const LATE_PACKET_VECTOR_OFFSET: u16 = 20;
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
pub struct HeartbeatLatePacketEntry {
    pub receiver_flow_index: u16,
    pub late_packet_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatLatePacketRecord {
    pub record_length: u16,
    pub extension_length: u16,
    pub payload_length: u16,
    pub sequence: u16,
    pub unknown_word_at_offset_10: u16,
    pub entry_count: u16,
    pub start_receiver_flow_index: u16,
    pub vector_offset: u16,
    pub unknown_word_at_offset_18: u16,
    pub entries: Vec<HeartbeatLatePacketEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatConnectionHealthRecords {
    pub device_extended_unique_identifier: String,
    pub latency_records: Vec<HeartbeatFlowLatencyRecord>,
    pub late_packet_records: Vec<HeartbeatLatePacketRecord>,
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
    let sample_rate_hertz = read_u32(record, 20)?;
    if sample_rate_hertz == 0 {
        return None;
    }
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
        sample_rate_hertz,
        entries,
    })
}

fn parse_late_packet_record(record: &[u8]) -> Option<HeartbeatLatePacketRecord> {
    let (record_length, extension_length, payload_length, entry_count) =
        validate_vector_geometry(record, 20, LATE_PACKET_VECTOR_OFFSET)?;
    let start_receiver_flow_index = read_u16(record, 14)?;
    let mut entries = Vec::with_capacity(usize::from(entry_count));
    for entry_index in 0..entry_count {
        let entry_offset = usize::from(LATE_PACKET_VECTOR_OFFSET)
            .checked_add(usize::from(entry_index).checked_mul(VECTOR_ENTRY_WIDTH)?)?;
        entries.push(HeartbeatLatePacketEntry {
            receiver_flow_index: start_receiver_flow_index.checked_add(entry_index)?,
            late_packet_count: read_u32(record, entry_offset)?,
        });
    }

    Some(HeartbeatLatePacketRecord {
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

pub fn parse_heartbeat_connection_health_packet(
    data: &[u8],
) -> Option<HeartbeatConnectionHealthRecords> {
    let records = parse_heartbeat_records(data)?;
    let mut latency_records = Vec::new();
    let mut late_packet_records = Vec::new();

    for record in records {
        match record.record_type {
            FLOW_LATENCY_RECORD_TYPE => {
                latency_records.push(parse_flow_latency_record(record.bytes)?);
            }
            LATE_PACKET_RECORD_TYPE => {
                late_packet_records.push(parse_late_packet_record(record.bytes)?);
            }
            _ => {}
        }
    }

    if latency_records.is_empty() && late_packet_records.is_empty() {
        return None;
    }

    Some(HeartbeatConnectionHealthRecords {
        device_extended_unique_identifier: parse_heartbeat_device_extended_unique_identifier(data)?,
        latency_records,
        late_packet_records,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heartbeat::{HEARTBEAT_HEADER_SIZE, HEARTBEAT_PROTOCOL};
    use crate::test_support::decode_hexadecimal;

    fn packet(records: &[u8]) -> Vec<u8> {
        let length = HEARTBEAT_HEADER_SIZE + records.len();
        let mut data = vec![0; HEARTBEAT_HEADER_SIZE];
        data[0..2].copy_from_slice(&HEARTBEAT_PROTOCOL.to_be_bytes());
        data[2..4].copy_from_slice(&u16::try_from(length).unwrap().to_be_bytes());
        data[8..16].copy_from_slice(&decode_hexadecimal("001dc1fffe50368b"));
        data.extend_from_slice(records);
        data
    }

    fn paired_records(sequence: u16, latency: [u32; 2], late_packet_counts: [u32; 2]) -> Vec<u8> {
        let mut latency_record =
            decode_hexadecimal("00208003000400140000000000020000001800000000bb800000000000000000");
        latency_record[8..10].copy_from_slice(&sequence.to_be_bytes());
        latency_record[24..28].copy_from_slice(&latency[0].to_be_bytes());
        latency_record[28..32].copy_from_slice(&latency[1].to_be_bytes());

        let mut late_packet_record =
            decode_hexadecimal("001c8004000400100000000000020000001400000000000000000000");
        late_packet_record[8..10].copy_from_slice(&sequence.to_be_bytes());
        late_packet_record[20..24].copy_from_slice(&late_packet_counts[0].to_be_bytes());
        late_packet_record[24..28].copy_from_slice(&late_packet_counts[1].to_be_bytes());
        latency_record.extend_from_slice(&late_packet_record);
        latency_record
    }

    #[test]
    fn reproduces_baseline_treatment_and_cumulative_late_packet_records() {
        let cases = [
            (41130, [14, 0], [0, 0]),
            (41132, [1006, 0], [825, 0]),
            (41133, [14, 0], [825, 0]),
        ];

        for (sequence, latency, late_packet_counts) in cases {
            let parsed = parse_heartbeat_connection_health_packet(&packet(&paired_records(
                sequence,
                latency,
                late_packet_counts,
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
                parsed.late_packet_records[0].entries[0].late_packet_count,
                late_packet_counts[0]
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
            parsed.late_packet_records[0].entries[1].receiver_flow_index,
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
    fn accepts_independent_record_families_and_rejects_invalid_records() {
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

        let latency_only = parse_heartbeat_connection_health_packet(&packet(&valid[..32])).unwrap();
        assert_eq!(latency_only.latency_records.len(), 1);
        assert!(latency_only.late_packet_records.is_empty());

        let late_packet_only =
            parse_heartbeat_connection_health_packet(&packet(&valid[32..])).unwrap();
        assert!(late_packet_only.latency_records.is_empty());
        assert_eq!(late_packet_only.late_packet_records.len(), 1);

        let mut overlapping = valid.clone();
        let mut second_pair = paired_records(41132, [18, 0], [7, 0]);
        second_pair[14..16].copy_from_slice(&1u16.to_be_bytes());
        second_pair[32 + 14..32 + 16].copy_from_slice(&1u16.to_be_bytes());
        overlapping.extend_from_slice(&second_pair);
        let overlapping = parse_heartbeat_connection_health_packet(&packet(&overlapping)).unwrap();
        assert_eq!(overlapping.latency_records.len(), 2);
        assert_eq!(overlapping.late_packet_records.len(), 2);

        let mut missing_identity = packet(&valid);
        missing_identity[8..16].fill(0);
        assert_eq!(
            parse_heartbeat_connection_health_packet(&missing_identity),
            None
        );
    }
}
