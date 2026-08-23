use serde::Serialize;

use crate::bytes::{read_u16, read_u32};
use crate::heartbeat::parse_heartbeat_records;

const CLOCK_FREQUENCY_OFFSET_RECORD_TYPE: u16 = 0x8001;
const CLOCK_FREQUENCY_OFFSET_MINIMUM_SIZE: usize = 0x10;
const CLOCK_FREQUENCY_OFFSET_EXTENSION_LENGTH: u16 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatClockFrequencyOffsetRecord {
    pub record_length: u16,
    pub extension_length: u16,
    pub payload_length: u16,
    pub sequence: u16,
    pub unknown_word_at_offset_10: u16,
    pub clock_frequency_offset_parts_per_billion: i32,
    pub trailing_payload: Vec<u8>,
}

fn parse_clock_frequency_offset_record(
    record: &[u8],
) -> Option<HeartbeatClockFrequencyOffsetRecord> {
    if record.len() < CLOCK_FREQUENCY_OFFSET_MINIMUM_SIZE {
        return None;
    }

    let record_length = read_u16(record, 0)?;
    let extension_length = read_u16(record, 4)?;
    let payload_length = read_u16(record, 6)?;
    if usize::from(record_length) != record.len()
        || record_length % 4 != 0
        || extension_length != CLOCK_FREQUENCY_OFFSET_EXTENSION_LENGTH
        || payload_length < 4
        || payload_length % 4 != 0
        || 8usize
            .checked_add(usize::from(extension_length))?
            .checked_add(usize::from(payload_length))?
            != record.len()
    {
        return None;
    }

    Some(HeartbeatClockFrequencyOffsetRecord {
        record_length,
        extension_length,
        payload_length,
        sequence: read_u16(record, 8)?,
        unknown_word_at_offset_10: read_u16(record, 10)?,
        clock_frequency_offset_parts_per_billion: read_u32(record, 12)? as i32,
        trailing_payload: record.get(16..)?.to_vec(),
    })
}

pub fn parse_heartbeat_clock_frequency_offset_packet(
    data: &[u8],
) -> Option<Vec<HeartbeatClockFrequencyOffsetRecord>> {
    let records = parse_heartbeat_records(data)?;
    Some(
        records
            .into_iter()
            .filter(|record| record.record_type == CLOCK_FREQUENCY_OFFSET_RECORD_TYPE)
            .filter_map(|record| parse_clock_frequency_offset_record(record.bytes))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heartbeat::{HEARTBEAT_HEADER_SIZE, HEARTBEAT_PROTOCOL};

    fn decode_hex(encoded: &str) -> Vec<u8> {
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
    fn authentic_a32_control_and_treatment_decode_as_signed_parts_per_billion() {
        let cases = [
            ("001080010004000419f80000fff9f9fb", -394_757),
            ("00108001000400041a1c0000fffc9bf2", -222_222),
        ];

        for (encoded, expected) in cases {
            let parsed =
                parse_heartbeat_clock_frequency_offset_packet(&packet(&decode_hex(encoded)))
                    .unwrap();
            assert_eq!(parsed.len(), 1);
            assert_eq!(parsed[0].clock_frequency_offset_parts_per_billion, expected);
            assert_eq!(parsed[0].trailing_payload, Vec::<u8>::new());
        }
    }

    #[test]
    fn longer_physical_record_preserves_unknown_trailing_payload() {
        let record = decode_hex("001c800100040010b5300000ffffb393000000000000000000000000");
        let parsed = parse_heartbeat_clock_frequency_offset_packet(&packet(&record)).unwrap();

        assert_eq!(parsed[0].clock_frequency_offset_parts_per_billion, -19_565);
        assert_eq!(parsed[0].trailing_payload, vec![0; 12]);
    }

    #[test]
    fn skips_unknown_and_malformed_target_records_without_partial_values() {
        let unknown = decode_hex("00048000");
        let malformed = decode_hex("001080010004000819f80000fff9f9fb");
        let valid = decode_hex("00108001000400041a1c0000fffc9bf2");
        let mut records = unknown;
        records.extend_from_slice(&malformed);
        records.extend_from_slice(&valid);

        let parsed = parse_heartbeat_clock_frequency_offset_packet(&packet(&records)).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].sequence, 0x1A1C);
    }
}
