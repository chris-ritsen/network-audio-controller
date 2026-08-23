use serde::Serialize;

use crate::bytes::read_u16;
use crate::heartbeat::parse_heartbeat_records;

const SIGNAL_PRESENCE_RECORD_TYPE: u16 = 0x8002;
const SIGNAL_PRESENCE_FIXED_SIZE: usize = 0x18;
const SIGNAL_PRESENCE_EXTENSION_LENGTH: u16 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SignalPresenceRecord {
    pub record_length: u16,
    pub extension_length: u16,
    pub payload_length: u16,
    pub sequence: u16,
    pub tx_count: u16,
    pub tx_first_channel_index: u16,
    pub rx_count: u16,
    pub rx_first_channel_index: u16,
    pub level_vector_offset: u16,
    pub tx_levels: Vec<u8>,
    pub rx_levels: Vec<u8>,
    pub padding_length: u16,
}

fn align_to_four(value: usize) -> Option<usize> {
    Some(value.checked_add(3)? & !3)
}

fn channel_range_fits(first_channel: u16, count: u16) -> bool {
    u32::from(first_channel) + u32::from(count) <= u32::from(u16::MAX) + 1
}

fn parse_signal_presence_record(record: &[u8]) -> Option<SignalPresenceRecord> {
    if record.len() < SIGNAL_PRESENCE_FIXED_SIZE {
        return None;
    }

    let record_length = read_u16(record, 0)?;
    let extension_length = read_u16(record, 4)?;
    let payload_length = read_u16(record, 6)?;
    if usize::from(record_length) != record.len()
        || record_length % 4 != 0
        || extension_length != SIGNAL_PRESENCE_EXTENSION_LENGTH
        || payload_length % 4 != 0
        || 8usize
            .checked_add(usize::from(extension_length))?
            .checked_add(usize::from(payload_length))?
            != record.len()
        || read_u16(record, 10)? != 0
        || read_u16(record, 22)? != 0
    {
        return None;
    }

    let tx_count = read_u16(record, 12)?;
    let tx_first_channel_index = read_u16(record, 14)?;
    let rx_count = read_u16(record, 16)?;
    let rx_first_channel_index = read_u16(record, 18)?;
    if !channel_range_fits(tx_first_channel_index, tx_count)
        || !channel_range_fits(rx_first_channel_index, rx_count)
    {
        return None;
    }

    let level_vector_offset = read_u16(record, 20)?;
    let level_start = usize::from(level_vector_offset);
    if level_start < SIGNAL_PRESENCE_FIXED_SIZE || level_start > record.len() {
        return None;
    }

    let tx_end = level_start.checked_add(usize::from(tx_count))?;
    let rx_end = tx_end.checked_add(usize::from(rx_count))?;
    if align_to_four(rx_end)? != record.len() {
        return None;
    }

    let tx_levels = record.get(level_start..tx_end)?.to_vec();
    let rx_levels = record.get(tx_end..rx_end)?.to_vec();
    let padding_length = u16::try_from(record.len().checked_sub(rx_end)?).ok()?;

    Some(SignalPresenceRecord {
        record_length,
        extension_length,
        payload_length,
        sequence: read_u16(record, 8)?,
        tx_count,
        tx_first_channel_index,
        rx_count,
        rx_first_channel_index,
        level_vector_offset,
        tx_levels,
        rx_levels,
        padding_length,
    })
}

pub fn parse_signal_presence_packet(data: &[u8]) -> Option<Vec<SignalPresenceRecord>> {
    Some(
        parse_heartbeat_records(data)?
            .into_iter()
            .filter(|record| record.record_type == SIGNAL_PRESENCE_RECORD_TYPE)
            .filter_map(|record| parse_signal_presence_record(record.bytes))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn packet(record_bytes: &[u8]) -> Vec<u8> {
        let length = crate::heartbeat::HEARTBEAT_HEADER_SIZE + record_bytes.len();
        let mut data = vec![0; crate::heartbeat::HEARTBEAT_HEADER_SIZE];
        data[0..2].copy_from_slice(&crate::heartbeat::HEARTBEAT_PROTOCOL.to_be_bytes());
        data[2..4].copy_from_slice(&u16::try_from(length).unwrap().to_be_bytes());
        data.extend_from_slice(record_bytes);
        data
    }

    #[test]
    fn physical_vectors_preserve_direction_order_counts_and_padding() {
        let cases = [
            (
                "001c8002000400104dec0000000200000001000000180000fefe6d00",
                vec![0xFE, 0xFE],
                vec![0x6D],
                1,
            ),
            (
                "001c800200040010473d0000000200000000000000180000a3a40000",
                vec![0xA3, 0xA4],
                vec![],
                2,
            ),
            (
                "001c80020004001047360000000000000002000000180000fdfd0000",
                vec![],
                vec![0xFD, 0xFD],
                2,
            ),
            (
                "001c8002000400107bb20000000200000002000000180000c1c1fdfd",
                vec![0xC1, 0xC1],
                vec![0xFD, 0xFD],
                0,
            ),
        ];

        for (encoded, expected_tx, expected_rx, expected_padding) in cases {
            let parsed = parse_signal_presence_packet(&packet(&decode_hex(encoded))).unwrap();
            assert_eq!(parsed.len(), 1);
            assert_eq!(parsed[0].tx_levels, expected_tx);
            assert_eq!(parsed[0].rx_levels, expected_rx);
            assert_eq!(parsed[0].padding_length, expected_padding);
        }
    }

    #[test]
    fn unknown_records_are_skipped_and_multiple_signal_records_are_preserved() {
        let first = decode_hex("001c8002000400100001000000010000000000000018000011000000");
        let second = decode_hex("001c8002000400100002000000000000000100000018000022000000");
        let unknown = decode_hex("00048001");
        let mut records = unknown.clone();
        records.extend_from_slice(&first);
        records.extend_from_slice(&unknown);
        records.extend_from_slice(&second);
        records.extend_from_slice(&unknown);

        let parsed = parse_signal_presence_packet(&packet(&records)).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].sequence, 1);
        assert_eq!(parsed[0].tx_levels, vec![0x11]);
        assert_eq!(parsed[1].sequence, 2);
        assert_eq!(parsed[1].rx_levels, vec![0x22]);
    }

    #[test]
    fn malformed_signal_record_is_ignored_without_partial_values() {
        let mut malformed = decode_hex("001c8002000400104dec0000000200000001000000180000fefe6d00");
        malformed[16..18].copy_from_slice(&3u16.to_be_bytes());
        let valid = decode_hex("001c80020004001047360000000000000002000000180000fdfd0000");
        malformed.extend_from_slice(&valid);

        let parsed = parse_signal_presence_packet(&packet(&malformed)).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].sequence, 0x4736);
    }

    #[test]
    fn malformed_outer_or_record_lengths_are_rejected() {
        let record = decode_hex("001c8002000400104dec0000000200000001000000180000fefe6d00");
        let original = packet(&record);

        for length in 0..crate::heartbeat::HEARTBEAT_HEADER_SIZE {
            assert_eq!(parse_signal_presence_packet(&original[..length]), None);
        }

        let mut wrong_outer_length = original.clone();
        wrong_outer_length[2..4].copy_from_slice(&0x20u16.to_be_bytes());
        assert_eq!(parse_signal_presence_packet(&wrong_outer_length), None);

        for record_length in [0u16, 3, 27, 32] {
            let mut malformed = original.clone();
            malformed[crate::heartbeat::HEARTBEAT_HEADER_SIZE
                ..crate::heartbeat::HEARTBEAT_HEADER_SIZE + 2]
                .copy_from_slice(&record_length.to_be_bytes());
            assert_eq!(parse_signal_presence_packet(&malformed), None);
        }
    }

    #[test]
    fn invalid_payload_lengths_offsets_and_counts_are_ignored() {
        let record = decode_hex("001c8002000400104dec0000000200000001000000180000fefe6d00");
        let cases = [
            (4usize, 3u16),
            (6, 12),
            (20, 22),
            (20, 32),
            (12, u16::MAX),
            (16, u16::MAX),
        ];

        for (field_offset, value) in cases {
            let mut malformed = record.clone();
            malformed[field_offset..field_offset + 2].copy_from_slice(&value.to_be_bytes());
            assert_eq!(
                parse_signal_presence_packet(&packet(&malformed)),
                Some(vec![])
            );
        }
    }

    #[test]
    fn raw_boundary_values_are_not_collapsed() {
        let record = decode_hex("001c8002000400100001000000040000000000000018000000fdfeff");
        let parsed = parse_signal_presence_packet(&packet(&record)).unwrap();
        assert_eq!(parsed[0].tx_levels, vec![0x00, 0xFD, 0xFE, 0xFF]);
    }
}
