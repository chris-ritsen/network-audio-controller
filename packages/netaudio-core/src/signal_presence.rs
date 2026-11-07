use serde::Serialize;

use crate::bytes::read_u16;
use crate::heartbeat::parse_heartbeat_records;

const SIGNAL_PRESENCE_RECORD_TYPE: u16 = 0x8002;
const SIGNAL_PRESENCE_FIXED_SIZE: usize = 0x18;
const SIGNAL_PRESENCE_EXTENSION_LENGTH: u16 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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
