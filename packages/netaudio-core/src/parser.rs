use serde::Serialize;

use crate::bytes::{read_u16, string_at_pointer, u16_at};

pub const RESPONSE_HEADER_SIZE: usize = 10;
pub const BODY_HEADER_SIZE: usize = 2;
pub const RX_RECORD_SIZE: usize = 20;
pub const TX_RECORD_SIZE: usize = 8;
pub const TX_FRIENDLY_RECORD_SIZE: usize = 6;
pub const RX_CHANNELS_PER_PAGE: u16 = 16;
pub const TX_CHANNELS_PER_PAGE: u16 = 32;

const CHANNEL_COUNT_TX_OFFSET: usize = 13;
const CHANNEL_COUNT_RX_OFFSET: usize = 15;
const CHANNEL_COUNT_LOCK_OFFSET: usize = 34;

const RX_RECORD_CHANNEL_NUMBER: usize = 0;
const RX_RECORD_TX_CHANNEL_POINTER: usize = 6;
const RX_RECORD_TX_DEVICE_POINTER: usize = 8;
const RX_RECORD_RX_CHANNEL_POINTER: usize = 10;
const RX_RECORD_RX_STATUS: usize = 12;
const RX_RECORD_SUBSCRIPTION_STATUS: usize = 14;

const TX_RECORD_CHANNEL_NUMBER: usize = 0;
const TX_RECORD_CHANNEL_GROUP: usize = 4;
const TX_RECORD_NAME_POINTER: usize = 6;

const TX_FRIENDLY_RECORD_CHANNEL_NUMBER: usize = 2;
const TX_FRIENDLY_RECORD_NAME_POINTER: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelCount {
    pub tx_count: u16,
    pub rx_count: u16,
    pub locked: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RxChannel {
    pub number: u16,
    pub rx_channel_name: Option<String>,
    pub tx_channel_name: Option<String>,
    pub tx_device_name: Option<String>,
    pub rx_status_code: u16,
    pub subscription_status_code: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TxChannel {
    pub number: u16,
    pub name: Option<String>,
    pub friendly_name: Option<String>,
}

pub fn parse_channel_count(response: &[u8]) -> Option<ChannelCount> {
    if response.len() < 16 {
        return None;
    }
    Some(ChannelCount {
        tx_count: response[CHANNEL_COUNT_TX_OFFSET] as u16,
        rx_count: response[CHANNEL_COUNT_RX_OFFSET] as u16,
        locked: read_u16(response, CHANNEL_COUNT_LOCK_OFFSET).map(|flags| flags != 0),
    })
}

fn page_records(response: &[u8], record_size: usize, records_per_page: u16) -> impl Iterator<Item = &[u8]> {
    let body = response.get(RESPONSE_HEADER_SIZE..).unwrap_or(&[]);
    (0..records_per_page as usize).map_while(move |index| {
        let record_offset = BODY_HEADER_SIZE + index * record_size;
        body.get(record_offset..record_offset + record_size)
    })
}

pub fn parse_rx_page(response: &[u8], starting_channel: u16) -> Vec<RxChannel> {
    let mut channels = Vec::new();

    for (index, record) in page_records(response, RX_RECORD_SIZE, RX_CHANNELS_PER_PAGE).enumerate() {
        let channel_number = u16_at(record, RX_RECORD_CHANNEL_NUMBER);
        let expected = starting_channel + index as u16;
        if channel_number == 0 || channel_number != expected {
            break;
        }

        let tx_channel_pointer = u16_at(record, RX_RECORD_TX_CHANNEL_POINTER);
        let tx_device_pointer = u16_at(record, RX_RECORD_TX_DEVICE_POINTER);
        let rx_channel_pointer = u16_at(record, RX_RECORD_RX_CHANNEL_POINTER);

        let rx_channel_name = string_at_pointer(response, rx_channel_pointer);
        let tx_device_name = string_at_pointer(response, tx_device_pointer);
        let tx_channel_name = if tx_channel_pointer != 0 {
            string_at_pointer(response, tx_channel_pointer)
        } else {
            rx_channel_name.clone()
        };

        channels.push(RxChannel {
            number: channel_number,
            rx_channel_name,
            tx_channel_name,
            tx_device_name,
            rx_status_code: u16_at(record, RX_RECORD_RX_STATUS),
            subscription_status_code: u16_at(record, RX_RECORD_SUBSCRIPTION_STATUS),
        });
    }

    channels
}

pub fn parse_tx_friendly_page(response: &[u8]) -> Vec<(u16, String)> {
    let mut names = Vec::new();

    for record in page_records(response, TX_FRIENDLY_RECORD_SIZE, TX_CHANNELS_PER_PAGE) {
        let channel_number = u16_at(record, TX_FRIENDLY_RECORD_CHANNEL_NUMBER);
        if channel_number == 0 {
            break;
        }
        let name_pointer = u16_at(record, TX_FRIENDLY_RECORD_NAME_POINTER);
        if let Some(friendly_name) = string_at_pointer(response, name_pointer) {
            names.push((channel_number, friendly_name));
        }
    }

    names
}

pub fn parse_tx_info_page(response: &[u8], starting_channel: u16) -> Vec<TxChannel> {
    let mut channels = Vec::new();
    let mut first_channel_group: Option<u16> = None;

    for (index, record) in page_records(response, TX_RECORD_SIZE, TX_CHANNELS_PER_PAGE).enumerate() {
        let channel_number = u16_at(record, TX_RECORD_CHANNEL_NUMBER);
        let expected = starting_channel + index as u16;
        if channel_number == 0 || channel_number != expected {
            break;
        }

        let channel_group = u16_at(record, TX_RECORD_CHANNEL_GROUP);
        match first_channel_group {
            None => first_channel_group = Some(channel_group),
            Some(group) if channel_group != group => break,
            _ => {}
        }

        let name_pointer = u16_at(record, TX_RECORD_NAME_POINTER);
        channels.push(TxChannel {
            number: channel_number,
            name: string_at_pointer(response, name_pointer),
            friendly_name: None,
        });
    }

    channels
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_count_parser_reads_counts_and_lock() {
        let mut response = vec![0u8; 36];
        response[13] = 4;
        response[15] = 8;
        response[34] = 0x01;
        response[35] = 0x00;
        let parsed = parse_channel_count(&response).unwrap();
        assert_eq!(parsed.tx_count, 4);
        assert_eq!(parsed.rx_count, 8);
        assert_eq!(parsed.locked, Some(true));
    }

    #[test]
    fn channel_count_parser_omits_lock_on_short_response() {
        let mut response = vec![0u8; 16];
        response[13] = 2;
        response[15] = 2;
        let parsed = parse_channel_count(&response).unwrap();
        assert_eq!(parsed.locked, None);
    }

    #[test]
    fn rx_parser_decodes_subscription() {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + RX_RECORD_SIZE];
        let strings_base = response.len() as u16;
        response.extend_from_slice(b"rx-1\x00mix-hi\x00mixer\x00");

        let record = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE;
        response[record..record + 2].copy_from_slice(&1u16.to_be_bytes());
        let tx_channel_pointer = strings_base + 5;
        let tx_device_pointer = strings_base + 12;
        let rx_channel_pointer = strings_base;
        response[record + 6..record + 8].copy_from_slice(&tx_channel_pointer.to_be_bytes());
        response[record + 8..record + 10].copy_from_slice(&tx_device_pointer.to_be_bytes());
        response[record + 10..record + 12].copy_from_slice(&rx_channel_pointer.to_be_bytes());
        response[record + 12..record + 14].copy_from_slice(&257u16.to_be_bytes());
        response[record + 14..record + 16].copy_from_slice(&9u16.to_be_bytes());

        let channels = parse_rx_page(&response, 1);
        assert_eq!(channels.len(), 1);
        let channel = &channels[0];
        assert_eq!(channel.number, 1);
        assert_eq!(channel.rx_channel_name.as_deref(), Some("rx-1"));
        assert_eq!(channel.tx_channel_name.as_deref(), Some("mix-hi"));
        assert_eq!(channel.tx_device_name.as_deref(), Some("mixer"));
        assert_eq!(channel.rx_status_code, 257);
        assert_eq!(channel.subscription_status_code, 9);
    }

    #[test]
    fn rx_parser_unsubscribed_falls_back_to_rx_name() {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + RX_RECORD_SIZE];
        let strings_base = response.len() as u16;
        response.extend_from_slice(b"unused-1\x00");

        let record = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE;
        response[record..record + 2].copy_from_slice(&1u16.to_be_bytes());
        response[record + 6..record + 8].copy_from_slice(&0u16.to_be_bytes());
        response[record + 8..record + 10].copy_from_slice(&0u16.to_be_bytes());
        response[record + 10..record + 12].copy_from_slice(&strings_base.to_be_bytes());
        response[record + 12..record + 14].copy_from_slice(&0u16.to_be_bytes());
        response[record + 14..record + 16].copy_from_slice(&1u16.to_be_bytes());

        let channels = parse_rx_page(&response, 1);
        assert_eq!(channels[0].tx_channel_name.as_deref(), Some("unused-1"));
        assert_eq!(channels[0].tx_device_name, None);
    }

    #[test]
    fn rx_parser_stops_at_gap() {
        let response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + RX_RECORD_SIZE];
        assert!(parse_rx_page(&response, 1).is_empty());
    }

    #[test]
    fn tx_info_parser_stops_on_channel_group_change() {
        let mut response = vec![0u8; RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE + TX_RECORD_SIZE * 2];
        let strings_base = response.len() as u16;
        response.extend_from_slice(b"ch-1\x00ch-2\x00");

        let first = RESPONSE_HEADER_SIZE + BODY_HEADER_SIZE;
        response[first..first + 2].copy_from_slice(&1u16.to_be_bytes());
        response[first + 4..first + 6].copy_from_slice(&0x0100u16.to_be_bytes());
        response[first + 6..first + 8].copy_from_slice(&strings_base.to_be_bytes());

        let second = first + TX_RECORD_SIZE;
        response[second..second + 2].copy_from_slice(&2u16.to_be_bytes());
        response[second + 4..second + 6].copy_from_slice(&0x0200u16.to_be_bytes());
        response[second + 6..second + 8].copy_from_slice(&(strings_base + 5).to_be_bytes());

        let channels = parse_tx_info_page(&response, 1);
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].name.as_deref(), Some("ch-1"));
    }
}
