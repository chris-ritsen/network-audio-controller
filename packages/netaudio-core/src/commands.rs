use std::collections::HashSet;

use crate::protocol::{
    build_control_packet, build_set_device_name, validate_dante_channel_name, validate_dante_name,
    NetaudioError, OPCODE_CHANNEL_COUNT, OPCODE_DEVICE_NAME_SET, OPCODE_RX_CHANNELS,
    OPCODE_TX_CHANNEL_INFO, OPCODE_TX_CHANNEL_NAMES,
};

pub const PROTOCOL_SETTINGS: u16 = 0xFFFF;
pub const PROTOCOL_CMC: u16 = 0x1200;
pub const PROTOCOL_DANTE_FLOW: u16 = 0x2729;
pub const PROTOCOL_DANTE_FLOW_2801: u16 = 0x2801;
pub const PROTOCOL_AES67_CONFIG: u16 = 0x2809;

pub const OPCODE_DEVICE_NAME: u16 = 0x1002;
pub const OPCODE_DEVICE_INFO: u16 = 0x1003;
pub const OPCODE_DEVICE_SETTINGS: u16 = 0x1100;
pub const OPCODE_DEVICE_SETTINGS_SET: u16 = 0x1101;
pub const OPCODE_TX_CHANNEL_NAME_SET: u16 = 0x2013;
pub const OPCODE_RX_CHANNEL_NAME_SET: u16 = 0x3001;
pub const OPCODE_SUBSCRIPTION_ADD: u16 = 0x3010;
pub const OPCODE_SUBSCRIPTION_REMOVE: u16 = 0x3014;
pub const OPCODE_QUERY_TX_FLOWS: u16 = 0x2200;
pub const OPCODE_CREATE_TX_FLOW: u16 = 0x2201;
pub const OPCODE_DELETE_TX_FLOW: u16 = 0x2202;
pub const OPCODE_QUERY_TX_FLOWS_2809: u16 = 0x2600;
pub const OPCODE_CREATE_TX_FLOW_2809: u16 = 0x2601;
pub const OPCODE_DELETE_TX_FLOW_2809: u16 = 0x2602;

pub const FLOW_TYPE_MULTICAST: u16 = 0x0002;

pub const MAGIC_VENDOR: &[u8] = b"Audinate";

const VENDOR_SEPARATOR: u8 = 0x07;

const SETTINGS_SUFFIX_SYSTEM_CONFIG: u8 = 0x3a;
const SETTINGS_SUFFIX_IDENTITY: u8 = 0x31;
const SETTINGS_SUFFIX_AES67_WRITE: u8 = 0x34;
const SETTINGS_SUFFIX_AUDIO_CONFIG: u8 = 0x27;

const AUDIO_CONFIG_PSEUDO_MAC: [u8; 6] = [b'R', b'T', 0, 0, 0, 0];

const LATENCY_SET_PREAMBLE: [u8; 22] = [
    0x05, 0x04, 0x82, 0x05, 0x00, 0x20, 0x02, 0x11, 0x00, 0x04, 0x83, 0x01, 0x00, 0x24, 0x03, 0x10,
    0x00, 0x04, 0x83, 0x02, 0x83, 0x06,
];

const LATENCY_CONFIG_QUERY_INFO_CODES: [u8; 48] = [
    0x00, 0x17, 0x02, 0x01, 0x82, 0x04, 0x82, 0x05, 0x02, 0x10, 0x02, 0x11, 0x82, 0x18, 0x82, 0x19,
    0x83, 0x01, 0x83, 0x02, 0x83, 0x06, 0x03, 0x10, 0x03, 0x11, 0x03, 0x03, 0x80, 0x21, 0x00, 0xF0,
    0x80, 0x60, 0x00, 0x22, 0x00, 0x63, 0x00, 0x64, 0x00, 0x65, 0x02, 0x22, 0x02, 0x12, 0x83, 0x21,
];

pub const MIN_GAIN_LEVEL: u8 = 1;
pub const MAX_GAIN_LEVEL: u8 = 5;
pub const MAX_LATENCY_MILLISECONDS: f64 = u32::MAX as f64 / 1_000_000.0;

pub fn build_cmc_register(sequence: u16, host_mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    let mut payload = Vec::with_capacity(16);
    payload.extend_from_slice(&sequence.to_be_bytes());
    payload.extend_from_slice(&0x1001u16.to_be_bytes());
    payload.extend_from_slice(&0u32.to_be_bytes());
    payload.extend_from_slice(&host_mac);
    payload.extend_from_slice(&0u16.to_be_bytes());

    let length = payload
        .len()
        .checked_add(4)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let encoded_length = u16::try_from(length).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut packet = Vec::with_capacity(length);
    packet.extend_from_slice(&PROTOCOL_CMC.to_be_bytes());
    packet.extend_from_slice(&encoded_length.to_be_bytes());
    packet.extend_from_slice(&payload);
    Ok(packet)
}

fn settings_packet(
    command_id: u16,
    mac: [u8; 6],
    suffix: u8,
    tail: &[u8],
) -> Result<Vec<u8>, NetaudioError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&command_id.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&mac);
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(MAGIC_VENDOR);
    payload.push(VENDOR_SEPARATOR);
    payload.push(suffix);
    payload.extend_from_slice(tail);

    let length = payload
        .len()
        .checked_add(4)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let encoded_length = u16::try_from(length).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut packet = Vec::with_capacity(length);
    packet.extend_from_slice(&PROTOCOL_SETTINGS.to_be_bytes());
    packet.extend_from_slice(&encoded_length.to_be_bytes());
    packet.extend_from_slice(&payload);
    Ok(packet)
}

fn channel_query_payload(starting_channel: u16) -> [u8; 8] {
    let mut payload = [0u8; 8];
    payload[3] = 0x01;
    payload[4..6].copy_from_slice(&starting_channel.to_be_bytes());
    payload
}

fn channel_range_query_payload(starting_channel: u16, ending_channel: u16) -> [u8; 8] {
    let mut payload = channel_query_payload(starting_channel);
    payload[6..8].copy_from_slice(&ending_channel.to_be_bytes());
    payload
}

pub fn build_device_info(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_DEVICE_INFO, &[0x00, 0x00], transaction_id)
}

pub fn build_device_name(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_DEVICE_NAME, &[0x00, 0x00], transaction_id)
}

pub fn build_channel_count(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_CHANNEL_COUNT, &[0x00, 0x00], transaction_id)
}

pub fn build_device_settings(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_DEVICE_SETTINGS, &[0x00, 0x00], transaction_id)
}

pub fn build_set_name(name: &str, transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_set_device_name(name, transaction_id)
}

pub fn build_reset_name(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet(OPCODE_DEVICE_NAME_SET, &[0x00, 0x00], transaction_id)
}

fn page_starting_channel(page: u16, channels_per_page: u16) -> Result<u16, NetaudioError> {
    page.checked_mul(channels_per_page)
        .and_then(|offset| offset.checked_add(1))
        .ok_or(NetaudioError::InvalidPage)
}

pub fn build_receivers(page: u16, transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    let starting_channel = page_starting_channel(page, 16)?;
    build_control_packet(
        OPCODE_RX_CHANNELS,
        &channel_query_payload(starting_channel),
        transaction_id,
    )
}

pub fn build_transmitters(
    page: u16,
    friendly_names: bool,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if friendly_names {
        return Err(NetaudioError::InvalidChannel);
    }
    let starting_channel = page_starting_channel(page, 32)?;
    build_control_packet(
        OPCODE_TX_CHANNEL_INFO,
        &channel_query_payload(starting_channel),
        transaction_id,
    )
}

pub fn build_transmitter_names(
    channel_count: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if channel_count == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    build_control_packet(
        OPCODE_TX_CHANNEL_NAMES,
        &channel_range_query_payload(1, channel_count),
        transaction_id,
    )
}

fn channel_name_payload(
    channel_type: ChannelType,
    channel_number: u8,
    name: Option<&str>,
) -> Vec<u8> {
    let mut payload = Vec::new();
    match channel_type {
        ChannelType::Rx => {
            payload.extend_from_slice(&[0x00, 0x00, 0x02, 0x01, 0x00, channel_number]);
            payload.extend_from_slice(&0x14u16.to_be_bytes());
            payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        }
        ChannelType::Tx => {
            payload.extend_from_slice(&[0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, channel_number]);
            payload.extend_from_slice(&0x18u16.to_be_bytes());
            payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        }
    }
    if let Some(name) = name {
        payload.extend_from_slice(name.as_bytes());
        payload.push(0);
    }
    payload
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelType {
    Rx,
    Tx,
}

pub fn build_reset_channel_name(
    channel_type: ChannelType,
    channel_number: u8,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if channel_number == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    let opcode = match channel_type {
        ChannelType::Rx => OPCODE_RX_CHANNEL_NAME_SET,
        ChannelType::Tx => OPCODE_TX_CHANNEL_NAME_SET,
    };
    build_control_packet(
        opcode,
        &channel_name_payload(channel_type, channel_number, None),
        transaction_id,
    )
}

pub fn build_set_channel_name(
    channel_type: ChannelType,
    channel_number: u8,
    name: &str,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if channel_number == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    validate_dante_channel_name(name)?;
    let opcode = match channel_type {
        ChannelType::Rx => OPCODE_RX_CHANNEL_NAME_SET,
        ChannelType::Tx => OPCODE_TX_CHANNEL_NAME_SET,
    };
    build_control_packet(
        opcode,
        &channel_name_payload(channel_type, channel_number, Some(name)),
        transaction_id,
    )
}

const SUBSCRIPTION_PACKET_HEADER_SIZE: usize = 8;
const SUBSCRIPTION_PAYLOAD_PREFIX_SIZE: usize = 4;
const SUBSCRIPTION_RECORD_SIZE: usize = 6;
const SUBSCRIPTION_STRING_TABLE_ALIGNMENT: usize = 44;

struct SubscriptionRecord {
    rx_channel_number: u8,
    tx_channel_pointer: u16,
    tx_device_pointer: u16,
}

pub fn build_add_subscriptions(
    subscriptions: &[(u16, String, String)],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let count = subscriptions.len();
    if !(1..=16).contains(&count) {
        return Err(NetaudioError::SubscriptionCount);
    }

    let record_block_size = SUBSCRIPTION_PAYLOAD_PREFIX_SIZE + SUBSCRIPTION_RECORD_SIZE * count;
    let padding_size = SUBSCRIPTION_STRING_TABLE_ALIGNMENT.saturating_sub(record_block_size);
    let string_table_offset = SUBSCRIPTION_PACKET_HEADER_SIZE + record_block_size + padding_size;

    let mut string_table: Vec<u8> = Vec::new();
    let mut records: Vec<SubscriptionRecord> = Vec::new();

    for (rx_channel_number, tx_channel_name, tx_device_name) in subscriptions {
        if *rx_channel_number == 0 {
            return Err(NetaudioError::InvalidSubscriptionChannel);
        }
        let rx_channel_number = u8::try_from(*rx_channel_number)
            .map_err(|_| NetaudioError::InvalidSubscriptionChannel)?;
        validate_dante_channel_name(tx_channel_name)?;
        if tx_device_name != "." {
            validate_dante_name(tx_device_name)?;
        }

        let tx_channel_offset = string_table_offset
            .checked_add(string_table.len())
            .ok_or(NetaudioError::PacketTooLarge)?;
        let tx_channel_pointer =
            u16::try_from(tx_channel_offset).map_err(|_| NetaudioError::PacketTooLarge)?;
        string_table.extend_from_slice(tx_channel_name.as_bytes());
        string_table.push(0);

        let tx_device_offset = string_table_offset
            .checked_add(string_table.len())
            .ok_or(NetaudioError::PacketTooLarge)?;
        let tx_device_pointer =
            u16::try_from(tx_device_offset).map_err(|_| NetaudioError::PacketTooLarge)?;
        string_table.extend_from_slice(tx_device_name.as_bytes());
        string_table.push(0);

        records.push(SubscriptionRecord {
            rx_channel_number,
            tx_channel_pointer,
            tx_device_pointer,
        });
    }

    let mut payload = Vec::new();
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.push(0x02);
    payload.push(u8::try_from(count).map_err(|_| NetaudioError::SubscriptionCount)?);
    for record in &records {
        payload.push(0x00);
        payload.push(record.rx_channel_number);
        payload.extend_from_slice(&record.tx_channel_pointer.to_be_bytes());
        payload.extend_from_slice(&record.tx_device_pointer.to_be_bytes());
    }
    payload.extend(std::iter::repeat_n(0, padding_size));
    payload.extend_from_slice(&string_table);

    build_control_packet(OPCODE_SUBSCRIPTION_ADD, &payload, transaction_id)
}

pub fn build_remove_subscriptions(
    rx_channels: &[u32],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if rx_channels.is_empty() {
        return Err(NetaudioError::SubscriptionCount);
    }
    if rx_channels.contains(&0) {
        return Err(NetaudioError::InvalidChannel);
    }
    let channel_bytes = rx_channels
        .len()
        .checked_mul(4)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let payload_length = 4usize
        .checked_add(channel_bytes)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let packet_length = 8usize
        .checked_add(payload_length)
        .ok_or(NetaudioError::PacketTooLarge)?;
    u16::try_from(packet_length).map_err(|_| NetaudioError::PacketTooLarge)?;
    let count = u32::try_from(rx_channels.len()).map_err(|_| NetaudioError::PacketTooLarge)?;

    let mut payload = Vec::with_capacity(payload_length);
    payload.extend_from_slice(&count.to_be_bytes());
    for channel in rx_channels {
        payload.extend_from_slice(&channel.to_be_bytes());
    }
    build_control_packet(OPCODE_SUBSCRIPTION_REMOVE, &payload, transaction_id)
}

pub fn build_set_latency(
    latency_milliseconds: f64,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !latency_milliseconds.is_finite()
        || !(0.0..=MAX_LATENCY_MILLISECONDS).contains(&latency_milliseconds)
    {
        return Err(NetaudioError::InvalidLatency);
    }
    let rounded_nanoseconds = (latency_milliseconds * 1_000_000.0).round();
    if rounded_nanoseconds > f64::from(u32::MAX) {
        return Err(NetaudioError::InvalidLatency);
    }
    let latency_ns = rounded_nanoseconds as u32;
    let latency_bytes = latency_ns.to_be_bytes();

    let mut payload = Vec::new();
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&LATENCY_SET_PREAMBLE);
    payload.extend_from_slice(&latency_bytes);
    payload.extend_from_slice(&latency_bytes);

    build_control_packet(OPCODE_DEVICE_SETTINGS_SET, &payload, transaction_id)
}

pub fn build_reboot(mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    settings_packet(
        0x0000,
        mac,
        SETTINGS_SUFFIX_SYSTEM_CONFIG,
        &[0x00, 0x90, 0x00, 0x00, 0x00, 0x64, 0x00, 0x01, 0x00, 0x00],
    )
}

pub fn build_identify() -> Result<Vec<u8>, NetaudioError> {
    settings_packet(
        0x0BC8,
        [0u8; 6],
        SETTINGS_SUFFIX_IDENTITY,
        &[0x00, 0x63, 0x00, 0x00, 0x00, 0x64],
    )
}

pub fn build_set_encoding(encoding: u32) -> Result<Vec<u8>, NetaudioError> {
    if encoding == 0 {
        return Err(NetaudioError::InvalidEncoding);
    }
    let mut tail = vec![0x00, 0x83, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x01];
    tail.extend_from_slice(&encoding.to_be_bytes());
    settings_packet(
        0x03D7,
        AUDIO_CONFIG_PSEUDO_MAC,
        SETTINGS_SUFFIX_AUDIO_CONFIG,
        &tail,
    )
}

pub fn build_set_sample_rate(sample_rate: u32) -> Result<Vec<u8>, NetaudioError> {
    if sample_rate == 0 {
        return Err(NetaudioError::InvalidSampleRate);
    }
    let mut tail = vec![0x00, 0x81, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x01];
    tail.extend_from_slice(&sample_rate.to_be_bytes());
    settings_packet(
        0x03D4,
        AUDIO_CONFIG_PSEUDO_MAC,
        SETTINGS_SUFFIX_AUDIO_CONFIG,
        &tail,
    )
}

fn build_audio_config_probe(
    host_mac: [u8; 6],
    sequence: u16,
    message_type: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut body = Vec::with_capacity(14);
    body.extend_from_slice(&message_type.to_be_bytes());
    body.extend_from_slice(&100u32.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    settings_packet(sequence, host_mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &body)
}

pub fn build_probe_sample_rate(host_mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    build_audio_config_probe(host_mac, sequence, 0x0081)
}

pub fn build_probe_encoding(host_mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    build_audio_config_probe(host_mac, sequence, 0x0083)
}

pub fn build_set_gain_level(
    channel_number: u8,
    gain_level: u8,
    is_input: bool,
) -> Result<Vec<u8>, NetaudioError> {
    if channel_number == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    if !(MIN_GAIN_LEVEL..=MAX_GAIN_LEVEL).contains(&gain_level) {
        return Err(NetaudioError::InvalidGainLevel);
    }
    let (command_id, type_byte) = if is_input {
        (0x0344u16, 0x01u8)
    } else {
        (0x0326u16, 0x02u8)
    };
    let mut tail = Vec::new();
    tail.extend_from_slice(&[0x10, type_byte]);
    tail.extend_from_slice(&[0x0A, 0x00, 0x00, 0x00, 0x00, 0x00]);
    tail.extend_from_slice(&[0x01, 0x00, 0x01, 0x00, 0x0C, 0x00]);
    tail.extend_from_slice(&[0x10, type_byte]);
    tail.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]);
    tail.push(channel_number);
    tail.extend_from_slice(&[0x00, 0x00, 0x00]);
    tail.push(gain_level);
    settings_packet(
        command_id,
        AUDIO_CONFIG_PSEUDO_MAC,
        SETTINGS_SUFFIX_AUDIO_CONFIG,
        &tail,
    )
}

pub fn build_enable_aes67(enabled: bool, mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = vec![0x10, 0x06, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0001u16.to_be_bytes());
    tail.extend_from_slice(&(if enabled { 0x0001u16 } else { 0x0000u16 }).to_be_bytes());
    settings_packet(0x22DC, mac, SETTINGS_SUFFIX_AES67_WRITE, &tail)
}

pub fn build_probe_interface_status(mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = Vec::new();
    tail.extend_from_slice(&0x0013u16.to_be_bytes());
    tail.extend_from_slice(&0x64u32.to_be_bytes());
    tail.extend(std::iter::repeat_n(0, 8));
    settings_packet(0x0000, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_set_interface_dhcp(mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = Vec::new();
    tail.extend_from_slice(&0x0013u16.to_be_bytes());
    tail.extend_from_slice(&0x64u32.to_be_bytes());
    tail.extend_from_slice(&[0x01, 0x1c, 0x00, 0x10]);
    tail.extend(std::iter::repeat_n(0, 16));
    tail.extend_from_slice(&[0x00, 0x02, 0x00, 0x00]);
    tail.extend(std::iter::repeat_n(0, 4));
    settings_packet(0x0000, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_set_interface_static(
    ip_address: [u8; 4],
    netmask: [u8; 4],
    dns_server: [u8; 4],
    gateway: [u8; 4],
    mac: [u8; 6],
) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = Vec::new();
    tail.extend_from_slice(&0x0013u16.to_be_bytes());
    tail.extend_from_slice(&0x64u32.to_be_bytes());
    tail.extend_from_slice(&[0x01, 0x1c, 0x0f, 0x10]);
    tail.extend(std::iter::repeat_n(0, 4));
    tail.extend_from_slice(&0x02u32.to_be_bytes());
    tail.extend_from_slice(&ip_address);
    tail.extend_from_slice(&netmask);
    tail.extend_from_slice(&dns_server);
    tail.extend_from_slice(&gateway);
    tail.extend_from_slice(&[0x00, 0x02, 0x00, 0x00]);
    tail.extend(std::iter::repeat_n(0, 4));
    settings_packet(0x0000, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_probe_aes67(mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = vec![0x10, 0x06, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0000u16.to_be_bytes());
    tail.extend_from_slice(&0x0000u16.to_be_bytes());
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_set_preferred_leader(
    is_preferred: bool,
    clock_source: u16,
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = vec![0x00, 0x21, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0002u16.to_be_bytes());
    tail.extend_from_slice(&clock_source.to_be_bytes());
    tail.push(if is_preferred { 0x01 } else { 0x00 });
    tail.extend(std::iter::repeat_n(0, 55));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_probe_preferred_leader(
    clock_source: u16,
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = vec![0x00, 0x21, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0000u16.to_be_bytes());
    tail.extend_from_slice(&clock_source.to_be_bytes());
    tail.push(0x00);
    tail.extend(std::iter::repeat_n(0, 55));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_bluetooth_status(mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    let tail = [
        0x10, 0x0d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x0c, 0x0a, 0x0a, 0x10, 0x09, 0x1a,
        0x06, 0x0a, 0x04, 0x0a, 0x02, 0x08, 0x01,
    ];
    settings_packet(0x0000, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_make_model(mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    settings_packet(
        0x0FDB,
        mac,
        SETTINGS_SUFFIX_IDENTITY,
        &[0x00, 0xC1, 0x00, 0x00, 0x00, 0x00],
    )
}

pub fn build_dante_model(mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    settings_packet(
        0x0FDB,
        mac,
        SETTINGS_SUFFIX_IDENTITY,
        &[0x00, 0x61, 0x00, 0x00, 0x00, 0x00],
    )
}

fn protocol_packet(
    protocol_id: u16,
    opcode: u16,
    body: &[u8],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let length = 10usize
        .checked_add(body.len())
        .ok_or(NetaudioError::PacketTooLarge)?;
    let encoded_length = u16::try_from(length).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut packet = Vec::with_capacity(length);
    packet.extend_from_slice(&protocol_id.to_be_bytes());
    packet.extend_from_slice(&encoded_length.to_be_bytes());
    packet.extend_from_slice(&transaction_id.to_be_bytes());
    packet.extend_from_slice(&opcode.to_be_bytes());
    packet.extend_from_slice(&0u16.to_be_bytes());
    packet.extend_from_slice(body);
    Ok(packet)
}

pub fn build_query_latency_config(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    protocol_packet(
        PROTOCOL_AES67_CONFIG,
        OPCODE_DEVICE_SETTINGS,
        &LATENCY_CONFIG_QUERY_INFO_CODES,
        transaction_id,
    )
}

fn flow_opcodes(flow_protocol_id: u16) -> Result<(u16, u16, u16), NetaudioError> {
    match flow_protocol_id {
        PROTOCOL_DANTE_FLOW | PROTOCOL_DANTE_FLOW_2801 => Ok((
            OPCODE_QUERY_TX_FLOWS,
            OPCODE_CREATE_TX_FLOW,
            OPCODE_DELETE_TX_FLOW,
        )),
        _ => Err(NetaudioError::InvalidFlowProtocol),
    }
}

pub fn build_query_tx_flows(
    flow_protocol_id: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_query_tx_flows_from(flow_protocol_id, 1, transaction_id)
}

pub fn build_query_tx_flows_from(
    flow_protocol_id: u16,
    starting_flow: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let (query_opcode, _, _) = flow_opcodes(flow_protocol_id)?;
    if !(1..=32).contains(&starting_flow) {
        return Err(NetaudioError::InvalidFlowSlot);
    }
    let mut body = [0u8; 6];
    body[1] = 0x01;
    body[2..4].copy_from_slice(&starting_flow.to_be_bytes());
    protocol_packet(flow_protocol_id, query_opcode, &body, transaction_id)
}

pub fn build_create_tx_flow(
    flow_protocol_id: u16,
    flow_slot: u16,
    channels: &[u16],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let (_, create_opcode, _) = flow_opcodes(flow_protocol_id)?;
    if !(1..=32).contains(&flow_slot) {
        return Err(NetaudioError::InvalidFlowSlot);
    }
    if channels.is_empty() || channels.contains(&0) {
        return Err(NetaudioError::InvalidChannel);
    }
    let channel_bytes = channels
        .len()
        .checked_mul(2)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let body_length = 46usize
        .checked_add(channel_bytes)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let packet_length = 10usize
        .checked_add(body_length)
        .ok_or(NetaudioError::PacketTooLarge)?;
    u16::try_from(packet_length).map_err(|_| NetaudioError::PacketTooLarge)?;
    let channel_count = u16::try_from(channels.len()).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut unique_channels = HashSet::with_capacity(channels.len());
    if !channels
        .iter()
        .all(|channel_number| unique_channels.insert(*channel_number))
    {
        return Err(NetaudioError::InvalidChannel);
    }

    let format_flags: u16 = 0x0010;

    let mut body = Vec::with_capacity(body_length);
    body.extend_from_slice(&0x0101u16.to_be_bytes());
    body.extend_from_slice(&format_flags.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&flow_slot.to_be_bytes());
    body.extend_from_slice(&FLOW_TYPE_MULTICAST.to_be_bytes());
    body.extend(std::iter::repeat_n(0, 10));
    body.extend_from_slice(&channel_count.to_be_bytes());
    for channel_number in channels {
        body.extend_from_slice(&channel_number.to_be_bytes());
    }
    let trailing_record_offset = 10usize
        .checked_add(body.len())
        .and_then(|length| length.checked_add(4))
        .ok_or(NetaudioError::PacketTooLarge)?;
    let trailing_record_pointer =
        u16::try_from(trailing_record_offset).map_err(|_| NetaudioError::PacketTooLarge)?;
    body.extend_from_slice(&trailing_record_pointer.to_be_bytes());
    body.extend_from_slice(&[0x00, 0x00]);
    body.extend_from_slice(&[0x0a, 0x00]);
    body.extend(std::iter::repeat_n(0, 14));
    body.extend_from_slice(&[0x00, 0x01, 0x00, 0x00]);

    protocol_packet(flow_protocol_id, create_opcode, &body, transaction_id)
}

pub fn build_delete_tx_flow(
    flow_protocol_id: u16,
    flow_slot: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let (_, _, delete_opcode) = flow_opcodes(flow_protocol_id)?;
    if !(1..=32).contains(&flow_slot) {
        return Err(NetaudioError::InvalidFlowSlot);
    }
    let mut body = Vec::new();
    body.extend_from_slice(&0x0001u16.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&flow_slot.to_be_bytes());
    protocol_packet(flow_protocol_id, delete_opcode, &body, transaction_id)
}

pub fn build_volume_start(
    device_name: &str,
    ipv4: [u8; 4],
    mac: [u8; 6],
    port: u16,
    timeout: bool,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    validate_dante_name(device_name)?;
    let mut name_bytes = device_name.as_bytes().to_vec();
    name_bytes.push(0);
    if !name_bytes.len().is_multiple_of(2) {
        name_bytes.push(0);
    }
    let padded_name_len = name_bytes.len();

    let offset_field_1 = padded_name_len
        .checked_add(0x0A)
        .and_then(|offset| u16::try_from(offset).ok())
        .ok_or(NetaudioError::PacketTooLarge)?;
    let offset_field_2 = padded_name_len
        .checked_add(0x0C)
        .and_then(|offset| u16::try_from(offset).ok())
        .ok_or(NetaudioError::PacketTooLarge)?;
    let tail_offset = offset_field_2
        .checked_add(4)
        .ok_or(NetaudioError::PacketTooLarge)?;

    let mut body = Vec::new();
    body.extend_from_slice(&0x3010u16.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&mac);
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&4u16.to_be_bytes());
    body.extend_from_slice(&offset_field_1.to_be_bytes());
    body.extend_from_slice(&2u16.to_be_bytes());
    body.extend_from_slice(&offset_field_2.to_be_bytes());
    body.extend_from_slice(&0x000Au16.to_be_bytes());
    body.extend_from_slice(&name_bytes);
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&tail_offset.to_be_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&port.to_be_bytes());
    body.extend_from_slice(&(if timeout { 1u16 } else { 0u16 }).to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&ipv4);
    body.extend_from_slice(&port.to_be_bytes());
    body.extend(std::iter::repeat_n(0, 10));

    let total_length = 6usize
        .checked_add(body.len())
        .ok_or(NetaudioError::PacketTooLarge)?;
    let encoded_length = u16::try_from(total_length).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut packet = Vec::with_capacity(total_length);
    packet.extend_from_slice(&PROTOCOL_CMC.to_be_bytes());
    packet.extend_from_slice(&encoded_length.to_be_bytes());
    packet.extend_from_slice(&transaction_id.to_be_bytes());
    packet.extend_from_slice(&body);
    Ok(packet)
}

pub fn build_volume_stop(device_name: &str, mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    build_volume_start(device_name, [0u8; 4], mac, 0, false, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cmc_register_builder_matches_wire_layout() {
        let packet = build_cmc_register(0x1234, [0x00, 0x1D, 0xC1, 0x50, 0x23, 0x68]).unwrap();
        assert_eq!(
            packet,
            [
                0x12, 0x00, 0x00, 0x14, 0x12, 0x34, 0x10, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1D,
                0xC1, 0x50, 0x23, 0x68, 0x00, 0x00,
            ]
        );
    }

    #[test]
    fn channel_count_builder_matches_python_layout() {
        let packet = build_channel_count(1).unwrap();
        assert_eq!(
            packet,
            [0x27, 0xFF, 0x00, 0x0A, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn receivers_builder_matches_python_layout() {
        let packet = build_receivers(0, 0x1234).unwrap();
        assert_eq!(
            packet,
            [
                0x27, 0xFF, 0x00, 0x10, 0x12, 0x34, 0x30, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
                0x00, 0x00
            ]
        );
    }

    #[test]
    fn receivers_builder_paginates() {
        let packet = build_receivers(2, 0).unwrap();
        assert_eq!(&packet[10..14], &[0x00, 0x01, 0x00, 33]);
    }

    #[test]
    fn receivers_builder_rejects_page_overflow() {
        let last_page = build_receivers(4095, 0).unwrap();
        assert_eq!(&last_page[12..14], &[0xFF, 0xF1]);
        assert_eq!(build_receivers(4096, 0), Err(NetaudioError::InvalidPage));
        assert_eq!(
            build_receivers(u16::MAX, 0),
            Err(NetaudioError::InvalidPage)
        );
    }

    #[test]
    fn transmitters_builder_queries_raw_pages_and_rejects_unbounded_friendly_queries() {
        let raw = build_transmitters(0, false, 0).unwrap();
        assert_eq!(&raw[6..8], &OPCODE_TX_CHANNEL_INFO.to_be_bytes());
        assert_eq!(
            build_transmitters(1, true, 0),
            Err(NetaudioError::InvalidChannel)
        );
    }

    #[test]
    fn transmitter_names_builder_queries_the_full_channel_range() {
        assert_eq!(
            build_transmitter_names(2, 0x1234).unwrap(),
            [
                0x27, 0xFF, 0x00, 0x10, 0x12, 0x34, 0x20, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
                0x00, 0x02,
            ]
        );
        assert_eq!(
            &build_transmitter_names(256, 0).unwrap()[10..],
            &[0x00, 0x01, 0x00, 0x01, 0x01, 0x00]
        );
        assert_eq!(
            build_transmitter_names(0, 0),
            Err(NetaudioError::InvalidChannel)
        );
    }

    #[test]
    fn transmitters_builder_rejects_page_overflow() {
        let last_page = build_transmitters(2047, false, 0).unwrap();
        assert_eq!(&last_page[12..14], &[0xFF, 0xE1]);
        assert_eq!(
            build_transmitters(2048, false, 0),
            Err(NetaudioError::InvalidPage)
        );
        assert_eq!(
            build_transmitters(u16::MAX, true, 0),
            Err(NetaudioError::InvalidChannel)
        );
    }

    #[test]
    fn add_subscriptions_layout_is_stable() {
        let packet = build_add_subscriptions(
            &[
                (1, "tx-a".to_owned(), "dev-a".to_owned()),
                (2, "tx-b".to_owned(), "dev-b".to_owned()),
            ],
            0,
        )
        .unwrap();
        assert_eq!(&packet[6..8], &OPCODE_SUBSCRIPTION_ADD.to_be_bytes());
        assert_eq!(&packet[8..12], &[0x00, 0x00, 0x02, 0x02]);
        assert_eq!(&packet[12..18], &[0x00, 0x01, 0x00, 52, 0x00, 57]);
        assert_eq!(&packet[18..24], &[0x00, 0x02, 0x00, 63, 0x00, 68]);
        assert!(packet[24..52].iter().all(|&byte| byte == 0));
        assert_eq!(&packet[52..], b"tx-a\x00dev-a\x00tx-b\x00dev-b\x00");
    }

    #[test]
    fn add_subscriptions_rejects_channels_that_do_not_fit_on_wire() {
        let result = build_add_subscriptions(&[(257, "tx-a".to_owned(), "dev-a".to_owned())], 0);
        assert_eq!(result, Err(NetaudioError::InvalidSubscriptionChannel));
    }

    #[test]
    fn subscriptions_reject_zero_channels_and_invalid_string_table_entries() {
        assert_eq!(
            build_add_subscriptions(&[(0, "tx-a".to_owned(), "dev-a".to_owned())], 0),
            Err(NetaudioError::InvalidSubscriptionChannel)
        );
        assert_eq!(
            build_add_subscriptions(&[(1, "tx\0a".to_owned(), "dev-a".to_owned())], 0),
            Err(NetaudioError::NameInvalidChars)
        );
        assert_eq!(
            build_add_subscriptions(&[(1, "tx-a".to_owned(), "dev\0a".to_owned())], 0),
            Err(NetaudioError::NameInvalidChars)
        );
        assert!(build_add_subscriptions(&[(1, "tx-a".to_owned(), ".".to_owned())], 0).is_ok());
    }

    #[test]
    fn remove_subscriptions_rejects_zero_channels_and_packet_overflow() {
        assert_eq!(
            build_remove_subscriptions(&[0], 0),
            Err(NetaudioError::InvalidChannel)
        );
        assert_eq!(
            build_remove_subscriptions(&vec![1; 16_381], 0),
            Err(NetaudioError::PacketTooLarge)
        );
    }

    #[test]
    fn set_latency_matches_captured_250_microsecond_packet() {
        let packet = build_set_latency(0.25, 0).unwrap();
        assert_eq!(packet.len(), 40);
        assert_eq!(
            &packet[0..8],
            &[0x27, 0xFF, 0x00, 0x28, 0x00, 0x00, 0x11, 0x01]
        );
        assert_eq!(
            &packet[8..32],
            &[
                0x00, 0x00, 0x05, 0x04, 0x82, 0x05, 0x00, 0x20, 0x02, 0x11, 0x00, 0x04, 0x83, 0x01,
                0x00, 0x24, 0x03, 0x10, 0x00, 0x04, 0x83, 0x02, 0x83, 0x06,
            ]
        );
        assert_eq!(
            &packet[32..40],
            &[0x00, 0x03, 0xD0, 0x90, 0x00, 0x03, 0xD0, 0x90]
        );
    }

    #[test]
    fn set_latency_preserves_full_nanosecond_high_byte() {
        let packet = build_set_latency(20.3125, 0).unwrap();
        assert_eq!(
            &packet[32..40],
            &[0x01, 0x35, 0xF1, 0xB4, 0x01, 0x35, 0xF1, 0xB4]
        );
    }

    #[test]
    fn set_latency_rejects_values_that_cannot_be_encoded() {
        for latency in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.001,
            MAX_LATENCY_MILLISECONDS + 0.001,
        ] {
            assert_eq!(
                build_set_latency(latency, 0),
                Err(NetaudioError::InvalidLatency),
                "{latency:?}"
            );
        }

        let maximum = build_set_latency(MAX_LATENCY_MILLISECONDS, 0).unwrap();
        assert_eq!(&maximum[32..36], &u32::MAX.to_be_bytes());
    }

    #[test]
    fn audio_settings_accept_nonzero_wire_values_without_truncation() {
        for sample_rate in [44_100, 48_000, 192_000, 123_456, u32::MAX] {
            assert!(build_set_sample_rate(sample_rate).is_ok(), "{sample_rate}");
        }
        assert_eq!(
            &build_set_sample_rate(u32::MAX).unwrap()[36..40],
            &u32::MAX.to_be_bytes()
        );
        assert_eq!(
            build_set_sample_rate(0),
            Err(NetaudioError::InvalidSampleRate)
        );

        for encoding in [1, 16, 24, 32, 256, u32::MAX] {
            assert!(build_set_encoding(encoding).is_ok(), "{encoding}");
        }
        assert_eq!(
            &build_set_encoding(u32::MAX).unwrap()[36..40],
            &u32::MAX.to_be_bytes()
        );
        assert_eq!(build_set_encoding(0), Err(NetaudioError::InvalidEncoding));

        assert_eq!(
            build_set_gain_level(0, 1, true),
            Err(NetaudioError::InvalidChannel)
        );
        for level in [0, 6, u8::MAX] {
            assert_eq!(
                build_set_gain_level(1, level, true),
                Err(NetaudioError::InvalidGainLevel),
                "{level}"
            );
        }
        for level in MIN_GAIN_LEVEL..=MAX_GAIN_LEVEL {
            assert!(build_set_gain_level(1, level, false).is_ok(), "{level}");
        }
    }

    #[test]
    fn probe_sample_rate_matches_captured_packet_4170622() {
        let packet = build_probe_sample_rate([0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24], 0x0042).unwrap();
        assert_eq!(
            packet,
            [
                0xFF, 0xFF, 0x00, 0x28, 0x00, 0x42, 0x00, 0x00, 0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24,
                0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x3A, 0x00, 0x81,
                0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ]
        );
    }

    #[test]
    fn probe_encoding_matches_captured_packet_204680() {
        let packet = build_probe_encoding([0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24], 0x985A).unwrap();
        assert_eq!(
            packet,
            [
                0xFF, 0xFF, 0x00, 0x28, 0x98, 0x5A, 0x00, 0x00, 0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24,
                0x00, 0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x3A, 0x00, 0x83,
                0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ]
        );
    }

    #[test]
    fn query_tx_flows_selects_opcode_per_protocol() {
        let legacy = build_query_tx_flows(0x2729, 7).unwrap();
        assert_eq!(
            legacy,
            [
                0x27, 0x29, 0x00, 0x10, 0x00, 0x07, 0x22, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
                0x00, 0x00,
            ]
        );

        let legacy_2801 = build_query_tx_flows(0x2801, 7).unwrap();
        assert_eq!(&legacy_2801[6..8], &OPCODE_QUERY_TX_FLOWS.to_be_bytes());

        let later_page = build_query_tx_flows_from(0x2729, 29, 7).unwrap();
        assert_eq!(&later_page[10..], &[0x00, 0x01, 0x00, 0x1D, 0x00, 0x00]);
    }

    #[test]
    fn flow_builders_reject_unknown_protocols() {
        for protocol in [0, 0x2728, 0x2800, 0x2808, 0x2809, u16::MAX] {
            assert_eq!(
                build_query_tx_flows(protocol, 0),
                Err(NetaudioError::InvalidFlowProtocol),
                "{protocol:#06x}"
            );
            assert_eq!(
                build_create_tx_flow(protocol, 1, &[1], 0),
                Err(NetaudioError::InvalidFlowProtocol),
                "{protocol:#06x}"
            );
            assert_eq!(
                build_delete_tx_flow(protocol, 1, 0),
                Err(NetaudioError::InvalidFlowProtocol),
                "{protocol:#06x}"
            );
        }
    }

    #[test]
    fn query_tx_flows_rejects_starting_slots_outside_device_range() {
        for starting_flow in [0, 33, u16::MAX] {
            assert_eq!(
                build_query_tx_flows_from(PROTOCOL_DANTE_FLOW, starting_flow, 0),
                Err(NetaudioError::InvalidFlowSlot),
                "{starting_flow}"
            );
        }
    }

    #[test]
    fn create_tx_flow_matches_captured_layout() {
        let packet = build_create_tx_flow(0x2729, 1, &[1, 2], 0).unwrap();
        let mut expected = vec![0x27, 0x29, 0x00, 0x3C, 0x00, 0x00, 0x22, 0x01, 0x00, 0x00];
        expected.extend_from_slice(&[0x01, 0x01, 0x00, 0x10]);
        expected.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        expected.extend_from_slice(&[0x00, 0x02]);
        expected.extend(std::iter::repeat_n(0, 10));
        expected.extend_from_slice(&[0x00, 0x02, 0x00, 0x01, 0x00, 0x02]);
        expected.extend_from_slice(&[0x00, 0x28]);
        expected.extend_from_slice(&[0x00, 0x00, 0x0a, 0x00]);
        expected.extend(std::iter::repeat_n(0, 14));
        expected.extend_from_slice(&[0x00, 0x01, 0x00, 0x00]);
        assert_eq!(packet, expected);
    }

    #[test]
    fn create_tx_flow_rejects_invalid_channels_and_packet_overflow() {
        assert_eq!(
            build_create_tx_flow(0x2729, 1, &[], 0),
            Err(NetaudioError::InvalidChannel)
        );
        assert_eq!(
            build_create_tx_flow(0x2729, 1, &[0], 0),
            Err(NetaudioError::InvalidChannel)
        );
        assert_eq!(
            build_create_tx_flow(0x2729, 1, &[1, 1], 0),
            Err(NetaudioError::InvalidChannel)
        );
        assert_eq!(
            build_create_tx_flow(0x2729, 1, &vec![1; 32_740], 0),
            Err(NetaudioError::PacketTooLarge)
        );
    }

    #[test]
    fn flow_mutations_reject_slots_outside_device_range() {
        for slot in [0, 33, u16::MAX] {
            assert_eq!(
                build_create_tx_flow(PROTOCOL_DANTE_FLOW, slot, &[1], 0),
                Err(NetaudioError::InvalidFlowSlot),
                "{slot}"
            );
            assert_eq!(
                build_delete_tx_flow(PROTOCOL_DANTE_FLOW, slot, 0),
                Err(NetaudioError::InvalidFlowSlot),
                "{slot}"
            );
        }

        for slot in 1..=32 {
            assert!(build_create_tx_flow(PROTOCOL_DANTE_FLOW_2801, slot, &[1], 0).is_ok());
            assert!(build_delete_tx_flow(PROTOCOL_DANTE_FLOW, slot, 0).is_ok());
        }
    }

    #[test]
    fn channel_mutations_reject_channel_zero() {
        assert_eq!(
            build_reset_channel_name(ChannelType::Rx, 0, 0),
            Err(NetaudioError::InvalidChannel)
        );
        assert_eq!(
            build_set_channel_name(ChannelType::Tx, 0, "tx-a", 0),
            Err(NetaudioError::InvalidChannel)
        );
    }

    #[test]
    fn volume_builder_rejects_unrepresentable_names_before_constructing_offsets() {
        assert_eq!(
            build_volume_start(&"a".repeat(65_521), [0; 4], [0; 6], 0, false, 0),
            Err(NetaudioError::NameTooLong)
        );
        assert_eq!(
            build_volume_start("dev\0name", [0; 4], [0; 6], 0, false, 0),
            Err(NetaudioError::NameInvalidChars)
        );
    }

    #[test]
    fn delete_tx_flow_matches_python_layout() {
        let packet = build_delete_tx_flow(0x2729, 3, 0).unwrap();
        assert_eq!(
            packet,
            [
                0x27, 0x29, 0x00, 0x10, 0x00, 0x00, 0x22, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
                0x00, 0x03
            ]
        );
    }
}
