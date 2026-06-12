use crate::protocol::{
    build_control_packet, build_set_device_name, validate_dante_channel_name, NetaudioError,
    OPCODE_CHANNEL_COUNT, OPCODE_DEVICE_NAME_SET, OPCODE_RX_CHANNELS, OPCODE_TX_CHANNEL_INFO,
    OPCODE_TX_CHANNEL_NAMES,
};

pub const PROTOCOL_SETTINGS: u16 = 0xFFFF;
pub const PROTOCOL_CMC: u16 = 0x1200;
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
    0x05, 0x03, 0x82, 0x05, 0x00, 0x20, 0x02, 0x11, 0x00, 0x10, 0x83, 0x01, 0x00, 0x24, 0x82,
    0x19, 0x83, 0x01, 0x83, 0x02, 0x83, 0x06,
];

const LATENCY_CONFIG_QUERY_INFO_CODES: [u8; 48] = [
    0x00, 0x17, 0x02, 0x01, 0x82, 0x04, 0x82, 0x05, 0x02, 0x10, 0x02, 0x11, 0x82, 0x18, 0x82,
    0x19, 0x83, 0x01, 0x83, 0x02, 0x83, 0x06, 0x03, 0x10, 0x03, 0x11, 0x03, 0x03, 0x80, 0x21,
    0x00, 0xF0, 0x80, 0x60, 0x00, 0x22, 0x00, 0x63, 0x00, 0x64, 0x00, 0x65, 0x02, 0x22, 0x02,
    0x12, 0x83, 0x21,
];

fn settings_packet(command_id: u16, mac: [u8; 6], suffix: u8, tail: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&command_id.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&mac);
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(MAGIC_VENDOR);
    payload.push(VENDOR_SEPARATOR);
    payload.push(suffix);
    payload.extend_from_slice(tail);

    let length = (payload.len() + 4) as u8;
    let mut packet = Vec::with_capacity(payload.len() + 4);
    packet.extend_from_slice(&PROTOCOL_SETTINGS.to_be_bytes());
    packet.push(0x00);
    packet.push(length);
    packet.extend_from_slice(&payload);
    packet
}

fn channel_query_payload(starting_channel: u16) -> [u8; 8] {
    let mut payload = [0u8; 8];
    payload[3] = 0x01;
    payload[4..6].copy_from_slice(&starting_channel.to_be_bytes());
    payload
}

pub fn build_device_info(transaction_id: u16) -> Vec<u8> {
    build_control_packet(OPCODE_DEVICE_INFO, &[0x00, 0x00], transaction_id)
}

pub fn build_device_name(transaction_id: u16) -> Vec<u8> {
    build_control_packet(OPCODE_DEVICE_NAME, &[0x00, 0x00], transaction_id)
}

pub fn build_channel_count(transaction_id: u16) -> Vec<u8> {
    build_control_packet(OPCODE_CHANNEL_COUNT, &[0x00, 0x00], transaction_id)
}

pub fn build_device_settings(transaction_id: u16) -> Vec<u8> {
    build_control_packet(OPCODE_DEVICE_SETTINGS, &[0x00, 0x00], transaction_id)
}

pub fn build_set_name(name: &str, transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_set_device_name(name, transaction_id)
}

pub fn build_reset_name(transaction_id: u16) -> Vec<u8> {
    build_control_packet(OPCODE_DEVICE_NAME_SET, &[0x00, 0x00], transaction_id)
}

pub fn build_receivers(page: u16, transaction_id: u16) -> Vec<u8> {
    let starting_channel = page * 16 + 1;
    build_control_packet(OPCODE_RX_CHANNELS, &channel_query_payload(starting_channel), transaction_id)
}

pub fn build_transmitters(page: u16, friendly_names: bool, transaction_id: u16) -> Vec<u8> {
    let opcode = if friendly_names {
        OPCODE_TX_CHANNEL_NAMES
    } else {
        OPCODE_TX_CHANNEL_INFO
    };
    let starting_channel = page * 32 + 1;
    build_control_packet(opcode, &channel_query_payload(starting_channel), transaction_id)
}

fn channel_name_payload(channel_type: ChannelType, channel_number: u8, name: Option<&str>) -> Vec<u8> {
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
) -> Vec<u8> {
    let opcode = match channel_type {
        ChannelType::Rx => OPCODE_RX_CHANNEL_NAME_SET,
        ChannelType::Tx => OPCODE_TX_CHANNEL_NAME_SET,
    };
    build_control_packet(opcode, &channel_name_payload(channel_type, channel_number, None), transaction_id)
}

pub fn build_set_channel_name(
    channel_type: ChannelType,
    channel_number: u8,
    name: &str,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    validate_dante_channel_name(name)?;
    let opcode = match channel_type {
        ChannelType::Rx => OPCODE_RX_CHANNEL_NAME_SET,
        ChannelType::Tx => OPCODE_TX_CHANNEL_NAME_SET,
    };
    Ok(build_control_packet(
        opcode,
        &channel_name_payload(channel_type, channel_number, Some(name)),
        transaction_id,
    ))
}

const SUBSCRIPTION_PACKET_HEADER_SIZE: usize = 8;
const SUBSCRIPTION_PAYLOAD_PREFIX_SIZE: usize = 4;
const SUBSCRIPTION_RECORD_SIZE: usize = 6;
const SUBSCRIPTION_STRING_TABLE_ALIGNMENT: usize = 44;

struct SubscriptionRecord {
    rx_channel_number: u16,
    tx_channel_pointer: u16,
    tx_device_pointer: u16,
}

pub fn build_add_subscriptions(
    subscriptions: &[(u16, String, String)],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let count = subscriptions.len();
    if count < 1 || count > 16 {
        return Err(NetaudioError::SubscriptionCount);
    }

    let record_block_size = SUBSCRIPTION_PAYLOAD_PREFIX_SIZE + SUBSCRIPTION_RECORD_SIZE * count;
    let padding_size = SUBSCRIPTION_STRING_TABLE_ALIGNMENT.saturating_sub(record_block_size);
    let string_table_offset = SUBSCRIPTION_PACKET_HEADER_SIZE + record_block_size + padding_size;

    let mut string_table: Vec<u8> = Vec::new();
    let mut records: Vec<SubscriptionRecord> = Vec::new();

    for (rx_channel_number, tx_channel_name, tx_device_name) in subscriptions {
        let tx_channel_pointer = (string_table_offset + string_table.len()) as u16;
        string_table.extend_from_slice(tx_channel_name.as_bytes());
        string_table.push(0);

        let tx_device_pointer = (string_table_offset + string_table.len()) as u16;
        string_table.extend_from_slice(tx_device_name.as_bytes());
        string_table.push(0);

        records.push(SubscriptionRecord {
            rx_channel_number: *rx_channel_number,
            tx_channel_pointer,
            tx_device_pointer,
        });
    }

    let mut payload = Vec::new();
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.push(0x02);
    payload.push(count as u8);
    for record in &records {
        payload.push(0x00);
        payload.push(record.rx_channel_number as u8);
        payload.extend_from_slice(&record.tx_channel_pointer.to_be_bytes());
        payload.extend_from_slice(&record.tx_device_pointer.to_be_bytes());
    }
    payload.extend(std::iter::repeat(0).take(padding_size));
    payload.extend_from_slice(&string_table);

    Ok(build_control_packet(OPCODE_SUBSCRIPTION_ADD, &payload, transaction_id))
}

pub fn build_remove_subscriptions(
    rx_channels: &[u32],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if rx_channels.is_empty() {
        return Err(NetaudioError::SubscriptionCount);
    }
    let mut payload = Vec::new();
    payload.extend_from_slice(&(rx_channels.len() as u32).to_be_bytes());
    for channel in rx_channels {
        payload.extend_from_slice(&channel.to_be_bytes());
    }
    Ok(build_control_packet(OPCODE_SUBSCRIPTION_REMOVE, &payload, transaction_id))
}

pub fn build_set_latency(latency_seconds: f64, transaction_id: u16) -> Vec<u8> {
    let latency_us = (latency_seconds * 1_000_000.0) as u32;
    let latency_bytes = &latency_us.to_be_bytes()[1..];

    let mut payload = Vec::new();
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&LATENCY_SET_PREAMBLE);
    payload.extend_from_slice(latency_bytes);
    payload.push(0x00);
    payload.extend_from_slice(latency_bytes);

    build_control_packet(OPCODE_DEVICE_SETTINGS_SET, &payload, transaction_id)
}

pub fn build_reboot(mac: [u8; 6]) -> Vec<u8> {
    settings_packet(
        0x0000,
        mac,
        SETTINGS_SUFFIX_SYSTEM_CONFIG,
        &[0x00, 0x90, 0x00, 0x00, 0x00, 0x64, 0x00, 0x01, 0x00, 0x00],
    )
}

pub fn build_identify() -> Vec<u8> {
    settings_packet(0x0BC8, [0u8; 6], SETTINGS_SUFFIX_IDENTITY, &[0x00, 0x63, 0x00, 0x00, 0x00, 0x64])
}

pub fn build_set_encoding(encoding: u8) -> Vec<u8> {
    let mut tail = vec![0x00, 0x83, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00];
    tail.push(encoding);
    settings_packet(0x03D7, AUDIO_CONFIG_PSEUDO_MAC, SETTINGS_SUFFIX_AUDIO_CONFIG, &tail)
}

pub fn build_set_sample_rate(sample_rate: u32) -> Vec<u8> {
    let mut tail = vec![0x00, 0x81, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x01, 0x00];
    tail.extend_from_slice(&sample_rate.to_be_bytes()[1..]);
    settings_packet(0x03D4, AUDIO_CONFIG_PSEUDO_MAC, SETTINGS_SUFFIX_AUDIO_CONFIG, &tail)
}

pub fn build_set_gain_level(channel_number: u8, gain_level: u8, is_input: bool) -> Vec<u8> {
    let (command_id, type_byte) = if is_input { (0x0344u16, 0x01u8) } else { (0x0326u16, 0x02u8) };
    let mut tail = Vec::new();
    tail.extend_from_slice(&[0x10, type_byte]);
    tail.extend_from_slice(&[0x0A, 0x00, 0x00, 0x00, 0x00, 0x00]);
    tail.extend_from_slice(&[0x01, 0x00, 0x01, 0x00, 0x0C, 0x00]);
    tail.extend_from_slice(&[0x10, type_byte]);
    tail.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]);
    tail.push(channel_number);
    tail.extend_from_slice(&[0x00, 0x00, 0x00]);
    tail.push(gain_level);
    settings_packet(command_id, AUDIO_CONFIG_PSEUDO_MAC, SETTINGS_SUFFIX_AUDIO_CONFIG, &tail)
}

pub fn build_enable_aes67(enabled: bool, mac: [u8; 6]) -> Vec<u8> {
    let mut tail = vec![0x10, 0x06, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0001u16.to_be_bytes());
    tail.extend_from_slice(&(if enabled { 0x0001u16 } else { 0x0000u16 }).to_be_bytes());
    settings_packet(0x22DC, mac, SETTINGS_SUFFIX_AES67_WRITE, &tail)
}

pub fn build_probe_interface_status(mac: [u8; 6]) -> Vec<u8> {
    let mut tail = Vec::new();
    tail.extend_from_slice(&0x0013u16.to_be_bytes());
    tail.extend_from_slice(&0x64u32.to_be_bytes());
    tail.extend(std::iter::repeat(0).take(8));
    settings_packet(0x0000, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_set_interface_dhcp(mac: [u8; 6]) -> Vec<u8> {
    let mut tail = Vec::new();
    tail.extend_from_slice(&0x0013u16.to_be_bytes());
    tail.extend_from_slice(&0x64u32.to_be_bytes());
    tail.extend_from_slice(&[0x01, 0x1c, 0x00, 0x10]);
    tail.extend(std::iter::repeat(0).take(16));
    tail.extend_from_slice(&[0x00, 0x02, 0x00, 0x00]);
    tail.extend(std::iter::repeat(0).take(4));
    settings_packet(0x0000, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_set_interface_static(
    ip_address: [u8; 4],
    netmask: [u8; 4],
    dns_server: [u8; 4],
    gateway: [u8; 4],
    mac: [u8; 6],
) -> Vec<u8> {
    let mut tail = Vec::new();
    tail.extend_from_slice(&0x0013u16.to_be_bytes());
    tail.extend_from_slice(&0x64u32.to_be_bytes());
    tail.extend_from_slice(&[0x01, 0x1c, 0x0f, 0x10]);
    tail.extend(std::iter::repeat(0).take(4));
    tail.extend_from_slice(&0x02u32.to_be_bytes());
    tail.extend_from_slice(&ip_address);
    tail.extend_from_slice(&netmask);
    tail.extend_from_slice(&dns_server);
    tail.extend_from_slice(&gateway);
    tail.extend_from_slice(&[0x00, 0x02, 0x00, 0x00]);
    tail.extend(std::iter::repeat(0).take(4));
    settings_packet(0x0000, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_probe_aes67(mac: [u8; 6], sequence: u16) -> Vec<u8> {
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
) -> Vec<u8> {
    let mut tail = vec![0x00, 0x21, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0002u16.to_be_bytes());
    tail.extend_from_slice(&clock_source.to_be_bytes());
    tail.push(if is_preferred { 0x01 } else { 0x00 });
    tail.extend(std::iter::repeat(0).take(55));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_probe_preferred_leader(clock_source: u16, mac: [u8; 6], sequence: u16) -> Vec<u8> {
    let mut tail = vec![0x00, 0x21, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0000u16.to_be_bytes());
    tail.extend_from_slice(&clock_source.to_be_bytes());
    tail.push(0x00);
    tail.extend(std::iter::repeat(0).take(55));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_bluetooth_status(mac: [u8; 6]) -> Vec<u8> {
    let tail = [
        0x10, 0x0d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x0c, 0x0a, 0x0a, 0x10, 0x09, 0x1a,
        0x06, 0x0a, 0x04, 0x0a, 0x02, 0x08, 0x01,
    ];
    settings_packet(0x0000, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_make_model(mac: [u8; 6]) -> Vec<u8> {
    settings_packet(0x0FDB, mac, SETTINGS_SUFFIX_IDENTITY, &[0x00, 0xC1, 0x00, 0x00, 0x00, 0x00])
}

pub fn build_dante_model(mac: [u8; 6]) -> Vec<u8> {
    settings_packet(0x0FDB, mac, SETTINGS_SUFFIX_IDENTITY, &[0x00, 0x61, 0x00, 0x00, 0x00, 0x00])
}

fn protocol_packet(protocol_id: u16, opcode: u16, body: &[u8], transaction_id: u16) -> Vec<u8> {
    let length = (10 + body.len()) as u16;
    let mut packet = Vec::with_capacity(10 + body.len());
    packet.extend_from_slice(&protocol_id.to_be_bytes());
    packet.extend_from_slice(&length.to_be_bytes());
    packet.extend_from_slice(&transaction_id.to_be_bytes());
    packet.extend_from_slice(&opcode.to_be_bytes());
    packet.extend_from_slice(&0u16.to_be_bytes());
    packet.extend_from_slice(body);
    packet
}

pub fn build_query_latency_config(transaction_id: u16) -> Vec<u8> {
    protocol_packet(
        PROTOCOL_AES67_CONFIG,
        OPCODE_DEVICE_SETTINGS,
        &LATENCY_CONFIG_QUERY_INFO_CODES,
        transaction_id,
    )
}

fn flow_opcodes(flow_protocol_id: u16) -> (u16, u16, u16) {
    if flow_protocol_id == PROTOCOL_AES67_CONFIG {
        (OPCODE_QUERY_TX_FLOWS_2809, OPCODE_CREATE_TX_FLOW_2809, OPCODE_DELETE_TX_FLOW_2809)
    } else {
        (OPCODE_QUERY_TX_FLOWS, OPCODE_CREATE_TX_FLOW, OPCODE_DELETE_TX_FLOW)
    }
}

pub fn build_query_tx_flows(flow_protocol_id: u16, transaction_id: u16) -> Vec<u8> {
    let (query_opcode, _, _) = flow_opcodes(flow_protocol_id);
    protocol_packet(flow_protocol_id, query_opcode, &[0x00, 0x00], transaction_id)
}

pub fn build_create_tx_flow(
    flow_protocol_id: u16,
    flow_slot: u16,
    channels: &[u16],
    transaction_id: u16,
) -> Vec<u8> {
    let (_, create_opcode, _) = flow_opcodes(flow_protocol_id);
    let format_flags: u16 = if flow_protocol_id == PROTOCOL_AES67_CONFIG { 0x0001 } else { 0x0010 };

    let mut body = Vec::new();
    body.extend_from_slice(&0x0101u16.to_be_bytes());
    body.extend_from_slice(&format_flags.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&flow_slot.to_be_bytes());
    body.extend_from_slice(&FLOW_TYPE_MULTICAST.to_be_bytes());
    body.extend(std::iter::repeat(0).take(10));
    body.extend_from_slice(&(channels.len() as u16).to_be_bytes());
    for channel_number in channels {
        body.extend_from_slice(&channel_number.to_be_bytes());
    }
    let trailing_record_pointer = (10 + body.len() + 4) as u16;
    body.extend_from_slice(&trailing_record_pointer.to_be_bytes());
    body.extend_from_slice(&[0x00, 0x02]);
    body.extend_from_slice(&[0x0a, 0x00]);
    body.extend(std::iter::repeat(0).take(14));
    body.extend_from_slice(&[0x00, 0x01, 0x00, 0x00]);

    protocol_packet(flow_protocol_id, create_opcode, &body, transaction_id)
}

pub fn build_delete_tx_flow(flow_protocol_id: u16, flow_slot: u16, transaction_id: u16) -> Vec<u8> {
    let (_, _, delete_opcode) = flow_opcodes(flow_protocol_id);
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
) -> Vec<u8> {
    let mut name_bytes = device_name.as_bytes().to_vec();
    name_bytes.push(0);
    if name_bytes.len() % 2 != 0 {
        name_bytes.push(0);
    }
    let padded_name_len = name_bytes.len();

    let offset_field_1 = (0x0A + padded_name_len) as u16;
    let offset_field_2 = (0x0C + padded_name_len) as u16;
    let tail_offset = offset_field_2 + 4;

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
    body.extend(std::iter::repeat(0).take(10));

    let total_length = (4 + 2 + body.len()) as u16;
    let mut packet = Vec::new();
    packet.extend_from_slice(&PROTOCOL_CMC.to_be_bytes());
    packet.extend_from_slice(&total_length.to_be_bytes());
    packet.extend_from_slice(&transaction_id.to_be_bytes());
    packet.extend_from_slice(&body);
    packet
}

pub fn build_volume_stop(device_name: &str, mac: [u8; 6]) -> Vec<u8> {
    build_volume_start(device_name, [0u8; 4], mac, 0, false, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_count_builder_matches_python_layout() {
        let packet = build_channel_count(1);
        assert_eq!(packet, [0x27, 0xFF, 0x00, 0x0A, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn receivers_builder_matches_python_layout() {
        let packet = build_receivers(0, 0x1234);
        assert_eq!(
            packet,
            [0x27, 0xFF, 0x00, 0x10, 0x12, 0x34, 0x30, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00]
        );
    }

    #[test]
    fn receivers_builder_paginates() {
        let packet = build_receivers(2, 0);
        assert_eq!(&packet[10..14], &[0x00, 0x01, 0x00, 33]);
    }

    #[test]
    fn transmitters_builder_selects_opcode_and_page() {
        let raw = build_transmitters(0, false, 0);
        assert_eq!(&raw[6..8], &OPCODE_TX_CHANNEL_INFO.to_be_bytes());
        let friendly = build_transmitters(1, true, 0);
        assert_eq!(&friendly[6..8], &OPCODE_TX_CHANNEL_NAMES.to_be_bytes());
        assert_eq!(&friendly[10..14], &[0x00, 0x01, 0x00, 33]);
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
    fn set_latency_embeds_microseconds_twice() {
        let packet = build_set_latency(0.005, 0);
        let microseconds = 5000u32.to_be_bytes();
        let expected_tail = [&microseconds[1..], &[0x00], &microseconds[1..]].concat();
        assert!(packet.ends_with(&expected_tail));
    }

    #[test]
    fn query_tx_flows_selects_opcode_per_protocol() {
        let legacy = build_query_tx_flows(0x2729, 7);
        assert_eq!(legacy, [0x27, 0x29, 0x00, 0x0C, 0x00, 0x07, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let aes67 = build_query_tx_flows(0x2809, 7);
        assert_eq!(aes67, [0x28, 0x09, 0x00, 0x0C, 0x00, 0x07, 0x26, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn create_tx_flow_matches_python_layout() {
        let packet = build_create_tx_flow(0x2729, 1, &[1, 2], 0);
        let mut expected = vec![0x27, 0x29, 0x00, 0x3C, 0x00, 0x00, 0x22, 0x01, 0x00, 0x00];
        expected.extend_from_slice(&[0x01, 0x01, 0x00, 0x10]);
        expected.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        expected.extend_from_slice(&[0x00, 0x02]);
        expected.extend(std::iter::repeat(0).take(10));
        expected.extend_from_slice(&[0x00, 0x02, 0x00, 0x01, 0x00, 0x02]);
        expected.extend_from_slice(&[0x00, 0x28]);
        expected.extend_from_slice(&[0x00, 0x02, 0x0a, 0x00]);
        expected.extend(std::iter::repeat(0).take(14));
        expected.extend_from_slice(&[0x00, 0x01, 0x00, 0x00]);
        assert_eq!(packet, expected);
    }

    #[test]
    fn create_tx_flow_aes67_uses_alternate_format_flags() {
        let packet = build_create_tx_flow(0x2809, 1, &[1], 0);
        assert_eq!(&packet[6..8], &OPCODE_CREATE_TX_FLOW_2809.to_be_bytes());
        assert_eq!(&packet[10..14], &[0x01, 0x01, 0x00, 0x01]);
    }

    #[test]
    fn delete_tx_flow_matches_python_layout() {
        let packet = build_delete_tx_flow(0x2729, 3, 0);
        assert_eq!(
            packet,
            [0x27, 0x29, 0x00, 0x10, 0x00, 0x00, 0x22, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03]
        );
    }
}
