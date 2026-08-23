use super::*;

const SUBSCRIPTION_PACKET_HEADER_SIZE: usize = 8;
const SUBSCRIPTION_PAYLOAD_PREFIX_SIZE: usize = 4;
const SUBSCRIPTION_RECORD_SIZE: usize = 6;
const SUBSCRIPTION_STRING_TABLE_ALIGNMENT: usize = 44;
const SUBSCRIPTION_PAGE_CAPACITY: usize = 32;
const SUBSCRIPTION_PAGE_STRING_TABLE_OFFSET: usize = 0x028C;
const RECEIVE_CHANNEL_NAME_PAGE_CAPACITY: usize = 32;
const RECEIVE_CHANNEL_NAME_PAGE_STRING_TABLE_OFFSET: usize = 0x008C;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveChannelNamePageRecord {
    pub rx_channel_number: u16,
    pub name: String,
}

pub fn build_receive_channel_name_page_2729(
    records: &[ReceiveChannelNamePageRecord],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if records.len() != RECEIVE_CHANNEL_NAME_PAGE_CAPACITY {
        return Err(NetaudioError::InvalidPage);
    }

    let mut seen_channels = HashSet::new();
    let mut encoded_records = Vec::with_capacity(records.len());
    let mut string_table = Vec::new();

    for record in records {
        if record.rx_channel_number == 0 || !seen_channels.insert(record.rx_channel_number) {
            return Err(NetaudioError::InvalidChannel);
        }
        validate_dante_channel_name(&record.name)?;
        let absolute_offset = RECEIVE_CHANNEL_NAME_PAGE_STRING_TABLE_OFFSET
            .checked_add(string_table.len())
            .ok_or(NetaudioError::PacketTooLarge)?;
        let name_pointer =
            u16::try_from(absolute_offset).map_err(|_| NetaudioError::PacketTooLarge)?;
        string_table.extend_from_slice(record.name.as_bytes());
        string_table.push(0);
        encoded_records.push((record.rx_channel_number, name_pointer));
    }

    let mut payload = Vec::with_capacity(
        RECEIVE_CHANNEL_NAME_PAGE_STRING_TABLE_OFFSET - SUBSCRIPTION_PACKET_HEADER_SIZE
            + string_table.len(),
    );
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.push(RECEIVE_CHANNEL_NAME_PAGE_CAPACITY as u8);
    payload.push(RECEIVE_CHANNEL_NAME_PAGE_CAPACITY as u8);
    for (rx_channel_number, name_pointer) in encoded_records {
        payload.extend_from_slice(&rx_channel_number.to_be_bytes());
        payload.extend_from_slice(&name_pointer.to_be_bytes());
    }
    payload.extend_from_slice(&string_table);

    build_control_packet_for_protocol(
        PROTOCOL_DANTE_FLOW,
        OPCODE_RX_CHANNEL_NAME_SET,
        &payload,
        transaction_id,
    )
}

struct SubscriptionRecord {
    rx_channel_number: u8,
    tx_channel_pointer: u16,
    tx_device_pointer: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionPageRecord {
    Set {
        rx_channel_number: u16,
        tx_channel_name: String,
        tx_device_name: String,
    },
    Clear {
        rx_channel_number: u16,
    },
}

fn intern_subscription_page_string(
    value: &str,
    string_table: &mut Vec<u8>,
    offsets: &mut HashMap<String, u16>,
) -> Result<u16, NetaudioError> {
    if let Some(offset) = offsets.get(value) {
        return Ok(*offset);
    }
    let absolute_offset = SUBSCRIPTION_PAGE_STRING_TABLE_OFFSET
        .checked_add(string_table.len())
        .ok_or(NetaudioError::PacketTooLarge)?;
    let encoded_offset =
        u16::try_from(absolute_offset).map_err(|_| NetaudioError::PacketTooLarge)?;
    string_table.extend_from_slice(value.as_bytes());
    string_table.push(0);
    offsets.insert(value.to_owned(), encoded_offset);
    Ok(encoded_offset)
}

pub fn build_subscription_page_2729(
    records: &[SubscriptionPageRecord],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if records.is_empty() || records.len() > SUBSCRIPTION_PAGE_CAPACITY {
        return Err(NetaudioError::SubscriptionCount);
    }

    let mut seen_channels = HashSet::new();
    let mut encoded_records = Vec::with_capacity(records.len());
    let mut string_table = Vec::new();
    let mut string_offsets = HashMap::new();

    for record in records {
        let (rx_channel_number, tx_channel_pointer, tx_device_pointer) = match record {
            SubscriptionPageRecord::Set {
                rx_channel_number,
                tx_channel_name,
                tx_device_name,
            } => {
                validate_dante_channel_reference(tx_channel_name)?;
                if tx_device_name != "." {
                    validate_dante_name(tx_device_name)?;
                }
                let tx_channel_pointer = intern_subscription_page_string(
                    tx_channel_name,
                    &mut string_table,
                    &mut string_offsets,
                )?;
                let tx_device_pointer = intern_subscription_page_string(
                    tx_device_name,
                    &mut string_table,
                    &mut string_offsets,
                )?;
                (*rx_channel_number, tx_channel_pointer, tx_device_pointer)
            }
            SubscriptionPageRecord::Clear { rx_channel_number } => (*rx_channel_number, 0, 0),
        };
        if rx_channel_number == 0 || !seen_channels.insert(rx_channel_number) {
            return Err(NetaudioError::InvalidSubscriptionChannel);
        }
        encoded_records.push((rx_channel_number, tx_channel_pointer, tx_device_pointer));
    }

    let mut payload = Vec::with_capacity(
        SUBSCRIPTION_PAGE_STRING_TABLE_OFFSET - SUBSCRIPTION_PACKET_HEADER_SIZE
            + string_table.len(),
    );
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.push(SUBSCRIPTION_PAGE_CAPACITY as u8);
    payload.push(u8::try_from(records.len()).map_err(|_| NetaudioError::SubscriptionCount)?);
    for (rx_channel_number, tx_channel_pointer, tx_device_pointer) in encoded_records {
        payload.extend_from_slice(&rx_channel_number.to_be_bytes());
        payload.extend_from_slice(&tx_channel_pointer.to_be_bytes());
        payload.extend_from_slice(&tx_device_pointer.to_be_bytes());
    }
    let fixed_payload_size =
        SUBSCRIPTION_PAGE_STRING_TABLE_OFFSET - SUBSCRIPTION_PACKET_HEADER_SIZE;
    payload.resize(fixed_payload_size, 0);
    payload.extend_from_slice(&string_table);

    build_control_packet_for_protocol(
        PROTOCOL_DANTE_FLOW,
        OPCODE_SUBSCRIPTION_ADD,
        &payload,
        transaction_id,
    )
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
        validate_dante_channel_reference(tx_channel_name)?;
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
