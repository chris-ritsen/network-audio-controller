use super::*;

fn flow_create_opcode(flow_protocol_id: u16) -> Result<u16, NetaudioError> {
    match flow_protocol_id {
        PROTOCOL_DANTE_FLOW | PROTOCOL_DANTE_FLOW_2801 => Ok(OPCODE_CREATE_TX_FLOW),
        _ => Err(NetaudioError::InvalidFlowProtocol),
    }
}

fn flow_delete_opcode(flow_protocol_id: u16) -> Result<u16, NetaudioError> {
    match flow_protocol_id {
        PROTOCOL_DANTE_FLOW | PROTOCOL_DANTE_FLOW_2801 => Ok(OPCODE_DELETE_TX_FLOW),
        PROTOCOL_ARC_2809 => Ok(OPCODE_DELETE_TX_FLOW_2809),
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
    if !(1..=32).contains(&starting_flow) {
        return Err(NetaudioError::InvalidFlowSlot);
    }
    match flow_protocol_id {
        PROTOCOL_DANTE_FLOW | PROTOCOL_DANTE_FLOW_2801 => {
            let mut body = [0u8; 6];
            body[1] = 0x01;
            body[2..4].copy_from_slice(&starting_flow.to_be_bytes());
            protocol_packet(
                flow_protocol_id,
                OPCODE_QUERY_TX_FLOWS,
                &body,
                transaction_id,
            )
        }
        PROTOCOL_ARC_2809 if starting_flow == 1 => {
            let mut body = [0u8; 24];
            body[6..12].copy_from_slice(&[0x00, 0x01, 0x00, 0x01, 0x00, 0x01]);
            protocol_packet(
                flow_protocol_id,
                OPCODE_QUERY_TX_FLOWS_2809,
                &body,
                transaction_id,
            )
        }
        PROTOCOL_ARC_2809 => Err(NetaudioError::InvalidFlowSlot),
        _ => Err(NetaudioError::InvalidFlowProtocol),
    }
}

fn build_channel_status_query(
    protocol_id: u16,
    opcode: u16,
    media_type: u16,
    starting_channel_identifier: u16,
    ending_channel_identifier: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !crate::protocol::is_modern_arc_protocol(protocol_id)
        || media_type == 0
        || starting_channel_identifier == 0
        || (ending_channel_identifier != 0
            && ending_channel_identifier < starting_channel_identifier)
    {
        return Err(NetaudioError::InvalidChannel);
    }
    let mut body = [0u8; 24];
    body[6..8].copy_from_slice(&1u16.to_be_bytes());
    body[8..10].copy_from_slice(&media_type.to_be_bytes());
    body[10..12].copy_from_slice(&starting_channel_identifier.to_be_bytes());
    body[12..14].copy_from_slice(&ending_channel_identifier.to_be_bytes());
    if protocol_id == PROTOCOL_ARC_2809 {
        body[18..24].copy_from_slice(&[0x83, 0x02, 0x83, 0x06, 0x03, 0x10]);
    }
    protocol_packet(protocol_id, opcode, &body, transaction_id)
}

pub fn build_query_transmitter_channel_status(
    protocol_id: u16,
    media_type: u16,
    starting_channel_identifier: u16,
    ending_channel_identifier: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_channel_status_query(
        protocol_id,
        OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
        media_type,
        starting_channel_identifier,
        ending_channel_identifier,
        transaction_id,
    )
}

pub fn build_query_transmitter_channel_status_2809(
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_query_transmitter_channel_status(PROTOCOL_ARC_2809, 1, 1, 0, transaction_id)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmitterChannelNameReconciliationRecord {
    pub channel_number: u16,
    pub name: String,
}

pub fn build_reconcile_transmitter_channel_names_2809(
    records: &[TransmitterChannelNameReconciliationRecord],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let record_count = u8::try_from(records.len()).map_err(|_| NetaudioError::PacketTooLarge)?;
    if record_count == 0 {
        return Err(NetaudioError::InvalidChannel);
    }

    let mut channel_numbers = HashSet::with_capacity(records.len());
    for record in records {
        if record.channel_number == 0 || !channel_numbers.insert(record.channel_number) {
            return Err(NetaudioError::InvalidChannel);
        }
        validate_dante_channel_name(&record.name)?;
    }

    let descriptor_bytes = records
        .len()
        .checked_mul(6)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let mut next_name_pointer = 20usize
        .checked_add(descriptor_bytes)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let mut body = Vec::new();
    body.extend_from_slice(&[0u8; 6]);
    body.extend_from_slice(&0x0600u16.to_be_bytes());
    body.push(record_count);
    body.push(record_count);
    for record in records {
        let name_pointer =
            u16::try_from(next_name_pointer).map_err(|_| NetaudioError::PacketTooLarge)?;
        body.extend_from_slice(&record.channel_number.to_be_bytes());
        body.extend_from_slice(&0x0003u16.to_be_bytes());
        body.extend_from_slice(&name_pointer.to_be_bytes());
        next_name_pointer = next_name_pointer
            .checked_add(record.name.len())
            .and_then(|length| length.checked_add(1))
            .ok_or(NetaudioError::PacketTooLarge)?;
    }
    for record in records {
        body.extend_from_slice(record.name.as_bytes());
        body.push(0);
    }

    protocol_packet(
        PROTOCOL_ARC_2809,
        OPCODE_RECONCILE_TRANSMITTER_CHANNEL_NAMES_2809,
        &body,
        transaction_id,
    )
}

pub fn build_query_receiver_channel_status_2809(
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_query_receiver_channel_status(PROTOCOL_ARC_2809, 1, 1, 0, transaction_id)
}

pub fn build_query_receiver_channel_status(
    protocol_id: u16,
    media_type: u16,
    starting_channel_identifier: u16,
    ending_channel_identifier: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_channel_status_query(
        protocol_id,
        OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809,
        media_type,
        starting_channel_identifier,
        ending_channel_identifier,
        transaction_id,
    )
}

pub fn build_query_receiver_flow_status_2809(
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut body = [0u8; 24];
    body[6..12].copy_from_slice(&[0x00, 0x01, 0x00, 0x01, 0x00, 0x01]);
    body[18..24].copy_from_slice(&[0x83, 0x02, 0x83, 0x06, 0x03, 0x10]);
    protocol_packet(
        PROTOCOL_ARC_2809,
        OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809,
        &body,
        transaction_id,
    )
}

pub fn build_set_receiver_channel_name_2809(
    channel_number: u16,
    name: &str,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if channel_number == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    validate_dante_channel_name(name)?;

    let mut body = Vec::with_capacity(17 + name.len());
    body.extend_from_slice(&[0u8; 6]);
    body.extend_from_slice(&0x0600u16.to_be_bytes());
    body.extend_from_slice(&0x0101u16.to_be_bytes());
    body.extend_from_slice(&channel_number.to_be_bytes());
    body.extend_from_slice(&0x0003u16.to_be_bytes());
    body.extend_from_slice(&0x001Au16.to_be_bytes());
    body.extend_from_slice(name.as_bytes());
    body.push(0);

    protocol_packet(
        PROTOCOL_ARC_2809,
        OPCODE_SET_RECEIVER_CHANNEL_NAME_2809,
        &body,
        transaction_id,
    )
}

pub fn build_query_receiver_flows(
    starting_flow: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if starting_flow == 0 {
        return Err(NetaudioError::InvalidFlowSlot);
    }
    let mut body = [0u8; 6];
    body[1] = 0x01;
    body[2..4].copy_from_slice(&starting_flow.to_be_bytes());
    protocol_packet(
        PROTOCOL_DANTE_FLOW,
        OPCODE_QUERY_RECEIVER_FLOWS,
        &body,
        transaction_id,
    )
}

pub fn build_query_transmit_channel_capabilities(
    starting_channel_identifier: u16,
    maximum_channel_count: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if starting_channel_identifier == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    let mut body = [0u8; 6];
    body[0..2].copy_from_slice(&1u16.to_be_bytes());
    body[2..4].copy_from_slice(&starting_channel_identifier.to_be_bytes());
    body[4..6].copy_from_slice(&maximum_channel_count.to_be_bytes());
    protocol_packet(
        PROTOCOL_DANTE_FLOW,
        OPCODE_QUERY_TRANSMIT_CHANNEL_CAPABILITIES,
        &body,
        transaction_id,
    )
}

pub fn build_query_receiver_port_ranges(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    protocol_packet(
        PROTOCOL_DANTE_FLOW,
        OPCODE_QUERY_RECEIVER_PORT_RANGES,
        &[],
        transaction_id,
    )
}

pub fn build_create_tx_flow(
    flow_protocol_id: u16,
    flow_slot: u16,
    channels: &[u16],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let create_opcode = flow_create_opcode(flow_protocol_id)?;
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
    let delete_opcode = flow_delete_opcode(flow_protocol_id)?;
    if !(1..=32).contains(&flow_slot) {
        return Err(NetaudioError::InvalidFlowSlot);
    }
    if flow_protocol_id == PROTOCOL_ARC_2809 {
        if flow_slot != 2 {
            return Err(NetaudioError::InvalidFlowSlot);
        }
        let mut body = [0u8; 24];
        body[6..8].copy_from_slice(&1u16.to_be_bytes());
        body[8..10].copy_from_slice(&3u16.to_be_bytes());
        body[12..14].copy_from_slice(&flow_slot.to_be_bytes());
        return protocol_packet(flow_protocol_id, delete_opcode, &body, transaction_id);
    }
    let mut body = Vec::new();
    body.extend_from_slice(&0x0001u16.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&flow_slot.to_be_bytes());
    protocol_packet(flow_protocol_id, delete_opcode, &body, transaction_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes_from_hex(hexadecimal: &str) -> Vec<u8> {
        hexadecimal
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    #[test]
    fn transmitter_channel_name_reconciliation_matches_shipping_controller_request() {
        let records = [
            TransmitterChannelNameReconciliationRecord {
                channel_number: 1,
                name: "vrroom:left".to_owned(),
            },
            TransmitterChannelNameReconciliationRecord {
                channel_number: 2,
                name: "vrroom:right".to_owned(),
            },
        ];
        assert_eq!(
            build_reconcile_transmitter_channel_names_2809(&records, 0x4A0C).unwrap(),
            bytes_from_hex(
                "280900394a0c243800000000000000000600020200010003002000020003002c7672726f6f6d3a6c656674007672726f6f6d3a726967687400"
            )
        );
        assert_eq!(
            build_reconcile_transmitter_channel_names_2809(&[], 0),
            Err(NetaudioError::InvalidChannel)
        );
        assert_eq!(
            build_reconcile_transmitter_channel_names_2809(
                &[
                    TransmitterChannelNameReconciliationRecord {
                        channel_number: 1,
                        name: "left".to_owned(),
                    },
                    TransmitterChannelNameReconciliationRecord {
                        channel_number: 1,
                        name: "right".to_owned(),
                    },
                ],
                0,
            ),
            Err(NetaudioError::InvalidChannel)
        );
    }
}
