use super::*;

pub const MAX_LEGACY_FLOW_ID: u16 = 32;
pub const SEGMENTED_DELETE_MEDIA_TYPE_AUDIO: u16 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
pub enum TxFlowDeleteSelection {
    Fixed {
        global_flow_id: u16,
    },
    Segmented {
        media_local_flow_id: u16,
        media_type: u16,
    },
}

#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FixedFlowDestination {
    pub address: std::net::Ipv4Addr,
    pub port: std::num::NonZeroU16,
}

#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FixedFlowConfiguration {
    pub sample_rate: std::num::NonZeroU32,
    pub encoding: std::num::NonZeroU16,
    pub media_class: u16,
    pub frames_per_packet: u16,
    pub label: Option<String>,
    pub persistent: bool,
    pub advertised: bool,
    pub destinations: Vec<FixedFlowDestination>,
}

pub fn build_fixed_multicast_flow(
    protocol: u16,
    flow_id: u16,
    channels: &[u16],
    configuration: &FixedFlowConfiguration,
    transaction: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !matches!(protocol, PROTOCOL_DANTE_FLOW | PROTOCOL_ARC_2809) {
        return Err(NetaudioError::InvalidFlowProtocol);
    }

    if !(1..=MAX_LEGACY_FLOW_ID).contains(&flow_id) {
        return Err(NetaudioError::InvalidFlowSlot);
    }

    if channels.is_empty()
        || channels.contains(&0)
        || channels.iter().collect::<HashSet<_>>().len() != channels.len()
    {
        return Err(NetaudioError::InvalidChannel);
    }

    if !matches!(configuration.media_class, 1 | 3) {
        return Err(NetaudioError::InvalidFlowIdentity);
    }

    let destinations = &configuration.destinations;
    if destinations.len() > 2
        || (configuration.media_class == 3 && destinations.is_empty())
        || destinations
            .iter()
            .any(|socket| !socket.address.is_multicast())
        || (destinations.len() == 2
            && destinations[0].address == destinations[1].address
            && destinations[0].port == destinations[1].port)
    {
        return Err(NetaudioError::InvalidDestination);
    }

    let label = configuration.label.as_deref().unwrap_or("");
    if label.as_bytes().contains(&0) {
        return Err(NetaudioError::NameInvalidChars);
    }
    if label.len() > 31 {
        return Err(NetaudioError::NameTooLong);
    }

    let align = |n: usize| {
        n.checked_add(3)
            .map(|v| v & !3)
            .ok_or(NetaudioError::PacketTooLarge)
    };
    let count = u16::try_from(channels.len()).map_err(|_| NetaudioError::PacketTooLarge)?;
    let record_end = channels
        .len()
        .checked_add(destinations.len())
        .and_then(|v| v.checked_mul(2))
        .and_then(|v| v.checked_add(34))
        .ok_or(NetaudioError::PacketTooLarge)?;
    let extension = align(record_end)?;
    let label_offset = extension
        .checked_add(20)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let socket_start = align(
        label_offset
            .checked_add(if label.is_empty() { 0 } else { label.len() + 1 })
            .ok_or(NetaudioError::PacketTooLarge)?,
    )?;
    let length = socket_start
        .checked_add(destinations.len() * 8)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let length_word = u16::try_from(length).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut packet = vec![0; length];
    fn word(packet: &mut [u8], offset: usize, value: u16) {
        packet[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
    }
    word(&mut packet, 0, protocol);
    word(&mut packet, 2, length_word);
    word(&mut packet, 4, transaction);
    word(&mut packet, 6, OPCODE_CREATE_TX_FLOW);
    word(&mut packet, 10, 0x0101);
    word(&mut packet, 12, 16);
    word(&mut packet, 16, flow_id);
    let flags = if configuration.persistent {
        if destinations.is_empty() {
            2
        } else {
            6
        }
    } else {
        0
    } | if configuration.advertised { 0 } else { 0x10 };
    word(&mut packet, 18, flags);
    packet[20..24].copy_from_slice(&configuration.sample_rate.get().to_be_bytes());
    word(&mut packet, 26, configuration.encoding.get());
    word(&mut packet, 28, destinations.len() as u16);
    word(&mut packet, 30, count);
    for (index, socket) in destinations.iter().enumerate() {
        let offset = socket_start + index * 8;
        word(&mut packet, 32 + index * 2, offset as u16);
        packet[offset] = 8;
        packet[offset + 1] = 2;
        word(&mut packet, offset + 2, socket.port.get());
        packet[offset + 4..offset + 8].copy_from_slice(&socket.address.octets());
    }
    for (index, channel) in channels.iter().enumerate() {
        word(
            &mut packet,
            32 + destinations.len() * 2 + index * 2,
            *channel,
        );
    }
    word(&mut packet, record_end - 2, extension as u16);
    packet[extension] = 10;
    word(&mut packet, extension + 8, configuration.frames_per_packet);
    word(&mut packet, extension + 16, configuration.media_class);
    if !label.is_empty() {
        word(&mut packet, extension + 10, label_offset as u16);
        packet[label_offset..label_offset + label.len()].copy_from_slice(label.as_bytes());
    }

    Ok(packet)
}

fn flow_create_opcode(flow_protocol_id: u16) -> Result<u16, NetaudioError> {
    match flow_protocol_id {
        PROTOCOL_DANTE_FLOW | PROTOCOL_DANTE_FLOW_2801 => Ok(OPCODE_CREATE_TX_FLOW),
        _ => Err(NetaudioError::InvalidFlowProtocol),
    }
}

pub fn build_query_tx_flows(
    flow_protocol_id: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_query_tx_flows_from(flow_protocol_id, 1, message_id)
}

pub fn build_query_tx_flows_from(
    flow_protocol_id: u16,
    starting_flow: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !(1..=MAX_LEGACY_FLOW_ID).contains(&starting_flow) {
        return Err(NetaudioError::InvalidFlowSlot);
    }
    match flow_protocol_id {
        PROTOCOL_DANTE_FLOW | PROTOCOL_DANTE_FLOW_2801 => {
            build_query_fixed_tx_flows_from(flow_protocol_id, starting_flow, message_id)
        }
        flow_protocol_id
            if crate::protocol::is_modern_arc_protocol(flow_protocol_id) && starting_flow == 1 =>
        {
            let mut body = [0u8; 24];
            body[6..12].copy_from_slice(&[0x00, 0x01, 0x00, 0x01, 0x00, 0x01]);
            arc_packet_with_reserved_word(
                flow_protocol_id,
                OPCODE_QUERY_TX_FLOWS_2809,
                &body,
                message_id,
            )
        }
        flow_protocol_id if crate::protocol::is_modern_arc_protocol(flow_protocol_id) => {
            Err(NetaudioError::InvalidFlowSlot)
        }
        _ => Err(NetaudioError::InvalidFlowProtocol),
    }
}

pub fn build_query_fixed_tx_flows_from(
    protocol: u16,
    starting_flow: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !matches!(
        protocol,
        crate::protocol::PROTOCOL_ID
            | PROTOCOL_DANTE_FLOW
            | PROTOCOL_DANTE_FLOW_2801
            | PROTOCOL_ARC_2809
    ) {
        return Err(NetaudioError::InvalidFlowProtocol);
    }
    if starting_flow == 0 {
        return Err(NetaudioError::InvalidFlowSlot);
    }
    let mut body = [0u8; 6];
    body[1] = 1;
    body[2..4].copy_from_slice(&starting_flow.to_be_bytes());
    arc_packet_with_reserved_word(protocol, OPCODE_QUERY_TX_FLOWS, &body, message_id)
}

fn build_channel_status_query(
    protocol_id: u16,
    opcode: u16,
    media_selector: u16,
    starting_channel_identifier: u16,
    ending_channel_identifier: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !crate::protocol::is_modern_arc_protocol(protocol_id)
        || media_selector == 0
        || starting_channel_identifier == 0
        || (ending_channel_identifier != 0
            && ending_channel_identifier < starting_channel_identifier)
    {
        return Err(NetaudioError::InvalidChannel);
    }
    let mut body = [0u8; 24];
    body[6..8].copy_from_slice(&1u16.to_be_bytes());
    body[8..10].copy_from_slice(&media_selector.to_be_bytes());
    body[10..12].copy_from_slice(&starting_channel_identifier.to_be_bytes());
    body[12..14].copy_from_slice(&ending_channel_identifier.to_be_bytes());
    if protocol_id == PROTOCOL_ARC_2809 {
        body[18..24].copy_from_slice(&[0x83, 0x02, 0x83, 0x06, 0x03, 0x10]);
    }
    arc_packet_with_reserved_word(protocol_id, opcode, &body, message_id)
}

pub fn build_query_transmitter_channel_status(
    protocol_id: u16,
    media_selector: u16,
    starting_channel_identifier: u16,
    ending_channel_identifier: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_channel_status_query(
        protocol_id,
        OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
        media_selector,
        starting_channel_identifier,
        ending_channel_identifier,
        message_id,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmitterChannelNameReconciliationRecord {
    pub channel_number: u16,
    pub name: String,
}

pub fn build_reconcile_transmitter_channel_names_2809(
    records: &[TransmitterChannelNameReconciliationRecord],
    message_id: u16,
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

    arc_packet_with_reserved_word(
        PROTOCOL_ARC_2809,
        OPCODE_RECONCILE_TRANSMITTER_CHANNEL_NAMES_2809,
        &body,
        message_id,
    )
}

pub fn build_query_receiver_channel_status(
    protocol_id: u16,
    media_selector: u16,
    starting_channel_identifier: u16,
    ending_channel_identifier: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_channel_status_query(
        protocol_id,
        OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809,
        media_selector,
        starting_channel_identifier,
        ending_channel_identifier,
        message_id,
    )
}

pub fn build_query_receiver_flow_status(
    protocol_id: u16,
    starting_flow: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !crate::protocol::is_modern_arc_protocol(protocol_id) || starting_flow == 0 {
        return Err(NetaudioError::InvalidFlowProtocol);
    }
    let mut body = [0u8; 24];
    body[6..8].copy_from_slice(&1u16.to_be_bytes());
    body[8..10].copy_from_slice(&1u16.to_be_bytes());
    body[10..12].copy_from_slice(&starting_flow.to_be_bytes());

    if protocol_id == PROTOCOL_ARC_2809 {
        body[18..24].copy_from_slice(&[0x83, 0x02, 0x83, 0x06, 0x03, 0x10]);
    }
    arc_packet_with_reserved_word(
        protocol_id,
        OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809,
        &body,
        message_id,
    )
}

pub fn build_set_receiver_channel_name_for_protocol(
    protocol_id: u16,
    channel_number: u16,
    name: &str,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !matches!(
        protocol_id,
        PROTOCOL_ARC_2809 | crate::protocol::PROTOCOL_ARC_280F
    ) {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }
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

    arc_packet_with_reserved_word(
        protocol_id,
        OPCODE_SET_RECEIVER_CHANNEL_NAME_2809,
        &body,
        message_id,
    )
}

pub fn build_query_receiver_flows(
    protocol_id: u16,
    starting_flow: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !crate::responses::FIXED_RECEIVER_FLOW_PROTOCOL_IDS.contains(&protocol_id) {
        return Err(NetaudioError::InvalidFlowProtocol);
    }
    if starting_flow == 0 {
        return Err(NetaudioError::InvalidFlowSlot);
    }
    let mut body = [0u8; 6];
    body[1] = 0x01;
    body[2..4].copy_from_slice(&starting_flow.to_be_bytes());
    arc_packet_with_reserved_word(protocol_id, OPCODE_QUERY_RECEIVER_FLOWS, &body, message_id)
}

pub fn build_query_transmit_channel_capabilities(
    starting_channel_identifier: u16,
    maximum_channel_count: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if starting_channel_identifier == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    let mut body = [0u8; 6];
    body[0..2].copy_from_slice(&1u16.to_be_bytes());
    body[2..4].copy_from_slice(&starting_channel_identifier.to_be_bytes());
    body[4..6].copy_from_slice(&maximum_channel_count.to_be_bytes());
    arc_packet_with_reserved_word(
        PROTOCOL_DANTE_FLOW,
        OPCODE_QUERY_TRANSMIT_CHANNEL_CAPABILITIES,
        &body,
        message_id,
    )
}

pub fn build_query_receiver_port_ranges(message_id: u16) -> Result<Vec<u8>, NetaudioError> {
    build_query_receiver_port_ranges_for_protocol(PROTOCOL_DANTE_FLOW, message_id)
}

pub fn build_query_receiver_port_ranges_for_protocol(
    protocol_id: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if protocol_id != PROTOCOL_DANTE_FLOW && !crate::protocol::is_modern_arc_protocol(protocol_id) {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }
    arc_packet_with_reserved_word(
        protocol_id,
        OPCODE_QUERY_RECEIVER_PORT_RANGES,
        &[],
        message_id,
    )
}

pub fn build_query_directional_audio_availability(
    protocol_id: u16,
    direction: AudioDirection,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !crate::protocol::is_modern_arc_protocol(protocol_id) {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }

    let operation = match direction {
        AudioDirection::Transmit => OPCODE_QUERY_TRANSMIT_AVAILABILITY,
        AudioDirection::Receive => OPCODE_QUERY_RECEIVE_AVAILABILITY,
    };

    arc_packet_with_reserved_word(protocol_id, operation, &[], message_id)
}

pub fn build_create_tx_flow(
    flow_protocol_id: u16,
    flow_slot: u16,
    channels: &[u16],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let create_opcode = flow_create_opcode(flow_protocol_id)?;
    if !(1..=MAX_LEGACY_FLOW_ID).contains(&flow_slot) {
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

    arc_packet_with_reserved_word(flow_protocol_id, create_opcode, &body, message_id)
}

pub fn build_delete_tx_flow(
    flow_protocol_id: u16,
    selection: TxFlowDeleteSelection,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !matches!(flow_protocol_id, PROTOCOL_DANTE_FLOW | PROTOCOL_ARC_2809) {
        return Err(NetaudioError::InvalidFlowProtocol);
    }
    match selection {
        TxFlowDeleteSelection::Fixed { global_flow_id } => {
            if !(1..=MAX_LEGACY_FLOW_ID).contains(&global_flow_id) {
                return Err(NetaudioError::InvalidFlowSlot);
            }
            let mut body = [0u8; 6];
            body[1] = 1;
            body[4..6].copy_from_slice(&global_flow_id.to_be_bytes());
            arc_packet_with_reserved_word(
                flow_protocol_id,
                OPCODE_DELETE_TX_FLOW,
                &body,
                message_id,
            )
        }
        TxFlowDeleteSelection::Segmented {
            media_local_flow_id,
            media_type,
        } => {
            if media_local_flow_id == 0 || media_type != SEGMENTED_DELETE_MEDIA_TYPE_AUDIO {
                return Err(NetaudioError::InvalidFlowSlot);
            }
            let mut body = [0u8; 24];
            body[6..8].copy_from_slice(&1u16.to_be_bytes());
            body[8..10].copy_from_slice(&media_type.to_be_bytes());
            body[12..14].copy_from_slice(&media_local_flow_id.to_be_bytes());
            arc_packet_with_reserved_word(
                flow_protocol_id,
                OPCODE_DELETE_TX_FLOW_2809,
                &body,
                message_id,
            )
        }
    }
}
