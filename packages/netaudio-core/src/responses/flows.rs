use super::*;

pub fn parse_tx_flow_page(response: &[u8]) -> Option<TxFlowPage> {
    let envelope = validate_response_envelope(
        response,
        &[
            (PROTOCOL_DANTE_FLOW, OPCODE_QUERY_TX_FLOWS),
            (PROTOCOL_DANTE_FLOW_2801, OPCODE_QUERY_TX_FLOWS),
        ],
        &[RESULT_CODE_SUCCESS, crate::protocol::RESULT_CODE_MORE_PAGES],
    )?;
    let body = envelope.body;
    let maximum_records = usize::from(*body.first()?);
    let active_count = usize::from(*body.get(1)?);
    if !(1..=32).contains(&maximum_records) || active_count > maximum_records {
        return None;
    }
    let pointer_table_size = maximum_records.checked_mul(2)?;
    let records_start = 2usize.checked_add(pointer_table_size)?;
    body.get(..records_start)?;

    let mut record_offsets = Vec::with_capacity(active_count);
    for index in 0..maximum_records {
        let record_pointer = read_u16(body, 2 + index * 2)?;
        if record_pointer == 0 {
            continue;
        }
        let record_offset = usize::from(record_pointer).checked_sub(RESPONSE_HEADER_SIZE)?;
        if record_offset < records_start
            || record_offsets
                .last()
                .is_some_and(|previous_offset| record_offset <= *previous_offset)
        {
            return None;
        }
        record_offsets.push(record_offset);
    }
    if record_offsets.len() != active_count {
        return None;
    }

    let mut flows = Vec::with_capacity(active_count);
    let mut flow_numbers = HashSet::with_capacity(active_count);
    for (index, record_offset) in record_offsets.iter().copied().enumerate() {
        let record_end = record_offsets.get(index + 1).copied().unwrap_or(body.len());
        let flow = parse_flow_record(body, record_offset, record_end)?;
        if !(1..=32).contains(&flow.flow_number) || !flow_numbers.insert(flow.flow_number) {
            return None;
        }
        flows.push(flow);
    }
    Some(TxFlowPage {
        max_flow_slots: u8::try_from(maximum_records).ok()?,
        flows,
    })
}

pub fn parse_tx_flows(response: &[u8]) -> Option<Vec<TxFlow>> {
    Some(parse_tx_flow_page(response)?.flows)
}

pub fn parse_transmitter_flow_status_page(response: &[u8]) -> Option<TransmitterFlowStatusPage> {
    let envelope = validate_response_envelope(
        response,
        &modern_arc_protocol_opcodes(OPCODE_QUERY_TX_FLOWS_2809),
        &[RESULT_CODE_SUCCESS],
    )?;
    let body = envelope.body;
    if body.len() < 12 {
        return None;
    }
    let maximum_flow_slots = *body.get(6)?;
    let reported_flow_count = *body.get(7)?;
    if !(1..=32).contains(&maximum_flow_slots) || reported_flow_count > maximum_flow_slots {
        return None;
    }

    let pointer_table_end = TRANSMITTER_FLOW_STATUS_POINTER_TABLE_OFFSET
        .checked_add(usize::from(reported_flow_count).checked_mul(2)?)?;
    response.get(TRANSMITTER_FLOW_STATUS_POINTER_TABLE_OFFSET..pointer_table_end)?;

    let mut record_pointers = Vec::with_capacity(usize::from(reported_flow_count));
    let mut seen_record_pointers = HashSet::with_capacity(usize::from(reported_flow_count));
    for index in 0..reported_flow_count {
        let pointer_offset = TRANSMITTER_FLOW_STATUS_POINTER_TABLE_OFFSET
            .checked_add(usize::from(index).checked_mul(2)?)?;
        let record_pointer = read_u16(response, pointer_offset)?;
        let record_offset = usize::from(record_pointer);
        if record_offset < pointer_table_end || !seen_record_pointers.insert(record_pointer) {
            return None;
        }
        record_pointers.push(record_pointer);
    }

    let mut flows = Vec::with_capacity(usize::from(reported_flow_count));
    let mut record_ranges = Vec::with_capacity(usize::from(reported_flow_count));
    let mut flow_numbers = HashSet::with_capacity(usize::from(reported_flow_count));
    let mut media_identities = HashSet::with_capacity(usize::from(reported_flow_count));
    for record_pointer in record_pointers {
        let flow = parse_transmitter_flow_status_record(response, record_pointer)?;
        if flow.flow_number > u16::from(maximum_flow_slots)
            || !flow_numbers.insert(flow.flow_number)
            || !media_identities.insert((flow.media_type, flow.media_local_flow_id))
        {
            return None;
        }
        let record_start = usize::from(record_pointer);
        let record_end = record_start.checked_add(usize::from(flow.record_length_bytes))?;
        record_ranges.push((record_start, record_end));
        flows.push(flow);
    }
    record_ranges.sort_unstable();
    if record_ranges
        .windows(2)
        .any(|ranges| ranges[0].1 > ranges[1].0)
    {
        return None;
    }

    Some(TransmitterFlowStatusPage {
        maximum_flow_slots,
        reported_flow_count,
        flows,
        raw_body_hexadecimal: bytes_to_hex(body),
    })
}

#[derive(Debug)]
struct TransmitterFlowStatusRecordGeometry {
    record_end: usize,
    segment_offsets: Vec<usize>,
}

fn transmitter_flow_status_record_geometry(
    response: &[u8],
    record_offset: usize,
) -> Option<TransmitterFlowStatusRecordGeometry> {
    let mut segment_offsets = Vec::new();
    let mut segment_offset = record_offset;
    let record_end = loop {
        let header = response.get(segment_offset..segment_offset.checked_add(2)?)?;
        let next_word_distance = usize::from(*header.first()?);
        if next_word_distance == 0 {
            return None;
        }
        segment_offsets.push(segment_offset);
        if *header.get(1)? == 0 {
            break segment_offset.checked_add(2)?;
        }
        let next_segment_offset = segment_offset.checked_add(next_word_distance.checked_mul(2)?)?;
        response.get(segment_offset..next_segment_offset)?;
        if segment_offsets.len() >= 32 {
            return None;
        }
        segment_offset = next_segment_offset;
    };
    if segment_offsets.len() < 3 {
        return None;
    }

    Some(TransmitterFlowStatusRecordGeometry {
        record_end,
        segment_offsets,
    })
}

fn transmitter_flow_status_segment_u16(
    response: &[u8],
    segment_start: usize,
    segment_end: usize,
    field_offset: usize,
) -> Option<u16> {
    let field_start = segment_start.checked_add(field_offset)?;
    let field_end = field_start.checked_add(2)?;
    (field_end <= segment_end).then(|| read_u16(response, field_start))?
}

fn parse_audio_channel_slot_segment(
    response: &[u8],
    geometry: &TransmitterFlowStatusRecordGeometry,
    media_type: u16,
) -> Option<(Option<u16>, Option<u16>, Vec<u16>)> {
    if media_type != MEDIA_TYPE_AUDIO {
        return Some((None, None, Vec::new()));
    }
    let mut parsed = None;
    for (index, segment_start) in geometry.segment_offsets.iter().copied().enumerate() {
        let segment_end = geometry
            .segment_offsets
            .get(index + 1)
            .copied()
            .unwrap_or(geometry.record_end);
        let Some(count) = transmitter_flow_status_segment_u16(
            response,
            segment_start,
            segment_end,
            TRANSMITTER_FLOW_STATUS_SLOT_COUNT,
        ) else {
            continue;
        };
        let Some(slot_ids_size) = usize::from(count).checked_mul(2) else {
            continue;
        };
        let Some(expected_size) = TRANSMITTER_FLOW_STATUS_SLOT_IDS
            .checked_add(slot_ids_size)
            .and_then(|size| size.checked_add(TRANSMITTER_FLOW_STATUS_SLOT_TRAILING_FIELD_SIZE))
        else {
            continue;
        };
        let header = read_u16(response, segment_start)?;
        let Ok(expected_next_words) = u8::try_from(expected_size / 2) else {
            continue;
        };
        let Some(expected_terminal_words) = expected_next_words.checked_add(2) else {
            continue;
        };
        if expected_size != segment_end.checked_sub(segment_start)?
            || header.to_be_bytes() != [expected_next_words, expected_terminal_words]
        {
            continue;
        }
        if parsed.is_some() {
            return None;
        }
        let mut channel_ids = Vec::with_capacity(usize::from(count));
        for slot_index in 0..count {
            let field_offset = TRANSMITTER_FLOW_STATUS_SLOT_IDS
                .checked_add(usize::from(slot_index).checked_mul(2)?)?;
            channel_ids.push(transmitter_flow_status_segment_u16(
                response,
                segment_start,
                segment_end,
                field_offset,
            )?);
        }
        parsed = Some((Some(header), Some(count), channel_ids));
    }
    parsed
}

fn parse_transmitter_flow_status_record(
    response: &[u8],
    record_pointer: u16,
) -> Option<TransmitterFlowStatus> {
    let record_offset = usize::from(record_pointer);
    let geometry = transmitter_flow_status_record_geometry(response, record_offset)?;
    let first_segment_end = *geometry.segment_offsets.get(1)?;
    let global_flow_id = transmitter_flow_status_segment_u16(
        response,
        record_offset,
        first_segment_end,
        TRANSMITTER_FLOW_STATUS_RECORD_FLOW_NUMBER,
    )?;
    let media_type = transmitter_flow_status_segment_u16(
        response,
        record_offset,
        first_segment_end,
        TRANSMITTER_FLOW_STATUS_RECORD_MEDIA_TYPE,
    )?;
    let media_local_flow_id = transmitter_flow_status_segment_u16(
        response,
        record_offset,
        first_segment_end,
        TRANSMITTER_FLOW_STATUS_RECORD_MEDIA_LOCAL_ID,
    )?;
    if global_flow_id == 0 || media_type == 0 || media_local_flow_id == 0 {
        return None;
    }
    let flow_type_code = transmitter_flow_status_segment_u16(
        response,
        record_offset,
        first_segment_end,
        TRANSMITTER_FLOW_STATUS_RECORD_FLOW_TYPE,
    )?;
    let flow_type = match flow_type_code {
        FLOW_TYPE_MULTICAST => Some("multicast".to_owned()),
        FLOW_TYPE_UNICAST => Some("unicast".to_owned()),
        _ => None,
    };

    let flow_name_pointer = transmitter_flow_status_segment_u16(
        response,
        record_offset,
        first_segment_end,
        TRANSMITTER_FLOW_STATUS_RECORD_NAME_POINTER,
    )?;
    let flow_name = string_at_pointer(response, flow_name_pointer)?;
    let format_pointer = transmitter_flow_status_segment_u16(
        response,
        record_offset,
        first_segment_end,
        TRANSMITTER_FLOW_STATUS_RECORD_FORMAT_POINTER,
    )?;
    let format_offset = usize::from(format_pointer);
    response.get(format_offset..format_offset.checked_add(TRANSMITTER_FLOW_STATUS_FORMAT_SIZE)?)?;
    let sample_rate = read_u32(response, format_offset)?;
    let encoding = read_u32(response, format_offset.checked_add(4)?)?;
    if sample_rate == 0 || encoding == 0 {
        return None;
    }

    let subscriber_segment_start = *geometry
        .segment_offsets
        .get(TRANSMITTER_FLOW_STATUS_SUBSCRIBER_SEGMENT_INDEX)?;
    let subscriber_segment_end = *geometry
        .segment_offsets
        .get(TRANSMITTER_FLOW_STATUS_SUBSCRIBER_SEGMENT_INDEX + 1)?;
    let subscriber_device_name_pointer = transmitter_flow_status_segment_u16(
        response,
        subscriber_segment_start,
        subscriber_segment_end,
        TRANSMITTER_FLOW_STATUS_SUBSCRIBER_DEVICE_POINTER,
    )?;
    let subscriber_device_name =
        optional_string_at_pointer(response, subscriber_device_name_pointer)?;
    let subscriber_flow_name_pointer = transmitter_flow_status_segment_u16(
        response,
        subscriber_segment_start,
        subscriber_segment_end,
        TRANSMITTER_FLOW_STATUS_SUBSCRIBER_FLOW_POINTER,
    )?;
    let subscriber_flow_name = optional_string_at_pointer(response, subscriber_flow_name_pointer)?;

    let endpoint_segment_start = *geometry
        .segment_offsets
        .get(TRANSMITTER_FLOW_STATUS_ENDPOINT_SEGMENT_INDEX)?;
    let endpoint_segment_end = *geometry
        .segment_offsets
        .get(TRANSMITTER_FLOW_STATUS_ENDPOINT_SEGMENT_INDEX + 1)?;
    let endpoint_descriptor_pointer = transmitter_flow_status_segment_u16(
        response,
        endpoint_segment_start,
        endpoint_segment_end,
        TRANSMITTER_FLOW_STATUS_ENDPOINT_POINTER,
    )?;
    let endpoint_descriptor_offset = usize::from(endpoint_descriptor_pointer);
    let endpoint_descriptor = response.get(
        endpoint_descriptor_offset
            ..endpoint_descriptor_offset.checked_add(TRANSMITTER_FLOW_STATUS_ENDPOINT_SIZE)?,
    )?;
    let (destination_user_datagram_port, destination_address) =
        if endpoint_descriptor.get(..2)? == [0x08, 0x02] {
            (
                read_u16(endpoint_descriptor, 2),
                ipv4_at(endpoint_descriptor, 4),
            )
        } else {
            (None, None)
        };

    let (channel_slot_segment_header, channel_slot_count, transmitter_channel_ids_by_slot) =
        parse_audio_channel_slot_segment(response, &geometry, media_type)?;
    let populated_transmitter_channel_ids: Vec<u16> = transmitter_channel_ids_by_slot
        .iter()
        .copied()
        .filter(|channel_id| *channel_id != 0)
        .collect();
    let populated_slot_count = u16::try_from(populated_transmitter_channel_ids.len()).ok()?;
    let record = response.get(record_offset..geometry.record_end)?;
    let record_length_bytes = u16::try_from(record.len()).ok()?;

    Some(TransmitterFlowStatus {
        record_pointer,
        record_length_bytes,
        global_flow_id,
        flow_number: global_flow_id,
        media_type,
        media_local_flow_id,
        flow_name_pointer,
        flow_name,
        flow_type_code,
        flow_type,
        format_pointer,
        sample_rate,
        encoding,
        channel_count: populated_slot_count,
        channel_slot_segment_header,
        channel_slot_count,
        transmitter_channel_ids_by_slot,
        populated_transmitter_channel_ids,
        populated_slot_count,
        endpoint_descriptor_pointer,
        endpoint_descriptor_hexadecimal: bytes_to_hex(endpoint_descriptor),
        destination_user_datagram_port,
        destination_internet_protocol_version_four_address: destination_address,
        subscriber_device_name_pointer,
        subscriber_device_name,
        subscriber_flow_name_pointer,
        subscriber_flow_name,
        raw_record_hexadecimal: bytes_to_hex(record),
    })
}

fn optional_string_at_pointer(data: &[u8], pointer: u16) -> Option<Option<String>> {
    if pointer == 0 {
        return Some(None);
    }
    Some(Some(string_at_pointer(data, pointer)?))
}

pub fn parse_receiver_flow_page(response: &[u8]) -> Option<ReceiverFlowPage> {
    let envelope = validate_response_envelope(
        response,
        &[(PROTOCOL_DANTE_FLOW, OPCODE_QUERY_RECEIVER_FLOWS)],
        &[RESULT_CODE_SUCCESS, crate::protocol::RESULT_CODE_MORE_PAGES],
    )?;
    let body = envelope.body;
    let maximum_records = usize::from(*body.first()?);
    let active_count = usize::from(*body.get(1)?);
    if !(1..=32).contains(&maximum_records) || active_count > maximum_records {
        return None;
    }
    let pointer_table_size = maximum_records.checked_mul(2)?;
    let records_start = 2usize.checked_add(pointer_table_size)?;
    body.get(..records_start)?;

    let mut record_offsets = Vec::with_capacity(active_count);
    for index in 0..maximum_records {
        let record_pointer = read_u16(body, 2 + index * 2)?;
        if record_pointer == 0 {
            continue;
        }
        let record_offset = usize::from(record_pointer).checked_sub(RESPONSE_HEADER_SIZE)?;
        if record_offset < records_start
            || record_offsets
                .last()
                .is_some_and(|previous_offset| record_offset <= *previous_offset)
        {
            return None;
        }
        record_offsets.push(record_offset);
    }
    if record_offsets.len() != active_count {
        return None;
    }

    let mut flows = Vec::with_capacity(active_count);
    let mut flow_numbers = HashSet::with_capacity(active_count);
    for (index, record_offset) in record_offsets.iter().copied().enumerate() {
        let record_end = record_offsets.get(index + 1).copied().unwrap_or(body.len());
        let flow = parse_receiver_flow_record(body, record_offset, record_end)?;
        if !(1..=32).contains(&flow.flow_number) || !flow_numbers.insert(flow.flow_number) {
            return None;
        }
        flows.push(flow);
    }

    Some(ReceiverFlowPage {
        maximum_flow_slots: u8::try_from(maximum_records).ok()?,
        flows,
    })
}

pub fn parse_receiver_port_ranges(response: &[u8]) -> Option<ReceiverPortRanges> {
    let envelope = validate_response_envelope(
        response,
        &[(PROTOCOL_DANTE_FLOW, OPCODE_QUERY_RECEIVER_PORT_RANGES)],
        &[RESULT_CODE_SUCCESS],
    )?;
    if envelope.body.len() != 8 {
        return None;
    }
    let ranges = ReceiverPortRanges {
        first_port_range_start: read_u16(envelope.body, 0)?,
        first_port_range_end: read_u16(envelope.body, 2)?,
        second_port_range_start: read_u16(envelope.body, 4)?,
        second_port_range_end: read_u16(envelope.body, 6)?,
    };
    if ranges.first_port_range_start > ranges.first_port_range_end
        || ranges.first_port_range_end >= ranges.second_port_range_start
        || ranges.second_port_range_start > ranges.second_port_range_end
    {
        return None;
    }
    Some(ranges)
}

pub fn parse_transmit_channel_capabilities(response: &[u8]) -> Option<TransmitChannelCapabilities> {
    let envelope = validate_response_envelope(
        response,
        &[(
            PROTOCOL_DANTE_FLOW,
            OPCODE_QUERY_TRANSMIT_CHANNEL_CAPABILITIES,
        )],
        &[RESULT_CODE_SUCCESS],
    )?;
    if envelope.body.len() != 8 {
        return None;
    }
    Some(TransmitChannelCapabilities {
        format_identifier: read_u16(envelope.body, 0)?,
        starting_channel_identifier: read_u16(envelope.body, 2)?,
        channel_count: read_u16(envelope.body, 4)?,
        capability_flags: read_u16(envelope.body, 6)?,
    })
}

fn parse_receiver_flow_record(
    body: &[u8],
    record_offset: usize,
    record_end: usize,
) -> Option<ReceiverFlow> {
    let channel_count = read_u16(body, record_offset.checked_add(14)?)?;
    if channel_count == 0 {
        return None;
    }
    let channel_pointer_bytes = usize::from(channel_count).checked_mul(2)?;
    let status_pointer_position = record_offset
        .checked_add(20)?
        .checked_add(channel_pointer_bytes)?;
    let record_data_start = status_pointer_position.checked_add(2)?;
    if record_data_start > record_end {
        return None;
    }

    let endpoint_descriptor_size = read_u16(body, record_offset.checked_add(16)?)?;
    if endpoint_descriptor_size < 4 {
        return None;
    }
    let endpoint_descriptor_offset = usize::from(read_u16(body, record_offset.checked_add(18)?)?)
        .checked_sub(RESPONSE_HEADER_SIZE)?;
    let endpoint_descriptor_end =
        endpoint_descriptor_offset.checked_add(usize::from(endpoint_descriptor_size))?;

    let mut channel_descriptor_ranges = Vec::with_capacity(usize::from(channel_count));
    for index in 0..usize::from(channel_count) {
        let pointer_position = record_offset
            .checked_add(20)?
            .checked_add(index.checked_mul(2)?)?;
        let descriptor_offset =
            usize::from(read_u16(body, pointer_position)?).checked_sub(RESPONSE_HEADER_SIZE)?;
        let descriptor_end = descriptor_offset.checked_add(16)?;
        channel_descriptor_ranges.push((descriptor_offset, descriptor_end));
    }

    let status_offset =
        usize::from(read_u16(body, status_pointer_position)?).checked_sub(RESPONSE_HEADER_SIZE)?;
    let status_end = status_offset.checked_add(16)?;
    let mut occupied_ranges = channel_descriptor_ranges.clone();
    occupied_ranges.push((endpoint_descriptor_offset, endpoint_descriptor_end));
    occupied_ranges.push((status_offset, status_end));
    occupied_ranges.sort_unstable();
    if occupied_ranges
        .iter()
        .any(|(start, end)| *start < record_data_start || *end > record_end || start >= end)
        || occupied_ranges
            .windows(2)
            .any(|ranges| ranges[0].1 > ranges[1].0)
    {
        return None;
    }

    let endpoint_descriptor = body.get(endpoint_descriptor_offset..endpoint_descriptor_end)?;
    let destination_address_offset = endpoint_descriptor_end.checked_sub(4)?;
    let destination_address = body.get(destination_address_offset..endpoint_descriptor_end)?;
    let endpoint_has_version_four_user_datagram_layout =
        endpoint_descriptor.len() == 8 && endpoint_descriptor.get(0..2) == Some(&[0x08, 0x02]);
    let destination_user_datagram_port = if endpoint_has_version_four_user_datagram_layout {
        read_u16(endpoint_descriptor, 2)
    } else {
        None
    };
    let flow_type = endpoint_has_version_four_user_datagram_layout.then(|| {
        if (224..=239).contains(&destination_address[0]) {
            "multicast".to_owned()
        } else {
            "unicast".to_owned()
        }
    });
    let mut channel_descriptors_hexadecimal = Vec::with_capacity(usize::from(channel_count));
    let mut receiver_channel_numbers_by_flow_channel =
        Vec::with_capacity(usize::from(channel_count));
    for (descriptor_offset, descriptor_end) in channel_descriptor_ranges {
        let descriptor = body.get(descriptor_offset..descriptor_end)?;
        channel_descriptors_hexadecimal.push(bytes_to_hex(descriptor));
        receiver_channel_numbers_by_flow_channel.push(receiver_channel_numbers(descriptor)?);
    }

    Some(ReceiverFlow {
        flow_number: read_u16(body, record_offset)?,
        flow_state_code: read_u16(body, record_offset.checked_add(2)?)?,
        flow_type,
        sample_rate: read_u32(body, record_offset.checked_add(4)?)?,
        encoding: u16::try_from(read_u32(body, record_offset.checked_add(8)?)?).ok()?,
        frames_per_packet: read_u16(body, record_offset.checked_add(12)?)?,
        channel_count,
        endpoint_descriptor_size,
        endpoint_descriptor_hexadecimal: bytes_to_hex(endpoint_descriptor),
        destination_user_datagram_port,
        destination_internet_protocol_version_four_address: ipv4_at(
            body,
            destination_address_offset,
        )?,
        channel_descriptors_hexadecimal,
        receiver_channel_numbers_by_flow_channel,
        subscription_status_code: read_u16(body, status_offset)?,
        status_field_at_byte_offset_two: read_u16(body, status_offset.checked_add(2)?)?,
        status_field_at_byte_offset_four: read_u16(body, status_offset.checked_add(4)?)?,
        status_field_at_byte_offset_six: read_u16(body, status_offset.checked_add(6)?)?,
        latency_nanoseconds: read_u32(body, status_offset.checked_add(8)?)?,
        status_field_at_byte_offset_twelve: read_u32(body, status_offset.checked_add(12)?)?,
        raw_record_hexadecimal: bytes_to_hex(body.get(record_offset..record_end)?),
    })
}

pub(super) fn receiver_channel_numbers(descriptor: &[u8]) -> Option<Vec<u16>> {
    if descriptor.len() != 16 {
        return None;
    }
    let mut receiver_channel_numbers = Vec::new();
    for (word_index, bytes) in descriptor.chunks_exact(2).enumerate() {
        let word = u16::from_be_bytes([bytes[0], bytes[1]]);
        for bit_index in 0..16 {
            if word & (1 << bit_index) != 0 {
                receiver_channel_numbers.push(u16::try_from(word_index * 16 + bit_index + 1).ok()?);
            }
        }
    }
    Some(receiver_channel_numbers)
}

fn parse_flow_record(body: &[u8], offset: usize, record_end: usize) -> Option<TxFlow> {
    let fixed_end = offset.checked_add(FLOW_RECORD_FIXED_SIZE)?;
    if fixed_end > record_end {
        return None;
    }
    body.get(offset..record_end)?;
    let flow_number = read_u16(body, offset)?;
    let flow_type_code = u16_at(body, offset + FLOW_RECORD_FLOW_TYPE);
    let sample_rate = read_u32(body, offset + FLOW_RECORD_SAMPLE_RATE)?;
    let encoding = u16::try_from(read_u32(body, offset + FLOW_RECORD_ENCODING)?).ok()?;
    let frames_per_packet = u16_at(body, offset + FLOW_RECORD_FRAMES_PER_PACKET);
    let channel_count = read_u16(body, offset + FLOW_RECORD_CHANNEL_COUNT)?;
    if channel_count == 0 {
        return None;
    }

    let (flow_type, channels) = match flow_type_code {
        FLOW_TYPE_MULTICAST => (
            "multicast".to_owned(),
            flow_channel_list(body, offset, record_end, frames_per_packet, channel_count)?,
        ),
        FLOW_TYPE_UNICAST => ("unicast".to_owned(), Vec::new()),
        _ => return None,
    };

    Some(TxFlow {
        flow_number,
        flow_type,
        sample_rate,
        encoding,
        frames_per_packet,
        channel_count,
        channels,
    })
}

fn flow_channel_list(
    body: &[u8],
    record_offset: usize,
    record_end: usize,
    frames_per_packet: u16,
    channel_count: u16,
) -> Option<Vec<u16>> {
    let channel_bytes = usize::from(channel_count).checked_mul(2)?;
    let variable_prefix_bytes = usize::from(frames_per_packet).checked_mul(2)?;
    let channels_start = record_offset
        .checked_add(FLOW_RECORD_FIXED_SIZE)?
        .checked_add(variable_prefix_bytes)?;
    let channels_end = channels_start.checked_add(channel_bytes)?;
    if channels_end > record_end {
        return None;
    }
    body.get(channels_start..channels_end)?;
    let mut channels = Vec::with_capacity(usize::from(channel_count));
    let mut seen = HashSet::with_capacity(usize::from(channel_count));
    let mut channel_offset = channels_start;
    for _ in 0..channel_count {
        let channel_number = read_u16(body, channel_offset)?;
        if channel_number != 0 && !seen.insert(channel_number) {
            return None;
        }
        channels.push(channel_number);
        channel_offset += 2;
    }
    Some(channels)
}
