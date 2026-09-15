use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MulticastFlowCreation2809 {
    pub global_flow_id: u16,
    pub media_type_code: u16,
    pub media_local_flow_id: u16,
    pub channels: Vec<u16>,
}

/// Allocation acknowledgments can contain an unspecified sample rate. They are
/// not inventory pages; callers must obtain fresh inventory before claiming that
/// an allocated flow is configured and transmitting.
pub fn parse_multicast_flow_creation_2809(response: &[u8]) -> Option<MulticastFlowCreation2809> {
    validate_response_envelope(
        response,
        &[(PROTOCOL_ARC_2809, OPCODE_CREATE_TX_FLOW_2809)],
        &[RESULT_CODE_SUCCESS],
    )?;
    if read_u16(response, 16)? != 0x0101 || read_u16(response, 18)? != 32 {
        return None;
    }
    let record_offset = 32;
    let geometry = transmitter_flow_status_record_geometry(response, record_offset)?;
    // All observed allocations have five segments followed by one terminal word.
    if geometry.segment_offsets.len() != 5 || geometry.record_end.checked_add(2)? != response.len()
    {
        return None;
    }
    let first_segment_end = *geometry.segment_offsets.get(1)?;
    let field = |offset| {
        transmitter_flow_status_segment_u16(response, record_offset, first_segment_end, offset)
    };
    let global_flow_id = field(TRANSMITTER_FLOW_STATUS_RECORD_FLOW_NUMBER)?;
    let media_type_code = field(TRANSMITTER_FLOW_STATUS_RECORD_MEDIA_TYPE)?;
    let media_local_flow_id = field(TRANSMITTER_FLOW_STATUS_RECORD_MEDIA_LOCAL_ID)?;
    if !(1..=32).contains(&global_flow_id)
        || media_type_code != MEDIA_TYPE_AUDIO
        || media_local_flow_id == 0
        || field(TRANSMITTER_FLOW_STATUS_RECORD_FLOW_TYPE)? != FLOW_TYPE_MULTICAST
    {
        return None;
    }
    let (_, count, channels) =
        parse_audio_channel_slot_segment(response, &geometry, media_type_code)?;
    if count? == 0 || channels.contains(&0) {
        return None;
    }
    let mut unique_channels = HashSet::with_capacity(channels.len());
    if !channels
        .iter()
        .all(|channel| unique_channels.insert(*channel))
    {
        return None;
    }
    Some(MulticastFlowCreation2809 {
        global_flow_id,
        media_type_code,
        media_local_flow_id,
        channels,
    })
}

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

    let flows = parse_pointer_table_page(
        response,
        MODERN_ARC_POINTER_TABLE_OFFSET,
        reported_flow_count,
        |flow: &TransmitterFlowStatus| usize::from(flow.record_length_bytes),
        |response, record_pointer, _| {
            parse_transmitter_flow_status_record(response, record_pointer)
        },
    )?;
    let mut flow_numbers = HashSet::with_capacity(flows.len());
    let mut media_identities = HashSet::with_capacity(flows.len());
    for flow in &flows {
        if flow.global_flow_id > u16::from(maximum_flow_slots)
            || !flow_numbers.insert(flow.global_flow_id)
            || !media_identities.insert((flow.media_type_code, flow.media_local_flow_id))
        {
            return None;
        }
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
    let media_type_code = transmitter_flow_status_segment_u16(
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
    if global_flow_id == 0 || media_type_code == 0 || media_local_flow_id == 0 {
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
    let format_size = match media_type_code {
        MEDIA_TYPE_AUDIO => 8,
        MEDIA_TYPE_VIDEO => 16,
        _ => return None,
    };
    let format_descriptor = response.get(format_offset..format_offset.checked_add(format_size)?)?;
    if format_offset.checked_add(format_size)? > record_offset {
        return None;
    }
    let (sample_rate, encoding) = if media_type_code == MEDIA_TYPE_AUDIO {
        let sample_rate = read_u32(format_descriptor, 0)?;
        let encoding = read_u32(format_descriptor, 4)?;
        if sample_rate == 0 || encoding == 0 {
            return None;
        }
        (Some(sample_rate), Some(encoding))
    } else {
        (None, None)
    };

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
        parse_audio_channel_slot_segment(response, &geometry, media_type_code)?;
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
        media_type_code,
        media_local_flow_id,
        flow_name_pointer,
        flow_name,
        flow_type_code,
        flow_type,
        format_pointer,
        format_descriptor_hexadecimal: bytes_to_hex(format_descriptor),
        sample_rate,
        encoding,
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
            || record_offset >= body.len()
            || record_offsets.contains(&record_offset)
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
    for record_offset in record_offsets.iter().copied() {
        let record_end = record_offsets
            .iter()
            .copied()
            .filter(|candidate| *candidate > record_offset)
            .min()
            .unwrap_or(body.len());
        let flow = parse_receiver_flow_record(body, record_offset, record_end)?;
        if !(1..=32).contains(&flow.flow_number) || !flow_numbers.insert(flow.flow_number) {
            return None;
        }
        flows.push(flow);
    }
    flows.sort_unstable_by_key(|flow| flow.flow_number);

    Some(ReceiverFlowPage {
        result_code: envelope.result_code,
        page_disposition: if envelope.result_code == RESULT_CODE_SUCCESS {
            ModernArcPageDisposition::Complete
        } else {
            ModernArcPageDisposition::MorePages
        },
        maximum_flow_slots: u8::try_from(maximum_records).ok()?,
        reported_flow_count: u8::try_from(active_count).ok()?,
        flows,
    })
}

pub fn parse_receiver_port_ranges(response: &[u8]) -> Option<ReceiverPortRanges> {
    let envelope = validate_response_envelope(
        response,
        &[
            (PROTOCOL_DANTE_FLOW, OPCODE_QUERY_RECEIVER_PORT_RANGES),
            (PROTOCOL_ARC_2809, OPCODE_QUERY_RECEIVER_PORT_RANGES),
        ],
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
        second_port_range_available: true,
    };
    if ranges.first_port_range_start > ranges.first_port_range_end {
        return None;
    }
    let second_port_range_available = ranges.first_port_range_end < ranges.second_port_range_start
        && ranges.second_port_range_start <= ranges.second_port_range_end;
    let modern_empty_second_port_range = envelope.protocol_id == PROTOCOL_ARC_2809
        && ranges.first_port_range_end == ranges.second_port_range_end
        && ranges
            .second_port_range_end
            .checked_add(1)
            .is_some_and(|next_port| next_port == ranges.second_port_range_start);
    if !second_port_range_available && !modern_empty_second_port_range {
        return None;
    }
    Some(ReceiverPortRanges {
        second_port_range_available,
        ..ranges
    })
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
    let reserved = *envelope.body.first()?;
    let record_count = *envelope.body.get(1)?;
    let expected_length = 2usize.checked_add(usize::from(record_count).checked_mul(6)?)?;
    if reserved != 0 || envelope.body.len() != expected_length {
        return None;
    }
    let mut ranges = Vec::with_capacity(usize::from(record_count));
    for index in 0..usize::from(record_count) {
        let offset = 2usize.checked_add(index.checked_mul(6)?)?;
        let first_transmit_channel = read_u16(envelope.body, offset)?;
        let last_transmit_channel = read_u16(envelope.body, offset.checked_add(2)?)?;
        if first_transmit_channel == 0 || last_transmit_channel < first_transmit_channel {
            return None;
        }
        ranges.push(TransmitChannelCapabilityRange {
            first_transmit_channel,
            last_transmit_channel,
            unknown_value: read_u16(envelope.body, offset.checked_add(4)?)?,
        });
    }
    Some(TransmitChannelCapabilities {
        record_count,
        ranges,
    })
}

fn parse_receiver_flow_record(
    body: &[u8],
    record_offset: usize,
    record_end: usize,
) -> Option<ReceiverFlow> {
    let flags = read_u16(body, record_offset.checked_add(2)?)?;
    if flags & 0x4000 != 0 {
        return None;
    }
    let interface_count = read_u16(body, record_offset.checked_add(12)?)?;
    let flow_channel_slot_count = read_u16(body, record_offset.checked_add(14)?)?;
    let receiver_bitmap_word_count = read_u16(body, record_offset.checked_add(16)?)?;
    if interface_count == 0 || flow_channel_slot_count == 0 || receiver_bitmap_word_count == 0 {
        return None;
    }
    let pointer_count = usize::from(interface_count)
        .checked_add(usize::from(flow_channel_slot_count))?
        .checked_add(1)?;
    let pointer_table_start = record_offset.checked_add(18)?;
    let record_data_start = pointer_table_start.checked_add(pointer_count.checked_mul(2)?)?;
    if record_data_start > record_end {
        return None;
    }
    let absolute_pointer = |position: usize| -> Option<(u16, usize)> {
        let pointer = read_u16(body, position)?;
        let offset = usize::from(pointer).checked_sub(RESPONSE_HEADER_SIZE)?;
        (offset >= record_data_start && offset < record_end).then_some((pointer, offset))
    };

    let mut occupied_ranges = Vec::with_capacity(pointer_count + 1);
    let mut interface_endpoints = Vec::with_capacity(usize::from(interface_count));
    for index in 0..usize::from(interface_count) {
        let pointer_position = pointer_table_start.checked_add(index.checked_mul(2)?)?;
        let (pointer, descriptor_offset) = absolute_pointer(pointer_position)?;
        let descriptor_length_bytes = *body.get(descriptor_offset)?;
        if !matches!(descriptor_length_bytes, 4 | 8) {
            return None;
        }
        let descriptor_end = descriptor_offset.checked_add(usize::from(descriptor_length_bytes))?;
        let descriptor = body.get(descriptor_offset..descriptor_end)?;
        if descriptor_end > record_end || *descriptor.get(1)? != 2 {
            return None;
        }
        occupied_ranges.push((descriptor_offset, descriptor_end));
        interface_endpoints.push(ReceiverFlowInterfaceEndpoint {
            pointer,
            descriptor_length_bytes,
            kind: 2,
            udp_port: read_u16(descriptor, 2)?,
            ipv4_address: if descriptor_length_bytes == 8 {
                Some(ipv4_at(descriptor, 4)?)
            } else {
                None
            },
            raw_descriptor_hexadecimal: bytes_to_hex(descriptor),
        });
    }

    let bitmap_size = usize::from(receiver_bitmap_word_count).checked_mul(2)?;
    let mut receiver_bitmaps_hexadecimal = Vec::with_capacity(usize::from(flow_channel_slot_count));
    let mut receiver_channel_numbers_by_flow_channel =
        Vec::with_capacity(usize::from(flow_channel_slot_count));
    for index in 0..usize::from(flow_channel_slot_count) {
        let table_index = usize::from(interface_count).checked_add(index)?;
        let pointer_position = pointer_table_start.checked_add(table_index.checked_mul(2)?)?;
        let (_, bitmap_offset) = absolute_pointer(pointer_position)?;
        let bitmap_end = bitmap_offset.checked_add(bitmap_size)?;
        let bitmap = body.get(bitmap_offset..bitmap_end)?;
        if bitmap_end > record_end {
            return None;
        }
        occupied_ranges.push((bitmap_offset, bitmap_end));
        receiver_bitmaps_hexadecimal.push(bytes_to_hex(bitmap));
        receiver_channel_numbers_by_flow_channel.push(receiver_channel_numbers(bitmap)?);
    }

    let status_pointer_position = pointer_table_start.checked_add(
        usize::from(interface_count)
            .checked_add(usize::from(flow_channel_slot_count))?
            .checked_mul(2)?,
    )?;
    let (_, status_offset) = absolute_pointer(status_pointer_position)?;
    let status_end = status_offset.checked_add(16)?;
    let status_descriptor = body.get(status_offset..status_end)?;
    if status_end > record_end {
        return None;
    }
    occupied_ranges.push((status_offset, status_end));

    let transport = read_u16(status_descriptor, 12)?;
    let external_identity_pointer = read_u16(status_descriptor, 14)?;
    let external_identity = if transport == 3 {
        let identity_offset =
            usize::from(external_identity_pointer).checked_sub(RESPONSE_HEADER_SIZE)?;
        if identity_offset < record_data_start || identity_offset >= record_end {
            return None;
        }
        let length_words = *body.get(identity_offset)?;
        let identity_length = usize::from(length_words).checked_mul(2)?;
        if identity_length != 28 {
            return None;
        }
        let identity_end = identity_offset.checked_add(identity_length)?;
        let descriptor = body.get(identity_offset..identity_end)?;
        if identity_end > record_end {
            return None;
        }
        occupied_ranges.push((identity_offset, identity_end));
        let presence_mask = read_u16(descriptor, 2)?;
        Some(ExternalRtpFlowIdentity {
            pointer: external_identity_pointer,
            length_words,
            reserved: *descriptor.get(1)?,
            presence_mask,
            source_ipv4: if presence_mask & 0x0001 != 0 {
                Some(ipv4_at(descriptor, 4)?)
            } else {
                None
            },
            session_id: if presence_mask & 0x0002 != 0 {
                Some(read_u64(descriptor, 8)?)
            } else {
                None
            },
            unknown_optional_field_raw: read_u64(descriptor, 16)?,
            clock_offset: if presence_mask & 0x0008 != 0 {
                Some(read_u32(descriptor, 24)?)
            } else {
                None
            },
            raw_descriptor_hexadecimal: bytes_to_hex(descriptor),
        })
    } else {
        None
    };

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

    let flow_type = interface_endpoints
        .iter()
        .find_map(|endpoint| endpoint.ipv4_address.as_deref())
        .and_then(|address| address.split('.').next())
        .and_then(|octet| octet.parse::<u8>().ok())
        .map(|first_octet| {
            if (224..=239).contains(&first_octet) {
                "multicast".to_owned()
            } else {
                "unicast".to_owned()
            }
        });
    let mut effective_subscription_identities = Vec::new();
    if let Some(identity) = &external_identity {
        if let (Some(source), Some(session_id)) =
            (identity.source_ipv4.as_ref(), identity.session_id)
        {
            for (slot_index, receiver_channels) in
                receiver_channel_numbers_by_flow_channel.iter().enumerate()
            {
                let flow_slot = u16::try_from(slot_index).ok()?.checked_add(1)?;
                for receiver_channel_number in receiver_channels {
                    effective_subscription_identities.push(ReceiverFlowSubscriptionIdentity {
                        receiver_channel: *receiver_channel_number,
                        flow_slot,
                        source_ipv4: source.clone(),
                        session_id,
                        interface_endpoints: interface_endpoints.clone(),
                    });
                }
            }
        }
    }

    Some(ReceiverFlow {
        flow_number: read_u16(body, record_offset)?,
        flags,
        flow_type,
        sample_rate: read_u32(body, record_offset.checked_add(4)?)?,
        encoding: read_u32(body, record_offset.checked_add(8)?)?,
        interface_count,
        flow_channel_slot_count,
        receiver_bitmap_word_count,
        interface_endpoints,
        receiver_bitmaps_hexadecimal,
        receiver_channel_numbers_by_flow_channel,
        subscription_status_code: read_u16(status_descriptor, 0)?,
        interface_state_bitmap: read_u16(status_descriptor, 2)?,
        status_flags: read_u16(status_descriptor, 4)?,
        status_unknown: read_u16(status_descriptor, 6)?,
        latency_nanoseconds: read_u32(status_descriptor, 8)?,
        transport,
        external_identity_pointer,
        external_identity,
        effective_subscription_identities,
        status_descriptor_hexadecimal: bytes_to_hex(status_descriptor),
        raw_record_hexadecimal: bytes_to_hex(body.get(record_offset..record_end)?),
    })
}

pub(super) fn receiver_channel_numbers(descriptor: &[u8]) -> Option<Vec<u16>> {
    if descriptor.is_empty() || descriptor.len() % 2 != 0 {
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
