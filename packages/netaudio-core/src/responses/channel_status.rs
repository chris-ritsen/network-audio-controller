use super::*;

fn modern_arc_page_disposition(result_code: u16) -> Option<ModernArcPageDisposition> {
    match result_code {
        RESULT_CODE_SUCCESS => Some(ModernArcPageDisposition::Complete),
        crate::protocol::RESULT_CODE_MORE_PAGES => Some(ModernArcPageDisposition::MorePages),
        _ => None,
    }
}

pub fn parse_transmitter_channel_name_reconciliation_2809(
    response: &[u8],
) -> Option<TransmitterChannelNameReconciliation2809> {
    let envelope = validate_response_envelope(
        response,
        &[(
            PROTOCOL_ARC_2809,
            OPCODE_RECONCILE_TRANSMITTER_CHANNEL_NAMES_2809,
        )],
        &[RESULT_CODE_SUCCESS],
    )?;
    let body = envelope.body;
    if body.len() < 10 || body.get(0..6)? != [0u8; 6] || read_u16(body, 6)? != 0x0600 {
        return None;
    }

    let declared_channel_count = *body.get(8)?;
    let reported_record_count = *body.get(9)?;
    if declared_channel_count == 0 || reported_record_count != declared_channel_count {
        return None;
    }

    let descriptor_table_end =
        20usize.checked_add(usize::from(reported_record_count).checked_mul(6)?)?;
    response.get(20..descriptor_table_end)?;
    let mut records = Vec::with_capacity(usize::from(reported_record_count));
    let mut channel_numbers = HashSet::with_capacity(usize::from(reported_record_count));
    let mut name_pointers = HashSet::with_capacity(usize::from(reported_record_count));
    let mut name_ranges = Vec::with_capacity(usize::from(reported_record_count));
    for index in 0..reported_record_count {
        let descriptor_offset = 20usize.checked_add(usize::from(index).checked_mul(6)?)?;
        let channel_number = read_u16(response, descriptor_offset)?;
        let record_type_code = read_u16(response, descriptor_offset.checked_add(2)?)?;
        let name_pointer = read_u16(response, descriptor_offset.checked_add(4)?)?;
        if channel_number == 0
            || record_type_code != 0x0003
            || usize::from(name_pointer) < descriptor_table_end
            || !channel_numbers.insert(channel_number)
            || !name_pointers.insert(name_pointer)
        {
            return None;
        }
        let name = string_at_pointer(response, name_pointer)?;
        if name.is_empty() {
            return None;
        }
        let name_end = usize::from(name_pointer)
            .checked_add(name.len())?
            .checked_add(1)?;
        response.get(usize::from(name_pointer)..name_end)?;
        name_ranges.push((usize::from(name_pointer), name_end));
        records.push(TransmitterChannelNameReconciliationRecord2809 {
            channel_number,
            record_type_code,
            name_pointer,
            name,
        });
    }
    name_ranges.sort_unstable();
    if name_ranges
        .windows(2)
        .any(|ranges| ranges[0].1 > ranges[1].0)
    {
        return None;
    }

    Some(TransmitterChannelNameReconciliation2809 {
        declared_channel_count,
        reported_record_count,
        records,
        raw_body_hexadecimal: bytes_to_hex(body),
    })
}

pub fn parse_transmitter_channel_status_page_2809(
    response: &[u8],
) -> Option<TransmitterChannelStatusPage2809> {
    let envelope = validate_response_envelope(
        response,
        &modern_arc_protocol_opcodes(OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809),
        &[RESULT_CODE_SUCCESS, crate::protocol::RESULT_CODE_MORE_PAGES],
    )?;
    let body = envelope.body;
    if body.len() < 8 {
        return None;
    }

    let page_capacity = *body.get(6)?;
    let reported_record_count = *body.get(7)?;
    if page_capacity == 0 || reported_record_count > page_capacity {
        return None;
    }

    let pointer_table_end = TRANSMITTER_CHANNEL_STATUS_POINTER_TABLE_OFFSET
        .checked_add(usize::from(reported_record_count).checked_mul(2)?)?;
    response.get(TRANSMITTER_CHANNEL_STATUS_POINTER_TABLE_OFFSET..pointer_table_end)?;

    let mut record_pointers = Vec::with_capacity(usize::from(reported_record_count));
    let mut seen_record_pointers = HashSet::with_capacity(usize::from(reported_record_count));
    for index in 0..reported_record_count {
        let pointer_offset = TRANSMITTER_CHANNEL_STATUS_POINTER_TABLE_OFFSET
            .checked_add(usize::from(index).checked_mul(2)?)?;
        let record_pointer = read_u16(response, pointer_offset)?;
        let record_offset = usize::from(record_pointer);
        let record_end = record_offset.checked_add(TRANSMITTER_CHANNEL_STATUS_RECORD_SIZE)?;
        if record_offset < pointer_table_end
            || record_end > response.len()
            || !seen_record_pointers.insert(record_pointer)
        {
            return None;
        }
        record_pointers.push(record_pointer);
    }

    let mut sorted_ranges: Vec<(usize, usize)> = record_pointers
        .iter()
        .map(|pointer| {
            let start = usize::from(*pointer);
            (start, start + TRANSMITTER_CHANNEL_STATUS_RECORD_SIZE)
        })
        .collect();
    sorted_ranges.sort_unstable();
    if sorted_ranges
        .windows(2)
        .any(|ranges| ranges[0].1 > ranges[1].0)
    {
        return None;
    }

    let mut records = Vec::with_capacity(usize::from(reported_record_count));
    let mut channel_numbers = HashSet::with_capacity(usize::from(reported_record_count));
    let mut media_identities = HashSet::with_capacity(usize::from(reported_record_count));
    for record_pointer in record_pointers {
        let record = parse_transmitter_channel_status_record_2809(
            response,
            record_pointer,
            pointer_table_end,
        )?;
        if !channel_numbers.insert(record.channel_number)
            || !media_identities.insert((record.media_type, record.media_local_channel_id))
        {
            return None;
        }
        records.push(record);
    }

    Some(TransmitterChannelStatusPage2809 {
        protocol_id: envelope.protocol_id,
        transaction_id: envelope.transaction_id,
        opcode: envelope.opcode,
        result_code: envelope.result_code,
        page_disposition: modern_arc_page_disposition(envelope.result_code)?,
        page_capacity,
        maximum_transmitter_channels: page_capacity,
        reported_record_count,
        records,
        raw_body_hexadecimal: bytes_to_hex(body),
    })
}

fn parse_transmitter_channel_status_record_2809(
    response: &[u8],
    record_pointer: u16,
    minimum_pointer: usize,
) -> Option<TransmitterChannelStatus2809> {
    let record_offset = usize::from(record_pointer);
    let record_end = record_offset.checked_add(TRANSMITTER_CHANNEL_STATUS_RECORD_SIZE)?;
    let record = response.get(record_offset..record_end)?;
    let channel_number = read_u16(
        response,
        record_offset.checked_add(TRANSMITTER_CHANNEL_STATUS_RECORD_CHANNEL_NUMBER)?,
    )?;
    if channel_number == 0 {
        return None;
    }
    let media_type = read_u16(
        response,
        record_offset.checked_add(TRANSMITTER_CHANNEL_STATUS_RECORD_MEDIA_TYPE)?,
    )?;
    let media_local_channel_id = read_u16(
        response,
        record_offset.checked_add(TRANSMITTER_CHANNEL_STATUS_RECORD_MEDIA_LOCAL_ID)?,
    )?;
    if media_type == 0 || media_local_channel_id == 0 {
        return None;
    }

    let channel_name_pointer = read_u16(
        response,
        record_offset.checked_add(TRANSMITTER_CHANNEL_STATUS_RECORD_NAME_POINTER)?,
    )?;
    let channel_name =
        required_status_string_at_pointer(response, channel_name_pointer, minimum_pointer)?;
    let format_pointer = read_u16(
        response,
        record_offset.checked_add(TRANSMITTER_CHANNEL_STATUS_RECORD_FORMAT_POINTER)?,
    )?;
    let format_offset = usize::from(format_pointer);
    if format_offset < minimum_pointer {
        return None;
    }
    let format_descriptor = response
        .get(format_offset..format_offset.checked_add(TRANSMITTER_CHANNEL_STATUS_FORMAT_SIZE)?)?;
    let sample_rate = read_u32(format_descriptor, 0)?;
    let encoding = read_u16(format_descriptor, 6)?;
    if sample_rate == 0 || encoding == 0 {
        return None;
    }

    let friendly_channel_name_pointer = read_u16(
        response,
        record_offset.checked_add(TRANSMITTER_CHANNEL_STATUS_RECORD_FRIENDLY_NAME_POINTER)?,
    )?;
    let friendly_channel_name = required_status_string_at_pointer(
        response,
        friendly_channel_name_pointer,
        minimum_pointer,
    )?;

    Some(TransmitterChannelStatus2809 {
        record_pointer,
        record_type_code: read_u16(record, 0)?,
        channel_number,
        media_type,
        media_local_channel_id,
        channel_name_pointer,
        channel_name,
        format_pointer,
        format_descriptor_hexadecimal: bytes_to_hex(format_descriptor),
        sample_rate,
        encoding,
        friendly_channel_name_pointer,
        friendly_channel_name,
        raw_record_hexadecimal: bytes_to_hex(record),
    })
}

pub fn parse_receiver_channel_status_page_2809(
    response: &[u8],
) -> Option<ReceiverChannelStatusPage2809> {
    let envelope = validate_response_envelope(
        response,
        &modern_arc_protocol_opcodes(OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809),
        &[RESULT_CODE_SUCCESS, crate::protocol::RESULT_CODE_MORE_PAGES],
    )?;
    let body = envelope.body;
    if body.len() < 8 {
        return None;
    }

    let page_capacity = *body.get(6)?;
    let reported_record_count = *body.get(7)?;
    if page_capacity == 0 || reported_record_count > page_capacity {
        return None;
    }

    let pointer_table_end = RECEIVER_CHANNEL_STATUS_POINTER_TABLE_OFFSET
        .checked_add(usize::from(reported_record_count).checked_mul(2)?)?;
    response.get(RECEIVER_CHANNEL_STATUS_POINTER_TABLE_OFFSET..pointer_table_end)?;

    let mut record_pointers = Vec::with_capacity(usize::from(reported_record_count));
    let mut seen_record_pointers = HashSet::with_capacity(usize::from(reported_record_count));
    for index in 0..reported_record_count {
        let pointer_offset = RECEIVER_CHANNEL_STATUS_POINTER_TABLE_OFFSET
            .checked_add(usize::from(index).checked_mul(2)?)?;
        let record_pointer = read_u16(response, pointer_offset)?;
        let record_offset = usize::from(record_pointer);
        let record_end = record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_SIZE)?;
        if record_offset < pointer_table_end
            || record_end > response.len()
            || !seen_record_pointers.insert(record_pointer)
        {
            return None;
        }
        record_pointers.push(record_pointer);
    }

    let mut sorted_ranges: Vec<(usize, usize)> = record_pointers
        .iter()
        .map(|pointer| {
            let start = usize::from(*pointer);
            (start, start + RECEIVER_CHANNEL_STATUS_RECORD_SIZE)
        })
        .collect();
    sorted_ranges.sort_unstable();
    if sorted_ranges
        .windows(2)
        .any(|ranges| ranges[0].1 > ranges[1].0)
    {
        return None;
    }

    let mut records = Vec::with_capacity(usize::from(reported_record_count));
    let mut channel_numbers = HashSet::with_capacity(usize::from(reported_record_count));
    let mut media_identities = HashSet::with_capacity(usize::from(reported_record_count));
    for record_pointer in record_pointers {
        let record =
            parse_receiver_channel_status_record_2809(response, record_pointer, pointer_table_end)?;
        if !channel_numbers.insert(record.channel_number)
            || !media_identities.insert((record.media_type, record.media_local_channel_id))
        {
            return None;
        }
        records.push(record);
    }

    Some(ReceiverChannelStatusPage2809 {
        protocol_id: envelope.protocol_id,
        transaction_id: envelope.transaction_id,
        opcode: envelope.opcode,
        result_code: envelope.result_code,
        page_disposition: modern_arc_page_disposition(envelope.result_code)?,
        page_capacity,
        maximum_receiver_channels: page_capacity,
        reported_record_count,
        records,
        raw_body_hexadecimal: bytes_to_hex(body),
    })
}

fn parse_receiver_channel_status_record_2809(
    response: &[u8],
    record_pointer: u16,
    minimum_pointer: usize,
) -> Option<ReceiverChannelStatus2809> {
    let record_offset = usize::from(record_pointer);
    let record_end = record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_SIZE)?;
    let record = response.get(record_offset..record_end)?;
    let channel_number = read_u16(
        response,
        record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_CHANNEL_NUMBER)?,
    )?;
    if channel_number == 0 {
        return None;
    }
    let media_type = read_u16(
        response,
        record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_MEDIA_TYPE)?,
    )?;
    let media_local_channel_id = read_u16(
        response,
        record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_MEDIA_LOCAL_ID)?,
    )?;
    if media_type == 0 || media_local_channel_id == 0 {
        return None;
    }

    let local_channel_name_pointer = read_u16(
        response,
        record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_LOCAL_NAME_POINTER)?,
    )?;
    let local_channel_name =
        required_status_string_at_pointer(response, local_channel_name_pointer, minimum_pointer)?;
    let format_pointer = read_u16(
        response,
        record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_FORMAT_POINTER)?,
    )?;
    let format_offset = usize::from(format_pointer);
    if format_offset < minimum_pointer {
        return None;
    }
    let format_descriptor = response
        .get(format_offset..format_offset.checked_add(RECEIVER_CHANNEL_STATUS_FORMAT_SIZE)?)?;
    let sample_rate = read_u32(format_descriptor, 0)?;
    let encoding = read_u16(format_descriptor, 6)?;
    if sample_rate == 0 || encoding == 0 {
        return None;
    }

    let friendly_channel_name_pointer = read_u16(
        response,
        record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_FRIENDLY_NAME_POINTER)?,
    )?;
    let friendly_channel_name = required_status_string_at_pointer(
        response,
        friendly_channel_name_pointer,
        minimum_pointer,
    )?;
    let source_channel_name_pointer = read_u16(
        response,
        record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_SOURCE_CHANNEL_POINTER)?,
    )?;
    let source_channel_name =
        optional_status_string_at_pointer(response, source_channel_name_pointer, minimum_pointer)?;
    let source_device_name_pointer = read_u16(
        response,
        record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_SOURCE_DEVICE_POINTER)?,
    )?;
    let source_device_name =
        optional_status_string_at_pointer(response, source_device_name_pointer, minimum_pointer)?;

    Some(ReceiverChannelStatus2809 {
        record_pointer,
        record_type_code: read_u16(record, 0)?,
        channel_number,
        media_type,
        media_local_channel_id,
        local_channel_name_pointer,
        local_channel_name,
        format_pointer,
        format_descriptor_hexadecimal: bytes_to_hex(format_descriptor),
        sample_rate,
        encoding,
        friendly_channel_name_pointer,
        friendly_channel_name,
        source_channel_name_pointer,
        source_channel_name,
        source_device_name_pointer,
        source_device_name,
        subscription_status_code: read_u16(
            response,
            record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_SUBSCRIPTION_STATUS)?,
        )?,
        receiver_status_code: read_u16(
            response,
            record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_RECEIVER_STATUS)?,
        )?,
        status_flags: read_u16(
            response,
            record_offset.checked_add(RECEIVER_CHANNEL_STATUS_RECORD_STATUS_FLAGS)?,
        )?,
        raw_record_hexadecimal: bytes_to_hex(record),
    })
}

fn required_status_string_at_pointer(
    data: &[u8],
    pointer: u16,
    minimum_pointer: usize,
) -> Option<String> {
    (usize::from(pointer) >= minimum_pointer).then(|| string_at_pointer(data, pointer))?
}

fn optional_status_string_at_pointer(
    data: &[u8],
    pointer: u16,
    minimum_pointer: usize,
) -> Option<Option<String>> {
    if pointer == 0 {
        return Some(None);
    }
    Some(Some(required_status_string_at_pointer(
        data,
        pointer,
        minimum_pointer,
    )?))
}

pub fn parse_receiver_flow_status_page_2809(response: &[u8]) -> Option<ReceiverFlowStatusPage2809> {
    let envelope = validate_response_envelope(
        response,
        &[(PROTOCOL_ARC_2809, OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809)],
        &[RESULT_CODE_SUCCESS],
    )?;
    let body = envelope.body;
    if body.len() < 8 {
        return None;
    }

    let maximum_flow_slots = *body.get(6)?;
    let reported_flow_count = *body.get(7)?;
    if !(1..=32).contains(&maximum_flow_slots) || reported_flow_count > maximum_flow_slots {
        return None;
    }

    let pointer_table_end = RECEIVER_FLOW_STATUS_POINTER_TABLE_OFFSET
        .checked_add(usize::from(reported_flow_count).checked_mul(2)?)?;
    response.get(RECEIVER_FLOW_STATUS_POINTER_TABLE_OFFSET..pointer_table_end)?;

    let mut record_pointers = Vec::with_capacity(usize::from(reported_flow_count));
    let mut seen_record_pointers = HashSet::with_capacity(usize::from(reported_flow_count));
    for index in 0..reported_flow_count {
        let pointer_offset = RECEIVER_FLOW_STATUS_POINTER_TABLE_OFFSET
            .checked_add(usize::from(index).checked_mul(2)?)?;
        let record_pointer = read_u16(response, pointer_offset)?;
        let record_offset = usize::from(record_pointer);
        let record_end = record_offset.checked_add(RECEIVER_FLOW_STATUS_RECORD_SIZE)?;
        if record_offset < pointer_table_end
            || record_end > response.len()
            || !seen_record_pointers.insert(record_pointer)
        {
            return None;
        }
        record_pointers.push(record_pointer);
    }

    let mut sorted_ranges: Vec<(usize, usize)> = record_pointers
        .iter()
        .map(|pointer| {
            let start = usize::from(*pointer);
            (start, start + RECEIVER_FLOW_STATUS_RECORD_SIZE)
        })
        .collect();
    sorted_ranges.sort_unstable();
    if sorted_ranges
        .windows(2)
        .any(|ranges| ranges[0].1 > ranges[1].0)
    {
        return None;
    }

    let mut flows = Vec::with_capacity(usize::from(reported_flow_count));
    let mut flow_numbers = HashSet::with_capacity(usize::from(reported_flow_count));
    for record_pointer in record_pointers {
        let flow =
            parse_receiver_flow_status_record_2809(response, record_pointer, pointer_table_end)?;
        if flow.flow_number > u16::from(maximum_flow_slots)
            || !flow_numbers.insert(flow.flow_number)
        {
            return None;
        }
        flows.push(flow);
    }

    Some(ReceiverFlowStatusPage2809 {
        maximum_flow_slots,
        reported_flow_count,
        flows,
        raw_body_hexadecimal: bytes_to_hex(body),
    })
}

fn parse_receiver_flow_status_record_2809(
    response: &[u8],
    record_pointer: u16,
    minimum_pointer: usize,
) -> Option<ReceiverFlowStatus2809> {
    let record_offset = usize::from(record_pointer);
    let record_end = record_offset.checked_add(RECEIVER_FLOW_STATUS_RECORD_SIZE)?;
    let record = response.get(record_offset..record_end)?;
    let flow_number = read_u16(record, RECEIVER_FLOW_STATUS_RECORD_FLOW_NUMBER)?;
    let channel_count = read_u16(record, RECEIVER_FLOW_STATUS_RECORD_CHANNEL_COUNT)?;
    let local_receiver_channel_count =
        read_u16(record, RECEIVER_FLOW_STATUS_RECORD_LOCAL_RECEIVER_COUNT)?;
    if flow_number == 0 || channel_count == 0 || local_receiver_channel_count == 0 {
        return None;
    }

    let flow_name_pointer = read_u16(record, RECEIVER_FLOW_STATUS_RECORD_NAME_POINTER)?;
    let flow_name =
        required_status_string_at_pointer(response, flow_name_pointer, minimum_pointer)?;
    let format_pointer = read_u16(record, RECEIVER_FLOW_STATUS_RECORD_FORMAT_POINTER)?;
    let format_offset = usize::from(format_pointer);
    if format_offset < minimum_pointer {
        return None;
    }
    let format_descriptor = response
        .get(format_offset..format_offset.checked_add(RECEIVER_FLOW_STATUS_FORMAT_SIZE)?)?;
    let sample_rate = read_u32(format_descriptor, 0)?;
    let encoding = read_u32(format_descriptor, 4)?;
    if sample_rate == 0 || encoding == 0 {
        return None;
    }

    let receiver_mapping_descriptor_pointer =
        read_u16(record, RECEIVER_FLOW_STATUS_RECORD_MAPPING_POINTER)?;
    let receiver_mapping_descriptor_offset = usize::from(receiver_mapping_descriptor_pointer);
    if receiver_mapping_descriptor_offset < minimum_pointer {
        return None;
    }
    let receiver_mapping_descriptor = response.get(
        receiver_mapping_descriptor_offset
            ..receiver_mapping_descriptor_offset.checked_add(RECEIVER_FLOW_STATUS_MAPPING_SIZE)?,
    )?;

    let endpoint_descriptor = record.get(
        RECEIVER_FLOW_STATUS_RECORD_ENDPOINT
            ..RECEIVER_FLOW_STATUS_RECORD_ENDPOINT
                .checked_add(RECEIVER_FLOW_STATUS_ENDPOINT_SIZE)?,
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

    Some(ReceiverFlowStatus2809 {
        record_pointer,
        record_type_code: read_u16(record, 0)?,
        flow_number,
        channel_count,
        flow_type_code: read_u16(record, RECEIVER_FLOW_STATUS_RECORD_FLOW_TYPE)?,
        flow_name_pointer,
        flow_name,
        format_pointer,
        sample_rate,
        encoding,
        latency_nanoseconds: read_u32(record, RECEIVER_FLOW_STATUS_RECORD_LATENCY)?,
        local_receiver_channel_count,
        receiver_mapping_descriptor_pointer,
        receiver_mapping_descriptor_hexadecimal: bytes_to_hex(receiver_mapping_descriptor),
        status_flags_at_record_offset_60: read_u16(
            record,
            RECEIVER_FLOW_STATUS_RECORD_STATUS_FLAGS,
        )?,
        status_code_at_record_offset_62: read_u16(record, RECEIVER_FLOW_STATUS_RECORD_STATUS_CODE)?,
        endpoint_descriptor_hexadecimal: bytes_to_hex(endpoint_descriptor),
        destination_user_datagram_port,
        destination_internet_protocol_version_four_address: destination_address,
        raw_record_hexadecimal: bytes_to_hex(record),
    })
}
