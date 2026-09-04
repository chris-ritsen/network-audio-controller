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

fn modern_arc_page_counts(body: &[u8]) -> Option<(u8, u8)> {
    if body.len() < 8 {
        return None;
    }
    let page_capacity = *body.get(6)?;
    let reported_record_count = *body.get(7)?;
    if page_capacity == 0 || reported_record_count > page_capacity {
        return None;
    }
    Some((page_capacity, reported_record_count))
}

pub fn parse_modern_arc_transmitter_channel_status_page(
    response: &[u8],
) -> Option<ModernArcTransmitterChannelStatusPage> {
    let envelope = validate_response_envelope(
        response,
        &modern_arc_protocol_opcodes(OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809),
        &[RESULT_CODE_SUCCESS, crate::protocol::RESULT_CODE_MORE_PAGES],
    )?;
    let body = envelope.body;
    let (page_capacity, reported_record_count) = modern_arc_page_counts(body)?;
    let records = parse_pointer_table_page(
        response,
        MODERN_ARC_POINTER_TABLE_OFFSET,
        reported_record_count,
        |record: &ModernArcTransmitterChannelStatus| usize::from(record.record_length_bytes),
        parse_modern_arc_transmitter_channel_status_record,
    )?;
    let mut channel_numbers = HashSet::with_capacity(records.len());
    let mut media_identities = HashSet::with_capacity(records.len());
    for record in &records {
        if !channel_numbers.insert(record.channel_number)
            || !media_identities.insert((record.media_type_code, record.media_local_channel_id))
        {
            return None;
        }
    }

    Some(ModernArcTransmitterChannelStatusPage {
        protocol_id: envelope.protocol_id,
        transaction_id: envelope.transaction_id,
        opcode: envelope.opcode,
        result_code: envelope.result_code,
        page_disposition: modern_arc_page_disposition(envelope.result_code)?,
        page_capacity,
        reported_record_count,
        records,
        raw_body_hexadecimal: bytes_to_hex(body),
    })
}

struct ModernArcChannelStatusRecordPrefix<'a> {
    channel_number: u16,
    encoding: Option<u16>,
    format_descriptor: &'a [u8],
    format_pointer: u16,
    friendly_channel_name: String,
    friendly_channel_name_pointer: u16,
    media_local_channel_id: u16,
    media_type: &'static str,
    media_type_code: u16,
    name: String,
    name_pointer: u16,
    record: &'a [u8],
    record_type_code: u16,
    sample_rate: Option<u32>,
}

fn parse_modern_arc_channel_status_record_prefix(
    response: &[u8],
    record_pointer: u16,
    record_size: usize,
    minimum_pointer: usize,
) -> Option<ModernArcChannelStatusRecordPrefix<'_>> {
    let record_offset = usize::from(record_pointer);
    let record_end = record_offset.checked_add(record_size)?;
    let record = response.get(record_offset..record_end)?;
    let channel_number = read_u16(record, CHANNEL_STATUS_RECORD_CHANNEL_NUMBER)?;
    if channel_number == 0 {
        return None;
    }
    let media_type_code = read_u16(record, CHANNEL_STATUS_RECORD_MEDIA_TYPE)?;
    let media_local_channel_id = read_u16(record, CHANNEL_STATUS_RECORD_MEDIA_LOCAL_ID)?;
    let media_type = match media_type_code {
        MEDIA_TYPE_AUDIO => "audio",
        MEDIA_TYPE_VIDEO => "video",
        MEDIA_TYPE_ANCILLARY => "ancillary",
        _ => return None,
    };
    if media_local_channel_id == 0 {
        return None;
    }

    let name_pointer = read_u16(record, CHANNEL_STATUS_RECORD_NAME_POINTER)?;
    let name = required_status_string_at_pointer(response, name_pointer, minimum_pointer)?;
    let format_pointer = read_u16(record, CHANNEL_STATUS_RECORD_FORMAT_POINTER)?;
    let format_offset = usize::from(format_pointer);
    if format_offset < minimum_pointer {
        return None;
    }
    let format_descriptor =
        response.get(format_offset..format_offset.checked_add(CHANNEL_STATUS_FORMAT_SIZE)?)?;
    let (sample_rate, encoding) = if media_type_code == MEDIA_TYPE_AUDIO {
        let sample_rate = read_u32(format_descriptor, 0)?;
        let encoding = read_u16(format_descriptor, 6)?;
        if sample_rate == 0 || encoding == 0 {
            return None;
        }
        (Some(sample_rate), Some(encoding))
    } else {
        (None, None)
    };

    let friendly_channel_name_pointer =
        read_u16(record, CHANNEL_STATUS_RECORD_FRIENDLY_NAME_POINTER)?;
    let friendly_channel_name = required_status_string_at_pointer(
        response,
        friendly_channel_name_pointer,
        minimum_pointer,
    )?;

    Some(ModernArcChannelStatusRecordPrefix {
        channel_number,
        encoding,
        format_descriptor,
        format_pointer,
        friendly_channel_name,
        friendly_channel_name_pointer,
        media_local_channel_id,
        media_type,
        media_type_code,
        name,
        name_pointer,
        record,
        record_type_code: read_u16(record, 0)?,
        sample_rate,
    })
}

fn parse_modern_arc_transmitter_channel_status_record(
    response: &[u8],
    record_pointer: u16,
    minimum_pointer: usize,
) -> Option<ModernArcTransmitterChannelStatus> {
    let record_type_code = read_u16(response, usize::from(record_pointer))?;
    let record_size = match record_type_code {
        0x1414 => 40,
        0x1616 => 44,
        _ => return None,
    };
    let prefix = parse_modern_arc_channel_status_record_prefix(
        response,
        record_pointer,
        record_size,
        minimum_pointer,
    )?;

    Some(ModernArcTransmitterChannelStatus {
        record_pointer,
        record_length_bytes: u16::try_from(record_size).ok()?,
        record_type_code: prefix.record_type_code,
        channel_number: prefix.channel_number,
        media_type_code: prefix.media_type_code,
        media_type: prefix.media_type.to_owned(),
        media_local_channel_id: prefix.media_local_channel_id,
        channel_name_pointer: prefix.name_pointer,
        channel_name: prefix.name,
        format_pointer: prefix.format_pointer,
        format_descriptor_hexadecimal: bytes_to_hex(prefix.format_descriptor),
        sample_rate: prefix.sample_rate,
        encoding: prefix.encoding,
        friendly_channel_name_pointer: prefix.friendly_channel_name_pointer,
        friendly_channel_name: prefix.friendly_channel_name,
        raw_record_hexadecimal: bytes_to_hex(prefix.record),
    })
}

pub fn parse_modern_arc_receiver_channel_status_page(
    response: &[u8],
) -> Option<ModernArcReceiverChannelStatusPage> {
    let envelope = validate_response_envelope(
        response,
        &modern_arc_protocol_opcodes(OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809),
        &[RESULT_CODE_SUCCESS, crate::protocol::RESULT_CODE_MORE_PAGES],
    )?;
    let body = envelope.body;
    let (page_capacity, reported_record_count) = modern_arc_page_counts(body)?;
    let records = parse_pointer_table_page(
        response,
        MODERN_ARC_POINTER_TABLE_OFFSET,
        reported_record_count,
        |record: &ModernArcReceiverChannelStatus| usize::from(record.record_length_bytes),
        parse_modern_arc_receiver_channel_status_record,
    )?;
    let mut channel_numbers = HashSet::with_capacity(records.len());
    let mut media_identities = HashSet::with_capacity(records.len());
    for record in &records {
        if !channel_numbers.insert(record.channel_number)
            || !media_identities.insert((record.media_type_code, record.media_local_channel_id))
        {
            return None;
        }
    }

    Some(ModernArcReceiverChannelStatusPage {
        protocol_id: envelope.protocol_id,
        transaction_id: envelope.transaction_id,
        opcode: envelope.opcode,
        result_code: envelope.result_code,
        page_disposition: modern_arc_page_disposition(envelope.result_code)?,
        page_capacity,
        reported_record_count,
        records,
        raw_body_hexadecimal: bytes_to_hex(body),
    })
}

fn parse_modern_arc_receiver_channel_status_record(
    response: &[u8],
    record_pointer: u16,
    minimum_pointer: usize,
) -> Option<ModernArcReceiverChannelStatus> {
    let record_type_code = read_u16(response, usize::from(record_pointer))?;
    let (
        record_size,
        source_channel_offset,
        source_device_offset,
        subscription_status_offset,
        receiver_status_offset,
        status_flags_offset,
    ) = match record_type_code {
        0x141C => (56, 44, 46, 48, 50, Some(52)),
        0x161C => (56, 48, 50, 52, 54, None),
        0x161E => (60, 48, 50, 52, 54, Some(56)),
        _ => return None,
    };
    let prefix = parse_modern_arc_channel_status_record_prefix(
        response,
        record_pointer,
        record_size,
        minimum_pointer,
    )?;
    let record = prefix.record;
    let source_channel_name_pointer = read_u16(record, source_channel_offset)?;
    let source_channel_name =
        optional_status_string_at_pointer(response, source_channel_name_pointer, minimum_pointer)?;
    let source_device_name_pointer = read_u16(record, source_device_offset)?;
    let source_device_name =
        optional_status_string_at_pointer(response, source_device_name_pointer, minimum_pointer)?;

    Some(ModernArcReceiverChannelStatus {
        record_pointer,
        record_length_bytes: u16::try_from(record_size).ok()?,
        record_type_code: prefix.record_type_code,
        channel_number: prefix.channel_number,
        media_type_code: prefix.media_type_code,
        media_type: prefix.media_type.to_owned(),
        media_local_channel_id: prefix.media_local_channel_id,
        local_channel_name_pointer: prefix.name_pointer,
        local_channel_name: prefix.name,
        format_pointer: prefix.format_pointer,
        format_descriptor_hexadecimal: bytes_to_hex(prefix.format_descriptor),
        sample_rate: prefix.sample_rate,
        encoding: prefix.encoding,
        friendly_channel_name_pointer: prefix.friendly_channel_name_pointer,
        friendly_channel_name: prefix.friendly_channel_name,
        source_channel_name_pointer,
        source_channel_name,
        source_device_name_pointer,
        source_device_name,
        subscription_status_code: read_u16(record, subscription_status_offset)?,
        receiver_status_code: read_u16(record, receiver_status_offset)?,
        status_flags: match status_flags_offset {
            Some(offset) => Some(read_u16(record, offset)?),
            None => None,
        },
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

pub fn parse_modern_arc_receiver_flow_status_page(
    response: &[u8],
) -> Option<ModernArcReceiverFlowStatusPage> {
    let envelope = validate_response_envelope(
        response,
        &modern_arc_protocol_opcodes(OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809),
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

    let protocol_id = envelope.protocol_id;
    let flows = parse_pointer_table_page(
        response,
        MODERN_ARC_POINTER_TABLE_OFFSET,
        reported_flow_count,
        |flow: &ModernArcReceiverFlowStatus| usize::from(flow.record_length_bytes),
        |response, record_pointer, minimum_pointer| {
            parse_receiver_flow_status_record_2809(
                response,
                record_pointer,
                minimum_pointer,
                protocol_id,
            )
        },
    )?;
    let mut flow_numbers = HashSet::with_capacity(flows.len());
    for flow in &flows {
        if flow.global_flow_id > u16::from(maximum_flow_slots)
            || !flow_numbers.insert(flow.global_flow_id)
        {
            return None;
        }
    }

    Some(ModernArcReceiverFlowStatusPage {
        protocol_id: envelope.protocol_id,
        transaction_id: envelope.transaction_id,
        opcode: envelope.opcode,
        result_code: envelope.result_code,
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
    protocol_id: u16,
) -> Option<ModernArcReceiverFlowStatus> {
    let (
        record_size,
        local_receiver_count_offset,
        mapping_pointer_offset,
        status_flags_offset,
        status_code_offset,
        endpoint_offset,
    ) = match protocol_id {
        PROTOCOL_ARC_2809 => (84usize, 52usize, 54usize, 60usize, 62usize, 68usize),
        PROTOCOL_ARC_280F => (92usize, 56usize, 58usize, 68usize, 70usize, 76usize),
        _ => return None,
    };
    let record_offset = usize::from(record_pointer);
    let record_end = record_offset.checked_add(record_size)?;
    let record = response.get(record_offset..record_end)?;
    let expected_record_type = match protocol_id {
        PROTOCOL_ARC_2809 => 0x1422,
        PROTOCOL_ARC_280F => 0x1626,
        _ => return None,
    };
    let record_type_code = read_u16(record, 0)?;
    if record_type_code != expected_record_type {
        return None;
    }
    let global_flow_id = read_u16(record, RECEIVER_FLOW_STATUS_RECORD_FLOW_NUMBER)?;
    let media_type_code = read_u16(record, RECEIVER_FLOW_STATUS_RECORD_MEDIA_TYPE)?;
    let media_local_flow_id = read_u16(record, RECEIVER_FLOW_STATUS_RECORD_MEDIA_LOCAL_ID)?;
    let local_receiver_channel_count = read_u16(record, local_receiver_count_offset)?;
    if global_flow_id == 0
        || !matches!(media_type_code, MEDIA_TYPE_AUDIO | MEDIA_TYPE_VIDEO)
        || media_local_flow_id == 0
        || local_receiver_channel_count == 0
    {
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
    let format_size = match media_type_code {
        MEDIA_TYPE_AUDIO => 8,
        MEDIA_TYPE_VIDEO => 16,
        _ => return None,
    };
    let format_descriptor = response.get(format_offset..format_offset.checked_add(format_size)?)?;
    if format_offset.checked_add(format_size)? > record_offset {
        return None;
    }
    let (sample_rate, encoding, latency_nanoseconds) = if media_type_code == MEDIA_TYPE_AUDIO {
        let sample_rate = read_u32(format_descriptor, 0)?;
        let encoding = read_u32(format_descriptor, 4)?;
        if sample_rate == 0 || encoding == 0 {
            return None;
        }
        (
            Some(sample_rate),
            Some(encoding),
            Some(read_u32(record, RECEIVER_FLOW_STATUS_RECORD_LATENCY)?),
        )
    } else {
        (None, None, None)
    };

    let receiver_mapping_descriptor_pointer = read_u16(record, mapping_pointer_offset)?;
    let receiver_mapping_descriptor_offset = usize::from(receiver_mapping_descriptor_pointer);
    if receiver_mapping_descriptor_offset < minimum_pointer {
        return None;
    }
    let receiver_mapping_descriptor = response.get(
        receiver_mapping_descriptor_offset
            ..receiver_mapping_descriptor_offset.checked_add(RECEIVER_FLOW_STATUS_MAPPING_SIZE)?,
    )?;

    let endpoint_descriptor = record
        .get(endpoint_offset..endpoint_offset.checked_add(RECEIVER_FLOW_STATUS_ENDPOINT_SIZE)?)?;
    let (destination_user_datagram_port, destination_address) =
        if endpoint_descriptor.get(..2)? == [0x08, 0x02] {
            (
                read_u16(endpoint_descriptor, 2),
                ipv4_at(endpoint_descriptor, 4),
            )
        } else {
            (None, None)
        };

    Some(ModernArcReceiverFlowStatus {
        record_pointer,
        record_length_bytes: u16::try_from(record_size).ok()?,
        record_type_code,
        global_flow_id,
        media_type_code,
        media_local_flow_id,
        flow_type_code: read_u16(record, RECEIVER_FLOW_STATUS_RECORD_FLOW_TYPE)?,
        flow_name_pointer,
        flow_name,
        format_pointer,
        format_descriptor_hexadecimal: bytes_to_hex(format_descriptor),
        sample_rate,
        encoding,
        latency_nanoseconds,
        local_receiver_channel_count,
        receiver_mapping_descriptor_pointer,
        receiver_mapping_descriptor_hexadecimal: bytes_to_hex(receiver_mapping_descriptor),
        status_flags: read_u16(record, status_flags_offset)?,
        status_code: read_u16(record, status_code_offset)?,
        endpoint_descriptor_hexadecimal: bytes_to_hex(endpoint_descriptor),
        destination_user_datagram_port,
        destination_internet_protocol_version_four_address: destination_address,
        raw_record_hexadecimal: bytes_to_hex(record),
    })
}
