use super::*;

impl BluetoothStatus {
    fn disconnected() -> BluetoothStatus {
        BluetoothStatus {
            connected: false,
            device_name: None,
        }
    }
}

pub fn parse_metering_frame(data: &[u8]) -> Option<MeteringFrame> {
    if data.len() < METERING_V2_HEADER_SIZE
        || read_u16(data, 0)? != 0xFFFF
        || usize::from(read_u16(data, 2)?) != data.len()
        || read_u16(data, 6)? != 0
        || data.get(16..24)? != b"Audinate"
    {
        return None;
    }

    let (tx_count, rx_count, levels_offset) = match data.get(METERING_FAMILY_OFFSET).copied()? {
        0x02 => {
            if data.get(METERING_V2_SUFFIX_OFFSET).copied()? != 0xFE {
                return None;
            }
            (
                u16::from(data.get(METERING_V2_TX_COUNT_OFFSET).copied()?),
                u16::from(data.get(METERING_V2_RX_COUNT_OFFSET).copied()?),
                METERING_V2_LEVELS_OFFSET,
            )
        }
        0x03 => {
            if data.len() < METERING_V3_HEADER_SIZE
                || data.get(METERING_V3_RESERVED_OFFSET).copied()? != 0
            {
                return None;
            }
            (
                read_u16(data, METERING_V3_TX_COUNT_OFFSET)?,
                read_u16(data, METERING_V3_RX_COUNT_OFFSET)?,
                METERING_V3_LEVELS_OFFSET,
            )
        }
        _ => return None,
    };
    let tx_levels_end = levels_offset.checked_add(usize::from(tx_count))?;
    let rx_levels_end = tx_levels_end.checked_add(usize::from(rx_count))?;
    if rx_levels_end != data.len() {
        return None;
    }

    let mut source_eui64 = String::with_capacity(16);
    for value in data.get(8..16)? {
        write!(source_eui64, "{value:02x}").ok()?;
    }

    Some(MeteringFrame {
        sequence: read_u16(data, 4)?,
        source_eui64,
        tx_count,
        rx_count,
        tx_levels: data.get(levels_offset..tx_levels_end)?.to_vec(),
        rx_levels: data.get(tx_levels_end..rx_levels_end)?.to_vec(),
    })
}

pub fn parse_device_name(response: &[u8]) -> Option<String> {
    let body = validate_response_envelope(
        response,
        &common_arc_protocol_opcodes(OPCODE_DEVICE_NAME),
        &[RESULT_CODE_SUCCESS],
    )?
    .body;
    let (&terminator, name) = body.split_last()?;
    if terminator != 0 || name.contains(&0) {
        return None;
    }
    std::str::from_utf8(name).ok().map(str::to_owned)
}

pub fn parse_device_info(response: &[u8]) -> Option<DeviceInfo> {
    let body = validate_response_envelope(
        response,
        &common_arc_protocol_opcodes(OPCODE_DEVICE_INFO),
        &[RESULT_CODE_SUCCESS],
    )?
    .body;
    if body.len() < 18 {
        return None;
    }

    let minimum_string_pointer = RESPONSE_HEADER_SIZE + 18;
    let required_string = |pointer: u16| -> Option<String> {
        (usize::from(pointer) >= minimum_string_pointer)
            .then(|| string_at_pointer(response, pointer))?
    };

    let code_pointer = read_u16(body, 6)?;
    let port_pointer = read_u16(body, 8)?;
    let model_pointer = read_u16(body, 12)?;
    let display_pointer = read_u16(body, 14)?;

    let (model_name, display_name) = if (model_pointer == 0 && display_pointer == 0)
        || usize::from(model_pointer) < minimum_string_pointer
    {
        (String::new(), String::new())
    } else {
        (
            required_string(model_pointer)?,
            required_string(display_pointer)?,
        )
    };

    Some(DeviceInfo {
        model_name,
        display_name,
        model_code: required_string(code_pointer)?,
        port: required_string(port_pointer)?,
    })
}

pub fn parse_device_settings(response: &[u8]) -> Option<DeviceSettings> {
    let body = validate_response_envelope(
        response,
        &device_settings_arc_protocol_opcodes(OPCODE_DEVICE_SETTINGS),
        &[RESULT_CODE_SUCCESS],
    )?
    .body;

    let record_count = usize::from(*body.get(1)?);
    let record_bytes = record_count.checked_mul(4)?;
    let values_offset = 2usize.checked_add(record_bytes)?;
    body.get(..values_offset)?;
    let mut settings = DeviceSettings {
        sample_rate: None,
        latency_ns: None,
        configured_latency_ns: None,
        active_latency_ns: None,
        default_latency_ns: None,
        min_latency_ns: None,
        max_latency_ns: None,
        aes67_multicast_prefix: None,
        inline_values: Vec::new(),
        referenced_values: Vec::new(),
        unavailable_property_ids: Vec::new(),
    };
    let mut info_codes = HashSet::with_capacity(record_count);
    let mut records = Vec::with_capacity(record_count);

    for index in 0..record_count {
        let offset = 2 + index * 4;
        let info_code = u16_at(body, offset);
        let value_pointer = u16_at(body, offset + 2);
        if info_code == 0 {
            records.push((info_code, value_pointer));
            continue;
        }
        if !info_codes.insert(info_code) {
            return None;
        }
        records.push((info_code, value_pointer));
    }

    let minimum_value_pointer = RESPONSE_HEADER_SIZE.checked_add(values_offset)?;
    let mut distinct_value_pointers = records
        .iter()
        .filter_map(|(info_code, value_pointer)| {
            (info_code & 0x8000 != 0).then_some(usize::from(*value_pointer))
        })
        .collect::<Vec<_>>();
    distinct_value_pointers.sort_unstable();
    distinct_value_pointers.dedup();
    if distinct_value_pointers.iter().any(|value_pointer| {
        *value_pointer < minimum_value_pointer
            || value_pointer
                .checked_add(4)
                .is_none_or(|value_end| value_end > response.len())
    }) {
        return None;
    }

    for (info_code, value_pointer) in records {
        if info_code == 0 {
            settings.unavailable_property_ids.push(value_pointer);
            continue;
        }
        if info_code & 0x8000 == 0 {
            settings.inline_values.push(DeviceSettingsInlineValue {
                info_code,
                value: value_pointer,
            });
            continue;
        }
        let value_pointer_offset = usize::from(value_pointer);
        let value_end = distinct_value_pointers
            .iter()
            .copied()
            .find(|candidate| *candidate > value_pointer_offset)
            .unwrap_or(response.len());
        let value_bytes = response.get(value_pointer_offset..value_end)?;
        if value_bytes.len() < 4 {
            return None;
        }
        let value = read_u32(value_bytes, 0)?;

        settings
            .referenced_values
            .push(DeviceSettingsReferencedValue {
                info_code,
                pointer: value_pointer,
                value_hexadecimal: bytes_to_hex(value_bytes),
            });
        match info_code {
            DEVICE_SETTINGS_INFO_SAMPLE_RATE => settings.sample_rate = Some(value),
            DEVICE_SETTINGS_INFO_DEFAULT_LATENCY_NS => settings.default_latency_ns = Some(value),
            DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS => {
                settings.configured_latency_ns = Some(value)
            }
            DEVICE_SETTINGS_INFO_ACTIVE_LATENCY_NS => settings.active_latency_ns = Some(value),
            DEVICE_SETTINGS_INFO_MIN_LATENCY_NS => settings.min_latency_ns = Some(value),
            DEVICE_SETTINGS_INFO_MAX_LATENCY_NS => settings.max_latency_ns = Some(value),
            DEVICE_SETTINGS_INFO_AES67_MULTICAST_PREFIX => {
                settings.aes67_multicast_prefix =
                    Some(std::net::Ipv4Addr::from(value.to_be_bytes()).to_string())
            }
            _ => {}
        }
    }

    settings.latency_ns = settings
        .active_latency_ns
        .or(settings.configured_latency_ns);

    Some(settings)
}

pub fn parse_property_directory(response: &[u8]) -> Option<PropertyDirectory> {
    let body = validate_response_envelope(
        response,
        &device_settings_arc_protocol_opcodes(OPCODE_PROPERTY_DIRECTORY),
        &[RESULT_CODE_SUCCESS],
    )?
    .body;
    let property_count = usize::from(read_u16(body, 0)?);
    let expected_length = 2usize.checked_add(property_count.checked_mul(4)?)?;
    if body.len() != expected_length {
        return None;
    }

    let mut properties = Vec::with_capacity(property_count);
    let mut property_ids = HashSet::with_capacity(property_count);
    for index in 0..property_count {
        let offset = 2 + index * 4;
        let property_id = read_u16(body, offset)?;
        let flags = read_u16(body, offset + 2)?;
        if !property_ids.insert(property_id) {
            return None;
        }
        properties.push(PropertyDirectoryEntry { property_id, flags });
    }

    Some(PropertyDirectory {
        aes67_supported: property_ids.contains(&DEVICE_SETTINGS_INFO_AES67_CONFIGURED),
        properties,
    })
}

pub fn parse_aes67_configured(response: &[u8]) -> Option<Option<bool>> {
    let body = validate_response_envelope(
        response,
        &device_settings_arc_protocol_opcodes(OPCODE_DEVICE_SETTINGS),
        &[RESULT_CODE_SUCCESS],
    )?
    .body;
    let record_count = usize::from(*body.get(1)?);
    let record_bytes = record_count.checked_mul(4)?;
    let records_end = 2usize.checked_add(record_bytes)?;
    body.get(..records_end)?;
    let mut configured = None;
    for record_index in 0..record_count {
        let record_offset = 2 + record_index * 4;
        let info_code = u16_at(body, record_offset);
        let inline_value = u16_at(body, record_offset + 2);
        if info_code == 0 {
            continue;
        }
        if info_code != DEVICE_SETTINGS_INFO_AES67_CONFIGURED {
            continue;
        }
        if configured.is_some() {
            return None;
        }
        configured = match inline_value {
            0x0003 => Some(true),
            0x0001 => Some(false),
            _ => return None,
        };
    }
    Some(configured)
}

fn conmon_string_bytes(raw: &[u8]) -> Option<String> {
    let null_position = raw.iter().position(|&byte| byte == 0).unwrap_or(raw.len());
    let raw = &raw[..null_position];
    let text = std::str::from_utf8(raw).ok()?.trim().to_owned();
    if text
        .chars()
        .all(|character| !character.is_control() || character == ' ')
    {
        Some(text)
    } else {
        None
    }
}

fn conmon_string(data: &[u8], start: usize, end: usize) -> Option<String> {
    conmon_string_bytes(data.get(start..end)?)
}

pub fn parse_make_model(data: &[u8]) -> Option<MakeModel> {
    validate_conmon_envelope(data, CONMON_OPCODE_MAKE_MODEL_RESPONSE)?;
    let manufacturer_field = data.get(CONMON_MANUFACTURER_OFFSET..CONMON_MANUFACTURER_END)?;
    let manufacturer = conmon_string_bytes(manufacturer_field)?;
    let manufacturer_field_hexadecimal = bytes_to_hex(manufacturer_field);
    let unmapped_field_at_byte_offset_74 =
        read_u16(data, CONMON_UNMAPPED_FIELD_BEFORE_MANUFACTURER_OFFSET)?;
    let product_name = conmon_string(data, CONMON_PRODUCT_NAME_OFFSET, CONMON_PRODUCT_NAME_END)?;
    let version = data.get(CONMON_PRODUCT_VERSION_OFFSET..CONMON_PRODUCT_VERSION_END)?;
    let product_version_components: [u8; 4] = version.try_into().ok()?;
    let mut product_version = String::new();
    if version.iter().any(|&byte| byte != 0) {
        product_version = format!(
            "{}.{}.{}.{}",
            version[0], version[1], version[2], version[3]
        );
    }
    Some(MakeModel {
        manufacturer,
        manufacturer_field_hexadecimal,
        unmapped_field_at_byte_offset_74,
        product_name,
        product_version,
        product_version_components,
    })
}

pub fn parse_dante_model(data: &[u8]) -> Option<DanteModel> {
    validate_conmon_envelope(data, CONMON_OPCODE_DANTE_MODEL_RESPONSE)?;
    Some(DanteModel {
        board_codename: conmon_string(
            data,
            CONMON_BOARD_CODENAME_OFFSET,
            CONMON_BOARD_CODENAME_END,
        )?,
        board_name: conmon_string(data, CONMON_BOARD_NAME_OFFSET, CONMON_BOARD_NAME_END)?,
    })
}

pub fn parse_result_code(response: &[u8]) -> Option<u16> {
    let envelope = response_envelope(response)?;
    let common_opcode = matches!(
        envelope.opcode,
        OPCODE_CHANNEL_COUNT
            | OPCODE_DEVICE_NAME_SET
            | OPCODE_DEVICE_NAME
            | OPCODE_DEVICE_INFO
            | OPCODE_DEVICE_SETTINGS
            | OPCODE_DEVICE_SETTINGS_SET
            | OPCODE_PROPERTY_DIRECTORY
            | OPCODE_TX_CHANNEL_INFO
            | OPCODE_TX_CHANNEL_NAMES
            | OPCODE_TX_CHANNEL_NAME_SET
            | OPCODE_RX_CHANNELS
            | OPCODE_RX_CHANNEL_NAME_SET
            | OPCODE_SUBSCRIPTION_ADD
            | OPCODE_SUBSCRIPTION_REMOVE
    );
    let flow_opcode = match envelope.protocol_id {
        PROTOCOL_DANTE_FLOW | PROTOCOL_DANTE_FLOW_2801 => matches!(
            envelope.opcode,
            OPCODE_QUERY_TX_FLOWS
                | OPCODE_QUERY_TRANSMIT_CHANNEL_CAPABILITIES
                | OPCODE_QUERY_RECEIVER_FLOWS
                | OPCODE_QUERY_RECEIVER_PORT_RANGES
                | OPCODE_CREATE_TX_FLOW
                | OPCODE_DELETE_TX_FLOW
        ),
        PROTOCOL_ARC_2809 | crate::protocol::PROTOCOL_ARC_280F => matches!(
            envelope.opcode,
            OPCODE_QUERY_TX_FLOWS_2809
                | OPCODE_CREATE_TX_FLOW_2809
                | OPCODE_DELETE_TX_FLOW_2809
                | OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809
                | OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809
                | OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809
                | OPCODE_SET_RECEIVER_CHANNEL_NAME_2809
        ),
        _ => false,
    };
    let modern_receiver_port_ranges = envelope.protocol_id == PROTOCOL_ARC_2809
        && envelope.opcode == OPCODE_QUERY_RECEIVER_PORT_RANGES;
    let valid = (is_common_arc_protocol(envelope.protocol_id) && common_opcode)
        || flow_opcode
        || modern_receiver_port_ranges;
    valid.then_some(envelope.result_code)
}

pub fn parse_cmc_registration_response(response: &[u8]) -> Option<CmcRegistrationResponse> {
    if response.len() < 10
        || read_u16(response, 0)? != PROTOCOL_CMC
        || usize::from(read_u16(response, 2)?) != response.len()
        || read_u16(response, 6)? != 0x1001
    {
        return None;
    }
    Some(CmcRegistrationResponse {
        sequence: read_u16(response, 4)?,
        status: read_u16(response, 8)?,
    })
}

pub fn parse_bluetooth_status(response: &[u8]) -> Option<BluetoothStatus> {
    validate_conmon_envelope(response, CONMON_OPCODE_BLUETOOTH_STATUS)?;
    if response.len() < 50 {
        return None;
    }
    if response[36] != 0x12 || response[38] != 0x0a {
        return None;
    }

    let field1_len = usize::from(response[39]);
    let mut position = 40usize.checked_add(field1_len)?;

    if position < response.len() && response[position] == 0x18 {
        position = position.checked_add(1)?;
        while position < response.len() && response[position] & 0x80 != 0 {
            position = position.checked_add(1)?;
        }
        position = position.checked_add(1)?;
    }

    if position >= response.len() || response[position] != 0x22 {
        return None;
    }

    position = position.checked_add(1)?;
    if position >= response.len() {
        return None;
    }

    let field4_len = usize::from(response[position]);
    position = position.checked_add(1)?;
    let field4_end = position.checked_add(field4_len)?;
    if field4_end != response.len() {
        return None;
    }

    parse_bluetooth_payload(&response[position..field4_end])
}

fn length_delimited_payload(data: &[u8], tag: u8) -> Option<&[u8]> {
    if data.first().copied()? != tag {
        return None;
    }
    let length = usize::from(*data.get(1)?);
    (length.checked_add(2)? == data.len()).then_some(&data[2..])
}

fn parse_bluetooth_payload(data: &[u8]) -> Option<BluetoothStatus> {
    let level_one = length_delimited_payload(data, 0x0A)?;
    let level_two = length_delimited_payload(level_one, 0x12)?;
    let state = length_delimited_payload(level_two, 0x0A)?;
    if state.get(0..2)? != [0x08, 0x02] {
        if state.get(0..2)? != [0x08, 0x01] {
            return None;
        }
        let name_payload = length_delimited_payload(state.get(2..)?, 0x12)?;
        if name_payload.is_empty() {
            return None;
        }
        let name = std::str::from_utf8(name_payload).ok()?;
        return Some(BluetoothStatus {
            connected: true,
            device_name: Some(name.to_owned()),
        });
    }

    (state.len() == 2).then(BluetoothStatus::disconnected)
}
