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

    let message_version = data.get(METERING_FAMILY_OFFSET).copied()?;
    let (tx_count, rx_count, levels_offset) = match message_version {
        0x01 | 0x02 => (
            u16::from(data.get(METERING_V2_TX_COUNT_OFFSET).copied()?),
            u16::from(data.get(METERING_V2_RX_COUNT_OFFSET).copied()?),
            METERING_V2_LEVELS_OFFSET,
        ),
        0x03 => {
            if data.len() < METERING_V3_HEADER_SIZE {
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
    if rx_levels_end > data.len() {
        return None;
    }

    let mut source_eui64 = String::with_capacity(16);
    for value in data.get(8..16)? {
        write!(source_eui64, "{value:02x}").ok()?;
    }

    Some(MeteringFrame {
        message_version,
        sequence: read_u16(data, 4)?,
        source_eui64,
        tx_count,
        rx_count,
        tx_levels: data.get(levels_offset..tx_levels_end)?.to_vec(),
        rx_levels: data.get(tx_levels_end..rx_levels_end)?.to_vec(),
        trailing_bytes: data.get(rx_levels_end..)?.to_vec(),
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
        aes67_configured_property_advertised: property_ids
            .contains(&DEVICE_SETTINGS_INFO_AES67_CONFIGURED),
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
    let null_position = raw.iter().position(|&byte| byte == 0)?;
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

fn optional_conmon_string(data: &[u8], start: usize, end: usize) -> Option<String> {
    data.get(start..end)
        .and_then(conmon_string_bytes)
        .filter(|value| !value.is_empty())
}

fn version_components(word: u32, fourth: Option<u32>) -> Option<Vec<u32>> {
    if word == 0 && fourth.unwrap_or(0) == 0 {
        return None;
    }
    let mut components = vec![word >> 24, (word >> 16) & 0xff, word & 0xffff];
    if let Some(fourth) = fourth.filter(|value| *value != 0) {
        components.push(fourth);
    }
    Some(components)
}

fn formatted_version(word: u32, fourth: Option<u32>) -> Option<String> {
    version_components(word, fourth).map(|parts| {
        parts
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(".")
    })
}

pub fn parse_make_model(data: &[u8]) -> Option<ManufacturerVersions> {
    validate_conmon_envelope(data, CONMON_OPCODE_MAKE_MODEL_RESPONSE)?;
    let record_protocol_version = read_u16(data, CONMON_DANTE_MODEL_BODY_OFFSET)?;
    if record_protocol_version > 0x0731 {
        return None;
    }
    let record_start = CONMON_DANTE_MODEL_BODY_OFFSET;
    let identifier = |offset: usize| data.get(record_start + offset..record_start + offset + 8);
    let manufacturer_identifier_raw = identifier(0x08)?;
    let product_identifier_raw = identifier(0x10)?;
    let serial_identifier_raw = identifier(0x18)?;
    let manufacturer_software_word = read_u32(data, record_start + 0x20)?;
    let manufacturer_firmware_word = read_u32(data, record_start + 0x24)?;
    let manufacturer_capabilities = if record_protocol_version >= 0x0606 {
        Some(read_u32(data, record_start + 0x28)?)
    } else {
        None
    };
    let manufacturer_software_fourth = if record_protocol_version >= 0x0701 {
        Some(read_u32(data, record_start + 0x2c)?)
    } else {
        None
    };
    let manufacturer_firmware_fourth = if record_protocol_version >= 0x0701 {
        Some(read_u32(data, record_start + 0x30)?)
    } else {
        None
    };
    let manufacturer = if record_protocol_version >= 0x0701 {
        data.get(record_start + 0x34..record_start + 0xb4)?;
        optional_conmon_string(data, record_start + 0x34, record_start + 0xb4)
    } else {
        None
    };
    let product_name = if record_protocol_version >= 0x0701 {
        data.get(record_start + 0xb4..record_start + 0x134)?;
        optional_conmon_string(data, record_start + 0xb4, record_start + 0x134)
    } else {
        None
    };
    let product_version_word = if record_protocol_version >= 0x0704 {
        Some(read_u32(data, record_start + 0x134)?)
    } else {
        None
    };
    let friendly_product_version = if record_protocol_version >= 0x0712 {
        optional_conmon_string(data, record_start + 0x138, data.len())
    } else {
        None
    };
    let product_version = product_version_word.and_then(|word| formatted_version(word, None));
    let display_product_version = friendly_product_version
        .clone()
        .or_else(|| product_version.clone());
    Some(ManufacturerVersions {
        record_protocol_version,
        manufacturer_identifier: conmon_string_bytes(manufacturer_identifier_raw)
            .filter(|value| !value.is_empty()),
        manufacturer_identifier_hexadecimal: bytes_to_hex(manufacturer_identifier_raw),
        product_identifier: conmon_string_bytes(product_identifier_raw)
            .filter(|value| !value.is_empty()),
        product_identifier_hexadecimal: bytes_to_hex(product_identifier_raw),
        serial_number_identifier: conmon_string_bytes(serial_identifier_raw)
            .filter(|value| !value.is_empty()),
        serial_number_identifier_hexadecimal: bytes_to_hex(serial_identifier_raw),
        manufacturer_software_version: formatted_version(
            manufacturer_software_word,
            manufacturer_software_fourth,
        ),
        manufacturer_software_version_components: version_components(
            manufacturer_software_word,
            manufacturer_software_fourth,
        ),
        manufacturer_firmware_version: formatted_version(
            manufacturer_firmware_word,
            manufacturer_firmware_fourth,
        ),
        manufacturer_firmware_version_components: version_components(
            manufacturer_firmware_word,
            manufacturer_firmware_fourth,
        ),
        manufacturer_capabilities,
        manufacturer,
        product_name,
        product_version: product_version.clone(),
        product_version_components: product_version_word
            .and_then(|word| version_components(word, None)),
        friendly_product_version,
        display_product_version,
        raw_record_hexadecimal: bytes_to_hex(data.get(record_start..)?),
    })
}

pub fn parse_dante_model(data: &[u8]) -> Option<PlatformVersions> {
    validate_conmon_envelope(data, CONMON_OPCODE_DANTE_MODEL_RESPONSE)?;
    let record_start = CONMON_DANTE_MODEL_BODY_OFFSET;
    let record_protocol_version = read_u16(data, record_start)?;
    if record_protocol_version > 0x0731 {
        return None;
    }
    let platform_software_word = read_u32(data, record_start + 0x08)?;
    let platform_hardware_word = read_u32(data, record_start + 0x0c)?;
    let platform_api_word = read_u32(data, record_start + 0x10)?;
    let platform_model_identifier_raw = data.get(record_start + 0x14..record_start + 0x1c)?;
    let primary_capabilities = if record_protocol_version >= 0x0200 {
        Some(read_u32(
            data,
            CONMON_DANTE_MODEL_PRIMARY_CAPABILITIES_OFFSET,
        )?)
    } else {
        None
    };
    let read_only_capabilities = if record_protocol_version >= 0x070A {
        Some(read_u32(
            data,
            CONMON_DANTE_MODEL_READ_ONLY_CAPABILITIES_OFFSET,
        )?)
    } else {
        None
    };
    let monitoring_capabilities = if record_protocol_version >= 0x0717 {
        read_u32(data, CONMON_DANTE_MODEL_MONITORING_CAPABILITIES_OFFSET)?
    } else {
        0
    };
    let secondary_capabilities = if record_protocol_version >= 0x071E {
        read_u32(data, CONMON_DANTE_MODEL_SECONDARY_CAPABILITIES_OFFSET)?
    } else {
        0
    };
    let (domain_capability_values, domain_capability_validity) =
        if record_protocol_version >= 0x0723 {
            (
                read_u32(data, CONMON_DANTE_MODEL_DOMAIN_CAPABILITY_VALUES_OFFSET)?,
                read_u32(data, CONMON_DANTE_MODEL_DOMAIN_CAPABILITY_VALIDITY_OFFSET)?,
            )
        } else {
            (0, 0)
        };
    let software_fourth = if record_protocol_version >= 0x0701 {
        Some(read_u32(data, record_start + 0x28)?)
    } else {
        None
    };
    let hardware_fourth = if record_protocol_version >= 0x0701 {
        Some(read_u32(data, record_start + 0x2c)?)
    } else {
        None
    };
    let preferred_link_speed = if record_protocol_version >= 0x0200 {
        Some(read_u32(data, record_start + 0x20)?)
    } else {
        None
    };
    let device_status_flags = if record_protocol_version >= 0x0704 {
        Some(read_u32(data, record_start + 0x24)?)
    } else {
        None
    };
    let rom_boot_word = if record_protocol_version >= 0x0704 {
        Some(read_u32(data, record_start + 0x30)?)
    } else {
        None
    };
    let supported_clock_protocol_flags = if record_protocol_version >= 0x0707 {
        read_u32(data, record_start + 0x34)?
    } else {
        1
    };
    let platform_model_name = if record_protocol_version >= 0x070c {
        data.get(record_start + 0x40..record_start + 0xc0)?;
        optional_conmon_string(data, record_start + 0x40, record_start + 0xc0)
    } else {
        None
    };
    let mut plugin_identifiers = Vec::new();
    let mut plugin_records_hexadecimal = Vec::new();
    if record_protocol_version >= 0x0731 {
        let count = usize::from(read_u16(data, record_start + 0xd0)?);
        let vector_offset = usize::from(read_u16(data, record_start + 0xd2)?);
        if count != 0 && vector_offset < 0xd4 {
            return None;
        }
        let vector_start = record_start.checked_add(vector_offset)?;
        let vector_size = count.checked_mul(0x18)?;
        let vector_end = vector_start.checked_add(vector_size)?;
        let vector = data.get(vector_start..vector_end)?;
        for record in vector.chunks_exact(0x18) {
            plugin_identifiers.push(conmon_string_bytes(record).filter(|value| !value.is_empty()));
            plugin_records_hexadecimal.push(bytes_to_hex(record));
        }
    }
    Some(PlatformVersions {
        record_protocol_version,
        platform_software_version: formatted_version(platform_software_word, software_fourth),
        platform_software_version_components: version_components(
            platform_software_word,
            software_fourth,
        ),
        platform_hardware_version: formatted_version(platform_hardware_word, hardware_fourth),
        platform_hardware_version_components: version_components(
            platform_hardware_word,
            hardware_fourth,
        ),
        platform_api_version: formatted_version(platform_api_word, None),
        platform_api_version_components: version_components(platform_api_word, None),
        platform_model_identifier: conmon_string_bytes(platform_model_identifier_raw)
            .filter(|value| !value.is_empty()),
        platform_model_identifier_hexadecimal: bytes_to_hex(platform_model_identifier_raw),
        primary_capabilities,
        preferred_link_speed,
        device_status_flags,
        rom_boot_version: rom_boot_word.and_then(|word| formatted_version(word, None)),
        rom_boot_version_components: rom_boot_word.and_then(|word| version_components(word, None)),
        supported_clock_protocol_flags,
        read_only_capabilities,
        platform_model_name,
        monitoring_capabilities,
        secondary_capabilities,
        domain_capability_values,
        domain_capability_validity,
        effective_domain_capabilities: domain_capability_values & domain_capability_validity,
        plugin_identifiers,
        plugin_records_hexadecimal,
        raw_record_hexadecimal: bytes_to_hex(data.get(record_start..)?),
        identify_supported: primary_capabilities.unwrap_or(0)
            & DANTE_MODEL_IDENTIFY_CAPABILITY_MASK
            != 0,
        sample_rate_configuration_supported: primary_capabilities.unwrap_or(0)
            & DANTE_MODEL_SAMPLE_RATE_CAPABILITY_MASK
            != 0,
        encoding_configuration_supported: primary_capabilities.unwrap_or(0)
            & DANTE_MODEL_ENCODING_CAPABILITY_MASK
            != 0,
        sample_rate_pullup_configuration_supported: primary_capabilities.unwrap_or(0)
            & DANTE_MODEL_SAMPLE_RATE_PULLUP_CAPABILITY_MASK
            != 0,
        switch_redundancy_supported: primary_capabilities
            .map(|capabilities| capabilities & DANTE_MODEL_SWITCH_REDUNDANCY_CAPABILITY_MASK != 0),
        static_ipv4_configuration_supported: primary_capabilities.unwrap_or(0)
            & DANTE_MODEL_STATIC_IPV4_CAPABILITY_MASK
            != 0,
        detailed_metering_supported: primary_capabilities.unwrap_or(0)
            & DANTE_MODEL_DETAILED_METERING_CAPABILITY_MASK
            != 0,
        aes67_configuration_supported: primary_capabilities.unwrap_or(0)
            & DANTE_MODEL_AES67_CAPABILITY_MASK
            != 0,
        device_locking_supported: primary_capabilities.unwrap_or(0)
            & DANTE_MODEL_LOCKING_CAPABILITY_MASK
            != 0,
        external_word_clock_read_only: read_only_capabilities.unwrap_or(0)
            & DANTE_MODEL_EXTERNAL_WORD_CLOCK_READ_ONLY_MASK
            != 0,
        switch_redundancy_read_only: read_only_capabilities
            .map(|capabilities| capabilities & DANTE_MODEL_SWITCH_REDUNDANCY_READ_ONLY_MASK != 0),
        static_ipv4_configuration_read_only: read_only_capabilities.unwrap_or(0)
            & DANTE_MODEL_STATIC_IPV4_READ_ONLY_MASK
            != 0,
        generic_codec_control_supported: secondary_capabilities
            & DANTE_MODEL_GENERIC_CODEC_CAPABILITY_MASK
            != 0,
        interface_statistics_supported: monitoring_capabilities
            & MONITORING_INTERFACE_STATISTICS_MASK
            != 0,
        clock_monitoring_supported: monitoring_capabilities & MONITORING_CLOCK_MASK != 0,
        per_channel_signal_presence_supported: monitoring_capabilities
            & MONITORING_PER_CHANNEL_SIGNAL_PRESENCE_MASK
            != 0,
        rx_flow_maximum_latency_monitoring_supported: monitoring_capabilities
            & MONITORING_RX_FLOW_MAXIMUM_LATENCY_MASK
            != 0,
        rx_flow_late_packet_monitoring_supported: monitoring_capabilities
            & MONITORING_RX_FLOW_LATE_PACKET_MASK
            != 0,
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
                | OPCODE_TX_CHANNEL_NAME_SET
                | crate::commands::OPCODE_MODERN_ARC_SUBSCRIPTION
        ),
        _ => false,
    };
    let modern_receiver_port_ranges = envelope.protocol_id == PROTOCOL_ARC_2809
        && envelope.opcode == OPCODE_QUERY_RECEIVER_PORT_RANGES;
    let device_settings_protocol =
        crate::protocol::DEVICE_SETTINGS_ARC_PROTOCOL_IDS.contains(&envelope.protocol_id);
    let device_settings_opcode = matches!(
        envelope.opcode,
        OPCODE_DEVICE_SETTINGS | OPCODE_DEVICE_SETTINGS_SET | OPCODE_PROPERTY_DIRECTORY
    );
    let device_settings_result = device_settings_protocol && device_settings_opcode;
    let configuration_storage =
        device_settings_protocol && envelope.opcode == OPCODE_STORE_CURRENT_CONFIGURATION;
    let valid = (is_common_arc_protocol(envelope.protocol_id) && common_opcode)
        || flow_opcode
        || modern_receiver_port_ranges
        || device_settings_result
        || configuration_storage;
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
