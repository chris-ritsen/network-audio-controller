use super::conmon_common::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PtpClockPortRecord {
    pub record_flags: u16,
    pub link_down: bool,
    pub record_number: u16,
    pub ptp_version: u8,
    pub record_format_code: u8,
    pub transport_path_code: u8,
    pub transport_path: Option<String>,
    pub reserved_byte: u8,
    pub network_interface_index: u32,
    pub state_code: u16,
    pub role: Option<String>,
    pub status_flags: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PtpClockStatus {
    pub preferred_leader: bool,
    pub clock_source_code: u16,
    pub clock_identity: [u8; CONMON_CLOCK_IDENTITY_SIZE],
    pub first_leader_clock_identity_field: [u8; CONMON_LEADER_CLOCK_IDENTITY_FIELD_SIZE],
    pub second_leader_clock_identity_field: [u8; CONMON_LEADER_CLOCK_IDENTITY_FIELD_SIZE],
    pub leader_clock_identity: Option<[u8; CONMON_CLOCK_IDENTITY_SIZE]>,
    pub clock_subdomain: [u8; 16],
    pub clock_frequency_offset_parts_per_billion: i32,
    pub clock_port_state_code: u16,
    pub clock_role: Option<String>,
    pub clock_port_records: Option<Vec<PtpClockPortRecord>>,
}

fn clock_role(state_code: u16) -> Option<String> {
    match state_code {
        0x0006 => Some("Leader".to_string()),
        0x0009 => Some("Follower".to_string()),
        _ => None,
    }
}

fn clock_transport_path(transport_path_code: u8) -> Option<String> {
    match transport_path_code {
        0x01 => Some("multicast".to_owned()),
        0x02 => Some("unicast".to_owned()),
        _ => None,
    }
}

fn validated_leader_clock_identity(
    first_field: [u8; CONMON_LEADER_CLOCK_IDENTITY_FIELD_SIZE],
    second_field: [u8; CONMON_LEADER_CLOCK_IDENTITY_FIELD_SIZE],
) -> Option<[u8; CONMON_CLOCK_IDENTITY_SIZE]> {
    if first_field != second_field || first_field[..2] != [0, 0] {
        return None;
    }
    let identity: [u8; CONMON_CLOCK_IDENTITY_SIZE] = first_field[2..].try_into().ok()?;
    identity.iter().any(|value| *value != 0).then_some(identity)
}

fn parse_ptp_clock_port_records(data: &[u8]) -> Option<Vec<PtpClockPortRecord>> {
    let record = data.get(CONMON_CLOCK_RECORD_PAYLOAD_OFFSET..)?;
    let preceding_count = usize::from(read_u16(
        record,
        CONMON_CLOCK_PRECEDING_COUNT_RECORD_OFFSET,
    )?);
    let descriptor_offset = CONMON_CLOCK_PORT_DESCRIPTOR_BASE_RECORD_OFFSET
        .checked_add(preceding_count.checked_mul(4)?)?;
    let descriptor_target = usize::from(read_u16(record, descriptor_offset)?);
    let descriptor_size = usize::from(read_u16(record, descriptor_offset.checked_add(2)?)?);
    if descriptor_size != CONMON_CLOCK_PORT_DESCRIPTOR_SIZE
        || descriptor_target != descriptor_offset.checked_add(descriptor_size)?
    {
        return None;
    }

    let port_count = usize::from(read_u16(record, descriptor_target)?);
    let reserved_header = read_u16(record, descriptor_target.checked_add(2)?)?;
    let records_offset = usize::from(read_u16(record, descriptor_target.checked_add(4)?)?);
    let record_size = usize::from(read_u16(record, descriptor_target.checked_add(6)?)?);
    if reserved_header != 0
        || records_offset != descriptor_target.checked_add(CONMON_CLOCK_PORT_HEADER_SIZE)?
        || record_size != CONMON_CLOCK_PORT_RECORD_SIZE
    {
        return None;
    }
    let records_end = records_offset.checked_add(port_count.checked_mul(record_size)?)?;
    if records_end != record.len() {
        return None;
    }

    let mut records = Vec::with_capacity(port_count);
    for record_index in 0..port_count {
        let offset = records_offset.checked_add(record_index.checked_mul(record_size)?)?;
        let clock_port_record = record.get(offset..offset.checked_add(record_size)?)?;
        let record_flags = read_u16(clock_port_record, 0)?;
        let state_code = read_u16(clock_port_record, 12)?;
        let transport_path_code = *clock_port_record.get(6)?;
        records.push(PtpClockPortRecord {
            record_flags,
            link_down: record_flags & 0x0002 != 0,
            record_number: read_u16(clock_port_record, 2)?,
            ptp_version: *clock_port_record.get(4)?,
            record_format_code: *clock_port_record.get(5)?,
            transport_path_code,
            transport_path: clock_transport_path(transport_path_code),
            reserved_byte: *clock_port_record.get(7)?,
            network_interface_index: read_u32(clock_port_record, 8)?,
            state_code,
            role: clock_role(state_code),
            status_flags: read_u16(clock_port_record, 14)?,
        });
    }
    Some(records)
}

pub fn parse_ptp_clock_status(data: &[u8]) -> Option<PtpClockStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_PTP_CLOCK_STATUS)?;
    if data.len() <= CONMON_PREFERRED_LEADER_OFFSET {
        return None;
    }
    let preferred_leader = match data[CONMON_PREFERRED_LEADER_OFFSET] {
        0x00 => false,
        0x01 => true,
        _ => return None,
    };
    let clock_source_code = read_u16(data, CONMON_CLOCK_SOURCE_CODE_OFFSET)?;
    let clock_identity = data
        .get(
            CONMON_CLOCK_IDENTITY_OFFSET..CONMON_CLOCK_IDENTITY_OFFSET + CONMON_CLOCK_IDENTITY_SIZE,
        )?
        .try_into()
        .ok()?;
    let first_leader_clock_identity_field = data
        .get(
            CONMON_FIRST_LEADER_CLOCK_IDENTITY_OFFSET
                ..CONMON_FIRST_LEADER_CLOCK_IDENTITY_OFFSET
                    + CONMON_LEADER_CLOCK_IDENTITY_FIELD_SIZE,
        )?
        .try_into()
        .ok()?;
    let second_leader_clock_identity_field = data
        .get(
            CONMON_SECOND_LEADER_CLOCK_IDENTITY_OFFSET
                ..CONMON_SECOND_LEADER_CLOCK_IDENTITY_OFFSET
                    + CONMON_LEADER_CLOCK_IDENTITY_FIELD_SIZE,
        )?
        .try_into()
        .ok()?;
    let leader_clock_identity = validated_leader_clock_identity(
        first_leader_clock_identity_field,
        second_leader_clock_identity_field,
    );
    let mut clock_subdomain = [0u8; CONMON_CLOCK_SUBDOMAIN_SIZE];
    if data.len() >= CONMON_CLOCK_SUBDOMAIN_OFFSET + CONMON_CLOCK_SUBDOMAIN_SIZE {
        clock_subdomain.copy_from_slice(
            &data[CONMON_CLOCK_SUBDOMAIN_OFFSET
                ..CONMON_CLOCK_SUBDOMAIN_OFFSET + CONMON_CLOCK_SUBDOMAIN_SIZE],
        );
    }
    let clock_frequency_offset_parts_per_billion =
        read_u32(data, CONMON_CLOCK_FREQUENCY_OFFSET_PARTS_PER_BILLION_OFFSET)? as i32;
    let clock_port_state_code = read_u16(data, CONMON_CLOCK_PORT_STATE_OFFSET)?;
    let clock_role = clock_role(clock_port_state_code);
    let clock_port_records = parse_ptp_clock_port_records(data);
    Some(PtpClockStatus {
        preferred_leader,
        clock_source_code,
        clock_identity,
        first_leader_clock_identity_field,
        second_leader_clock_identity_field,
        leader_clock_identity,
        clock_subdomain,
        clock_frequency_offset_parts_per_billion,
        clock_port_state_code,
        clock_role,
        clock_port_records,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Aes67Status {
    pub aes67_current: Option<bool>,
    pub aes67_configured: Option<bool>,
}

pub fn parse_aes67_status(data: &[u8]) -> Option<Aes67Status> {
    validate_conmon_envelope(data, CONMON_OPCODE_AES67_CURRENT_NEW)?;
    if data.len() <= CONMON_AES67_CURRENT_NEW_OFFSET {
        return None;
    }
    let (current, configured) = match data[CONMON_AES67_CURRENT_NEW_OFFSET] {
        0x00 => (Some(false), Some(false)),
        0x01 => (Some(true), Some(false)),
        0x02 => (Some(false), Some(true)),
        0x03 => (Some(true), Some(true)),
        _ => return None,
    };
    Some(Aes67Status {
        aes67_current: current,
        aes67_configured: configured,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LockResetStatus {
    pub record_protocol_identifier: u16,
    pub unmapped_prefix_word: u32,
    pub lock_state_code: u16,
    pub is_locked: Option<bool>,
    pub status_code: u16,
    pub lock_identifier_count: u16,
    pub lock_identifier_width: u16,
    pub lock_identifier_data_offset: u16,
    pub unmapped_trailer_words: [u16; 3],
    pub lock_identifiers: Vec<String>,
    pub raw_record_hexadecimal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConmonExportFragment {
    pub envelope_sequence_identifier: u16,
    pub record_protocol_identifier: u16,
    pub echoed_tag_hexadecimal: String,
    pub total_encoded_size: u32,
    pub selector_value: u16,
    pub fragment_identifier: u16,
    pub has_more_fragments: bool,
    pub fragment_size: u16,
    pub header_size: u16,
    pub data_hexadecimal: String,
}

pub fn parse_conmon_export_fragment(data: &[u8]) -> Option<ConmonExportFragment> {
    validate_conmon_envelope(data, CONMON_OPCODE_EXPORT_FRAGMENT)?;
    if data.len() <= CONMON_EXPORT_DATA_OFFSET
        || data.get(28..32)? != [0, 0, 0, 0]
        || data.get(50..52)? != [0, 0]
    {
        return None;
    }
    let total_encoded_size = read_u32(data, 36)?;
    if total_encoded_size == 0 {
        return None;
    }
    let selector_value = read_u16(data, 40)?;
    let fragment_identifier = read_u16(data, 42)?;
    let continuation_flag = read_u16(data, 44)?;
    let fragment_size = read_u16(data, 46)?;
    let header_size = read_u16(data, 48)?;
    let fragment_data = data.get(CONMON_EXPORT_DATA_OFFSET..)?;
    if fragment_identifier == 0
        || continuation_flag > 1
        || (continuation_flag == 1 && fragment_identifier == u16::MAX)
        || fragment_size == 0
        || usize::from(fragment_size) != fragment_data.len()
        || u32::from(fragment_size) > total_encoded_size
        || usize::from(header_size) != CONMON_EXPORT_HEADER_SIZE
    {
        return None;
    }
    Some(ConmonExportFragment {
        envelope_sequence_identifier: read_u16(data, 4)?,
        record_protocol_identifier: read_u16(data, 24)?,
        echoed_tag_hexadecimal: bytes_to_hex(data.get(32..36)?),
        total_encoded_size,
        selector_value,
        fragment_identifier,
        has_more_fragments: continuation_flag == 1,
        fragment_size,
        header_size,
        data_hexadecimal: bytes_to_hex(fragment_data),
    })
}

pub fn parse_lock_reset_status(data: &[u8]) -> Option<LockResetStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_LOCK_RESET_STATUS)?;
    let record = data.get(CONMON_LOCK_RESET_RECORD_OFFSET..)?;
    if record.len() < CONMON_LOCK_RESET_FIXED_RECORD_SIZE {
        return None;
    }

    let lock_identifier_count = read_u16(record, 12)?;
    let lock_identifier_width = read_u16(record, 14)?;
    let lock_identifier_data_offset = read_u16(record, 16)?;
    if usize::from(lock_identifier_width) != CONMON_LOCK_RESET_IDENTIFIER_WIDTH {
        return None;
    }

    let identifier_bytes =
        usize::from(lock_identifier_count).checked_mul(CONMON_LOCK_RESET_IDENTIFIER_WIDTH)?;
    let expected_record_size = CONMON_LOCK_RESET_FIXED_RECORD_SIZE.checked_add(identifier_bytes)?;
    if record.len() != expected_record_size {
        return None;
    }

    let status_code = read_u16(record, 10)?;
    if lock_identifier_count == 0 {
        if lock_identifier_data_offset != 0 {
            return None;
        }
    } else if usize::from(lock_identifier_data_offset) != CONMON_LOCK_RESET_FIXED_RECORD_SIZE {
        return None;
    }

    let mut lock_identifiers = Vec::with_capacity(usize::from(lock_identifier_count));
    for identifier_index in 0..usize::from(lock_identifier_count) {
        let identifier_offset = CONMON_LOCK_RESET_FIXED_RECORD_SIZE
            .checked_add(identifier_index.checked_mul(CONMON_LOCK_RESET_IDENTIFIER_WIDTH)?)?;
        let identifier_end = identifier_offset.checked_add(CONMON_LOCK_RESET_IDENTIFIER_WIDTH)?;
        lock_identifiers.push(bytes_to_hex(record.get(identifier_offset..identifier_end)?));
    }

    let lock_state_code = read_u16(record, 8)?;
    let is_locked = match lock_state_code {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    };
    Some(LockResetStatus {
        record_protocol_identifier: read_u16(record, 0)?,
        unmapped_prefix_word: read_u32(record, 4)?,
        lock_state_code,
        is_locked,
        status_code,
        lock_identifier_count,
        lock_identifier_width,
        lock_identifier_data_offset,
        unmapped_trailer_words: [
            read_u16(record, 18)?,
            read_u16(record, 20)?,
            read_u16(record, 22)?,
        ],
        lock_identifiers,
        raw_record_hexadecimal: bytes_to_hex(record),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceStatus {
    pub link_speed_mbps: u32,
    pub interfaces: Vec<InterfaceStatusEntry>,
    pub reboot_required: bool,
    pub pending_config: Option<PendingInterfaceConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceStatusEntry {
    pub mode: String,
    pub mac_address: String,
    pub ip_address: String,
    pub netmask: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_server: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingInterfaceConfig {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub netmask: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_server: Option<String>,
}

pub(super) fn pending_interface_config_matches_running(
    pending_config: &PendingInterfaceConfig,
    running_interface: &InterfaceStatusEntry,
) -> bool {
    if pending_config.mode != running_interface.mode {
        return false;
    }

    match pending_config.mode.as_str() {
        "dynamic" => true,
        "static" => {
            pending_config.ip_address.as_deref() == Some(running_interface.ip_address.as_str())
                && pending_config.netmask.as_deref() == Some(running_interface.netmask.as_str())
                && pending_config.gateway.as_deref() == running_interface.gateway.as_deref()
                && pending_config.dns_server.as_deref() == running_interface.dns_server.as_deref()
        }
        _ => false,
    }
}

pub fn parse_interface_status(data: &[u8]) -> Option<InterfaceStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_INTERFACE_STATUS)?;
    if data.len() < CONMON_INTERFACE_MINIMUM_SIZE {
        return None;
    }

    let interface_count = read_u16(data, CONMON_INTERFACE_COUNT_OFFSET)?;
    if interface_count > 8 {
        return None;
    }
    let link_speed_mbps = read_u32(data, CONMON_INTERFACE_LINK_SPEED_OFFSET)?;
    let mut interfaces = Vec::with_capacity(usize::from(interface_count));
    let mut mac_addresses = HashSet::with_capacity(usize::from(interface_count));
    let mut offset = CONMON_INTERFACE_RECORDS_OFFSET;

    for _ in 0..interface_count {
        data.get(offset..offset.checked_add(CONMON_INTERFACE_RECORD_SIZE)?)?;

        let mode_value = read_u16(data, offset)?;
        let (mode, configured) = match mode_value {
            INTERFACE_MODE_DYNAMIC => ("dynamic".to_owned(), true),
            INTERFACE_MODE_STATIC => ("static".to_owned(), true),
            value => (format!("unknown(0x{value:04X})"), false),
        };
        let (record_size, record_stride) = if configured {
            (
                CONMON_INTERFACE_CONFIGURED_RECORD_SIZE,
                CONMON_INTERFACE_CONFIGURED_RECORD_STRIDE,
            )
        } else {
            (CONMON_INTERFACE_RECORD_SIZE, CONMON_INTERFACE_RECORD_SIZE)
        };
        data.get(offset..offset.checked_add(record_size)?)?;

        let mac = &data[offset + 2..offset + 8];
        let mac_address = format!(
            "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
            mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
        );
        if !mac_addresses.insert(mac_address.clone()) {
            return None;
        }
        let ip_address = ipv4_at(data, offset + 8)?;
        let netmask = ipv4_at(data, offset + 12)?;
        let (gateway, dns_server) = match mode_value {
            INTERFACE_MODE_DYNAMIC => (
                Some(ipv4_at(data, offset + 16)?),
                Some(ipv4_at(data, offset + 20)?),
            ),
            INTERFACE_MODE_STATIC => (
                Some(ipv4_at(data, offset + 20)?),
                Some(ipv4_at(data, offset + 16)?),
            ),
            _ => (None, None),
        };

        interfaces.push(InterfaceStatusEntry {
            mode,
            mac_address,
            ip_address,
            netmask,
            gateway,
            dns_server,
        });
        offset = offset.checked_add(record_stride)?;
    }

    let reboot_flag = if interface_count == 1 {
        read_u16(data, CONMON_INTERFACE_REBOOT_FLAG_OFFSET)?
    } else {
        0
    };
    let reported_pending_config = match reboot_flag {
        INTERFACE_REBOOT_PENDING_DYNAMIC => Some(PendingInterfaceConfig {
            mode: "dynamic".to_owned(),
            ip_address: None,
            netmask: None,
            gateway: None,
            dns_server: None,
        }),
        INTERFACE_REBOOT_PENDING_STATIC
            if data.len() >= CONMON_INTERFACE_PENDING_STATIC_OFFSET + 16 =>
        {
            Some(PendingInterfaceConfig {
                mode: "static".to_owned(),
                ip_address: Some(ipv4_at(data, CONMON_INTERFACE_PENDING_STATIC_OFFSET)?),
                netmask: Some(ipv4_at(data, CONMON_INTERFACE_PENDING_STATIC_OFFSET + 4)?),
                dns_server: Some(ipv4_at(data, CONMON_INTERFACE_PENDING_STATIC_OFFSET + 8)?),
                gateway: Some(ipv4_at(data, CONMON_INTERFACE_PENDING_STATIC_OFFSET + 12)?),
            })
        }
        0 => None,
        _ => return None,
    };
    let pending_config = reported_pending_config.filter(|pending_config| {
        interfaces.first().is_none_or(|running_interface| {
            !pending_interface_config_matches_running(pending_config, running_interface)
        })
    });

    Some(InterfaceStatus {
        link_speed_mbps,
        interfaces,
        reboot_required: pending_config.is_some(),
        pending_config,
    })
}
