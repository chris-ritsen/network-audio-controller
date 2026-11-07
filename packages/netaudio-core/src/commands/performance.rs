use super::*;

pub const PROPERTY_TX_FLOW_LATENCY_NS: u16 = 0x8204;
pub const PROPERTY_UNICAST_CONFIGURED_LATENCY_NS: u16 = 0x8205;
pub const PROPERTY_TX_FLOW_FRAMES_PER_PACKET: u16 = 0x0210;
pub const PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET: u16 = 0x0211;
pub const PROPERTY_RX_FLOW_LATENCY_NS: u16 = 0x8301;
pub const PROPERTY_RX_FLOW_FRAMES_PER_PACKET: u16 = 0x0310;
pub const PROPERTY_RX_FLOW_DEFAULT_SLOTS: u16 = 0x0303;
pub const PROPERTY_PRE_3_COMPATIBILITY: u16 = 0x8304;

pub const PERFORMANCE_PROPERTY_IDS: [u16; 8] = [
    PROPERTY_TX_FLOW_LATENCY_NS,
    PROPERTY_TX_FLOW_FRAMES_PER_PACKET,
    PROPERTY_UNICAST_CONFIGURED_LATENCY_NS,
    PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET,
    PROPERTY_RX_FLOW_LATENCY_NS,
    PROPERTY_RX_FLOW_FRAMES_PER_PACKET,
    PROPERTY_RX_FLOW_DEFAULT_SLOTS,
    PROPERTY_PRE_3_COMPATIBILITY,
];

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PerformanceProperty {
    pub property_id: u16,
    pub value: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PropertyValue {
    InlineU16(u16),
    ReferencedU32(u32),
}

pub(crate) fn require_performance_protocol(
    negotiated_protocol_id: u16,
) -> Result<u16, NetaudioError> {
    if !crate::protocol::is_supported_arc_protocol(negotiated_protocol_id)
        && !matches!(negotiated_protocol_id, 0x2601 | 0x280a)
    {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }

    Ok(negotiated_protocol_id.min(PROTOCOL_ARC_2809))
}

pub(crate) fn latency_nanoseconds(latency_microseconds: u64) -> Result<u32, NetaudioError> {
    latency_microseconds
        .checked_mul(1_000)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(NetaudioError::InvalidLatency)
}

fn require_properties(
    supported_property_ids: &[u16],
    required_property_ids: &[u16],
) -> Result<(), NetaudioError> {
    if required_property_ids
        .iter()
        .all(|property_id| supported_property_ids.contains(property_id))
    {
        Ok(())
    } else {
        Err(NetaudioError::UnsupportedProtocolOperation)
    }
}

fn build_property_write(
    negotiated_protocol_id: u16,
    properties: &[(u16, PropertyValue)],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let protocol_id = require_performance_protocol(negotiated_protocol_id)?;
    let body = property_write_body(properties)?;

    arc_packet_with_reserved_word(protocol_id, OPCODE_DEVICE_SETTINGS_SET, &body, message_id)
}

pub(crate) fn property_write_body(
    properties: &[(u16, PropertyValue)],
) -> Result<Vec<u8>, NetaudioError> {
    let count = u8::try_from(properties.len()).map_err(|_| NetaudioError::PacketTooLarge)?;
    let directory_bytes = properties
        .len()
        .checked_mul(4)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let first_value_pointer = 12usize
        .checked_add(directory_bytes)
        .ok_or(NetaudioError::PacketTooLarge)?;
    let mut next_value_pointer = first_value_pointer;
    let mut body = Vec::with_capacity(2 + directory_bytes + properties.len() * 4);
    body.push(count);
    body.push(4);

    for (property_id, value) in properties {
        body.extend_from_slice(&property_id.to_be_bytes());
        match value {
            PropertyValue::InlineU16(value) => body.extend_from_slice(&value.to_be_bytes()),
            PropertyValue::ReferencedU32(_) => {
                let pointer =
                    u16::try_from(next_value_pointer).map_err(|_| NetaudioError::PacketTooLarge)?;
                body.extend_from_slice(&pointer.to_be_bytes());
                next_value_pointer = next_value_pointer
                    .checked_add(4)
                    .ok_or(NetaudioError::PacketTooLarge)?;
            }
        }
    }
    for (_, value) in properties {
        if let PropertyValue::ReferencedU32(value) = value {
            body.extend_from_slice(&value.to_be_bytes());
        }
    }

    Ok(body)
}

pub fn build_store_current_configuration(
    negotiated_protocol_id: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    arc_packet_with_reserved_word(
        require_performance_protocol(negotiated_protocol_id)?,
        OPCODE_STORE_CURRENT_CONFIGURATION,
        &[],
        message_id,
    )
}

pub fn build_query_performance_settings(
    negotiated_protocol_id: u16,
    property_ids: &[u16],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let protocol_id = require_performance_protocol(negotiated_protocol_id)?;
    let count = u16::try_from(property_ids.len()).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut body = Vec::with_capacity(2 + property_ids.len() * 2);
    body.extend_from_slice(&count.to_be_bytes());
    for property_id in property_ids {
        body.extend_from_slice(&property_id.to_be_bytes());
    }
    arc_packet_with_reserved_word(protocol_id, OPCODE_DEVICE_SETTINGS, &body, message_id)
}

pub fn build_set_receive_flow_performance(
    negotiated_protocol_id: u16,
    supported_property_ids: &[u16],
    latency_microseconds: u64,
    frames_per_packet: u16,
    platform_software_version: [u16; 3],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let properties = receive_flow_performance_properties(
        supported_property_ids,
        latency_microseconds,
        frames_per_packet,
        platform_software_version,
    )?;
    build_property_write(negotiated_protocol_id, &properties, message_id)
}

pub(crate) fn receive_flow_performance_properties(
    supported_property_ids: &[u16],
    latency_microseconds: u64,
    frames_per_packet: u16,
    platform_software_version: [u16; 3],
) -> Result<Vec<(u16, PropertyValue)>, NetaudioError> {
    let mut properties = vec![
        (
            PROPERTY_RX_FLOW_LATENCY_NS,
            PropertyValue::ReferencedU32(latency_nanoseconds(latency_microseconds)?),
        ),
        (
            PROPERTY_RX_FLOW_FRAMES_PER_PACKET,
            PropertyValue::InlineU16(frames_per_packet),
        ),
    ];
    if platform_software_version < [3, 0, 0] {
        properties.push((
            PROPERTY_PRE_3_COMPATIBILITY,
            PropertyValue::ReferencedU32(1),
        ));
    }
    require_properties(
        supported_property_ids,
        &properties
            .iter()
            .map(|(property_id, _)| *property_id)
            .collect::<Vec<_>>(),
    )?;
    Ok(properties)
}

pub fn build_set_transmit_flow_performance(
    negotiated_protocol_id: u16,
    supported_property_ids: &[u16],
    latency_microseconds: u64,
    frames_per_packet: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let properties = transmit_flow_performance_properties(
        supported_property_ids,
        latency_microseconds,
        frames_per_packet,
    )?;
    build_property_write(negotiated_protocol_id, &properties, message_id)
}

pub(crate) fn transmit_flow_performance_properties(
    supported_property_ids: &[u16],
    latency_microseconds: u64,
    frames_per_packet: u16,
) -> Result<Vec<(u16, PropertyValue)>, NetaudioError> {
    let properties = vec![
        (
            PROPERTY_TX_FLOW_LATENCY_NS,
            PropertyValue::ReferencedU32(latency_nanoseconds(latency_microseconds)?),
        ),
        (
            PROPERTY_TX_FLOW_FRAMES_PER_PACKET,
            PropertyValue::InlineU16(frames_per_packet),
        ),
    ];
    require_properties(
        supported_property_ids,
        &[
            PROPERTY_TX_FLOW_LATENCY_NS,
            PROPERTY_TX_FLOW_FRAMES_PER_PACKET,
        ],
    )?;
    Ok(properties)
}

pub fn build_set_unicast_performance(
    negotiated_protocol_id: u16,
    supported_property_ids: &[u16],
    latency_microseconds: u64,
    frames_per_packet: u16,
    platform_software_version: [u16; 3],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let properties = unicast_performance_properties(
        supported_property_ids,
        latency_microseconds,
        frames_per_packet,
        platform_software_version,
    )?;
    build_property_write(negotiated_protocol_id, &properties, message_id)
}

pub(crate) fn unicast_performance_properties(
    supported_property_ids: &[u16],
    latency_microseconds: u64,
    frames_per_packet: u16,
    platform_software_version: [u16; 3],
) -> Result<Vec<(u16, PropertyValue)>, NetaudioError> {
    let latency_ns = latency_nanoseconds(latency_microseconds)?;
    let mut properties = Vec::new();
    for (property_id, value) in [
        (
            PROPERTY_UNICAST_CONFIGURED_LATENCY_NS,
            PropertyValue::ReferencedU32(latency_ns),
        ),
        (
            PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET,
            PropertyValue::InlineU16(frames_per_packet),
        ),
        (
            PROPERTY_RX_FLOW_LATENCY_NS,
            PropertyValue::ReferencedU32(latency_ns),
        ),
        (
            PROPERTY_RX_FLOW_FRAMES_PER_PACKET,
            PropertyValue::InlineU16(frames_per_packet),
        ),
    ] {
        if supported_property_ids.contains(&property_id) {
            properties.push((property_id, value));
        }
    }
    if platform_software_version < [3, 0, 0] {
        require_properties(supported_property_ids, &[PROPERTY_PRE_3_COMPATIBILITY])?;
        properties.push((
            PROPERTY_PRE_3_COMPATIBILITY,
            PropertyValue::ReferencedU32(1_000),
        ));
    } else if properties.is_empty() {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }
    Ok(properties)
}

pub fn build_set_receive_flow_default_slots(
    negotiated_protocol_id: u16,
    supported_property_ids: &[u16],
    default_slots: u16,
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let properties = receive_flow_default_slot_properties(supported_property_ids, default_slots)?;
    build_property_write(negotiated_protocol_id, &properties, message_id)
}

pub(crate) fn receive_flow_default_slot_properties(
    supported_property_ids: &[u16],
    default_slots: u16,
) -> Result<Vec<(u16, PropertyValue)>, NetaudioError> {
    require_properties(supported_property_ids, &[PROPERTY_RX_FLOW_DEFAULT_SLOTS])?;
    Ok(vec![(
        PROPERTY_RX_FLOW_DEFAULT_SLOTS,
        PropertyValue::InlineU16(default_slots),
    )])
}
