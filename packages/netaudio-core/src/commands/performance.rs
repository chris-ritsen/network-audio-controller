use super::*;

pub const PERFORMANCE_PROTOCOL_FLOOR_2601: u16 = 0x2601;
pub const PERFORMANCE_PROTOCOL_FLOOR_280A: u16 = 0x280a;
const PERFORMANCE_PROTOCOL_FLOORS: [u16; 2] = [
    PERFORMANCE_PROTOCOL_FLOOR_2601,
    PERFORMANCE_PROTOCOL_FLOOR_280A,
];

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropertyValue {
    InlineU16(u16),
    ReferencedU32(u32),
}

fn capped_protocol_id(negotiated_protocol_id: u16) -> u16 {
    negotiated_protocol_id.min(PROTOCOL_ARC_2809)
}

fn require_performance_protocol(negotiated_protocol_id: u16) -> Result<u16, NetaudioError> {
    if PERFORMANCE_PROTOCOL_FLOORS
        .iter()
        .all(|floor| negotiated_protocol_id < *floor)
    {
        return Err(NetaudioError::UnsupportedProtocolOperation);
    }
    Ok(capped_protocol_id(negotiated_protocol_id))
}

fn latency_nanoseconds(latency_microseconds: u64) -> Result<u32, NetaudioError> {
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
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let protocol_id = require_performance_protocol(negotiated_protocol_id)?;
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

    arc_packet_with_reserved_word(
        protocol_id,
        OPCODE_DEVICE_SETTINGS_SET,
        &body,
        transaction_id,
    )
}

pub fn build_store_current_configuration(
    negotiated_protocol_id: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    arc_packet_with_reserved_word(
        capped_protocol_id(negotiated_protocol_id),
        OPCODE_STORE_CURRENT_CONFIGURATION,
        &[],
        transaction_id,
    )
}

pub fn build_query_performance_settings(
    negotiated_protocol_id: u16,
    property_ids: &[u16],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    require_performance_protocol(negotiated_protocol_id)?;
    let count = u16::try_from(property_ids.len()).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut body = Vec::with_capacity(2 + property_ids.len() * 2);
    body.extend_from_slice(&count.to_be_bytes());
    for property_id in property_ids {
        body.extend_from_slice(&property_id.to_be_bytes());
    }
    arc_packet_with_reserved_word(
        capped_protocol_id(negotiated_protocol_id),
        OPCODE_DEVICE_SETTINGS,
        &body,
        transaction_id,
    )
}

pub fn build_set_receive_flow_performance(
    negotiated_protocol_id: u16,
    supported_property_ids: &[u16],
    latency_microseconds: u64,
    frames_per_packet: u16,
    platform_software_version: [u16; 3],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
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
    build_property_write(negotiated_protocol_id, &properties, transaction_id)
}

pub fn build_set_transmit_flow_performance(
    negotiated_protocol_id: u16,
    supported_property_ids: &[u16],
    latency_microseconds: u64,
    frames_per_packet: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let properties = [
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
    build_property_write(negotiated_protocol_id, &properties, transaction_id)
}

pub fn build_set_unicast_performance(
    negotiated_protocol_id: u16,
    supported_property_ids: &[u16],
    latency_microseconds: u64,
    frames_per_packet: u16,
    platform_software_version: [u16; 3],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
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
    build_property_write(negotiated_protocol_id, &properties, transaction_id)
}

pub fn build_set_receive_flow_default_slots(
    negotiated_protocol_id: u16,
    supported_property_ids: &[u16],
    default_slots: u16,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    require_properties(supported_property_ids, &[PROPERTY_RX_FLOW_DEFAULT_SLOTS])?;
    build_property_write(
        negotiated_protocol_id,
        &[(
            PROPERTY_RX_FLOW_DEFAULT_SLOTS,
            PropertyValue::InlineU16(default_slots),
        )],
        transaction_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_request_caps_protocol_and_has_only_the_reserved_word() {
        assert_eq!(
            build_store_current_configuration(0x280f, 0x1234).unwrap(),
            [0x28, 0x09, 0x00, 0x0a, 0x12, 0x34, 0x1f, 0x01, 0x00, 0x00]
        );
    }

    #[test]
    fn receive_performance_encodes_typed_values_and_old_software_property() {
        let packet = build_set_receive_flow_performance(
            0x280f,
            &[0x8301, 0x0310, 0x8304],
            250,
            8,
            [2, 9, 9],
            0x0102,
        )
        .unwrap();
        assert_eq!(
            packet,
            [
                0x28, 0x09, 0x00, 0x20, 0x01, 0x02, 0x11, 0x01, 0x00, 0x00, 0x03, 0x04, 0x83, 0x01,
                0x00, 0x18, 0x03, 0x10, 0x00, 0x08, 0x83, 0x04, 0x00, 0x1c, 0x00, 0x03, 0xd0, 0x90,
                0x00, 0x00, 0x00, 0x01,
            ]
        );
    }

    #[test]
    fn transmit_performance_and_default_slots_use_their_own_properties() {
        let transmit =
            build_set_transmit_flow_performance(0x2729, &[0x8204, 0x0210], 1_000, 16, 7).unwrap();
        assert_eq!(
            &transmit[..10],
            &[0x27, 0x29, 0x00, 0x18, 0, 7, 0x11, 1, 0, 0]
        );
        assert_eq!(
            &transmit[10..],
            &[2, 4, 0x82, 4, 0, 20, 2, 0x10, 0, 16, 0, 15, 0x42, 0x40]
        );

        let slots = build_set_receive_flow_default_slots(0x2809, &[0x0303], 32, 8).unwrap();
        assert_eq!(&slots[10..], &[1, 4, 3, 3, 0, 32]);
    }

    #[test]
    fn unicast_writes_only_advertised_values_and_compatibility_when_required() {
        let modern =
            build_set_unicast_performance(0x2809, &[0x8205, 0x0310], 500, 4, [3, 0, 0], 9).unwrap();
        assert_eq!(&modern[10..20], &[2, 4, 0x82, 5, 0, 20, 3, 0x10, 0, 4]);
        assert_eq!(&modern[20..], &500_000u32.to_be_bytes());

        let old = build_set_unicast_performance(0x2809, &[0x8304], 500, 4, [2, 0, 0], 10).unwrap();
        assert_eq!(&old[10..], &[1, 4, 0x83, 4, 0, 16, 0, 0, 3, 0xe8]);
    }

    #[test]
    fn performance_builders_fail_closed() {
        assert_eq!(
            build_set_transmit_flow_performance(0x2600, &[0x8204, 0x0210], 1, 1, 1),
            Err(NetaudioError::UnsupportedProtocolOperation)
        );
        assert_eq!(
            build_set_transmit_flow_performance(0x2809, &[0x8204], 1, 1, 1),
            Err(NetaudioError::UnsupportedProtocolOperation)
        );
        assert_eq!(
            build_set_transmit_flow_performance(
                0x2809,
                &[0x8204, 0x0210],
                u64::from(u32::MAX) / 1_000 + 1,
                1,
                1,
            ),
            Err(NetaudioError::InvalidLatency)
        );
        assert_eq!(
            build_set_unicast_performance(0x2809, &[], 1, 1, [3, 0, 0], 1),
            Err(NetaudioError::UnsupportedProtocolOperation)
        );
    }

    #[test]
    fn performance_query_uses_requested_properties() {
        assert_eq!(
            build_query_performance_settings(0x280f, &[0x8301, 0x0310], 0x20).unwrap(),
            [
                0x28, 0x09, 0x00, 0x10, 0x00, 0x20, 0x11, 0x00, 0x00, 0x00, 0x00, 0x02, 0x83, 0x01,
                0x03, 0x10,
            ]
        );
    }
}
