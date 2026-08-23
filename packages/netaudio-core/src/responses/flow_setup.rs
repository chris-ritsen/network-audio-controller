use super::*;

pub fn parse_dante_brooklyn_control_protocol_flow_setup_request(
    data: &[u8],
) -> Option<DanteBrooklynControlProtocolFlowSetupRequest> {
    if data.len() <= 24
        || read_u16(data, 0)? != 0x1102
        || usize::from(read_u16(data, 2)?) != data.len()
    {
        return None;
    }

    let receiver_device_name_pointer = read_u32(data, 8)?;
    let receiver_device_name_pointer_u16 = u16::try_from(receiver_device_name_pointer).ok()?;
    let transport_descriptor_pointer = read_u16(data, 24)?;
    let transport_descriptor_count = read_u16(data, 26)?;
    let transport_descriptor_size = usize::from(transport_descriptor_count).checked_mul(4)?;
    let transport_descriptor_end =
        usize::from(transport_descriptor_pointer).checked_add(transport_descriptor_size)?;
    let transport_descriptor =
        data.get(usize::from(transport_descriptor_pointer)..transport_descriptor_end)?;
    let address_value_pointer = read_u32(data, 32)?;
    let address_value_offset = usize::try_from(address_value_pointer).ok()?;
    let receiver_channel_name_pointer = read_u16(data, 42)?;
    let receiver_address_offset = data.len().checked_sub(4)?;

    Some(DanteBrooklynControlProtocolFlowSetupRequest {
        transaction_identifier_hex: bytes_to_hex(data.get(4..8)?),
        receiver_device_name_pointer,
        sample_rate: read_u32(data, 12)?,
        encoding: read_u32(data, 16)?,
        transport_descriptor_pointer,
        transport_descriptor_count,
        address_value_pointer,
        flow_span_value: read_u16(data, 40)?,
        receiver_channel_name_pointer,
        receiver_device_name: string_at_pointer(data, receiver_device_name_pointer_u16)?,
        receiver_channel_name: string_at_pointer(data, receiver_channel_name_pointer)?,
        address_at_pointer: ipv4_at(data, address_value_offset)?,
        transport_descriptor_hex: bytes_to_hex(transport_descriptor),
        receiver_address: ipv4_at(data, receiver_address_offset)?,
        raw_payload_hex: bytes_to_hex(data),
    })
}

pub fn parse_dante_brooklyn_control_protocol_flow_setup_response(
    data: &[u8],
) -> Option<DanteBrooklynControlProtocolFlowSetupResponse> {
    if data.len() != 24
        || read_u16(data, 0)? != 0x1102
        || usize::from(read_u16(data, 2)?) != data.len()
    {
        return None;
    }

    Some(DanteBrooklynControlProtocolFlowSetupResponse {
        transaction_identifier_hex: bytes_to_hex(data.get(4..8)?),
        field_at_offset_8_hex: bytes_to_hex(data.get(8..12)?),
        flow_identifier: read_u32(data, 12)?,
        field_at_offset_16_hex: bytes_to_hex(data.get(16..20)?),
        field_at_offset_20_hex: bytes_to_hex(data.get(20..24)?),
        raw_payload_hex: bytes_to_hex(data),
    })
}
