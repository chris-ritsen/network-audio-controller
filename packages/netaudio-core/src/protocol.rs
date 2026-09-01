pub const PROTOCOL_ID: u16 = 0x27FF;
pub const PROTOCOL_ARC_2809: u16 = 0x2809;
pub const PROTOCOL_ARC_280F: u16 = 0x280F;
pub const OPCODE_CHANNEL_COUNT: u16 = 0x1000;
pub const OPCODE_DEVICE_NAME_SET: u16 = 0x1001;
pub const OPCODE_TX_CHANNEL_INFO: u16 = 0x2000;
pub const OPCODE_TX_CHANNEL_NAMES: u16 = 0x2010;
pub const OPCODE_RX_CHANNELS: u16 = 0x3000;
pub const SERVICE_ARC: &str = "_netaudio-arc._udp.local.";
pub const DANTE_NAME_MAX_LENGTH: usize = 31;
pub const RESPONSE_HEADER_SIZE: usize = 10;
pub const RESULT_CODE_SUCCESS: u16 = 0x0001;
pub const RESULT_CODE_MORE_PAGES: u16 = 0x8112;
pub const COMMON_ARC_PROTOCOL_IDS: [u16; 3] = [PROTOCOL_ID, 0x2729, PROTOCOL_ARC_2809];
pub const DEVICE_SETTINGS_ARC_PROTOCOL_IDS: [u16; 4] =
    [PROTOCOL_ID, 0x2729, 0x2801, PROTOCOL_ARC_2809];
pub const MODERN_ARC_PROTOCOL_IDS: [u16; 2] = [PROTOCOL_ARC_2809, PROTOCOL_ARC_280F];

const PROTOCOL_SETTINGS: u16 = 0xFFFF;
const CONMON_MINIMUM_SIZE: usize = 28;
const CONMON_MAGIC_OFFSET: usize = 16;
const CONMON_FAMILY_OFFSET: usize = 24;
const CONMON_OPCODE_OFFSET: usize = 26;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseEnvelope<'a> {
    pub protocol_id: u16,
    pub transaction_id: u16,
    pub opcode: u16,
    pub result_code: u16,
    pub body: &'a [u8],
}

pub fn response_envelope(response: &[u8]) -> Option<ResponseEnvelope<'_>> {
    if response.len() < RESPONSE_HEADER_SIZE
        || usize::from(crate::bytes::read_u16(response, 2)?) != response.len()
    {
        return None;
    }
    Some(ResponseEnvelope {
        protocol_id: crate::bytes::read_u16(response, 0)?,
        transaction_id: crate::bytes::read_u16(response, 4)?,
        opcode: crate::bytes::read_u16(response, 6)?,
        result_code: crate::bytes::read_u16(response, 8)?,
        body: &response[RESPONSE_HEADER_SIZE..],
    })
}

pub fn validate_response_envelope<'a>(
    response: &'a [u8],
    expected_protocol_opcodes: &[(u16, u16)],
    accepted_results: &[u16],
) -> Option<ResponseEnvelope<'a>> {
    let envelope = response_envelope(response)?;
    if !expected_protocol_opcodes.contains(&(envelope.protocol_id, envelope.opcode))
        || !accepted_results.contains(&envelope.result_code)
    {
        return None;
    }
    Some(envelope)
}

pub fn common_arc_protocol_opcodes(opcode: u16) -> [(u16, u16); 3] {
    COMMON_ARC_PROTOCOL_IDS.map(|protocol_id| (protocol_id, opcode))
}

pub fn device_settings_arc_protocol_opcodes(opcode: u16) -> [(u16, u16); 4] {
    DEVICE_SETTINGS_ARC_PROTOCOL_IDS.map(|protocol_id| (protocol_id, opcode))
}

pub fn modern_arc_protocol_opcodes(opcode: u16) -> [(u16, u16); 2] {
    MODERN_ARC_PROTOCOL_IDS.map(|protocol_id| (protocol_id, opcode))
}

pub fn is_modern_arc_protocol(protocol_id: u16) -> bool {
    MODERN_ARC_PROTOCOL_IDS.contains(&protocol_id)
}

pub fn is_common_arc_protocol(protocol_id: u16) -> bool {
    COMMON_ARC_PROTOCOL_IDS.contains(&protocol_id)
}

pub fn conmon_opcode(data: &[u8]) -> Option<u16> {
    if data.len() < CONMON_MINIMUM_SIZE
        || crate::bytes::read_u16(data, 0)? != PROTOCOL_SETTINGS
        || usize::from(crate::bytes::read_u16(data, 2)?) != data.len()
        || crate::bytes::read_u16(data, 6)? != 0
        || data.get(CONMON_MAGIC_OFFSET..CONMON_MAGIC_OFFSET + 8)? != b"Audinate"
        || data.get(CONMON_FAMILY_OFFSET).copied()? != 0x07
    {
        return None;
    }
    crate::bytes::read_u16(data, CONMON_OPCODE_OFFSET)
}

pub fn validate_conmon_envelope(data: &[u8], expected_opcode: u16) -> Option<()> {
    (conmon_opcode(data)? == expected_opcode).then_some(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetaudioError {
    NameTooLong,
    NameInvalidHyphen,
    NameInvalidChars,
    SubscriptionCount,
    InvalidPage,
    InvalidSubscriptionChannel,
    PacketTooLarge,
    InvalidChannel,
    InvalidLatency,
    InvalidSampleRate,
    InvalidEncoding,
    InvalidGainLevel,
    InvalidFlowSlot,
    InvalidFlowProtocol,
    InvalidSequence,
    UnsupportedProtocolOperation,
}

fn validate_dante_name_with_character_policy(
    name: &str,
    allow_colon: bool,
    allow_underscore: bool,
) -> Result<(), NetaudioError> {
    if name.chars().count() > DANTE_NAME_MAX_LENGTH {
        return Err(NetaudioError::NameTooLong);
    }

    let valid_pattern = !name.is_empty()
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || character == '-'
                || (allow_colon && character == ':')
                || (allow_underscore && character == '_')
        })
        && !name.starts_with('-')
        && !name.ends_with('-')
        && (!allow_colon || (!name.starts_with(':') && !name.ends_with(':')));

    if valid_pattern {
        return Ok(());
    }

    if name.starts_with('-') || name.ends_with('-') || name.starts_with(':') || name.ends_with(':')
    {
        return Err(NetaudioError::NameInvalidHyphen);
    }

    Err(NetaudioError::NameInvalidChars)
}

pub fn validate_dante_name(name: &str) -> Result<(), NetaudioError> {
    validate_dante_name_with_character_policy(name, false, false)
}

pub fn validate_dante_channel_name(name: &str) -> Result<(), NetaudioError> {
    validate_dante_name_with_character_policy(name, true, true)
}

pub fn validate_dante_channel_reference(name: &str) -> Result<(), NetaudioError> {
    if name.chars().count() > DANTE_NAME_MAX_LENGTH {
        return Err(NetaudioError::NameTooLong);
    }
    if name.is_empty()
        || !name
            .chars()
            .all(|character| character.is_ascii_graphic() || character == ' ')
    {
        return Err(NetaudioError::NameInvalidChars);
    }
    Ok(())
}

pub fn build_control_packet(
    opcode: u16,
    payload: &[u8],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet_for_protocol(PROTOCOL_ID, opcode, payload, transaction_id)
}

pub fn build_control_packet_for_protocol(
    protocol_id: u16,
    opcode: u16,
    payload: &[u8],
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let length = 8usize
        .checked_add(payload.len())
        .ok_or(NetaudioError::PacketTooLarge)?;
    let encoded_length = u16::try_from(length).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut packet = Vec::with_capacity(length);
    packet.extend_from_slice(&protocol_id.to_be_bytes());
    packet.extend_from_slice(&encoded_length.to_be_bytes());
    packet.extend_from_slice(&transaction_id.to_be_bytes());
    packet.extend_from_slice(&opcode.to_be_bytes());
    packet.extend_from_slice(payload);
    Ok(packet)
}

pub fn build_set_device_name(name: &str, transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    validate_dante_name(name)?;

    let name_bytes = name.as_bytes();
    let mut payload = Vec::with_capacity(2 + name_bytes.len() + 1);
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(name_bytes);
    payload.push(0);

    build_control_packet_for_protocol(
        PROTOCOL_ARC_2809,
        OPCODE_DEVICE_NAME_SET,
        &payload,
        transaction_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_device_name_known_bytes() {
        let packet = build_set_device_name("AVIO", 0).unwrap();
        let expected = [
            0x28, 0x09, 0x00, 0x0F, 0x00, 0x00, 0x10, 0x01, 0x00, 0x00, 0x41, 0x56, 0x49, 0x4F,
            0x00,
        ];
        assert_eq!(packet, expected);
    }

    #[test]
    fn set_device_name_transaction_id() {
        let packet = build_set_device_name("AVIO", 0xBEEF).unwrap();
        assert_eq!(&packet[4..6], &[0xBE, 0xEF]);
        assert_eq!(&packet[0..4], &[0x28, 0x09, 0x00, 0x0F]);
    }

    #[test]
    fn set_device_name_matches_controller_request() {
        let packet = build_set_device_name("avio-bt-11", 0x261B).unwrap();
        let expected = [
            0x28, 0x09, 0x00, 0x15, 0x26, 0x1B, 0x10, 0x01, 0x00, 0x00, 0x61, 0x76, 0x69, 0x6F,
            0x2D, 0x62, 0x74, 0x2D, 0x31, 0x31, 0x00,
        ];
        assert_eq!(packet, expected);
    }

    #[test]
    fn set_device_name_length_field_matches_packet_length() {
        let packet = build_set_device_name("Studio-AVIO", 0).unwrap();
        let length = u16::from_be_bytes([packet[2], packet[3]]) as usize;
        assert_eq!(length, packet.len());
    }

    #[test]
    fn control_packet_rejects_declared_length_overflow() {
        assert_eq!(
            build_control_packet(0x1000, &vec![0; 65_528], 0),
            Err(NetaudioError::PacketTooLarge)
        );

        let maximum = build_control_packet(0x1000, &vec![0; 65_527], 0).unwrap();
        assert_eq!(maximum.len(), u16::MAX as usize);
        assert_eq!(&maximum[2..4], &u16::MAX.to_be_bytes());
    }

    #[test]
    fn validate_accepts_valid_names() {
        for name in ["a", "A1", "Studio-AVIO", "x-1-y", "Z", &"a".repeat(31)] {
            assert_eq!(validate_dante_name(name), Ok(()), "{name}");
        }
    }

    #[test]
    fn validate_channel_name_accepts_controller_labels() {
        for name in [
            "windows-gaming:left",
            "main-mix:right",
            "shelford-channel:0dB",
            "system:capture_13",
        ] {
            assert_eq!(validate_dante_channel_name(name), Ok(()), "{name}");
        }
    }

    #[test]
    fn validate_channel_reference_accepts_firmware_reported_labels() {
        for name in ["Output 01", "Main Mix Left", "windows-gaming:left"] {
            assert_eq!(validate_dante_channel_reference(name), Ok(()), "{name}");
        }
    }

    #[test]
    fn validate_channel_reference_rejects_non_wire_characters() {
        for name in ["", "tx\0a", "tx\na", "über"] {
            assert_eq!(
                validate_dante_channel_reference(name),
                Err(NetaudioError::NameInvalidChars),
                "{name:?}"
            );
        }
    }

    #[test]
    fn validate_rejects_long_names() {
        assert_eq!(
            validate_dante_name(&"a".repeat(32)),
            Err(NetaudioError::NameTooLong)
        );
    }

    #[test]
    fn validate_rejects_hyphen_at_edges() {
        assert_eq!(
            validate_dante_name("-foo"),
            Err(NetaudioError::NameInvalidHyphen)
        );
        assert_eq!(
            validate_dante_name("foo-"),
            Err(NetaudioError::NameInvalidHyphen)
        );
    }

    #[test]
    fn validate_rejects_invalid_characters() {
        for name in ["", "foo_bar", "foo bar", "über", "foo.bar"] {
            assert_eq!(
                validate_dante_name(name),
                Err(NetaudioError::NameInvalidChars),
                "{name}"
            );
        }
    }

    #[test]
    fn validate_device_name_rejects_colon_labels() {
        assert_eq!(
            validate_dante_name("windows-gaming:left"),
            Err(NetaudioError::NameInvalidChars)
        );
    }
}
