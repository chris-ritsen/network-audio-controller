pub const PROTOCOL_ID: u16 = 0x27FF;
pub const OPCODE_CHANNEL_COUNT: u16 = 0x1000;
pub const OPCODE_DEVICE_NAME_SET: u16 = 0x1001;
pub const OPCODE_TX_CHANNEL_INFO: u16 = 0x2000;
pub const OPCODE_TX_CHANNEL_NAMES: u16 = 0x2010;
pub const OPCODE_RX_CHANNELS: u16 = 0x3000;
pub const SERVICE_ARC: &str = "_netaudio-arc._udp.local.";
pub const DANTE_NAME_MAX_LENGTH: usize = 31;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetaudioError {
    NameTooLong,
    NameInvalidHyphen,
    NameInvalidChars,
    SubscriptionCount,
}

fn validate_dante_name_with_colon_policy(name: &str, allow_colon: bool) -> Result<(), NetaudioError> {
    if name.chars().count() > DANTE_NAME_MAX_LENGTH {
        return Err(NetaudioError::NameTooLong);
    }

    let valid_pattern = !name.is_empty()
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || character == '-'
                || (allow_colon && character == ':')
        })
        && !name.starts_with('-')
        && !name.ends_with('-')
        && (!allow_colon || (!name.starts_with(':') && !name.ends_with(':')));

    if valid_pattern {
        return Ok(());
    }

    if name.starts_with('-') || name.ends_with('-') || name.starts_with(':') || name.ends_with(':') {
        return Err(NetaudioError::NameInvalidHyphen);
    }

    Err(NetaudioError::NameInvalidChars)
}

pub fn validate_dante_name(name: &str) -> Result<(), NetaudioError> {
    validate_dante_name_with_colon_policy(name, false)
}

pub fn validate_dante_channel_name(name: &str) -> Result<(), NetaudioError> {
    validate_dante_name_with_colon_policy(name, true)
}

pub fn build_control_packet(opcode: u16, payload: &[u8], transaction_id: u16) -> Vec<u8> {
    let length = 8 + payload.len();
    let mut packet = Vec::with_capacity(length);
    packet.extend_from_slice(&PROTOCOL_ID.to_be_bytes());
    packet.extend_from_slice(&(length as u16).to_be_bytes());
    packet.extend_from_slice(&transaction_id.to_be_bytes());
    packet.extend_from_slice(&opcode.to_be_bytes());
    packet.extend_from_slice(payload);
    packet
}

pub fn build_set_device_name(name: &str, transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    validate_dante_name(name)?;

    let name_bytes = name.as_bytes();
    let mut payload = Vec::with_capacity(2 + name_bytes.len() + 1);
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(name_bytes);
    payload.push(0);

    Ok(build_control_packet(OPCODE_DEVICE_NAME_SET, &payload, transaction_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_device_name_known_bytes() {
        let packet = build_set_device_name("AVIO", 0).unwrap();
        let expected = [
            0x27, 0xFF, 0x00, 0x0F, 0x00, 0x00, 0x10, 0x01, 0x00, 0x00, 0x41, 0x56, 0x49,
            0x4F, 0x00,
        ];
        assert_eq!(packet, expected);
    }

    #[test]
    fn set_device_name_transaction_id() {
        let packet = build_set_device_name("AVIO", 0xBEEF).unwrap();
        assert_eq!(&packet[4..6], &[0xBE, 0xEF]);
        assert_eq!(&packet[0..4], &[0x27, 0xFF, 0x00, 0x0F]);
    }

    #[test]
    fn set_device_name_length_field_matches_packet_length() {
        let packet = build_set_device_name("Studio-AVIO", 0).unwrap();
        let length = u16::from_be_bytes([packet[2], packet[3]]) as usize;
        assert_eq!(length, packet.len());
    }

    #[test]
    fn validate_accepts_valid_names() {
        for name in ["a", "A1", "Studio-AVIO", "x-1-y", "Z", &"a".repeat(31)] {
            assert_eq!(validate_dante_name(name), Ok(()), "{name}");
        }
    }

    #[test]
    fn validate_channel_name_accepts_colon_labels() {
        for name in ["windows-gaming:left", "main-mix:right", "shelford-channel:0dB"] {
            assert_eq!(validate_dante_channel_name(name), Ok(()), "{name}");
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
        assert_eq!(validate_dante_name("-foo"), Err(NetaudioError::NameInvalidHyphen));
        assert_eq!(validate_dante_name("foo-"), Err(NetaudioError::NameInvalidHyphen));
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
