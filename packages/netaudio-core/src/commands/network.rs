use super::*;
use crate::network::DanteRedundancyMode;

pub fn build_set_dante_redundancy(
    record_protocol_identifier: u16,
    mode: DanteRedundancyMode,
    mac: [u8; 6],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if message_id == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let (operation, value, interface_word) = match (record_protocol_identifier, mode) {
        (0x0724, DanteRedundancyMode::Switched) => (0x0013u16, 0u16, true),
        (0x0724, DanteRedundancyMode::Redundant) => (0x0013, 1, true),
        (0x072e, DanteRedundancyMode::Switched) => (0x0015, 1, false),
        (0x072e, DanteRedundancyMode::SplitRedundant) => (0x0015, 2, false),
        _ => return Err(NetaudioError::UnsupportedProtocolOperation),
    };
    let mut body = Vec::new();
    body.extend_from_slice(&operation.to_be_bytes());
    body.extend_from_slice(&100u32.to_be_bytes());
    if interface_word {
        body.extend_from_slice(&0u32.to_be_bytes());
    }
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&value.to_be_bytes());
    settings_packet(message_id, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &body)
}
