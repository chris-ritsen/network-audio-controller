use super::*;
use crate::network::{DanteRedundancyMode, NetworkInterface, StaticInterfaceConfiguration};

fn interface_selector(
    interface: NetworkInterface,
    record_protocol_identifier: Option<u16>,
) -> Result<u16, NetaudioError> {
    match (interface, record_protocol_identifier) {
        (NetworkInterface::Primary, None | Some(0x0724 | 0x0727 | 0x072e | 0x0738 | 0x073d)) => {
            Ok(0)
        }
        (NetworkInterface::Secondary, Some(0x073d)) => Ok(1),
        _ => Err(NetaudioError::UnsupportedProtocolOperation),
    }
}

fn build_interface_configuration(
    interface: NetworkInterface,
    record_protocol_identifier: Option<u16>,
    configuration: Option<StaticInterfaceConfiguration>,
    mac: [u8; 6],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if message_id == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let selector = interface_selector(interface, record_protocol_identifier)?;
    let mut body = Vec::new();
    body.extend_from_slice(&0x0013u16.to_be_bytes());
    body.extend_from_slice(&100u32.to_be_bytes());
    body.extend_from_slice(&[0x01, 0x1c]);
    body.extend_from_slice(
        &if configuration.is_some() {
            0x0f10u16
        } else {
            0x0010
        }
        .to_be_bytes(),
    );
    // These preceding words also affect network mode; targeting belongs in
    // the interface word immediately before the address-mode value.
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&selector.to_be_bytes());
    body.extend_from_slice(&if configuration.is_some() { 2u16 } else { 0 }.to_be_bytes());
    if let Some(configuration) = configuration {
        body.extend_from_slice(&configuration.ip_address);
        body.extend_from_slice(&configuration.netmask);
        body.extend_from_slice(&configuration.dns_server);
        body.extend_from_slice(&configuration.gateway);
    } else {
        body.extend_from_slice(&[0; 16]);
    }
    body.extend_from_slice(&[0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    settings_packet(message_id, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &body)
}

pub fn build_set_interface_dhcp(
    interface: NetworkInterface,
    record_protocol_identifier: Option<u16>,
    mac: [u8; 6],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_interface_configuration(interface, record_protocol_identifier, None, mac, message_id)
}

pub fn build_set_interface_static(
    configuration: StaticInterfaceConfiguration,
    interface: NetworkInterface,
    record_protocol_identifier: Option<u16>,
    mac: [u8; 6],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_interface_configuration(
        interface,
        record_protocol_identifier,
        Some(configuration),
        mac,
        message_id,
    )
}

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
        (0x0724 | 0x073d, DanteRedundancyMode::Switched) => (0x0013u16, 0u16, true),
        (0x0724 | 0x073d, DanteRedundancyMode::Redundant) => (0x0013, 1, true),
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
