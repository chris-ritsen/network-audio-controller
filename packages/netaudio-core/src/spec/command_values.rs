use super::*;

pub(super) fn parse_mac(
    value: &Option<String>,
    default: Option<[u8; 6]>,
) -> Result<[u8; 6], SpecError> {
    match value {
        None => default.ok_or(SpecError::InvalidMac),
        Some(text) => parse_mac_required(text),
    }
}

pub(super) fn parse_mac_required(text: &str) -> Result<[u8; 6], SpecError> {
    let cleaned: Vec<u8> = text
        .bytes()
        .filter(|character| *character != b':')
        .collect();
    if cleaned.len() != 12 || !cleaned.iter().all(u8::is_ascii_hexdigit) {
        return Err(SpecError::InvalidMac);
    }
    let mut mac = [0u8; 6];
    for (destination, pair) in mac.iter_mut().zip(cleaned.chunks_exact(2)) {
        let pair = std::str::from_utf8(pair).map_err(|_| SpecError::InvalidMac)?;
        *destination = u8::from_str_radix(pair, 16).map_err(|_| SpecError::InvalidMac)?;
    }
    Ok(mac)
}

/// A target device ID: a 12-digit MAC (process 0000) or the full 16-digit device ID it publishes.
pub(super) fn parse_device_id_required(text: &str) -> Result<[u8; 8], SpecError> {
    let mut device_id = [0u8; 8];
    let cleaned: Vec<u8> = text
        .bytes()
        .filter(|character| *character != b':')
        .collect();
    if cleaned.len() == 12 {
        device_id[..6].copy_from_slice(&parse_mac_required(text)?);
        return Ok(device_id);
    }
    if cleaned.len() != 16 || !cleaned.iter().all(u8::is_ascii_hexdigit) {
        return Err(SpecError::InvalidMac);
    }
    for (destination, pair) in device_id.iter_mut().zip(cleaned.chunks_exact(2)) {
        let pair = std::str::from_utf8(pair).map_err(|_| SpecError::InvalidMac)?;
        *destination = u8::from_str_radix(pair, 16).map_err(|_| SpecError::InvalidMac)?;
    }
    Ok(device_id)
}

pub(super) fn parse_required_ipv4_address(text: &str) -> Result<[u8; 4], SpecError> {
    if text.is_empty() {
        return Err(SpecError::InvalidIp);
    }
    text.parse::<Ipv4Addr>()
        .map(|address| address.octets())
        .map_err(|_| SpecError::InvalidIp)
}

pub(super) fn parse_optional_ipv4_address(text: &str) -> Result<[u8; 4], SpecError> {
    if text.is_empty() {
        return Ok([0u8; 4]);
    }
    parse_required_ipv4_address(text)
}

pub(super) fn parse_channel_type(text: &str) -> Result<ChannelType, SpecError> {
    match text {
        "rx" => Ok(ChannelType::Rx),
        "tx" => Ok(ChannelType::Tx),
        _ => Err(SpecError::InvalidChannelType),
    }
}

pub(crate) fn parse_gain_device_type(text: &str) -> Result<bool, SpecError> {
    match text {
        "input" => Ok(true),
        "output" => Ok(false),
        _ => Err(SpecError::InvalidDeviceType),
    }
}
