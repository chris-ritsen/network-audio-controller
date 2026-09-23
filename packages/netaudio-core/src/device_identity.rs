use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DeviceIdentityRequest {
    Mac(Value),
    Ptpv1(Value),
    ManagedDevice(Value),
    ManagedInventory(Value),
    ManagedDomain(Value),
    ManagedPrimary(Value),
}

fn hexadecimal(value: &str, bytes: usize) -> Option<String> {
    (value.len() == bytes * 2 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| value.to_ascii_lowercase())
}

pub fn managed_device_id(value: &str) -> Option<String> {
    let value = value.strip_suffix(":0").unwrap_or(value);
    hexadecimal(value, 8)
}

pub fn normalize(request: DeviceIdentityRequest) -> Option<String> {
    match request {
        DeviceIdentityRequest::Mac(value) => canonical_device_mac(value.as_str()?),
        DeviceIdentityRequest::ManagedDevice(value) => managed_device_id(value.as_str()?),
        DeviceIdentityRequest::ManagedInventory(value)
        | DeviceIdentityRequest::ManagedDomain(value) => hexadecimal(value.as_str()?, 16),
        DeviceIdentityRequest::ManagedPrimary(value) => {
            let compact = value.as_str()?.replace([':', '-'], "");
            let mac = hexadecimal(&compact, 6)?;

            if mac == "000000000000" || u8::from_str_radix(&mac[..2], 16).ok()? & 1 != 0 {
                return None;
            }

            Some(format!("{}fffe{}", &mac[..6], &mac[6..]))
        }
        DeviceIdentityRequest::Ptpv1(value) => {
            if let Some(text) = value.as_str() {
                return hexadecimal(&text.replace([':', '-'], ""), 6);
            }

            let bytes = value.as_array()?;

            if bytes.len() != 6 {
                return None;
            }

            bytes
                .iter()
                .map(|value| {
                    let byte = u8::try_from(value.as_u64()?).ok()?;
                    Some(format!("{byte:02x}"))
                })
                .collect()
        }
    }
}

/// Canonicalize a MAC or Dante device identifier for identity comparisons.
/// Preserve unrecognized 64-bit identities rather than guessing a 48-bit address.
pub fn canonical_device_mac(value: &str) -> Option<String> {
    let mut digits: Vec<u8> = value
        .trim()
        .bytes()
        .filter(|byte| !matches!(byte, b':' | b'-' | b'.'))
        .map(|byte| byte.to_ascii_lowercase())
        .collect();

    if !matches!(digits.len(), 12 | 16) || !digits.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }

    if digits.len() == 16 {
        if &digits[6..10] == b"fffe" {
            digits.drain(6..10);
        } else if digits.ends_with(b"0000") {
            digits.truncate(12);
        }
    }

    if digits.iter().all(|byte| *byte == b'0') {
        return None;
    }

    String::from_utf8(digits).ok()
}
