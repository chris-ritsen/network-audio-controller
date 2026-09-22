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
