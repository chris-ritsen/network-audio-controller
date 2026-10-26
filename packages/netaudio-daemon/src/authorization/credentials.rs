use super::{AuthorizationError, READ_SCOPE, SCOPES, WRITE_SCOPE};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use url::Url;

pub const SECRET_SALT_BYTES: usize = 16;
pub const SECRET_DIGEST_BYTES: usize = 32;
pub const SECRET_ITERATIONS: u32 = 600_000;
pub const TOKEN_BYTES: usize = 32;
pub const CLIENT_IDENTIFIER_BYTES: usize = 16;

pub fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    bool::from(left.ct_eq(right))
}

pub fn token(length: usize) -> String {
    let mut bytes = vec![0; length];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn digest(value: &str) -> String {
    hexadecimal(&Sha256::digest(value.as_bytes()))
}

pub fn hexadecimal(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(value.len() * 2);

    for byte in value {
        result.push(char::from(DIGITS[usize::from(byte >> 4)]));
        result.push(char::from(DIGITS[usize::from(byte & 15)]));
    }

    result
}

pub fn decode_hexadecimal(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) || !value.is_ascii() {
        return None;
    }

    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = char::from(pair[0]).to_digit(16)?;
            let low = char::from(pair[1]).to_digit(16)?;
            u8::try_from((high << 4) | low).ok()
        })
        .collect()
}

pub fn secret_digest(secret: &str, salt: &[u8]) -> [u8; SECRET_DIGEST_BYTES] {
    pbkdf2::pbkdf2_hmac_array::<Sha256, SECRET_DIGEST_BYTES>(
        secret.as_bytes(),
        salt,
        SECRET_ITERATIONS,
    )
}

pub fn secret_record(secret: &str) -> Result<serde_json::Value, AuthorizationError> {
    if secret.chars().count() < 12 {
        return Err(AuthorizationError::new(
            "invalid_request",
            "The login secret must be at least 12 characters",
        ));
    }

    let mut salt = [0; SECRET_SALT_BYTES];
    OsRng.fill_bytes(&mut salt);
    Ok(serde_json::json!({
        "salt": hexadecimal(&salt),
        "hash": hexadecimal(&secret_digest(secret, &salt)),
    }))
}

pub fn verify_challenge(verifier: &str, challenge: &str) -> bool {
    if !(43..=128).contains(&verifier.len())
        || !verifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte))
    {
        return false;
    }

    let computed = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    constant_time_equal(computed.as_bytes(), challenge.as_bytes())
}

pub fn valid_redirect_uri(uri: &str) -> bool {
    let Ok(parsed) = Url::parse(uri) else {
        return false;
    };

    if parsed.fragment().is_some()
        || parsed.host().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return false;
    }

    parsed.scheme() == "https"
        || (parsed.scheme() == "http"
            && matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "[::1]")))
}

pub fn normalize_scope(requested: Option<&str>) -> Result<String, AuthorizationError> {
    let Some(requested) = requested.filter(|requested| !requested.is_empty()) else {
        return Ok(WRITE_SCOPE.to_owned());
    };
    let scopes: Vec<&str> = requested
        .split_whitespace()
        .filter(|scope| SCOPES.contains(scope))
        .collect();

    if scopes.is_empty() {
        return Err(AuthorizationError::new(
            "invalid_scope",
            format!("scope must be one of {}", SCOPES.join(", ")),
        ));
    }

    Ok(if scopes.contains(&WRITE_SCOPE) {
        WRITE_SCOPE
    } else {
        READ_SCOPE
    }
    .to_owned())
}
