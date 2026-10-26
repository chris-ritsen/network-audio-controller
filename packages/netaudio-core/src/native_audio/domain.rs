use serde::Serialize;

use super::source_control::Error;

pub const NAMED_DOMAIN_BYTES: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct NamedDomain([u8; NAMED_DOMAIN_BYTES]);

impl NamedDomain {
    pub const DEFAULT: Self = Self([0; NAMED_DOMAIN_BYTES]);

    pub fn from_text(value: &[u8]) -> Result<Self, Error> {
        if value.len() >= NAMED_DOMAIN_BYTES || value.contains(&0) {
            return Err(Error(
                "audio named domain exceeds its bounded non-NUL representation",
            ));
        }

        let mut bytes = [0; NAMED_DOMAIN_BYTES];
        bytes[..value.len()].copy_from_slice(value);
        Ok(Self(bytes))
    }

    pub fn bytes(&self) -> &[u8; NAMED_DOMAIN_BYTES] {
        &self.0
    }

    pub fn text(&self) -> &[u8] {
        let length = self
            .0
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(NAMED_DOMAIN_BYTES);
        &self.0[..length]
    }
}
