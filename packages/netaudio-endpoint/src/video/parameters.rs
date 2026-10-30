use anyhow::Result;
pub use h264_parameters::Format;
use netaudio_core::video::{h264::AccessUnit, h264_parameters};

use super::configuration::{Configuration, decode_hex};

pub struct Parameters(h264_parameters::Parameters);

impl Parameters {
    pub fn new(configuration: &Configuration) -> Result<Self> {
        let sequence = decode_hex(&configuration.sequence_parameters_hex)?;
        let picture = decode_hex(&configuration.picture_parameters_hex)?;
        Self::from_bytes(
            sequence,
            picture,
            Format {
                width: configuration.width,
                height: configuration.height,
                profile_identifier: configuration.profile_identifier,
                level_identifier: configuration.level_identifier,
                maximum_reference_frames: configuration.limits.maximum_reference_frames,
            },
        )
    }

    pub fn from_bytes(sequence: Vec<u8>, picture: Vec<u8>, format: Format) -> Result<Self> {
        h264_parameters::Parameters::from_bytes(sequence, picture, format).map(Self)
    }

    pub fn validate(&self, access_unit: &AccessUnit) -> Result<()> {
        self.0.validate(access_unit)
    }

    pub fn prefix(&self) -> Vec<u8> {
        self.0.prefix()
    }
}
