mod control;
mod device;
mod wire;

pub use control::{Change, described, setting_key};
pub use device::Device;
pub use wire::{Decoder, Message, Protocol, Report, command, indexed, meter_value, sampled};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

fn invalid(description: impl Into<String>) -> Error {
    Error(description.into())
}
