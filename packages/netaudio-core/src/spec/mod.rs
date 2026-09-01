use std::net::Ipv4Addr;

use serde::Deserialize;

use crate::commands::{self, ChannelType, ReceiveChannelNamePageRecord, SubscriptionPageRecord};
use crate::protocol::NetaudioError;

mod command_defaults;
mod command_values;

#[derive(Debug)]
pub enum SpecError {
    Protocol(NetaudioError),
    InvalidJson,
    InvalidMac,
    InvalidIp,
    InvalidChannelType,
    InvalidDeviceType,
}

impl From<NetaudioError> for SpecError {
    fn from(error: NetaudioError) -> Self {
        SpecError::Protocol(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Arc,
    Settings,
    Control,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    StampedRequest,
    Request,
    Fire { repeat: u32, interval_ms: u64 },
}

#[derive(Debug, Clone)]
pub struct Routed {
    pub packet: Vec<u8>,
    pub target: Target,
    pub io: IoMode,
}

pub fn build_routed_command(json: &str, default_host_mac: [u8; 6]) -> Result<Routed, SpecError> {
    let spec = parse_command_spec(json)?;
    let (target, io) = spec.route();
    let packet = build_command(spec, default_host_mac)?;
    Ok(Routed { packet, target, io })
}

pub fn build_command_from_json(json: &str) -> Result<Vec<u8>, SpecError> {
    build_command(parse_command_spec(json)?, [0u8; 6])
}

mod command_spec;

use command_spec::{build_command, parse_command_spec};

#[cfg(test)]
mod tests;
