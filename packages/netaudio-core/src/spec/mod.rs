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

fn route(command: &str) -> (Target, IoMode) {
    use IoMode::*;
    use Target::*;
    match command {
        "set_encoding" | "set_sample_rate" | "set_sample_rate_pullup" | "set_gain_level" => (
            Settings,
            Fire {
                repeat: 1,
                interval_ms: 0,
            },
        ),
        "reboot" | "factory_reset" => (
            Settings,
            Fire {
                repeat: 1,
                interval_ms: 0,
            },
        ),
        "enable_aes67" => (
            Settings,
            Fire {
                repeat: 3,
                interval_ms: 100,
            },
        ),
        "set_preferred_leader" | "set_clock_source" | "set_clock_subdomain" => (
            Settings,
            Fire {
                repeat: 3,
                interval_ms: 500,
            },
        ),
        "identify"
        | "probe_interface_status"
        | "probe_link_status"
        | "probe_switch_configuration"
        | "probe_sample_rate"
        | "probe_encoding"
        | "probe_sample_rate_pullup"
        | "probe_gain_level"
        | "set_interface_dhcp"
        | "set_interface_static"
        | "probe_aes67"
        | "probe_lock_reset_status"
        | "probe_clear_configuration_status"
        | "clear_all_configuration"
        | "clear_all_configuration_preserving_internet_protocol_settings"
        | "probe_preferred_leader"
        | "refresh_clock_status"
        | "bluetooth_status"
        | "make_model"
        | "dante_model" => (
            Settings,
            Fire {
                repeat: 1,
                interval_ms: 0,
            },
        ),
        "cmc_register" => (Control, Request),
        "volume_start" | "volume_stop" | "metering_start" | "metering_stop" => (
            Control,
            Fire {
                repeat: 1,
                interval_ms: 0,
            },
        ),
        _ => (Arc, StampedRequest),
    }
}

fn command_name(json: &str) -> Result<String, SpecError> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|_| SpecError::InvalidJson)?;
    value
        .get("command")
        .and_then(|name| name.as_str())
        .map(str::to_owned)
        .ok_or(SpecError::InvalidJson)
}

pub fn build_routed_command(json: &str, default_host_mac: [u8; 6]) -> Result<Routed, SpecError> {
    let name = command_name(json)?;
    let packet = build_command_with_default_host_mac(json, default_host_mac)?;
    let (target, io) = route(&name);
    Ok(Routed { packet, target, io })
}

pub fn build_command_from_json(json: &str) -> Result<Vec<u8>, SpecError> {
    build_command_with_default_host_mac(json, [0u8; 6])
}

mod command_spec;

use command_spec::build_command_with_default_host_mac;

#[cfg(test)]
mod tests;
