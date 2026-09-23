use std::net::Ipv4Addr;

use serde::Deserialize;

use crate::commands::{self, ChannelType, ReceiveChannelNamePageRecord, SubscriptionPageRecord};
use crate::protocol::NetaudioError;

mod command_defaults;
mod command_values;
pub(crate) use command_values::parse_gain_device_type;
mod external_subscription;
mod managed;
mod subscription_plan;

pub use command_spec::plan_performance_command;
pub(crate) use command_spec::SubscriptionPageEntry;
pub use external_subscription::{
    external_subscription_readback, plan_external_subscription, ExternalReadbackRequest,
    ExternalSubscriptionCommand, ExternalSubscriptionPlanRequest, ExternalSubscriptionReadback,
};
use external_subscription::{ExternalRtpDestinationSpec, ExternalSubscriptionSpec};
pub use managed::{build_managed_command, ManagedCommand, ManagedCommandRequest};
pub use subscription_plan::{plan_subscription_commands, Receiver};
pub(crate) use subscription_plan::{plan_subscription_request, SubscriptionPlanRequest};

#[derive(Debug)]
pub enum SpecError {
    InvalidChannelType,
    InvalidDeviceType,
    InvalidIp,
    InvalidJson(String),
    InvalidMac,
    Protocol(NetaudioError),
}

impl std::fmt::Display for SpecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpecError::InvalidChannelType => {
                formatter.write_str("channel_type must be \"rx\" or \"tx\"")
            }
            SpecError::InvalidDeviceType => {
                formatter.write_str("device_type must be \"input\" or \"output\"")
            }
            SpecError::InvalidIp => formatter.write_str("value is not a dotted IPv4 address"),
            SpecError::InvalidJson(message) => write!(formatter, "invalid command json: {message}"),
            SpecError::InvalidMac => formatter
                .write_str("host_mac must be twelve hexadecimal digits with optional colons"),
            SpecError::Protocol(error) => write!(formatter, "{error}"),
        }
    }
}

impl From<NetaudioError> for SpecError {
    fn from(error: NetaudioError) -> Self {
        SpecError::Protocol(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Target {
    Arc,
    Control,
    Settings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    Fire { repeat: u32, interval_ms: u64 },
    Request,
}

#[derive(Debug, Clone)]
pub struct Routed {
    pub io: IoMode,
    pub message_id: u16,
    pub packet: Vec<u8>,
    pub target: Target,
}

pub fn build_routed_command(
    json: &str,
    default_host_mac: Option<[u8; 6]>,
    assign_message_id: impl FnOnce() -> u16,
) -> Result<Routed, SpecError> {
    let mut spec = parse_command_spec(json)?;
    let message_id = if let Some(message_id) = spec.message_id_mut() {
        if *message_id == 0 {
            *message_id = assign_message_id();
        }

        *message_id
    } else {
        0
    };

    let (target, io) = spec.route();
    let packet = build_command(spec, default_host_mac)?;
    Ok(Routed {
        io,
        message_id,
        packet,
        target,
    })
}

pub fn build_command_from_json(json: &str) -> Result<Vec<u8>, SpecError> {
    build_command(parse_command_spec(json)?, None)
}

mod command_spec;

use command_spec::{build_command, parse_command_spec};

#[cfg(test)]
mod tests;
