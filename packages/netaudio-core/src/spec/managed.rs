use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::command_spec::CommandSpec;
use super::command_values::parse_mac_required;
use super::{build_command, parse_command_spec, SpecError, Target};
use crate::protocol::{NetaudioError, PROTOCOL_ARC_2809};

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ManagedCommandRequest {
    pub specification: Value,
    pub host_mac: Option<String>,
    pub message_id: u16,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagedCommand {
    pub transport: Target,
    pub packet: Vec<u8>,
    pub response_opcode: Option<u16>,
}

pub fn build_managed_command(request: ManagedCommandRequest) -> Result<ManagedCommand, SpecError> {
    let mut spec = parse_command_spec(&request.specification.to_string())?;

    match &mut spec {
        CommandSpec::ChannelCount { protocol_id, .. }
        | CommandSpec::DeviceInfo { protocol_id, .. }
        | CommandSpec::PropertyDirectory { protocol_id, .. }
        | CommandSpec::QueryReceiverPortRanges { protocol_id, .. }
        | CommandSpec::SetChannelName { protocol_id, .. }
        | CommandSpec::SetLatency { protocol_id, .. }
        | CommandSpec::TransmitterNames { protocol_id, .. } => {
            // This transport carries modern ARC. An explicit incompatible revision
            // is rejected rather than being rewritten to fit the transport.
            if request.specification.get("protocol_id").is_none() {
                *protocol_id = PROTOCOL_ARC_2809;
            }

            if *protocol_id != PROTOCOL_ARC_2809 {
                return Err(NetaudioError::UnsupportedProtocolOperation.into());
            }
        }
        _ => {}
    }

    if request.specification.get("message_id").is_none() {
        if let Some(message_id) = spec.message_id_mut() {
            *message_id = request.message_id;
        }
    }

    let (target, _) = spec.route();
    let response_opcode = match &spec {
        CommandSpec::ClearAllConfiguration { .. }
        | CommandSpec::ClearAllConfigurationPreservingInternetProtocolSettings { .. }
        | CommandSpec::ProbeClearConfigurationStatus { .. } => Some(0x0078),
        CommandSpec::DanteModel { .. } => Some(0x0060),
        CommandSpec::EnableAes67 { .. } | CommandSpec::ProbeAes67 { .. } => Some(0x1007),
        CommandSpec::MakeModel { .. } => Some(0x00C0),
        CommandSpec::ProbeEncoding { .. } | CommandSpec::SetEncoding { .. } => Some(0x0082),
        CommandSpec::ProbeCodecStatus { .. } | CommandSpec::SetGainLevel { .. } => Some(0x100B),
        CommandSpec::ProbeInterfaceStatus { .. }
        | CommandSpec::SetDanteRedundancy { .. }
        | CommandSpec::SetInterfaceDhcp { .. }
        | CommandSpec::SetInterfaceStatic { .. } => Some(0x0011),
        CommandSpec::ProbeInterfaceStatistics { .. } => Some(0x0040),
        CommandSpec::ProbeLockResetStatus { .. } => Some(0x1009),
        CommandSpec::ProbeSampleRate { .. } | CommandSpec::SetSampleRate { .. } => Some(0x0080),
        CommandSpec::ProbeSampleRatePullup { .. } | CommandSpec::SetSampleRatePullup { .. } => {
            Some(0x0084)
        }
        CommandSpec::ProbeSwitchConfiguration { .. } => Some(0x0014),
        CommandSpec::RefreshClockStatus { .. } => Some(0x0020),
        _ => None,
    };

    match target {
        Target::Arc => {}
        Target::Settings if response_opcode.is_some() => {}
        _ => return Err(NetaudioError::UnsupportedProtocolOperation.into()),
    }

    let host_mac = request
        .host_mac
        .as_deref()
        .map(parse_mac_required)
        .transpose()?;
    let packet = build_command(spec, host_mac)?;

    if target == Target::Arc && !packet.starts_with(&PROTOCOL_ARC_2809.to_be_bytes()) {
        return Err(NetaudioError::UnsupportedProtocolOperation.into());
    }

    Ok(ManagedCommand {
        transport: target,
        packet,
        response_opcode,
    })
}
