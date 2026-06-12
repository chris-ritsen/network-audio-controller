use std::net::Ipv4Addr;

use serde::Deserialize;

use crate::commands::{self, ChannelType};
use crate::protocol::NetaudioError;

#[derive(Debug)]
pub enum SpecError {
    Protocol(NetaudioError),
    InvalidJson,
    InvalidMac,
    InvalidIp,
    InvalidChannelType,
}

impl From<NetaudioError> for SpecError {
    fn from(error: NetaudioError) -> Self {
        SpecError::Protocol(error)
    }
}

#[derive(Debug, Deserialize)]
struct Subscription {
    rx_channel: u16,
    tx_channel: String,
    tx_device: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum CommandSpec {
    DeviceInfo { #[serde(default)] transaction_id: u16 },
    DeviceName { #[serde(default)] transaction_id: u16 },
    ChannelCount { #[serde(default)] transaction_id: u16 },
    DeviceSettings { #[serde(default)] transaction_id: u16 },
    SetName { name: String, #[serde(default)] transaction_id: u16 },
    ResetName { #[serde(default)] transaction_id: u16 },
    Receivers { page: u16, #[serde(default)] transaction_id: u16 },
    Transmitters { page: u16, #[serde(default)] friendly_names: bool, #[serde(default)] transaction_id: u16 },
    ResetChannelName { channel_type: String, channel_number: u8, #[serde(default)] transaction_id: u16 },
    SetChannelName { channel_type: String, channel_number: u8, name: String, #[serde(default)] transaction_id: u16 },
    AddSubscriptions { subscriptions: Vec<Subscription>, #[serde(default)] transaction_id: u16 },
    RemoveSubscriptions { rx_channels: Vec<u32>, #[serde(default)] transaction_id: u16 },
    SetLatency { latency: f64, #[serde(default)] transaction_id: u16 },
    Reboot { #[serde(default)] host_mac: Option<String> },
    Identify {},
    SetEncoding { encoding: u8 },
    SetSampleRate { sample_rate: u32 },
    SetGainLevel { channel_number: u8, gain_level: u8, device_type: String },
    EnableAes67 { enabled: bool, #[serde(default)] host_mac: Option<String> },
    ProbeInterfaceStatus { #[serde(default)] host_mac: Option<String> },
    SetInterfaceDhcp { #[serde(default)] host_mac: Option<String> },
    SetInterfaceStatic {
        ip: String,
        netmask: String,
        #[serde(default)] dns: String,
        #[serde(default)] gateway: String,
        #[serde(default)] host_mac: Option<String>,
    },
    ProbeAes67 { #[serde(default)] host_mac: Option<String>, #[serde(default = "default_aes67_sequence")] sequence: u16 },
    SetPreferredLeader {
        preferred: bool,
        #[serde(default)] clock_source: u16,
        #[serde(default)] host_mac: Option<String>,
        #[serde(default = "default_leader_sequence")] sequence: u16,
    },
    ProbePreferredLeader {
        #[serde(default)] clock_source: u16,
        #[serde(default)] host_mac: Option<String>,
        #[serde(default = "default_leader_sequence")] sequence: u16,
    },
    QueryLatencyConfig { #[serde(default)] transaction_id: u16 },
    VolumeStart {
        device_name: String,
        #[serde(default)] ipv4: String,
        mac: String,
        port: u16,
        #[serde(default = "default_true")] timeout: bool,
        #[serde(default)] transaction_id: u16,
    },
    VolumeStop { device_name: String, mac: String },
    MeteringStart {
        device_name: String,
        #[serde(default)] ipv4: String,
        mac: String,
        port: u16,
        #[serde(default = "default_true")] timeout: bool,
        #[serde(default)] transaction_id: u16,
    },
    MeteringStop { device_name: String, mac: String },
    BluetoothStatus { #[serde(default)] host_mac: Option<String> },
    MakeModel { mac: String },
    DanteModel { mac: String },
    QueryTxFlows { flow_protocol_id: u16, #[serde(default)] transaction_id: u16 },
    CreateTxFlow {
        flow_protocol_id: u16,
        flow_slot: u16,
        channels: Vec<u16>,
        #[serde(default)] transaction_id: u16,
    },
    DeleteTxFlow { flow_protocol_id: u16, flow_slot: u16, #[serde(default)] transaction_id: u16 },
}

fn default_true() -> bool {
    true
}

fn default_aes67_sequence() -> u16 {
    0x1007
}

fn default_leader_sequence() -> u16 {
    0x0021
}

fn parse_mac(value: &Option<String>, default: [u8; 6]) -> Result<[u8; 6], SpecError> {
    match value {
        None => Ok(default),
        Some(text) => parse_mac_required(text),
    }
}

fn parse_mac_required(text: &str) -> Result<[u8; 6], SpecError> {
    let cleaned: String = text.chars().filter(|character| *character != ':').collect();
    if cleaned.len() != 12 {
        return Err(SpecError::InvalidMac);
    }
    let mut mac = [0u8; 6];
    for (index, byte) in mac.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&cleaned[index * 2..index * 2 + 2], 16)
            .map_err(|_| SpecError::InvalidMac)?;
    }
    Ok(mac)
}

fn parse_ip(text: &str) -> Result<[u8; 4], SpecError> {
    if text.is_empty() {
        return Ok([0u8; 4]);
    }
    text.parse::<Ipv4Addr>()
        .map(|address| address.octets())
        .map_err(|_| SpecError::InvalidIp)
}

fn parse_channel_type(text: &str) -> Result<ChannelType, SpecError> {
    match text {
        "rx" => Ok(ChannelType::Rx),
        "tx" => Ok(ChannelType::Tx),
        _ => Err(SpecError::InvalidChannelType),
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
        "set_encoding" | "set_sample_rate" | "set_gain_level" => (Settings, Request),
        "reboot" | "enable_aes67" => (Settings, Fire { repeat: 3, interval_ms: 100 }),
        "set_preferred_leader" => (Settings, Fire { repeat: 3, interval_ms: 500 }),
        "identify" | "probe_interface_status" | "set_interface_dhcp" | "set_interface_static"
        | "probe_aes67" | "probe_preferred_leader" | "bluetooth_status" | "make_model"
        | "dante_model" => (Settings, Fire { repeat: 1, interval_ms: 0 }),
        "volume_start" | "volume_stop" | "metering_start" | "metering_stop" => {
            (Control, Fire { repeat: 1, interval_ms: 0 })
        }
        _ => (Arc, StampedRequest),
    }
}

fn command_name(json: &str) -> Result<String, SpecError> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|_| SpecError::InvalidJson)?;
    value
        .get("command")
        .and_then(|name| name.as_str())
        .map(str::to_owned)
        .ok_or(SpecError::InvalidJson)
}

pub fn build_routed_command(json: &str, default_host_mac: [u8; 6]) -> Result<Routed, SpecError> {
    let name = command_name(json)?;
    let spec: CommandSpec = serde_json::from_str(json).map_err(|_| SpecError::InvalidJson)?;
    let packet = build_command(spec, default_host_mac)?;
    let (target, io) = route(&name);
    Ok(Routed { packet, target, io })
}

pub fn build_command_from_json(json: &str) -> Result<Vec<u8>, SpecError> {
    let spec: CommandSpec = serde_json::from_str(json).map_err(|_| SpecError::InvalidJson)?;
    build_command(spec, [0u8; 6])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_arc_commands_as_stamped_requests() {
        for command in ["set_channel_name", "add_subscriptions", "set_latency", "channel_count"] {
            assert_eq!(route(command), (Target::Arc, IoMode::StampedRequest), "{command}");
        }
    }

    #[test]
    fn routes_settings_request_commands() {
        for command in ["set_encoding", "set_sample_rate", "set_gain_level"] {
            assert_eq!(route(command), (Target::Settings, IoMode::Request), "{command}");
        }
    }

    #[test]
    fn routes_repeated_fire_commands() {
        assert_eq!(route("reboot"), (Target::Settings, IoMode::Fire { repeat: 3, interval_ms: 100 }));
        assert_eq!(route("enable_aes67"), (Target::Settings, IoMode::Fire { repeat: 3, interval_ms: 100 }));
        assert_eq!(route("set_preferred_leader"), (Target::Settings, IoMode::Fire { repeat: 3, interval_ms: 500 }));
    }

    #[test]
    fn routes_single_fire_and_control_commands() {
        assert_eq!(route("identify"), (Target::Settings, IoMode::Fire { repeat: 1, interval_ms: 0 }));
        assert_eq!(route("metering_start"), (Target::Control, IoMode::Fire { repeat: 1, interval_ms: 0 }));
    }

    #[test]
    fn default_host_mac_fills_in_when_omitted() {
        let mac = [0x0c, 0x9d, 0x92, 0xc5, 0x12, 0xf8];
        let routed = build_routed_command("{\"command\":\"reboot\"}", mac).unwrap();
        assert_eq!(&routed.packet[8..14], &mac);
    }

    #[test]
    fn explicit_host_mac_overrides_default() {
        let routed = build_routed_command(
            "{\"command\":\"reboot\",\"host_mac\":\"001122334455\"}",
            [0xFF; 6],
        )
        .unwrap();
        assert_eq!(&routed.packet[8..14], &[0x00, 0x11, 0x22, 0x33, 0x44, 0x55]);
    }
}

fn build_command(spec: CommandSpec, default_host_mac: [u8; 6]) -> Result<Vec<u8>, SpecError> {
    let packet = match spec {
        CommandSpec::DeviceInfo { transaction_id } => commands::build_device_info(transaction_id),
        CommandSpec::DeviceName { transaction_id } => commands::build_device_name(transaction_id),
        CommandSpec::ChannelCount { transaction_id } => commands::build_channel_count(transaction_id),
        CommandSpec::DeviceSettings { transaction_id } => commands::build_device_settings(transaction_id),
        CommandSpec::SetName { name, transaction_id } => commands::build_set_name(&name, transaction_id)?,
        CommandSpec::ResetName { transaction_id } => commands::build_reset_name(transaction_id),
        CommandSpec::Receivers { page, transaction_id } => commands::build_receivers(page, transaction_id),
        CommandSpec::Transmitters { page, friendly_names, transaction_id } => {
            commands::build_transmitters(page, friendly_names, transaction_id)
        }
        CommandSpec::ResetChannelName { channel_type, channel_number, transaction_id } => {
            commands::build_reset_channel_name(parse_channel_type(&channel_type)?, channel_number, transaction_id)
        }
        CommandSpec::SetChannelName { channel_type, channel_number, name, transaction_id } => {
            commands::build_set_channel_name(parse_channel_type(&channel_type)?, channel_number, &name, transaction_id)?
        }
        CommandSpec::AddSubscriptions { subscriptions, transaction_id } => {
            let records: Vec<(u16, String, String)> = subscriptions
                .into_iter()
                .map(|entry| (entry.rx_channel, entry.tx_channel, entry.tx_device))
                .collect();
            commands::build_add_subscriptions(&records, transaction_id)?
        }
        CommandSpec::RemoveSubscriptions { rx_channels, transaction_id } => {
            commands::build_remove_subscriptions(&rx_channels, transaction_id)?
        }
        CommandSpec::SetLatency { latency, transaction_id } => commands::build_set_latency(latency, transaction_id),
        CommandSpec::Reboot { host_mac } => commands::build_reboot(parse_mac(&host_mac, default_host_mac)?),
        CommandSpec::Identify {} => commands::build_identify(),
        CommandSpec::SetEncoding { encoding } => commands::build_set_encoding(encoding),
        CommandSpec::SetSampleRate { sample_rate } => commands::build_set_sample_rate(sample_rate),
        CommandSpec::SetGainLevel { channel_number, gain_level, device_type } => {
            commands::build_set_gain_level(channel_number, gain_level, device_type == "input")
        }
        CommandSpec::EnableAes67 { enabled, host_mac } => commands::build_enable_aes67(enabled, parse_mac(&host_mac, default_host_mac)?),
        CommandSpec::ProbeInterfaceStatus { host_mac } => commands::build_probe_interface_status(parse_mac(&host_mac, default_host_mac)?),
        CommandSpec::SetInterfaceDhcp { host_mac } => commands::build_set_interface_dhcp(parse_mac(&host_mac, default_host_mac)?),
        CommandSpec::SetInterfaceStatic { ip, netmask, dns, gateway, host_mac } => {
            commands::build_set_interface_static(
                parse_ip(&ip)?,
                parse_ip(&netmask)?,
                parse_ip(&dns)?,
                parse_ip(&gateway)?,
                parse_mac(&host_mac, default_host_mac)?,
            )
        }
        CommandSpec::ProbeAes67 { host_mac, sequence } => commands::build_probe_aes67(parse_mac(&host_mac, default_host_mac)?, sequence),
        CommandSpec::SetPreferredLeader { preferred, clock_source, host_mac, sequence } => {
            commands::build_set_preferred_leader(preferred, clock_source, parse_mac(&host_mac, default_host_mac)?, sequence)
        }
        CommandSpec::ProbePreferredLeader { clock_source, host_mac, sequence } => {
            commands::build_probe_preferred_leader(clock_source, parse_mac(&host_mac, default_host_mac)?, sequence)
        }
        CommandSpec::QueryLatencyConfig { transaction_id } => commands::build_query_latency_config(transaction_id),
        CommandSpec::VolumeStart { device_name, ipv4, mac, port, timeout, transaction_id }
        | CommandSpec::MeteringStart { device_name, ipv4, mac, port, timeout, transaction_id } => {
            commands::build_volume_start(
                &device_name,
                parse_ip(&ipv4)?,
                parse_mac_required(&mac)?,
                port,
                timeout,
                transaction_id,
            )
        }
        CommandSpec::VolumeStop { device_name, mac } | CommandSpec::MeteringStop { device_name, mac } => {
            commands::build_volume_stop(&device_name, parse_mac_required(&mac)?)
        }
        CommandSpec::BluetoothStatus { host_mac } => commands::build_bluetooth_status(parse_mac(&host_mac, default_host_mac)?),
        CommandSpec::MakeModel { mac } => commands::build_make_model(parse_mac_required(&mac)?),
        CommandSpec::DanteModel { mac } => commands::build_dante_model(parse_mac_required(&mac)?),
        CommandSpec::QueryTxFlows { flow_protocol_id, transaction_id } => {
            commands::build_query_tx_flows(flow_protocol_id, transaction_id)
        }
        CommandSpec::CreateTxFlow { flow_protocol_id, flow_slot, channels, transaction_id } => {
            commands::build_create_tx_flow(flow_protocol_id, flow_slot, &channels, transaction_id)
        }
        CommandSpec::DeleteTxFlow { flow_protocol_id, flow_slot, transaction_id } => {
            commands::build_delete_tx_flow(flow_protocol_id, flow_slot, transaction_id)
        }
    };
    Ok(packet)
}
