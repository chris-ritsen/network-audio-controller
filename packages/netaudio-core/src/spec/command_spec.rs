use super::command_defaults::*;
use super::command_values::*;
use super::*;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Subscription {
    rx_channel: u16,
    tx_channel: String,
    tx_device: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(super) enum SubscriptionPageEntry {
    Set {
        rx_channel: u16,
        tx_channel: String,
        tx_device: String,
    },
    Clear {
        rx_channel: u16,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReceiveChannelNamePageEntry {
    rx_channel: u16,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TransmitterChannelNameReconciliationEntry {
    channel_number: u16,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "command", rename_all = "snake_case")]
pub(super) enum CommandSpec {
    AddSubscriptions {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        subscriptions: Vec<Subscription>,
    },
    BluetoothStatus {
        #[serde(default)]
        host_mac: Option<String>,
    },
    CapabilityPartitionExport {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ChannelCount {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_arc_protocol")]
        protocol_id: u16,
    },
    ClearAllConfiguration {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ClearAllConfigurationPreservingInternetProtocolSettings {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    CmcRegister {
        host_mac: String,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    CreateTxFlow {
        channels: Vec<u16>,
        flow_protocol_id: u16,
        flow_slot: u16,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    DanteModel {
        mac: String,
    },
    DeleteTxFlow {
        flow_protocol_id: u16,
        flow_slot: u16,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    DeviceInfo {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_arc_protocol")]
        protocol_id: u16,
    },
    DeviceLogExport {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    DeviceName {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    DeviceSettings {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    EnableAes67 {
        enabled: bool,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    FactoryReset {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    Identify {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    MakeModel {
        mac: String,
    },
    MeteringStart {
        device_name: String,
        #[serde(default)]
        ipv4: String,
        mac: String,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        port: u16,
        #[serde(default = "default_true")]
        timeout: bool,
    },
    MeteringStop {
        device_name: String,
        mac: String,
    },
    ProbeAes67 {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ProbeClearConfigurationStatus {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ProbeEncoding {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ProbeGainLevel {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ProbeInterfaceStatus {
        #[serde(default)]
        host_mac: Option<String>,
    },
    ProbeLinkStatus {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ProbeLockResetStatus {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_lock_reset_request_value")]
        request_value: u32,
    },
    ProbePreferredLeader {
        #[serde(default)]
        clock_source: u16,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ProbeSampleRate {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ProbeSampleRatePullup {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ProbeSwitchConfiguration {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    PropertyDirectory {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_arc_protocol")]
        protocol_id: u16,
    },
    QueryLatencyConfig {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    #[serde(rename = "query_modern_arc_receiver_channel_status")]
    QueryModernArcReceiverChannelStatus {
        #[serde(default)]
        ending_channel_identifier: u16,
        #[serde(default = "default_flow_start")]
        media_selector: u16,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_modern_arc_protocol")]
        protocol_id: u16,
        #[serde(default = "default_flow_start")]
        starting_channel_identifier: u16,
    },
    #[serde(rename = "query_modern_arc_receiver_flow_status")]
    QueryModernArcReceiverFlowStatus {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_modern_arc_protocol")]
        protocol_id: u16,
    },
    QueryReceiverFlows {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_flow_start")]
        starting_flow: u16,
    },
    QueryReceiverPortRanges {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_flow_protocol")]
        protocol_id: u16,
    },
    QueryTransmitChannelCapabilities {
        #[serde(default)]
        maximum_channel_count: u16,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_flow_start")]
        starting_channel_identifier: u16,
    },
    #[serde(rename = "query_modern_arc_transmitter_channel_status")]
    QueryModernArcTransmitterChannelStatus {
        #[serde(default)]
        ending_channel_identifier: u16,
        #[serde(default = "default_flow_start")]
        media_selector: u16,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_modern_arc_protocol")]
        protocol_id: u16,
        #[serde(default = "default_flow_start")]
        starting_channel_identifier: u16,
    },
    QueryTxFlows {
        flow_protocol_id: u16,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_flow_start")]
        starting_flow: u16,
    },
    Reboot {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    #[serde(rename = "receive_channel_name_page_2729")]
    ReceiveChannelNamePage2729 {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        records: Vec<ReceiveChannelNamePageEntry>,
    },
    Receivers {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        page: u16,
    },
    #[serde(rename = "reconcile_transmitter_channel_names_2809")]
    ReconcileTransmitterChannelNames2809 {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        records: Vec<TransmitterChannelNameReconciliationEntry>,
    },
    RefreshClockStatus {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    RemoveSubscriptions {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        rx_channels: Vec<u32>,
    },
    ResetChannelName {
        channel_number: u8,
        channel_type: String,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    ResetName {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    SetAes67MulticastPrefix {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        prefix: String,
    },
    SetChannelName {
        channel_number: u16,
        channel_type: String,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        name: String,
        #[serde(default = "default_channel_name_protocol")]
        protocol_id: u16,
    },
    SetClockSource {
        clock_source: u16,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    SetClockSubdomain {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        subdomain: [u8; 16],
    },
    SetEncoding {
        encoding: u32,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    SetGainLevel {
        channel_number: u16,
        device_type: String,
        gain_level: u8,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    SetInterfaceDhcp {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
    },
    SetInterfaceStatic {
        #[serde(default)]
        dns: String,
        #[serde(default)]
        gateway: String,
        #[serde(default)]
        host_mac: Option<String>,
        ip: String,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        netmask: String,
    },
    SetLatency {
        latency: f64,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_arc_protocol")]
        protocol_id: u16,
    },
    SetName {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        name: String,
    },
    SetPreferredLeader {
        #[serde(default)]
        clock_source: u16,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        preferred: bool,
    },
    SetSampleRate {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        sample_rate: u32,
    },
    SetSampleRatePullup {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        raw_value: u32,
    },
    #[serde(rename = "subscription_page_2729")]
    SubscriptionPage2729 {
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        records: Vec<SubscriptionPageEntry>,
    },
    ModernArcSubscriptionPage {
        #[serde(default = "default_flow_start")]
        media_type_code: u16,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        page_capacity: u8,
        #[serde(default = "default_modern_arc_protocol")]
        protocol_id: u16,
        records: Vec<SubscriptionPageEntry>,
    },
    TransmitterNames {
        channel_count: u16,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        #[serde(default = "default_arc_protocol")]
        protocol_id: u16,
    },
    Transmitters {
        #[serde(default)]
        friendly_names: bool,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        page: u16,
    },
    VolumeStart {
        device_name: String,
        #[serde(default)]
        ipv4: String,
        mac: String,
        #[serde(default, alias = "sequence", alias = "transaction_id")]
        message_id: u16,
        port: u16,
        #[serde(default = "default_true")]
        timeout: bool,
    },
    VolumeStop {
        device_name: String,
        mac: String,
    },
}

impl CommandSpec {
    pub(super) fn message_id(&self) -> u16 {
        match self {
            CommandSpec::AddSubscriptions { message_id, .. }
            | CommandSpec::CapabilityPartitionExport { message_id, .. }
            | CommandSpec::ChannelCount { message_id, .. }
            | CommandSpec::ClearAllConfiguration { message_id, .. }
            | CommandSpec::ClearAllConfigurationPreservingInternetProtocolSettings {
                message_id,
                ..
            }
            | CommandSpec::CmcRegister { message_id, .. }
            | CommandSpec::CreateTxFlow { message_id, .. }
            | CommandSpec::DeleteTxFlow { message_id, .. }
            | CommandSpec::DeviceInfo { message_id, .. }
            | CommandSpec::DeviceLogExport { message_id, .. }
            | CommandSpec::DeviceName { message_id, .. }
            | CommandSpec::DeviceSettings { message_id, .. }
            | CommandSpec::EnableAes67 { message_id, .. }
            | CommandSpec::FactoryReset { message_id, .. }
            | CommandSpec::Identify { message_id, .. }
            | CommandSpec::MeteringStart { message_id, .. }
            | CommandSpec::ProbeAes67 { message_id, .. }
            | CommandSpec::ProbeClearConfigurationStatus { message_id, .. }
            | CommandSpec::ProbeEncoding { message_id, .. }
            | CommandSpec::ProbeGainLevel { message_id, .. }
            | CommandSpec::ProbeLinkStatus { message_id, .. }
            | CommandSpec::ProbeLockResetStatus { message_id, .. }
            | CommandSpec::ProbePreferredLeader { message_id, .. }
            | CommandSpec::ProbeSampleRate { message_id, .. }
            | CommandSpec::ProbeSampleRatePullup { message_id, .. }
            | CommandSpec::ProbeSwitchConfiguration { message_id, .. }
            | CommandSpec::PropertyDirectory { message_id, .. }
            | CommandSpec::QueryLatencyConfig { message_id, .. }
            | CommandSpec::QueryModernArcReceiverChannelStatus { message_id, .. }
            | CommandSpec::QueryModernArcReceiverFlowStatus { message_id, .. }
            | CommandSpec::QueryReceiverFlows { message_id, .. }
            | CommandSpec::QueryReceiverPortRanges { message_id, .. }
            | CommandSpec::QueryTransmitChannelCapabilities { message_id, .. }
            | CommandSpec::QueryModernArcTransmitterChannelStatus { message_id, .. }
            | CommandSpec::QueryTxFlows { message_id, .. }
            | CommandSpec::Reboot { message_id, .. }
            | CommandSpec::ReceiveChannelNamePage2729 { message_id, .. }
            | CommandSpec::Receivers { message_id, .. }
            | CommandSpec::ReconcileTransmitterChannelNames2809 { message_id, .. }
            | CommandSpec::RefreshClockStatus { message_id, .. }
            | CommandSpec::RemoveSubscriptions { message_id, .. }
            | CommandSpec::ResetChannelName { message_id, .. }
            | CommandSpec::ResetName { message_id, .. }
            | CommandSpec::SetAes67MulticastPrefix { message_id, .. }
            | CommandSpec::SetChannelName { message_id, .. }
            | CommandSpec::SetClockSource { message_id, .. }
            | CommandSpec::SetClockSubdomain { message_id, .. }
            | CommandSpec::SetEncoding { message_id, .. }
            | CommandSpec::SetGainLevel { message_id, .. }
            | CommandSpec::SetInterfaceDhcp { message_id, .. }
            | CommandSpec::SetInterfaceStatic { message_id, .. }
            | CommandSpec::SetLatency { message_id, .. }
            | CommandSpec::SetName { message_id, .. }
            | CommandSpec::SetPreferredLeader { message_id, .. }
            | CommandSpec::SetSampleRate { message_id, .. }
            | CommandSpec::SetSampleRatePullup { message_id, .. }
            | CommandSpec::SubscriptionPage2729 { message_id, .. }
            | CommandSpec::ModernArcSubscriptionPage { message_id, .. }
            | CommandSpec::TransmitterNames { message_id, .. }
            | CommandSpec::Transmitters { message_id, .. }
            | CommandSpec::VolumeStart { message_id, .. } => *message_id,
            CommandSpec::BluetoothStatus { .. }
            | CommandSpec::DanteModel { .. }
            | CommandSpec::MakeModel { .. }
            | CommandSpec::MeteringStop { .. }
            | CommandSpec::ProbeInterfaceStatus { .. }
            | CommandSpec::VolumeStop { .. } => 0,
        }
    }

    pub(super) fn message_id_mut(&mut self) -> Option<&mut u16> {
        match self {
            CommandSpec::AddSubscriptions { message_id, .. }
            | CommandSpec::CapabilityPartitionExport { message_id, .. }
            | CommandSpec::ChannelCount { message_id, .. }
            | CommandSpec::ClearAllConfiguration { message_id, .. }
            | CommandSpec::ClearAllConfigurationPreservingInternetProtocolSettings {
                message_id,
                ..
            }
            | CommandSpec::CmcRegister { message_id, .. }
            | CommandSpec::CreateTxFlow { message_id, .. }
            | CommandSpec::DeleteTxFlow { message_id, .. }
            | CommandSpec::DeviceInfo { message_id, .. }
            | CommandSpec::DeviceLogExport { message_id, .. }
            | CommandSpec::DeviceName { message_id, .. }
            | CommandSpec::DeviceSettings { message_id, .. }
            | CommandSpec::EnableAes67 { message_id, .. }
            | CommandSpec::FactoryReset { message_id, .. }
            | CommandSpec::Identify { message_id, .. }
            | CommandSpec::MeteringStart { message_id, .. }
            | CommandSpec::ProbeAes67 { message_id, .. }
            | CommandSpec::ProbeClearConfigurationStatus { message_id, .. }
            | CommandSpec::ProbeEncoding { message_id, .. }
            | CommandSpec::ProbeGainLevel { message_id, .. }
            | CommandSpec::ProbeLinkStatus { message_id, .. }
            | CommandSpec::ProbeLockResetStatus { message_id, .. }
            | CommandSpec::ProbePreferredLeader { message_id, .. }
            | CommandSpec::ProbeSampleRate { message_id, .. }
            | CommandSpec::ProbeSampleRatePullup { message_id, .. }
            | CommandSpec::ProbeSwitchConfiguration { message_id, .. }
            | CommandSpec::PropertyDirectory { message_id, .. }
            | CommandSpec::QueryLatencyConfig { message_id, .. }
            | CommandSpec::QueryModernArcReceiverChannelStatus { message_id, .. }
            | CommandSpec::QueryModernArcReceiverFlowStatus { message_id, .. }
            | CommandSpec::QueryReceiverFlows { message_id, .. }
            | CommandSpec::QueryReceiverPortRanges { message_id, .. }
            | CommandSpec::QueryTransmitChannelCapabilities { message_id, .. }
            | CommandSpec::QueryModernArcTransmitterChannelStatus { message_id, .. }
            | CommandSpec::QueryTxFlows { message_id, .. }
            | CommandSpec::Reboot { message_id, .. }
            | CommandSpec::ReceiveChannelNamePage2729 { message_id, .. }
            | CommandSpec::Receivers { message_id, .. }
            | CommandSpec::ReconcileTransmitterChannelNames2809 { message_id, .. }
            | CommandSpec::RefreshClockStatus { message_id, .. }
            | CommandSpec::RemoveSubscriptions { message_id, .. }
            | CommandSpec::ResetChannelName { message_id, .. }
            | CommandSpec::ResetName { message_id, .. }
            | CommandSpec::SetAes67MulticastPrefix { message_id, .. }
            | CommandSpec::SetChannelName { message_id, .. }
            | CommandSpec::SetClockSource { message_id, .. }
            | CommandSpec::SetClockSubdomain { message_id, .. }
            | CommandSpec::SetEncoding { message_id, .. }
            | CommandSpec::SetGainLevel { message_id, .. }
            | CommandSpec::SetInterfaceDhcp { message_id, .. }
            | CommandSpec::SetInterfaceStatic { message_id, .. }
            | CommandSpec::SetLatency { message_id, .. }
            | CommandSpec::SetName { message_id, .. }
            | CommandSpec::SetPreferredLeader { message_id, .. }
            | CommandSpec::SetSampleRate { message_id, .. }
            | CommandSpec::SetSampleRatePullup { message_id, .. }
            | CommandSpec::SubscriptionPage2729 { message_id, .. }
            | CommandSpec::ModernArcSubscriptionPage { message_id, .. }
            | CommandSpec::TransmitterNames { message_id, .. }
            | CommandSpec::Transmitters { message_id, .. }
            | CommandSpec::VolumeStart { message_id, .. } => Some(message_id),
            CommandSpec::BluetoothStatus { .. }
            | CommandSpec::DanteModel { .. }
            | CommandSpec::MakeModel { .. }
            | CommandSpec::MeteringStop { .. }
            | CommandSpec::ProbeInterfaceStatus { .. }
            | CommandSpec::VolumeStop { .. } => None,
        }
    }

    pub(super) fn route(&self) -> (Target, IoMode) {
        use IoMode::*;
        use Target::*;
        match self {
            CommandSpec::AddSubscriptions { .. }
            | CommandSpec::ChannelCount { .. }
            | CommandSpec::CreateTxFlow { .. }
            | CommandSpec::DeleteTxFlow { .. }
            | CommandSpec::DeviceInfo { .. }
            | CommandSpec::DeviceName { .. }
            | CommandSpec::DeviceSettings { .. }
            | CommandSpec::PropertyDirectory { .. }
            | CommandSpec::QueryLatencyConfig { .. }
            | CommandSpec::QueryModernArcReceiverChannelStatus { .. }
            | CommandSpec::QueryModernArcReceiverFlowStatus { .. }
            | CommandSpec::QueryReceiverFlows { .. }
            | CommandSpec::QueryReceiverPortRanges { .. }
            | CommandSpec::QueryTransmitChannelCapabilities { .. }
            | CommandSpec::QueryModernArcTransmitterChannelStatus { .. }
            | CommandSpec::QueryTxFlows { .. }
            | CommandSpec::ReceiveChannelNamePage2729 { .. }
            | CommandSpec::ReconcileTransmitterChannelNames2809 { .. }
            | CommandSpec::Receivers { .. }
            | CommandSpec::RemoveSubscriptions { .. }
            | CommandSpec::ResetChannelName { .. }
            | CommandSpec::ResetName { .. }
            | CommandSpec::SetAes67MulticastPrefix { .. }
            | CommandSpec::SetChannelName { .. }
            | CommandSpec::SetLatency { .. }
            | CommandSpec::SetName { .. }
            | CommandSpec::SubscriptionPage2729 { .. }
            | CommandSpec::ModernArcSubscriptionPage { .. }
            | CommandSpec::TransmitterNames { .. }
            | CommandSpec::Transmitters { .. } => (Arc, Request),
            CommandSpec::BluetoothStatus { .. }
            | CommandSpec::ClearAllConfiguration { .. }
            | CommandSpec::ClearAllConfigurationPreservingInternetProtocolSettings { .. }
            | CommandSpec::DanteModel { .. }
            | CommandSpec::FactoryReset { .. }
            | CommandSpec::Identify { .. }
            | CommandSpec::MakeModel { .. }
            | CommandSpec::ProbeAes67 { .. }
            | CommandSpec::ProbeClearConfigurationStatus { .. }
            | CommandSpec::ProbeEncoding { .. }
            | CommandSpec::ProbeGainLevel { .. }
            | CommandSpec::ProbeInterfaceStatus { .. }
            | CommandSpec::ProbeLinkStatus { .. }
            | CommandSpec::ProbeLockResetStatus { .. }
            | CommandSpec::ProbePreferredLeader { .. }
            | CommandSpec::ProbeSampleRate { .. }
            | CommandSpec::ProbeSampleRatePullup { .. }
            | CommandSpec::ProbeSwitchConfiguration { .. }
            | CommandSpec::Reboot { .. }
            | CommandSpec::RefreshClockStatus { .. }
            | CommandSpec::SetEncoding { .. }
            | CommandSpec::SetGainLevel { .. }
            | CommandSpec::SetInterfaceDhcp { .. }
            | CommandSpec::SetInterfaceStatic { .. }
            | CommandSpec::SetSampleRate { .. }
            | CommandSpec::SetSampleRatePullup { .. } => (
                Settings,
                Fire {
                    repeat: 1,
                    interval_ms: 0,
                },
            ),
            CommandSpec::CapabilityPartitionExport { .. } | CommandSpec::DeviceLogExport { .. } => {
                (
                    Settings,
                    Fire {
                        repeat: 1,
                        interval_ms: 0,
                    },
                )
            }
            CommandSpec::CmcRegister { .. } => (Control, Request),
            CommandSpec::EnableAes67 { .. } => (
                Settings,
                Fire {
                    repeat: 3,
                    interval_ms: 100,
                },
            ),
            CommandSpec::MeteringStart { .. }
            | CommandSpec::MeteringStop { .. }
            | CommandSpec::VolumeStart { .. }
            | CommandSpec::VolumeStop { .. } => (
                Control,
                Fire {
                    repeat: 1,
                    interval_ms: 0,
                },
            ),
            CommandSpec::SetClockSource { .. }
            | CommandSpec::SetClockSubdomain { .. }
            | CommandSpec::SetPreferredLeader { .. } => (
                Settings,
                Fire {
                    repeat: 3,
                    interval_ms: 500,
                },
            ),
        }
    }
}

pub(super) fn build_command(
    spec: CommandSpec,
    default_host_mac: Option<[u8; 6]>,
) -> Result<Vec<u8>, SpecError> {
    let packet = match spec {
        CommandSpec::AddSubscriptions {
            subscriptions,
            message_id,
        } => {
            let records: Vec<(u16, String, String)> = subscriptions
                .into_iter()
                .map(|entry| (entry.rx_channel, entry.tx_channel, entry.tx_device))
                .collect();
            commands::build_add_subscriptions(&records, message_id)?
        }
        CommandSpec::BluetoothStatus { host_mac } => {
            commands::build_bluetooth_status(parse_mac(&host_mac, default_host_mac)?)?
        }
        CommandSpec::CapabilityPartitionExport {
            host_mac,
            message_id,
        } => commands::build_capability_partition_export(
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::ChannelCount {
            message_id,
            protocol_id,
        } => commands::build_channel_count_for_protocol(protocol_id, message_id)?,
        CommandSpec::ClearAllConfiguration {
            host_mac,
            message_id,
        } => commands::build_clear_all_configuration(
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::ClearAllConfigurationPreservingInternetProtocolSettings {
            host_mac,
            message_id,
        } => commands::build_clear_all_configuration_preserving_internet_protocol_settings(
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::CmcRegister {
            message_id,
            host_mac,
        } => commands::build_cmc_register(message_id, parse_mac_required(&host_mac)?)?,
        CommandSpec::CreateTxFlow {
            flow_protocol_id,
            flow_slot,
            channels,
            message_id,
        } => commands::build_create_tx_flow(flow_protocol_id, flow_slot, &channels, message_id)?,
        CommandSpec::DanteModel { mac } => commands::build_dante_model(parse_mac_required(&mac)?)?,
        CommandSpec::DeleteTxFlow {
            flow_protocol_id,
            flow_slot,
            message_id,
        } => commands::build_delete_tx_flow(flow_protocol_id, flow_slot, message_id)?,
        CommandSpec::DeviceInfo {
            message_id,
            protocol_id,
        } => commands::build_device_info_for_protocol(protocol_id, message_id)?,
        CommandSpec::DeviceLogExport {
            host_mac,
            message_id,
        } => {
            commands::build_device_log_export(parse_mac(&host_mac, default_host_mac)?, message_id)?
        }
        CommandSpec::DeviceName { message_id } => commands::build_device_name(message_id)?,
        CommandSpec::DeviceSettings { message_id } => commands::build_device_settings(message_id)?,
        CommandSpec::EnableAes67 {
            enabled,
            host_mac,
            message_id,
        } => commands::build_enable_aes67(
            enabled,
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::FactoryReset {
            host_mac,
            message_id,
        } => commands::build_factory_reset(parse_mac(&host_mac, default_host_mac)?, message_id)?,
        CommandSpec::Identify { message_id } => commands::build_identify(message_id)?,
        CommandSpec::MakeModel { mac } => commands::build_make_model(parse_mac_required(&mac)?)?,
        CommandSpec::ProbeAes67 {
            host_mac,
            message_id,
        } => commands::build_probe_aes67(parse_mac(&host_mac, default_host_mac)?, message_id)?,
        CommandSpec::ProbeClearConfigurationStatus {
            host_mac,
            message_id,
        } => commands::build_probe_clear_configuration_status(
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::ProbeEncoding {
            host_mac,
            message_id,
        } => commands::build_probe_encoding(parse_mac(&host_mac, default_host_mac)?, message_id)?,
        CommandSpec::ProbeGainLevel {
            host_mac,
            message_id,
        } => commands::build_probe_gain_level(parse_mac(&host_mac, default_host_mac)?, message_id)?,
        CommandSpec::ProbeInterfaceStatus { host_mac } => {
            commands::build_probe_interface_status(parse_mac(&host_mac, default_host_mac)?)?
        }
        CommandSpec::ProbeLinkStatus {
            host_mac,
            message_id,
        } => {
            commands::build_probe_link_status(parse_mac(&host_mac, default_host_mac)?, message_id)?
        }
        CommandSpec::ProbeLockResetStatus {
            host_mac,
            message_id,
            request_value,
        } => commands::build_probe_lock_reset_status(
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
            request_value,
        )?,
        CommandSpec::ProbePreferredLeader {
            clock_source,
            host_mac,
            message_id,
        } => commands::build_probe_preferred_leader(
            clock_source,
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::ProbeSampleRate {
            host_mac,
            message_id,
        } => {
            commands::build_probe_sample_rate(parse_mac(&host_mac, default_host_mac)?, message_id)?
        }
        CommandSpec::ProbeSampleRatePullup {
            host_mac,
            message_id,
        } => commands::build_probe_sample_rate_pullup(
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::ProbeSwitchConfiguration {
            host_mac,
            message_id,
        } => commands::build_probe_switch_configuration(
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::PropertyDirectory {
            message_id,
            protocol_id,
        } => commands::build_property_directory_for_protocol(protocol_id, message_id)?,
        CommandSpec::QueryLatencyConfig { message_id } => {
            commands::build_query_latency_config(message_id)?
        }
        CommandSpec::QueryModernArcReceiverChannelStatus {
            protocol_id,
            media_selector,
            starting_channel_identifier,
            ending_channel_identifier,
            message_id,
        } => commands::build_query_receiver_channel_status(
            protocol_id,
            media_selector,
            starting_channel_identifier,
            ending_channel_identifier,
            message_id,
        )?,
        CommandSpec::QueryModernArcReceiverFlowStatus {
            message_id,
            protocol_id,
        } => commands::build_query_receiver_flow_status(protocol_id, message_id)?,
        CommandSpec::QueryReceiverFlows {
            starting_flow,
            message_id,
        } => commands::build_query_receiver_flows(starting_flow, message_id)?,
        CommandSpec::QueryReceiverPortRanges {
            message_id,
            protocol_id,
        } => commands::build_query_receiver_port_ranges_for_protocol(protocol_id, message_id)?,
        CommandSpec::QueryTransmitChannelCapabilities {
            starting_channel_identifier,
            maximum_channel_count,
            message_id,
        } => commands::build_query_transmit_channel_capabilities(
            starting_channel_identifier,
            maximum_channel_count,
            message_id,
        )?,
        CommandSpec::QueryModernArcTransmitterChannelStatus {
            protocol_id,
            media_selector,
            starting_channel_identifier,
            ending_channel_identifier,
            message_id,
        } => commands::build_query_transmitter_channel_status(
            protocol_id,
            media_selector,
            starting_channel_identifier,
            ending_channel_identifier,
            message_id,
        )?,
        CommandSpec::QueryTxFlows {
            flow_protocol_id,
            starting_flow,
            message_id,
        } => commands::build_query_tx_flows_from(flow_protocol_id, starting_flow, message_id)?,
        CommandSpec::Reboot {
            host_mac,
            message_id,
        } => commands::build_reboot(parse_mac(&host_mac, default_host_mac)?, message_id)?,
        CommandSpec::ReceiveChannelNamePage2729 {
            records,
            message_id,
        } => {
            let records: Vec<ReceiveChannelNamePageRecord> = records
                .into_iter()
                .map(|entry| ReceiveChannelNamePageRecord {
                    rx_channel_number: entry.rx_channel,
                    name: entry.name,
                })
                .collect();
            commands::build_receive_channel_name_page_2729(&records, message_id)?
        }
        CommandSpec::Receivers { page, message_id } => commands::build_receivers(page, message_id)?,
        CommandSpec::ReconcileTransmitterChannelNames2809 {
            records,
            message_id,
        } => commands::build_reconcile_transmitter_channel_names_2809(
            &records
                .into_iter()
                .map(
                    |record| commands::TransmitterChannelNameReconciliationRecord {
                        channel_number: record.channel_number,
                        name: record.name,
                    },
                )
                .collect::<Vec<_>>(),
            message_id,
        )?,
        CommandSpec::RefreshClockStatus {
            host_mac,
            message_id,
        } => commands::build_refresh_clock_status(
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::RemoveSubscriptions {
            rx_channels,
            message_id,
        } => commands::build_remove_subscriptions(&rx_channels, message_id)?,
        CommandSpec::ResetChannelName {
            channel_type,
            channel_number,
            message_id,
        } => commands::build_reset_channel_name(
            parse_channel_type(&channel_type)?,
            channel_number,
            message_id,
        )?,
        CommandSpec::ResetName { message_id } => commands::build_reset_name(message_id)?,
        CommandSpec::SetAes67MulticastPrefix { prefix, message_id } => {
            let address = prefix
                .parse::<Ipv4Addr>()
                .map_err(|_| SpecError::InvalidIp)?;
            commands::build_set_aes67_multicast_prefix(address, message_id)?
        }
        CommandSpec::SetChannelName {
            channel_type,
            channel_number,
            name,
            protocol_id,
            message_id,
        } => commands::build_set_channel_name_for_protocol(
            protocol_id,
            parse_channel_type(&channel_type)?,
            channel_number,
            &name,
            message_id,
        )?,
        CommandSpec::SetClockSource {
            clock_source,
            host_mac,
            message_id,
        } => commands::build_set_clock_source(
            clock_source,
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::SetClockSubdomain {
            subdomain,
            host_mac,
            message_id,
        } => commands::build_set_clock_subdomain(
            subdomain,
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::SetEncoding {
            encoding,
            message_id,
        } => commands::build_set_encoding(encoding, message_id)?,
        CommandSpec::SetGainLevel {
            channel_number,
            gain_level,
            device_type,
            host_mac,
            message_id,
        } => commands::build_set_gain_level(
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
            channel_number,
            gain_level,
            parse_gain_device_type(&device_type)?,
        )?,
        CommandSpec::SetInterfaceDhcp {
            host_mac,
            message_id,
        } => {
            commands::build_set_interface_dhcp(parse_mac(&host_mac, default_host_mac)?, message_id)?
        }
        CommandSpec::SetInterfaceStatic {
            ip,
            netmask,
            dns,
            gateway,
            host_mac,
            message_id,
        } => commands::build_set_interface_static(
            parse_required_ipv4_address(&ip)?,
            parse_required_ipv4_address(&netmask)?,
            parse_optional_ipv4_address(&dns)?,
            parse_optional_ipv4_address(&gateway)?,
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::SetLatency {
            latency,
            message_id,
            protocol_id,
        } => commands::build_set_latency_for_protocol(protocol_id, latency, message_id)?,
        CommandSpec::SetName { name, message_id } => {
            crate::protocol::build_set_device_name(&name, message_id)?
        }
        CommandSpec::SetPreferredLeader {
            preferred,
            clock_source,
            host_mac,
            message_id,
        } => commands::build_set_preferred_leader(
            preferred,
            clock_source,
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
        )?,
        CommandSpec::SetSampleRate {
            sample_rate,
            message_id,
        } => commands::build_set_sample_rate(sample_rate, message_id)?,
        CommandSpec::SetSampleRatePullup {
            raw_value,
            host_mac,
            message_id,
        } => commands::build_set_sample_rate_pullup(
            parse_mac(&host_mac, default_host_mac)?,
            message_id,
            raw_value,
        )?,
        CommandSpec::SubscriptionPage2729 {
            records,
            message_id,
        } => {
            let records: Vec<SubscriptionPageRecord> = records
                .into_iter()
                .map(|entry| match entry {
                    SubscriptionPageEntry::Set {
                        rx_channel,
                        tx_channel,
                        tx_device,
                    } => SubscriptionPageRecord::Set {
                        rx_channel_number: rx_channel,
                        tx_channel_name: tx_channel,
                        tx_device_name: tx_device,
                    },
                    SubscriptionPageEntry::Clear { rx_channel } => SubscriptionPageRecord::Clear {
                        rx_channel_number: rx_channel,
                    },
                })
                .collect();
            commands::build_subscription_page_2729(&records, message_id)?
        }
        CommandSpec::ModernArcSubscriptionPage {
            media_type_code,
            message_id,
            page_capacity,
            protocol_id,
            records,
        } => {
            let records: Vec<SubscriptionPageRecord> = records
                .into_iter()
                .map(|record| match record {
                    SubscriptionPageEntry::Set {
                        rx_channel,
                        tx_channel,
                        tx_device,
                    } => SubscriptionPageRecord::Set {
                        rx_channel_number: rx_channel,
                        tx_channel_name: tx_channel,
                        tx_device_name: tx_device,
                    },
                    SubscriptionPageEntry::Clear { rx_channel } => SubscriptionPageRecord::Clear {
                        rx_channel_number: rx_channel,
                    },
                })
                .collect();
            commands::build_modern_arc_subscription_page(
                protocol_id,
                page_capacity,
                media_type_code,
                &records,
                message_id,
            )?
        }
        CommandSpec::TransmitterNames {
            channel_count,
            message_id,
            protocol_id,
        } => {
            commands::build_transmitter_names_for_protocol(protocol_id, channel_count, message_id)?
        }
        CommandSpec::Transmitters {
            page,
            friendly_names,
            message_id,
        } => commands::build_transmitters(page, friendly_names, message_id)?,
        CommandSpec::VolumeStart {
            device_name,
            ipv4,
            mac,
            port,
            timeout,
            message_id,
        }
        | CommandSpec::MeteringStart {
            device_name,
            ipv4,
            mac,
            port,
            timeout,
            message_id,
        } => commands::build_volume_start(
            &device_name,
            parse_optional_ipv4_address(&ipv4)?,
            parse_mac_required(&mac)?,
            port,
            timeout,
            message_id,
        )?,
        CommandSpec::VolumeStop { device_name, mac }
        | CommandSpec::MeteringStop { device_name, mac } => {
            commands::build_volume_stop(&device_name, parse_mac_required(&mac)?)?
        }
    };
    Ok(packet)
}

pub(super) fn parse_command_spec(json: &str) -> Result<CommandSpec, SpecError> {
    serde_json::from_str(json).map_err(|error| SpecError::InvalidJson(error.to_string()))
}
