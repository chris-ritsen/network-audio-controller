use super::command_defaults::*;
use super::*;

#[derive(Debug, Deserialize)]
struct Subscription {
    rx_channel: u16,
    tx_channel: String,
    tx_device: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum SubscriptionPageEntry {
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
struct ReceiveChannelNamePageEntry {
    rx_channel: u16,
    name: String,
}

#[derive(Debug, Deserialize)]
struct TransmitterChannelNameReconciliationEntry {
    channel_number: u16,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum CommandSpec {
    DeviceInfo {
        #[serde(default)]
        transaction_id: u16,
    },
    DeviceName {
        #[serde(default)]
        transaction_id: u16,
    },
    ChannelCount {
        #[serde(default)]
        transaction_id: u16,
    },
    DeviceSettings {
        #[serde(default)]
        transaction_id: u16,
    },
    PropertyDirectory {
        #[serde(default)]
        transaction_id: u16,
    },
    SetName {
        name: String,
        #[serde(default)]
        transaction_id: u16,
    },
    ResetName {
        #[serde(default)]
        transaction_id: u16,
    },
    Receivers {
        page: u16,
        #[serde(default)]
        transaction_id: u16,
    },
    Transmitters {
        page: u16,
        #[serde(default)]
        friendly_names: bool,
        #[serde(default)]
        transaction_id: u16,
    },
    TransmitterNames {
        channel_count: u16,
        #[serde(default)]
        transaction_id: u16,
    },
    ResetChannelName {
        channel_type: String,
        channel_number: u8,
        #[serde(default)]
        transaction_id: u16,
    },
    SetChannelName {
        channel_type: String,
        channel_number: u16,
        name: String,
        #[serde(default = "default_channel_name_protocol")]
        protocol_id: u16,
        #[serde(default)]
        transaction_id: u16,
    },
    #[serde(rename = "receive_channel_name_page_2729")]
    ReceiveChannelNamePage2729 {
        records: Vec<ReceiveChannelNamePageEntry>,
        #[serde(default)]
        transaction_id: u16,
    },
    AddSubscriptions {
        subscriptions: Vec<Subscription>,
        #[serde(default)]
        transaction_id: u16,
    },
    RemoveSubscriptions {
        rx_channels: Vec<u32>,
        #[serde(default)]
        transaction_id: u16,
    },
    #[serde(rename = "subscription_page_2729")]
    SubscriptionPage2729 {
        records: Vec<SubscriptionPageEntry>,
        #[serde(default)]
        transaction_id: u16,
    },
    SetLatency {
        latency: f64,
        #[serde(default)]
        transaction_id: u16,
    },
    Reboot {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_system_reset_sequence")]
        sequence: u16,
    },
    FactoryReset {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_system_reset_sequence")]
        sequence: u16,
    },
    Identify {
        sequence: u16,
    },
    SetEncoding {
        encoding: u32,
        #[serde(default = "default_set_encoding_sequence")]
        sequence: u16,
    },
    SetSampleRate {
        sample_rate: u32,
        #[serde(default = "default_set_sample_rate_sequence")]
        sequence: u16,
    },
    ProbeSampleRate {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_sample_rate_sequence")]
        sequence: u16,
    },
    ProbeEncoding {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_encoding_sequence")]
        sequence: u16,
    },
    SetSampleRatePullup {
        raw_value: u32,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_sample_rate_pullup_sequence")]
        sequence: u16,
    },
    ProbeSampleRatePullup {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_sample_rate_pullup_sequence")]
        sequence: u16,
    },
    ProbeGainLevel {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_gain_sequence")]
        sequence: u16,
    },
    SetGainLevel {
        channel_number: u16,
        gain_level: u8,
        device_type: String,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_gain_sequence")]
        sequence: u16,
    },
    EnableAes67 {
        enabled: bool,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_enable_aes67_sequence")]
        sequence: u16,
    },
    ProbeInterfaceStatus {
        #[serde(default)]
        host_mac: Option<String>,
    },
    ProbeLinkStatus {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_link_status_sequence")]
        sequence: u16,
    },
    ProbeSwitchConfiguration {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_switch_configuration_sequence")]
        sequence: u16,
    },
    SetInterfaceDhcp {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_interface_sequence")]
        sequence: u16,
    },
    SetInterfaceStatic {
        ip: String,
        netmask: String,
        #[serde(default)]
        dns: String,
        #[serde(default)]
        gateway: String,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_interface_sequence")]
        sequence: u16,
    },
    ProbeAes67 {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_aes67_sequence")]
        sequence: u16,
    },
    ProbeLockResetStatus {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_lock_reset_sequence")]
        sequence: u16,
        #[serde(default = "default_lock_reset_request_value")]
        request_value: u32,
    },
    DeviceLogExport {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_conmon_export_sequence")]
        sequence: u16,
    },
    CapabilityPartitionExport {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_conmon_export_sequence")]
        sequence: u16,
    },
    ProbeClearConfigurationStatus {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_clear_configuration_sequence")]
        sequence: u16,
    },
    ClearAllConfiguration {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_clear_configuration_sequence")]
        sequence: u16,
    },
    ClearAllConfigurationPreservingInternetProtocolSettings {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_clear_configuration_sequence")]
        sequence: u16,
    },
    SetPreferredLeader {
        preferred: bool,
        #[serde(default)]
        clock_source: u16,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_leader_sequence")]
        sequence: u16,
    },
    SetClockSource {
        clock_source: u16,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_leader_sequence")]
        sequence: u16,
    },
    SetClockSubdomain {
        subdomain: [u8; 16],
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_leader_sequence")]
        sequence: u16,
    },
    ProbePreferredLeader {
        #[serde(default)]
        clock_source: u16,
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_leader_sequence")]
        sequence: u16,
    },
    RefreshClockStatus {
        #[serde(default)]
        host_mac: Option<String>,
        #[serde(default = "default_leader_sequence")]
        sequence: u16,
    },
    QueryLatencyConfig {
        #[serde(default)]
        transaction_id: u16,
    },
    SetAes67MulticastPrefix {
        prefix: String,
        #[serde(default)]
        transaction_id: u16,
    },
    VolumeStart {
        device_name: String,
        #[serde(default)]
        ipv4: String,
        mac: String,
        port: u16,
        #[serde(default = "default_true")]
        timeout: bool,
        #[serde(default)]
        transaction_id: u16,
    },
    VolumeStop {
        device_name: String,
        mac: String,
    },
    MeteringStart {
        device_name: String,
        #[serde(default)]
        ipv4: String,
        mac: String,
        port: u16,
        #[serde(default = "default_true")]
        timeout: bool,
        #[serde(default)]
        transaction_id: u16,
    },
    MeteringStop {
        device_name: String,
        mac: String,
    },
    BluetoothStatus {
        #[serde(default)]
        host_mac: Option<String>,
    },
    MakeModel {
        mac: String,
    },
    DanteModel {
        mac: String,
    },
    CmcRegister {
        sequence: u16,
        host_mac: String,
    },
    QueryTxFlows {
        flow_protocol_id: u16,
        #[serde(default = "default_flow_start")]
        starting_flow: u16,
        #[serde(default)]
        transaction_id: u16,
    },
    #[serde(rename = "query_transmitter_channel_status_2809")]
    QueryTransmitterChannelStatus2809 {
        #[serde(default)]
        transaction_id: u16,
    },
    #[serde(rename = "reconcile_transmitter_channel_names_2809")]
    ReconcileTransmitterChannelNames2809 {
        records: Vec<TransmitterChannelNameReconciliationEntry>,
        #[serde(default)]
        transaction_id: u16,
    },
    #[serde(rename = "query_receiver_channel_status_2809")]
    QueryReceiverChannelStatus2809 {
        #[serde(default)]
        transaction_id: u16,
    },
    #[serde(rename = "query_receiver_flow_status_2809")]
    QueryReceiverFlowStatus2809 {
        #[serde(default)]
        transaction_id: u16,
    },
    QueryReceiverFlows {
        #[serde(default = "default_flow_start")]
        starting_flow: u16,
        #[serde(default)]
        transaction_id: u16,
    },
    QueryTransmitChannelCapabilities {
        #[serde(default = "default_flow_start")]
        starting_channel_identifier: u16,
        #[serde(default)]
        maximum_channel_count: u16,
        #[serde(default)]
        transaction_id: u16,
    },
    QueryReceiverPortRanges {
        #[serde(default)]
        transaction_id: u16,
    },
    CreateTxFlow {
        flow_protocol_id: u16,
        flow_slot: u16,
        channels: Vec<u16>,
        #[serde(default)]
        transaction_id: u16,
    },
    DeleteTxFlow {
        flow_protocol_id: u16,
        flow_slot: u16,
        #[serde(default)]
        transaction_id: u16,
    },
}

fn parse_mac(value: &Option<String>, default: [u8; 6]) -> Result<[u8; 6], SpecError> {
    match value {
        None if default != [0u8; 6] => Ok(default),
        None => Err(SpecError::InvalidMac),
        Some(text) => parse_mac_required(text),
    }
}

fn parse_mac_required(text: &str) -> Result<[u8; 6], SpecError> {
    let cleaned: Vec<u8> = text
        .bytes()
        .filter(|character| *character != b':')
        .collect();
    if cleaned.len() != 12 || !cleaned.iter().all(u8::is_ascii_hexdigit) {
        return Err(SpecError::InvalidMac);
    }
    let mut mac = [0u8; 6];
    for (destination, pair) in mac.iter_mut().zip(cleaned.chunks_exact(2)) {
        let pair = std::str::from_utf8(pair).map_err(|_| SpecError::InvalidMac)?;
        *destination = u8::from_str_radix(pair, 16).map_err(|_| SpecError::InvalidMac)?;
    }
    Ok(mac)
}

fn parse_required_ipv4_address(text: &str) -> Result<[u8; 4], SpecError> {
    if text.is_empty() {
        return Err(SpecError::InvalidIp);
    }
    text.parse::<Ipv4Addr>()
        .map(|address| address.octets())
        .map_err(|_| SpecError::InvalidIp)
}

fn parse_optional_ipv4_address(text: &str) -> Result<[u8; 4], SpecError> {
    if text.is_empty() {
        return Ok([0u8; 4]);
    }
    parse_required_ipv4_address(text)
}

fn parse_channel_type(text: &str) -> Result<ChannelType, SpecError> {
    match text {
        "rx" => Ok(ChannelType::Rx),
        "tx" => Ok(ChannelType::Tx),
        _ => Err(SpecError::InvalidChannelType),
    }
}

fn parse_gain_device_type(text: &str) -> Result<bool, SpecError> {
    match text {
        "input" => Ok(true),
        "output" => Ok(false),
        _ => Err(SpecError::InvalidDeviceType),
    }
}

fn build_command(spec: CommandSpec, default_host_mac: [u8; 6]) -> Result<Vec<u8>, SpecError> {
    let packet = match spec {
        CommandSpec::DeviceInfo { transaction_id } => commands::build_device_info(transaction_id)?,
        CommandSpec::DeviceName { transaction_id } => commands::build_device_name(transaction_id)?,
        CommandSpec::ChannelCount { transaction_id } => {
            commands::build_channel_count(transaction_id)?
        }
        CommandSpec::DeviceSettings { transaction_id } => {
            commands::build_device_settings(transaction_id)?
        }
        CommandSpec::PropertyDirectory { transaction_id } => {
            commands::build_property_directory(transaction_id)?
        }
        CommandSpec::SetName {
            name,
            transaction_id,
        } => commands::build_set_name(&name, transaction_id)?,
        CommandSpec::ResetName { transaction_id } => commands::build_reset_name(transaction_id)?,
        CommandSpec::Receivers {
            page,
            transaction_id,
        } => commands::build_receivers(page, transaction_id)?,
        CommandSpec::Transmitters {
            page,
            friendly_names,
            transaction_id,
        } => commands::build_transmitters(page, friendly_names, transaction_id)?,
        CommandSpec::TransmitterNames {
            channel_count,
            transaction_id,
        } => commands::build_transmitter_names(channel_count, transaction_id)?,
        CommandSpec::ResetChannelName {
            channel_type,
            channel_number,
            transaction_id,
        } => commands::build_reset_channel_name(
            parse_channel_type(&channel_type)?,
            channel_number,
            transaction_id,
        )?,
        CommandSpec::SetChannelName {
            channel_type,
            channel_number,
            name,
            protocol_id,
            transaction_id,
        } => commands::build_set_channel_name_for_protocol(
            protocol_id,
            parse_channel_type(&channel_type)?,
            channel_number,
            &name,
            transaction_id,
        )?,
        CommandSpec::ReceiveChannelNamePage2729 {
            records,
            transaction_id,
        } => {
            let records: Vec<ReceiveChannelNamePageRecord> = records
                .into_iter()
                .map(|entry| ReceiveChannelNamePageRecord {
                    rx_channel_number: entry.rx_channel,
                    name: entry.name,
                })
                .collect();
            commands::build_receive_channel_name_page_2729(&records, transaction_id)?
        }
        CommandSpec::AddSubscriptions {
            subscriptions,
            transaction_id,
        } => {
            let records: Vec<(u16, String, String)> = subscriptions
                .into_iter()
                .map(|entry| (entry.rx_channel, entry.tx_channel, entry.tx_device))
                .collect();
            commands::build_add_subscriptions(&records, transaction_id)?
        }
        CommandSpec::RemoveSubscriptions {
            rx_channels,
            transaction_id,
        } => commands::build_remove_subscriptions(&rx_channels, transaction_id)?,
        CommandSpec::SubscriptionPage2729 {
            records,
            transaction_id,
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
            commands::build_subscription_page_2729(&records, transaction_id)?
        }
        CommandSpec::SetLatency {
            latency,
            transaction_id,
        } => commands::build_set_latency(latency, transaction_id)?,
        CommandSpec::Reboot { host_mac, sequence } => {
            commands::build_reboot(parse_mac(&host_mac, default_host_mac)?, sequence)?
        }
        CommandSpec::FactoryReset { host_mac, sequence } => {
            commands::build_factory_reset(parse_mac(&host_mac, default_host_mac)?, sequence)?
        }
        CommandSpec::Identify { sequence } => commands::build_identify(sequence)?,
        CommandSpec::SetEncoding { encoding, sequence } => {
            commands::build_set_encoding_with_sequence(encoding, sequence)?
        }
        CommandSpec::SetSampleRate {
            sample_rate,
            sequence,
        } => commands::build_set_sample_rate_with_sequence(sample_rate, sequence)?,
        CommandSpec::ProbeSampleRate { host_mac, sequence } => {
            commands::build_probe_sample_rate(parse_mac(&host_mac, default_host_mac)?, sequence)?
        }
        CommandSpec::ProbeEncoding { host_mac, sequence } => {
            commands::build_probe_encoding(parse_mac(&host_mac, default_host_mac)?, sequence)?
        }
        CommandSpec::SetSampleRatePullup {
            raw_value,
            host_mac,
            sequence,
        } => commands::build_set_sample_rate_pullup(
            parse_mac(&host_mac, default_host_mac)?,
            sequence,
            raw_value,
        )?,
        CommandSpec::ProbeSampleRatePullup { host_mac, sequence } => {
            commands::build_probe_sample_rate_pullup(
                parse_mac(&host_mac, default_host_mac)?,
                sequence,
            )?
        }
        CommandSpec::ProbeGainLevel { host_mac, sequence } => {
            commands::build_probe_gain_level(parse_mac(&host_mac, default_host_mac)?, sequence)?
        }
        CommandSpec::SetGainLevel {
            channel_number,
            gain_level,
            device_type,
            host_mac,
            sequence,
        } => commands::build_set_gain_level(
            parse_mac(&host_mac, default_host_mac)?,
            sequence,
            channel_number,
            gain_level,
            parse_gain_device_type(&device_type)?,
        )?,
        CommandSpec::EnableAes67 {
            enabled,
            host_mac,
            sequence,
        } => commands::build_enable_aes67_with_sequence(
            enabled,
            parse_mac(&host_mac, default_host_mac)?,
            sequence,
        )?,
        CommandSpec::ProbeInterfaceStatus { host_mac } => {
            commands::build_probe_interface_status(parse_mac(&host_mac, default_host_mac)?)?
        }
        CommandSpec::ProbeLinkStatus { host_mac, sequence } => {
            commands::build_probe_link_status(parse_mac(&host_mac, default_host_mac)?, sequence)?
        }
        CommandSpec::ProbeSwitchConfiguration { host_mac, sequence } => {
            commands::build_probe_switch_configuration(
                parse_mac(&host_mac, default_host_mac)?,
                sequence,
            )?
        }
        CommandSpec::SetInterfaceDhcp { host_mac, sequence } => {
            commands::build_set_interface_dhcp(parse_mac(&host_mac, default_host_mac)?, sequence)?
        }
        CommandSpec::SetInterfaceStatic {
            ip,
            netmask,
            dns,
            gateway,
            host_mac,
            sequence,
        } => commands::build_set_interface_static(
            parse_required_ipv4_address(&ip)?,
            parse_required_ipv4_address(&netmask)?,
            parse_optional_ipv4_address(&dns)?,
            parse_optional_ipv4_address(&gateway)?,
            parse_mac(&host_mac, default_host_mac)?,
            sequence,
        )?,
        CommandSpec::ProbeAes67 { host_mac, sequence } => {
            commands::build_probe_aes67(parse_mac(&host_mac, default_host_mac)?, sequence)?
        }
        CommandSpec::ProbeLockResetStatus {
            host_mac,
            sequence,
            request_value,
        } => commands::build_probe_lock_reset_status(
            parse_mac(&host_mac, default_host_mac)?,
            sequence,
            request_value,
        )?,
        CommandSpec::DeviceLogExport { host_mac, sequence } => {
            commands::build_device_log_export(parse_mac(&host_mac, default_host_mac)?, sequence)?
        }
        CommandSpec::CapabilityPartitionExport { host_mac, sequence } => {
            commands::build_capability_partition_export(
                parse_mac(&host_mac, default_host_mac)?,
                sequence,
            )?
        }
        CommandSpec::ProbeClearConfigurationStatus { host_mac, sequence } => {
            commands::build_probe_clear_configuration_status(
                parse_mac(&host_mac, default_host_mac)?,
                sequence,
            )?
        }
        CommandSpec::ClearAllConfiguration { host_mac, sequence } => {
            commands::build_clear_all_configuration(
                parse_mac(&host_mac, default_host_mac)?,
                sequence,
            )?
        }
        CommandSpec::ClearAllConfigurationPreservingInternetProtocolSettings {
            host_mac,
            sequence,
        } => commands::build_clear_all_configuration_preserving_internet_protocol_settings(
            parse_mac(&host_mac, default_host_mac)?,
            sequence,
        )?,
        CommandSpec::SetPreferredLeader {
            preferred,
            clock_source,
            host_mac,
            sequence,
        } => commands::build_set_preferred_leader(
            preferred,
            clock_source,
            parse_mac(&host_mac, default_host_mac)?,
            sequence,
        )?,
        CommandSpec::SetClockSource {
            clock_source,
            host_mac,
            sequence,
        } => commands::build_set_clock_source(
            clock_source,
            parse_mac(&host_mac, default_host_mac)?,
            sequence,
        )?,
        CommandSpec::SetClockSubdomain {
            subdomain,
            host_mac,
            sequence,
        } => commands::build_set_clock_subdomain(
            subdomain,
            parse_mac(&host_mac, default_host_mac)?,
            sequence,
        )?,
        CommandSpec::ProbePreferredLeader {
            clock_source,
            host_mac,
            sequence,
        } => commands::build_probe_preferred_leader(
            clock_source,
            parse_mac(&host_mac, default_host_mac)?,
            sequence,
        )?,
        CommandSpec::RefreshClockStatus { host_mac, sequence } => {
            commands::build_refresh_clock_status(parse_mac(&host_mac, default_host_mac)?, sequence)?
        }
        CommandSpec::QueryLatencyConfig { transaction_id } => {
            commands::build_query_latency_config(transaction_id)?
        }
        CommandSpec::SetAes67MulticastPrefix {
            prefix,
            transaction_id,
        } => {
            let address = prefix
                .parse::<Ipv4Addr>()
                .map_err(|_| SpecError::InvalidIp)?;
            commands::build_set_aes67_multicast_prefix(address, transaction_id)?
        }
        CommandSpec::VolumeStart {
            device_name,
            ipv4,
            mac,
            port,
            timeout,
            transaction_id,
        }
        | CommandSpec::MeteringStart {
            device_name,
            ipv4,
            mac,
            port,
            timeout,
            transaction_id,
        } => commands::build_volume_start(
            &device_name,
            parse_optional_ipv4_address(&ipv4)?,
            parse_mac_required(&mac)?,
            port,
            timeout,
            transaction_id,
        )?,
        CommandSpec::VolumeStop { device_name, mac }
        | CommandSpec::MeteringStop { device_name, mac } => {
            commands::build_volume_stop(&device_name, parse_mac_required(&mac)?)?
        }
        CommandSpec::BluetoothStatus { host_mac } => {
            commands::build_bluetooth_status(parse_mac(&host_mac, default_host_mac)?)?
        }
        CommandSpec::MakeModel { mac } => commands::build_make_model(parse_mac_required(&mac)?)?,
        CommandSpec::DanteModel { mac } => commands::build_dante_model(parse_mac_required(&mac)?)?,
        CommandSpec::CmcRegister { sequence, host_mac } => {
            commands::build_cmc_register(sequence, parse_mac_required(&host_mac)?)?
        }
        CommandSpec::QueryTxFlows {
            flow_protocol_id,
            starting_flow,
            transaction_id,
        } => commands::build_query_tx_flows_from(flow_protocol_id, starting_flow, transaction_id)?,
        CommandSpec::QueryTransmitterChannelStatus2809 { transaction_id } => {
            commands::build_query_transmitter_channel_status_2809(transaction_id)?
        }
        CommandSpec::ReconcileTransmitterChannelNames2809 {
            records,
            transaction_id,
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
            transaction_id,
        )?,
        CommandSpec::QueryReceiverChannelStatus2809 { transaction_id } => {
            commands::build_query_receiver_channel_status_2809(transaction_id)?
        }
        CommandSpec::QueryReceiverFlowStatus2809 { transaction_id } => {
            commands::build_query_receiver_flow_status_2809(transaction_id)?
        }
        CommandSpec::QueryReceiverFlows {
            starting_flow,
            transaction_id,
        } => commands::build_query_receiver_flows(starting_flow, transaction_id)?,
        CommandSpec::QueryTransmitChannelCapabilities {
            starting_channel_identifier,
            maximum_channel_count,
            transaction_id,
        } => commands::build_query_transmit_channel_capabilities(
            starting_channel_identifier,
            maximum_channel_count,
            transaction_id,
        )?,
        CommandSpec::QueryReceiverPortRanges { transaction_id } => {
            commands::build_query_receiver_port_ranges(transaction_id)?
        }
        CommandSpec::CreateTxFlow {
            flow_protocol_id,
            flow_slot,
            channels,
            transaction_id,
        } => {
            commands::build_create_tx_flow(flow_protocol_id, flow_slot, &channels, transaction_id)?
        }
        CommandSpec::DeleteTxFlow {
            flow_protocol_id,
            flow_slot,
            transaction_id,
        } => commands::build_delete_tx_flow(flow_protocol_id, flow_slot, transaction_id)?,
    };
    Ok(packet)
}

pub(super) fn build_command_with_default_host_mac(
    json: &str,
    default_host_mac: [u8; 6],
) -> Result<Vec<u8>, SpecError> {
    let spec: CommandSpec = serde_json::from_str(json).map_err(|_| SpecError::InvalidJson)?;
    build_command(spec, default_host_mac)
}
