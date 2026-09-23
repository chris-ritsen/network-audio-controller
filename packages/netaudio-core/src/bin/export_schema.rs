use netaudio_core::heartbeat_connection_health;
use netaudio_core::performance_configuration;
use netaudio_core::{
    capabilities, network, sample_rate_topology, subscription_readback, subscription_reconciliation,
};
use schemars::{generate::SchemaSettings, JsonSchema};
use serde_json::{json, Value};

macro_rules! registry {
    ({ $($name:literal: $schema:expr),* $(,)? }) => {
        serde_json::Map::from_iter([$(($name.to_owned(), $schema)),*])
    };
}

fn output<T: JsonSchema>() -> Value {
    serde_json::to_value(
        SchemaSettings::draft2020_12()
            .for_serialize()
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .expect("schema is serializable")
}

fn main() {
    use netaudio_core::protocol::{
        MULTICAST_GROUP_CONTROL_MONITORING, MULTICAST_GROUP_HEARTBEAT, SERVICE_ARC, SERVICE_CHAN,
        SERVICE_CMC, SERVICE_DBC, SERVICE_VIDEO,
    };
    let outputs = registry!({
        "DiagnosticAudioCapabilities": output::<netaudio_core::diagnostic_audio::DiagnosticAudioCapabilities>(),
        "ExportProgress": output::<netaudio_core::conmon_export::ExportProgress<'static>>(),
        "ClockControlAvailability": output::<netaudio_core::commands::ClockControlAvailability>(),
        "ManagedSessionStep": output::<netaudio_core::managed_session::ManagedSessionStep>(),
        "FlowCreatePreflight": output::<netaudio_core::flow_readback::FlowCreatePreflight>(),
        "FlowDeletePreflight": output::<netaudio_core::flow_readback::FlowDeletePreflight>(),
        "InterfaceRedundancyResult": output::<network::InterfaceRedundancyResult>(),
        "InventoryState": output::<netaudio_core::inventory::InventoryState>(),
        "LatencyConfiguration": output::<netaudio_core::latency_configuration::LatencyConfiguration>(),
        "CommandReceipt": output::<netaudio_core::responses::CommandReceipt>(),
        "ExternalSubscriptionReadback": output::<netaudio_core::spec::ExternalSubscriptionReadback>(),
        "ManagedCommand": output::<netaudio_core::spec::ManagedCommand>(),
        "TopologyImpact": output::<sample_rate_topology::TopologyImpact>(),
        "ObservedTransmitFlowSpecification": output::<netaudio_core::flow_readback::ObservedTransmitFlowSpecification>(),
        "ReceiverCapabilities": output::<netaudio_core::receiver_capabilities::ReceiverCapabilities>(),
        "FlowTopology": output::<netaudio_core::flow_readback::FlowTopology>(),
        "NetworkControlState": output::<network::NetworkControlState>(),
        "GainMetadata": output::<netaudio_core::responses::GainMetadata>(),
        "AnalogLevelPlan": output::<netaudio_core::responses::AnalogLevelPlan>(),
        "AudioReadbackResult": output::<netaudio_core::responses::AudioReadbackResult>(),
        "FlowAuthoringCapabilities": output::<netaudio_core::parser::FlowAuthoringCapabilities>(),
        "ChannelAudioPublication": output::<netaudio_core::parser::ChannelAudioPublication>(),
        "ArcProtocol": output::<netaudio_core::protocol::ArcProtocol>(),
        "ServiceAdvertisement": output::<netaudio_core::discovery::ServiceAdvertisement>(),
        "ClockPlan": output::<netaudio_core::clock_configuration::ClockPlan>(),
        "SubscriptionStatus": output::<netaudio_core::subscription_status::SubscriptionStatus>(),
        "SubscriptionClassification": output::<netaudio_core::subscription_status::SubscriptionClassification>(),
        "ManagedSubscriptionStatus": output::<netaudio_core::subscription_status::ManagedSubscriptionStatus>(),
        "ClockSubdomainPresentation": output::<netaudio_core::clock_configuration::ClockSubdomainPresentation>(),
        "FlowVerification": output::<netaudio_core::flow_readback::FlowVerification>(),
        "FlowTopologyChange": output::<netaudio_core::flow_readback::FlowTopologyChange>(),
        "FlowCandidate": output::<netaudio_core::flow_readback::FlowCandidate>(),
        "FlowComparison": output::<netaudio_core::flow_readback::FlowComparison>(),
        "FlowCommandPlan": output::<netaudio_core::flow_plan::FlowCommandPlan>(),
        "SettingsCapabilities": output::<capabilities::SettingsCapabilities>(),
        "ClockSources": output::<netaudio_core::clock_configuration::ClockSources>(),
        "LatencyCompletion": output::<netaudio_core::latency_configuration::LatencyCompletion>(),
        "ManagedSettingsResult": output::<netaudio_core::dapi::SettingsExchangeResult>(),
        "ManagedArcResponse": output::<netaudio_core::dapi::ArcResponse>(),
        "PanelPlan": output::<netaudio_core::device_controls::planning::PanelPlan>(),
        "PanelPresentation": output::<netaudio_core::device_controls::presentation::PanelPresentation>(),
        "ControllerApiRoutes": output::<netaudio_core::controller_api::ControllerApiRoutes>(),
        "ControllerEndpoints": output::<netaudio_core::controller_api::ControllerEndpoints>(),
        "ControllerLoginResult": output::<Option<netaudio_core::controller_api::ControllerLogin>>(),
        "PanelProfile": output::<netaudio_core::device_controls::PanelProfile>(),
        "InterfaceConfiguration": output::<network::InterfaceConfiguration>(),
        "SdpDocument": output::<netaudio_core::sdp::SdpDocument>(),
        "SapAnnouncement": output::<netaudio_core::sap::SapAnnouncement>(),
        "SapTransitionResult": output::<Option<netaudio_core::sap::SapTransition>>(),
        "PerformanceCompletion": output::<performance_configuration::PerformanceCompletion>(),
        "PerformanceCapabilities": output::<performance_configuration::PerformanceCapabilities>(),
        "PerformanceSnapshot": output::<performance_configuration::PerformanceSnapshot>(),
        "ConnectionHealthUpdate": output::<heartbeat_connection_health::ConnectionHealthUpdate>(),
        "Availability": output::<capabilities::Availability>(),
        "ChannelCapacity": output::<sample_rate_topology::ChannelCapacity>(),
        "SampleRateStatus": output::<sample_rate_topology::SampleRateStatus>(),
        "RedundancyControl": output::<network::RedundancyControl>(),
        "SubscriptionReadback": output::<subscription_readback::SubscriptionReadback>(),
        "SubscriptionPlan": output::<subscription_reconciliation::Plan>(),
    });
    let inputs = registry!({
        "ExportConfiguration": input::<netaudio_core::conmon_export::ExportConfiguration>(),
        "ConmonExportFragment": input::<netaudio_core::responses::ConmonExportFragment>(),
        "ExternalSubscriptionPlanRequest": input::<netaudio_core::spec::ExternalSubscriptionPlanRequest>(),
        "ManagedCredential": input::<netaudio_core::dapi::ManagedCredential>(),
        "ManagedSessionRequest": input::<netaudio_core::managed_session::ManagedSessionRequest>(),
        "FlowCreatePreflightRequest": input::<netaudio_core::flow_readback::FlowCreatePreflightRequest>(),
        "FlowDeletePreflightRequest": input::<netaudio_core::flow_readback::FlowDeletePreflightRequest>(),
        "ExternalReadbackRequest": input::<netaudio_core::spec::ExternalReadbackRequest>(),
        "ManagedCommandRequest": input::<netaudio_core::spec::ManagedCommandRequest>(),
        "TopologyImpactRequest": input::<sample_rate_topology::TopologyImpactRequest>(),
        "TopologyReadbackRequest": input::<sample_rate_topology::TopologyReadbackRequest>(),
        "ReceiverCapabilityRequest": input::<netaudio_core::receiver_capabilities::ReceiverCapabilityRequest>(),
        "FlowReadbackRequest": input::<netaudio_core::flow_readback::FlowReadbackRequest>(),
        "NetworkControlFacts": input::<network::NetworkControlFacts>(),
        "AnalogAccess": input::<netaudio_core::responses::AnalogAccess>(),
        "AnalogLevelRequest": input::<netaudio_core::responses::AnalogLevelRequest>(),
        "AudioCapabilityReadback": input::<netaudio_core::responses::AudioCapabilityReadback>(),
        "ChannelAudioConfiguration": input::<netaudio_core::parser::ChannelAudioConfiguration>(),
        "VirtualDeviceAdvertisement": input::<netaudio_core::discovery::VirtualDeviceAdvertisement>(),
        "FlowVerificationRequest": input::<netaudio_core::flow_readback::FlowVerificationRequest>(),
        "FlowTopologyChangeRequest": input::<netaudio_core::flow_readback::FlowTopologyChangeRequest>(),
        "FlowCandidateRequest": input::<netaudio_core::flow_readback::FlowCandidateRequest>(),
        "FlowCreateRequest": input::<netaudio_core::flow_plan::FlowCreateRequest>(),
        "FlowDeviceFacts": input::<netaudio_core::flow_plan::FlowDeviceFacts>(),
        "FlowDeleteRequest": input::<netaudio_core::flow_plan::FlowDeleteRequest>(),
        "SettingsCapabilityFacts": input::<capabilities::SettingsCapabilityFacts>(),
        "ClockSourceFacts": input::<netaudio_core::clock_configuration::ClockSourceFacts>(),
        "DeviceIdentityRequest": input::<netaudio_core::device_identity::DeviceIdentityRequest>(),
        "ConfigurationRequest": input::<netaudio_core::configuration::ConfigurationRequest>(),
        "LatencyControl": input::<netaudio_core::latency_configuration::LatencyControl>(),
        "ManagedSettingsRequest": input::<netaudio_core::dapi::SettingsExchangeRequest>(),
        "ManagedArcCorrelationRequest": input::<netaudio_core::dapi::ArcCorrelationRequest>(),
        "PanelProfileRequest": input::<netaudio_core::device_controls::PanelProfileRequest>(),
        "PanelPlanRequest": input::<netaudio_core::device_controls::planning::PanelPlanRequest>(),
        "PanelPresentationRequest": input::<netaudio_core::device_controls::presentation::PanelPresentationRequest>(),
        "ManagedStatusRequest": input::<netaudio_core::subscription_status::ManagedStatusRequest>(),
        "FlowInventoryProtocolFacts": input::<netaudio_core::protocol::FlowInventoryProtocolFacts>(),
        "ExternalSubscriptionCommand": input::<netaudio_core::spec::ExternalSubscriptionCommand>(),
        "SapTransitionRequest": input::<netaudio_core::sap::SapTransitionRequest>(),
        "PanelReadbackRequest": input::<netaudio_core::device_controls::planning::PanelReadbackRequest>(),
        "InterfaceRedundancyObservation": input::<network::InterfaceRedundancyObservation>(),
        "InterfaceConfigurationRequest": input::<network::InterfaceConfigurationRequest>(),
        "InterfaceReadbackRequest": input::<network::InterfaceReadbackRequest>(),
        "PerformanceCompletionRequest": input::<performance_configuration::PerformanceCompletionRequest>(),
        "PerformanceFacts": input::<performance_configuration::PerformanceFacts>(),
        "PerformanceSnapshotFacts": input::<performance_configuration::PerformanceSnapshotFacts>(),
        "ConnectionHealthUpdateRequest": input::<heartbeat_connection_health::UpdateRequest>(),
        "AvailabilityRequest": input::<capabilities::AvailabilityRequest>(),
        "SampleRateStatus": input::<sample_rate_topology::SampleRateStatus>(),
        "ChannelCapacity": input::<sample_rate_topology::ChannelCapacity>(),
        "RedundancyControlRequest": input::<network::RedundancyControlRequest>(),
        "SubscriptionReadbackRequest": input::<subscription_readback::Request>(),
        "SubscriptionReconciliationRequest": input::<subscription_reconciliation::Request>(),
    });
    let schemas = json!({
        "inputs": inputs,
        "outputs": outputs,
        "protocols": netaudio_core::protocol::protocol_catalog(),
        "discovery": {
            "CONTROLLER_AUTH_PORT": netaudio_core::controller_api::AUTH_PORT,
            "CONTROLLER_VERSIONS_PATH": netaudio_core::controller_api::VERSIONS_PATH,
            "SAP_MULTICAST_ADDRESS": netaudio_core::sap::MULTICAST_ADDRESS,
            "SAP_PORT": netaudio_core::sap::PORT,
            "SERVICE_ARC": SERVICE_ARC, "SERVICE_CHAN": SERVICE_CHAN, "SERVICE_CMC": SERVICE_CMC,
            "SERVICE_DBC": SERVICE_DBC, "SERVICE_VIDEO": SERVICE_VIDEO,
            "SERVICES": [SERVICE_ARC, SERVICE_CHAN, SERVICE_CMC, SERVICE_DBC, SERVICE_VIDEO],
            "MEDIA_SERVICE_TYPES": [SERVICE_CHAN, SERVICE_VIDEO],
            "MULTICAST_GROUP_HEARTBEAT": MULTICAST_GROUP_HEARTBEAT,
            "MULTICAST_GROUP_CONTROL_MONITORING": MULTICAST_GROUP_CONTROL_MONITORING,
        },
        "audio_choices": capabilities::sample_rate_pullup_choices(),
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&schemas).expect("schema is serializable")
    );
}

fn input<T: JsonSchema>() -> Value {
    serde_json::to_value(
        SchemaSettings::draft2020_12()
            .for_deserialize()
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .expect("schema is serializable")
}
