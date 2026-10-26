use serde::de::DeserializeOwned;
use serde_json::Value;

fn read<T: DeserializeOwned>(record: &Value, name: &str) -> zbus::fdo::Result<T> {
    serde_json::from_value(record[name].clone())
        .map_err(|_| zbus::fdo::Error::Failed(format!("{name} has no current observation")))
}

macro_rules! record_interface {
    ($name:ident, $interface:literal, $(($method:ident, $property:literal, $value:ty)),* $(,)?) => {
        pub(super) struct $name {
            pub record: Value,
        }

        #[zbus::interface(name = $interface)]
        impl $name {
            $(
                #[zbus(property, name = $property)]
                fn $method(&self) -> zbus::fdo::Result<$value> {
                    read(&self.record, $property)
                }
            )*
        }
    };
}

pub(super) struct Manager {
    pub record: Value,
}

#[zbus::interface(name = "com.netaudio.Manager")]
impl Manager {
    #[zbus(property)]
    fn dante_device_count(&self) -> zbus::fdo::Result<u32> {
        read(&self.record, "DanteDeviceCount")
    }

    #[zbus(property)]
    fn shure_device_count(&self) -> zbus::fdo::Result<u32> {
        read(&self.record, "ShureDeviceCount")
    }

    #[zbus(property)]
    fn dante_devices(&self) -> zbus::fdo::Result<Vec<String>> {
        read(&self.record, "DanteDevices")
    }

    #[zbus(property)]
    fn shure_devices(&self) -> zbus::fdo::Result<Vec<String>> {
        read(&self.record, "ShureDevices")
    }

    #[zbus(signal)]
    pub async fn dante_device_added(
        emitter: &zbus::object_server::SignalEmitter<'_>,
        identity: &str,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn dante_device_removed(
        emitter: &zbus::object_server::SignalEmitter<'_>,
        identity: &str,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn shure_device_added(
        emitter: &zbus::object_server::SignalEmitter<'_>,
        identity: &str,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn shure_device_removed(
        emitter: &zbus::object_server::SignalEmitter<'_>,
        identity: &str,
    ) -> zbus::Result<()>;
}

record_interface!(DanteDevice, "com.netaudio.DanteDevice",
    (server_name, "ServerName", String),
    (name, "Name", String),
    (mac_address, "MacAddress", String),
    (ipv4, "Ipv4", String),
    (model, "Model", String),
    (model_id, "ModelId", String),
    (manufacturer, "Manufacturer", String),
    (dante_model, "DanteModel", String),
    (platform_model_name, "PlatformModelName", String),
    (link_speed_mbps, "LinkSpeedMbps", u32),
    (sample_rate, "SampleRate", u32),
    (supported_sample_rates, "SupportedSampleRates", Vec<u32>),
    (encoding, "Encoding", u32),
    (supported_encodings, "SupportedEncodings", Vec<u32>),
    (bit_depth, "BitDepth", u32),
    (latency, "Latency", f64),
    (active_latency, "ActiveLatency", f64),
    (configured_latency, "ConfiguredLatency", f64),
    (default_latency, "DefaultLatency", f64),
    (min_latency, "MinLatency", f64),
    (max_latency, "MaxLatency", f64),
    (tx_count, "TxCount", u32),
    (rx_count, "RxCount", u32),
    (online, "Online", bool),
    (is_locked, "IsLocked", bool),
    (lock_state_known, "LockStateKnown", bool),
    (aes67_enabled, "Aes67Enabled", bool),
    (aes67_configuration_supported, "Aes67ConfigurationSupported", bool),
    (aes67_configuration_support_known, "Aes67ConfigurationSupportKnown", bool),
    (aes67_multicast_prefix, "Aes67MulticastPrefix", String),
    (sample_rate_pullup_raw_value, "SampleRatePullupRawValue", u32),
    (requested_sample_rate_pullup_raw_value, "RequestedSampleRatePullupRawValue", u32),
    (supported_sample_rate_pullup_raw_values, "SupportedSampleRatePullupRawValues", Vec<u32>),
    (sample_rate_pullup_known, "SampleRatePullupKnown", bool),
    (transmitter_flows, "TransmitterFlows", Vec<(u32, u32, String, u32, u32, String, u32, String, String)>),
    (last_seen, "LastSeen", f64),
    (clock_frequency_offset_parts_per_billion, "ClockFrequencyOffsetPartsPerBillion", i32),
    (clock_source_code, "ClockSourceCode", u16),
    (clock_subdomain, "ClockSubdomain", String),
    (clock_role, "ClockRole", String),
    (clock_port_records, "ClockPortRecords", String),
    (clock_status, "ClockStatus", String),
    (ptpv1_grandmaster_uuid, "Ptpv1GrandmasterUuid", String),
    (ptpv1_device_uuid, "Ptpv1DeviceUuid", String),
    (ptpv1_master_uuid, "Ptpv1MasterUuid", String),
    (platform_software_version, "PlatformSoftwareVersion", String),
    (platform_hardware_version, "PlatformHardwareVersion", String),
    (platform_api_version, "PlatformApiVersion", String),
    (rom_boot_version, "RomBootVersion", String),
    (product_version, "ProductVersion", String),
    (friendly_product_version, "FriendlyProductVersion", String),
    (manufacturer_software_version, "ManufacturerSoftwareVersion", String),
    (manufacturer_firmware_version, "ManufacturerFirmwareVersion", String),
    (cmc_server_version, "CmcServerVersion", String),
    (router_protocol_version, "RouterProtocolVersion", String),
    (ddm_product_version, "DdmProductVersion", String),
    (ddm_product_software_version, "DdmProductSoftwareVersion", String),
    (ddm_dante_version, "DdmDanteVersion", String),
    (ddm_dante_hardware_version, "DdmDanteHardwareVersion", String),
    (subscriptions, "Subscriptions", Vec<(String, String, String, String, u16)>),
);

record_interface!(
    DanteChannel,
    "com.netaudio.DanteChannel",
    (number, "Number", u32),
    (name, "Name", String),
    (friendly_name, "FriendlyName", String),
    (factory_name, "FactoryName", String),
    (status_text, "StatusText", String),
    (volume, "Volume", u32),
    (muted, "Muted", bool),
    (bit_depth, "BitDepth", u32),
    (samples_per_frame, "SamplesPerFrame", u32),
);

record_interface!(
    ShureDevice,
    "com.netaudio.ShureDevice",
    (ip, "Ip", String),
    (mac, "Mac", String),
    (device_type, "DeviceType", String),
    (name, "Name", String),
    (model, "Model", String),
    (firmware_version, "FirmwareVersion", String),
    (rf_band, "RfBand", String),
    (transmission_mode, "TransmissionMode", String),
    (quadversity_mode, "QuadversityMode", String),
    (encryption_mode, "EncryptionMode", String),
    (online, "Online", bool),
    (last_seen, "LastSeen", f64),
);

record_interface!(
    ShureChannel,
    "com.netaudio.ShureChannel",
    (number, "Number", u32),
    (name, "Name", String),
    (frequency, "Frequency", u32),
    (audio_gain, "AudioGain", i32),
    (audio_mute, "AudioMute", bool),
    (audio_level_peak, "AudioLevelPeak", i32),
    (audio_level_rms, "AudioLevelRms", i32),
    (signal_quality, "SignalQuality", u32),
    (antenna_status, "AntennaStatus", String),
    (active, "Active", bool),
    (transmitter_model, "TransmitterModel", String),
    (transmitter_connected, "TransmitterConnected", bool),
    (battery_minutes, "BatteryMinutes", u32),
    (battery_charge_percent, "BatteryChargePercent", u32),
);
