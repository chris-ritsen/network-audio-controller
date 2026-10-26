use serde_json::{Map, Value, json};

pub(super) fn dante_device(record: &Value) -> Value {
    let mut result = fields(
        record,
        &[
            ("ServerName", "server_name"),
            ("Name", "name"),
            ("MacAddress", "mac_address"),
            ("Ipv4", "ipv4"),
            ("Model", "model"),
            ("ModelId", "model_id"),
            ("Manufacturer", "manufacturer"),
            ("DanteModel", "dante_model"),
            ("PlatformModelName", "platform_model_name"),
            ("LinkSpeedMbps", "link_speed_mbps"),
            ("SampleRate", "sample_rate_hz"),
            ("SupportedSampleRates", "supported_sample_rates_hz"),
            ("Encoding", "encoding"),
            ("SupportedEncodings", "supported_encodings"),
            ("BitDepth", "bit_depth"),
            ("Latency", "latency_ms"),
            ("ActiveLatency", "active_latency_ms"),
            ("ConfiguredLatency", "configured_latency_ms"),
            ("DefaultLatency", "default_latency_ms"),
            ("MinLatency", "min_latency_ms"),
            ("MaxLatency", "max_latency_ms"),
            ("TxCount", "tx_count"),
            ("RxCount", "rx_count"),
            ("Online", "online"),
            ("IsLocked", "is_locked"),
            ("LockStateKnown", "is_locked"),
            (
                "Aes67ConfigurationSupported",
                "aes67_configuration_supported",
            ),
            (
                "Aes67ConfigurationSupportKnown",
                "aes67_configuration_supported",
            ),
            ("Aes67MulticastPrefix", "aes67_multicast_prefix"),
            ("SampleRatePullupRawValue", "sample_rate_pullup_raw_value"),
            (
                "RequestedSampleRatePullupRawValue",
                "requested_sample_rate_pullup_raw_value",
            ),
            (
                "SupportedSampleRatePullupRawValues",
                "supported_sample_rate_pullup_raw_values",
            ),
            (
                "SampleRatePullupKnown",
                "supported_sample_rate_pullup_raw_values",
            ),
            ("LastSeen", "last_seen"),
            (
                "ClockFrequencyOffsetPartsPerBillion",
                "clock_frequency_offset_parts_per_billion",
            ),
            ("ClockSourceCode", "clock_source_code"),
            ("ClockSubdomain", "clock_subdomain"),
            ("ClockRole", "clock_role"),
            ("ClockStatus", "clock_status"),
            ("Ptpv1GrandmasterUuid", "ptpv1_grandmaster_uuid"),
            ("Ptpv1DeviceUuid", "ptpv1_device_uuid"),
            ("Ptpv1MasterUuid", "ptpv1_master_uuid"),
            ("PlatformSoftwareVersion", "platform_software_version"),
            ("PlatformHardwareVersion", "platform_hardware_version"),
            ("PlatformApiVersion", "platform_api_version"),
            ("RomBootVersion", "rom_boot_version"),
            ("ProductVersion", "product_version"),
            ("FriendlyProductVersion", "friendly_product_version"),
            (
                "ManufacturerSoftwareVersion",
                "manufacturer_software_version",
            ),
            (
                "ManufacturerFirmwareVersion",
                "manufacturer_firmware_version",
            ),
            ("CmcServerVersion", "cmc_server_version"),
            ("RouterProtocolVersion", "router_protocol_version"),
            ("DdmProductVersion", "ddm_product_version"),
            ("DdmProductSoftwareVersion", "ddm_product_software_version"),
            ("DdmDanteVersion", "ddm_dante_version"),
            ("DdmDanteHardwareVersion", "ddm_dante_hardware_version"),
        ],
    );
    result["LockStateKnown"] = json!(record["is_locked"].is_boolean());
    result["Aes67Enabled"] = record["aes67_current"].clone();
    result["Aes67ConfigurationSupportKnown"] =
        json!(record["aes67_configuration_supported"].is_boolean());
    result["SampleRatePullupKnown"] =
        json!(record["supported_sample_rate_pullup_raw_values"].is_array());
    result["ClockSubdomain"] = json!(
        netaudio_core::clock_configuration::subdomain_presentation(&record["clock_subdomain"]).text
    );
    result["ClockPortRecords"] = json!(record["clock_port_records"].to_string());
    result["ClockStatus"] = json!(record["clock_status"].to_string());
    result["Subscriptions"] = subscriptions(record);
    result["TransmitterFlows"] = flows(record);
    result
}

pub(super) fn dante_channel(record: &Value) -> Value {
    fields(
        record,
        &[
            ("Number", "number"),
            ("Name", "name"),
            ("FriendlyName", "friendly_name"),
            ("FactoryName", "factory_name"),
            ("StatusText", "status_text"),
            ("Volume", "volume"),
            ("Muted", "muted"),
            ("BitDepth", "bit_depth"),
            ("SamplesPerFrame", "samples_per_frame"),
        ],
    )
}

pub(super) fn shure_device(record: &Value) -> Value {
    fields(
        record,
        &[
            ("Ip", "ip"),
            ("Mac", "mac"),
            ("DeviceType", "device_type"),
            ("Name", "name"),
            ("Model", "model"),
            ("FirmwareVersion", "firmware_version"),
            ("RfBand", "rf_band"),
            ("TransmissionMode", "transmission_mode"),
            ("QuadversityMode", "quadversity_mode"),
            ("EncryptionMode", "encryption_mode"),
            ("Online", "online"),
            ("LastSeen", "last_seen"),
        ],
    )
}

pub(super) fn shure_channel(record: &Value, protocol: &str) -> Value {
    let mut result = fields(
        record,
        &[
            ("Number", "number"),
            ("Name", "name"),
            ("Frequency", "frequency"),
            ("AudioLevelPeak", "audio_level_peak"),
            ("AudioLevelRms", "audio_level_rms"),
            ("SignalQuality", "signal_quality"),
            ("AntennaStatus", "antenna_status"),
            ("Active", "active"),
        ],
    );
    let (gain, mute) = if protocol.eq_ignore_ascii_case("p10t") {
        ("audio_in_level", "rf_mute")
    } else {
        ("audio_gain", "audio_mute")
    };
    result["AudioGain"] = record[gain].clone();
    result["AudioMute"] = record[mute].clone();
    for (public, field) in [
        ("TransmitterModel", "model"),
        ("TransmitterConnected", "connected"),
        ("BatteryMinutes", "battery_minutes"),
        ("BatteryChargePercent", "battery_charge_percent"),
    ] {
        result[public] = record["transmitter"][field].clone();
    }
    result
}

fn fields(record: &Value, fields: &[(&str, &str)]) -> Value {
    Value::Object(
        fields
            .iter()
            .map(|(name, field)| (name.to_string(), record[field].clone()))
            .collect::<Map<_, _>>(),
    )
}

fn subscriptions(record: &Value) -> Value {
    let Some(subscriptions) = record["subscriptions"].as_array() else {
        return Value::Null;
    };
    json!(
        subscriptions
            .iter()
            .map(|value| json!([
                record["server_name"],
                value["rx_channel"],
                value["tx_device"],
                value["tx_channel"],
                value["status"]["code"],
            ]))
            .collect::<Vec<_>>()
    )
}

fn flows(record: &Value) -> Value {
    let Some(flows) = record["transmitter_flows"].as_array() else {
        return Value::Null;
    };
    json!(
        flows
            .iter()
            .map(|flow| json!([
                flow["flow_number"],
                flow.get("channel_count")
                    .cloned()
                    .unwrap_or_else(|| json!(flow["channels"].as_array().map(Vec::len))),
                flow["flow_type"],
                flow["sample_rate"],
                flow["encoding"],
                flow["destination_internet_protocol_version_four_address"],
                flow["destination_user_datagram_port"],
                flow["subscriber_device_name"].as_str().unwrap_or(""),
                flow["subscriber_flow_name"].as_str().unwrap_or(""),
            ]))
            .collect::<Vec<_>>()
    )
}
