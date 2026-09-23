//! Validation of configuration intent independent of a preset file format or transport.

use std::net::Ipv4Addr;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroU8};

use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Map, Value};

use crate::network::{DanteRedundancyMode, InterfaceConfigurationRequest};

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConfigurationRequest {
    Validate { values: Map<String, Value> },
    CapturePanel { fresh_values: Map<String, Value> },
}

pub fn resolve(request: ConfigurationRequest) -> Result<Value, String> {
    match request {
        ConfigurationRequest::Validate { values } => {
            validate(Value::Object(values))?;
            Ok(Value::Null)
        }
        ConfigurationRequest::CapturePanel { fresh_values } => Ok(Value::Object(
            fresh_values
                .into_iter()
                .filter_map(|(category, value)| {
                    panel_setting(&category, &value).map(|setting| (category, setting))
                })
                .collect(),
        )),
    }
}

pub(crate) fn panel_setting(category: &str, value: &Value) -> Option<Value> {
    let field = |name| value.get(name).filter(|value| !value.is_null());

    Some(match category {
        "bluetooth_identification" | "serial" if value.is_object() => value.clone(),
        "bluetooth_discovery" => match value.as_u64()? {
            1 => json!(true),
            2 => json!(false),
            _ => return None,
        },
        "video_format" => json!({"format": field("configured")?, "selection": field("selection")?}),
        "codec_format" => field("current")?.clone(),
        "bandwidth" => match field("enabled")?.as_u64()? {
            0 => json!({"target": 0, "enabled": false}),
            1 => json!({"target": field("target")?, "enabled": true}),
            _ => return None,
        },
        "hdcp" => json!({"mode": field("configured_mode")?}),
        _ => return None,
    })
}

fn decode<T: DeserializeOwned>(value: &Value, field: &str) -> Result<T, String> {
    serde_json::from_value(value.clone()).map_err(|error| format!("{field}: {error}"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceValues {
    pub latency_microseconds: u64,
    pub frames_per_packet: u16,
}

#[derive(Deserialize)]
pub struct ExternalIdentity {
    pub source_ipv4: Ipv4Addr,
    pub session_id: NonZeroU64,
}

#[derive(Deserialize)]
pub struct ExternalEndpoint {
    pub ipv4_address: Option<Ipv4Addr>,
    pub udp_port: NonZeroU16,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Subscription {
    NativeDante {
        tx_channel: String,
        tx_device: String,
    },
    ExternalRtp {
        flow_identity: ExternalIdentity,
        flow_slot: NonZeroU16,
        interface_endpoints: Option<Vec<ExternalEndpoint>>,
        receiver_supports_multiple_interfaces: Option<bool>,
    },
}

#[derive(Deserialize)]
pub struct Gain {
    pub channel: NonZeroU16,
    pub level: NonZeroU8,
    pub device_type: String,
}

fn validate_subscription(value: &Value) -> Result<(), String> {
    if value.is_null() {
        return Ok(());
    }

    match decode(value, "rx_subscriptions")? {
        Subscription::NativeDante {
            tx_channel,
            tx_device,
        } => {
            if tx_channel.is_empty() || tx_device.is_empty() {
                return Err(
                    "native receiver subscriptions require tx_channel and tx_device".into(),
                );
            }
        }
        Subscription::ExternalRtp {
            interface_endpoints,
            ..
        } => {
            if let Some(endpoints) = interface_endpoints {
                if !(1..=2).contains(&endpoints.len()) {
                    return Err(
                        "external RTP interface_endpoints must contain one or two destinations"
                            .into(),
                    );
                }
            }
        }
    }

    Ok(())
}

pub fn validate(values: Value) -> Result<(), String> {
    let values: Map<String, Value> = decode(&values, "configuration")?;

    for (field, value) in &values {
        match field.as_str() {
            "preferred_leader"
            | "external_word_clock"
            | "global_unicast_delay_requests"
            | "aggregate_ptpv1_unicast_delay_requests" => {
                decode::<Option<bool>>(value, field)?;
            }
            "sample_rate" | "encoding" => {
                decode::<NonZeroU32>(value, field)?;
            }
            "sample_rate_pullup" => {
                decode::<u32>(value, field)?;
            }
            "clock_source_code" | "receive_flow_default_slots" => {
                decode::<u16>(value, field)?;
            }
            "clock_subdomain" => {
                crate::clock_configuration::normalize_subdomain(value)?;
            }
            "redundancy_mode" => {
                decode::<DanteRedundancyMode>(value, field)?;
            }
            "receive_flow_performance" | "transmit_flow_performance" | "unicast_performance" => {
                let values: PerformanceValues = decode(value, field)?;
                crate::commands::latency_nanoseconds(values.latency_microseconds)
                    .map_err(|error| format!("{field}: {error}"))?;
            }
            "codec_gain" => {
                for gain in decode::<Vec<Gain>>(value, field)? {
                    crate::spec::parse_gain_device_type(&gain.device_type)
                        .map_err(|error| error.to_string())?;
                }
            }
            "transmitter_channel_names" | "receiver_channel_names" | "rx_subscriptions" => {
                let channels: Map<String, Value> = decode(value, field)?;

                for (number, value) in channels {
                    number
                        .parse::<NonZeroU16>()
                        .map_err(|_| format!("{field}: invalid channel identifier"))?;

                    if field == "rx_subscriptions" {
                        validate_subscription(&value)?;
                    } else {
                        decode::<String>(&value, field)?;
                    }
                }
            }
            "interfaces" => {
                for interface in decode::<Vec<Value>>(value, field)? {
                    if interface.get("mode").and_then(Value::as_str) != Some("static") {
                        continue;
                    }

                    let fields = ["mode", "ip_address", "netmask", "gateway", "dns_server"];
                    let configuration = Value::Object(
                        fields
                            .into_iter()
                            .filter_map(|name| {
                                interface
                                    .get(name)
                                    .map(|value| (name.to_owned(), value.clone()))
                            })
                            .collect(),
                    );
                    decode::<InterfaceConfigurationRequest>(&configuration, field)?
                        .normalize()
                        .map_err(|error| error.to_string())?;
                }
            }
            _ => {}
        }
    }

    Ok(())
}
