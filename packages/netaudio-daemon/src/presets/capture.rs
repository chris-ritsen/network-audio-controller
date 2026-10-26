use super::{Error, Service, device_identifiers};
use crate::devices::registry::DeviceRegistry;
use anyhow::{Context, Result, ensure};
use netaudio_core::presets::{Document, XmlIdentity};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

pub struct CaptureRequest {
    pub name: String,
    pub identifiers: Vec<String>,
    pub sections: BTreeSet<String>,
}

pub struct CapturedDevice {
    pub configuration: Map<String, Value>,
    pub identity: XmlIdentity,
    pub omitted: Vec<Value>,
}

impl CaptureRequest {
    pub fn parse(parameters: &Value) -> Result<Self, Error> {
        let name = parameters["name"]
            .as_str()
            .filter(|name| !name.trim().is_empty() && name.chars().count() <= 120)
            .ok_or("Enter a preset name of 1–120 characters.")?
            .trim()
            .to_owned();
        let identifiers = device_identifiers(&parameters["devices"], false)?;
        let values = parameters["sections"]
            .as_array()
            .filter(|values| !values.is_empty())
            .ok_or("Choose routing, audio, network, or device-control settings to save.")?;
        let mut sections = BTreeSet::new();

        for section in values {
            let section = section
                .as_str()
                .filter(|section| {
                    matches!(
                        *section,
                        "routing" | "audio" | "network" | "device_controls"
                    )
                })
                .ok_or("Choose routing, audio, network, or device-control settings to save.")?;
            sections.insert(section.to_owned());
        }

        Ok(Self {
            name,
            identifiers,
            sections,
        })
    }
}

impl Service {
    pub async fn capture(
        &self,
        parameters: &Value,
        devices: &Arc<DeviceRegistry>,
    ) -> Result<Value, Error> {
        let request = CaptureRequest::parse(parameters)?;
        let _operation = self
            .operation
            .try_lock()
            .map_err(|_| Error::Conflict("Another preset operation is in progress.".into()))?;
        let captured =
            tokio::time::timeout(Duration::from_secs(60), devices.capture_preset(&request))
                .await
                .map_err(|_| {
                    Error::Conflict(
                        "Preset was not saved: fresh capture exceeded its 60-second deadline."
                            .into(),
                    )
                })??;
        self.blocking(move || {
            let mut document = Document::new(&request.name);
            let mut omitted = Vec::new();

            for device in captured {
                document.insert(Value::Object(device.configuration), device.identity)?;
                omitted.extend(device.omitted);
            }

            let content = document.to_xml()?;
            let filename: String = request
                .name
                .chars()
                .map(|character| {
                    if character.is_alphanumeric() || matches!(character, '_' | ' ' | '.' | '-') {
                        character
                    } else {
                        '_'
                    }
                })
                .collect::<String>()
                .trim_matches([' ', '.'])
                .chars()
                .take(100)
                .collect();
            let filename = if filename.is_empty() {
                "preset"
            } else {
                &filename
            };
            Ok(json!({
                "name": request.name,
                "filename": format!("{filename}.xml"),
                "xml": content,
                "device_count": document.devices().len(),
                "saved_at": chrono::Utc::now().to_rfc3339(),
                "omitted": omitted,
            }))
        })
        .await
        .map_err(|error| match error {
            Error::Invalid(message) => Error::Conflict(format!("Preset was not saved: {message}")),
            error => error,
        })
    }
}

pub fn identity(record: &Value) -> CapturedDevice {
    let mut identity = Map::from_iter([("server_name".into(), record["server_name"].clone())]);

    for field in [
        "cmc_source_identifier",
        "cmc_process",
        "mac_address",
        "inventory_id",
        "product_name",
        "platform_model_name",
        "platform_model_identifier",
        "platform_software_version",
        "platform_hardware_version",
        "platform_api_version",
        "rom_boot_version",
        "product_version",
        "friendly_product_version",
        "manufacturer_software_version",
        "manufacturer_firmware_version",
        "cmc_server_version",
        "router_protocol_version",
        "ddm_product_version",
        "ddm_product_software_version",
        "ddm_dante_version",
        "ddm_dante_hardware_version",
    ] {
        if let Some(value) = record.get(field).filter(|value| !value.is_null()) {
            identity.insert(field.into(), value.clone());
        }
    }

    if let Some(sources) = record.get("field_sources").and_then(Value::as_object) {
        let sources: Map<_, _> = sources
            .iter()
            .filter(|(field, _)| identity.contains_key(*field))
            .map(|(field, value)| (field.clone(), value.clone()))
            .collect();
        if !sources.is_empty() {
            identity.insert("field_sources".into(), json!(sources));
        }
    }

    let text = |fields: &[&str]| {
        fields.iter().find_map(|field| {
            record
                .get(*field)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        })
    };
    CapturedDevice {
        configuration: Map::from_iter([
            ("name".into(), record["name"].clone()),
            ("device_name".into(), record["name"].clone()),
            ("device_identity".into(), json!(identity)),
        ]),
        identity: XmlIdentity {
            server_name: record["server_name"].as_str().map(str::to_owned),
            mac_address: text(&["mac_address"]),
            manufacturer: text(&["manufacturer"]),
            model_identifier: text(&["model_id"]),
            model_name: text(&[
                "product_name",
                "model",
                "platform_model_name",
                "dante_model",
                "model_id",
            ]),
            model_version: text(&["friendly_product_version", "product_version"]),
        },
        omitted: Vec::new(),
    }
}

pub fn routing(
    configuration: &mut Map<String, Value>,
    record: &Value,
    transmit_flows: &Value,
) -> Result<()> {
    for (field, channels) in [
        (
            "receiver_channel_names",
            record["channels"]["receivers"]
                .as_object()
                .context("receiver inventory unavailable")?,
        ),
        (
            "transmitter_channel_names",
            record["channels"]["transmitters"]
                .as_object()
                .context("transmitter inventory unavailable")?,
        ),
    ] {
        let mut names = Map::new();
        for (number, channel) in channels {
            ensure!(
                channel
                    .get("media_type")
                    .or_else(|| channel.get("ddm_media_type"))
                    .and_then(Value::as_str)
                    .is_none_or(|media| media.eq_ignore_ascii_case("audio")),
                "non-audio channel capture requires media-qualified preset support"
            );
            let name = channel
                .get("friendly_name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .or_else(|| channel.get("name").and_then(Value::as_str))
                .context("channel label is unavailable")?;
            names.insert(number.to_string(), json!(name));
        }
        if !names.is_empty() {
            configuration.insert(field.into(), json!(names));
        }
    }

    let mut subscriptions: Map<String, Value> = record["channels"]["receivers"]
        .as_object()
        .context("receiver inventory unavailable")?
        .keys()
        .map(|number| (number.to_string(), Value::Null))
        .collect();
    let receiver_flows = record
        .get("receiver_flows")
        .and_then(Value::as_array)
        .context("fresh receiver-flow inventory is unavailable")?;
    for flow in receiver_flows {
        ensure!(
            flow["endpoint_decoding_complete"] == true,
            "receiver-flow endpoints cannot be represented losslessly"
        );
        if let Some(identity) = flow
            .get("external_identity")
            .filter(|value| !value.is_null())
        {
            ensure!(
                identity["source_ipv4"].is_string()
                    && identity["session_id"]
                        .as_u64()
                        .is_some_and(|value| value > 0)
                    && identity["inconsistent_presence_mask"] == 0,
                "external receiver-flow identity is incomplete or inconsistent"
            );
        }
        for identity in flow["effective_subscription_identities"]
            .as_array()
            .context("receiver-flow subscription identities are unavailable")?
        {
            let number = identity["receiver_channel"]
                .as_u64()
                .context("external receiver channel identity is unavailable")?;
            let entry = subscriptions
                .get_mut(&number.to_string())
                .context("external receiver channel is absent from the channel inventory")?;
            ensure!(
                entry.is_null(),
                "receiver channel {number} has multiple external RTP identities"
            );
            let endpoints = identity["interface_endpoints"]
                .as_array()
                .context("external flow endpoints are unavailable")?;
            let endpoints: Vec<_> = endpoints.iter().map(|endpoint| json!({"ipv4_address": endpoint["ipv4_address"], "udp_port": endpoint["udp_port"]})).collect();
            *entry = json!({
                "kind": "external_rtp",
                "flow_identity": {"source_ipv4": identity["source_ipv4"], "session_id": identity["session_id"]},
                "flow_slot": identity["flow_slot"],
                "receiver_supports_multiple_interfaces": endpoints.len() > 1,
                "interface_endpoints": endpoints,
            });
        }
    }
    for subscription in record["subscriptions"]
        .as_array()
        .context("receiver subscriptions unavailable")?
    {
        let number = subscription["rx_channel_number"]
            .as_u64()
            .context("receiver subscription identity is unavailable")?;
        let entry = subscriptions
            .get_mut(&number.to_string())
            .context("receiver subscription is absent from the channel inventory")?;
        let transmitter = subscription["tx_device"]
            .as_str()
            .filter(|name| !name.is_empty());
        let channel = subscription["tx_channel"]
            .as_str()
            .filter(|name| !name.is_empty());
        ensure!(
            transmitter.is_some() == channel.is_some(),
            "receiver channel {number} has an incomplete native subscription identity"
        );
        if let (Some(transmitter), Some(channel)) = (transmitter, channel) {
            ensure!(
                entry.is_null(),
                "receiver channel {number} reports both native and external subscriptions"
            );
            *entry =
                json!({"kind": "native_dante", "tx_device": transmitter, "tx_channel": channel});
        }
    }
    if !subscriptions.is_empty() {
        configuration.insert("rx_subscriptions".into(), json!(subscriptions));
    }
    ensure!(
        transmit_flows["unparsed_records"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "transmitter-flow inventory cannot be represented losslessly"
    );
    configuration.insert(
        "transmit_flows".into(),
        transmit_flows
            .get("flows")
            .filter(|value| value.is_array())
            .context("canonical transmit flows are unavailable")?
            .clone(),
    );
    Ok(())
}

pub fn clock(configuration: &mut Map<String, Value>, observed: &Map<String, Value>) -> Result<()> {
    let preferred = observed
        .get("preferred_leader")
        .filter(|value| value.is_boolean())
        .context("preferred-leader state could not be read")?;
    configuration.insert("preferred_leader".into(), preferred.clone());
    let status = observed
        .get("clock_status")
        .context("clock status is unavailable")?;
    for field in [
        "clock_source_code",
        "clock_subdomain",
        "global_unicast_delay_requests",
        "aggregate_ptpv1_unicast_delay_requests",
    ] {
        if let Some(value) = status.get(field).filter(|value| !value.is_null()) {
            configuration.insert(field.into(), value.clone());
        }
    }
    let allowed = serde_json::to_value(netaudio_core::clock_configuration::control_availability(
        status,
    ))?;
    let mut changes = Map::new();
    for field in [
        "follower_only",
        "priority_mapping",
        "preferred_protocol",
        "ptpv2_clock_class",
        "ptpv2_domain",
        "ptpv2_priority1",
        "ptpv2_priority2",
        "multicast_dscp",
    ] {
        if allowed[field] == true
            && let Some(value) = status.get(field).filter(|value| !value.is_null())
        {
            changes.insert(field.into(), value.clone());
        }
    }
    let mut ports = Vec::new();
    for port in status["extended_ports"].as_array().into_iter().flatten() {
        let fields: Map<_, _> = [
            "port_id",
            "ttl",
            "sync_interval",
            "announce_interval",
            "delay_request_interval",
            "peer_delay_interval",
            "delay_mechanism",
            "follower_only",
        ]
        .into_iter()
        .filter_map(|field| {
            port.get(field)
                .filter(|value| !value.is_null())
                .map(|value| (field.to_owned(), value.clone()))
        })
        .collect();
        if fields.contains_key("port_id") && fields.len() > 1 {
            ports.push(Value::Object(fields));
        }
    }
    if !ports.is_empty() {
        changes.insert("ports".into(), json!(ports));
    }
    if !changes.is_empty() {
        configuration.insert("clock_configuration".into(), json!(changes));
    }
    Ok(())
}

pub fn network(
    configuration: &mut Map<String, Value>,
    observed: &Map<String, Value>,
    redundancy_supported: bool,
) -> Result<()> {
    let interfaces = observed
        .get("interfaces")
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty())
        .context("network interfaces could not be read")?;
    let mut result = Vec::new();
    for (index, interface) in interfaces.iter().enumerate() {
        let configured = interface
            .get("configured")
            .filter(|value| value.is_object())
            .unwrap_or(interface);
        let mut fields: Map<_, _> = [
            "mode",
            "ip_address",
            "netmask",
            "gateway",
            "dns_server",
            "unknown_fields",
        ]
        .into_iter()
        .filter_map(|field| {
            configured
                .get(field)
                .filter(|value| !value.is_null())
                .map(|value| (field.to_owned(), value.clone()))
        })
        .collect();
        let mode = configured["mode"]
            .as_str()
            .context("network mode is unknown")?;
        ensure!(
            matches!(mode, "static" | "dynamic" | "dhcp"),
            "network mode is unknown"
        );
        if mode == "dhcp" {
            fields.insert("mode".into(), json!("dynamic"));
        }
        let identity = interface
            .get("interface")
            .or_else(|| interface.get("identity"))
            .filter(|value| !value.is_null());
        let identity = identity
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string())
            })
            .unwrap_or_else(|| match index {
                0 => "primary".into(),
                1 => "secondary".into(),
                _ => index.to_string(),
            });
        fields.insert("identity".into(), json!(identity));
        result.push(fields);
    }
    configuration.insert("interfaces".into(), json!(result));
    if redundancy_supported {
        let state = observed
            .get("dante_redundancy")
            .context("fresh redundancy state is unavailable")?;
        let configured = &state["configured_mode"];
        ensure!(
            state["state_fresh"] == true
                && state["available_modes_fresh"] == true
                && !configured.is_null()
                && state["available_modes"]
                    .as_array()
                    .is_some_and(|modes| modes.iter().any(|mode| mode["mode"] == *configured)),
            "configured redundancy mode or available modes are incomplete"
        );
        configuration.insert("redundancy_mode".into(), configured.clone());
    }
    Ok(())
}
