use super::discovery::DiscoveredService;
use super::inventory::ChannelInventory;
use super::transport::DeviceAddress;
use anyhow::{Context, Result, bail};
use netaudio_core::parser::ChannelCount;
use netaudio_core::protocol::{SERVICE_ARC, SERVICE_CMC, arc_protocol};
use netaudio_core::responses::{DeviceSettings, PropertyDirectory};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

const NANOSECONDS_PER_MILLISECOND: f64 = 1_000_000.0;

#[derive(Clone)]
pub struct DeviceRecord {
    pub key: String,
    pub name: String,
    pub address: Ipv4Addr,
    pub online: bool,
    pub retired: bool,
    pub services: BTreeMap<String, DiscoveredService>,
    pub receivers: BTreeMap<u16, Value>,
    pub transmitters: BTreeMap<u16, Value>,
    pub subscriptions: Vec<Value>,
    pub properties: Map<String, Value>,
}

impl DeviceRecord {
    pub fn discovered(service: &DiscoveredService) -> Result<Self> {
        let key = service.device_key()?;
        let mut record = Self {
            name: key.trim_end_matches(".local.").to_owned(),
            key,
            address: service.ipv4,
            online: true,
            retired: false,
            services: BTreeMap::new(),
            receivers: BTreeMap::new(),
            transmitters: BTreeMap::new(),
            subscriptions: Vec::new(),
            properties: Map::new(),
        };
        record.attach(service.clone())?;
        Ok(record)
    }

    pub fn attach(&mut self, service: DiscoveredService) -> Result<bool> {
        let changed = self.services.get(&service.name) != Some(&service);

        if service.service_type == SERVICE_ARC {
            self.address = service.ipv4;
        }

        for (property, field) in [
            ("model", "model_id"),
            ("mf", "manufacturer"),
            ("router_vers", "router_protocol_version"),
        ] {
            if let Some(value) = service.property(property) {
                self.properties.insert(field.to_owned(), json!(value));
            }
        }

        if service.service_type == SERVICE_CMC {
            for (property, field) in [("id", "mac_address"), ("server_vers", "cmc_server_version")]
            {
                if let Some(value) = service.property(property) {
                    self.properties.insert(field.to_owned(), json!(value));
                }
            }
        }

        if let Some(value) = service.property("rate") {
            let rate: u32 = value.parse().context("invalid advertised sample rate")?;
            self.properties
                .insert("sample_rate_hz".to_owned(), json!(rate));
        }

        if let Some(value) = service.property("latency_ns") {
            let latency: u32 = value.parse().context("invalid advertised latency")?;
            self.properties.insert(
                "latency_ms".to_owned(),
                json!(f64::from(latency) / NANOSECONDS_PER_MILLISECOND),
            );
        }

        self.online = true;
        self.services.insert(service.name.clone(), service);
        Ok(changed)
    }

    pub fn control_address(&self) -> Option<DeviceAddress> {
        if self.retired {
            return None;
        }

        let routing = self.service(SERVICE_ARC)?;
        let mut address = DeviceAddress::new(routing.ipv4, routing.port);

        if let Some(monitoring) = self.service(SERVICE_CMC) {
            address.monitoring_port = monitoring.port;
        }

        if let Some(settings) = self.properties.get("settings_port").and_then(Value::as_u64) {
            address.settings_port = u16::try_from(settings).ok()?;
        }

        Some(address)
    }

    pub fn routing_protocol(&self) -> Result<Option<netaudio_core::protocol::ArcProtocol>> {
        let version = self
            .service(SERVICE_ARC)
            .and_then(|service| service.property("arcp_vers"));
        arc_protocol(version, false).map_err(anyhow::Error::msg)
    }

    pub fn service(&self, service_type: &str) -> Option<&DiscoveredService> {
        self.services
            .values()
            .find(|service| service.service_type == service_type)
    }

    pub fn apply_counts(&mut self, counts: &ChannelCount) -> Result<()> {
        let Value::Object(mut fields) = serde_json::to_value(counts)? else {
            bail!("channel count must serialize as an object");
        };
        fields.remove("uses_modern_transmit_flow_authoring");
        fields.remove("locked");
        fields.retain(|_, value| !value.is_null());
        fields.insert("tx_count_raw".to_owned(), json!(counts.tx_count));
        fields.insert("rx_count_raw".to_owned(), json!(counts.rx_count));
        fields.insert("is_locked".to_owned(), json!(counts.locked));
        let capabilities = netaudio_core::parser::flow_authoring_capabilities(
            counts.transmit_flow_authoring_capability_word,
        );
        let Value::Object(capabilities) = serde_json::to_value(capabilities)? else {
            bail!("flow capabilities must serialize as an object");
        };
        fields.extend(capabilities);
        self.properties.extend(fields);
        Ok(())
    }

    pub fn apply_legacy_channels(&mut self, inventory: ChannelInventory) -> Result<()> {
        let mut receivers = BTreeMap::new();
        let mut transmitters = BTreeMap::new();
        let mut subscriptions = Vec::new();

        for channel in inventory.receivers {
            receivers.insert(
                channel.number,
                json!({
                    "name": channel.rx_channel_name,
                    "receiver_flags": channel.receiver_flags,
                    "receiver_capability_flags": null,
                    "can_subscribe_self": channel.can_subscribe_self,
                    "direct_can_subscribe_self": channel.can_subscribe_self,
                    "can_rename": channel.can_rename,
                }),
            );
            subscriptions.push(
                SubscriptionObservation {
                    receiver: &self.name,
                    number: channel.number,
                    receiver_channel: channel.rx_channel_name.as_deref(),
                    transmitter_channel: channel.tx_channel_name.as_deref(),
                    transmitter: channel.tx_device_name.as_deref(),
                    self_connection: channel.is_self_connection,
                    status_code: channel.subscription_status_code,
                    receiver_status: channel.rx_status_code,
                }
                .presentation()?,
            );
        }

        for channel in inventory.transmitters {
            let mut value = json!({"name": channel.name});

            if let Some(friendly) = channel.friendly_name {
                value["friendly_name"] = json!(friendly);
            }

            transmitters.insert(channel.number, value);
        }

        if let Some(audio) = inventory.audio {
            self.properties
                .insert("sample_rate_hz".to_owned(), json!(audio.sample_rate));

            if audio
                .supported_encodings
                .as_ref()
                .is_some_and(|values| values.contains(&audio.current_encoding))
            {
                self.properties
                    .insert("encoding".to_owned(), json!(audio.current_encoding));
                self.properties.insert(
                    "supported_encodings".to_owned(),
                    json!(audio.supported_encodings),
                );
            }
        }

        self.receivers = receivers;
        self.transmitters = transmitters;
        self.subscriptions = subscriptions;
        Ok(())
    }

    pub fn apply_modern_channels(&mut self, inventory: &Value, receiver: bool) -> Result<()> {
        let records = inventory
            .get("records")
            .and_then(Value::as_array)
            .context("channel inventory has no records")?;
        let mut channels = BTreeMap::new();
        let mut subscriptions = Vec::new();

        for record in records {
            let number = record
                .get("channel_number")
                .and_then(Value::as_u64)
                .and_then(|number| u16::try_from(number).ok())
                .context("channel has no valid channel number")?;
            let name = record
                .get(if receiver {
                    "local_channel_name"
                } else {
                    "channel_name"
                })
                .and_then(Value::as_str);
            let mut channel = Map::new();
            channel.insert("name".to_owned(), json!(name));

            if let Some(name) = name.filter(|name| !name.is_empty()) {
                channel.insert("friendly_name".to_owned(), json!(name));
            }

            if let Some(name) = record
                .get("friendly_channel_name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
            {
                channel.insert("factory_name".to_owned(), json!(name));
            }

            for (source, destination) in [
                ("media_type_code", "media_type_code"),
                ("media_type", "media_type"),
                ("media_local_channel_id", "media_local_id"),
                (
                    "format_descriptor_hexadecimal",
                    "format_descriptor_hexadecimal",
                ),
                ("sample_rate", "sample_rate"),
                ("encoding", "encoding"),
            ] {
                if let Some(value) = record.get(source).filter(|value| !value.is_null()) {
                    channel.insert(destination.to_owned(), value.clone());
                }
            }

            if receiver {
                channel.insert("receiver_flags".to_owned(), Value::Null);

                for (source, destination) in [
                    ("receiver_capability_flags", "receiver_capability_flags"),
                    ("status_flags", "receiver_status_flags"),
                    ("can_subscribe_self", "can_subscribe_self"),
                    ("can_subscribe_self", "direct_can_subscribe_self"),
                    ("can_rename", "can_rename"),
                ] {
                    if let Some(value) = record.get(source).filter(|value| !value.is_null()) {
                        channel.insert(destination.to_owned(), value.clone());
                    }
                }

                let transmitter = record.get("source_device_name").and_then(Value::as_str);
                let transmitter_channel = record.get("source_channel_name").and_then(Value::as_str);

                if transmitter.is_some_and(|name| !name.is_empty())
                    || transmitter_channel.is_some_and(|name| !name.is_empty())
                {
                    subscriptions.push(
                        SubscriptionObservation {
                            receiver: &self.name,
                            number,
                            receiver_channel: name,
                            transmitter_channel,
                            transmitter,
                            self_connection: record
                                .get("is_self_connection")
                                .and_then(Value::as_bool)
                                == Some(true),
                            status_code: required_u16(record, "subscription_status_code")?,
                            receiver_status: required_u16(record, "receiver_status_code")?,
                        }
                        .presentation()?,
                    );
                }
            }

            if channels.insert(number, Value::Object(channel)).is_some() {
                bail!("duplicate channel number in completed inventory");
            }
        }

        if receiver {
            self.receivers = channels;
            self.subscriptions = subscriptions;
            self.properties
                .insert("rx_count".to_owned(), json!(self.receivers.len()));
        } else {
            self.transmitters = channels;
            self.properties
                .insert("tx_count".to_owned(), json!(self.transmitters.len()));
        }

        let media: BTreeSet<_> = self
            .receivers
            .values()
            .chain(self.transmitters.values())
            .filter_map(|channel| channel.get("media_type").and_then(Value::as_str))
            .map(str::to_owned)
            .collect();

        if !media.is_empty() {
            self.properties
                .insert("media_types".to_owned(), json!(media));
        }

        Ok(())
    }

    pub fn apply_settings(&mut self, settings: &DeviceSettings) -> Result<()> {
        super::latency::apply_settings(&mut self.properties, settings)?;

        if let Some(rate) = settings.sample_rate {
            self.properties
                .insert("sample_rate_hz".to_owned(), json!(rate));
        }

        if let Some(prefix) = &settings.aes67_multicast_prefix {
            self.properties
                .insert("aes67_multicast_prefix".to_owned(), json!(prefix));
        }

        if !settings.performance_values.is_empty() {
            let performance = self
                .properties
                .entry("performance_settings")
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .context("performance settings are not an object")?;

            for property in &settings.performance_values {
                performance.insert(property.property_id.to_string(), json!(property.value));
            }
        }
        Ok(())
    }

    pub fn apply_property_directory(&mut self, directory: &PropertyDirectory) -> Result<()> {
        self.properties.insert(
            "settings_properties".to_owned(),
            serde_json::to_value(&directory.properties)?,
        );
        self.properties.insert(
            "aes67_configured_property_advertised".to_owned(),
            json!(directory.aes67_configured_property_advertised),
        );
        Ok(())
    }

    pub fn apply_receiver_inventory(
        &mut self,
        family: Option<&str>,
        inventory: Option<Value>,
        partial: Option<Value>,
        error: Option<&str>,
    ) -> Result<()> {
        let completeness = if inventory.is_some() {
            "complete"
        } else if partial.is_some() {
            "partial"
        } else {
            "unknown"
        };
        self.properties
            .insert("receiver_flow_completeness".to_owned(), json!(completeness));
        self.properties.insert(
            "receiver_flow_partial_inventory".to_owned(),
            partial.unwrap_or(Value::Null),
        );
        self.properties
            .insert("receiver_flow_read_error".to_owned(), json!(error));
        self.properties
            .insert("receiver_flow_inventory_family".to_owned(), json!(family));

        if let Some(inventory) = inventory {
            anyhow::ensure!(
                netaudio_core::flow_readback::inventory_complete(inventory.clone()),
                "receiver inventory is incomplete"
            );
            let flows = inventory["flows"]
                .as_array()
                .context("receiver inventory has no flow records")?;
            self.properties
                .insert("rx_flow_count".to_owned(), json!(flows.len()));
            self.properties.insert(
                "receiver_flow_latency_ns".to_owned(),
                flows
                    .first()
                    .map_or(Value::Null, |flow| flow["latency_nanoseconds"].clone()),
            );
            self.properties
                .insert("receiver_flows".to_owned(), json!(flows));
            self.properties
                .insert("receiver_flow_status_page".to_owned(), inventory);
        }

        Ok(())
    }

    pub fn presentation(&self) -> Result<Value> {
        let mut record = self.properties.clone();
        record.extend(Map::from_iter([
            ("name".to_owned(), json!(self.name)),
            ("server_name".to_owned(), json!(self.key)),
            ("ipv4".to_owned(), json!(self.address)),
            ("online".to_owned(), json!(self.online)),
            ("services".to_owned(), json!(self.services)),
            (
                "channels".to_owned(),
                json!({"receivers": self.receivers, "transmitters": self.transmitters}),
            ),
            ("subscriptions".to_owned(), json!(self.subscriptions)),
            ("kind".to_owned(), json!(self.kind())),
        ]));
        record.insert(
            "device_controls".to_owned(),
            super::panels::snapshot(
                &self.properties,
                self.online,
                !self.address.is_unspecified(),
            )?,
        );
        super::analog::present(&mut record);
        record.entry("is_locked").or_insert(Value::Null);
        record.insert(
            "performance_operation_availability".to_owned(),
            super::performance::capabilities(self)?["operations"].clone(),
        );
        let capabilities = netaudio_core::receiver_capabilities::self_connection(
            &json!({
                "authority": "observed",
                "channels": self.receivers.values().map(|channel| json!({
                    "direct": channel.get("can_subscribe_self"),
                    "managed": null,
                    "managed_fresh": false,
                })).collect::<Vec<_>>()
            })
            .to_string(),
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;
        record.insert(
            "self_connection_support".to_owned(),
            serde_json::to_value(capabilities)?["support"].clone(),
        );
        let clock = netaudio_core::clock_configuration::clock_sources(serde_json::from_value(
            json!({
                "current": record.get("clock_source_code"),
                "supported": record.get("supported_clock_sources").cloned().unwrap_or_else(|| json!([])),
            }),
        )?);
        record.insert("clock_source".to_owned(), json!(clock.current));
        record.insert(
            "clock_source_choices".to_owned(),
            serde_json::to_value(clock.choices)?,
        );
        record.insert(
            "clock_control_availability".to_owned(),
            serde_json::to_value(netaudio_core::clock_configuration::control_availability(
                record.get("clock_status").unwrap_or(&Value::Null),
            ))?,
        );
        record.insert(
            "clock_subdomain_presentation".to_owned(),
            serde_json::to_value(netaudio_core::clock_configuration::subdomain_presentation(
                record.get("clock_subdomain").unwrap_or(&Value::Null),
            ))?,
        );
        let capabilities = Value::Object(record.clone());
        record.insert(
            "operation_availability".to_owned(),
            super::capabilities::operations(&capabilities)?,
        );
        let network = super::capabilities::network(&capabilities)?;
        record.insert(
            "network_redundancy".to_owned(),
            network["redundancy"].clone(),
        );

        if self
            .properties
            .get("interfaces")
            .is_some_and(|interfaces| !interfaces.is_null())
        {
            record.insert(
                "interface_configuration_modes".to_owned(),
                network["interface_configuration_modes"].clone(),
            );
        }
        Ok(Value::Object(record))
    }

    fn kind(&self) -> &'static str {
        let names = ["dante_model", "manufacturer", "model", "model_id"];

        if names.iter().any(|field| {
            self.properties
                .get(*field)
                .and_then(Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case("netaudio"))
        }) {
            return "virtual";
        }

        let mac = self
            .properties
            .get("mac_address")
            .and_then(Value::as_str)
            .and_then(netaudio_core::device_identity::canonical_device_mac)
            .unwrap_or_default();
        let encoded_address = netaudio_core::bytes::hexadecimal(&self.address.octets());

        if mac.starts_with("525400")
            || mac.starts_with("0000c0a8")
            || (mac.starts_with("0000") && mac.get(4..) == Some(encoded_address.as_str()))
            || self.name.starts_with("netaudio-page-probe")
        {
            "emulated"
        } else {
            "hardware"
        }
    }
}

fn required_u16(record: &Value, field: &str) -> Result<u16> {
    record
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|number| u16::try_from(number).ok())
        .with_context(|| format!("invalid {field} in channel inventory"))
}

struct SubscriptionObservation<'a> {
    receiver: &'a str,
    number: u16,
    receiver_channel: Option<&'a str>,
    transmitter_channel: Option<&'a str>,
    transmitter: Option<&'a str>,
    self_connection: bool,
    status_code: u16,
    receiver_status: u16,
}

impl SubscriptionObservation<'_> {
    fn presentation(&self) -> Result<Value> {
        let status = netaudio_core::subscription_status::decode(
            self.status_code,
            Some(self.receiver_status),
        );
        let mut status = serde_json::to_value(status)?;
        status["icon"] = json!("");
        let mut record = json!({
            "rx_channel": self.receiver_channel,
            "rx_channel_number": self.number,
            "rx_device": self.receiver,
            "tx_channel": self.transmitter_channel,
            "tx_device": if self.self_connection { Some(self.receiver) } else { self.transmitter },
            "status": status,
            "rx_channel_status": {
                "code": self.receiver_status,
                "detail": null,
                "label": "Unknown receiver status",
                "state": "uncharacterized",
            },
        });

        if self.self_connection {
            record["self_connection"] = json!(true);
        }

        Ok(record)
    }
}
