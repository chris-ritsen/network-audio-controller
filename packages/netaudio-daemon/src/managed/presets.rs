use super::{control::DeviceRequest, registry::Registry};
use crate::devices::{
    operations::{ControlCompletion, ControlError, ControlOperation},
    registry::Inspection,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::OwnedMutexGuard;

pub(crate) struct Session {
    registry: Arc<Registry>,
    revision: String,
    _guard: OwnedMutexGuard<()>,
}

pub(crate) struct Device {
    session: Arc<Session>,
    identity: Value,
}

impl Registry {
    pub(crate) async fn preset_session(self: &Arc<Self>) -> Result<Arc<Session>> {
        let guard = Arc::clone(&self.configuration_change).lock_owned().await;
        self.load_configuration().await?;
        Ok(Arc::new(Session {
            registry: Arc::clone(self),
            revision: self.configuration_revision()?,
            _guard: guard,
        }))
    }
}

impl Session {
    pub(crate) fn device(self: &Arc<Self>, record: &Value) -> Result<Device> {
        ensure!(
            super::control::required(record),
            "device no longer requires managed control"
        );
        Ok(Device {
            session: Arc::clone(self),
            identity: identity(record),
        })
    }
}

impl Device {
    pub(crate) fn record(&self) -> Result<Value> {
        let record = self
            .session
            .registry
            .model
            .device(
                self.identity["server_name"]
                    .as_str()
                    .context("managed target has no server name")?,
            )?
            .context("managed preset target disappeared")?;
        ensure!(
            identity(&record) == self.identity && super::control::required(&record),
            "managed preset target ownership changed"
        );
        ensure!(
            self.session.registry.configuration_revision()? == self.session.revision,
            "managed configuration changed during the preset"
        );
        Ok(record.as_ref().clone())
    }

    pub(crate) async fn control(
        &self,
        operation: &ControlOperation,
    ) -> Result<ControlCompletion, ControlError> {
        let record = self.record()?;
        self.session
            .registry
            .control_locked(DeviceRequest {
                record,
                operation: operation.clone(),
                precondition: None,
                configuration_revision: self.session.revision.clone(),
            })
            .await
    }

    pub(crate) async fn inventory(&self) -> Result<Value> {
        let request = DeviceRequest {
            record: self.record()?,
            operation: ControlOperation::InspectStatus(Inspection::Lock),
            precondition: None,
            configuration_revision: self.session.revision.clone(),
        };
        let server = request.record["ddm_server_profile"]
            .as_str()
            .context("managed target has no server")?;
        let registry = &self.session.registry;
        let source = registry.source(server)?;
        let fresh = registry
            .fresh_target(&request, &source, &self.session.revision)
            .await?;
        let mut record = fresh.record.as_ref().clone();
        let receivers = fresh
            .target
            .device
            .rx_channels
            .as_ref()
            .context("managed receiver inventory is unavailable")?;
        let transmitters = fresh
            .target
            .device
            .tx_channels
            .as_ref()
            .context("managed transmitter inventory is unavailable")?;
        ensure!(
            record["channels"]["receivers"]
                .as_object()
                .is_some_and(|channels| channels.len() == receivers.len())
                && record["channels"]["transmitters"]
                    .as_object()
                    .is_some_and(|channels| channels.len() == transmitters.len()),
            "managed channel identities are incomplete or duplicated"
        );
        let mut subscriptions = Vec::new();
        for channel in receivers {
            let device = channel
                .subscribed_device
                .as_deref()
                .context("managed receiver source device is unknown")?;
            let source = channel
                .subscribed_channel
                .as_deref()
                .context("managed receiver source channel is unknown")?;
            ensure!(
                device.is_empty() == source.is_empty(),
                "managed receiver subscription is incomplete"
            );
            subscriptions.push(json!({"rx_channel_number": channel.index, "tx_device": if device == "." { record["name"].as_str().unwrap_or_default() } else { device }, "tx_channel": source}));
        }
        record["subscriptions"] = json!(subscriptions);
        self.control(&request.operation).await?;
        let current = self.record()?;
        ensure!(
            routing_condition(&current) == routing_condition(&fresh.record),
            "managed routing changed during preset inventory observation"
        );
        record["is_locked"] = current["is_locked"].clone();
        ensure!(
            record["is_locked"].is_boolean(),
            "fresh managed lock state is unavailable"
        );
        Ok(record)
    }
}

fn identity(record: &Value) -> Value {
    Value::Object(
        [
            "server_name",
            "ddm_server_profile",
            "ddm_domain_id",
            "ddm_device_id",
            "mac_address",
        ]
        .into_iter()
        .map(|field| (field.to_owned(), record[field].clone()))
        .collect(),
    )
}

impl Device {
    pub(crate) async fn capture(
        &self,
        request: &crate::presets::capture::CaptureRequest,
    ) -> Result<crate::presets::capture::CapturedDevice> {
        use super::{device_session::DeviceSession, device_topology, status::Query};
        use crate::{monitoring::requests::StatusQuery, presets::capture};
        let observed = self.inventory().await?;
        let registry = &self.session.registry;
        let source = registry.source(
            observed["ddm_server_profile"]
                .as_str()
                .context("managed server unavailable")?,
        )?;
        let fresh = registry
            .fresh_target(
                &DeviceRequest {
                    record: observed.clone(),
                    operation: ControlOperation::InspectStatus(Inspection::Lock),
                    precondition: None,
                    configuration_revision: self.session.revision.clone(),
                },
                &source,
                &self.session.revision,
            )
            .await?;
        let mut session = DeviceSession::open(&source, &fresh).await?;
        let mut captured = capture::identity(&observed);
        if request.sections.contains("routing") {
            let protocol = netaudio_core::protocol::arc_protocol(None, true)
                .map_err(|error| anyhow::anyhow!("{error}"))?
                .context("managed routing protocol unavailable")?
                .protocol_id;
            let first =
                device_topology::query(&mut session, json!({"command": "channel_count"})).await?;
            let counts = device_topology::counts(&first)?;
            ensure!(
                observed["channels"]["receivers"]
                    .as_object()
                    .is_some_and(|channels| channels.len() == usize::from(counts.rx_count))
                    && observed["channels"]["transmitters"]
                        .as_object()
                        .is_some_and(|channels| channels.len() == usize::from(counts.tx_count)),
                "managed channel inventory disagrees with the Controller channel counts"
            );
            let mut routing = observed.clone();
            routing["receiver_flows"] = if counts.rx_count == 0 {
                json!([])
            } else {
                let capabilities = netaudio_core::parser::flow_authoring_capabilities(
                    counts.transmit_flow_authoring_capability_word,
                );
                let kind = match capabilities.receiver_flow_query_family {
                    "fixed" => "fixed_rx_flows",
                    "segmented" => "segmented_rx_flows",
                    _ => anyhow::bail!("managed receiver inventory family unavailable"),
                };
                let inventory = device_topology::inventory(&mut session, kind, protocol).await?;
                ensure!(
                    netaudio_core::flow_readback::inventory_complete(inventory.clone()),
                    "managed receiver inventory incomplete"
                );
                inventory["flows"].clone()
            };
            let flows = if counts.tx_count == 0 {
                json!({"flows": [], "unparsed_records": []})
            } else {
                let inventory =
                    device_topology::inventory(&mut session, "tx_flows", protocol).await?;
                crate::devices::flows::canonical_inventory(&inventory, protocol)?
            };
            let last =
                device_topology::query(&mut session, json!({"command": "channel_count"})).await?;
            ensure!(
                device_topology::same_topology(&counts, &device_topology::counts(&last)?),
                "managed topology changed during preset capture"
            );
            capture::routing(&mut captured.configuration, &routing, &flows)?;
        }
        if request.sections.contains("audio") {
            for (setting, field, query) in [
                ("sample_rate", "sample_rate_hz", StatusQuery::SampleRate),
                ("encoding", "encoding", StatusQuery::Encoding),
            ] {
                if crate::devices::capabilities::operation(&observed, setting)?.supported
                    == Some(false)
                {
                    captured.omitted.push(json!({"device": observed["name"], "setting": setting, "reason": "The device does not advertise this setting."}));
                    continue;
                }
                registry
                    .controller_status(&source, &fresh, &mut session, query)
                    .await?;
                let record = registry.status_record(&fresh)?;
                let value = record
                    .get(field)
                    .filter(|value| value.as_u64().is_some_and(|value| value > 0))
                    .context("fresh managed audio value unavailable")?;
                captured.configuration.insert(setting.into(), value.clone());
            }
            registry
                .controller_status(&source, &fresh, &mut session, Query::Latency)
                .await?;
            let record = registry.status_record(&fresh)?;
            let latency = record["configured_latency_ns"]
                .as_u64()
                .context("fresh managed latency unavailable")?;
            captured
                .configuration
                .insert("latency".into(), json!(latency as f64 / 1_000_000.0));
            let preferred = observed
                .get("preferred_leader")
                .filter(|value| value.is_boolean())
                .context("managed preferred-leader state unavailable")?;
            captured
                .configuration
                .insert("preferred_leader".into(), preferred.clone());
            if record["sample_rate_pullup_configuration_supported"] == true {
                registry
                    .controller_status(&source, &fresh, &mut session, StatusQuery::Pullup)
                    .await?;
                let record = registry.status_record(&fresh)?;
                let value = record
                    .get("sample_rate_pullup_raw_value")
                    .filter(|value| value.is_u64())
                    .context("managed sample-rate pull-up unavailable")?;
                captured
                    .configuration
                    .insert("sample_rate_pullup".into(), value.clone());
            }
            if record["generic_codec_control_supported"] == true {
                registry
                    .controller_status(&source, &fresh, &mut session, StatusQuery::Codec)
                    .await?;
                let record = registry.status_record(&fresh)?;
                match (record["gain_device_type"].as_str(), record["gain_levels"].as_array()) {
                    (Some(direction @ ("input" | "output")), Some(levels)) => {
                        captured.configuration.insert("codec_gain".into(), json!(levels.iter().enumerate().map(|(index, level)| json!({"channel": index + 1, "device_type": direction, "level": level})).collect::<Vec<_>>()));
                    }
                    _ => captured.omitted.push(json!({"device": observed["name"], "setting": "codec_gain", "reason": "Fresh codec status does not expose a supported gain adapter."})),
                }
            }
            captured.omitted.push(json!({"device": observed["name"], "setting": "performance", "reason": "Managed performance configuration has no established transport."}));
        }
        if request.sections.contains("network") {
            registry
                .controller_status(&source, &fresh, &mut session, StatusQuery::Interface)
                .await?;
            let record = registry.status_record(&fresh)?;
            let redundancy = crate::devices::capabilities::operation(&record, "redundancy")?
                .supported
                == Some(true);
            if redundancy {
                registry
                    .controller_status(
                        &source,
                        &fresh,
                        &mut session,
                        StatusQuery::SwitchConfiguration,
                    )
                    .await?;
            }
            let record = registry.status_record(&fresh)?;
            capture::network(
                &mut captured.configuration,
                record
                    .as_object()
                    .context("managed record is not an object")?,
                redundancy,
            )?;
        }
        if request.sections.contains("device_controls") {
            captured.omitted.push(json!({"device": observed["name"], "setting": "device_controls", "reason": "Managed device-control panels have no established transport."}));
        }
        let after = self.inventory().await?;
        ensure!(
            routing_condition(&after) == routing_condition(&observed)
                && after["preferred_leader"] == observed["preferred_leader"],
            "managed routing or preferred-leader state changed during preset capture"
        );
        Ok(captured)
    }
}

pub(crate) fn routing_condition(record: &Value) -> Value {
    let mut channels = serde_json::Map::new();
    for direction in ["receivers", "transmitters"] {
        let selected: serde_json::Map<_, _> = record["channels"][direction]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(number, channel)| {
                (
                    number.clone(),
                    Value::Object(
                        [
                            "name",
                            "ddm_channel_id",
                            "ddm_media_type",
                            "managed_can_subscribe_self",
                            "managed_can_subscribe_self_fresh",
                        ]
                        .into_iter()
                        .map(|field| (field.to_owned(), channel[field].clone()))
                        .collect(),
                    ),
                )
            })
            .collect();
        channels.insert(direction.to_owned(), Value::Object(selected));
    }
    let subscriptions: Vec<_> = record["subscriptions"].as_array().into_iter().flatten().filter(|route| ["tx_device", "tx_channel"].iter().any(|field| route[field].as_str().is_some_and(|value| !value.is_empty()))).map(|route| json!({"rx_channel_number": route["rx_channel_number"], "tx_device": route["tx_device"], "tx_channel": route["tx_channel"]})).collect();
    json!({"name": record["name"], "channels": channels, "subscriptions": subscriptions})
}
