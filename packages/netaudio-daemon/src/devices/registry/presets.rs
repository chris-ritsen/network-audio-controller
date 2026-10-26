use super::{Device, DeviceRegistry};
use crate::devices::record::DeviceRecord;
use crate::devices::transport::DeviceTransport;
use crate::devices::{capabilities, flows, inventory};
use crate::monitoring::requests::StatusQuery;
use crate::presets::Error;
use crate::presets::capture::{self, CaptureRequest, CapturedDevice};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

struct CaptureTarget {
    device: Arc<Device>,
    generation: u64,
    record: DeviceRecord,
}

impl CaptureTarget {
    fn verify(&self) -> Result<()> {
        let state = self
            .device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        ensure!(
            state.generation == self.generation && state.record.online && !state.record.retired,
            "{}: device identity or inventory changed during preset capture",
            self.record.name
        );
        Ok(())
    }
}

impl DeviceRegistry {
    pub async fn capture_preset(
        &self,
        request: &CaptureRequest,
    ) -> Result<Vec<CapturedDevice>, Error> {
        let records = self.model.records()?;
        let managed = if request.identifiers.iter().any(|identifier| {
            records
                .get(identifier)
                .is_some_and(|record| crate::managed::control::required(record))
        }) {
            Some(
                self.managed
                    .get()
                    .context("managed provider unavailable")?
                    .preset_session()
                    .await?,
            )
        } else {
            None
        };
        let mut managed_targets = BTreeMap::new();
        let mut selected = BTreeMap::new();
        let mut names = BTreeSet::new();
        for identifier in &request.identifiers {
            if !records.contains_key(identifier) {
                return Err(Error::Invalid(
                    "The selected inventory has changed. Review the device selection again.".into(),
                ));
            }
            let presented = records
                .get(identifier)
                .context("preset target disappeared")?;
            let name = presented["name"]
                .as_str()
                .filter(|name| !name.is_empty())
                .context("preset target has no name")?;
            if !names.insert(name.to_owned()) {
                return Err(Error::Invalid("Selected devices must have distinct names so the preset can be loaded unambiguously.".into()));
            }
            if crate::managed::control::required(presented) {
                managed_targets.insert(
                    identifier.clone(),
                    managed
                        .as_ref()
                        .context("managed preset session unavailable")?
                        .device(presented)?,
                );
                continue;
            }
            let device = self
                .controlled_device(identifier)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
            if !state.record.online || state.record.retired {
                return Err(Error::Invalid(format!(
                    "{}: device is unavailable or offline.",
                    state.record.name
                )));
            }
            let target = CaptureTarget {
                device: Arc::clone(&device),
                generation: state.generation,
                record: state.record.clone(),
            };
            selected.insert(state.record.key.clone(), target);
        }
        let work = async {
            let _permit = self
                .pending
                .try_acquire()
                .context("device operation queue is full")?;
            let mut guards = Vec::new();
            for target in selected.values() {
                guards.push(target.device.refresh.lock().await);
            }
            for target in selected.values() {
                target.verify()?;
            }
            let mut result = Vec::new();
            for target in selected.values() {
                result.push(
                    self.capture_preset_device(target, request)
                        .await
                        .with_context(|| format!("{}: fresh preset capture", target.record.name))?,
                );
            }
            for target in managed_targets.values() {
                result.push(target.capture(request).await?);
            }
            for target in selected.values() {
                target.verify()?;
            }
            for target in managed_targets.values() {
                target.record()?;
            }
            drop(guards);
            Ok::<_, anyhow::Error>(result)
        };
        tokio::select! {
            _ = self.cancellation.cancelled() => Err(Error::Conflict("Preset was not saved: device registry is stopping".into())),
            result = work => result.map_err(|error| Error::Conflict(format!("Preset was not saved: {error:#}"))),
        }
    }

    async fn capture_preset_device(
        &self,
        target: &CaptureTarget,
        request: &CaptureRequest,
    ) -> Result<CapturedDevice> {
        let mut record = target.record.clone();
        let address = record
            .control_address()
            .context("device has no routing service")?;
        let protocol = record
            .routing_protocol()?
            .context("device has no routing protocol")?;
        let mut owned_transport = target.device.transport.lock().await;
        if owned_transport
            .as_ref()
            .is_none_or(|transport| transport.address() != address)
        {
            *owned_transport = Some(
                DeviceTransport::connect(address, self.local_address)
                    .await?
                    .with_observer(self.wire_observer()),
            );
        }
        let transport = owned_transport
            .as_mut()
            .context("device transport was not created")?;
        let name = inventory::device_name(transport).await?;
        ensure!(
            name == record.name,
            "fresh device name did not match; review the device selection again"
        );
        let counts = inventory::channel_count(transport).await?;
        record.apply_counts(&counts)?;
        let mut captured = capture::identity(&record.presentation()?);
        if request.sections.contains("routing") {
            if netaudio_core::protocol::is_modern_arc_protocol(protocol.protocol_id) {
                for (kind, receiver) in [("rx_channels", true), ("tx_channels", false)] {
                    let channels =
                        inventory::paged_inventory(transport, kind, protocol.protocol_id, |_| {})
                            .await?;
                    record.apply_modern_channels(&channels, receiver)?;
                }
            } else {
                record
                    .apply_legacy_channels(inventory::legacy_channels(transport, &counts).await?)?;
            }
            ensure!(
                record.receivers.len() == usize::from(counts.rx_count)
                    && record.transmitters.len() == usize::from(counts.tx_count),
                "channel inventory is incomplete"
            );
            let flow_capabilities = netaudio_core::parser::flow_authoring_capabilities(
                counts.transmit_flow_authoring_capability_word,
            );
            let (kind, family) = match flow_capabilities.receiver_flow_query_family {
                "fixed" => ("fixed_rx_flows", "legacy"),
                "segmented" => ("segmented_rx_flows", "modern"),
                family => bail!("unsupported receiver-flow inventory family {family}"),
            };
            let receiver_flows =
                inventory::paged_inventory(transport, kind, protocol.protocol_id, |_| {}).await?;
            record.apply_receiver_inventory(Some(family), Some(receiver_flows), None, None)?;
            let transmit_flows =
                flows::read_transmit(transport, &flows::protocols(&record)?).await?;
            capture::routing(
                &mut captured.configuration,
                &record.presentation()?,
                &transmit_flows.canonical,
            )?;
            let after = inventory::channel_count(transport).await?;
            ensure!(
                counts.rx_count == after.rx_count
                    && counts.tx_count == after.tx_count
                    && counts.transmit_flow_authoring_capability_word
                        == after.transmit_flow_authoring_capability_word
                    && counts.maximum_transmit_flow_channel_slots
                        == after.maximum_transmit_flow_channel_slots
                    && counts.maximum_receive_flow_channel_slots
                        == after.maximum_receive_flow_channel_slots
                    && counts.maximum_transmit_flows == after.maximum_transmit_flows
                    && counts.maximum_receive_flows == after.maximum_receive_flows,
                "channel topology changed during capture"
            );
        }
        if request.sections.contains("audio") {
            self.capture_preset_audio(target, transport, &record, &mut captured)
                .await?;
        }
        if request.sections.contains("network") {
            let mut fields = self
                .capture_preset_status(target, transport, StatusQuery::Interface)
                .await?;
            let mut fresh = record.presentation()?;
            fresh.as_object_mut().unwrap().extend(fields.clone());
            let redundancy = capabilities::operation(&fresh, "redundancy")?.supported == Some(true);
            if redundancy {
                fields.extend(
                    self.capture_preset_status(target, transport, StatusQuery::SwitchConfiguration)
                        .await?,
                );
            }
            capture::network(&mut captured.configuration, &fields, redundancy)?;
        }
        if request.sections.contains("device_controls") {
            let observed = self
                .capture_preset_status(target, transport, StatusQuery::Platform)
                .await?;
            self.accept_status(&target.device, target.generation, observed)?;
            let panels = self
                .capture_panels(&target.device, transport, target.generation)
                .await?;
            if !panels.is_null() {
                captured
                    .configuration
                    .insert("device_controls".into(), panels);
            }
        }
        target.verify()?;
        Ok(captured)
    }

    async fn capture_preset_status(
        &self,
        target: &CaptureTarget,
        transport: &mut DeviceTransport,
        query: StatusQuery,
    ) -> Result<Map<String, Value>> {
        target.verify()?;
        let fields = self
            .status_requests
            .query(
                transport,
                &target.record.key,
                target
                    .record
                    .properties
                    .get("mac_address")
                    .and_then(Value::as_str),
                query,
            )
            .await?;
        target.verify()?;
        Ok(fields)
    }

    async fn capture_preset_audio(
        &self,
        target: &CaptureTarget,
        transport: &mut DeviceTransport,
        record: &DeviceRecord,
        captured: &mut CapturedDevice,
    ) -> Result<()> {
        let presented = record.presentation()?;
        for (setting, field, query) in [
            ("sample_rate", "sample_rate_hz", StatusQuery::SampleRate),
            ("encoding", "encoding", StatusQuery::Encoding),
        ] {
            if capabilities::operation(&presented, setting)?.supported == Some(false) {
                captured.omitted.push(json!({"device": record.name, "setting": setting, "reason": "The device's advertised capabilities do not include this setting."}));
                continue;
            }
            let fields = self.capture_preset_status(target, transport, query).await?;
            let current = fields
                .get(field)
                .filter(|value| value.as_u64().is_some_and(|value| value > 0))
                .with_context(|| format!("fresh {setting} value is unavailable"))?;
            captured
                .configuration
                .insert(setting.into(), current.clone());
        }
        let settings = inventory::settings(transport).await?;
        let latency = settings
            .configured_latency_ns
            .context("configured latency is unavailable")?;
        captured
            .configuration
            .insert("latency".into(), json!(f64::from(latency) / 1_000_000.0));
        let properties = inventory::properties(transport).await?;
        let values: BTreeMap<_, _> = settings
            .performance_values
            .iter()
            .map(|entry| (entry.property_id.to_string(), entry.value))
            .collect();
        let facts = serde_json::from_value(
            json!({"property_ids": properties.properties.iter().map(|entry| entry.property_id).collect::<Vec<_>>(), "values": values}),
        )?;
        let performance =
            serde_json::to_value(netaudio_core::performance_configuration::snapshot(facts))?;
        let facts = serde_json::from_value(json!({
            "protocol_id": record.routing_protocol()?.map(|protocol| protocol.protocol_id),
            "managed": false,
            "property_ids": properties.properties.iter().map(|entry| entry.property_id).collect::<Vec<_>>(),
            "platform_software_version": record.properties.get("platform_software_version"),
        }))?;
        let availability = serde_json::to_value(
            netaudio_core::performance_configuration::capabilities(facts)
                .map_err(|error| anyhow::anyhow!("{error}"))?,
        )?;
        for (field, availability) in availability["operations"]
            .as_object()
            .context("performance capabilities are unavailable")?
        {
            ensure!(
                availability["readable"] != true || performance.get(field).is_some(),
                "fresh {field} values are incomplete or inconsistent"
            );
        }
        captured.configuration.extend(
            performance
                .as_object()
                .context("performance snapshot is not an object")?
                .clone(),
        );
        let clock = self
            .capture_preset_status(target, transport, StatusQuery::Clock)
            .await?;
        capture::clock(&mut captured.configuration, &clock)?;
        if record
            .properties
            .get("sample_rate_pullup_configuration_supported")
            == Some(&json!(true))
        {
            let fields = self
                .capture_preset_status(target, transport, StatusQuery::Pullup)
                .await?;
            let value = fields
                .get("sample_rate_pullup_raw_value")
                .filter(|value| value.is_u64())
                .context("fresh sample-rate pull-up value is unavailable")?;
            captured
                .configuration
                .insert("sample_rate_pullup".into(), value.clone());
        }
        if record.properties.get("generic_codec_control_supported") == Some(&json!(true)) {
            let fields = self
                .capture_preset_status(target, transport, StatusQuery::Codec)
                .await?;
            match (fields.get("gain_device_type").and_then(Value::as_str), fields.get("gain_levels").and_then(Value::as_array)) {
                (Some(direction @ ("input" | "output")), Some(levels)) => {
                    let values: Vec<_> = levels.iter().enumerate().map(|(index, level)| json!({"channel": index + 1, "device_type": direction, "level": level})).collect();
                    captured.configuration.insert("codec_gain".into(), json!(values));
                }
                _ => captured.omitted.push(json!({"device": record.name, "setting": "codec_gain", "reason": "The fresh codec status does not expose a supported gain adapter."})),
            }
        }
        Ok(())
    }
}
