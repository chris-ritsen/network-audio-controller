use super::Inspection;
use super::{
    Device, DeviceRegistry, RefreshData, RefreshReport, STATUS_ATTEMPTS, STATUS_RETRY_DELAY,
};
use crate::devices::inventory;
use crate::devices::operations::ControlOperation;
use crate::devices::transport::DeviceTransport;
use crate::monitoring::requests::{self, StatusQuery};
use anyhow::{bail, Context, Result};
use netaudio_core::protocol::SERVICE_CMC;
use netaudio_core::responses::NetaudioNotification;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::time::Instant;

impl DeviceRegistry {
    pub async fn refresh(self: &Arc<Self>, reference: &str) -> Result<RefreshReport> {
        let record = self.model.device(reference)?.context("device not found")?;
        let key = record
            .get("server_name")
            .and_then(Value::as_str)
            .context("device has no server name")?;
        let device = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?
            .get(key)
            .cloned();
        match device {
            Some(device) => self.refresh_entry(device, None).await,
            None if crate::managed::control::required(&record) => {
                let completion = self
                    .apply(
                        reference,
                        ControlOperation::InspectStatus(Inspection::Settings),
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                let mut errors = BTreeMap::new();
                if !completion.readback_errors.is_empty() {
                    errors.insert("settings", completion.readback_errors.join("; "));
                }
                Ok(RefreshReport {
                    observed: Vec::new(),
                    errors,
                    superseded: false,
                })
            }
            None => bail!("device has no native control transport"),
        }
    }

    pub fn request_refresh(&self, reference: &str) -> Result<()> {
        let Some(record) = self.model.device(reference)? else {
            return Ok(());
        };
        let key = record["server_name"]
            .as_str()
            .context("device has no server name")?;
        let device = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?
            .get(key)
            .cloned();

        if let Some(device) = device {
            let generation = {
                let state = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device state lock poisoned"))?;

                if state.record.control_address().is_none() {
                    return Ok(());
                }

                state.generation
            };
            self.schedule_refresh(key.to_owned(), device, generation)?;
        }

        Ok(())
    }

    pub(super) async fn refresh_entry(
        &self,
        device: Arc<Device>,
        expected_generation: Option<u64>,
    ) -> Result<RefreshReport> {
        let _serial = tokio::select! {
            _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
            guard = device.refresh.lock() => guard,
        };
        let started = Instant::now();
        let (generation, address, protocol_identifier) = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if expected_generation.is_some_and(|expected| expected != state.generation) {
                return Ok(RefreshReport {
                    superseded: true,
                    ..RefreshReport::default()
                });
            }

            let address = state
                .record
                .control_address()
                .context("device has no routing service")?;
            let protocol = state
                .record
                .routing_protocol()?
                .context("device has no ARC protocol metadata")?;
            (state.generation, address, protocol.protocol_id)
        };
        let _permit = tokio::select! {
            _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
            permit = self.refreshes.acquire() => permit?,
        };
        let mut owned_transport = device.transport.lock().await;

        if owned_transport
            .as_ref()
            .is_none_or(|transport| transport.address() != address)
        {
            *owned_transport = Some(
                DeviceTransport::connect(address, self.local_address(address.address))
                    .await?
                    .with_observer(self.wire_observer()),
            );
        }

        let transport = owned_transport
            .as_mut()
            .context("device transport was not created")?;

        let data = tokio::select! {
            _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
            data = read_inventory(transport, protocol_identifier) => data?,
        };
        let mut report = self.accept_inventory(&device, generation, started, data)?;

        if !report.superseded {
            tokio::select! {
                _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
                result = self.acquire_status(&device, transport, generation, &mut report) => result?,
            }
        }

        Ok(report)
    }

    pub(super) async fn acquire_status(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        report: &mut RefreshReport,
    ) -> Result<()> {
        let (key, identity) = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation || state.record.service(SERVICE_CMC).is_none() {
                return Ok(());
            }

            (
                state.record.key.clone(),
                state
                    .record
                    .properties
                    .get("mac_address")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            )
        };
        let registration = match requests::register(transport, identity.as_deref()).await {
            Ok(registration) => registration,
            Err(error) => {
                report
                    .errors
                    .insert("monitoring_registration", error.to_string());
                return Ok(());
            }
        };
        {
            let mut state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation {
                report.superseded = true;
                return Ok(());
            }

            state.record.properties.insert(
                "control_addresses".to_owned(),
                json!(registration.control_addresses),
            );
            state.record.properties.insert(
                "settings_port".to_owned(),
                json!(transport.address().settings_port),
            );
            self.publish(&state.record)?;
        }

        let mut pending = vec![
            StatusQuery::Manufacturer,
            StatusQuery::Platform,
            StatusQuery::Interface,
            StatusQuery::Clock,
            StatusQuery::SampleRate,
            StatusQuery::Encoding,
            StatusQuery::Pullup,
            StatusQuery::Codec,
            StatusQuery::Aes67,
            StatusQuery::Lock,
        ];
        let mut unavailable = BTreeMap::new();

        for attempt in 0..STATUS_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(STATUS_RETRY_DELAY * attempt).await;
            }

            let mut unanswered = Vec::new();
            unavailable.clear();

            for query in pending {
                {
                    let state = device
                        .state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

                    if state.generation != generation {
                        report.superseded = true;
                        return Ok(());
                    }

                    if let Some(capability) = query.required_capability() {
                        match state
                            .record
                            .properties
                            .get(capability)
                            .and_then(Value::as_bool)
                        {
                            Some(true) => {}
                            Some(false) => continue,
                            None => {
                                unanswered.push(query);
                                continue;
                            }
                        }
                    }
                }

                match self
                    .status_requests
                    .query(transport, &key, identity.as_deref(), query)
                    .await
                {
                    Ok(_) => report.observed.push(query.kind().as_str()),
                    Err(error) => {
                        tracing::debug!(device = key, status = query.kind().as_str(), attempt, %error, "initial status unavailable");
                        unavailable.insert(query.kind().as_str(), error.to_string());
                        unanswered.push(query);
                    }
                }
            }

            if unavailable.is_empty() {
                break;
            }

            pending = unanswered;
        }

        if !unavailable.is_empty() {
            for (status, error) in &unavailable {
                tracing::warn!(device = key, status, error, "device status unavailable");
            }

            report.errors.extend(unavailable);
            let mut state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation == generation {
                state
                    .record
                    .properties
                    .insert("read_errors".to_owned(), json!(report.errors));
                self.publish(&state.record)?;
            }
        }

        Ok(())
    }

    pub(super) fn accept_inventory(
        &self,
        device: &Device,
        generation: u64,
        started: Instant,
        data: RefreshData,
    ) -> Result<RefreshReport> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        let mut report = data.report;

        if generation != state.generation {
            report.superseded = true;
            return Ok(report);
        }

        let mut record = state.record.clone();
        record.name = data.name;
        record.apply_counts(&data.counts)?;

        if let Some(channels) = data.channels {
            record.apply_legacy_channels(channels)?;
        }

        if let Some(receivers) = data.modern_receivers {
            record.apply_modern_channels(&receivers, true)?;
        }

        if let Some(transmitters) = data.modern_transmitters {
            record.apply_modern_channels(&transmitters, false)?;
        }

        if let Some(settings) = data.settings {
            record.apply_settings(&settings)?;
        }

        if let Some(properties) = data.properties {
            record.apply_property_directory(&properties)?;
        }

        record.apply_receiver_inventory(
            data.receiver_flow_family,
            data.receiver_flows,
            data.partial_receiver_flows,
            data.receiver_flow_error.as_deref(),
        )?;

        for (field, observed_at) in &state.field_observed_at {
            if *observed_at > started {
                if let Some(value) = state.record.properties.get(field) {
                    record.properties.insert(field.clone(), value.clone());
                } else {
                    record.properties.remove(field);
                }
            }
        }

        record.presence.heard(Instant::now(), false);
        record.properties.insert(
            "last_seen".to_owned(),
            json!(chrono::Utc::now().timestamp_micros() as f64 / 1_000_000.0),
        );
        for (operation, error) in &report.errors {
            tracing::warn!(
                device = record.key,
                operation,
                error,
                "device read unavailable"
            );
        }
        if report.errors.is_empty() {
            record.properties.remove("read_errors");
        } else {
            record
                .properties
                .insert("read_errors".to_owned(), json!(report.errors));
        }

        self.publish(&record)?;
        state.record = record;
        Ok(report)
    }
}

pub(super) fn inventory_changed(identifier: u16) -> bool {
    [
        NetaudioNotification::TopologyChange,
        NetaudioNotification::TxChannelChange,
        NetaudioNotification::RxChannelChange,
        NetaudioNotification::TxLabelChange,
        NetaudioNotification::TxFlowChange,
        NetaudioNotification::RxFlowChange,
        NetaudioNotification::PropertyChange,
        NetaudioNotification::SettingsChange,
        NetaudioNotification::RoutingDeviceChange,
        NetaudioNotification::RoutingReady,
    ]
    .into_iter()
    .any(|notification| notification as u16 == identifier)
}

pub(super) async fn read_inventory(
    transport: &mut DeviceTransport,
    protocol_identifier: u16,
) -> Result<RefreshData> {
    let name = inventory::device_name(transport).await?;
    let counts = inventory::channel_count(transport).await?;
    let mut data = RefreshData {
        name,
        counts,
        channels: None,
        modern_receivers: None,
        modern_transmitters: None,
        settings: None,
        properties: None,
        receiver_flows: None,
        partial_receiver_flows: None,
        receiver_flow_family: None,
        receiver_flow_error: None,
        report: RefreshReport {
            observed: vec!["name", "channel_count"],
            ..RefreshReport::default()
        },
    };

    if netaudio_core::protocol::is_modern_arc_protocol(protocol_identifier) {
        for (kind, receiver) in [("rx_channels", true), ("tx_channels", false)] {
            match inventory::paged_inventory(transport, kind, protocol_identifier, |_| {}).await {
                Ok(inventory) => {
                    data.report.observed.push(kind);

                    if receiver {
                        data.modern_receivers = Some(inventory);
                    } else {
                        data.modern_transmitters = Some(inventory);
                    }
                }
                Err(error) => {
                    data.report.errors.insert(kind, error.to_string());
                }
            }
        }
    } else {
        match inventory::legacy_channels(transport, &data.counts).await {
            Ok(channels) => {
                data.channels = Some(channels);
                data.report.observed.push("channels");
            }
            Err(error) => {
                data.report.errors.insert("channels", error.to_string());
            }
        }
    }

    match inventory::settings(transport).await {
        Ok(settings) => {
            data.settings = Some(settings);
            data.report.observed.push("settings");
        }
        Err(error) => {
            data.report.errors.insert("settings", error.to_string());
        }
    }

    match inventory::properties(transport).await {
        Ok(properties) => {
            data.properties = Some(properties);
            data.report.observed.push("properties");
        }
        Err(error) => {
            data.report.errors.insert("properties", error.to_string());
        }
    }

    let capabilities = netaudio_core::parser::flow_authoring_capabilities(
        data.counts.transmit_flow_authoring_capability_word,
    );
    let (kind, family) = match capabilities.receiver_flow_query_family {
        "fixed" => ("fixed_rx_flows", "legacy"),
        "segmented" => ("segmented_rx_flows", "modern"),
        family => bail!("unsupported receive-flow inventory family {family}"),
    };
    data.receiver_flow_family = Some(family);

    match inventory::paged_inventory(transport, kind, protocol_identifier, |partial| {
        data.partial_receiver_flows = Some(Value::Object(partial.clone()));
    })
    .await
    {
        Ok(inventory) => {
            data.receiver_flows = Some(inventory);
            data.partial_receiver_flows = None;
            data.report.observed.push("receiver_flows");
        }
        Err(error) => {
            data.receiver_flow_error = Some(error.to_string());
        }
    }

    Ok(data)
}
