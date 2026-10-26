use super::{DeviceRegistry, Selected};
use crate::devices::record::DeviceRecord;
use crate::devices::transport::DeviceTransport;
use crate::monitoring::requests::StatusQuery;
use anyhow::{Context, Result, ensure};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use tokio::time::Instant;

pub(super) struct InventoryEvidence {
    pub generation: Value,
    pub managed: Option<Value>,
    observed: BTreeSet<&'static str>,
    errors: BTreeMap<&'static str, String>,
    settings: Map<String, Value>,
}

impl InventoryEvidence {
    pub fn require(&self, field: &'static str) -> Result<()> {
        ensure!(
            self.observed.contains(field),
            "fresh {field} unavailable: {}",
            self.errors
                .get(field)
                .map(String::as_str)
                .unwrap_or("response did not include complete evidence")
        );
        Ok(())
    }

    pub fn channels(&self, receiver: bool) -> Result<()> {
        if self.observed.contains("channels") {
            return Ok(());
        }
        self.require(if receiver {
            "rx_channels"
        } else {
            "tx_channels"
        })
    }

    pub fn record(&self, target: &Selected) -> Result<DeviceRecord> {
        let (mut record, generation) = target.record()?;
        ensure!(
            json!(generation) == self.generation,
            "device inventory changed during preset planning"
        );
        record.properties.extend(self.settings.clone());
        Ok(record)
    }
}

impl DeviceRegistry {
    pub(super) async fn preset_inventory(&self, target: &Selected) -> Result<InventoryEvidence> {
        if let Some(device) = target.managed() {
            let record = device.inventory().await?;
            return Ok(InventoryEvidence {
                generation: target.checkpoint()?,
                managed: Some(record),
                observed: BTreeSet::new(),
                errors: BTreeMap::new(),
                settings: Map::new(),
            });
        }
        let (record, generation) = target.record()?;
        ensure!(
            record.properties.get("requires_managed_control") != Some(&json!(true)),
            "managed preset planning is not available in the native provider"
        );
        let address = record
            .control_address()
            .context("device has no routing service")?;
        let protocol = record
            .routing_protocol()?
            .context("device has no routing protocol")?;
        let mut owner = target.direct()?.transport.lock().await;
        if owner
            .as_ref()
            .is_none_or(|transport| transport.address() != address)
        {
            *owner = Some(
                DeviceTransport::connect(address, self.local_address)
                    .await?
                    .with_observer(self.wire_observer()),
            );
        }
        let transport = owner.as_mut().context("device transport unavailable")?;
        let started = Instant::now();
        let data = tokio::select! {
            _ = self.cancellation.cancelled() => anyhow::bail!("device registry is stopping"),
            data = super::super::read_inventory(transport, protocol.protocol_id) => data?,
        };
        ensure!(
            data.name == record.name,
            "fresh device name changed; review the preset target selection again"
        );
        let mut settings = Map::new();
        if let Some(values) = &data.settings {
            settings.insert(
                "configured_latency_ns".into(),
                json!(values.configured_latency_ns),
            );
            settings.insert(
                "performance_settings".into(),
                json!(
                    values
                        .performance_values
                        .iter()
                        .map(|entry| (entry.property_id.to_string(), entry.value))
                        .collect::<BTreeMap<_, _>>()
                ),
            );
        }
        let report = self.accept_inventory(target.direct()?, generation, started, data)?;
        ensure!(
            !report.superseded,
            "device changed during the fresh preset inventory read"
        );
        self.read_status(target.direct()?, transport, generation, StatusQuery::Lock)
            .await?;
        ensure!(
            target
                .record()?
                .0
                .properties
                .get("is_locked")
                .is_some_and(Value::is_boolean),
            "fresh lock state is unavailable"
        );
        Ok(InventoryEvidence {
            generation: json!(generation),
            managed: None,
            observed: report.observed.into_iter().collect(),
            errors: report.errors,
            settings,
        })
    }

    pub(super) async fn preset_query(
        &self,
        target: &Selected,
        query: StatusQuery,
    ) -> Result<Map<String, Value>> {
        let (record, generation) = target.record()?;
        let mut owner = target.direct()?.transport.lock().await;
        let transport = owner.as_mut().context("device transport unavailable")?;
        let observed = tokio::select! {
            _ = self.cancellation.cancelled() => anyhow::bail!("device registry is stopping"),
            observed = self.status_requests.query(transport, &record.key, record.properties.get("mac_address").and_then(Value::as_str), query) => observed?,
        };
        self.accept_status(target.direct()?, generation, observed.clone())?;
        Ok(observed)
    }
}
