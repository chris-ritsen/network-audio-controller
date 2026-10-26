use super::{Device, DeviceRegistry};
use crate::devices::record::DeviceRecord;
use anyhow::Result;
use netaudio_core::conmon_identity::Identity;
use netaudio_core::protocol::SERVICE_CMC;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Signature {
    state: &'static str,
    source_identifier: Option<String>,
    process: Option<u16>,
    process_state: &'static str,
    endpoints: Vec<(Ipv4Addr, u16, String)>,
}

pub(super) fn identity(record: &DeviceRecord) -> Identity {
    Identity::from_memberships(
        record
            .services
            .values()
            .filter(|service| service.service_type == SERVICE_CMC)
            .map(|service| &service.properties),
    )
}

impl DeviceRegistry {
    pub(super) fn refresh_associations(
        &self,
        devices: &BTreeMap<String, Arc<Device>>,
    ) -> Result<()> {
        let mut states = devices
            .values()
            .map(|device| {
                device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))
            })
            .collect::<Result<Vec<_>>>()?;
        let identities = states
            .iter()
            .map(|state| identity(&state.record))
            .collect::<Vec<_>>();
        let mut holders = BTreeMap::new();
        for identity in &identities {
            if let Some(identity) = identity.advertised() {
                *holders.entry(identity).or_insert(0usize) += 1;
            }
        }
        for (state, identity) in states.iter_mut().zip(&identities) {
            let association_state = match identity.advertised() {
                Some(identity) if holders.get(&identity) == Some(&1) => "current",
                Some(_) => "conflicting",
                None => "none",
            };
            let signature = Signature {
                state: association_state,
                source_identifier: identity.cmc_source_identifier.clone(),
                process: identity.cmc_process,
                process_state: identity.cmc_process_state,
                endpoints: state
                    .record
                    .services
                    .values()
                    .filter(|service| service.service_type == SERVICE_CMC)
                    .map(|service| (service.ipv4, service.port, service.name.clone()))
                    .collect(),
            };
            if state.association.as_ref() != Some(&signature) {
                let generation = self
                    .association_generation
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                        value.checked_add(1)
                    })
                    .map_err(|_| anyhow::anyhow!("device association generations exhausted"))?;
                state.association = Some(signature);
                state.associated_at = Instant::now();
                state.generation += 1;
                state.identity_generation += 1;
                state
                    .record
                    .properties
                    .insert("association_generation".into(), json!(generation));
                self.status_requests.invalidate(&state.record.key)?;
                self.panel_requests.invalidate(&state.record.key)?;
                self.exports.invalidate(&state.record.key)?;
                self.metering.invalidate_direct(&state.record.key)?;
            }
            let Value::Object(fields) = serde_json::to_value(identity)? else {
                anyhow::bail!("monitoring identity must be an object");
            };
            state.record.properties.extend(fields);
            state
                .record
                .properties
                .insert("association_state".into(), json!(association_state));
            self.publish(&state.record)?;
        }
        Ok(())
    }

    pub(super) fn conmon_device(
        &self,
        identifier: Option<&str>,
        process: Option<u16>,
    ) -> Result<Option<(Arc<Device>, u64)>> {
        let Some((identifier, process)) = identifier.zip(process) else {
            return Ok(None);
        };
        let devices = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;
        let mut matched = None;
        for device in devices.values() {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
            if state.record.retired
                || state.record.properties["association_state"] != "current"
                || state.record.properties["cmc_source_identifier"]
                    != identifier.to_ascii_lowercase()
                || state.record.properties["cmc_process"].as_u64() != Some(u64::from(process))
            {
                continue;
            }
            if matched.is_some() {
                return Ok(None);
            }
            matched = Some((Arc::clone(device), state.identity_generation));
        }
        Ok(matched)
    }

    pub(super) fn retire_association(&self, key: &str) -> Result<()> {
        self.status_requests.invalidate(key)?;
        self.panel_requests.invalidate(key)?;
        self.exports.invalidate(key)?;
        Ok(())
    }
}
