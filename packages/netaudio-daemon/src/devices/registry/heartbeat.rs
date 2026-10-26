use super::{ControlError, Device, DeviceRegistry, DeviceState, ReceivedPacket};
use crate::monitoring::diagnostics::{self, HISTORY_LIMIT, PRESENCE_LIFETIME};
use anyhow::Result;
use netaudio_core::device_identity::canonical_device_mac;
use netaudio_core::heartbeat::parse_heartbeat_device_extended_unique_identifier;
use netaudio_core::heartbeat_connection_health::Topology;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::time::Instant;
use tokio_util::time::{DelayQueue, delay_queue::Key};

pub struct DiagnosticOptions {
    pub include_clock: bool,
    pub receiver_history: bool,
    pub clock_history: bool,
    pub points: bool,
    pub reset: bool,
    pub warning: Option<bool>,
}

impl Default for DiagnosticOptions {
    fn default() -> Self {
        Self {
            include_clock: true,
            receiver_history: true,
            clock_history: true,
            points: false,
            reset: false,
            warning: None,
        }
    }
}

impl DeviceRegistry {
    pub(super) fn observation_device(
        &self,
        packet: &ReceivedPacket,
        identity: Option<&str>,
    ) -> Result<Option<Arc<Device>>> {
        let identity = identity.and_then(canonical_device_mac);
        let mut identified = Vec::new();
        let mut unknown = Vec::new();

        for device in self.entries()? {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.record.retired
                || std::net::IpAddr::V4(state.record.address) != packet.source.ip()
            {
                continue;
            }

            let advertised = state
                .record
                .properties
                .get("mac_address")
                .and_then(Value::as_str)
                .and_then(canonical_device_mac);

            match (&identity, advertised) {
                (Some(identity), Some(advertised)) if *identity == advertised => {
                    identified.push(Arc::clone(&device))
                }
                (None, _) | (_, None) => unknown.push(Arc::clone(&device)),
                _ => {}
            }
        }

        if identified.len() == 1 {
            Ok(identified.pop())
        } else if identified.is_empty() && unknown.len() == 1 {
            Ok(unknown.pop())
        } else {
            Ok(None)
        }
    }

    pub(super) fn observe_heartbeat(&self, packet: &ReceivedPacket) -> Result<Option<String>> {
        let Some(identity) = parse_heartbeat_device_extended_unique_identifier(&packet.payload)
        else {
            return Ok(None);
        };
        let Some(device) = self.observation_device(packet, Some(&identity))? else {
            return Ok(None);
        };
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        if state.record.retired {
            return Ok(None);
        }

        let topology = Topology {
            inventory_family: state
                .record
                .properties
                .get("receiver_flow_inventory_family")
                .and_then(Value::as_str)
                .map(str::to_owned),
            capacity: state
                .record
                .properties
                .get("receiver_telemetry_capacity")
                .cloned(),
            complete: state
                .record
                .properties
                .get("receiver_flow_completeness")
                .and_then(Value::as_str)
                == Some("complete"),
            flows: state
                .record
                .properties
                .get("receiver_flows")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
        };
        state
            .diagnostics
            .heartbeat(packet, &identity, self.observation_origin, topology)?;
        if let Err(error) = self.metering.observe_passive(&state.record.key, packet) {
            tracing::warn!(source = %packet.source, %error, "invalid signal presence observation");
        }
        let online_changed = !state.record.online;
        state.record.online = true;
        state.record.properties.insert(
            "last_seen".to_owned(),
            json!(packet.received_at.timestamp_micros() as f64 / 1_000_000.0),
        );
        state
            .field_observed_at
            .insert("last_seen".to_owned(), packet.received_monotonic);
        self.publish_diagnostics(&mut state, online_changed)?;
        Ok(Some(state.record.key.clone()))
    }

    pub(super) fn schedule_diagnostics(
        &self,
        key: &str,
        deadlines: &mut DelayQueue<String>,
        leases: &mut BTreeMap<String, Key>,
    ) -> Result<()> {
        if let Some(previous) = leases.remove(key) {
            deadlines.remove(&previous);
        }

        let devices = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;

        if let Some(device) = devices.get(key) {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if let Some(deadline) = state
                .diagnostics
                .next_expiry(self.observation_origin, Instant::now())
            {
                leases.insert(
                    key.to_owned(),
                    deadlines.insert_at(key.to_owned(), deadline),
                );
            }
        }

        Ok(())
    }

    pub(super) fn expire_diagnostics(&self, key: &str) -> Result<()> {
        let devices = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;
        let Some(device) = devices.get(key) else {
            return Ok(());
        };
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        let now = Instant::now();
        state
            .diagnostics
            .refresh(now, self.observation_origin, false, None)?;
        let offline = state.record.online
            && state.diagnostics.identity.is_some()
            && state
                .diagnostics
                .last_seen
                .is_some_and(|observed| now >= observed + PRESENCE_LIFETIME);

        if offline {
            state.record.online = false;
            self.metering.invalidate_direct(&state.record.key)?;
        }

        self.publish_diagnostics(&mut state, offline)
    }

    pub(super) fn publish_diagnostics(&self, state: &mut DeviceState, full: bool) -> Result<()> {
        let mut fields = state.diagnostics.fields()?;
        fields.insert(
            "last_seen".to_owned(),
            state
                .record
                .properties
                .get("last_seen")
                .cloned()
                .unwrap_or(Value::Null),
        );
        let now = Instant::now();

        for (field, value) in &fields {
            state.record.properties.insert(field.clone(), value.clone());
            state.field_observed_at.insert(field.clone(), now);
        }

        if full {
            self.publish(&state.record)?;
        } else {
            self.model.update_telemetry(&state.record.key, fields)?;
        }

        Ok(())
    }

    pub fn diagnostics_snapshot(
        &self,
        reference: &str,
        options: DiagnosticOptions,
    ) -> Result<Value, ControlError> {
        self.diagnostics_snapshot_conditionally(reference, options, None)
    }

    pub fn diagnostics_snapshot_conditionally(
        &self,
        reference: &str,
        options: DiagnosticOptions,
        precondition: Option<&Value>,
    ) -> Result<Value, ControlError> {
        let device = self.controlled_device(reference)?;
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if let Some(precondition) = precondition
            && diagnostic_precondition(&state.record.presentation()?) != *precondition
        {
            return Err(ControlError::Conflict(
                "device identity or diagnostic policy changed since the preview".to_owned(),
            ));
        }

        state.diagnostics.refresh(
            Instant::now(),
            self.observation_origin,
            options.reset,
            options.warning,
        )?;
        self.publish_diagnostics(&mut state, false)?;
        let mut result = json!({
            "device": state.record.key,
            "device_identity": state.diagnostics.identity,
            "receiver": state.diagnostics.receiver_snapshot(options.receiver_history || options.points)?,
            "clock_state": state.record.properties.get("clock_status"),
            "clock_state_observed_at": state.record.properties.get("clock_observed_at"),
            "retention_limit": HISTORY_LIMIT,
            "timestamp_provenance": "local_receive_time",
        });

        if options.include_clock {
            result["clock"] = state
                .diagnostics
                .clock_snapshot(options.clock_history || options.points)?;
        }

        if options.points {
            diagnostics::points(&mut result)?;
        }

        Ok(result)
    }
}

pub fn diagnostic_precondition(record: &Value) -> Value {
    json!({
        "name": record["name"],
        "mac_address": record["mac_address"],
        "ipv4": record["ipv4"],
        "warning_enabled": record["clock_observations"]["warning_enabled"],
        "heartbeat_epoch": record["clock_observations"]["heartbeat"]["display_epoch"],
        "conmon_epoch": record["clock_observations"]["conmon"]["display_epoch"],
        "receiver_epochs": record["receiver_flow_connection_health"]["paths"].as_array().into_iter().flatten().map(|path| json!({
            "telemetry_index": path["telemetry_index"],
            "latency": path["latency"]["display_epoch"],
            "late_packets": path["late_packets"]["display_epoch"],
        })).collect::<Vec<_>>(),
    })
}
