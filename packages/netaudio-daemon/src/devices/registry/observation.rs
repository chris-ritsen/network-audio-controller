use super::inventory_changed;
use super::{
    ClockLease, DeviceRegistry, PacketObservation, CLOCK_FRESHNESS, CLOCK_FRESHNESS_LIMIT,
};
use crate::monitoring::{multicast::ReceivedPacket, status};
use anyhow::{Context, Result};
use futures_util::StreamExt;
use netaudio_core::responses::parse_notification_envelope;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::time::DelayQueue;

impl DeviceRegistry {
    pub async fn monitor(
        self: Arc<Self>,
        mut packets: mpsc::Receiver<ReceivedPacket>,
        mut heartbeats: mpsc::Receiver<ReceivedPacket>,
        mut meters: mpsc::Receiver<ReceivedPacket>,
    ) -> Result<()> {
        let mut deadlines = DelayQueue::<String>::new();
        let mut clock_leases = BTreeMap::new();
        let mut diagnostic_deadlines = DelayQueue::<String>::new();
        let mut diagnostic_leases = BTreeMap::new();

        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => return Ok(()),
                packet = meters.recv() => {
                    let packet = packet.context("metering receiver stopped")?;

                    match self.observe_metering(&packet) {
                        Ok(Some(key)) => self.schedule_diagnostics(&key, &mut diagnostic_deadlines, &mut diagnostic_leases)?,
                        Ok(None) => {}
                        Err(error) => tracing::warn!(source = %packet.source, %error, "invalid detailed meter observation"),
                    }
                }
                expired = diagnostic_deadlines.next(), if !diagnostic_deadlines.is_empty() => {
                    if let Some(expired) = expired {
                        let key = expired.into_inner();
                        diagnostic_leases.remove(&key);
                        self.expire_diagnostics(&key)?;
                        self.schedule_diagnostics(&key, &mut diagnostic_deadlines, &mut diagnostic_leases)?;
                    }
                }
                packet = heartbeats.recv() => {
                    let packet = packet.context("heartbeat receiver stopped")?;

                    match self.observe_heartbeat(&packet) {
                        Ok(Some(key)) => self.schedule_diagnostics(&key, &mut diagnostic_deadlines, &mut diagnostic_leases)?,
                        Ok(None) => {},
                        Err(error) => tracing::warn!(source = %packet.source, %error, "invalid heartbeat observation"),
                    }
                }
                expired = deadlines.next(), if !deadlines.is_empty() => {
                    if let Some(expired) = expired {
                        let key = expired.into_inner();
                        clock_leases.remove(&key);
                        self.expire_clock(&key)?;
                    }
                }
                packet = packets.recv() => {
                    let packet = packet.context("monitoring receiver stopped")?;
                    match self.observe_packet(&packet) {
                        Ok(Some(observation)) => {
                            if let Some(lease) = observation.clock {
                                if let Some(deadline) = clock_leases.remove(&lease.key) {
                                    deadlines.remove(&deadline);
                                }

                                if let Some(expires) = lease.expires {
                                    clock_leases.insert(
                                        lease.key.clone(),
                                        deadlines.insert_at(lease.key, expires),
                                    );
                                }
                            }

                            self.schedule_diagnostics(&observation.key, &mut diagnostic_deadlines, &mut diagnostic_leases)?;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::warn!(source = %packet.source, %error, "invalid monitoring observation");
                        }
                    }
                }
            }
        }
    }

    pub(super) fn observe_packet(
        self: &Arc<Self>,
        packet: &ReceivedPacket,
    ) -> Result<Option<PacketObservation>> {
        if packet.payload.is_empty() {
            if let Some((device, generation)) = self.observation_device(packet, None)? {
                let state = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
                if state.identity_generation == generation && !state.record.retired {
                    self.exports.observe(&state.record.key, packet)?;
                }
            }

            return Ok(None);
        }

        let Some(envelope) = parse_notification_envelope(&packet.payload) else {
            return Ok(None);
        };
        let device = if envelope.is_conmon {
            self.conmon_device(envelope.device_id.as_deref(), envelope.process)?
        } else {
            self.observation_device(packet, None)?
        };
        let Some((device, generation)) = device else {
            return Ok(None);
        };
        let mut refresh = None;
        let mut clock = None;
        let key;
        {
            let mut state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
            key = state.record.key.clone();
            if state.record.retired
                || state.identity_generation != generation
                || packet.received_monotonic < state.associated_at
            {
                return Ok(None);
            }

            let returned = state
                .record
                .presence
                .heard(packet.received_monotonic, false);

            if envelope.response_kind == Some("panel_status") {
                let family = crate::devices::panels::family_name(
                    netaudio_core::device_controls::profile(&crate::devices::panels::record_facts(
                        &state.record,
                    ))
                    .family,
                );
                if let Some(status) =
                    netaudio_core::device_controls::parse_panel_status(&packet.payload, family)
                {
                    let correlated =
                        self.panel_requests
                            .observe(&key, packet.received_monotonic, &status)?;
                    crate::devices::panels::observe(
                        &mut state.record,
                        &status,
                        correlated,
                        packet.received_at.timestamp_micros() as f64 / 1_000_000.0,
                    )?;
                    state
                        .field_observed_at
                        .insert("device_controls".to_owned(), packet.received_monotonic);
                    self.publish(&state.record)?;
                }
            } else if envelope.response_kind == Some("conmon_export_fragment") {
                self.exports.observe(&key, packet)?;
            } else if let Some(kind) = envelope.response_kind
                && let Some(observation) = status::decode(
                    kind,
                    &packet.payload,
                    &state.record.properties,
                    packet.received_at,
                )?
            {
                let mut clock_interval = None;
                let mut clock_updated = false;
                let mut changed = false;

                for (field, value) in &observation.fields {
                    if state
                        .field_observed_at
                        .get(field)
                        .is_some_and(|observed| *observed > packet.received_monotonic)
                    {
                        continue;
                    }

                    if field == "clock_status" {
                        let latest = state.field_observed_at.get(field).map(|previous| {
                            packet
                                .received_monotonic
                                .saturating_duration_since(*previous)
                        });
                        clock_interval = latest.max(state.clock_interval);
                        state.clock_interval = latest;
                        clock_updated = true;
                    }
                    changed = true;
                    state
                        .field_observed_at
                        .insert(field.clone(), packet.received_monotonic);
                    state.record.properties.insert(field.clone(), value.clone());
                }

                if clock_updated {
                    let status = state
                        .record
                        .properties
                        .get("clock_status")
                        .cloned()
                        .unwrap_or(Value::Null);
                    state
                        .diagnostics
                        .clock_status(&status, packet, self.observation_origin)?;
                    let fields = state.diagnostics.fields()?;

                    for (field, value) in fields {
                        state.record.properties.insert(field.clone(), value);
                        state
                            .field_observed_at
                            .insert(field, packet.received_monotonic);
                    }

                    let mut expires = None;

                    if let Some(status) = state.record.properties.get_mut("clock_status") {
                        let deadline = packet.received_monotonic + clock_freshness(clock_interval);

                        if status.get("status_supported").and_then(Value::as_bool) == Some(false) {
                            status["observation_state"] = json!("unavailable");
                        } else if deadline <= Instant::now() {
                            expire_clock_status(status);
                        } else {
                            status["observation_state"] = json!("fresh");
                            expires = Some(deadline);
                        }
                    }

                    clock = Some(ClockLease {
                        key: state.record.key.clone(),
                        expires,
                    });
                }

                if changed {
                    self.publish(&state.record)?;
                    self.status_requests.observe(
                        &state.record.key,
                        observation.kind,
                        packet.received_monotonic,
                        &observation.fields,
                    )?;
                }
            } else if inventory_changed(envelope.notification_id) {
                state.generation += 1;
                device.inventory_changes.send_replace(());

                if state.record.control_address().is_some() {
                    refresh = Some((state.record.key.clone(), state.generation));
                }
            }

            if returned {
                self.publish(&state.record)?;
            }
        }

        if let Some((key, generation)) = refresh {
            self.schedule_refresh(key, device, generation)?;
        }

        Ok(Some(PacketObservation { key, clock }))
    }

    pub(super) fn expire_clock(&self, key: &str) -> Result<()> {
        let device = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?
            .get(key)
            .cloned();
        let Some(device) = device else {
            return Ok(());
        };
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if let Some(status) = state.record.properties.get_mut("clock_status") {
            expire_clock_status(status);
            self.publish(&state.record)?;
        }

        Ok(())
    }
}

pub(super) fn clock_freshness(interval: Option<Duration>) -> Duration {
    interval.map_or(CLOCK_FRESHNESS, |interval| {
        (interval * 2).clamp(CLOCK_FRESHNESS, CLOCK_FRESHNESS_LIMIT)
    })
}

pub(super) fn expire_clock_status(status: &mut Value) {
    if status.get("observation_state").and_then(Value::as_str) == Some("stale") {
        return;
    }

    status["observation_state"] = json!("stale");
    status["observed_synchronization"] = status
        .get("synchronization")
        .cloned()
        .unwrap_or_else(|| json!("unknown"));
    status["synchronization"] = json!("unknown");
}
