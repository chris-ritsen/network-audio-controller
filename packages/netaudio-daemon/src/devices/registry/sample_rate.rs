use super::{Device, DeviceRegistry};
use crate::devices::audit::Audit;
use crate::devices::flows;
use crate::devices::inventory;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::record::DeviceRecord;
use crate::devices::sample_rate::{self, Operation};
use crate::devices::transport::DeviceTransport;
use crate::monitoring::requests::StatusQuery;
use anyhow::Context;
use serde_json::{Map, Value, json};
use std::time::Duration;
use tokio::time::{Instant, timeout, timeout_at};

const PREFLIGHT_WINDOW: Duration = Duration::from_secs(30);
const READBACK_WINDOW: Duration = Duration::from_secs(20);
const CAPACITY_EXPORT_WINDOW: Duration = Duration::from_secs(4);
const STATUS_WINDOW: Duration = Duration::from_secs(3);

struct Prepared {
    preflight: Value,
    status: Value,
    record: DeviceRecord,
    generation: u64,
}

impl DeviceRegistry {
    pub(super) async fn configure_sample_rate(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        operation: &Operation,
        precondition: Option<&Value>,
        audit: Option<&mut Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        let identity = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation || !state.record.online {
                return Err(ControlError::Conflict(
                    "device changed before sample-rate preflight".to_owned(),
                ));
            }

            state.identity_generation
        };
        let prepared = tokio::select! {
            _ = self.cancellation.cancelled() => return Err(ControlError::Unavailable("device registry is stopping".to_owned())),
            prepared = timeout(PREFLIGHT_WINDOW, self.prepare_sample_rate(device, transport, identity, operation)) => {
                prepared.map_err(|_| ControlError::ObservationUnavailable("sample-rate preflight deadline expired; no change was sent".to_owned()))??
            }
        };
        let presented = prepared.record.presentation()?;
        let control = ControlOperation::SampleRate(operation.clone());

        if precondition.is_some_and(|expected| control.precondition(&presented) != *expected) {
            return Err(ControlError::Conflict("sample-rate or topology facts changed after confirmation; obtain a new preview before retrying".to_owned()));
        }

        if operation.preview {
            return Ok(completion(
                json!({"state": "preview", "preflight": prepared.preflight, "change_sent": false}),
                Vec::new(),
            ));
        }

        if prepared.status["current_value"] == operation.target {
            return Ok(completion(
                sample_rate::success(&prepared.preflight, &prepared.status, None, false),
                Vec::new(),
            ));
        }

        if let Some((state, reason)) = operation.refusal(&prepared.preflight) {
            return Ok(completion(
                json!({"error": reason, "state": state, "preflight": prepared.preflight, "change_sent": false}),
                Vec::new(),
            ));
        }

        operation.writable(&presented)?;
        {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.identity_generation != identity
                || state.generation != prepared.generation
                || !state.record.online
            {
                return Err(ControlError::Conflict(
                    "device changed before sample-rate mutation; no change was sent".to_owned(),
                ));
            }
        }

        let mut changes = device.inventory_changes.subscribe();
        transport
            .send_command(&json!({"command": "set_sample_rate", "sample_rate": operation.target}))
            .await
            .map_err(|error| ControlError::Transport {
                error,
                acknowledged_commands: 0,
            })?;

        if let Some(audit) = audit {
            audit
                .acknowledgement(&self.journal, &[], 1)
                .await
                .context("record sample-rate transmission; do not replay the command")?;
        }

        let deadline = Instant::now() + READBACK_WINDOW;
        let mut observed_status = None;
        let mut resulting_snapshot = None;
        let mut errors = Vec::new();

        loop {
            changes.borrow_and_update();
            let observation = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    errors.push("device registry stopped during verification".to_owned());
                    break;
                },
                observed = timeout_at(deadline, async {
                    let status = self.observe_sample_rate(device, transport, identity, Some(operation.target), deadline.saturating_duration_since(Instant::now())).await?;
                    observed_status = Some(status.clone());

                    if status["current_value"] != operation.target {
                        return Err(ControlError::Conflict(format!("device reports {} Hz instead of {} Hz", status["current_value"], operation.target)));
                    }

                    let (_, snapshot, _) = self.capture_sample_rate_topology(device, transport, identity, operation.target).await?;
                    resulting_snapshot = Some(snapshot.clone());
                    operation.verify(&prepared.preflight, &snapshot)?;
                    Ok(sample_rate::success(&prepared.preflight, &status, Some(&snapshot), true))
                }) => observed,
            };

            match observation {
                Ok(Ok(result)) => return Ok(completion(result, errors)),
                Ok(Err(error)) => errors.push(error.to_string()),
                Err(_) => {
                    errors.push("sample-rate topology verification deadline expired".to_owned());
                    break;
                }
            }

            tokio::select! {
                _ = self.cancellation.cancelled() => break,
                changed = timeout_at(deadline, changes.changed()) => {
                    if !matches!(changed, Ok(Ok(()))) {
                        break;
                    }
                }
            }
        }

        let message = errors
            .last()
            .map(String::as_str)
            .unwrap_or("sample-rate readback was unavailable");
        Ok(completion(
            json!({
                "error": format!("sample-rate change was sent, but complete post-write verification failed: {message}"),
                "state": "unverified",
                "change_sent": true,
                "state_verified": false,
                "observed_sample_rate_hertz": observed_status.as_ref().map(|status| &status["current_value"]),
                "resulting_topology": resulting_snapshot,
                "preflight": prepared.preflight,
            }),
            errors,
        ))
    }

    async fn prepare_sample_rate(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        identity: u64,
        operation: &Operation,
    ) -> Result<Prepared, ControlError> {
        let status = self
            .observe_sample_rate(device, transport, identity, None, STATUS_WINDOW)
            .await?;
        let (record, _) = sample_rate_record(device, identity)?;
        operation.validate_status(&record.presentation()?)?;

        if status["current_value"] == operation.target {
            let (record, generation) = sample_rate_record(device, identity)?;
            return Ok(Prepared {
                preflight: operation.preflight(&record.presentation()?, None)?,
                status,
                record,
                generation,
            });
        }

        if record
            .properties
            .get("sample_rate_channel_capacities")
            .is_none_or(Value::is_null)
            && record.properties.get("diagnostic_log_export_supported") != Some(&Value::Bool(false))
            && record.properties.get("requires_managed_control") != Some(&Value::Bool(true))
        {
            let fields = match self
                .exports
                .audio_capabilities(
                    transport,
                    &record.key,
                    CAPACITY_EXPORT_WINDOW,
                    &self.cancellation,
                )
                .await
            {
                Ok(fields) => fields,
                Err(error) => {
                    tracing::info!(device = record.key, %error, "sample-rate capacity table unavailable");

                    if error.is::<crate::monitoring::exports::Unsupported>() {
                        Map::from_iter([(
                            "diagnostic_log_export_supported".to_owned(),
                            json!(false),
                        )])
                    } else {
                        Map::new()
                    }
                }
            };
            self.accept_sample_rate_fields(device, identity, fields)?;
        }

        let rate = status["current_value"]
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .context("validated sample rate disappeared")?;
        let (_, snapshot, captured_generation) = self
            .capture_sample_rate_topology(device, transport, identity, rate)
            .await?;
        let fields = self
            .status_requests
            .query(transport, &record.key, None, StatusQuery::Lock)
            .await
            .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
        self.accept_sample_rate_fields(device, identity, fields)?;
        let (record, generation) = sample_rate_record(device, identity)?;
        if generation != captured_generation {
            return Err(ControlError::Conflict(
                "device topology changed after sample-rate observation; no change was sent"
                    .to_owned(),
            ));
        }

        let presentation = record.presentation()?;
        operation.writable(&presentation)?;
        Ok(Prepared {
            preflight: operation.preflight(&presentation, Some(snapshot))?,
            status,
            record,
            generation,
        })
    }

    async fn observe_sample_rate(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        identity: u64,
        expected: Option<u32>,
        duration: Duration,
    ) -> Result<Value, ControlError> {
        let (record, _) = sample_rate_record(device, identity)?;
        let fields = self
            .status_requests
            .query_with_deadline(
                transport,
                &record.key,
                StatusQuery::SampleRate,
                duration,
                |fields| {
                    expected.is_none_or(|value| fields.get("sample_rate_hz") == Some(&json!(value)))
                },
            )
            .await
            .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
        self.accept_sample_rate_fields(device, identity, fields.clone())?;
        sample_rate::status(&json!(fields))
    }

    fn accept_sample_rate_fields(
        &self,
        device: &Device,
        identity: u64,
        fields: Map<String, Value>,
    ) -> Result<(), ControlError> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.identity_generation != identity || !state.record.online {
            return Err(ControlError::Conflict(
                "device identity changed during sample-rate observation".to_owned(),
            ));
        }

        for (name, value) in fields {
            state.record.properties.insert(name.clone(), value);
            state.field_observed_at.insert(name, Instant::now());
        }

        self.publish(&state.record)?;
        Ok(())
    }

    async fn capture_sample_rate_topology(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        identity: u64,
        rate: u32,
    ) -> Result<(DeviceRecord, Value, u64), ControlError> {
        let (mut record, generation) = sample_rate_record(device, identity)?;
        let started = Instant::now();
        let protocol = record
            .routing_protocol()?
            .context("device has no ARC protocol")?
            .protocol_id;
        let counts = inventory::channel_count(transport)
            .await
            .map_err(readback_failure)?;
        record.apply_counts(&counts)?;

        if netaudio_core::protocol::is_modern_arc_protocol(protocol) {
            for (kind, receiver) in [("rx_channels", true), ("tx_channels", false)] {
                let observed = inventory::paged_inventory(transport, kind, protocol, |_| {})
                    .await
                    .map_err(readback_failure)?;
                record.apply_modern_channels(&observed, receiver)?;
            }
        } else {
            let channels = inventory::legacy_channels(transport, &counts)
                .await
                .map_err(readback_failure)?;
            record.apply_legacy_channels(channels)?;
        }

        if record.properties.get("sample_rate_hz") != Some(&json!(rate)) {
            return Err(ControlError::Conflict(
                "channel inventory disagrees with observed sample rate".to_owned(),
            ));
        }

        let flows = flows::read_transmit(transport, &flows::protocols(&record)?).await?;
        let verified_counts = inventory::channel_count(transport)
            .await
            .map_err(readback_failure)?;

        if counts.rx_count != verified_counts.rx_count
            || counts.tx_count != verified_counts.tx_count
        {
            return Err(ControlError::Conflict(
                "channel counts changed while observing sample-rate topology".to_owned(),
            ));
        }

        record
            .properties
            .insert("flow_protocol_id".to_owned(), json!(flows.protocol));
        record
            .properties
            .insert("transmitter_flows".to_owned(), flows.raw["flows"].clone());
        record
            .properties
            .insert("transmitter_flow_inventory".to_owned(), flows.raw);
        let snapshot = sample_rate::snapshot(&record, flows.protocol, rate)?;
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.identity_generation != identity
            || state.generation != generation
            || !state.record.online
        {
            return Err(ControlError::Conflict(
                "device topology changed during observation".to_owned(),
            ));
        }

        for (field, observed_at) in &state.field_observed_at {
            if *observed_at > started {
                if let Some(value) = state.record.properties.get(field) {
                    record.properties.insert(field.clone(), value.clone());
                } else {
                    record.properties.remove(field);
                }
            }
        }

        if record.properties.get("sample_rate_hz") != Some(&json!(rate)) {
            return Err(ControlError::Conflict(
                "sample rate changed during topology observation".to_owned(),
            ));
        }

        self.publish(&record)?;
        state.record = record.clone();
        Ok((record, snapshot, generation))
    }
}

fn sample_rate_record(device: &Device, identity: u64) -> Result<(DeviceRecord, u64), ControlError> {
    let state = device
        .state
        .lock()
        .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

    if state.identity_generation != identity || !state.record.online {
        return Err(ControlError::Conflict(
            "device identity changed during sample-rate operation".to_owned(),
        ));
    }

    Ok((state.record.clone(), state.generation))
}

fn readback_failure(error: crate::devices::transport::TransportError) -> ControlError {
    ControlError::ObservationUnavailable(error.to_string())
}

fn completion(result: Value, errors: Vec<String>) -> ControlCompletion {
    ControlCompletion {
        acknowledged_commands: 0,
        readback_completed: result["state_verified"] == true,
        readback_errors: errors,
        observation: Some(result),
    }
}
