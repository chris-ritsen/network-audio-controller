use super::{Device, DeviceRegistry};
use crate::devices::capabilities;
use crate::devices::operations::{ControlCompletion, ControlError};
use crate::devices::transport::DeviceTransport;
use crate::monitoring::requests::StatusQuery;
use anyhow::Result;
use serde_json::{Value, json};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug)]
pub enum Inspection {
    Status(StatusQuery),
    Lock,
    Latency,
    MulticastPrefix,
    Network,
    NetworkConfiguration,
    Redundancy,
}

impl DeviceRegistry {
    pub(super) async fn inspect_status(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        inspection: Inspection,
    ) -> Result<ControlCompletion, ControlError> {
        match inspection {
            Inspection::MulticastPrefix => {
                return self
                    .configure_multicast_prefix(device, transport, generation, None, None, None)
                    .await;
            }
            Inspection::Latency => {
                self.read_status(device, transport, generation, StatusQuery::Lock)
                    .await?;
                self.read_latency(device, transport, generation).await?;
            }
            Inspection::Status(query) => {
                self.read_status(device, transport, generation, StatusQuery::Lock)
                    .await?;
                self.read_status(device, transport, generation, query)
                    .await?;
            }
            Inspection::Lock => {
                if let Err(error) = self
                    .read_status(device, transport, generation, StatusQuery::Lock)
                    .await
                {
                    let mut state = device
                        .state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

                    if state.generation == generation {
                        for field in ["is_locked", "lock_reset_status", "lock_status_observed_at"] {
                            state
                                .record
                                .properties
                                .insert(field.to_owned(), Value::Null);
                            state.field_observed_at.remove(field);
                        }

                        self.publish(&state.record)?;
                    }

                    return Err(error);
                }
            }
            Inspection::Network | Inspection::NetworkConfiguration | Inspection::Redundancy => {
                if matches!(inspection, Inspection::NetworkConfiguration) {
                    self.read_status(device, transport, generation, StatusQuery::Lock)
                        .await?;
                }

                self.read_status(device, transport, generation, StatusQuery::Interface)
                    .await?;
                let support = {
                    let state = device
                        .state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
                    capabilities::operation(&state.record.presentation()?, "redundancy")?.supported
                };
                let outcome = if support == Some(false) {
                    "skipped_unsupported"
                } else {
                    match self
                        .read_status(
                            device,
                            transport,
                            generation,
                            StatusQuery::SwitchConfiguration,
                        )
                        .await
                    {
                        Ok(()) => "response",
                        Err(ControlError::ObservationUnavailable(_)) => "timeout",
                        Err(error) => return Err(error),
                    }
                };
                let mut state = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

                if state.generation != generation || !state.record.online {
                    return Err(ControlError::Conflict(
                        "device changed during network inspection".to_owned(),
                    ));
                }

                if outcome == "timeout"
                    && let Some(redundancy) = state
                        .record
                        .properties
                        .get_mut("dante_redundancy")
                        .and_then(Value::as_object_mut)
                {
                    redundancy.insert("available_modes_fresh".to_owned(), json!(false));
                    redundancy.insert("state_fresh".to_owned(), json!(false));
                }

                let outcomes = state
                    .record
                    .properties
                    .entry("redundancy_probe_outcomes")
                    .or_insert_with(|| json!({}));
                outcomes["switch_configuration"] = json!({
                    "outcome": outcome,
                    "observational": support.is_none(),
                    "observed_at_unix": chrono::Utc::now().timestamp_micros() as f64 / 1_000_000.0,
                });
                self.publish(&state.record)?;
            }
        }

        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        let record = state.record.presentation()?;
        let observation = match inspection {
            Inspection::Status(_) | Inspection::Latency | Inspection::MulticastPrefix => {
                record.clone()
            }
            Inspection::Lock => json!({
                "device": record["server_name"],
                "is_locked": record["is_locked"],
                "lock_state_code": record["lock_reset_status"]["lock_state_code"],
                "status_code": record["lock_reset_status"]["status_code"],
                "observed_at": record["lock_status_observed_at"],
                "observation_source": "observed_after_0x1008",
                "operation_availability": record["operation_availability"]["locking"],
            }),
            Inspection::Network | Inspection::NetworkConfiguration => {
                capabilities::network(&record)?
            }
            Inspection::Redundancy => {
                json!({"device": record["server_name"], "redundancy": capabilities::redundancy(&record)?})
            }
        };
        Ok(ControlCompletion {
            acknowledged_commands: 0,
            readback_completed: true,
            readback_errors: Vec::new(),
            observation: Some(observation),
        })
    }

    pub(super) async fn read_status(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        query: StatusQuery,
    ) -> Result<(), ControlError> {
        let (key, identity) = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation || !state.record.online {
                return Err(ControlError::Conflict(
                    "device changed before status observation".to_owned(),
                ));
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
        let fields = tokio::select! {
            _ = self.cancellation.cancelled() => return Err(ControlError::Unavailable("device registry is stopping".to_owned())),
            observed = self.status_requests.query(transport, &key, identity.as_deref(), query) => observed.map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?,
        };
        self.accept_status(device, generation, fields)
    }

    pub(super) fn accept_status(
        &self,
        device: &Device,
        generation: u64,
        fields: serde_json::Map<String, Value>,
    ) -> Result<(), ControlError> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation || !state.record.online {
            return Err(ControlError::Conflict(
                "device changed during status observation".to_owned(),
            ));
        }

        for (field, value) in fields {
            state.record.properties.insert(field.clone(), value);
            state.field_observed_at.insert(field, Instant::now());
        }

        self.publish(&state.record)?;
        Ok(())
    }
}
