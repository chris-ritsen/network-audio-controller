use super::{Device, DeviceRegistry, read_inventory};
use crate::devices::audit::Audit;
use crate::devices::flow_authoring::{self, Operation};
use crate::devices::flows::{self, TransmitInventory};
use crate::devices::inventory;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::record::DeviceRecord;
use crate::devices::transport::{CommandIntent, DeviceTransport, TransportError};
use crate::monitoring::requests::StatusQuery;
use anyhow::Context;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

const READBACK_WINDOW: Duration = Duration::from_secs(5);

impl DeviceRegistry {
    pub async fn plan_transmit_flow(
        &self,
        reference: &str,
        operation: &Operation,
    ) -> Result<ControlCompletion, ControlError> {
        if !operation.is_planning() {
            return Err(ControlError::Invalid(
                "flow planning requires a planning operation".to_owned(),
            ));
        }

        let device = self.controlled_device(reference)?;
        let record = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?
            .record
            .clone();
        let snapshot = record.presentation()?;
        let control = ControlOperation::FlowAuthoring(operation.clone());
        let audit = Audit::begin(&self.journal, snapshot.clone(), &control).await?;
        let result = operation.plan(&record).map(|plan| {
            completion(json!({
                "device": record.key,
                "plan": plan.presentation,
                "mutation_sent": false,
                "operation_id": audit.as_ref().map(Audit::identifier),
            }))
        });

        if let Some(audit) = audit {
            audit
                .finish(&self.journal, &control, snapshot, &result)
                .await?;
        }

        result
    }

    pub(super) async fn author_flow(
        &self,
        device: &Arc<Device>,
        transport: &mut DeviceTransport,
        generation: u64,
        operation: &Operation,
        precondition: Option<&Value>,
        audit: Option<&mut Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        let original = self.flow_record(device, generation)?;
        let protocol = original
            .routing_protocol()?
            .context("flow protocol is unknown")?
            .protocol_id;
        let started = Instant::now();
        let fresh = tokio::select! {
            _ = self.cancellation.cancelled() => return Err(stopping()),
            fresh = read_inventory(transport, protocol) => fresh?,
        };
        let channels_observed = fresh.channels.is_some() || fresh.modern_transmitters.is_some();
        let report = self.accept_inventory(device, generation, started, fresh)?;

        if report.superseded || (!operation.is_delete() && !channels_observed) {
            return Err(ControlError::ObservationUnavailable(
                "fresh transmitter channels unavailable; no request was sent".to_owned(),
            ));
        }

        self.read_status(device, transport, generation, StatusQuery::Lock)
            .await?;

        if !operation.is_delete() {
            for query in [StatusQuery::SampleRate, StatusQuery::Encoding] {
                self.read_status(device, transport, generation, query)
                    .await?;
            }

            if operation.requested()["media_mode"] == "rtp_aes67" {
                self.read_status(device, transport, generation, StatusQuery::Aes67)
                    .await?;
            }
        }

        let record = self.flow_record(device, generation)?;
        let plan = operation.plan(&record)?;
        let protocols = flows::protocols(&record)?;
        let before = tokio::select! {
            _ = self.cancellation.cancelled() => return Err(stopping()),
            inventory = flows::read_transmit(transport, &protocols) => inventory?,
        };
        self.accept_transmit_inventory(device, generation, &before)?;
        let record = self.flow_record(device, generation)?;

        let presented = record.presentation()?;

        if precondition.is_some_and(|expected| {
            ControlOperation::FlowAuthoring(operation.clone()).precondition(&presented) != *expected
        }) {
            return Err(ControlError::Conflict(
                "flow facts changed after confirmation; obtain a new preview before retrying"
                    .to_owned(),
            ));
        }

        if operation.is_planning() {
            return Ok(completion(
                json!({"device": record.key, "plan": plan.presentation, "mutation_sent": false}),
            ));
        }

        let mut evidence = vec![
            json!({"phase": "preflight", "attempt": 1, "inventory": before.raw, "outcome": "available"}),
        ];

        if !plan.native.reasons.is_empty() {
            return Ok(completion(flow_authoring::result(
                operation,
                "unsupported",
                false,
                &Value::Null,
                None,
                &plan.native.reasons.join("; "),
                &evidence,
            )));
        }

        let preflight = operation.preflight(&record, before.protocol, &before.raw)?;

        if preflight["state"] != "ready" {
            let state = if preflight["state"] == "unavailable" {
                "pending"
            } else {
                "rejected"
            };
            return Ok(completion(flow_authoring::result(
                operation,
                state,
                false,
                &Value::Null,
                None,
                preflight["reason"]
                    .as_str()
                    .unwrap_or("flow preflight was not ready"),
                &evidence,
            )));
        }

        if operation.is_delete() {
            evidence.push(
                json!({"phase": "preflight", "selected_specification": preflight["specification"]}),
            );
        }

        let command = match operation {
            Operation::Delete(_) => preflight["command"].clone(),
            _ => Value::Object(
                plan.native
                    .command
                    .context("supported flow plan has no command")?,
            ),
        };
        let identity_generation = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation || !state.record.online {
                return Err(ControlError::Conflict(
                    "device changed before flow mutation; no request was sent".to_owned(),
                ));
            }

            state.identity_generation
        };
        let mut changes = device.inventory_changes.subscribe();
        let response = match transport.execute(&command, CommandIntent::Write).await {
            Ok(response) => Some(response),
            Err(TransportError::WriteOutcomeUnknown(message)) => {
                evidence.push(json!({"phase": "request", "attempt": 1, "outcome": "unknown", "details": {"reason": message}}));
                None
            }
            Err(error) => {
                return Err(ControlError::Transport {
                    error,
                    acknowledged_commands: 0,
                });
            }
        };
        let receipt = response
            .as_ref()
            .map(|response| {
                serde_json::to_value(netaudio_core::responses::command_receipt(response))
            })
            .transpose()
            .map_err(anyhow::Error::from)?
            .unwrap_or(Value::Null);

        if let Some(audit) = audit {
            audit
                .acknowledgement(&self.journal, &response.into_iter().collect::<Vec<_>>(), 1)
                .await
                .context("record flow mutation receipt; do not replay the command")?;
        }

        if receipt["accepted"] == false {
            return Ok(completion(flow_authoring::result(
                operation,
                "rejected",
                true,
                &receipt,
                None,
                "device rejected the transmit-flow request",
                &evidence,
            )));
        }

        let capability = record
            .properties
            .get("transmit_flow_authoring_capability_word")
            .and_then(Value::as_u64)
            .and_then(|word| u16::try_from(word).ok())
            .context("flow authoring capability disappeared")?;
        let family = netaudio_core::parser::flow_authoring_capabilities(capability)
            .receiver_flow_query_family;
        let mut authoring_refreshed = false;
        let allocation = receipt["allocation"]["global_flow_id"]
            .as_u64()
            .and_then(|identifier| u16::try_from(identifier).ok());
        let deadline = Instant::now() + READBACK_WINDOW;
        let readback_protocols = [before.protocol];
        let mut observation = None;
        let mut attempt = 0;

        loop {
            changes.borrow_and_update();
            attempt += 1;

            if !authoring_refreshed {
                let refreshed = tokio::select! {
                    _ = self.cancellation.cancelled() => break,
                    refreshed = timeout_at(deadline, self.refresh_flow_family(device, transport, identity_generation, protocol, family)) => refreshed,
                };

                match refreshed {
                    Ok(Ok(())) => authoring_refreshed = true,
                    Ok(Err(error)) => evidence.push(json!({
                        "phase": "authoring_family_refresh",
                        "attempt": attempt,
                        "outcome": "unavailable",
                        "details": {"error": error.to_string()},
                    })),
                    Err(_) => break,
                }
            }

            let after = tokio::select! {
                _ = self.cancellation.cancelled() => break,
                inventory = timeout_at(deadline, flows::read_transmit(transport, &readback_protocols)) => inventory,
            };

            match after {
                Ok(Ok(after)) => {
                    if let Err(error) =
                        self.accept_flow_readback(device, identity_generation, &after)
                    {
                        evidence.push(json!({
                            "phase": "post_write",
                            "attempt": attempt,
                            "outcome": "identity_changed",
                            "details": {"error": error.to_string()},
                        }));
                        break;
                    }

                    let readback = operation.inspect_readback(
                        &before.raw,
                        &after.raw,
                        before.protocol,
                        allocation,
                        authoring_refreshed,
                    );

                    match readback {
                        Ok(readback) => {
                            let outcome = readback["verification"]["outcome"]
                                .as_str()
                                .unwrap_or("unavailable")
                                .to_owned();
                            evidence.push(json!({
                                "phase": "post_write",
                                "attempt": attempt,
                                "inventory": after.raw,
                                "outcome": outcome,
                                "details": readback,
                            }));
                            observation = Some(readback);

                            if matches!(outcome.as_str(), "confirmed" | "contradiction") {
                                let (state, message) = if outcome == "contradiction" {
                                    (
                                        "inconsistent",
                                        "fresh inventory contradicts the flow operation",
                                    )
                                } else if operation.is_delete() {
                                    ("deleted", "fresh inventory verified flow deletion")
                                } else {
                                    ("confirmed", "fresh inventory verified flow creation")
                                };
                                return Ok(completion(flow_authoring::result(
                                    operation,
                                    state,
                                    true,
                                    &receipt,
                                    observation.as_ref(),
                                    message,
                                    &evidence,
                                )));
                            }
                        }
                        Err(error) => evidence.push(json!({
                            "phase": "post_write",
                            "attempt": attempt,
                            "inventory": after.raw,
                            "outcome": "unavailable",
                            "details": {"error": error.to_string()},
                        })),
                    }
                }
                Ok(Err(error)) => evidence.push(json!({
                    "phase": "post_write",
                    "attempt": attempt,
                    "outcome": "unavailable",
                    "details": {"error": error.to_string()},
                })),
                Err(_) => break,
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

        let state = if receipt["accepted"] == true {
            "partial"
        } else {
            "pending"
        };
        Ok(completion(flow_authoring::result(
            operation,
            state,
            true,
            &receipt,
            observation.as_ref(),
            "bounded fresh readback did not confirm the flow operation; no mutation retry was sent",
            &evidence,
        )))
    }

    async fn refresh_flow_family(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        identity_generation: u64,
        protocol: u16,
        family: &str,
    ) -> Result<(), ControlError> {
        let (kind, presentation) = match family {
            "fixed" => ("fixed_rx_flows", "legacy"),
            "segmented" => ("segmented_rx_flows", "modern"),
            _ => {
                return Err(ControlError::Conflict(
                    "unknown receive-flow inventory family".to_owned(),
                ));
            }
        };
        let inventory = inventory::paged_inventory(transport, kind, protocol, |_| {})
            .await
            .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.identity_generation != identity_generation || !state.record.online {
            return Err(ControlError::Conflict(
                "device identity changed during flow verification".to_owned(),
            ));
        }

        state
            .record
            .apply_receiver_inventory(Some(presentation), Some(inventory), None, None)?;
        self.publish(&state.record)?;
        Ok(())
    }

    fn accept_flow_readback(
        &self,
        device: &Device,
        identity_generation: u64,
        inventory: &TransmitInventory,
    ) -> Result<(), ControlError> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.identity_generation != identity_generation || !state.record.online {
            return Err(ControlError::Conflict(
                "device identity changed during flow readback".to_owned(),
            ));
        }

        for (name, value) in transmit_inventory_fields(inventory) {
            state.record.properties.insert(name.clone(), value);
            state.field_observed_at.insert(name, Instant::now());
        }

        self.publish(&state.record)?;
        Ok(())
    }

    fn flow_record(&self, device: &Device, generation: u64) -> Result<DeviceRecord, ControlError> {
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation || !state.record.online {
            return Err(ControlError::Conflict(
                "device changed during flow operation".to_owned(),
            ));
        }

        Ok(state.record.clone())
    }

    fn accept_transmit_inventory(
        &self,
        device: &Device,
        generation: u64,
        inventory: &TransmitInventory,
    ) -> Result<(), ControlError> {
        let fields = transmit_inventory_fields(inventory);
        self.accept_status(device, generation, fields)
    }
}

fn completion(observation: Value) -> ControlCompletion {
    ControlCompletion {
        acknowledged_commands: usize::from(
            observation["request_acknowledgement"]["accepted"] == true,
        ),
        readback_completed: observation["effective_state_confirmation"].is_boolean(),
        readback_errors: Vec::new(),
        observation: Some(observation),
    }
}

fn stopping() -> ControlError {
    ControlError::Unavailable("device registry is stopping".to_owned())
}

fn transmit_inventory_fields(inventory: &TransmitInventory) -> serde_json::Map<String, Value> {
    serde_json::Map::from_iter([
        ("flow_protocol_id".to_owned(), json!(inventory.protocol)),
        (
            "transmitter_flows".to_owned(),
            inventory.raw["flows"].clone(),
        ),
        (
            "transmitter_flow_inventory".to_owned(),
            inventory.raw.clone(),
        ),
    ])
}
