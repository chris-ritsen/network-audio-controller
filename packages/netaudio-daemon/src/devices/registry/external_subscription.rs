use super::{Device, DeviceRegistry, read_inventory};
use crate::devices::audit::Audit;
use crate::devices::external_subscription::ExternalSubscription;
use crate::devices::inventory;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::transport::{DeviceTransport, TransportError};
use crate::monitoring::requests::StatusQuery;
use anyhow::Context;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

const READBACK_WINDOW: Duration = Duration::from_secs(2);

pub(super) struct ReceiverBaseline {
    protocol: u16,
    pub before: Option<Value>,
    family: Option<&'static str>,
    pub errors: Vec<String>,
}

impl DeviceRegistry {
    pub(super) async fn refresh_external_receiver(
        &self,
        device: &Arc<Device>,
        transport: &mut DeviceTransport,
        generation: u64,
    ) -> Result<ReceiverBaseline, ControlError> {
        let protocol = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
            state
                .record
                .routing_protocol()?
                .context("device has no ARC service metadata")?
                .protocol_id
        };
        let started = Instant::now();
        let fresh = tokio::select! {
            _ = self.cancellation.cancelled() => return Err(stopping()),
            fresh = read_inventory(transport, protocol) => fresh,
        };
        let fresh = fresh.map_err(|error| {
            ControlError::ObservationUnavailable(format!(
                "fresh receiver baseline unavailable: {error}; no request was sent"
            ))
        })?;
        let before = fresh.receiver_flows.clone();
        let family = fresh.receiver_flow_family;
        let channels_observed = fresh.channels.is_some() || fresh.modern_receivers.is_some();
        let report = self.accept_inventory(device, generation, started, fresh)?;

        if report.superseded {
            return Err(ControlError::Conflict(
                "device inventory changed during subscription preparation; no request was sent"
                    .to_owned(),
            ));
        }

        if !channels_observed {
            return Err(ControlError::ObservationUnavailable(
                "fresh receiver channels unavailable; no request was sent".to_owned(),
            ));
        }

        for query in [
            StatusQuery::Lock,
            StatusQuery::Aes67,
            StatusQuery::SampleRate,
            StatusQuery::Encoding,
        ] {
            self.read_status(device, transport, generation, query)
                .await
                .map_err(|error| match error {
                    ControlError::ObservationUnavailable(message) => {
                        ControlError::Conflict(format!(
                            "fresh flow preconditions unavailable: {message}; no request was sent"
                        ))
                    }
                    error => error,
                })?;
        }

        Ok(ReceiverBaseline {
            protocol,
            before,
            family,
            errors: report.errors.into_values().collect(),
        })
    }

    pub(super) async fn execute_external_subscription(
        &self,
        device: &Arc<Device>,
        transport: &mut DeviceTransport,
        subscription: &ExternalSubscription,
        generation: u64,
        precondition: Option<Value>,
        audit: Option<&mut Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        subscription.validate_source(&self.model)?;
        let ReceiverBaseline {
            protocol,
            before,
            family,
            errors,
        } = self
            .refresh_external_receiver(device, transport, generation)
            .await?;
        let mut completion = ControlCompletion {
            acknowledged_commands: 0,
            readback_completed: false,
            readback_errors: errors,
            observation: None,
        };

        let operation = ControlOperation::ExternalSubscription(subscription.clone());
        let (mut plan, command, identity) = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation || !state.record.online {
                return Err(ControlError::Conflict(
                    "device changed before external subscription request".to_owned(),
                ));
            }

            let record = state.record.presentation()?;

            if precondition
                .as_ref()
                .is_some_and(|expected| operation.precondition(&record) != *expected)
            {
                return Err(ControlError::Conflict("the device configuration changed after confirmation; obtain a new preview before retrying".to_owned()));
            }

            (
                operation.plan(&state.record)?,
                subscription.command(&state.record)?,
                record["mac_address"].clone(),
            )
        };
        if before.is_none() {
            completion.observation =
                Some(subscription.completion(&command, None, None, None, false)?);
            return Ok(completion);
        }

        let mut changes = device.inventory_changes.subscribe();
        subscription.validate_source(&self.model)?;

        if self.cancellation.is_cancelled() {
            return Err(stopping());
        }

        let execution = plan.execute(transport).await;

        if matches!(
            &execution,
            Err(ControlError::Transport {
                error: TransportError::InvalidCommand(_) | TransportError::Network(_),
                ..
            })
        ) {
            return execution.map(|_| completion);
        }

        if let Some(audit) = audit {
            audit
                .acknowledgement(&self.journal, &plan.acknowledgements, plan.command_count())
                .await
                .context(
                    "record external subscription acknowledgement; do not replay the command",
                )?;
        }

        let receipt = plan.acknowledgements.first().map(Vec::as_slice);
        let mut after = None;
        completion.acknowledged_commands = execution.as_ref().copied().unwrap_or_default();

        if execution.is_ok() {
            let deadline = Instant::now() + READBACK_WINDOW;
            let kind = match family {
                Some("legacy") => "fixed_rx_flows",
                Some("modern") => "segmented_rx_flows",
                _ => {
                    return Err(ControlError::Unavailable(
                        "receiver flow query family is unknown".to_owned(),
                    ));
                }
            };

            loop {
                let queried = tokio::select! {
                    _ = self.cancellation.cancelled() => break,
                    queried = timeout_at(deadline, inventory::paged_inventory(transport, kind, protocol, |_| {})) => queried,
                };

                match queried {
                    Ok(Ok(inventory)) => {
                        let mut state = device
                            .state
                            .lock()
                            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

                        if !state.record.online
                            || state.record.control_address() != Some(transport.address())
                            || state.record.properties.get("mac_address") != Some(&identity)
                        {
                            completion.readback_errors.push(
                                "device identity changed during receiver flow readback".to_owned(),
                            );
                            after = None;
                            break;
                        }

                        state.record.apply_receiver_inventory(
                            family,
                            Some(inventory.clone()),
                            None,
                            None,
                        )?;
                        self.publish(&state.record)?;
                        after = Some(inventory);
                    }
                    Ok(Err(error)) => {
                        after = None;
                        completion.readback_errors.push(error.to_string());
                    }
                    Err(_) => {
                        after = None;
                        completion
                            .readback_errors
                            .push("receiver flow readback deadline elapsed".to_owned());
                        break;
                    }
                }

                let observed = subscription.completion(
                    &command,
                    before.clone(),
                    after.clone(),
                    receipt,
                    true,
                )?;

                if observed["arc_effective_state_confirmed"] == true {
                    break;
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
        }

        let mut observed = subscription.completion(&command, before, after, receipt, true)?;

        if let Err(error) = execution {
            observed["transport_error"] = json!(error.to_string());
            observed["outcome_unknown"] = json!(matches!(
                error,
                ControlError::Transport {
                    error: TransportError::WriteOutcomeUnknown(_),
                    ..
                }
            ));
        }

        completion.readback_completed = !observed["arc_effective_state_confirmed"].is_null();
        completion.observation = Some(observed);
        Ok(completion)
    }
}

fn stopping() -> ControlError {
    ControlError::Unavailable("device registry is stopping".to_owned())
}
