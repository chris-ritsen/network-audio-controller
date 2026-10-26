use super::{Device, DeviceRegistry};
use crate::devices::audit::Audit;
use crate::devices::inventory;
use crate::devices::multicast_prefix::Configuration;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::transport::{CommandIntent, DeviceTransport, TransportError};
use crate::monitoring::requests::StatusQuery;
use anyhow::Context;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

const READBACK_WINDOW: Duration = Duration::from_secs(3);

impl DeviceRegistry {
    pub(super) async fn configure_multicast_prefix(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        configuration: Option<&Configuration>,
        precondition: Option<&Value>,
        audit: Option<&mut Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        self.read_status(device, transport, generation, StatusQuery::Lock)
            .await?;
        self.read_status(device, transport, generation, StatusQuery::Aes67)
            .await?;
        let identity = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
            state.identity_generation
        };
        let before = self
            .read_multicast_prefix(device, transport, identity)
            .await?;
        let Some(configuration) = configuration else {
            return Ok(ControlCompletion {
                acknowledged_commands: 0,
                readback_completed: true,
                readback_errors: Vec::new(),
                observation: Some(json!({"prefix": before})),
            });
        };
        let command = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation || !state.record.online {
                return Err(ControlError::Conflict(
                    "device changed before prefix configuration; no request was sent".to_owned(),
                ));
            }

            let record = state.record.presentation()?;

            if precondition.is_some_and(|expected| {
                ControlOperation::MulticastPrefix(configuration.clone()).precondition(&record)
                    != *expected
            }) {
                return Err(ControlError::Conflict(
                    "prefix configuration changed after confirmation; obtain a new preview"
                        .to_owned(),
                ));
            }

            configuration.command(&record)?
        };
        let mut errors = Vec::new();
        let mut receipt = Value::Null;
        let sent = command.is_some();
        let mut observed = before;

        if let Some(command) = command {
            let mut changes = device.inventory_changes.subscribe();
            let responses = match transport.execute(&command, CommandIntent::Write).await {
                Ok(response) => vec![response],
                Err(TransportError::WriteOutcomeUnknown(error)) => {
                    errors.push(error);
                    Vec::new()
                }
                Err(error) => {
                    return Err(ControlError::Transport {
                        error,
                        acknowledged_commands: 0,
                    });
                }
            };

            if let Some(response) = responses.first() {
                receipt = serde_json::to_value(netaudio_core::responses::command_receipt(response))
                    .map_err(anyhow::Error::from)?;
            }

            if let Some(audit) = audit {
                audit
                    .acknowledgement(&self.journal, &responses, 1)
                    .await
                    .context("record multicast prefix receipt; do not replay the command")?;
            }

            if receipt["accepted"] == false {
                return Err(ControlError::Rejected {
                    result_code: receipt["result_code"]
                        .as_u64()
                        .and_then(|code| u16::try_from(code).ok())
                        .context("rejected prefix receipt has no result code")?,
                    acknowledged_commands: 0,
                });
            }

            observed = None;
            let deadline = Instant::now() + READBACK_WINDOW;

            loop {
                changes.borrow_and_update();
                let readback = tokio::select! {
                    _ = self.cancellation.cancelled() => break,
                    readback = timeout_at(deadline, self.read_multicast_prefix(device, transport, identity)) => readback,
                };

                match readback {
                    Ok(Ok(prefix)) => observed = prefix,
                    Ok(Err(error)) => {
                        observed = None;
                        errors.push(error.to_string());
                    }
                    Err(_) => break,
                }

                if observed.as_deref() == Some(configuration.prefix.to_string().as_str()) {
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

        Ok(ControlCompletion {
            acknowledged_commands: usize::from(receipt["accepted"] == true),
            readback_completed: observed.is_some(),
            readback_errors: errors,
            observation: Some(configuration.completion(observed.as_deref(), sent, &receipt)),
        })
    }

    async fn read_multicast_prefix(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        identity: u64,
    ) -> Result<Option<String>, ControlError> {
        let settings = inventory::settings(transport)
            .await
            .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.identity_generation != identity || !state.record.online {
            return Err(ControlError::Conflict(
                "device identity changed during multicast prefix observation".to_owned(),
            ));
        }

        state.record.properties.insert(
            "aes67_multicast_prefix".to_owned(),
            json!(settings.aes67_multicast_prefix),
        );
        state
            .field_observed_at
            .insert("aes67_multicast_prefix".to_owned(), Instant::now());
        self.publish(&state.record)?;
        Ok(settings.aes67_multicast_prefix)
    }
}
