use super::{Device, DeviceRegistry};
use crate::devices::audit::Audit;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::status_configuration::Configuration;
use crate::devices::transport::DeviceTransport;
use crate::monitoring::requests::StatusQuery;
use anyhow::Context;
use serde_json::Value;

impl DeviceRegistry {
    pub(super) async fn configure_status(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        requested: &Configuration,
        precondition: Option<&Value>,
        audit: Option<&mut Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        self.read_status(device, transport, generation, StatusQuery::Lock)
            .await?;
        self.read_status(device, transport, generation, requested.query())
            .await?;
        let (command, before, key) = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
            if state.generation != generation || !state.record.online {
                return Err(ControlError::Conflict(
                    "device changed before device configuration".to_owned(),
                ));
            }

            let record = state.record.presentation()?;

            if precondition.is_some_and(|expected| {
                ControlOperation::StatusConfiguration(requested.clone()).precondition(&record)
                    != *expected
            }) {
                return Err(ControlError::Conflict("the device configuration changed after confirmation; obtain a new preview before retrying".to_owned()));
            }

            (
                requested.command(&record)?,
                state.record.properties.clone(),
                state.record.key.clone(),
            )
        };
        let sent = command.is_some();

        if let Some(command) = command {
            transport
                .send_command(&command)
                .await
                .map_err(|error| ControlError::Transport {
                    error,
                    acknowledged_commands: 0,
                })?;

            if let Some(audit) = audit {
                audit
                    .acknowledgement(&self.journal, &[], 1)
                    .await
                    .context("record configuration transmission; do not replay the command")?;
            }
        }

        let mut errors = Vec::new();
        let after = if sent {
            let observed = tokio::select! {
                _ = self.cancellation.cancelled() => Err(anyhow::anyhow!("device registry is stopping")),
                observed = self.status_requests.query_with_deadline(transport, &key, requested.query(), requested.deadline(), |fields| requested.matches(fields)) => observed,
            };
            match observed {
                Ok(fields) => match self.accept_status(device, generation, fields.clone()) {
                    Ok(()) => Some(fields),
                    Err(error) => {
                        errors.push(error.to_string());
                        None
                    }
                },
                Err(error) => {
                    errors.push(error.to_string());
                    None
                }
            }
        } else {
            Some(before)
        };
        Ok(ControlCompletion {
            acknowledged_commands: 0,
            readback_completed: after.is_some(),
            readback_errors: errors,
            observation: Some(requested.completion(after.as_ref(), sent)),
        })
    }
}
