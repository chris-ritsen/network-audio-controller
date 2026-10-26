use super::{Device, DeviceRegistry, Inspection};
use crate::devices::audit::Audit;
use crate::devices::capabilities;
use crate::devices::network::Change;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::transport::DeviceTransport;
use crate::monitoring::requests::StatusQuery;
use anyhow::Context;
use serde_json::Value;
use std::time::Duration;

const READBACK_DEADLINE: Duration = Duration::from_secs(2);

impl DeviceRegistry {
    pub(super) async fn configure_network(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        requested: &Change,
        precondition: Option<&Value>,
        audit: Option<&mut Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        self.read_status(device, transport, generation, StatusQuery::Lock)
            .await?;
        self.inspect_status(device, transport, generation, Inspection::Network)
            .await?;
        let before = self.network_snapshot(device, generation)?;

        if precondition.is_some_and(|expected| {
            ControlOperation::NetworkConfiguration(requested.clone()).precondition(&before)
                != *expected
        }) {
            return Err(ControlError::Conflict("the device configuration changed after confirmation; obtain a new preview before retrying".to_owned()));
        }

        let command = requested.command(&before)?;
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
                audit.acknowledgement(&self.journal, &[], 1).await.context(
                    "record interface configuration transmission; do not replay the command",
                )?;
            }
        }

        let mut errors = Vec::new();
        let after = if sent {
            match self
                .observe_network_configuration(device, transport, generation, requested, &before)
                .await
            {
                Ok(after) => Some(after),
                Err(error) => {
                    errors.push(error.to_string());
                    None
                }
            }
        } else {
            Some(before.clone())
        };
        let mut observation = requested.completion(&before, after.as_ref(), sent);

        if let Some(after) = &after {
            observation["network"] = capabilities::network(after)?;

            if matches!(requested, Change::Redundancy(_)) {
                observation["effective_readback"] = capabilities::redundancy(after)?;
            }
        }

        Ok(ControlCompletion {
            acknowledged_commands: 0,
            readback_completed: after.is_some(),
            readback_errors: errors,
            observation: Some(observation),
        })
    }

    async fn observe_network_configuration(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        requested: &Change,
        before: &Value,
    ) -> Result<Value, ControlError> {
        let key = before["server_name"]
            .as_str()
            .context("device identity is missing")?;
        let query = requested.query(before);

        if matches!(query, StatusQuery::SwitchConfiguration) {
            self.read_status(device, transport, generation, StatusQuery::Interface)
                .await?;
        }

        let observed = tokio::select! {
            _ = self.cancellation.cancelled() => return Err(ControlError::Unavailable("device registry is stopping".to_owned())),
            observed = self.status_requests.query_with_deadline(transport, key, query, READBACK_DEADLINE, |fields| requested.matches(fields)) => observed.map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?,
        };
        self.accept_status(device, generation, observed)?;

        if matches!(query, StatusQuery::Interface) && Change::uses_switch_status(before) {
            self.read_status(
                device,
                transport,
                generation,
                StatusQuery::SwitchConfiguration,
            )
            .await?;
        }

        self.network_snapshot(device, generation)
    }

    fn network_snapshot(&self, device: &Device, generation: u64) -> Result<Value, ControlError> {
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation || !state.record.online {
            return Err(ControlError::Conflict(
                "device changed during network configuration".to_owned(),
            ));
        }

        Ok(state.record.presentation()?)
    }
}
