use super::{Device, DeviceRegistry};
use crate::devices::audit::Audit;
use crate::devices::maintenance::Operation;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::transport::DeviceTransport;
use crate::monitoring::requests::{self, StatusQuery};
use crate::monitoring::status::StatusKind;
use anyhow::Context;
use serde_json::{Value, json};
use std::time::Duration;

const CLEAR_OBSERVATION_WINDOW: Duration = Duration::from_secs(3);

impl DeviceRegistry {
    pub(super) async fn maintain_device(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        operation: &Operation,
        precondition: Option<&Value>,
        mut audit: Option<&mut Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        let expected_identity = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
            state
                .record
                .properties
                .get("mac_address")
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        requests::register(transport, expected_identity.as_deref())
            .await
            .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
        self.read_status(device, transport, generation, StatusQuery::Lock)
            .await?;
        let (key, identity) = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation {
                return Err(ControlError::Conflict(
                    "device changed before maintenance; no command was sent".to_owned(),
                ));
            }

            let record = state.record.presentation()?;
            operation.validate(&record)?;

            if precondition.is_some_and(|expected| {
                ControlOperation::Maintenance(operation.clone()).precondition(&record) != *expected
            }) {
                return Err(ControlError::Conflict(
                    "device facts changed after confirmation; obtain a new preview before retrying"
                        .to_owned(),
                ));
            }

            (state.record.key.clone(), state.identity_generation)
        };
        let observation = if matches!(operation, Operation::ClearConfiguration { .. }) {
            Some(
                self.status_requests
                    .subscribe(&key, StatusKind::ClearConfiguration)?,
            )
        } else {
            None
        };
        transport
            .send_command(&json!({"command": operation.command_name()}))
            .await
            .map_err(|error| ControlError::Transport {
                error,
                acknowledged_commands: 0,
            })?;

        if let Some(audit) = audit.as_mut() {
            audit
                .acknowledgement(&self.journal, &[], 1)
                .await
                .context("record maintenance transmission; do not replay the command")?;
        }

        let Some(observation) = observation else {
            return Ok(completion(
                json!({"accepted": true, "verified": false, "change_sent": true}),
                None,
            ));
        };
        let observed = tokio::select! {
            _ = self.cancellation.cancelled() => Err(anyhow::anyhow!("device registry stopped during clear verification")),
            observed = observation.wait(CLEAR_OBSERVATION_WINDOW, |fields| fields["clear_configuration_status"]["executed_action"] == operation.command_name()) => observed,
        };
        let fields = match observed {
            Ok(fields) => fields,
            Err(error) => {
                return Ok(completion(
                    json!({
                        "error": format!("clear command was sent but execution was not confirmed: {error}"),
                        "cleared": false, "change_sent": true, "verified": false, "rebooting": false,
                    }),
                    Some(error.to_string()),
                ));
            }
        };
        let status = &fields["clear_configuration_status"];

        if status["executed_action"] != operation.command_name() {
            return Ok(completion(
                json!({
                    "error": "device did not confirm the requested clear mode", "cleared": false,
                    "change_sent": true, "verified": false, "rebooting": false, "observed": status,
                }),
                Some("clear execution disagrees with request".to_owned()),
            ));
        }

        let mut result = json!({
            "cleared": true, "mode": operation.requested()["mode"], "executed_mode": status["executed_mode"],
            "change_sent": true, "execution_confirmed": true, "verified": false, "rebooting": false,
            "takes_effect": "at the next reboot on devices that defer clears",
        });
        {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.identity_generation != identity || !state.record.online {
                result["error"] = json!(
                    "clear execution was reported, but device identity changed; no follow-up reboot was sent"
                );
                return Ok(completion(
                    result,
                    Some("device changed after clear execution".to_owned()),
                ));
            }
        }

        self.request_refresh(&key)?;

        if matches!(
            operation,
            Operation::ClearConfiguration { reboot: true, .. }
        ) {
            let reboot = async {
                requests::register(transport, expected_identity.as_deref()).await?;
                {
                    let state = device
                        .state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
                    anyhow::ensure!(
                        state.identity_generation == identity && state.record.online,
                        "device changed before follow-up reboot"
                    );
                }

                transport
                    .send_command(&json!({"command": "reboot"}))
                    .await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;

            if let Err(error) = reboot {
                result["error"] = json!(format!(
                    "configuration clear was confirmed, but the follow-up reboot failed: {error}"
                ));
                return Ok(completion(result, Some(error.to_string())));
            }

            if let Some(audit) = audit {
                audit
                    .acknowledgement(&self.journal, &[], 2)
                    .await
                    .context("record follow-up reboot transmission; do not replay the command")?;
            }

            result["rebooting"] = json!(true);
            result["reboot_verified"] = json!(false);
            result["takes_effect"] = json!("now rebooting");
        }

        Ok(completion(result, None))
    }
}

fn completion(result: Value, error: Option<String>) -> ControlCompletion {
    ControlCompletion {
        acknowledged_commands: 0,
        readback_completed: result["execution_confirmed"] == true,
        readback_errors: error.into_iter().collect(),
        observation: Some(result),
    }
}
