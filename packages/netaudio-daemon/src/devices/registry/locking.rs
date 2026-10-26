use super::{Device, DeviceRegistry};
use crate::devices::audit::Audit;
use crate::devices::locking::{Exchange, Operation, Settings};
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::transport::DeviceTransport;
use crate::monitoring::requests::StatusQuery;
use anyhow::{Context, Result};
use serde_json::{Value, json};

impl DeviceRegistry {
    pub fn configure_locking(&self, settings: Settings) -> Result<()> {
        self.lock_settings
            .set(settings)
            .map_err(|_| anyhow::anyhow!("device locking settings were already initialized"))
    }

    pub fn lock_key(&self) -> Result<String, ControlError> {
        self.lock_settings
            .get()
            .ok_or_else(|| ControlError::Unavailable("device_lock_key not configured".to_owned()))?
            .export()
    }

    pub(super) async fn configure_lock(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        operation: &Operation,
        precondition: Option<&Value>,
        audit: Option<&mut Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        let settings = self.lock_settings.get().ok_or_else(|| {
            ControlError::Unavailable("device_lock_key not configured".to_owned())
        })?;
        let key = settings.key()?;
        self.read_status(device, transport, generation, StatusQuery::Lock)
            .await?;
        let device_key = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
            let record = state.record.presentation()?;

            if state.generation != generation {
                return Err(ControlError::Conflict(
                    "device changed before lock operation; no change was sent".to_owned(),
                ));
            }

            operation.validate(&record)?;

            if precondition.is_some_and(|expected| {
                ControlOperation::Locking(operation.clone()).precondition(&record) != *expected
            }) {
                return Err(ControlError::Conflict("device lock facts changed after confirmation; obtain a new preview before retrying".to_owned()));
            }

            state.record.key.clone()
        };
        let exchange = tokio::select! {
            _ = self.cancellation.cancelled() => return Err(ControlError::Unavailable("device registry is stopping".to_owned())),
            exchange = Exchange::prepare(transport.local_address().map_err(anyhow::Error::from)?.ip(), transport.address().address, operation, key) => exchange.map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?,
        };
        self.invalidate_lock(device, generation)?;
        exchange
            .send()
            .await
            .context("send lock authentication once")?;

        if let Some(audit) = audit {
            audit
                .acknowledgement(&self.journal, &[], 1)
                .await
                .context("record lock transmission; do not repeat authentication")?;
        }

        let receipt = tokio::select! {
            _ = self.cancellation.cancelled() => Err(anyhow::anyhow!("device registry stopped after lock authentication was sent")),
            result = exchange.result() => result,
        };
        let mut errors = Vec::new();
        let mut result = match receipt {
            Ok(receipt) => {
                let receipt = serde_json::to_value(receipt).map_err(anyhow::Error::from)?;
                let mut result = receipt.clone();
                result["operation_result"] = receipt;
                result
            }
            Err(error) => {
                errors.push(error.to_string());
                json!({"success": false, "already": false, "lock_state": null, "status": null, "operation_result": null, "error": error.to_string()})
            }
        };
        result["change_sent"] = json!(true);
        result["requested_is_locked"] = json!(operation.locking);
        result["device"] = json!(device_key);
        result["state_verified"] = json!(false);
        let observed = tokio::select! {
            _ = self.cancellation.cancelled() => Err(anyhow::anyhow!("device registry is stopping")),
            fields = self.status_requests.query_with_deadline(transport, &device_key, StatusQuery::Lock, settings.observation_window, |fields| fields.get("is_locked") == Some(&json!(operation.locking))) => fields,
        };

        match observed {
            Ok(fields) => {
                if let Err(error) = self.accept_status(device, generation, fields.clone()) {
                    errors.push(error.to_string());
                    result["error"] = json!(error.to_string());
                    result["observation_unavailable"] = json!(true);
                    result["is_locked"] = Value::Null;
                } else {
                    result["is_locked"] = fields.get("is_locked").cloned().unwrap_or(Value::Null);
                    result["state_verified"] = json!(result["is_locked"] == operation.locking);
                    result["lock_state_code"] =
                        fields["lock_reset_status"]["lock_state_code"].clone();
                    result["status_code"] = fields["lock_reset_status"]["status_code"].clone();
                    result["observed_at"] = fields["lock_status_observed_at"].clone();
                    result["observation_source"] = json!("observed_after_0x1008");

                    if result["state_verified"] != true {
                        result["error"] = json!("lock operation did not reach the requested state");
                    }
                }
            }
            Err(error) => {
                errors.push(error.to_string());
                result["error"] = json!("lock status readback was not reported");
                result["observation_unavailable"] = json!(true);
                result["is_locked"] = Value::Null;
            }
        }

        if result["success"] != true && result.get("error").is_none() {
            result["error"] = json!("device rejected lock authentication");
        }

        result["success"] = json!(
            result["operation_result"]["success"] == true && result["state_verified"] == true
        );

        Ok(ControlCompletion {
            acknowledged_commands: usize::from(result["operation_result"]["success"] == true),
            readback_completed: result["is_locked"].is_boolean(),
            readback_errors: errors,
            observation: Some(result),
        })
    }

    fn invalidate_lock(&self, device: &Device, generation: u64) -> Result<(), ControlError> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation || !state.record.online {
            return Err(ControlError::Conflict(
                "device changed during lock challenge; no authentication was sent".to_owned(),
            ));
        }

        for field in ["is_locked", "lock_reset_status", "lock_status_observed_at"] {
            state
                .record
                .properties
                .insert(field.to_owned(), Value::Null);
            state
                .field_observed_at
                .insert(field.to_owned(), tokio::time::Instant::now());
        }

        self.publish(&state.record)?;
        Ok(())
    }
}
