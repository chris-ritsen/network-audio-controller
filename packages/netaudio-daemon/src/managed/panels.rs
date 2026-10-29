use super::{
    control::DeviceRequest,
    registry::{Registry, Source},
};
use crate::{
    devices::{
        analog,
        operations::{ControlCompletion, ControlError, ControlOperation},
        panels::{Change, Operation},
        registry::Inspection,
        status_configuration,
    },
    monitoring::requests::StatusQuery,
};
use serde_json::{json, Value};
use std::sync::Arc;

pub fn configuration(operation: &Operation) -> Result<Option<analog::Configuration>, ControlError> {
    match operation.change() {
        None => Ok(None),
        Some(change) => change_configuration(change).map(Some),
    }
}

pub fn status_operation(configuration: analog::Configuration) -> ControlOperation {
    ControlOperation::StatusConfiguration(status_configuration::Configuration::Analog(
        configuration,
    ))
}

fn change_configuration(change: &Change) -> Result<analog::Configuration, ControlError> {
    if change.category != "analog_level" {
        return Err(ControlError::Unavailable(
            "Only analog level device controls are integrated with native managed control".into(),
        ));
    }
    analog::Configuration::new(
        &change.requested["channel"],
        &change.requested["level"],
        &change.requested["direction"],
    )
}

impl Registry {
    pub(super) async fn controller_panel(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
        operation: &Operation,
    ) -> Result<ControlCompletion, ControlError> {
        let configuration = configuration(operation)?;
        let inspected = self
            .controller_inspect(
                request,
                source,
                revision,
                Inspection::Status(StatusQuery::Codec),
            )
            .await?;
        let record = inspected.observation.ok_or_else(|| {
            ControlError::ObservationUnavailable("Managed analog status is unavailable".into())
        })?;
        let errors = inspected.readback_errors;
        let Some(configuration) = configuration else {
            let observation = json!({
                "analog": {
                    "status": record["codec_status"], "levels": record["gain_levels"],
                    "direction": record["gain_device_type"], "choices": record["gain_level_choices"],
                    "observed_at_unix": record["codec_observed_at"],
                    "read_unavailable_reason": analog::access(&record, false)?,
                    "write_unavailable_reason": analog::access(&record, true)?,
                },
                "transport": "ddm", "control_transport": "controller",
            });
            return Ok(completion(observation, errors));
        };
        let reason = analog::access(&record, false)?;
        let mut plan = json!({"category": "analog_level", "requested": configuration.requested(), "action": "unavailable", "reason": reason});
        if reason.is_none() {
            let planned = serde_json::to_value(configuration.plan(&record["gain_adapter"])?)
                .map_err(anyhow::Error::from)?;
            if let (Some(plan), Some(planned)) = (plan.as_object_mut(), planned.as_object()) {
                plan.extend(planned.clone());
            }
            plan["status"] = record["gain_adapter"].clone();
            if plan["action"] == "change"
                && let Some(reason) = analog::access(&record, true)?
            {
                plan["action"] = json!("unavailable");
                plan["reason"] = json!(reason);
            }
        }
        let status = status_operation(configuration);
        if matches!(operation, Operation::Plan(_)) {
            let mut condition = status.precondition(&record);
            condition["managed_configuration"] = json!(revision);
            return Ok(completion(
                json!({"plan": plan, "precondition": condition}),
                errors,
            ));
        }
        let mut result = json!({
            "plan": plan, "reason": plan["reason"], "request_sent": false, "request_acknowledged": null,
            "transport_reply": false, "correlated_status": false, "effective_state_confirmed": plan["action"] == "unchanged",
            "persistence": "unknown", "media_readiness": "unknown", "status": plan["status"],
            "transport": "ddm", "control_transport": "controller",
        });
        if plan["action"] != "change" {
            return Ok(completion(result, errors));
        }
        let configure = DeviceRequest {
            record: request.record.clone(),
            operation: status,
            precondition: request.precondition.clone(),
            configuration_revision: request.configuration_revision.clone(),
        };
        let Some(configuration) =
            super::device_configuration::Configuration::from_operation(&configure.operation)
        else {
            return Err(ControlError::Unavailable(
                "Managed analog configuration is unavailable".into(),
            ));
        };
        let mut completed = self
            .controller_configure(&configure, source, revision, &configuration)
            .await?;
        if let Some(observation) = completed.observation.take()
            && let (Some(result), Some(observation)) =
                (result.as_object_mut(), observation.as_object())
        {
            result.extend(observation.clone());
        }
        completed.observation = Some(result);
        completed.readback_errors.extend(errors);
        Ok(completed)
    }
}

fn completion(observation: Value, errors: Vec<String>) -> ControlCompletion {
    ControlCompletion {
        acknowledged_commands: 0,
        readback_completed: errors.is_empty(),
        readback_errors: errors,
        observation: Some(observation),
    }
}
