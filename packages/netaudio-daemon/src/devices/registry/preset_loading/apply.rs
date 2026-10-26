use super::{DeviceRegistry, PreparedAction, PreparedDevice, Selected};
use crate::devices::audit::{Audit, outcome};
use crate::devices::operations::{ControlError, ControlOperation, SubscriptionChange};
use crate::presets::{
    Error,
    audit::PresetAudit,
    report::{OperationResult, Report},
};
use anyhow::{Context, Result, ensure};
use netaudio_core::presets::planning::{ActionKind, ActionState};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::Instant;

impl DeviceRegistry {
    pub(super) async fn apply_preset(
        &self,
        name: &str,
        digest: &str,
        selected: &[Selected],
        prepared: Vec<PreparedDevice>,
        store: bool,
    ) -> Result<Value, Error> {
        for (target, plan) in selected.iter().zip(&prepared) {
            if target.checkpoint()? != plan.generation {
                return Err(Error::Conflict(
                    "No changes were sent: device inventory changed before preset execution."
                        .into(),
                ));
            }
        }
        let audit = PresetAudit::begin(
            &self.journal,
            name,
            digest,
            &selected
                .iter()
                .map(|target| target.target.identifier.clone())
                .collect::<Vec<_>>(),
            store,
        )
        .await?;
        let run_id = audit.identifier().to_owned();
        let mut ordered = prepared
            .iter()
            .enumerate()
            .flat_map(|(device, plan)| {
                plan.actions
                    .iter()
                    .enumerate()
                    .map(move |(index, action)| (action.presentation.kind, device, index))
            })
            .collect::<Vec<_>>();
        ordered.sort_unstable();
        let mut report = Report::default();
        let mut interrupted = false;
        let mut changed = BTreeSet::new();
        let mut result_indices = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(180);
        for (_, device, index) in ordered {
            result_indices.push((device, index, report.operations.len()));
            let target = &selected[device];
            if self.cancellation.is_cancelled() || Instant::now() >= deadline {
                interrupted = true;
            }
            let planned = &prepared[device].actions[index];
            let refreshed = if !interrupted
                && !changed.is_empty()
                && planned.presentation.kind != ActionKind::Preserved
                && planned
                    .presentation
                    .details
                    .get("conflicting_properties")
                    .is_none()
            {
                match tokio::time::timeout_at(deadline, self.replan_preset_action(target, planned))
                    .await
                {
                    Ok(Ok(action)) => Some(action),
                    outcome => {
                        let mut presentation = planned.presentation.clone();
                        presentation.state = ActionState::Unavailable;
                        presentation.reason = match outcome {
                            Ok(Err(error)) => {
                                format!("Fresh dependent-action planning failed: {error:#}")
                            }
                            Err(_) => {
                                interrupted = true;
                                "Preset apply deadline expired during read-only replanning.".into()
                            }
                            Ok(Ok(_)) => unreachable!(),
                        };
                        Some(PreparedAction {
                            presentation,
                            operations: Vec::new(),
                        })
                    }
                }
            } else {
                None
            };
            let action = refreshed.as_ref().unwrap_or(planned);
            let presented = &action.presentation;
            let kind = serde_json::to_value(presented.kind)
                .map_err(anyhow::Error::from)?
                .as_str()
                .context("preset action name unavailable")?
                .to_owned();
            if self.cancellation.is_cancelled() || Instant::now() >= deadline {
                interrupted = true;
            }
            if interrupted {
                report.record(OperationResult::new(&target.target.name, kind, "skipped", presented.requested.clone(), "Not sent because the preset stopped after a failure, unverified change, cancellation, or deadline."));
                continue;
            }
            if presented.state != ActionState::Change {
                let state = serde_json::to_value(presented.state).map_err(anyhow::Error::from)?;
                let mut result = OperationResult::new(
                    &target.target.name,
                    kind,
                    state.as_str().context("preset action state unavailable")?,
                    presented.requested.clone(),
                    &presented.reason,
                );
                result.effective = presented.current.clone();
                result.effective_state_confirmation =
                    (presented.state == ActionState::Unchanged).then_some(true);
                if presented.state == ActionState::Unchanged
                    && matches!(
                        presented.kind,
                        ActionKind::Interface | ActionKind::RedundancyMode
                    )
                {
                    result.configuration_confirmation =
                        presented.details["configuration_confirmed"].as_bool();
                    result.effective_state_confirmation =
                        presented.details["effective_state_confirmed"].as_bool();
                    result.verification_observations.push(
                        json!({"phase": "initial_readback", "observation": presented.details}),
                    );
                    if result.effective_state_confirmation != Some(true) {
                        result.state = "configuration_confirmed".into();
                        result.message = "Configured settings match, but effective device state is not confirmed; no command was sent.".into();
                    }
                    if presented.details["reboot_required"] == true
                        && !report.needs_reboot.contains(&target.target.name)
                    {
                        report.needs_reboot.push(target.target.name.clone());
                    }
                }
                report.record(result);
                continue;
            }
            let result = self
                .apply_preset_action(target, action, &kind, &run_id)
                .await;
            let result = match result {
                Ok(result) => result,
                Err(error) => OperationResult::new(
                    &target.target.name,
                    kind,
                    "failed",
                    presented.requested.clone(),
                    format!(
                        "Preset action failed; any preceding commands must not be replayed: {error:#}"
                    ),
                ),
            };
            changed.insert(device);
            interrupted = result.failed() || result.unverified();
            if result.verification_observations.iter().any(|value| {
                value.pointer("/completion/observation/reboot_required") == Some(&json!(true))
            }) && !report.needs_reboot.contains(&target.target.name)
            {
                report.needs_reboot.push(target.target.name.clone());
            }
            report.record(result);
        }
        if !changed.is_empty() {
            self.verify_preset_result(selected, &prepared, &result_indices, &mut report, deadline)
                .await;
            interrupted |= report.failures != 0;
        }
        if store {
            for device in changed {
                let target = &selected[device];
                let eligible = !interrupted
                    && !self.cancellation.is_cancelled()
                    && Instant::now() < deadline
                    && report
                        .operations
                        .iter()
                        .filter(|operation| operation.device_name == target.target.name)
                        .all(|operation| !operation.failed() && !operation.unverified());
                if !eligible {
                    report.record(OperationResult::new(&target.target.name, "store_current_configuration".into(), "skipped", json!({}), "Configuration storage was not sent because the requested preset was not completely verified."));
                    continue;
                }
                let operation = ControlOperation::Performance(
                    crate::devices::performance::PerformanceOperation::StoreCurrentConfiguration,
                );
                let result = self.preset_control(target, &operation, &run_id).await;
                let result = result.unwrap_or_else(|error| {
                    OperationResult::new(
                        &target.target.name,
                        "store_current_configuration".into(),
                        "failed",
                        json!({}),
                        format!("Configuration storage failed: {error:#}"),
                    )
                });
                interrupted |= result.failed() || result.unverified();
                report.record(result);
            }
        }
        if let Err(error) = audit.finish(&self.journal, &report, interrupted).await {
            interrupted = true;
            report.record(OperationResult::new(
                name,
                "apply_preset".into(),
                "unverified",
                json!({"digest": digest}),
                format!("Final preset journal write failed; do not replay the preset: {error:#}"),
            ));
        }
        let complete = !interrupted && report.failures == 0 && report.unverified == 0;
        Ok(
            json!({"name": name, "digest": digest, "run_id": run_id, "complete": complete, "interrupted": interrupted, "report": report}),
        )
    }

    async fn apply_preset_action(
        &self,
        target: &Selected,
        action: &PreparedAction,
        kind: &str,
        run_id: &str,
    ) -> Result<OperationResult> {
        ensure!(
            !action.operations.is_empty(),
            "preset action has no prepared operation"
        );
        let mut result = OperationResult::new(
            &target.target.name,
            kind.into(),
            "confirmed",
            action.presentation.requested.clone(),
            "Requested state was confirmed by fresh readback; persistence is unknown.",
        );
        result.effective_state_confirmation = Some(true);
        let mut effective = Vec::new();
        let mut acknowledgements = Vec::new();
        for operation in &action.operations {
            let mut operation = operation.clone();
            let record = target.presentation()?;
            if let ControlOperation::Subscriptions { changes } = &mut operation {
                for change in changes {
                    if let SubscriptionChange::Set {
                        transmitter_device, ..
                    } = change
                        && transmitter_device == "."
                    {
                        *transmitter_device = record["name"]
                            .as_str()
                            .context("device name unavailable")?
                            .to_owned();
                    }
                }
            }
            let child = self.preset_control(target, &operation, run_id).await?;
            let incomplete = child.failed() || child.unverified();
            result.configuration_confirmation = child.configuration_confirmation;
            effective.push(child.effective.clone());
            acknowledgements.push(child.request_acknowledgement.clone());
            result
                .verification_observations
                .extend(child.verification_observations);
            if incomplete {
                result.state = child.state;
                result.message = child.message;
                result.effective_state_confirmation = child.effective_state_confirmation;
                result.configuration_confirmation = child.configuration_confirmation;
                break;
            }
        }
        result.effective = if effective.len() == 1 {
            effective.pop().unwrap()
        } else {
            json!(effective)
        };
        result.request_acknowledgement = if acknowledgements.len() == 1 {
            acknowledgements.pop().unwrap()
        } else {
            json!(acknowledgements)
        };
        Ok(result)
    }

    async fn preset_control(
        &self,
        target: &Selected,
        operation: &ControlOperation,
        run_id: &str,
    ) -> Result<OperationResult> {
        let snapshot = target.presentation()?;
        let mut audit =
            Audit::begin_child(&self.journal, snapshot, operation, Some(run_id)).await?;
        let operation_id = audit.as_ref().map(|audit| audit.identifier().to_owned());
        let completion = if let Some(device) = target.managed() {
            device.control(operation).await
        } else {
            self.execute_recorded_operation(target.direct()?, operation, None, audit.as_mut())
                .await
        };
        let snapshot = target.presentation()?;
        let kind = if matches!(
            operation,
            ControlOperation::Performance(
                crate::devices::performance::PerformanceOperation::StoreCurrentConfiguration
            )
        ) {
            "store_current_configuration"
        } else {
            "device_control"
        };
        let mut result = OperationResult::new(
            &target.target.name,
            kind.into(),
            "unverified",
            json!({}),
            "Fresh readback did not confirm the requested state.",
        );
        match &completion {
            Ok(completion) => {
                let observed = if target.managed().is_some() {
                    outcome::Observation {
                        confirmed: completion
                            .observation
                            .as_ref()
                            .and_then(|value| value["state_verified"].as_bool()),
                        effective: completion.observation.clone(),
                    }
                } else {
                    outcome::observe(operation, &snapshot, completion)
                };
                let detail = completion.observation.as_ref().unwrap_or(&Value::Null);
                result.state = match observed.confirmed {
                    Some(true) => "confirmed",
                    Some(false) => "contradicted",
                    None => "unverified",
                }
                .into();
                result.effective_state_confirmation = observed.confirmed;
                if matches!(operation, ControlOperation::NetworkConfiguration(_)) {
                    result.configuration_confirmation = detail["configuration_confirmed"].as_bool();
                    result.effective_state_confirmation =
                        detail["effective_state_confirmed"].as_bool();
                    result.state = if result.configuration_confirmation == Some(true) {
                        if result.effective_state_confirmation == Some(true) {
                            "confirmed"
                        } else {
                            "configuration_confirmed"
                        }
                    } else {
                        "unverified"
                    }
                    .into();
                }
                result.effective = observed.effective.unwrap_or(Value::Null);
                result.request_acknowledgement = detail
                    .get("request_acknowledgement")
                    .or_else(|| detail.get("request_acknowledged"))
                    .or_else(|| detail.get("accepted"))
                    .cloned()
                    .unwrap_or(Value::Null);
                result.persistence_request_acknowledgement =
                    detail["persistence_request_acknowledgement"].clone();
                result.persistence_confirmation = detail["persistence_confirmation"].as_bool();
                if let Some(state) = detail["state"].as_str().filter(|state| {
                    matches!(
                        *state,
                        "rejected" | "failed" | "unsupported" | "confirmation_required"
                    )
                }) {
                    result.state = state.into();
                }
                if matches!(operation, ControlOperation::Performance(crate::devices::performance::PerformanceOperation::StoreCurrentConfiguration)) {
                    result.state = detail["state"].as_str().unwrap_or("unverified").into();
                }
                result.message = detail["message"].as_str().map(str::to_owned).unwrap_or_else(|| if result.effective_state_confirmation == Some(true) { "Fresh readback confirmed the requested state.".into() } else { "The request completed without confirmed effective state; it must not be replayed automatically.".into() });
                result
                    .verification_observations
                    .push(json!({"operation_id": operation_id, "completion": completion}));
            }
            Err(error) => {
                result.state = if matches!(error, ControlError::Rejected { .. }) {
                    "rejected"
                } else {
                    "failed"
                }
                .into();
                result.message = error.to_string();
                result.verification_observations.push(
                    json!({"operation_id": operation_id, "failure": outcome::failure(error), "managed_receipt": crate::managed::control::outcome(&completion)}),
                );
            }
        }
        if let Some(audit) = audit
            && let Err(error) = audit
                .finish(&self.journal, operation, snapshot, &completion)
                .await
        {
            result.state = "unverified".into();
            result.message =
                format!("Final command journal write failed; do not replay the command: {error:#}");
            result.effective_state_confirmation = None;
            result
                .verification_observations
                .push(json!({"journal_error": format!("{error:#}")}));
        }
        Ok(result)
    }
}
