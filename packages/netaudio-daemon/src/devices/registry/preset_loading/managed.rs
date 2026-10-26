use super::{DeviceRegistry, Selected};
use crate::{
    devices::{operations::ControlOperation, registry::Inspection},
    monitoring::requests::StatusQuery,
    presets::diff,
};
use anyhow::{Context, Result, ensure};
use netaudio_core::presets::planning::{Action, ActionKind, ActionState, RequestedAction};
use serde_json::{Value, json};

impl DeviceRegistry {
    pub(super) async fn plan_managed_preset_action(
        &self,
        target: &Selected,
        inventory: &Value,
        request: &RequestedAction,
        operations: &[ControlOperation],
    ) -> Result<Action> {
        let device = target
            .managed()
            .context("managed preset target unavailable")?;
        if matches!(
            request.kind,
            ActionKind::Preserved
                | ActionKind::DeviceControl
                | ActionKind::TransmitFlow
                | ActionKind::ExternalReceiverSubscriptions
                | ActionKind::ReceiveFlowPerformance
                | ActionKind::TransmitFlowPerformance
                | ActionKind::UnicastPerformance
                | ActionKind::ReceiveFlowDefaultSlots
        ) {
            return Ok(request.result(
                ActionState::Unsupported,
                Value::Null,
                "The managed transport has no established writer for this preset field.",
            ));
        }
        if let Some(ControlOperation::SampleRate(operation)) = operations.first() {
            let mut preview = operation.clone();
            preview.preview = true;
            let observed = device
                .control(&ControlOperation::SampleRate(preview))
                .await?
                .observation
                .context("managed sample-rate preview unavailable")?;
            let current = observed["preflight"]["current_sample_rate_hertz"].clone();
            let mut action = request.compare(current.clone(), current == operation.target);
            if action.state == ActionState::Change
                && let Some((state, reason)) = operation.refusal(&observed["preflight"])
            {
                action.state = if state == "unsupported" {
                    ActionState::Unsupported
                } else {
                    ActionState::Unavailable
                };
                action.reason = reason.into();
            }
            action.details = observed;
            return Ok(action);
        }
        if let Some(ControlOperation::Clock(configuration)) = operations.first() {
            if configuration.changes.len() != 1
                || !configuration.changes.contains_key("preferred_leader")
            {
                return Ok(request.result(
                    ActionState::Unsupported,
                    Value::Null,
                    "Managed clock presets support the preferred-leader setting only.",
                ));
            }
            let current = inventory["preferred_leader"].clone();
            ensure!(
                current.is_boolean(),
                "managed preferred-leader state is unavailable"
            );
            let mut action = request.compare(
                json!({"preferred_leader": current}),
                current == configuration.changes["preferred_leader"],
            );
            if action.state == ActionState::Change
                && let Err(error) = crate::managed::control::validate(inventory, &operations[0])
            {
                action.state = ActionState::Unavailable;
                action.reason = error.to_string();
            }
            return Ok(action);
        }
        let inspection = match request.kind {
            ActionKind::Encoding => Some(Inspection::Status(StatusQuery::Encoding)),
            ActionKind::SampleRatePullup => Some(Inspection::Status(StatusQuery::Pullup)),
            ActionKind::CodecGain => Some(Inspection::Status(StatusQuery::Codec)),
            ActionKind::Interface | ActionKind::RedundancyMode => {
                Some(Inspection::NetworkConfiguration)
            }
            ActionKind::Latency => Some(Inspection::Latency),
            _ => None,
        };
        if let Some(inspection) = inspection {
            device
                .control(&ControlOperation::InspectStatus(inspection))
                .await?;
        }
        let mut record = device.record()?;
        ensure!(
            crate::managed::presets::routing_condition(&record)
                == crate::managed::presets::routing_condition(inventory),
            "managed routing changed while planning the preset"
        );
        record["channels"] = inventory["channels"].clone();
        record["subscriptions"] = inventory["subscriptions"].clone();
        record["name"] = inventory["name"].clone();
        diff::compare_presented(request, operations, &record, |operation| {
            crate::managed::control::validate(&record, operation)
        })
    }
}
