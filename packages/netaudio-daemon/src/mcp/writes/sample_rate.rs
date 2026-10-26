use super::{NEXT_STEP, Prepared, PreparedDevice, PreparedKind, require_record, required_text};
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::registry::DeviceRegistry;
use crate::devices::sample_rate::Operation;
use crate::mcp::service::CallError;
use crate::mcp::views::Records;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) async fn prepare(
    request: &Value,
    registry: &Arc<DeviceRegistry>,
    model: &crate::state::ReadModel,
) -> Result<Prepared, CallError> {
    let operation =
        Operation::new(request).map_err(|error| CallError::invalid(error.to_string()))?;
    let mut preview_operation = operation.clone();
    preview_operation.preview = true;
    let planned = registry
        .apply(
            required_text(request, "device")?,
            ControlOperation::SampleRate(preview_operation),
        )
        .await
        .map_err(|error| CallError::invalid(error.to_string()))?;
    let observation = planned
        .observation
        .ok_or_else(|| CallError::unavailable("sample-rate preflight"))?;
    let records = model.records()?;
    let record = require_record(&records, &request["device"])?;
    let reference = required_text(record, "server_name")?.to_owned();
    let target = json!(operation.target);
    let mut warnings =
        vec!["Other devices on affected routes must use the same sample rate.".to_owned()];

    if let Some((_, reason)) = operation.refusal(&observation["preflight"]) {
        warnings.push(reason.to_owned());
    }

    let operation = ControlOperation::SampleRate(operation);
    let precondition = operation.precondition(record);
    Ok(Prepared {
        preview: json!({
            "changed": false,
            "confirmation_required": true,
            "operation": "set_sample_rate",
            "device": record["name"],
            "change": {"sample rate": {"from": record["sample_rate_hz"], "to": target}},
            "preflight": observation["preflight"],
            "warnings": warnings,
            "next": NEXT_STEP,
        }),
        devices: vec![PreparedDevice {
            reference: reference.clone(),
            operation,
            precondition,
        }],
        kind: PreparedKind::SampleRate { reference },
    })
}

pub(super) fn outcome(
    reference: &str,
    outcomes: &BTreeMap<String, Result<ControlCompletion, ControlError>>,
    records: &Records,
) -> Result<(Value, bool), CallError> {
    if let Some(result) = outcomes
        .get(reference)
        .and_then(crate::managed::control::outcome)
    {
        let mut result = result.clone();
        result["verified"] = json!(result["state_verified"] == true);
        let failed =
            outcomes.get(reference).is_some_and(Result::is_err) || result["success"] != true;
        return Ok((result, failed));
    }
    let completion = match outcomes.get(reference) {
        Some(Ok(completion)) => completion,
        Some(Err(error)) => return Ok((json!({"error": error.to_string()}), true)),
        None => return Err(CallError::unavailable("sample-rate operation result")),
    };
    let observation = completion
        .observation
        .as_ref()
        .ok_or_else(|| CallError::unavailable("sample-rate operation result"))?;
    let record = require_record(records, &json!(reference))?;
    let mut result = observation.clone();
    result["device"] = record["name"].clone();
    result["operation"] = json!("set_sample_rate");
    result["verified"] = json!(observation["state_verified"] == true);
    Ok((result, observation["success"] != true))
}
