use super::{NEXT_STEP, Prepared, PreparedDevice, PreparedKind, require_record, required_text};
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::panels::Operation;
use crate::devices::registry::DeviceRegistry;
use crate::mcp::service::CallError;
use crate::mcp::views;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) async fn prepare(
    request: &Value,
    registry: &Arc<DeviceRegistry>,
    model: &crate::state::ReadModel,
) -> Result<Prepared, CallError> {
    let change = crate::mcp::panels::change(request, registry).await?;
    let completion = registry
        .apply(
            required_text(request, "device")?,
            ControlOperation::Panel(Operation::Plan(change.clone())),
        )
        .await
        .map_err(|error| CallError::invalid(error.to_string()))?;
    let observed = completion
        .observation
        .ok_or_else(|| CallError::unavailable("device control plan"))?;
    let plan = &observed["plan"];
    if matches!(plan["action"].as_str(), Some("unsupported" | "unavailable")) {
        return Err(CallError::invalid(
            plan["reason"]
                .as_str()
                .unwrap_or("device control is unavailable"),
        ));
    }
    let view = views::panels::plan(plan);
    let records = model.records()?;
    let record = require_record(&records, &request["device"])?;
    let reference = required_text(record, "server_name")?.to_owned();
    let operation = ControlOperation::Panel(Operation::Apply(change));
    Ok(Prepared {
        preview: views::compact(json!({
            "changed": false, "confirmation_required": plan["action"] == "change", "operation": "apply_device_control", "device": record["name"],
            "change": (plan["action"] == "change").then(|| json!({"from": view["from"], "to": view["to"]})),
            "result": view["result"], "reason": view["reason"], "next": (plan["action"] == "change").then_some(NEXT_STEP),
        })),
        devices: vec![PreparedDevice {
            reference: reference.clone(),
            precondition: observed["precondition"].clone(),
            operation,
        }],
        kind: PreparedKind::Panel { reference },
    })
}

pub(super) fn outcome(
    reference: &str,
    outcomes: &BTreeMap<String, Result<ControlCompletion, ControlError>>,
) -> Result<(Value, bool), CallError> {
    let completion = match outcomes.get(reference) {
        Some(Ok(completion)) => completion,
        Some(Err(error)) => return Ok((json!({"error": error.to_string()}), true)),
        None => return Err(CallError::unavailable("device control result")),
    };
    let observation = completion
        .observation
        .as_ref()
        .ok_or_else(|| CallError::unavailable("device control result"))?;
    Ok((
        views::panels::applied(observation),
        observation["effective_state_confirmed"] != true,
    ))
}
