use super::{NEXT_STEP, Prepared, PreparedDevice, PreparedKind, require_record, required_text};
use crate::devices::flow_authoring::Operation;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::registry::DeviceRegistry;
use crate::mcp::service::CallError;
use crate::mcp::views::{self, Records};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) fn supports(operation: &str) -> bool {
    matches!(operation, "create_transmit_flow" | "delete_transmit_flow")
}

pub(super) async fn prepare(
    name: &str,
    request: &Value,
    registry: &Arc<DeviceRegistry>,
    model: &crate::state::ReadModel,
) -> Result<Prepared, CallError> {
    let operation = if name == "delete_transmit_flow" {
        Operation::delete(&request["flow_id"])
    } else {
        Operation::create(&request["specification"], false)
    }
    .map_err(|error| CallError::invalid(error.to_string()))?;
    let selector = required_text(request, "device")?;
    let planned = registry
        .apply(
            selector,
            ControlOperation::FlowAuthoring(operation.preview()),
        )
        .await
        .map_err(|error| CallError::invalid(error.to_string()))?;
    let observation = planned
        .observation
        .ok_or_else(|| CallError::unavailable("flow plan"))?;
    let records = model.records()?;
    let record = require_record(&records, &request["device"])?;
    let reference = required_text(record, "server_name")?.to_owned();
    let plan = &observation["plan"];
    let change = if operation.is_delete() {
        json!({"delete flow": request["flow_id"]})
    } else {
        json!({"new flow": views::flow_plan_view(plan, record)["flow"]})
    };
    let preview = views::compact(json!({
        "changed": false,
        "confirmation_required": true,
        "operation": name,
        "device": record["name"],
        "change": change,
        "supported": plan["supported"],
        "warnings": plan["reasons"],
        "next": NEXT_STEP,
    }));
    let operation = ControlOperation::FlowAuthoring(operation);
    let precondition = operation.precondition(record);
    Ok(Prepared {
        preview,
        devices: vec![PreparedDevice {
            reference: reference.clone(),
            operation,
            precondition,
        }],
        kind: PreparedKind::Flow { reference },
    })
}

pub(super) fn outcome(
    reference: &str,
    outcomes: &BTreeMap<String, Result<ControlCompletion, ControlError>>,
    records: &Records,
) -> Result<(Value, bool), CallError> {
    let completion = match outcomes.get(reference) {
        Some(Ok(completion)) => completion,
        Some(Err(error)) => return Ok((json!({"error": error.to_string()}), true)),
        None => return Err(CallError::unavailable("flow operation result")),
    };
    let observation = completion
        .observation
        .as_ref()
        .ok_or_else(|| CallError::unavailable("flow operation result"))?;
    let record = require_record(records, &json!(reference))?;
    let failed = !matches!(
        observation["state"].as_str(),
        Some("confirmed" | "deleted" | "partial" | "pending")
    );
    Ok((
        views::flow_result_view(observation, record, records),
        failed,
    ))
}
