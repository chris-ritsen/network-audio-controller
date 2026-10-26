use super::{NEXT_STEP, Prepared, PreparedDevice, PreparedKind, require_record, required_text};
use crate::devices::locking::Operation;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::mcp::service::CallError;
use crate::mcp::views::Records;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(super) fn supports(operation: &str) -> bool {
    matches!(operation, "lock_device" | "unlock_device")
}

pub(super) fn prepare(
    name: &str,
    request: &Value,
    records: &Records,
) -> Result<Prepared, CallError> {
    let locking = name == "lock_device";
    let operation =
        Operation::new(request, locking).map_err(|error| CallError::invalid(error.to_string()))?;
    let record = require_record(records, &request["device"])?;
    operation
        .validate(record)
        .map_err(|error| CallError::invalid(error.to_string()))?;
    let reference = required_text(record, "server_name")?.to_owned();
    let preview = json!({
        "changed": false, "confirmation_required": true, "operation": name, "device": record["name"],
        "change": {"locked": {"from": record["is_locked"], "to": locking}}, "next": NEXT_STEP,
    });
    let operation = ControlOperation::Locking(operation);
    let precondition = operation.precondition(record);
    Ok(Prepared {
        preview,
        devices: vec![PreparedDevice {
            reference: reference.clone(),
            operation,
            precondition,
        }],
        kind: PreparedKind::Locking { reference },
    })
}

pub(super) fn outcome(
    reference: &str,
    outcomes: &BTreeMap<String, Result<ControlCompletion, ControlError>>,
) -> Result<(Value, bool), CallError> {
    match outcomes.get(reference) {
        Some(Ok(completion)) => {
            let mut result = completion
                .observation
                .clone()
                .ok_or_else(|| CallError::unavailable("lock operation result"))?;
            result["verified"] = json!(result["state_verified"] == true);
            let failed = result["success"] != true;
            Ok((result, failed))
        }
        Some(Err(error)) => Ok((json!({"error": error.to_string(), "verified": false}), true)),
        None => Err(CallError::unavailable("lock operation result")),
    }
}
