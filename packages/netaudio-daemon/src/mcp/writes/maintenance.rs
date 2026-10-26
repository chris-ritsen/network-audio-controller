use super::{NEXT_STEP, Prepared, PreparedDevice, PreparedKind, require_record, required_text};
use crate::devices::maintenance::Operation;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::mcp::service::CallError;
use crate::mcp::views::Records;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(super) fn supports(operation: &str) -> bool {
    matches!(
        operation,
        "reboot" | "factory_reset" | "clear_configuration"
    )
}

pub(super) fn prepare(
    name: &str,
    request: &Value,
    records: &Records,
) -> Result<Prepared, CallError> {
    let operation =
        Operation::new(name, request).map_err(|error| CallError::invalid(error.to_string()))?;
    let record = require_record(records, &request["device"])?;
    let validation = if crate::managed::control::required(record) {
        crate::managed::control::validate(record, &ControlOperation::Maintenance(operation.clone()))
    } else {
        operation.validate(record)
    };
    validation.map_err(|error| CallError::invalid(error.to_string()))?;
    let reference = required_text(record, "server_name")?.to_owned();
    let preview = json!({
        "changed": false, "confirmation_required": true, "operation": name,
        "device": record["name"], "change": operation.requested(),
        "effect": operation.effect(), "next": NEXT_STEP,
    });
    let operation = ControlOperation::Maintenance(operation);
    let precondition = operation.precondition(record);
    Ok(Prepared {
        preview,
        devices: vec![PreparedDevice {
            reference: reference.clone(),
            operation,
            precondition,
        }],
        kind: PreparedKind::Maintenance { reference },
    })
}

pub(super) fn outcome(
    reference: &str,
    outcomes: &BTreeMap<String, Result<ControlCompletion, ControlError>>,
) -> Result<(Value, bool), CallError> {
    match outcomes.get(reference) {
        Some(Ok(completion)) => {
            let result = completion
                .observation
                .clone()
                .ok_or_else(|| CallError::unavailable("maintenance operation result"))?;
            let failed = result.get("error").is_some();
            Ok((result, failed))
        }
        Some(Err(ControlError::Managed { result, .. })) => Ok((result.clone(), true)),
        Some(Err(error)) => Ok((json!({"error": error.to_string(), "verified": false}), true)),
        None => Err(CallError::unavailable("maintenance operation result")),
    }
}
