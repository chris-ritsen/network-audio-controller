use super::{
    CallError, ControlError, NEXT_STEP, Prepared, PreparedDevice, PreparedKind, Records,
    require_record, required_text,
};
use crate::devices::external_subscription::ExternalSubscription;
use crate::devices::operations::{ControlCompletion, ControlOperation};
use crate::state::ReadModel;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(super) fn prepare(
    request: &Value,
    records: &Records,
    model: &ReadModel,
) -> Result<Prepared, CallError> {
    let record = require_record(records, &request["rx_device"])?;
    let reference = required_text(record, "server_name")?.to_owned();
    let subscription = ExternalSubscription::prepare(request, model)
        .map_err(|error| CallError::invalid(error.to_string()))?;
    let selection = subscription.requested();
    let operation = ControlOperation::ExternalSubscription(subscription);
    let precondition = operation.precondition(record);
    Ok(Prepared {
        preview: json!({
            "changed": false,
            "confirmation_required": true,
            "operation": "subscribe_external_flow",
            "device": record["name"],
            "requested": selection,
            "before": precondition["external_subscription"]["channels"],
            "effect": "Replace the selected receiver channel mappings with slots from the selected AES67 stream.",
            "next": NEXT_STEP,
        }),
        devices: vec![PreparedDevice {
            reference: reference.clone(),
            operation,
            precondition,
        }],
        kind: PreparedKind::ExternalSubscription { reference },
    })
}

pub(super) fn outcome(
    reference: &str,
    outcomes: &BTreeMap<String, Result<ControlCompletion, ControlError>>,
) -> Result<(Value, bool), CallError> {
    match outcomes.get(reference) {
        Some(Ok(completion)) => {
            let observation = completion.observation.clone().ok_or_else(|| {
                CallError::invalid("external subscription completion is unavailable")
            })?;
            let (status, result) = crate::devices::external_subscription::response(observation);
            Ok((result, !status.is_success()))
        }
        Some(Err(error)) => Ok((json!({"error": error.to_string(), "changed": false}), true)),
        None => Err(CallError::invalid(
            "external subscription execution is unavailable",
        )),
    }
}
