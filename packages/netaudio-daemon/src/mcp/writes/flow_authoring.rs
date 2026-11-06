use super::{
    NEXT_STEP, Prepared, PreparedDevice, PreparedKind, channel_number, require_record,
    required_text,
};
use crate::devices::flow_authoring::Operation;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::registry::DeviceRegistry;
use crate::mcp::service::CallError;
use crate::mcp::views::{self, Records};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
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
    let selector = required_text(request, "device")?;
    let operation = if name == "delete_transmit_flow" {
        Operation::delete(&request["flow_id"])
    } else {
        let records = model.records()?;
        let record = require_record(&records, &request["device"])?;
        Operation::create(
            &specification(&request["specification"], record, registry).await?,
            false,
        )
    }
    .map_err(|error| CallError::invalid(error.to_string()))?;
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

pub(in crate::mcp) async fn specification(
    requested: &Value,
    record: &Value,
    registry: &Arc<DeviceRegistry>,
) -> Result<Value, CallError> {
    let mut specification = requested
        .as_object()
        .cloned()
        .ok_or_else(|| CallError::invalid("specification must be an object"))?;
    specification
        .entry("flow_type")
        .or_insert_with(|| json!("multicast"));
    specification
        .entry("media_mode")
        .or_insert_with(|| json!("native_dante"));
    if let Some(channels) = specification.remove("channels") {
        if specification.contains_key("channel_slots") {
            return Err(CallError::invalid(
                "give either channels or channel_slots, not both",
            ));
        }
        let channels = channels
            .as_array()
            .filter(|channels| !channels.is_empty())
            .ok_or_else(|| {
                CallError::invalid(
                    "channels must be a non-empty list of transmit channel numbers or labels",
                )
            })?;
        specification.insert(
            "channel_slots".into(),
            json!(channels.iter().enumerate().map(|(index, channel)| json!({"slot": index + 1, "transmitter_channel": channel})).collect::<Vec<_>>()),
        );
    }
    let slots = specification
        .get_mut("channel_slots")
        .and_then(Value::as_array_mut)
        .filter(|slots| !slots.is_empty())
        .ok_or_else(|| {
            CallError::invalid(
                "the specification needs channels, a list of transmit channel numbers or labels",
            )
        })?;
    for (index, slot) in slots.iter_mut().enumerate() {
        let Some(slot) = slot.as_object_mut() else {
            continue;
        };
        slot.entry("slot").or_insert_with(|| json!(index + 1));
        let channel = slot
            .get("transmitter_channel")
            .cloned()
            .unwrap_or(Value::Null);
        slot.insert(
            "transmitter_channel".into(),
            json!(channel_number(record, &channel, "transmitters")?),
        );
    }
    let identity = specification
        .entry("identity")
        .or_insert_with(|| json!({}));
    let authoring = &record["transmit_flow_authoring"];
    if !identity.is_object()
        || identity.get("global_flow_id").is_some()
        || authoring["identity_field"] != "global_flow_id"
    {
        return Ok(Value::Object(specification));
    }
    let reference = required_text(record, "server_name")?;
    let inventory = registry
        .apply(reference, ControlOperation::InspectTransmitFlows)
        .await
        .map_err(|error| CallError::invalid(error.to_string()))?
        .observation
        .ok_or_else(|| CallError::unavailable("transmitter flow readback"))?;
    if inventory["unparsed_records"]
        .as_array()
        .is_none_or(|records| !records.is_empty())
    {
        return Ok(Value::Object(specification));
    }
    let used: BTreeSet<_> = inventory["flows"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|flow| flow["identity"]["global_flow_id"].as_u64())
        .collect();
    if let Some(free) = authoring["identifier_max"]
        .as_u64()
        .and_then(|maximum| (1..=maximum).find(|identifier| !used.contains(identifier)))
    {
        identity["global_flow_id"] = json!(free);
    }
    Ok(Value::Object(specification))
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
