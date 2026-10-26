use super::{CallError, NEXT_STEP, Prepared, PreparedKind, Records, require_record};
use crate::devices::registry::DeviceRegistry;
use crate::presets::{Service, loading::confirmation};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(super) struct PreparedLoad {
    request: Value,
    plan: Value,
    applied_to: Map<String, Value>,
    skipped: Vec<Value>,
}

impl PreparedLoad {
    pub fn conditions(&self) -> BTreeMap<String, Value> {
        BTreeMap::from_iter([("preset_plan".into(), self.plan.clone())])
    }

    pub async fn apply(
        self,
        presets: &Arc<Service>,
        registry: &Arc<DeviceRegistry>,
    ) -> Result<(Value, bool), CallError> {
        let mut result = presets
            .load_with_plan(&self.request, registry, Some(self.plan))
            .await?;
        let incomplete = result["complete"] != true;
        result["applied_to"] = self.applied_to.into();
        result["skipped"] = self.skipped.into();
        Ok((result, incomplete))
    }
}

pub(super) async fn prepare(
    arguments: &Value,
    records: &Records,
    presets: &Arc<Service>,
    registry: &Arc<DeviceRegistry>,
) -> Result<Prepared, CallError> {
    let (content, parsed) = match (arguments.get("preset"), arguments.get("xml")) {
        (Some(Value::String(reference)), None) if !reference.is_empty() => {
            let stored = presets.read(reference).await?;
            (stored.content, stored.parsed)
        }
        (None, Some(Value::String(content))) if !content.is_empty() => {
            (content.clone(), presets.parse(arguments).await?)
        }
        _ => {
            return Err(CallError::invalid(
                "Give exactly one saved preset name or non-empty XML string.",
            ));
        }
    };
    if arguments
        .get("digest")
        .is_some_and(|digest| digest.as_str() != Some(&parsed.digest))
    {
        return Err(CallError::invalid(
            "This preset XML does not match the digest; preview it again.",
        ));
    }
    let requested = match arguments.get("targets") {
        None => Map::new(),
        Some(Value::Object(targets)) => targets.clone(),
        _ => {
            return Err(CallError::invalid(
                "targets must map preset device names to network devices",
            ));
        }
    };
    let mut skipped_names = BTreeSet::new();
    if let Some(skip) = arguments.get("skip") {
        for entry in skip
            .as_array()
            .ok_or_else(|| CallError::invalid("skip must contain preset device names"))?
        {
            let name = entry
                .as_str()
                .ok_or_else(|| CallError::invalid("skip must contain preset device names"))?;
            if !skipped_names.insert(name.to_owned()) {
                return Err(CallError::invalid(
                    "Skipped preset device names must be distinct.",
                ));
            }
        }
    }
    for name in requested.keys().chain(skipped_names.iter()) {
        if !parsed.document.devices().contains_key(name) {
            return Err(CallError::invalid(format!(
                "The preset has no device named {name:?}."
            )));
        }
        if requested.contains_key(name) && skipped_names.contains(name) {
            return Err(CallError::invalid(format!(
                "{name} cannot be both targeted and skipped."
            )));
        }
    }
    let mut targets = Map::new();
    let mut applied_to = Map::new();
    let mut skipped = Vec::new();
    for (name, device) in parsed.document.devices() {
        if skipped_names.contains(name) {
            skipped.push(json!({"preset_device": name, "reason": "Skipped as requested."}));
            continue;
        }
        let candidates: Vec<_> = records
            .iter()
            .filter(|(identifier, record)| {
                crate::presets::matches_record(device.configuration(), identifier, record)
            })
            .collect();
        let selected = if let Some(selector) = requested.get(name) {
            let record = require_record(records, selector)?;
            let identifier = record["server_name"]
                .as_str()
                .ok_or_else(|| CallError::unavailable("device identity"))?;
            if !candidates
                .iter()
                .any(|(candidate, _)| candidate.as_str() == identifier)
            {
                return Err(CallError::invalid(format!(
                    "Selected device does not match the name or identity of preset device {name}."
                )));
            }
            Some((identifier.to_owned(), record["name"].clone()))
        } else if candidates.len() == 1 {
            Some((candidates[0].0.clone(), candidates[0].1["name"].clone()))
        } else {
            None
        };
        if let Some((identifier, device_name)) = selected {
            targets.insert(name.clone(), json!(identifier));
            applied_to.insert(name.clone(), device_name);
        } else {
            skipped_names.insert(name.clone());
            skipped.push(json!({"preset_device": name, "reason": if candidates.is_empty() { "No matching device is in the inventory." } else { "Multiple devices match; select an inventory ID in targets." }}));
        }
    }
    if targets.is_empty() {
        return Err(CallError::invalid(
            "No preset device has a selected matching target.",
        ));
    }
    let request = json!({
        "xml": content, "digest": parsed.digest, "confirmed": true, "targets": targets,
        "excluded": skipped_names,
        "confirm_destructive": arguments.get("confirm_destructive").cloned().unwrap_or(json!(false)),
        "store_current_configuration": arguments.get("store_current_configuration").cloned().unwrap_or(json!(false)),
    });
    let observed = presets.plan_load(&request, registry).await?;
    let plan = confirmation(&observed);
    Ok(Prepared {
        preview: json!({"changed": false, "confirmation_required": true, "operation": "apply_preset", "preset": observed["name"], "digest": observed["digest"], "apply": plan["devices"], "skipped": skipped, "next": NEXT_STEP}),
        devices: Vec::new(),
        kind: PreparedKind::ApplyPreset(PreparedLoad {
            request,
            plan,
            applied_to,
            skipped,
        }),
    })
}
