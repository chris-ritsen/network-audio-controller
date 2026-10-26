use super::{CallError, NEXT_STEP, Prepared, PreparedKind, Records, required_text};
use crate::devices::registry::DeviceRegistry;
use crate::presets::{self, Service};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(super) struct PreparedSave {
    request: Value,
    reference: String,
    replace: bool,
    names: Vec<String>,
    conditions: BTreeMap<String, Value>,
}

impl PreparedSave {
    pub(super) fn conditions(&self) -> BTreeMap<String, Value> {
        self.conditions.clone()
    }

    pub(super) async fn apply(
        self,
        presets: &Service,
        registry: &Arc<DeviceRegistry>,
    ) -> Result<(Value, bool), CallError> {
        let captured = presets.capture(&self.request, registry).await?;
        let content = captured["xml"]
            .as_str()
            .ok_or_else(|| CallError::invalid("preset capture returned no XML"))?;
        let path = presets
            .write(&self.reference, content.into(), self.replace)
            .await?;
        Ok((
            json!({
                "preset": self.reference,
                "name": captured["name"],
                "path": path,
                "devices": self.names,
                "sections": self.request["sections"],
                "saved_at": captured["saved_at"],
                "omitted": captured["omitted"],
                "next": format!("preview_preset with preset={:?} lists what it holds; apply_preset with the same restores it.", self.reference),
            }),
            false,
        ))
    }
}

pub(super) async fn prepare(
    arguments: &Value,
    records: &Records,
    presets: &Service,
) -> Result<Prepared, CallError> {
    let name = required_text(arguments, "name")?;
    let reference = presets::storage::reference(name)?;
    let replace = match arguments.get("replace") {
        None | Some(Value::Bool(false)) => false,
        Some(Value::Bool(true)) => true,
        _ => return Err(CallError::invalid("replace must be a boolean")),
    };
    let exists = presets.exists(&reference).await?;
    if exists && !replace {
        return Err(CallError::invalid(format!(
            "a preset named {reference:?} is already saved; choose another name or set replace=true"
        )));
    }
    let requested = arguments["devices"]
        .as_array()
        .ok_or_else(|| CallError::invalid("devices must be named"))?;
    let mut identifiers = Vec::new();
    let mut names = Vec::new();
    let mut seen = BTreeSet::new();
    let mut conditions = BTreeMap::new();
    for selector in requested {
        let selector = selector
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| CallError::invalid("devices must be named"))?;
        let mut matches = records.iter().filter(|(key, record)| {
            std::iter::once(key.as_str())
                .chain(
                    ["name", "server_name", "inventory_id"]
                        .into_iter()
                        .filter_map(|field| record[field].as_str()),
                )
                .any(|value| caseless::default_caseless_match_str(value, selector))
        });
        let (identifier, record) = matches.next().ok_or_else(|| {
            CallError::invalid(format!("no device named {selector:?} is on the network"))
        })?;
        if matches.next().is_some() {
            return Err(CallError::invalid(format!(
                "device {selector:?} is ambiguous; use its inventory ID"
            )));
        }
        if !seen.insert(identifier.clone()) {
            return Err(CallError::invalid("A device may only be selected once."));
        }
        if record["online"] != true {
            return Err(CallError::invalid(format!(
                "{selector}: device is unavailable or offline"
            )));
        }
        identifiers.push(identifier.clone());
        names.push(record["name"].as_str().unwrap_or(identifier).to_owned());
        conditions.insert(identifier.clone(), json!({"name": record["name"], "server_name": record["server_name"], "inventory_id": record["inventory_id"], "mac_address": record["mac_address"], "online": record["online"]}));
    }
    conditions.insert(
        "saved_preset".into(),
        json!({"reference": reference, "exists": exists}),
    );
    let sections = arguments
        .get("sections")
        .filter(|value| value.as_array().is_some_and(|values| !values.is_empty()))
        .cloned()
        .unwrap_or_else(|| json!(["audio", "routing"]));
    let request = json!({"name": name, "devices": identifiers, "sections": sections});
    presets::capture::CaptureRequest::parse(&request)?;
    Ok(Prepared {
        preview: json!({
            "changed": false,
            "confirmation_required": true,
            "operation": "save_preset",
            "change": {"preset": reference, "devices": names, "sections": sections},
            "effect": format!("netaudio reads the selected settings of {} devices and saves them as preset {reference}{}; nothing on the devices changes", names.len(), if exists { ", replacing the saved preset with that name" } else { "" }),
            "next": NEXT_STEP,
        }),
        devices: Vec::new(),
        kind: PreparedKind::SavePreset(PreparedSave {
            request,
            reference,
            replace,
            names,
            conditions,
        }),
    })
}
