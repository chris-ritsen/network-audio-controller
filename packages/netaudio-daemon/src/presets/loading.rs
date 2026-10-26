use super::{Error, ParsedPreset, Service, matches_record};
use crate::devices::registry::DeviceRegistry;
use netaudio_core::presets::planning::{RequestedAction, requested_actions};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub struct Target {
    pub identifier: String,
    pub name: String,
    pub configuration: Map<String, Value>,
    pub actions: Vec<RequestedAction>,
}

pub struct LoadRequest {
    pub name: String,
    pub digest: String,
    pub targets: Vec<Target>,
    pub confirm_destructive: bool,
    pub store_current_configuration: bool,
    pub confirmed_plan: Option<Value>,
}

impl LoadRequest {
    pub fn new(
        parsed: ParsedPreset,
        parameters: &Value,
        records: &BTreeMap<String, Arc<Value>>,
        apply: bool,
    ) -> Result<Self, Error> {
        if apply && (parameters["confirmed"] != true || parameters["digest"] != parsed.digest) {
            return Err("Review and confirm this preset before applying it.".into());
        }
        let boolean = |name, message| -> Result<bool, Error> {
            parameters.get(name).map_or(Ok(false), |value| {
                value.as_bool().ok_or_else(|| Error::Invalid(message))
            })
        };
        let confirm_destructive = boolean(
            "confirm_destructive",
            "Destructive-change confirmation must be a boolean.".into(),
        )?;
        let store_current_configuration = boolean(
            "store_current_configuration",
            "Configuration-storage selection must be a boolean.".into(),
        )?;
        let selected = parameters["targets"]
            .as_object()
            .filter(|value| !value.is_empty())
            .ok_or("Select named preset devices to load.")?;
        let excluded = parameters["excluded"]
            .as_array()
            .ok_or("Explicitly identify any preset devices being skipped.")?;
        let mut accounted = BTreeSet::new();
        for value in excluded {
            let name = value
                .as_str()
                .ok_or("Skipped preset device names must be strings.")?;
            if !accounted.insert(name) || !parsed.document.devices().contains_key(name) {
                return Err(
                    "Skipped preset devices must be distinct names from this preset.".into(),
                );
            }
        }
        let mut identifiers = BTreeSet::new();
        let mut targets = Vec::new();
        for (name, identifier) in selected {
            let identifier = identifier
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or("Devices must be identified by their inventory IDs.")?;
            if !identifiers.insert(identifier) || !accounted.insert(name.as_str()) {
                return Err("Each preset device and target must be selected only once.".into());
            }
            let configuration = parsed
                .document
                .devices()
                .get(name)
                .ok_or("Select named preset devices to load.")?
                .configuration();
            let record = records
                .get(identifier)
                .ok_or("The selected inventory has changed. Review the device selection again.")?;
            if !matches_record(configuration, identifier, record) {
                return Err(Error::Invalid(format!(
                    "{name}: the selected target no longer matches this preset device."
                )));
            }
            targets.push(Target {
                identifier: identifier.into(),
                name: name.clone(),
                configuration: configuration.clone(),
                actions: requested_actions(configuration)?,
            });
        }
        if accounted.len() != parsed.document.devices().len() {
            return Err("Every preset device must be selected or explicitly skipped.".into());
        }
        if !targets.iter().any(|target| !target.actions.is_empty()) {
            return Err("This selection contains no preset settings.".into());
        }
        Ok(Self {
            name: parsed.document.name,
            digest: parsed.digest,
            targets,
            confirm_destructive,
            store_current_configuration,
            confirmed_plan: None,
        })
    }
}

impl Service {
    pub async fn plan_load(
        &self,
        parameters: &Value,
        registry: &Arc<DeviceRegistry>,
    ) -> Result<Value, Error> {
        let request = LoadRequest::new(
            self.parse(parameters).await?,
            parameters,
            &registry.preset_records()?,
            false,
        )?;
        let _serial = self
            .operation
            .try_lock()
            .map_err(|_| Error::Conflict("Another preset operation is in progress.".into()))?;
        registry.plan_preset(request).await
    }

    pub async fn load(
        self: &Arc<Self>,
        parameters: &Value,
        registry: &Arc<DeviceRegistry>,
    ) -> Result<Value, Error> {
        self.load_with_plan(parameters, registry, None).await
    }

    pub async fn load_with_plan(
        self: &Arc<Self>,
        parameters: &Value,
        registry: &Arc<DeviceRegistry>,
        confirmed_plan: Option<Value>,
    ) -> Result<Value, Error> {
        let mut request = LoadRequest::new(
            self.parse(parameters).await?,
            parameters,
            &registry.preset_records()?,
            true,
        )?;
        request.confirmed_plan = confirmed_plan;
        let serial = Arc::clone(&self.operation)
            .try_lock_owned()
            .map_err(|_| Error::Conflict("Another preset operation is in progress.".into()))?;
        registry.load_preset(request, serial).await
    }
}

pub fn confirmation(plan: &Value) -> Value {
    serde_json::json!({
        "name": plan["name"], "digest": plan["digest"],
        "devices": plan["devices"].as_array().into_iter().flatten().map(|device| serde_json::json!({
            "device_name": device["device_name"], "server_name": device["server_name"],
            "actions": device["actions"].as_array().into_iter().flatten().map(|action| {
                let current = if action["kind"] == "redundancy_mode" {
                    serde_json::json!({"current": action["current"]["current"], "configured": action["current"]["configured"], "available_modes": action["current"]["available_modes"]})
                } else { action["current"].clone() };
                serde_json::json!({"kind": action["kind"], "requested": action["requested"], "current": current, "state": action["state"], "reason": action["reason"], "topology": action["details"]["preflight"], "panel": action["details"]["precondition"]})
            }).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}
